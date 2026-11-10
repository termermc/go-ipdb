package ipdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func TestNewFsStorageDriver_PathDoesNotExist(t *testing.T) {
	// Point to a definitely non-existent path inside temp dir
	tmp := t.TempDir()
	missing := filepath.Join(tmp, "does-not-exist")
	drv, err := NewFsStorageDriver(missing)
	if err == nil {
		t.Fatalf("expected error for non-existent path, got driver: %+v", drv)
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected wrapped ENOENT, got: %v", err)
	}
}

func TestNewFsStorageDriver_PathIsNotDirectory(t *testing.T) {
	tmp := t.TempDir()
	// Create a file to pass as path
	filePath := filepath.Join(tmp, "file.txt")
	if err := os.WriteFile(filePath, []byte("hi"), 0644); err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	drv, err := NewFsStorageDriver(filePath)
	if err == nil {
		t.Fatalf("expected error for non-directory path, got driver: %+v", drv)
	}
	if !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("expected ENOTDIR, got: %v", err)
	}
	if strings.Contains(err.Error(), "%!") {
		t.Fatalf("malformed error message: %q", err.Error())
	}
}

func TestFsStorageDriver_WriteAndReadDatabase(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	name := "my db/name?with spaces" // exercises URL escaping
	content := "hello world"

	rc := io.NopCloser(strings.NewReader(content))
	if err := drv.WriteDatabase(name, rc); err != nil {
		t.Fatalf("WriteDatabase failed: %v", err)
	}

	r, err := drv.ReadDatabase(name)
	if err != nil {
		t.Fatalf("ReadDatabase failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != content {
		t.Fatalf("unexpected content: got %q want %q", string(data), content)
	}
}

// faultyReader simulates a reader that fails after some bytes
type faultyReader struct{}

func (f *faultyReader) Read(_ []byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (f *faultyReader) Close() error               { return nil }

func TestFsStorageDriver_BackupAndRestoreOnWriteFailure(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	name := "db1"

	// First, write valid content
	if err := drv.WriteDatabase(name, io.NopCloser(strings.NewReader("original"))); err != nil {
		t.Fatalf("initial WriteDatabase failed: %v", err)
	}

	// Now write with a reader that fails, which should keep the original via backup-restore
	err = drv.WriteDatabase(name, &faultyReader{})
	if err == nil {
		t.Fatalf("expected error from WriteDatabase with faulty reader")
	}

	// Ensure the content is still the original
	r, err := drv.ReadDatabase(name)
	if err != nil {
		t.Fatalf("ReadDatabase failed: %v", err)
	}
	defer func() {
		_ = r.Close()
	}()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(data) != "original" {
		t.Fatalf("backup restore failed, got %q", string(data))
	}
}

func TestFsStorageDriver_ReadDatabase_MissingReturnsENOENT(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	_, err = drv.ReadDatabase("missing-db")
	if err == nil {
		t.Fatalf("expected error when reading missing db")
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected wrapped ENOENT, got: %v", err)
	}
}

func TestFsStorageDriver_Checkpoints_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	cps := &AllCheckpoints{Checkpoints: map[string]Checkpoint{
		"a": {LastUpdatedUnix: 123},
		"b": {LastUpdatedUnix: 456},
	}}

	if err := drv.WriteCheckpoints(cps); err != nil {
		t.Fatalf("WriteCheckpoints failed: %v", err)
	}

	got, err := drv.ReadCheckpoints()
	if err != nil {
		t.Fatalf("ReadCheckpoints failed: %v", err)
	}

	if got.Checkpoints["a"].LastUpdatedUnix != 123 || got.Checkpoints["b"].LastUpdatedUnix != 456 {
		t.Fatalf("unexpected checkpoints data: %+v", got)
	}
}

func TestFsStorageDriver_ReadCheckpoints_MissingReturnsENOENT(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	_, err = drv.ReadCheckpoints()
	if err == nil {
		t.Fatalf("expected error when reading missing checkpoints file")
	}
	if !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("expected wrapped ENOENT, got: %v", err)
	}
}

func TestFsStorageDriver_ReadCheckpoints_EmptyFileResultsInNonNilMap(t *testing.T) {
	tmp := t.TempDir()
	// Manually create an empty JSON object to simulate older format with missing field
	if err := os.WriteFile(filepath.Join(tmp, "checkpoints.json"), []byte("{}"), 0644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	got, err := drv.ReadCheckpoints()
	if err != nil {
		t.Fatalf("ReadCheckpoints failed: %v", err)
	}
	if got.Checkpoints == nil {
		t.Fatalf("expected non-nil checkpoints map for empty JSON object")
	}
}

func TestDbNameTooLong_Error(t *testing.T) {
	tmp := t.TempDir()
	drv, err := NewFsStorageDriver(tmp)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	// Create a name exceeding DbNameMaxSize bytes
	longName := strings.Repeat("x", DbNameMaxSize+1)
	err = drv.WriteDatabase(longName, io.NopCloser(strings.NewReader("data")))
	if err == nil {
		t.Fatalf("expected error for too long database name")
	}
	if !errors.Is(err, ErrDbNameTooLong) {
		t.Fatalf("expected ErrDbNameTooLong, got: %v", err)
	}
}

// A temp file left behind by an interrupted write does not corrupt the next
// checkpoints file.
func TestFsStorageDriver_WriteCheckpoints_IgnoresStaleTempFile(t *testing.T) {
	tmpDir := t.TempDir()
	drv, err := NewFsStorageDriver(tmpDir)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	// Simulate a longer temp file left behind by an interrupted write.
	stale := []byte(`{"checkpoints":{"some_very_long_database_name_here":{"last_updated_unix":1234567890}}}`)
	if err := os.WriteFile(filepath.Join(tmpDir, checkpointsFilename+".tmp"), stale, 0644); err != nil {
		t.Fatalf("failed to create stale temp file: %v", err)
	}

	cps := &AllCheckpoints{Checkpoints: map[string]Checkpoint{
		"a": {LastUpdatedUnix: 1},
	}}
	if err := drv.WriteCheckpoints(cps); err != nil {
		t.Fatalf("WriteCheckpoints failed: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(tmpDir, checkpointsFilename))
	if err != nil {
		t.Fatalf("failed to read checkpoints file: %v", err)
	}
	if !json.Valid(raw) {
		t.Errorf("checkpoints file is not valid JSON after WriteCheckpoints: %q", string(raw))
	}
}

// Writing a database does not affect another database whose name looks like a
// temp file name.
func TestFsStorageDriver_WriteDatabase_DoesNotClobberTempLikeNames(t *testing.T) {
	drv, err := NewFsStorageDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	if err := drv.WriteDatabase("a.tmp", io.NopCloser(strings.NewReader("database a.tmp contents"))); err != nil {
		t.Fatalf("initial WriteDatabase failed: %v", err)
	}
	if err := drv.WriteDatabase("a", io.NopCloser(strings.NewReader("database a contents"))); err != nil {
		t.Fatalf("WriteDatabase failed: %v", err)
	}

	r, err := drv.ReadDatabase("a.tmp")
	if err != nil {
		t.Fatalf("writing database \"a\" destroyed database \"a.tmp\": %v", err)
	}
	defer func() {
		_ = r.Close()
	}()

	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(got) != "database a.tmp contents" {
		t.Errorf("database \"a.tmp\" was corrupted by writing database \"a\": got %q", string(got))
	}
}

// Concurrent writes of the same database each use their own temp file, so the
// result is one complete write and no temp files are left behind.
func TestFsStorageDriver_WriteDatabase_ConcurrentWritesDoNotInterleave(t *testing.T) {
	dir := t.TempDir()
	drv, err := NewFsStorageDriver(dir)
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	const writers = 8
	const size = 1 << 20

	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			content := bytes.Repeat([]byte{byte('a' + i)}, size)
			if err := drv.WriteDatabase("db", io.NopCloser(bytes.NewReader(content))); err != nil {
				t.Errorf("WriteDatabase failed: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := os.ReadFile(filepath.Join(dir, "db"))
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if len(got) != size || !bytes.Equal(got, bytes.Repeat(got[:1], size)) {
		t.Errorf("database file is a mix of multiple writes (length %d)", len(got))
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected only the database file in the data directory, found %d entries", len(entries))
	}
}

// Names that would collide with the checkpoints file or temp files, or are not
// valid filenames, are rejected without touching existing data.
func TestFsStorageDriver_RejectsReservedDatabaseNames(t *testing.T) {
	drv, err := NewFsStorageDriver(t.TempDir())
	if err != nil {
		t.Fatalf("NewFsStorageDriver failed: %v", err)
	}

	cps := &AllCheckpoints{Checkpoints: map[string]Checkpoint{"a": {LastUpdatedUnix: 1}}}
	if err := drv.WriteCheckpoints(cps); err != nil {
		t.Fatalf("WriteCheckpoints failed: %v", err)
	}

	for _, name := range []string{checkpointsFilename, "", ".", "..", tmpFilePrefix + "123"} {
		err := drv.WriteDatabase(name, io.NopCloser(strings.NewReader("db contents")))
		if !errors.Is(err, ErrInvalidDbName) {
			t.Errorf("WriteDatabase(%q) returned %v, want ErrInvalidDbName", name, err)
		}
		if _, err := drv.ReadDatabase(name); !errors.Is(err, ErrInvalidDbName) {
			t.Errorf("ReadDatabase(%q) returned %v, want ErrInvalidDbName", name, err)
		}
	}

	if _, err := drv.ReadCheckpoints(); err != nil {
		t.Errorf("checkpoints are unreadable after rejected writes: %v", err)
	}
}
