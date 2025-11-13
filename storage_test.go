package ipdb

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
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
	// Implementation currently returns a generic wrapped error; just assert it's an error
	_ = drv
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
	defer r.Close()

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

func (f *faultyReader) Read(p []byte) (int, error) { return 0, io.ErrUnexpectedEOF }
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
	defer r.Close()
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
