package ipdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// StorageDriver is an interface that stores IP databases and checkpoint data.
type StorageDriver interface {
	// WriteDatabase writes the contents of the reader to the database file with the specified name.
	// The reader will be closed by the function regardless of whether an error occurs.
	// If an error occurs, any previously written database with the same name must be left intact.
	// May be called concurrently for different names.
	WriteDatabase(name string, input io.ReadCloser) error

	// ReadDatabase opens the database file with the specified name for reading.
	// The caller is expected to close the reader.
	// If there is no cached database with the specified name, the function must return an error that matches
	// fs.ErrNotExist when checked with errors.Is (such as syscall.ENOENT or a wrapped error from the os package).
	ReadDatabase(name string) (io.ReadCloser, error)

	// WriteCheckpoints writes all checkpoints.
	// Checkpoints must not be nil.
	WriteCheckpoints(checkpoints *AllCheckpoints) error

	// ReadCheckpoints reads and returns all checkpoints.
	// The returned checkpoints will never be nil if there is no error.
	// If checkpoints have not been saved yet, the function must return an error that matches
	// fs.ErrNotExist when checked with errors.Is (such as syscall.ENOENT or a wrapped error from the os package).
	ReadCheckpoints() (*AllCheckpoints, error)
}

// DbNameValidator can optionally be implemented by a StorageDriver to reject database names it cannot store.
// If implemented, NewIpdb validates all range database names with it and fails early if any are rejected.
type DbNameValidator interface {
	// ValidateDatabaseName returns an error if a database with the specified name cannot be stored.
	ValidateDatabaseName(name string) error
}

const fsPermBits = 0644
const checkpointsFilename = "checkpoints.json"

// tmpFilePrefix is the prefix of temporary files created while writing.
const tmpFilePrefix = ".tmp-"

// FsStorageDriver implements StorageDriver by storing databases and checkpoints inside a data directory.
// Use NewFsStorageDriver to create an instance.
type FsStorageDriver struct {
	dataDir string
}

// NewFsStorageDriver creates a new instance of StorageDriver.
// The specified directory must exist and be readable and writable by the current user.
// If the directory does not exist, returns an error matching fs.ErrNotExist (and syscall.ENOENT on Unix-like systems).
// If the path is not a directory, returns a wrapped syscall.ENOTDIR.
func NewFsStorageDriver(dataDir string) (*FsStorageDriver, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf(`failed to get absolute path of input path %q when creating FsStorageDriver instance: %w`, dataDir, err)
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf(`path %q did not exist when creating FsStorageDriver instance: %w`, absPath, err)
		}
		return nil, fmt.Errorf(`unexpected error statting path %q when creating FsStorageDriver instance: %w`, absPath, err)
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf(`path %q did not point to a directory when creating FsStorageDriver instance: %w`, absPath, syscall.ENOTDIR)
	}

	return &FsStorageDriver{
		dataDir: absPath,
	}, nil
}

// ValidateDatabaseName returns an error if a database with the specified name cannot be stored by the driver.
// If the name exceeds DbNameMaxSize bytes, returns ErrDbNameTooLong.
// If the name is empty or would collide with files used internally by the driver, returns a wrapped ErrInvalidDbName.
func (s *FsStorageDriver) ValidateDatabaseName(name string) error {
	if len(name) > DbNameMaxSize {
		return ErrDbNameTooLong
	}

	switch {
	case name == "":
		return fmt.Errorf("%w: name must not be empty", ErrInvalidDbName)
	case name == "." || name == "..":
		return fmt.Errorf(`%w: %q is not a valid filename`, ErrInvalidDbName, name)
	case name == checkpointsFilename:
		return fmt.Errorf(`%w: %q is reserved for the checkpoints file`, ErrInvalidDbName, name)
	case strings.HasPrefix(name, tmpFilePrefix):
		return fmt.Errorf(`%w: names starting with %q are reserved for temporary files`, ErrInvalidDbName, tmpFilePrefix)
	}

	return nil
}

// Returns the filename for the database with the specified name.
// If the name is invalid, returns the error from ValidateDatabaseName.
func (s *FsStorageDriver) dbNameToFilename(name string) (string, error) {
	if err := s.ValidateDatabaseName(name); err != nil {
		return "", err
	}

	return url.QueryEscape(name), nil
}

// writeFileAtomic writes a file in the data directory by writing to a unique temp file and renaming it to the destination.
// If anything fails, the destination file is left untouched and the temp file is removed.
func (s *FsStorageDriver) writeFileAtomic(filename string, write func(w io.Writer) error) (err error) {
	destFilePath := filepath.Join(s.dataDir, filename)

	file, err := os.CreateTemp(s.dataDir, tmpFilePrefix+"*")
	if err != nil {
		return fmt.Errorf(`failed to create temp file for writing %q: %w`, destFilePath, err)
	}
	tmpFilePath := file.Name()

	defer func() {
		if err != nil {
			_ = file.Close()
			_ = os.Remove(tmpFilePath)
		}
	}()

	if err = write(file); err != nil {
		return err
	}

	if err = file.Chmod(fsPermBits); err != nil {
		return fmt.Errorf(`failed to set permissions of temp file %q: %w`, tmpFilePath, err)
	}

	// Flush to disk before renaming, otherwise a crash could leave an empty or partial file at the destination.
	if err = file.Sync(); err != nil {
		return fmt.Errorf(`failed to sync temp file %q: %w`, tmpFilePath, err)
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf(`failed to close temp file %q: %w`, tmpFilePath, err)
	}

	// Move temp file to destination path.
	if err = os.Rename(tmpFilePath, destFilePath); err != nil {
		return fmt.Errorf(`failed to move temp file %q to destination path %q: %w`, tmpFilePath, destFilePath, err)
	}

	return nil
}

func (s *FsStorageDriver) WriteDatabase(name string, input io.ReadCloser) error {
	defer func() {
		_ = input.Close()
	}()

	filename, err := s.dbNameToFilename(name)
	if err != nil {
		return err
	}

	return s.writeFileAtomic(filename, func(w io.Writer) error {
		if _, err := io.Copy(w, input); err != nil {
			return fmt.Errorf(`failed to copy input to file for writing database %q: %w`, name, err)
		}
		return nil
	})
}

func (s *FsStorageDriver) ReadDatabase(name string) (io.ReadCloser, error) {
	filename, err := s.dbNameToFilename(name)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(s.dataDir, filename)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf(`failed to open file %q for database type %s: %w`, filePath, name, err)
	}

	return file, nil
}

func (s *FsStorageDriver) WriteCheckpoints(checkpoints *AllCheckpoints) error {
	return s.writeFileAtomic(checkpointsFilename, func(w io.Writer) error {
		if err := json.NewEncoder(w).Encode(checkpoints); err != nil {
			return fmt.Errorf("failed to encode checkpoints to JSON file: %w", err)
		}
		return nil
	})
}

func (s *FsStorageDriver) ReadCheckpoints() (*AllCheckpoints, error) {
	filePath := filepath.Join(s.dataDir, checkpointsFilename)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf(`failed to open file %q for reading checkpoints: %w`, filePath, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var res AllCheckpoints
	dec := json.NewDecoder(file)
	err = dec.Decode(&res)
	if err != nil {
		return nil, fmt.Errorf(`failed to decode checkpoints from JSON file at %q: %w`, filePath, err)
	}

	if res.Checkpoints == nil {
		// Might be an old version of the file.
		// Create an empty checkpoints map.
		res.Checkpoints = make(map[string]Checkpoint)
	}

	return &res, nil
}
