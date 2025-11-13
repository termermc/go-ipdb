package ipdb

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
)

// StorageDriver is an interface that stores IP databases and checkpoint data.
type StorageDriver interface {
	// WriteDatabase opens the database file with the specified name for writing.
	// The reader will be closed by the function regardless of whether an error occurs.
	WriteDatabase(name string, input io.ReadCloser) error

	// ReadDatabase opens the database file with the specified name for reading.
	// The caller is expected to close the reader.
	// If there is no cached database with the specified name, the function will return syscall.ENOENT.
	ReadDatabase(name string) (io.ReadCloser, error)

	// WriteCheckpoints writes all checkpoints.
	// Checkpoints must not be nil.
	WriteCheckpoints(checkpoints *AllCheckpoints) error

	// ReadCheckpoints reads and returns all checkpoints.
	// The returned checkpoints will never be nil if there is no error.
	// If checkpoints have not been saved yet, the function will return syscall.ENOENT.
	ReadCheckpoints() (*AllCheckpoints, error)
}

const fsPermBits = 0644
const checkpointsFilename = "checkpoints.json"

// FsStorageDriver implements StorageDriver by storing databases and checkpoints inside a data directory.
// Use NewFsStorageDriver to create an instance.
type FsStorageDriver struct {
	dataDir string
}

// NewFsStorageDriver creates a new instance of StorageDriver.
// The specified directory must exist and be readable and writable by the current user.
// If the directory does not exist, returns a wrapped syscall.ENOENT.
// If the path is not a directory, returns a wrapped syscall.ENOTDIR.
func NewFsStorageDriver(dataDir string) (*FsStorageDriver, error) {
	absPath, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf(`failed to get absolute path of input path "%s" when creating FsStorageDriver instance: %w`, dataDir, err)
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil, fmt.Errorf(`path "%s" did not exist when creating FsStorageDriver instance: %w`, absPath, err)
		} else {
			return nil, fmt.Errorf(`unexpected error statting path "%s" when creating FsStorageDriver instance: %w`, absPath, err)
		}
	}

	if !stat.IsDir() {
		return nil, fmt.Errorf(`path "%s" did not point to a directory when creating FsStorageDriver instance: %w`, absPath, err)
	}

	return &FsStorageDriver{
		dataDir: absPath,
	}, nil
}

// Returns the filename for the specified DB type.
// If the type valid is invalid, returns ErrInvalidIpdbType.
func (s *FsStorageDriver) dbNameToFilename(name string) (string, error) {
	if len(name) > DbNameMaxSize {
		return "", ErrDbNameTooLong
	}

	return url.QueryEscape(name), nil
}

func (s *FsStorageDriver) WriteDatabase(name string, input io.ReadCloser) error {
	defer func() {
		_ = input.Close()
	}()

	filename, err := s.dbNameToFilename(name)
	if err != nil {
		return err
	}

	destFilePath := filepath.Join(s.dataDir, filename)
	tmpFilePath := filepath.Join(s.dataDir, filename+".tmp")

	file, err := os.OpenFile(tmpFilePath, os.O_CREATE|os.O_WRONLY, fsPermBits)
	if err != nil {
		return fmt.Errorf(`failed to open file "%s" for writing database "%s": %w`, destFilePath, name, err)
	}

	_, err = io.Copy(file, input)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf(`failed to copy input to file "%s" for writing database "%s": %w`, destFilePath, name, err)
	}
	_ = file.Close()

	// Move temp file to destination path.
	err = os.Rename(tmpFilePath, destFilePath)
	if err != nil {
		return fmt.Errorf(`failed to move temp file "%s" to destination path "%s": %w`, tmpFilePath, destFilePath, err)
	}

	return nil
}

func (s *FsStorageDriver) ReadDatabase(name string) (io.ReadCloser, error) {
	filename, err := s.dbNameToFilename(name)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(s.dataDir, filename)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf(`failed to open file "%s" for database type %s: %w`, filePath, name, err)
	}

	return file, nil
}

func (s *FsStorageDriver) WriteCheckpoints(checkpoints *AllCheckpoints) error {
	destFilePath := filepath.Join(s.dataDir, checkpointsFilename)
	tmpFilePath := filepath.Join(s.dataDir, checkpointsFilename+".tmp")

	file, err := os.OpenFile(tmpFilePath, syscall.O_CREAT|syscall.O_WRONLY, fsPermBits)
	if err != nil {
		return fmt.Errorf(`failed to open file "%s" for writing checkpoints: %w`, destFilePath, err)
	}

	enc := json.NewEncoder(file)
	err = enc.Encode(checkpoints)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf(`failed to encode checkpoints to JSON file at "%s": %w`, destFilePath, err)
	}
	_ = file.Close()

	// Move temp file to destination path.
	err = os.Rename(tmpFilePath, destFilePath)
	if err != nil {
		return fmt.Errorf(`failed to move temp file "%s" to destination path "%s": %w`, tmpFilePath, destFilePath, err)
	}

	return nil
}

func (s *FsStorageDriver) ReadCheckpoints() (*AllCheckpoints, error) {
	filePath := filepath.Join(s.dataDir, checkpointsFilename)
	file, err := os.OpenFile(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf(`failed to open file "%s" for reading checkpoints: %w`, filePath, err)
	}

	var res AllCheckpoints
	dec := json.NewDecoder(file)
	err = dec.Decode(&res)
	if err != nil {
		return nil, fmt.Errorf(`failed to decode checkpoints from JSON file at "%s": %w`, filePath, err)
	}

	if res.Checkpoints == nil {
		// Might be an old version of the file.
		// Create an empty checkpoints map.
		res.Checkpoints = make(map[string]Checkpoint)
	}

	return &res, nil
}
