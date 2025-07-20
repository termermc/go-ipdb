package ipdb_geoip_tools

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

// StorageDriver is an interface that stores IP databases and checkpoint data.
type StorageDriver interface {
	// WriteDatabase opens a database file of the specified type for writing.
	// The reader will be closed by the function regardless of whether an error occurs.
	WriteDatabase(dbType IpdbType, input io.ReadCloser) error

	// ReadDatabase opens a database file of the specified type for reading.
	// The caller is expected to close the reader.
	// If there is no cached database of the specified type, the function will return syscall.ENOENT.
	ReadDatabase(dbType IpdbType) (io.ReadCloser, error)

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
		return nil, errors.Wrapf(err, "failed to get absolute path of input path \"%s\" when creating FsStorageDriver instance", dataDir)
	}

	stat, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil, errors.Wrapf(err, "path \"%s\" did not exist when creating FsStorageDriver instance", absPath)
		} else {
			return nil, errors.Wrapf(err, "unexpected error statting path \"%s\" when creating FsStorageDriver instance", absPath)
		}
	}

	if !stat.IsDir() {
		return nil, errors.Wrapf(syscall.ENOTDIR, "path \"%s\" did not point to a directory when creating FsStorageDriver instance", absPath)
	}

	return &FsStorageDriver{
		dataDir: absPath,
	}, nil
}

// Returns the filename for the specified DB type.
// If the type valid is invalid, returns ErrInvalidIpdbType.
func (s *FsStorageDriver) dbTypeToFilename(dbType IpdbType) (string, error) {
	switch dbType {
	case IpdbTypeTorExit:
		return "tor_exit_node_ips.txt", nil
	case IpdbTypeDatacenter:
		return "datacenter_cidr_ranges.txt", nil
	case IpdbTypeGeoIpv4:
		return "geo_ipv4.mmdb", nil
	case IpdbTypeGeoIpv6:
		return "geo_ipv6.mmdb", nil
	default:
		return "", ErrInvalidIpdbType
	}
}

func (s *FsStorageDriver) WriteDatabase(dbType IpdbType, input io.ReadCloser) error {
	defer func() {
		_ = input.Close()
	}()

	filename, err := s.dbTypeToFilename(dbType)
	if err != nil {
		return err
	}

	filePath := filepath.Join(s.dataDir, filename)
	bakFilePath := filepath.Join(s.dataDir, filename+".bak")

	backedUp := false

	// Move existing file to backup if it exists.
	if _, err = os.Stat(filePath); err == nil {
		err = os.Rename(filePath, bakFilePath)
		if err != nil {
			return errors.Wrapf(err, "failed to move existing file \"%s\" to backup path \"%s\"", filePath, bakFilePath)
		}

		backedUp = true
	}

	err = nil

	if backedUp {
		// If the function returns with an error, try to restore the backup.
		defer func() {
			if err != nil {
				_ = os.Rename(bakFilePath, filePath)
			}
		}()
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, fsPermBits)
	if err != nil {
		return errors.Wrapf(err, "failed to open file \"%s\" for writing database type %s", filePath, dbType.String())
	}

	_, err = io.Copy(file, input)
	if err != nil {
		return errors.Wrapf(err, "failed to copy input to file \"%s\" for writing database type %s", filePath, dbType.String())
	}

	return nil
}

func (s *FsStorageDriver) ReadDatabase(dbType IpdbType) (io.ReadCloser, error) {
	filename, err := s.dbTypeToFilename(dbType)
	if err != nil {
		return nil, err
	}

	filePath := filepath.Join(s.dataDir, filename)

	file, err := os.OpenFile(filePath, os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file \"%s\" for database type %s", filePath, dbType.String())
	}

	return file, nil
}

func (s *FsStorageDriver) WriteCheckpoints(checkpoints *AllCheckpoints) error {
	filePath := filepath.Join(s.dataDir, checkpointsFilename)
	file, err := os.OpenFile(filePath, syscall.O_CREAT|syscall.O_WRONLY, fsPermBits)
	if err != nil {
		return errors.Wrapf(err, "failed to open file \"%s\" for writing checkpoints", filePath)
	}

	defer func() {
		_ = file.Close()
	}()

	enc := json.NewEncoder(file)
	err = enc.Encode(checkpoints)
	if err != nil {
		return errors.Wrapf(err, "failed to encode checkpoints to JSON file at \"%s\"", filePath)
	}

	return nil
}

func (s *FsStorageDriver) ReadCheckpoints() (*AllCheckpoints, error) {
	filePath := filepath.Join(s.dataDir, checkpointsFilename)
	file, err := os.OpenFile(filePath, syscall.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file \"%s\" for reading checkpoints", filePath)
	}

	var res AllCheckpoints
	dec := json.NewDecoder(file)
	err = dec.Decode(&res)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode checkpoints from JSON file at \"%s\"", filePath)
	}

	return &res, nil
}
