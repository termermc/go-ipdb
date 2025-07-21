package ipdb

import (
	"fmt"
	"github.com/pkg/errors"
)

// ErrInvalidIpdbType is returned when an invalid DbType value is specified.
var ErrInvalidIpdbType = errors.New("invalid DbType value")

// ErrNoCacheAndNoDownload is returned when there is no cached database, and downloading is disabled so there is no way to get the database.
var ErrNoCacheAndNoDownload = errors.New("no cached copy of database existed, and downloading is disabled")

// ErrDataSourceNoSource is returned when a data source has no sources.
// "No sources" means that the data source has no URLs and the Get method is nil.
var ErrDataSourceNoSource = errors.New("data source has no sources: len(Urls) == 0 or Get method is nil")

// ErrAllUrlsFailed is returned when all URLs in a data source failed.
var ErrAllUrlsFailed = errors.New("all URLs in data source failed")

// NotInitializedError is returned when a function is run that required an IP database to be initialized, but it was not initialized.
// Includes the IP database type that was required but not initialized.
type NotInitializedError struct {
	// The type of database that was not initialized.
	Type DbType

	// The message.
	Msg string
}

func (err *NotInitializedError) Error() string {
	return fmt.Sprintf("ipdb %s not initialized: %s", err.Type, err.Msg)
}

// NewNotInitializedError creates a new NewNotInitializedError instance with the specified database type and message.
func NewNotInitializedError(ipdbType DbType, msg string) *NotInitializedError {
	return &NotInitializedError{
		Type: ipdbType,
		Msg:  msg,
	}
}

// ErrIpdbClosed is returned when an operation is attempted on a closed IP database.
var ErrIpdbClosed = errors.New("ipdb closed")
