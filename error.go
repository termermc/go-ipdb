package ipdb

import (
	"errors"
	"fmt"
)

// ErrNoCacheAndNoDownload is returned when there is no cached database, and downloading is disabled so there is no way to get the database.
var ErrNoCacheAndNoDownload = errors.New("no cached copy of database existed, and downloading is disabled")

// ErrDataSourceNoSource is returned when a data source has no sources.
// "No sources" means that the data source has no URLs and the Get method is nil.
var ErrDataSourceNoSource = errors.New("data source has no sources: len(Urls) == 0 or Get method is nil")

// ErrAllUrlsFailed is returned when all URLs in a data source failed.
var ErrAllUrlsFailed = errors.New("all URLs in data source failed")

// ErrFailedByRetryPolicy is returned when a database update was failed by the RetryPolicy returning RetryActionFail.
// The error also wraps the error of the source that failed.
var ErrFailedByRetryPolicy = errors.New("database update was failed by the retry policy")

// HttpStatusError is returned when downloading a source yields a response with a status code other than 200.
type HttpStatusError struct {
	// The response status code.
	StatusCode int

	// The start of the response body, which may help explain the error.
	BodyPreview string
}

func (err *HttpStatusError) Error() string {
	return fmt.Sprintf("status code was %d (expected %d): %q", err.StatusCode, 200, err.BodyPreview)
}

// NotInitializedError is returned when a function is run that required an IP database to be initialized, but it was not initialized.
// Includes the IP database type that was required but not initialized.
type NotInitializedError struct {
	// The type of database that was not initialized.
	Name string

	// The message.
	Msg string
}

func (err *NotInitializedError) Error() string {
	return fmt.Sprintf("ipdb %s not initialized: %s", err.Name, err.Msg)
}

// NewNotInitializedError creates a new NotInitializedError instance with the specified database name and message.
func NewNotInitializedError(name string, msg string) *NotInitializedError {
	return &NotInitializedError{
		Name: name,
		Msg:  msg,
	}
}

// ErrDatabaseTooLarge is returned when a database is larger than the maximum database size.
// See Options.MaxDatabaseSize.
var ErrDatabaseTooLarge = errors.New("database is too large")

// ErrNoRanges is returned when a range database contains no IP addresses or CIDR ranges.
// A source that yields an empty list is treated as broken, rather than replacing existing data with nothing.
var ErrNoRanges = errors.New("range database contains no IP addresses or CIDR ranges")

// ErrInvalidRefreshInterval is returned when a data source's RefreshInterval is not positive while downloading is enabled.
var ErrInvalidRefreshInterval = errors.New("data source RefreshInterval must be positive")

// ErrInvalidDbName is returned when a database name is empty, reserved or otherwise unusable.
var ErrInvalidDbName = errors.New("invalid database name")

// ErrIpdbClosed is returned when an operation is attempted on a closed IP database.
var ErrIpdbClosed = errors.New("ipdb closed")

// ErrDbNameTooLong is returned when a database name exceeds DbNameMaxSize bytes.
var ErrDbNameTooLong = fmt.Errorf("database name too long, must be at most %d bytes long", DbNameMaxSize)

// NoSuchDatabaseError is returned when trying to access a IP range database that does not exist.
// Includes the requested database name that did not exist.
type NoSuchDatabaseError struct {
	// The name of database that did not exist.
	Name string
}

func (err *NoSuchDatabaseError) Error() string {
	return fmt.Sprintf(`IP range database %q does not exist`, err.Name)
}

// NewNoSuchDatabaseError creates a new NoSuchDatabaseError instance with the specified database name.
func NewNoSuchDatabaseError(name string) *NoSuchDatabaseError {
	return &NoSuchDatabaseError{
		Name: name,
	}
}
