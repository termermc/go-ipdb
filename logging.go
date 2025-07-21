package ipdb

import (
	"fmt"
	"os"
	"slices"
)

// LogLevel is a log level (log message type).
type LogLevel string

func (l LogLevel) String() string {
	return string(l)
}

const (
	// LogLevelInfo is an info message.
	// If logging to the console, should use stdout.
	LogLevelInfo LogLevel = "info"

	// LogLevelWarn is a warning message.
	// If logging to the console, should use stderr.
	LogLevelWarn LogLevel = "warn"

	// LogLevelError is an error message.
	// If logging to the console, should use stderr.
	LogLevelError LogLevel = "error"

	// LogLevelDebug is a debug message.
	// If logging to the console, should use stderr.
	LogLevelDebug LogLevel = "debug"
)

// Logger is an interface for logging messages from Ipdb.
type Logger interface {
	// Log a message.
	// Note that the `err` field passed to it may be nil.
	// Whether `err` is nil or not is not guaranteed by the log level; LogLevelInfo and any other level may include an error.
	Log(logLevel LogLevel, msg string, err error)
}

// DefaultLogger is the default logger for Ipdb.
// It logs messages to stdout and stderr.
// Messages are prefixed with `[ipdb]`, followed by the log level.
type DefaultLogger struct {
	// The log levels to exclude.
	// If empty, all log levels are shown.
	ExcludeLogLevels []LogLevel
}

func (l *DefaultLogger) Log(logLevel LogLevel, msg string, err error) {
	if slices.Contains(l.ExcludeLogLevels, logLevel) {
		return
	}

	var f *os.File
	if logLevel == LogLevelInfo {
		f = os.Stdout
	} else {
		f = os.Stderr
	}

	if err == nil {
		_, _ = fmt.Fprintf(f, "[ipdb] [%s] %s\n", logLevel.String(), msg)
	} else {
		_, _ = fmt.Fprintf(f, "[ipdb] [%s] %s (error = %v)\n", logLevel.String(), msg, err)
	}
}

// NewDefaultLogger creates a new DefaultLogger instance.
// It logs messages to stdout and stderr, with the log level prefixed.
// You may specify any log levels you wish to exclude, or nil to show all log levels.
func NewDefaultLogger(excludeLogLevels []LogLevel) *DefaultLogger {
	return &DefaultLogger{
		ExcludeLogLevels: excludeLogLevels,
	}
}
