package types

// This interface will be created by the client, so its
// own logger can be provided. If none is provided the default
// logger will use the the golang logger.
type Logger interface {
	// Utilities to log at info level.
	Info(v ...interface{})
	Infof(format string, v ...interface{})

	// Utilities to log at warn level.
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})

	// Utilities to log at error level.
	Error(v ...interface{})
	Errorf(format string, v ...interface{})

	// Utilities to log at debug level.
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})

	// Toggle debug on/off
	ToggleDebug(value bool) bool
}
