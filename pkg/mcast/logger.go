package mcast

import (
	"fmt"
	"log"
	"os"
)

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
}

const (
	calldepth = 2
	info      = "INFO"
	warn      = "WARN"
	errorl    = "ERROR"
	debug     = "DEBUG"
	fatal     = "FATAL"
)

func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		Logger: log.New(os.Stderr, "mcast", log.LstdFlags),
		debug:  false,
	}
}

// Use the given log level as prefix
func level(prefix, message string) string {
	return fmt.Sprintf("[%s]: %s", prefix, message)
}

// The default logger used if the user does not provide its
// own implementation.
type DefaultLogger struct {
	*log.Logger
	debug bool
}

func (l *DefaultLogger) Info(v ...interface{}) {
	l.Output(calldepth, level(info, fmt.Sprint(v...)))
}

func (l *DefaultLogger) Infof(format string, v ...interface{}) {
	l.Output(calldepth, level(info, fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Warn(v ...interface{}) {
	l.Output(calldepth, level(warn, fmt.Sprint(v...)))
}

func (l *DefaultLogger) Warnf(format string, v ...interface{}) {
	l.Output(calldepth, level(warn, fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Error(v ...interface{}) {
	l.Output(calldepth, level(errorl, fmt.Sprint(v...)))
}

func (l *DefaultLogger) Errorf(format string, v ...interface{}) {
	l.Output(calldepth, level(errorl, fmt.Sprintf(format, v...)))
}

func (l *DefaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(calldepth, level(debug, fmt.Sprint(v...)))
	}
}

func (l *DefaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(calldepth, level(debug, fmt.Sprintf(format, v...)))
	}
}

func (l *DefaultLogger) Fatal(v ...interface{}) {
	l.Output(calldepth, level(fatal, fmt.Sprint(v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Fatalf(format string, v ...interface{}) {
	l.Output(calldepth, level(fatal, fmt.Sprintf(format, v...)))
	os.Exit(1)
}

func (l *DefaultLogger) Panic(v ...interface{}) {
	l.Logger.Panic(v...)
}

func (l *DefaultLogger) Panicf(format string, v ...interface{}) {
	l.Logger.Panicf(format, v...)
}
