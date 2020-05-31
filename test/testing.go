package test

import (
	"github.com/hashicorp/go-hclog"
	"os"
	"testing"
)

func newTestLogger(t *testing.T) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{
		Output: os.Stdout,
		Level:  hclog.DefaultLevel,
	})
}
