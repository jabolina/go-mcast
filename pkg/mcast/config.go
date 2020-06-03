package mcast

import (
	"fmt"
	"github.com/hashicorp/go-hclog"
	"os"
)

// The version of the protocol, that includes RPC messages
// and the implementation specific treatment. Use the
// ProtocolVersion member on the config to control the protocol
// version at which the servers will communicate with each other.
// This enables the possibility to easy backwards compatibility, and
// if unsure to which version to use, just jump to the most recent version.
// Each new version will have a short description of each version
//
// 0: Original and first available implementation.
type ProtocolVersion uint

const LatestProtocolVersion = 0

type Config struct {
	// Control for versioning the protocol, so the server
	// can work on different versions and known which
	// version each one is talking.
	Version ProtocolVersion

	// User provided logger to be used.
	Logger hclog.Logger

	// LogLevel represents a log level.
	LogLevel string
}

// Creates a default configuration that can ready to be used.
func Default() *Config {
	return &Config{
		Version:  LatestProtocolVersion,
		Logger:   hclog.New(&hclog.LoggerOptions{Level: hclog.Debug, Output: os.Stdout}),
		LogLevel: "DEBUG",
	}
}

// Verify if the given configuration is valid to be used
func ValidateConfig(config *Config) error {
	if config.Version > LatestProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must be in 0 up to %d", config.Version, LatestProtocolVersion)
	}

	if config.Logger == nil {
		config.Logger = hclog.New(&hclog.LoggerOptions{Level: hclog.Debug, Output: os.Stdout})
	}

	return nil
}
