package mcast

import (
	"fmt"
	"net"
	"time"
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

const (
	// Holds which version is the latest.
	LatestProtocolVersion = 0

	// Use a maximum timeout for transport configuration.
	transportMaxTimeout = 30 * time.Second
)

// Unique ServerID across all servers used for identification.
type ServerID string

// Network server address so transport can contact.
type ServerAddress string

// Context to hold information about a single server instance.
type Server struct {
	// ID of the server information held in context.
	ID ServerID
	// Address of the server held in context.
	Address ServerAddress
}

// The basic configuration to be used throughout the protocol.
// Will inform the protocol version used, the logger and the
// log level.
type BaseConfiguration struct {
	// Control for versioning the protocol, so the server
	// can work on different versions and known which
	// version each one is talking.
	Version ProtocolVersion

	// User provided logger to be used.
	Logger Logger

	// LogLevel represents a log level.
	LogLevel string
}

// Used for transport configuration, this will be applied to all
// bootstrapped transports, cannot be applied individually.
type TransportConfiguration struct {
	// Use advertise address on transport, defaults to nil.
	UseAdvertiseAddress net.Addr

	// How many connections can be pooled, default to 3 connections.
	PoolSize uint8

	// Transport timeout for communication, default to 5 seconds.
	Timeout time.Duration

	// Resolver for server addresses.
	Resolver ServerAddressResolver
}

// Holds information about the whole cluster.
type ClusterConfiguration struct {
	// Cluster servers
	Servers []Server

	// Configuration to be applied when bootstrapping the transport
	// for the group nodes.
	TransportConfiguration TransportConfiguration
}

// Creates a default configuration that can ready to be used.
func DefaultBaseConfiguration() *BaseConfiguration {
	return &BaseConfiguration{
		Version:  LatestProtocolVersion,
		Logger:   NewDefaultLogger(),
		LogLevel: "ERROR",
	}
}

// Create a configuration for transport using the default values.
// For default will not be used the advertise address, will be
// pooled 3 connections and the transport timeout is 5 seconds.
func DefaultTransportConfiguration() *TransportConfiguration {
	return &TransportConfiguration{
		UseAdvertiseAddress: nil,
		PoolSize:            3,
		Timeout:             5 * time.Second,
	}
}

// Verify if the given configuration is valid to be used.
func ValidateBaseConfiguration(config *BaseConfiguration) error {
	if config.Version > LatestProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must be in 0 up to %d", config.Version, LatestProtocolVersion)
	}

	if config.Logger == nil {
		config.Logger = NewDefaultLogger()
	}

	return nil
}

// Verify the transport configuration for errors.
func ValidateTransportConfiguration(configuration *TransportConfiguration) error {
	if configuration.Timeout > transportMaxTimeout {
		return fmt.Errorf("transport timeout too high %v max is %v", configuration.Timeout, transportMaxTimeout)
	}

	if configuration.Resolver == nil {
		return fmt.Errorf("server address resolver cannot be null")
	}

	return nil
}

// Validate a cluster configuration for errors, verifying for empty and duplicated values.
func ValidateClusterConfiguration(configuration *ClusterConfiguration) error {
	ids := make(map[ServerID]bool)
	addresses := make(map[ServerAddress]bool)

	if len(configuration.Servers) == 0 {
		return fmt.Errorf("server configuration can not be empty")
	}

	for _, server := range configuration.Servers {
		if len(server.ID) == 0 {
			return fmt.Errorf("empty id for server: %v", configuration)
		}

		if len(server.Address) == 0 {
			return fmt.Errorf("empty address for server: %v", server)
		}

		if _, ok := ids[server.ID]; ok {
			return fmt.Errorf("duplicate id for server: %v", server)
		}
		ids[server.ID] = true

		if _, ok := addresses[server.Address]; ok {
			return fmt.Errorf("duplicate address for server: %v", server)
		}
		addresses[server.Address] = true
	}
	return nil
}
