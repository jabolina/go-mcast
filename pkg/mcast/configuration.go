package mcast

import "fmt"

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

// Holds information about the whole cluster.
type ClusterConfiguration struct {
	// Cluster servers
	Servers []Server
}

// Validate a cluster configuration for errors, verifying for empty and duplicated values.
func ValidateClusterConfiguration(configuration ClusterConfiguration) error {
	ids := make(map[ServerID]bool)
	addresses := make(map[ServerAddress]bool)

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
