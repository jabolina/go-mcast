package go_mcast

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
