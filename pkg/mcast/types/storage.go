package types

// Used to provide storage for the state machine values.
type Storage interface {
	// Set the value associated with the key
	Set(entry StorageEntry) error

	// Get the serialized value associated with the key.
	Get() ([]StorageEntry, error)
}
