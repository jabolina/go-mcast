package types

// Used to transfer information.
type DataHolder struct {
	// What kind of operation is being executed.
	Operation Operation

	// This will be used to associate the value with something
	// so the retrieval can be done more easily.
	// If nothing is provided, will be generated a value to
	// be used based on the cluster information.
	Key []byte

	// If there is any value to be written into the
	// state machine it will be hold here.
	// This will only have a value if the operation is
	// of type command.
	// If the value is nil, then the state machine value
	// associated will also be nil.
	Content []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	// The entry will hold the same extension sent by the used.
	Extensions []byte
}

// Entry that is committed into the state machine.
type Entry struct {
	// Which kind of entry is this.
	Operation Operation

	// The message identifier.
	Identifier UID

	// Key for association when writing the entry into the store.
	Key []byte

	// Holds the final timestamp when the message was committed.
	FinalTimestamp uint64

	// Holds the information to be replicated across replicas.
	Data []byte

	// Extensions holds an opaque byte slice of information for middleware. It
	// is up to the client of the library to properly modify this as it adds
	// layers and remove those layers when appropriate.
	// The entry will hold the same extension sent by the used.
	Extensions []byte
}
