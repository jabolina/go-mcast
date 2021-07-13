package protocol

// Step identifies a step for the protocol. This is used so after a
// message is processed, a client knows what to do next.
type Step uint8

const (
	// NoOp means that there is no need to execute a next step.
	NoOp Step = iota

	// ExchangeAll means that the processed message must be
	// exchanged with all processes that participate in the protocol,
	// the message must be sent to all processes in all partitions.
	ExchangeAll

	// ExchangeInternal means that the processed message must be
	// sent to all processes inside the current partition. The client
	// that belongs to a partition requested the protocol to process a
	// message, and the processed message must be sent to all processes
	// inside the same partition as the one who requested the processing.
	ExchangeInternal

	// Ended means that the message finished processing and already contains
	// the final sequence number, now only needs to wait to eventually it will
	// be delivered and committed.
	Ended
)
