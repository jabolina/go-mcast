package protocol

type Step uint8

const (
	NoOp Step = iota

	ExchangeAll

	ExchangeInternal

	Ended
)
