package types

// This interface is used as a replacement for the heap.Interface.
// The default golang interface executes the Swap method between the
// head and the last element when a Pop is called, this leads to an
// undesired behaviour where the head changes for the wrong reasons.
//
// The minimum, e.g., the next message to be delivered must
// be on the "head".
// Using this structure, we should not be manually trying to
// retrieve the element on the head, for a given validation
// function the value is returned through a channel, notifying
// that the value is ready.
type ReceivedQueue interface {
	// Add a new element to the RecvQueue. After this push the
	// elements will be sorted again if a change occurred.
	// This also should be used when just updating a value.
	Push(message Message)

	// Remove the next element that is ready to be delivered from
	// the queue. After the element is removed the heap algorithm
	// will be executed again.
	Pop() interface{}

	// Remove the message that contains the given identifier.
	// After the item is removed the heap sorted again.
	Remove(uid UID) interface{}

	// Return all the elements present on the queue at the time
	// of the read. After the elements are returned the actual
	// values can be different.
	Values() []Message

	// Get the Message element by the given UID. If the value is
	// not present returns nil.
	GetByKey(uid UID) (Message, bool)
}
