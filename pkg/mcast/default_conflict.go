package mcast

import "github.com/jabolina/go-mcast/internal"

// A default structure for the conflict relationship
// that will always conflict.
type AlwaysConflict struct{}

// Always returns true for the conflict.
func (a AlwaysConflict) Conflict(_ internal.Message, __ []internal.Message) bool {
	return true
}
