package definition

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// A default structure for the conflict relationship
// that will always conflict.
type AlwaysConflict struct{}

// Always returns true for the conflict.
func (a AlwaysConflict) Conflict(_ types.Message, __ []types.Message) bool {
	return true
}
