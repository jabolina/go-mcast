package definition

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// AlwaysConflict a default structure for the conflict relationship  that will always conflict.
type AlwaysConflict struct{}

// Conflict always returns true for the conflict.
func (a AlwaysConflict) Conflict(_ types.Message, __ types.Message) bool {
	return true
}
