package definition

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

// The default Storage implementation. This struct do not hold
// any information.
type EmptyStorage struct {
}

func NewDefaultStorage() types.Storage {
	return &EmptyStorage{}
}

func (e EmptyStorage) Set(entry types.StorageEntry) error {
	return nil
}

func (e EmptyStorage) Get() ([]types.StorageEntry, error) {
	return nil, nil
}
