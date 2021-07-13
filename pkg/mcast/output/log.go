package output

import (
	"encoding/json"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
	"sync/atomic"
)

// Log abstraction for the applied commands on the protocol.
// This is an append only log, where every command will be added
// at the tail of the log.
//
// With this approach we have the whole history of commands executed
// on each partition. See that for the peers inside the partition, this
// structure will be partially equal, since some messages can be delivered
// in a generic manner.
// But between partitions, this whole log can be very different. If a intersection
// function is applied for the log between partitions, the result should be partially
// equal as well.
type Log interface {
	// Append add the message to the tail of the log structure. This is thread-safe.
	Append(types.Message, bool) error

	// Dump the whole command history at the time of the issued request. This
	// will lock the structure for reading, so there is no guarantee that the
	// read value is the latest.
	Dump() ([]types.Message, error)

	// Size is the size of the log counting the applied commands.
	Size() uint64

	// SizeInBytes is not the real size, this is counting only the parsed Message
	// as byte size, which means that the log is *at least* this size.
	SizeInBytes() uint64
}

// AppendOnlyLog is a Log implementation to hold the information.
// This struct will keep information in-memory, a good todo
// is to find a better approach to hold this information.
type AppendOnlyLog struct {
	// Synchronize operations.
	mutex *sync.Mutex

	// A Storage implementation, that can be used to persist the information
	// to a stable storage.
	storage types.Storage

	// List of commands, append only.
	log []types.LogEntry

	// Count the number of operations.
	opsCount uint64

	// Count the byte size of messages.
	bytesCount uint64
}

func NewLogStructure(storage types.Storage) Log {
	return &AppendOnlyLog{
		mutex:   &sync.Mutex{},
		storage: storage,
	}
}

// Append a new command to the log structure.
// Only messages of type Command will be added, if the operation
// is only read, since there is no change to the structure, the
// operation will not be appended.
func (a *AppendOnlyLog) Append(message types.Message, isGenericDeliver bool) error {
	if types.Command != message.Content.Operation {
		return nil
	}

	message.Content.Meta = types.Meta{
		Timestamp:  message.Timestamp,
		Identifier: message.Identifier,
	}
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	entry := types.LogEntry{
		Data:             data,
		Operation:        message.Content.Operation,
		GenericDelivered: isGenericDeliver,
	}

	atomic.AddUint64(&a.opsCount, 1)
	atomic.AddUint64(&a.bytesCount, uint64(len(data)))

	a.mutex.Lock()
	a.log = append(a.log, entry)
	a.mutex.Unlock()

	storage := types.StorageEntry{Key: message.Identifier, Value: message.Content}
	return a.storage.Set(storage)
}

func (a *AppendOnlyLog) Dump() ([]types.Message, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	var messages []types.Message
	for _, entry := range a.log {
		var message types.Message
		if err := json.Unmarshal(entry.Data, &message); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func (a *AppendOnlyLog) Size() uint64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.opsCount
}

func (a *AppendOnlyLog) SizeInBytes() uint64 {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.bytesCount
}
