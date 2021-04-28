package util

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
)

type Holder struct {
	timestamp uint64
	mutex     *sync.Mutex
}

func NewHolder() *Holder {
	return &Holder{mutex: &sync.Mutex{}}
}

func (h *Holder) Set(value uint64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.timestamp = value
}

func (h *Holder) Get() uint64 {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.timestamp
}

type MessageId struct {
	Id        types.UID
	Timestamp uint64
}

type SafeSlice struct {
	data  []MessageId
	mutex *sync.Mutex
}

func NewSafeSlice() *SafeSlice {
	return &SafeSlice{
		data:  []MessageId{},
		mutex: &sync.Mutex{},
	}
}

func (s *SafeSlice) Add(value MessageId) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data = append(s.data, value)
}

func (s *SafeSlice) Len() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return len(s.data)
}

func (s *SafeSlice) Get(index int) MessageId {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.data[index]
}
