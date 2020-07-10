package fuzzy

import "github.com/jabolina/go-mcast/internal"

func GenerateRandomRequest(partitions []internal.Partition) internal.Request {
	return internal.Request{
		Key:         []byte(internal.GenerateUID()),
		Value:       []byte(internal.GenerateUID()),
		Extra:       []byte(internal.GenerateUID()),
		Destination: partitions,
	}
}

func GenerateRandomRequestValue(key []byte, partitions []internal.Partition) internal.Request {
	return internal.Request{
		Key:         key,
		Value:       []byte(internal.GenerateUID()),
		Extra:       []byte(internal.GenerateUID()),
		Destination: partitions,
	}
}

func GenerateRequest(key, value []byte, partitions []internal.Partition) internal.Request {
	return internal.Request{
		Key:         key,
		Value:       value,
		Extra:       nil,
		Destination: partitions,
	}
}
