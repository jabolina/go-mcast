package test

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

func GenerateDataArray(size int) []string {
	data := make([]string, size)
	for i := 0; i < size; i++ {
		data[i] = internal.GenerateUID()
	}
	return data
}

var Alphabet = []string{
	"A",
	"B",
	"C",
	"D",
	"E",
	"F",
	"G",
	"H",
	"I",
	"J",
	"K",
	"L",
	"M",
	"N",
	"O",
	"P",
	"Q",
	"R",
	"S",
	"T",
	"U",
	"V",
	"W",
	"X",
	"Y",
	"Z",
}
