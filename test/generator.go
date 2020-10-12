package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

func GenerateRandomRequest(partitions []types.Partition) types.Request {
	return types.Request{
		Key:         []byte(helper.GenerateUID()),
		Value:       []byte(helper.GenerateUID()),
		Extra:       []byte(helper.GenerateUID()),
		Destination: partitions,
	}
}

func GenerateRandomRequestValue(key []byte, partitions []types.Partition) types.Request {
	return types.Request{
		Key:         key,
		Value:       []byte(helper.GenerateUID()),
		Extra:       []byte(helper.GenerateUID()),
		Destination: partitions,
	}
}

func GenerateRequest(key, value []byte, partitions []types.Partition) types.Request {
	return types.Request{
		Key:         key,
		Value:       value,
		Extra:       nil,
		Destination: partitions,
	}
}

func GenerateDataArray(size int) []string {
	data := make([]string, size)
	for i := 0; i < size; i++ {
		data[i] = helper.GenerateUID()
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
