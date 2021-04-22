package util

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
)

func GenerateRequest(value []byte, partitions []types.Partition) types.Request {
	return types.Request{
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
