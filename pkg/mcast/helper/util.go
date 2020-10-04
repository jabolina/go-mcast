package helper

import (
	crand "crypto/rand"
	"fmt"
)

// Generates a random 128-bit UUID, panic if not possible.
func GenerateUID() string {
	buf := make([]byte, 16)
	if _, err := crand.Read(buf); err != nil {
		panic(fmt.Errorf("failed generating uid: %v", err))
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// Return the greatest value in a uint64 slice.
func MaxValue(values []uint64) uint64 {
	var value uint64
	for _, v := range values {
		if v > value {
			value = v
		}
	}
	return value
}
