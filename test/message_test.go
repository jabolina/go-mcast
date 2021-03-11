package test

import (
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"testing"
)

func Test_ShouldVerifyPriority(t *testing.T) {
	first := types.Message{
		Identifier: "a",
		Timestamp:  uint64(1),
	}
	second := types.Message{
		Identifier: "b",
		Timestamp:  uint64(1),
	}

	if !first.HasHigherPriority(second) {
		t.Fatalf("first message should have higher priority")
	}
}

func Test_MessagesShouldBeDifferent(t *testing.T) {
	first := types.Message{
		Identifier: "a",
		Timestamp:  uint64(1),
	}
	second := types.Message{
		Identifier: "b",
		Timestamp:  uint64(1),
	}

	if !first.Diff(second) {
		t.Fatalf("messages should be different")
	}

	firstClone := first
	firstClone.Timestamp = uint64(2)
	if first.Diff(firstClone) {
		t.Fatalf("messages should be different")
	}

	secondClone := second
	secondClone.State = types.S2
	if !second.Diff(secondClone) {
		t.Fatalf("messages should be different")
	}
}
