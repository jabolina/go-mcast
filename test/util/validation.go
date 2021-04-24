package util

import (
	"bytes"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/prometheus/common/log"
	"testing"
)

func CompareOutputs(t *testing.T, unities []Unity, filter func(data types.DataHolder) bool) {
	if len(unities) == 0 {
		return
	}

	first := unities[0]
	output := first.Read()
	if !output.Success {
		t.Errorf("failed reading output. %#v", output.Failure)
		return
	}

	truth := FilterOutput(output.Data, filter)
	filterEntries := func(data []types.DataHolder) []types.DataHolder {
		var res []types.DataHolder
		for _, holder := range data {
			if filter(holder) {
				res = append(res, holder)
			}
		}
		return res
	}
	DoWeMatch(truth, unities, filterEntries, t)
}

func AssertOutputContainsAll(t *testing.T, contained [][]byte, unity Unity, filter func(data types.DataHolder) bool) {
	output := unity.Read()
	if !output.Success {
		t.Errorf("failed reading. %#v", output.Failure)
		return
	}

	filtered := FilterOutput(output.Data, filter)
	if len(filtered) != len(contained) {
		t.Errorf("should have size %d but have %d", len(contained), len(filtered))
		return
	}

	matchCount := 0
	for _, holder := range filtered {
		if !ContainsElement(holder.Content, contained) {
			t.Errorf("element %s should not exists", string(holder.Content))
			continue
		}
		matchCount++
	}

	if matchCount != len(contained) {
		t.Errorf("should have %d items, have only %d", len(contained), matchCount)
	}
}

func DoWeMatch(expected []types.DataHolder, unities []Unity, filter func([]types.DataHolder) []types.DataHolder, t *testing.T) {
	for _, unity := range unities {
		res := unity.Read()

		if !res.Success {
			t.Errorf("reading partition failed. %v", res.Failure)
			continue
		}
		outputValues(res.Data, string(unity.WhoAmI()))

		toVerify := filter(res.Data)
		for i, holder := range expected {
			if len(toVerify)-1 < i {
				t.Errorf("Content differ cmd %d for unity %s, expected %#v, found nothing", i, unity.WhoAmI(), holder)
				continue
			}

			actual := toVerify[i]
			if !bytes.Equal(holder.Content, actual.Content) {
				t.Errorf("Content differ cmd %d for unity %s, expected %#v, found %#v", i, unity.WhoAmI(), holder, actual)
			}
		}
	}
}

func (c UnityCluster) DoesClusterMatchTo(expected []types.DataHolder) {
	DoWeMatch(expected, c.Unities, onlyOrdered, c.T)
}

func ContainsElement(e []byte, slice [][]byte) bool {
	for _, item := range slice {
		if bytes.Equal(e, item) {
			return true
		}
	}
	return false
}

func FilterOutput(data []types.DataHolder, filter func(data types.DataHolder) bool) []types.DataHolder {
	var res []types.DataHolder
	for _, holder := range data {
		if filter(holder) {
			res = append(res, holder)
		}
	}
	return res
}

func onlyOrdered(expected []types.DataHolder) []types.DataHolder {
	return FilterOutput(expected, func(data types.DataHolder) bool {
		return data.Extensions == nil
	})
}

func outputValues(values []types.DataHolder, owner string) {
	log.Infof("--------------------%s-------------------------", owner)
	for _, value := range values {
		log.Infof("%s - %d - %v\n", value.Meta.Identifier, value.Meta.Timestamp, value.Extensions != nil)
	}
}
