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

	truth := filterOutput(output.Data, filter)
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

func filterOutput(data []types.DataHolder, filter func(data types.DataHolder) bool) []types.DataHolder {
	var res []types.DataHolder
	for _, holder := range data {
		if filter(holder) {
			res = append(res, holder)
		}
	}
	return res
}

func onlyOrdered(expected []types.DataHolder) []types.DataHolder {
	return filterOutput(expected, func(data types.DataHolder) bool {
		return data.Extensions == nil
	})
}

func outputValues(values []types.DataHolder, owner string) {
	log.Infof("--------------------%s-------------------------", owner)
	for _, value := range values {
		log.Infof("%s - %d - %v\n", value.Meta.Identifier, value.Meta.Timestamp, value.Extensions != nil)
	}
}
