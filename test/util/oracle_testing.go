package util

import (
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"strings"
)

const (
	PartitionSeparator = "-|-"
	baseAddress        = "127.0.0.1"
)

type OracleTesting struct{}

func (o *OracleTesting) ResolveByName(name types.PeerName) *types.TCPAddress {
	return nil
}

func (o *OracleTesting) ResolveByPartition(partition types.Partition) []types.TCPAddress {
	var addresses []types.TCPAddress
	stringified := string(partition)
	splitted := strings.Split(stringified, PartitionSeparator)
	if len(splitted) < 2 {
		return addresses
	}
	ports := splitted[1]
	for _, port := range strings.Split(ports, ".") {
		if len(port) == 0 {
			continue
		}

		address := fmt.Sprintf("%s:%s", baseAddress, port)
		addresses = append(addresses, types.TCPAddress(address))
	}
	return addresses
}
