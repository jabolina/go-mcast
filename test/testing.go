package test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/prometheus/common/log"
	"runtime"
	"sync"
	"testing"
	"time"
)

type TestInvoker struct {
	group *sync.WaitGroup
}

func (t *TestInvoker) Spawn(f func()) {
	t.group.Add(1)
	go func() {
		defer t.group.Done()
		f()
	}()
}

func (t *TestInvoker) Stop() {
	t.group.Wait()
}
func NewInvoker() core.Invoker {
	return &TestInvoker{
		group: &sync.WaitGroup{},
	}
}

type UnityCluster struct {
	T       *testing.T
	Names   []types.Partition
	Unities []mcast.Unity
	mutex   *sync.Mutex
	group   *sync.WaitGroup
	index   int
}

func (c *UnityCluster) Off() {
	for _, unity := range c.Unities {
		c.group.Add(1)
		go c.PoweroffUnity(unity)
	}

	c.group.Wait()
}

func NewTestingUnity(configuration *types.Configuration) (mcast.Unity, error) {
	invk := NewInvoker()
	var peers []core.PartitionPeer
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < configuration.Replication; i++ {
		pc := &types.PeerConfiguration{
			Name:      fmt.Sprintf("%s-%d", configuration.Name, i),
			Partition: configuration.Name,
			Version:   configuration.Version,
			Conflict:  configuration.Conflict,
			Storage:   configuration.Storage,
			Ctx:       ctx,
			Cancel:    cancel,
		}
		peer, err := core.NewPeer(pc, configuration.Logger)
		if err != nil {
			cancel()
			for _, prevCreated := range peers {
				prevCreated.Stop()
			}
			return nil, err
		}

		peers = append(peers, peer)
	}
	pu := &mcast.PeerUnity{
		Configuration: configuration,
		Peers:         peers,
		Last:          0,
		Invoker:       invk,
		Finish:        cancel,
	}
	return pu, nil
}

func CreateUnityConflict(name types.Partition, conflict types.ConflictRelationship, t *testing.T) mcast.Unity {
	conf := mcast.DefaultConfiguration(name)
	conf.Logger.ToggleDebug(false)
	conf.Logger.AddContext(string(name))
	conf.Conflict = conflict
	unity, err := NewTestingUnity(conf)
	if err != nil {
		t.Fatalf("failed creating unity %s. %v", name, err)
	}
	return unity
}

func CreateUnity(name types.Partition, t *testing.T) mcast.Unity {
	return CreateUnityConflict(name, definition.AlwaysConflict{}, t)
}

func CreateClusterConflict(clusterSize int, prefix string, conflict types.ConflictRelationship, t *testing.T) *UnityCluster {
	cluster := &UnityCluster{
		T:     t,
		group: &sync.WaitGroup{},
		mutex: &sync.Mutex{},
		Names: make([]types.Partition, clusterSize),
	}
	var unities []mcast.Unity
	for i := 0; i < clusterSize; i++ {
		name := types.Partition(fmt.Sprintf("%s-%s", prefix, helper.GenerateUID()))
		cluster.Names[i] = name
		unities = append(unities, CreateUnityConflict(name, conflict, t))
	}
	cluster.Unities = unities
	return cluster
}

func CreateCluster(clusterSize int, prefix string, t *testing.T) *UnityCluster {
	return CreateClusterConflict(clusterSize, prefix, definition.AlwaysConflict{}, t)
}

func (c *UnityCluster) Next() mcast.Unity {
	c.mutex.Lock()
	defer func() {
		c.index += 1
		c.mutex.Unlock()
	}()

	if c.index >= len(c.Unities) {
		c.index = 0
	}

	return c.Unities[c.index]
}

func DoWeMatch(expected []types.DataHolder, unities []mcast.Unity, t *testing.T) {
	for _, unity := range unities {
		res := unity.Read()

		if !res.Success {
			t.Errorf("reading partition failed. %v", res.Failure)
			continue
		}
		outputValues(res.Data, string(unity.WhoAmI()))

		toVerify := onlyOrdered(res.Data)
		for index, holder := range toVerify {
			expectedData := expected[index]
			if !bytes.Equal(expectedData.Content, holder.Content) {
				t.Errorf("Content differ cmd %d for unity %s, expected %#v, found %#v", index, unity.WhoAmI(), expectedData, holder)
				continue
			}
		}

		if len(res.Data) == len(toVerify) {
			if len(res.Data) != len(expected) {
				t.Errorf("C-Hist differ on size, expected %d found %d", len(expected), len(res.Data))
			}
		} else {
			t.Logf("had generic message @ %s", unity.WhoAmI())
		}
	}
}

func (c UnityCluster) DoesClusterMatchTo(expected []types.DataHolder) {
	DoWeMatch(expected, c.Unities, c.T)
}

func onlyOrdered(expected []types.DataHolder) []types.DataHolder {
	var notGeneric []types.DataHolder
	for _, holder := range expected {
		if holder.Extensions == nil {
			notGeneric = append(notGeneric, holder)
		}
	}
	return notGeneric
}

func outputValues(values []types.DataHolder, owner string) {
	log.Infof("--------------------%s-------------------------", owner)
	for _, value := range values {
		log.Infof("%s - %d - %v\n", value.Meta.Identifier, value.Meta.Timestamp, value.Extensions != nil)
	}
}

func (c UnityCluster) DoesAllClusterMatch() {
	first := c.Unities[0]
	res := first.Read()

	if !res.Success {
		c.T.Errorf("something wrong readin. %v", res.Failure)
		return
	}
	c.DoesClusterMatchTo(onlyOrdered(res.Data))
}

func (c *UnityCluster) PoweroffUnity(unity mcast.Unity) {
	defer c.group.Done()
	unity.Shutdown()
}

func PrintStackTrace(t *testing.T) {
	buf := make([]byte, 1<<16)
	runtime.Stack(buf, true)
	t.Errorf("%s", buf)
}

func WaitThisOrTimeout(cb func(), duration time.Duration) bool {
	done := make(chan bool)
	go func() {
		cb()
		done <- true
	}()
	select {
	case <-done:
		return true
	case <-time.After(duration):
		return false
	}
}
