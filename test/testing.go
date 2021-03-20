package test

import (
	"bytes"
	"fmt"
	"github.com/jabolina/go-mcast/pkg/mcast"
	"github.com/jabolina/go-mcast/pkg/mcast/core"
	"github.com/jabolina/go-mcast/pkg/mcast/definition"
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"github.com/prometheus/common/log"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

const DefaultTestTimeout = 5 * time.Second

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
	Unities []Unity
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

func CreateUnityConflict(name types.Partition, ports []int, conflict types.ConflictRelationship, t *testing.T) Unity {
	_, isCi := os.LookupEnv("CI_ENV")
	conf := mcast.DefaultConfiguration(name)
	conf.Logger.ToggleDebug(!isCi)
	conf.Logger.AddContext(string(name))
	conf.Conflict = conflict
	conf.Oracle = &OracleTesting{}
	if isCi {
		conf.Logger.Infof("CI environment. Timeout is 20 seconds!")
		conf.DefaultTimeout = 20 * time.Second
	}
	unity, err := NewUnity(conf, ports)
	if err != nil {
		t.Fatalf("failed creating unity %s. %v", name, err)
	}
	return unity
}

func CreateUnity(name types.Partition, ports []int, t *testing.T) Unity {
	return CreateUnityConflict(name, ports, definition.AlwaysConflict{}, t)
}

func CreateClusterConflict(prefix string, conflict types.ConflictRelationship, ports [][]int, t *testing.T) *UnityCluster {
	cluster := &UnityCluster{
		T:     t,
		group: &sync.WaitGroup{},
		mutex: &sync.Mutex{},
		Names: make([]types.Partition, len(ports)),
	}
	var unities []Unity
	for i, partitionPorts := range ports {
		name := ProperPartitionName(prefix, partitionPorts)
		cluster.Names[i] = name
		unities = append(unities, CreateUnityConflict(name, partitionPorts, conflict, t))
	}
	cluster.Unities = unities
	return cluster
}

func ProperPartitionName(prefix string, ports []int) types.Partition {
	addresses := ""
	for _, port := range ports {
		addresses = fmt.Sprintf("%s.%d", addresses, port)
	}
	baseName := types.Partition(fmt.Sprintf("%s-%s", prefix, helper.GenerateUID()))
	return types.Partition(fmt.Sprintf("%s%s%s", baseName, PartitionSeparator, addresses))
}

func CreateCluster(prefix string, ports [][]int, t *testing.T) *UnityCluster {
	return CreateClusterConflict(prefix, definition.AlwaysConflict{}, ports, t)
}

func (c *UnityCluster) Next() Unity {
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

func DoWeMatch(expected []types.DataHolder, unities []Unity, t *testing.T) {
	for _, unity := range unities {
		res := unity.Read()

		if !res.Success {
			t.Errorf("reading partition failed. %v", res.Failure)
			continue
		}
		outputValues(res.Data, string(unity.WhoAmI()))

		toVerify := onlyOrdered(res.Data)
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

func (c *UnityCluster) PoweroffUnity(unity Unity) {
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
