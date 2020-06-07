package amcast

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"go-mcast/internal/remote"
	"go-mcast/pkg/mcast"
	"sync"
)

var (
	// Err is returned when an RPC arrives in a version that the current
	// unity cannot handle.
	ErrUnsupportedProtocol = errors.New("protocol version not supported")
)

// Holds information for shutting down the whole group.
type poweroff struct {
	shutdown bool
	ch       chan struct{}
	mutex    sync.Mutex
}

// Unity is a group
type Unity struct {
	// Local peer id
	id mcast.ServerID

	// Hold information for the group, the group acts like a unity.
	// The unity *must* have a majority of non-faulty members.
	state GroupState

	// PreviousSet is the protocol needed for conflict evaluation.
	previousSet PreviousSet

	// Holds configuration about the unity. About the local group name,
	// logger utilities, protocol version, etc.
	configuration mcast.Config

	// The unity state machine to commit values.
	sm StateMachine

	// The global clock that can be used to synchronize groups.
	globalClock mcast.LogicalGlobalClock

	// Store the local unity address.
	address mcast.ServerAddress

	// Provided from the transport
	channel <-chan mcast.RPC

	// Transport layer for communication.
	trans mcast.Transport

	// Storage for storing information about the state machine.
	storage mcast.Storage

	// Shutdown channel to exit, protected to prevent concurrent exits.
	off poweroff
}

// Creates an RPC header to be sent across RPC requests.
func (u *Unity) getRPCHeader() mcast.RPCHeader {
	return mcast.RPCHeader{
		ProtocolVersion: u.configuration.Version,
	}
}

// Verify if the current version can handle the current RPC request.
func (u *Unity) checkRPCHeader(rpc mcast.RPC) error {
	h, ok := rpc.Command.(mcast.WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC doest not have a header")
	}

	header := h.GetRPCHeader()
	if header.ProtocolVersion > mcast.LatestProtocolVersion {
		return ErrUnsupportedProtocol
	}

	if header.ProtocolVersion != u.configuration.Version {
		return ErrUnsupportedProtocol
	}

	return nil
}

func (u *Unity) run() {
	for {
		select {
		case <-u.off.ch:
			// handle poweroff
			return
		default:
		}
	}
}

func (u *Unity) poll() {
	// Handle clean up when the node gives up and shutdown
	defer func() {
		u.configuration.Logger.Info("shutdown process", "id", u.id)
	}()

	for !u.off.shutdown {
		select {
		case rpc := <-u.channel:
			u.process(rpc)
		case <-u.off.ch:
			return
		}
	}
}

func (u *Unity) process(rpc mcast.RPC) {
	// Verify if the current unity is able to process
	// the rpc that just arrives.
	if err := u.checkRPCHeader(rpc); err != nil {
		return
	}

	switch cmd := rpc.Command.(type) {
	case *mcast.GMCastRequest:
		u.processGMCast(rpc, cmd)
	default:
		u.configuration.Logger.Error("unexpected command", "command", hclog.Fmt("%#v", rpc.Command))
	}
}

// Will process the received GM-Cast rpc request.
func (u *Unity) processGMCast(rpc mcast.RPC, r *mcast.GMCastRequest) {
	res := &mcast.GMCastResponse{
		RPCHeader:      u.getRPCHeader(),
		SequenceNumber: u.state.Clk.Tock(),
		Success:        false,
	}

	var rpcErr error
	defer func() {
		rpc.Respond(res, rpcErr)
	}()

	computeChannel := u.handleGMCast(r, r.Body)
	quorum := u.unityQuorum()
	votes := 0

	computedTimestamps := make([]uint64, 0)

	select {
	case newRpc := <-u.channel:
		u.process(newRpc)

	case computed := <-computeChannel:
		votes++

		switch computed.State {
		case remote.S1:
		case remote.S2:
			// There is more than one process group on the destination, need to execute
			// a gather request to exchange the timestamp between groups.
			computedTimestamps = append(computedTimestamps, computed.Timestamp)
			if votes >= quorum {
				tsm := max(computedTimestamps)
				gatherReq := &mcast.GatherRequest{
					RPCHeader: u.getRPCHeader(),
					UID:       r.UID,
					State:     computed.State,
					Timestamp: tsm,
				}
				u.configuration.Logger.Info("gathering now", "req", gatherReq)
				// send the gather request and return.
			}
		case remote.S3:
			// Ready to be committed into the state machine.
		default:
			u.configuration.Logger.Error("unknown compute response state", "computed", computed)
		}
	}

}

// Will handle the received GM-cast issuing the needed RPC requests.
// Each process that receives the message m, set the state of m to S0,
// this indicates that m don't have a timestamp associated yet. Then the
// message is broadcast for the processes groups.
func (u *Unity) handleGMCast(r *mcast.GMCastRequest, message remote.Message) <-chan *mcast.ComputeResponse {
	channel := make(chan *mcast.ComputeResponse, len(u.state.Nodes))
	req := &mcast.ComputeRequest{
		RPCHeader:   u.getRPCHeader(),
		UID:         r.UID,
		State:       message.MessageState,
		Destination: r.Destination,
	}

	ask := func(peer mcast.Server) {
		u.state.emit(func() {
			res := &mcast.ComputeResponse{}
			err := u.trans.Compute(peer.ID, peer.Address, req, res)
			if err != nil {
				u.configuration.Logger.Error("failed on compute RPC", "target", peer, "error", err)
				res.State = remote.S0
			}
			channel <- res
		})
	}

	for _, node := range u.state.Nodes {
		if node.Server.ID == u.id {
			// handle computation locally
		} else {
			ask(node.Server)
		}
	}

	return channel
}

// Process Compute requests, after the messages is broadcast to the process groups.
// First is verified if the message m conflicts with any other message on the unity
// previous set, if so, the process p increments the local clock and clear the previous set.
// Then m receives a timestamp and is added to the previous set, for future verifications.
// If m has only one destination, the message can jump to state S3, since there is no need
// to exchange the timestamp message group, that is already decided.
//
// Otherwise, for messages on state S0 the timestamp received is the local group clock,
// updates the state to S1 and exchange the message with all processes. If the message is
// already on state S2, it already have the final timestamp and just needs to update m to
// the final timestamp, update the group clock and clear the previous set.
func (u *Unity) processCompute(rpc mcast.RPC, r *mcast.ComputeRequest) {
	res := &mcast.ComputeResponse{
		RPCHeader: u.getRPCHeader(),
		UID:       r.UID,
	}
	var rpcError error

	defer func() {
		rpc.Respond(res, rpcError)
	}()

	if r.State == remote.S0 {
		if u.previousSet.Conflicts(r.Destination) {
			u.state.Clk.Tick()
			u.previousSet.Clear()
		}
		r.Timestamp = u.state.Clk.Tock()
		u.previousSet.Add(r.Destination, r.UID)
	}

	if len(r.Destination) > 1 {
		// On state S0, the message is receiving its first timestamp definition
		// the local group clock will define the message timestamp that will be
		// answered back, all answers will be grouped and the next step can be executed.
		if r.State == remote.S0 {
			res.State = remote.S1
			res.Timestamp = u.state.Clk.Tock()
		} else if r.State == remote.S2 {
			res.State = remote.S3
			if r.Timestamp > u.state.Clk.Tock() {
				u.state.Clk.Defines(r.Timestamp)
				u.previousSet.Clear()
			}
			res.Timestamp = u.state.Clk.Tock()
		}
	} else {
		res.Timestamp = u.state.Clk.Tock()
		res.State = remote.S3
	}
}

// Process a Gather request.
// When a message m has more than on destination group, the destination groups have
// to exchange its timestamps to decide the final timestamp on m. Thus, after
// receiving all other timestamp values, a temporary variable tsm is agreed upon the
// maximum timestamp value received. Once the algorithm have selected the tsm value,
// the process checks if the global consensus timestamp is greater or equal to tsm.
//
// The computed tsm will be already coupled into the GatherRequest.
func (u *Unity) processGather(rpc mcast.RPC, r *mcast.GatherRequest) {
	res := &mcast.GatherResponse{
		RPCHeader: u.getRPCHeader(),
		UID:       r.UID,
	}
	defer rpc.Respond(res, nil)

	if r.Timestamp >= u.state.Clk.Tock() {
		res.State = remote.S3
	} else {
		res.Timestamp = u.state.Clk.Tock()
		res.State = remote.S2
	}
}

// How many local nodes must reply to quorum is obtained.
func (u *Unity) unityQuorum() int {
	return len(u.state.Nodes)/2 + 1
}

func max(values []uint64) uint64 {
	var v uint64
	for _, e := range values {
		if e > v {
			v = e
		}
	}
	return v
}
