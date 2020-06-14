package mcast

import (
	"errors"
	"fmt"
	"time"
)

var (
	// Err is returned when an RPC arrives in a version that the current
	// peer cannot handle.
	ErrUnsupportedProtocol = errors.New("protocol version not supported")

	// Used when the configured timeout occurs while processing are request.
	ErrTimeoutProcessing = errors.New("timeout while processing request")
)

type Peer struct {
	// Peer identifier
	Id ServerID

	// Peer address.
	Address ServerAddress

	// Peer has started.
	started bool

	// Unity which the peer belongs to.
	unity *Unity

	// Peer logger from the unity.
	log Logger

	// Hold information for the group, the group acts like a unity.
	// The unity *must* have a majority of non-faulty members.
	State *GroupState

	// Provided from the peer transport.
	channel <-chan RPC

	// Peer transport layer for communication.
	Trans Transport

	// When the unity shutdown, each peer will also be shutdown.
	shutdown bool

	// Channel for shutdown notification.
	close chan bool
}

func NewPeer(server Server, trans Transport, group *GroupState, unity *Unity) *Peer {
	peer := &Peer{
		Id:      server.ID,
		Address: server.Address,
		started: false,
		State:   group,
		unity:   unity,
		log:     unity.configuration.Logger,
		channel: trans.Consumer(),
		Trans:   trans,
		close:   make(chan bool),
	}
	return peer
}

// Starts the peer and keep polling forever until shutdown.
func (p *Peer) Poll() {
	p.log.Debugf("started polling for %s", p.Id)
	// Handle clean up when the node gives up and shutdown
	defer func() {
		p.log.Debugf("shutdown process %s", p.Id)
		p.unity.off.ch <- true
	}()

	for !p.shutdown {
		select {
		case rpc := <-p.channel:
			p.process(rpc)
		case <-p.close:
			p.shutdown = true
			p.Trans.Close()
			return
		}
	}
}

// Process the current received RPC.
func (p *Peer) process(rpc RPC) {
	p.log.Debugf("received rpc %#v", rpc)

	// Verify if the current peer is able to process
	// the rpc that just arrives.
	if err := p.unity.checkRPCHeader(rpc); err != nil {
		p.log.Warnf("received version not handled at %s. %v", p.Id, err)
		rpc.Respond(nil, ErrUnsupportedProtocol)
		return
	}

	switch cmd := rpc.Command.(type) {
	case *GMCastRequest:
		p.processGMCast(rpc, cmd)
	case *ComputeRequest:
		p.processCompute(rpc, cmd)
	case *GatherRequest:
		p.processGather(rpc, cmd)
	default:
		p.log.Errorf("unexpected command: %#v", rpc.Command)
		rpc.Respond(nil, fmt.Errorf("unexpected command: %#v", rpc.Command))
	}
}

// Will process the received GM-Cast rpc request.
func (p *Peer) processGMCast(rpc RPC, r *GMCastRequest) {
	p.unity.deliver.Add(r)
	res := &GMCastResponse{
		RPCHeader:      p.unity.GetRPCHeader(),
		SequenceNumber: p.State.Clk.Tock(),
		Success:        false,
	}

	var rpcErr error
	defer func() {
		select {
		case v := <-p.unity.deliver.Deliver(r, res):
			p.log.Debugf("sending response back %#v", v)
			if rpcErr == nil {
				rpcErr = v.err
			}
			rpc.Respond(v.process, rpcErr)
		case <-time.After(p.unity.timeout):
			rpc.Respond(nil, ErrTimeoutProcessing)
		}
	}()

	rpcErr = p.handleCompute(p.handleGMCast(r, r.Body), r, res)
}

// Will handle the received GM-cast issuing the needed RPC requests.
// Each process that receives the message m, set the state of m to S0,
// this indicates that m don't have a timestamp associated yet. Then the
// message is broadcast for the processes groups.
func (p *Peer) handleGMCast(r *GMCastRequest, message Message) <-chan *ComputeResponse {
	channel := make(chan *ComputeResponse, len(p.State.Nodes))
	req := &ComputeRequest{
		RPCHeader:   p.unity.GetRPCHeader(),
		UID:         r.UID,
		State:       message.MessageState,
		Destination: r.Destination,
		Timestamp:   message.Timestamp,
	}

	ask := func(peer Server) {
		p.State.emit(func() {
			res := &ComputeResponse{}
			err := p.Trans.Compute(peer.ID, peer.Address, req, res)
			if err != nil {
				p.log.Errorf("failed on compute RPC to target %#v. error %v", peer, err)
				res.State = S0
			}
			channel <- res
		})
	}

	for _, node := range p.State.Nodes {
		// Is possible to avoid this call if the target
		// node id is the same as the local id, and just process
		// the request locally. Is the best option?
		ask(Server{
			ID:      node.Id,
			Address: node.Address,
		})
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
func (p *Peer) processCompute(rpc RPC, r *ComputeRequest) {
	p.unity.deliver.Update(rpc)
	res := &ComputeResponse{
		RPCHeader: p.unity.GetRPCHeader(),
		UID:       r.UID,
	}
	var rpcError error

	defer func() {
		rpc.Respond(res, rpcError)
	}()

	if r.State == S0 {
		var addresses []ServerAddress
		for _, v := range r.Destination {
			addresses = append(addresses, v.Address)
		}
		if p.unity.previousSet.Conflicts(addresses) {
			p.State.Clk.Tick()
			p.unity.previousSet.Clear()
		}
		r.Timestamp = p.State.Clk.Tock()
		p.unity.previousSet.Add(addresses, r.UID)
	}

	if len(r.Destination) > 1 {
		// On state S0, the message is receiving its first timestamp definition
		// the local group clock will define the message timestamp that will be
		// answered back, all answers will be grouped and the next step can be executed.
		if r.State == S0 {
			res.State = S1
			res.Timestamp = p.State.Clk.Tock()
		} else if r.State == S2 {
			res.State = S3
			if r.Timestamp > p.State.Clk.Tock() {
				p.State.Clk.Leap(r.Timestamp)
				p.unity.previousSet.Clear()
			}
			res.Timestamp = p.State.Clk.Tock()
		}
	} else {
		res.Timestamp = p.State.Clk.Tock()
		res.State = S3
	}
}

func (p *Peer) handleCompute(channel <-chan *ComputeResponse, req *GMCastRequest, res *GMCastResponse) error {
	// The protocol requires that a majority of peers
	// must be non-faulty. So is needed only a majority
	// of responses before proceeding.
	quorum := (len(p.State.Nodes) / 2) + 1
	done := false
	computedTimestamps := make([]uint64, 0)

	for {
		select {
		// A new RPC was received for processing.
		case newRpc := <-p.channel:
			p.process(newRpc)

		// Received a compute response back.
		case computed := <-channel:
			computedTimestamps = append(computedTimestamps, computed.Timestamp)
			// If received a majority of response back from the unity peers.
			// This is only calls that occur inside the unity.
			// Each unity could zone aware and be deployed on different
			// zones across the data center.
			if len(computedTimestamps) >= quorum && !done {
				done = true
				switch computed.State {
				case S1, S2:
					// There is more than one process group on the destination, need to execute
					// a gather request to exchange the timestamp between groups.
					tsm := max(computedTimestamps)
					gatherReq := &GatherRequest{
						RPCHeader:   p.unity.GetRPCHeader(),
						UID:         req.UID,
						State:       computed.State,
						Timestamp:   tsm,
						Destination: req.Destination,
					}
					sequenceNumber, err := p.handleGather(gatherReq, req, res)
					if err != nil {
						return err
					}
					res.Success = true
					res.SequenceNumber = sequenceNumber
					return nil
				case S3:
					res.Success = true
					res.SequenceNumber = max(computedTimestamps)
					return nil
				default:
					p.log.Errorf("unknown compute response state %#v", computed)
					res.Success = false
					return fmt.Errorf("unknown compute response state %#v", computed)
				}
			}
		// Timeout occurred before could process the request.
		case <-time.After(p.unity.timeout):
			p.log.Errorf("timeout handling request %#v", req)
			res.Success = false
			return ErrTimeoutProcessing
		}
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
func (p *Peer) processGather(rpc RPC, r *GatherRequest) {
	p.unity.deliver.Update(rpc)
	res := &GatherResponse{
		RPCHeader: p.unity.GetRPCHeader(),
		UID:       r.UID,
	}
	defer rpc.Respond(res, nil)

	if r.Timestamp >= p.unity.GlobalClock().Tock() {
		res.State = S3
		res.Timestamp = r.Timestamp
		p.unity.GlobalClock().Leap(r.Timestamp)
	} else {
		res.Timestamp = p.unity.GlobalClock().Tock()
		res.State = S2
	}
}

// Sends a gather request for peer on destination.
// This is used to exchange the global timestamp across
// all unities, after that the unities will be synchronized.
func (p *Peer) handleGather(req *GatherRequest, original *GMCastRequest, res *GMCastResponse) (uint64, error) {
	p.log.Debugf("sending gather req %#v", req)
	channel := make(chan *GatherResponse, len(req.Destination))
	emit := func(peer Server) {
		p.State.emit(func() {
			res := new(GatherResponse)
			err := p.Trans.Gather(peer.ID, peer.Address, req, res)
			if err != nil {
				p.log.Errorf("failed to gather to target %v. error %v", peer, err)
			}
			p.log.Debugf("recv gather response %#v", res)
			channel <- res
		})
	}

	for _, destination := range req.Destination {
		emit(destination)
	}

	var gathered []uint64
	for {
		select {
		// A new RPC was received for processing.
		case newRpc := <-p.channel:
			p.process(newRpc)

		case v := <-channel:
			gathered = append(gathered, v.Timestamp)
			if len(gathered) == len(req.Destination) {
				if v.State == S2 {
					// The global clock contains a higher value,
					// so the received timestamp must be exchanged
					// between processes within the unity.
					original.Body.MessageState = S2
					original.Body.Timestamp = req.Timestamp
					if err := p.handleCompute(p.handleGMCast(original, original.Body), original, res); err != nil {
						return 0, err
					}
					return res.SequenceNumber, nil
				} else {
					// The unity clock already contains a value
					// higher than the global clock and is ready
					// to be delivered.
					return max(gathered), nil
				}
			}
		case <-time.After(p.unity.timeout):
			return 0, ErrTimeoutProcessing
		}
	}
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
