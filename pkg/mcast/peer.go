package mcast

import (
	"errors"
	"time"
)

var (
	// Err is returned when an RPC arrives in a version that the current
	// peer cannot handle.
	ErrUnsupportedProtocol = errors.New("protocol version not supported")
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
	}
	return peer
}

func (p *Peer) Poll() {
	p.log.Debugf("started polling for %s", p.Id)
	// Handle clean up when the node gives up and shutdown
	defer func() {
		p.log.Debugf("shutdown process %s", p.Id)
	}()

	for !p.shutdown {
		select {
		case rpc := <-p.channel:
			p.log.Debugf("received rpc %#v", rpc)
			p.process(rpc)
		case <-p.unity.off.ch:
			p.shutdown = true
			return
		}
	}
}

// Process the current received RPC.
func (p *Peer) process(rpc RPC) {
	// Add or update the received RPC into the processing messages.

	p.unity.deliver.Add(rpc)
	// Verify if the current peer is able to process
	// the rpc that just arrives.
	if err := p.unity.checkRPCHeader(rpc); err != nil {
		p.log.Warnf("received version not handled at %s. %v", p.Id, err)
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
	}
}

// Will process the received GM-Cast rpc request.
func (p *Peer) processGMCast(rpc RPC, r *GMCastRequest) {
	res := &GMCastResponse{
		RPCHeader:      p.unity.GetRPCHeader(),
		SequenceNumber: p.State.Clk.Tock(),
		Success:        false,
	}

	var rpcErr error
	defer func() {
		p.unity.deliver.Deliver(r.Body.Data, r.Body.Extensions, r.UID, res)
		p.log.Debugf("sending response back %#v", res)
		rpc.Respond(res, rpcErr)
	}()

	quorum := (len(p.State.Nodes) / 2) + 1
	votes := 0
	done := false

	computedTimestamps := make([]uint64, 0)
	computeChannel := p.handleGMCast(r, r.Body)

	for {
		select {
		case newRpc := <-p.channel:
			p.process(newRpc)

		case computed := <-computeChannel:
			p.log.Debugf("received compute response from channel: %#v", computed)
			votes++
			computedTimestamps = append(computedTimestamps, computed.Timestamp)
			if votes >= quorum && !done {
				done = true
				switch computed.State {
				case S1:
				case S2:
					// There is more than one process group on the destination, need to execute
					// a gather request to exchange the timestamp between groups.
					if votes >= quorum {
						tsm := max(computedTimestamps)
						gatherReq := &GatherRequest{
							RPCHeader: p.unity.GetRPCHeader(),
							UID:       r.UID,
							State:     computed.State,
							Timestamp: tsm,
						}
						sequenceNumber := p.emitGather(gatherReq)
						p.log.Debugf("sequence number is %ld", sequenceNumber)
					}
				case S3:
					p.log.Debugf("delivering response %#v for req %#v", res, r)
					res.Success = true
					res.SequenceNumber = max(computedTimestamps)
					// Ready to be committed into the state machine.
					return
				default:
					p.log.Errorf("unknown compute response state %#v", computed)
					res.Success = false
					return
				}
			}
		case <-time.After(p.unity.timeout):
			p.log.Errorf("timeout handling request %#v", r)
			res.Success = false
			return
		}
	}

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
	}

	ask := func(peer Server) {
		p.State.emit(func() {
			res := &ComputeResponse{}
			err := p.Trans.Compute(peer.ID, peer.Address, req, res)
			if err != nil {
				p.log.Errorf("failed on compute RPC to target %#v. error %v", peer, err)
				res.State = S0
			}
			p.log.Debugf("sending compute response: %#v", res)
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

func (p *Peer) emitGather(req *GatherRequest) uint64 {
	channel := make(chan *GatherResponse, len(req.Destination))
	emit := func(peer Server) {
		p.State.emit(func() {
			res := new(GatherResponse)
			err := p.Trans.Gather(peer.ID, peer.Address, req, res)
			if err != nil {
				p.log.Errorf("failed to gather to target %v. error %v", peer, err)
			}
			channel <- res
		})
	}

	received := 0
	var tsm uint64
	select {
	case res := <-channel:
		received++
		if received == len(req.Destination) {
			tsm = res.Timestamp
		}
	case <-time.After(p.unity.timeout):
		// fixme: returns error on timeout
		panic("timeout gathering")
	}

	for _, destination := range req.Destination {
		emit(destination)
	}

	return tsm
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

// Process a Gather request.
// When a message m has more than on destination group, the destination groups have
// to exchange its timestamps to decide the final timestamp on m. Thus, after
// receiving all other timestamp values, a temporary variable tsm is agreed upon the
// maximum timestamp value received. Once the algorithm have selected the tsm value,
// the process checks if the global consensus timestamp is greater or equal to tsm.
//
// The computed tsm will be already coupled into the GatherRequest.
func (p *Peer) processGather(rpc RPC, r *GatherRequest) {
	res := &GatherResponse{
		RPCHeader: p.unity.GetRPCHeader(),
		UID:       r.UID,
	}
	defer rpc.Respond(res, nil)

	if r.Timestamp >= p.unity.GlobalClock().Tock() {
		res.State = S3
	} else {
		res.Timestamp = p.unity.GlobalClock().Tock()
		res.State = S2
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
