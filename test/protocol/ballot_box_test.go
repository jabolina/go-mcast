package protocol

import (
	"github.com/jabolina/go-mcast/pkg/mcast/helper"
	"github.com/jabolina/go-mcast/pkg/mcast/protocol"
	"github.com/jabolina/go-mcast/pkg/mcast/types"
	"sync"
	"testing"
)

func Test_RegisterConcurrentVotes(t *testing.T) {
	electionId := types.UID(helper.GenerateUID())
	voterSize := 500
	wg := sync.WaitGroup{}
	ballotBox := protocol.NewBallotBox()

	castVote := func() {
		defer wg.Done()
		ballotBox.Insert(electionId, types.Partition(helper.GenerateUID()), 1)
	}

	wg.Add(voterSize)
	for i := 0; i < voterSize; i++ {
		go castVote()
	}

	wg.Wait()
	votes := ballotBox.Read(electionId)

	if len(votes) != voterSize {
		t.Errorf("Expected %d votes, found %d", voterSize, len(votes))
	}

	electionSize := ballotBox.ElectionSize(electionId)

	if electionSize != voterSize {
		t.Errorf("Expected %d election size, found %d", voterSize, electionSize)
	}

	if len(votes) != electionSize {
		t.Errorf("Expected to be same size as election %d, found %d", electionSize, len(votes))
	}
}

func Test_RegisterDuplicatedVotes(t *testing.T) {
	electionId := types.UID(helper.GenerateUID())
	ballotBox := protocol.NewBallotBox()

	voteSize := 4
	expectedElectionSize := 2

	firstVoter := types.Partition(helper.GenerateUID())
	secondVoter := types.Partition(helper.GenerateUID())

	ballotBox.Insert(electionId, firstVoter, 0)
	ballotBox.Insert(electionId, firstVoter, 1)
	ballotBox.Insert(electionId, secondVoter, 2)
	ballotBox.Insert(electionId, secondVoter, 3)

	votes := ballotBox.Read(electionId)

	if len(votes) != voteSize {
		t.Errorf("Expected %d votes, found %d", voteSize, len(votes))
	}

	electionSize := ballotBox.ElectionSize(electionId)

	if electionSize != expectedElectionSize {
		t.Errorf("Expected %d election size, found %d", expectedElectionSize, electionSize)
	}

	for i, vote := range votes {
		if uint64(i) != vote {
			t.Errorf("Expected vote %d found %d", i, vote)
		}
	}
}
