package raft

import (
	"sync"
	"testing"
	"time"
)

// makeCluster builds n interconnected servers, each with its own applyCh,
// and returns the servers plus their apply channels.
func makeCluster(n int) ([]*Raft, []chan ApplyMsg) {
	peers := make([]*Raft, n)
	chans := make([]chan ApplyMsg, n)

	for i := 0; i < n; i++ {
		chans[i] = make(chan ApplyMsg, 1000) // buffered so applier never blocks
	}
	for i := 0; i < n; i++ {
		peers[i] = &Raft{
			peers:         peers,
			me:            i,
			votedFor:      -1,
			role:          Follower,
			electionReset: time.Now(),
			log:           []LogEntry{{Term: 0}},
			applyCh:       chans[i],
			persister:     MakePersister(), // each server has its own durable store
		}
		peers[i].applyCond = sync.NewCond(&peers[i].mu)
	}
	for i := 0; i < n; i++ {
		go peers[i].ticker()
		go peers[i].heartbeatTicker()
		go peers[i].applier()
	}
	return peers, chans
}

// findLeader returns the index of a current leader, or -1.
func findLeader(peers []*Raft) int {
	for i := range peers {
		peers[i].mu.Lock()
		isLeader := peers[i].role == Leader
		peers[i].mu.Unlock()
		if isLeader {
			return i
		}
	}
	return -1
}

func TestLeaderElection(t *testing.T) {
	peers, _ := makeCluster(3)
	time.Sleep(1 * time.Second)
	if findLeader(peers) == -1 {
		t.Fatalf("no leader elected")
	}
}

func TestReplication(t *testing.T) {
	n := 3
	peers, chans := makeCluster(n)

	// Wait for a leader.
	time.Sleep(1 * time.Second)
	leader := findLeader(peers)
	if leader == -1 {
		t.Fatalf("no leader elected")
	}

	// Submit a command to the leader.
	cmd := 42
	idx, _, ok := peers[leader].Start(cmd)
	if !ok {
		t.Fatalf("Start rejected on the leader")
	}
	t.Logf("submitted command %v at index %d via server %d", cmd, idx, leader)

	// Every server should apply that command at that index.
	for i := 0; i < n; i++ {
		select {
		case msg := <-chans[i]:
			if msg.CommandIndex != idx {
				t.Fatalf("server %d: applied index %d, want %d", i, msg.CommandIndex, idx)
			}
			if msg.Command != cmd {
				t.Fatalf("server %d: applied command %v, want %v", i, msg.Command, cmd)
			}
			t.Logf("server %d applied command %v at index %d", i, msg.Command, msg.CommandIndex)
		case <-time.After(2 * time.Second):
			t.Fatalf("server %d never applied the command", i)
		}
	}
}

func TestBackfillAfterReconnect(t *testing.T) {
	n := 3
	peers, _ := makeCluster(n)
	time.Sleep(1 * time.Second)
	leader := findLeader(peers)
	if leader == -1 {
		t.Fatalf("no leader elected")
	}

	// Disconnect one follower.
	follower := (leader + 1) % n
	peers[follower].Disconnect()
	t.Logf("disconnected server %d (leader is %d)", follower, leader)

	// Submit commands while the follower is offline; the majority commits them.
	for i := 0; i < 5; i++ {
		_, _, ok := peers[leader].Start(100 + i)
		if !ok {
			t.Fatalf("Start rejected on leader")
		}
	}
	time.Sleep(1 * time.Second)

	// Reconnect — backtracking should backfill the missing entries.
	peers[follower].Reconnect()
	t.Logf("reconnected server %d", follower)
	time.Sleep(2 * time.Second)

	// The follower's log should now match the leader's length.
	peers[leader].mu.Lock()
	leaderLen := len(peers[leader].log)
	peers[leader].mu.Unlock()

	peers[follower].mu.Lock()
	followerLen := len(peers[follower].log)
	peers[follower].mu.Unlock()

	if followerLen != leaderLen {
		t.Fatalf("backfill failed: follower log len %d, leader %d", followerLen, leaderLen)
	}
	t.Logf("backfill succeeded: both logs length %d", leaderLen)
}

func TestLeaderFailover(t *testing.T) {
	n := 3
	peers, _ := makeCluster(n)
	time.Sleep(1 * time.Second)

	leader1 := findLeader(peers)
	if leader1 == -1 {
		t.Fatalf("no initial leader")
	}

	// Commit a command under the first leader.
	peers[leader1].Start(111)
	time.Sleep(500 * time.Millisecond)

	// Record the committed log length before killing the leader.
	peers[leader1].mu.Lock()
	committedLen := len(peers[leader1].log)
	peers[leader1].mu.Unlock()
	t.Logf("leader %d committed; log length %d", leader1, committedLen)

	// Kill the leader.
	peers[leader1].Disconnect()
	t.Logf("disconnected leader %d", leader1)

	// A new leader should emerge among the remaining servers.
	time.Sleep(2 * time.Second)
	leader2 := -1
	for i := range peers {
		if i == leader1 {
			continue // the old leader is partitioned; ignore it
		}
		peers[i].mu.Lock()
		if peers[i].role == Leader {
			leader2 = i
		}
		peers[i].mu.Unlock()
	}
	if leader2 == -1 {
		t.Fatalf("no new leader elected after failover")
	}
	t.Logf("new leader is %d", leader2)

	// The new leader must still have the committed entry.
	peers[leader2].mu.Lock()
	newLeaderLen := len(peers[leader2].log)
	peers[leader2].mu.Unlock()
	if newLeaderLen < committedLen {
		t.Fatalf("new leader lost committed data: log len %d < %d", newLeaderLen, committedLen)
	}

	// The cluster should still accept new commands.
	_, _, ok := peers[leader2].Start(222)
	if !ok {
		t.Fatalf("new leader rejected a command")
	}
	time.Sleep(1 * time.Second)
	t.Logf("failover succeeded: new leader %d serving, committed data intact", leader2)
}

func makeClusterWithPersisters(n int) ([]*Raft, []chan ApplyMsg, []*Persister) {
	peers := make([]*Raft, n)
	chans := make([]chan ApplyMsg, n)
	persisters := make([]*Persister, n)

	for i := 0; i < n; i++ {
		chans[i] = make(chan ApplyMsg, 1000)
		persisters[i] = MakePersister() // each server gets its OWN
	}
	for i := 0; i < n; i++ {
		peers[i] = Make(peers, i, persisters[i], chans[i])
	}
	// Only start ticking once every peer is published.
	for i := 0; i < n; i++ {
		peers[i].Run()
	}
	return peers, chans, persisters
}

func TestPersistenceRestart(t *testing.T) {
	n := 3
	peers, chans, persisters := makeClusterWithPersisters(n)
	time.Sleep(1 * time.Second)

	leader := findLeader(peers)
	if leader == -1 {
		t.Fatalf("no leader elected")
	}

	// Commit some commands.
	for i := 0; i < 3; i++ {
		peers[leader].Start(200 + i)
	}
	time.Sleep(1 * time.Second)

	// Record the leader's log length before the crash.
	peers[leader].mu.Lock()
	beforeLen := len(peers[leader].log)
	beforeTerm := peers[leader].currentTerm
	peers[leader].mu.Unlock()
	t.Logf("before crash: leader %d, log length %d, term %d", leader, beforeLen, beforeTerm)

	// CRASH a follower: throw away its Raft object entirely, then rebuild
	// it from the SAME persister — simulating a process death + restart.
	victim := (leader + 1) % n
	peers[victim].Disconnect() // stop the old incarnation from acting
	newChan := make(chan ApplyMsg, 1000)
	restarted := Make(peers, victim, persisters[victim], newChan)
	peers[victim] = restarted // replace with the reborn server
	chans[victim] = newChan
	restarted.Run()
	t.Logf("crashed and restarted server %d", victim)

	// The restarted server must have recovered its log and term.
	restarted.mu.Lock()
	recoveredLen := len(restarted.log)
	recoveredTerm := restarted.currentTerm
	restarted.mu.Unlock()

	if recoveredLen < beforeLen {
		t.Fatalf("restarted server lost log: recovered len %d < %d", recoveredLen, beforeLen)
	}
	if recoveredTerm < beforeTerm {
		t.Fatalf("restarted server lost term: recovered %d < %d", recoveredTerm, beforeTerm)
	}
	t.Logf("recovered: log length %d, term %d", recoveredLen, recoveredTerm)
}
