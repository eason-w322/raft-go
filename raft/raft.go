package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type Raft struct {
	mu    sync.Mutex
	peers []*Raft
	me    int

	currentTerm int
	votedFor    int
	log         []LogEntry

	role          Role
	electionReset time.Time

	//volatile state on each server
	commitIndex int //highest index known to be committed
	lastApplied int //hieghest index apply to the state machine

	// applyCond is signalled whenever commitIndex advances, so applier wakes
	// immediately instead of polling. Its Locker is rf.mu.
	applyCond *sync.Cond

	//volatile state on leader only
	nextIndex  []int // for each peer, index of next entry to send them
	matchIndex []int // for each peer, highest index known replicated on them

	disconnected bool //simulated network partition: if true, RPCs to/from this server fail
	persister    *Persister

	applyCh chan ApplyMsg
}

type Persister struct {
	mu    sync.Mutex
	state []byte
}

func MakePersister() *Persister { //everyone refers to one underlying persister
	return &Persister{}
}

func (ps *Persister) Save(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.state = state //the actual store
}

func (ps *Persister) Read() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.state
}

func (rf *Raft) persist() {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.log)
	rf.persister.Save(buf.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	dec := gob.NewDecoder(bytes.NewBuffer(data))
	dec.Decode(&rf.currentTerm)
	dec.Decode(&rf.votedFor)
	dec.Decode(&rf.log)
}

func (rf *Raft) Disconnect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.disconnected = true
}

func (rf *Raft) Reconnect() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.disconnected = false
}

// how raft tells the application this entry is committed and then apply it
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//election starts here

// a candidate sends to server ask for vote
type RequestVoteArg struct {
	Term         int
	CandidateId  int //identify who is asking
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// a reply server send back to candidate
type RequestVoteReply struct {
	Term        int // this is responder's term
	VoteGranted bool
}

// RequestVote is a handler a candidate calls on a peer to ask for its vote
func (rf *Raft) RequestVote(args *RequestVoteArg, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.disconnected { //receiver side don't take RPC
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist() // the term bump must survive even if we go on to refuse the vote
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	// grant vote only if we haven't voted for someone else in this term
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (rf.isCandidateUptoDate(args.LastLogIndex, args.LastLogTerm)) {
		rf.votedFor = args.CandidateId
		rf.role = Follower
		rf.electionReset = time.Now()
		reply.VoteGranted = true
		rf.persist() //first persisit
	}
}

func (rf *Raft) isCandidateUptoDate(candLastIndex int, candLastTerm int) bool {
	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	if candLastTerm != myLastTerm {
		return candLastTerm > myLastTerm
	}
	return candLastIndex >= myLastIndex
}

// startElection runs when this server times out and becomes a candidate.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.disconnected {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persist() //second persist
	rf.electionReset = time.Now()
	me := rf.me
	term := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	votes := 1
	for i := range rf.peers {
		if i == me {
			continue
		}
		// fire vote requests at every peer in parallel
		go func(peer int) {
			args := &RequestVoteArg{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{} //wait for voters to fill in
			rf.peers[peer].RequestVote(args, reply)

			// lock the state after each goroutine returns with reply
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.role = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist() //third persist
				return
			}
			// stale vote reply
			if rf.role != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted && rf.role == Candidate {
				votes++
				if votes > len(rf.peers)/2 {
					rf.role = Leader
					// initalize leader state
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log) //we guess optimistically, assume cought up
						rf.matchIndex[i] = 0          //we assume knows nothing
					}
				}
			}

		}(i) //call the ith server
	}
}

// ticker is a background goroutine that starts an election if too much
// time passes without hearing from a leader.
func (rf *Raft) ticker() {
	for {
		timeout := time.Duration(300+rand.Intn(300)) * time.Millisecond
		time.Sleep(timeout)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.electionReset) >= timeout {
			rf.mu.Unlock()
			rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*Raft, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:         peers,
		me:            me,
		currentTerm:   0,
		votedFor:      -1,
		role:          Follower,
		electionReset: time.Now(),
		log:           []LogEntry{{Term: 0}},
		applyCh:       applyCh,
		persister:     persister,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.Read()) // read persister state
	return rf
}

// Run starts the background goroutines. It is deliberately separate from Make:
// the peers slice is shared by every server, so all of it must be filled in
// before anything reads it. The `go` statements below are what publish those
// writes to the new goroutines.
func (rf *Raft) Run() {
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applier()
}

// when leader sends to follower to replicate entries/ send heartbeats
type AppendEntriesArgs struct {
	Term     int //leader's term
	LeaderId int //so the follower knows who the leader is

	PrevLogIndex int        // index of the entry immediately before the new ones
	PrevLogTerm  int        // term of that entry
	Entries      []LogEntry // the new entries to store (empty = pure heartbeat)
	LeaderCommit int        // leader's commitIndex, so follower can advance its own
}

type AppendEntriesReplies struct {
	Term    int  //responder's term
	Success bool // did the follower accept?
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReplies) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.disconnected { //receiver side disconnect
		return
	}

	reply.Success = false
	reply.Term = rf.currentTerm

	//1. reject a stale leader
	if args.Term < rf.currentTerm {
		return //without resetting the timer
	}
	//2. valid leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist() // the consistency check below may return before we reach the persist at the end
	}
	rf.role = Follower
	rf.electionReset = time.Now()

	//3. consistency check: do I have an entry at PrevLogIndex with PrevLogTerm?
	if args.PrevLogIndex >= len(rf.log) { //my log is too short
		return
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { //term doesn't match
		return
	}
	//4.logs match now replicate/append
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx < len(rf.log) { //my log already has some entry at this position
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, entry)
			}
			// else already have this entry we just skip safely
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist() //fourth persisit
	reply.Success = true

	//5. advance commitIndex to min(leader's commit, my last index).
	if args.LeaderCommit > rf.commitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm != term || rf.disconnected { //sender side disconnect
				rf.mu.Unlock()
				return
			}
			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			entries := make([]LogEntry, len(rf.log)-rf.nextIndex[peer])
			copy(entries, rf.log[rf.nextIndex[peer]:])

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			reply := &AppendEntriesReplies{}
			rf.peers[peer].AppendEntries(args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.persist() //fifth persist
			}
			if rf.role != Leader || rf.currentTerm != term {
				return
			}
			if reply.Success {
				// followers now matches through prevLogIndex + len(entries).
				// only ever move forward: a delayed reply from an older round
				// would otherwise drag matchIndex/nextIndex backwards.
				if newMatch := args.PrevLogIndex + len(entries); newMatch > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatch
					rf.nextIndex[peer] = newMatch + 1
					rf.advanceCommit()
				}
			} else {
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
				}
			}
		}(i)
	}
}

func (rf *Raft) advanceCommit() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count += 1
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}

	}
}

func (rf *Raft) heartbeatTicker() {
	for {
		time.Sleep(100 * time.Millisecond)
		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()
		if isLeader {
			rf.sendAppendEntries()
		}
	}
}

// the client's entry point for submitting a command
// submit a new command to the log, returns the index the command will appear at, the current term, and whether this server is the leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.role != Leader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	index := len(rf.log) - 1
	term := rf.currentTerm
	rf.persist()
	rf.mu.Unlock()

	go rf.sendAppendEntries() // replicate now, don't wait for the heartbeat
	return index, term, true
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait() // releases rf.mu while parked
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
	}
}
