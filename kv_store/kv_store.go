package kvstore

import (
	"raftgo/raft"
	"sync"
	"time"
)

// Op is client operation
type Op struct {
	Type  string // specify which operation: "Put", "Get", or "Append"
	Key   string // every operation targets on a key
	Value string

	ClientId int64 // which client send this
	SeqNum   int   // the client's request counter
}

type KVServer struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	data    map[string]string //the actual key-value store. This is the state machine
	lastSeq map[int64]int
	waiters map[int]chan applyResult
}

type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "put" or "append"
	ClientId int64
	SeqNum   int
}

type PutAppendReply struct {
	Err string // "OK" or "ErrWrongLeader"
}

type GetArgs struct {
	Key      string
	ClientId int64
	SeqNum   int
}

type GetReply struct {
	Err   string // "OK" or "ErrWrongLeader"
	Value string
}

type applyResult struct {
	ClientId int64
	SeqNum   int
	Value    string //the value read for Get
}

func StartKVServer(peer []*raft.Raft, me int, persister *raft.Persister) *KVServer {
	kv := &KVServer{
		applyCh: make(chan raft.ApplyMsg, 1000),
		data:    make(map[string]string),
		lastSeq: make(map[int64]int),
		waiters: make(map[int]chan applyResult),
	}
	kv.rf = raft.Make(peer, me, persister, kv.applyCh)
	go kv.applyloop()
	return kv
}

func (kv *KVServer) applyloop() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		op := msg.Command.(Op) // type assertion. treat this interface{} as an Op
		kv.mu.Lock()
		lastSeq, seen := kv.lastSeq[op.ClientId]    //Go maps return two things: the value, and a bool (seen) saying whether the key existed.
		isDuplicate := seen && op.SeqNum <= lastSeq // guard to prevent retries

		if !isDuplicate {
			switch op.Type {
			case "Put":
				kv.data[op.Key] = op.Value
			case "Append":
				kv.data[op.Key] += op.Value
			case "Get":
				// no state change, naturally idempotent
			}
			kv.lastSeq[op.ClientId] = op.SeqNum
		}

		//waking the waiters
		result := applyResult{
			ClientId: op.ClientId,
			SeqNum:   op.SeqNum,
			Value:    kv.data[op.Key],
		}

		ch, waiting := kv.waiters[msg.CommandIndex]
		if waiting {
			ch <- result
			delete(kv.waiters, msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}
	// register a channel to be notified when this index is applied
	kv.mu.Lock()
	ch := make(chan applyResult, 1)
	kv.waiters[index] = ch
	kv.mu.Unlock()

	// Block until the apply loop applies our op (or we give up).
	select {
	case appliedOp := <-ch:
		if appliedOp.ClientId == op.ClientId && appliedOp.SeqNum == op.SeqNum {
			reply.Err = "OK"
		} else {
			reply.Err = "ErrWrongLeader"
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "ErrWrongLeader" // timed out; probably lost leadership
	}
	kv.mu.Lock()
	delete(kv.waiters, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	index, _, is_Leader := kv.rf.Start(op)
	if !is_Leader {
		reply.Err = "ErrWrongLeader"
		return
	}

	// same as in PutAppend, register a ch in waiters to be waken up later
	kv.mu.Lock()
	ch := make(chan applyResult, 1)
	kv.waiters[index] = ch
	kv.mu.Unlock()

	select {
	case result := <-ch:
		if result.ClientId == op.ClientId && result.SeqNum == op.SeqNum {
			reply.Err = "OK"
			reply.Value = result.Value
		} else {
			reply.Err = "ErrWrongLeader"
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = "ErrWrongLeader"
	}

	kv.mu.Lock()
	delete(kv.waiters, index)
	kv.mu.Unlock()

}
