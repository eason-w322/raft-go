package kvstore

import (
	"raftgo/raft"
	"sync"
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
	waiters map[int]chan Op
}

func StartKVServer(peer []*raft.Raft, me int, persister *raft.Persister) *KVServer {
	kv := &KVServer{
		applyCh: make(chan raft.ApplyMsg, 1000),
		data:    make(map[string]string),
		lastSeq: make(map[int64]int),
		waiters: make(map[int]chan Op),
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
		ch, waiting := kv.waiters[msg.CommandIndex]
		if waiting {
			ch <- op
			delete(kv.waiters, msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}
