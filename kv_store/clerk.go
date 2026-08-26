package kvstore

import (
	"crypto/rand"
	"math/big"
)

type Clerk struct {
	servers  []*KVServer // the kv server it can talk to
	clientId int64       // globally unique client id
	seqNum   int         //monotonically increasing request counter
	LeaderId int         //cached guess of who the leader is
}

// nrand returns a large random int64
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}

func makeClerk(servers []*KVServer) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: nrand(),
		LeaderId: 0,
	}
}

func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}
	for {
		reply := &GetReply{}
		ck.servers[ck.LeaderId].Get(args, reply)
		if reply.Err == "OK" {
			return reply.Value
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) { //need op to specify whether it is a "Put" or "Append" operation
	ck.seqNum++
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqNum:   ck.seqNum,
	}

	for {
		reply := &PutAppendReply{}
		ck.servers[ck.LeaderId].PutAppend(args, reply)
		if reply.Err == "OK" {
			return
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
