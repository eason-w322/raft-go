package kvstore

import (
	"testing"
	"time"
)

func makeKVCluster(n int) ([]*KVServer, *Clerk) {
	return MakeKVCluster(n)
}

func TestKVBasic(t *testing.T) {
	_, ck := makeKVCluster(3)
	time.Sleep(1 * time.Second) // let a leader emerge

	ck.Put("x", "hello")
	t.Logf("Put(x, hello) done")

	ck.Append("x", " world")
	t.Logf("Append(x, world) done")

	got := ck.Get("x")
	t.Logf("Get(x) = %q", got)

	if got != "hello world" {
		t.Fatalf("Get(x) = %q, want %q", got, "hello world")
	}
	t.Logf("SUCCESS")
}

func TestKVFaultTolerance(t *testing.T) {
	kvs, ck := makeKVCluster(3)
	time.Sleep(1 * time.Second)

	// Store some data.
	ck.Put("k1", "value1")
	ck.Put("k2", "value2")
	t.Logf("stored initial data")

	// Find the current leader and kill it.
	leader := -1
	for i := range kvs {
		if _, isLeader := kvs[i].rf.GetState(); isLeader {
			leader = i
			break
		}
	}
	if leader == -1 {
		t.Fatalf("no leader found")
	}
	kvs[leader].rf.Disconnect()
	t.Logf("killed leader (server %d)", leader)

	// The cluster should elect a new leader; the Clerk retries automatically.
	// Verify old data survived and new writes still work.
	if got := ck.Get("k1"); got != "value1" {
		t.Fatalf("after leader failure, Get(k1) = %q, want %q", got, "value1")
	}
	t.Logf("old data survived: k1 = value1")

	ck.Put("k3", "value3")
	if got := ck.Get("k3"); got != "value3" {
		t.Fatalf("after failover, Get(k3) = %q, want %q", got, "value3")
	}
	t.Logf("new writes work after failover: k3 = value3")

	// Append still works too (tests the trickier op).
	ck.Append("k1", "-more")
	if got := ck.Get("k1"); got != "value1-more" {
		t.Fatalf("Get(k1) after append = %q, want %q", got, "value1-more")
	}
	t.Logf("SUCCESS: cluster survived leader failure, data intact, still serving")
}

func TestKVDedup(t *testing.T) {
	kvs, _ := makeKVCluster(3)
	time.Sleep(1 * time.Second)

	// Find the leader (we send directly to it to control the requests).
	leader := -1
	for i := range kvs {
		if _, isLeader := kvs[i].rf.GetState(); isLeader {
			leader = i
			break
		}
	}
	if leader == -1 {
		t.Fatalf("no leader found")
	}

	clientId := int64(12345)

	// First append: k = "A"
	args1 := &PutAppendArgs{Key: "k", Value: "A", Op: "Append", ClientId: clientId, SeqNum: 1}
	reply1 := &PutAppendReply{}
	kvs[leader].PutAppend(args1, reply1)
	t.Logf("first append (seq 1): Err=%s", reply1.Err)

	// DUPLICATE: same ClientId and SeqNum, sent again.
	// The server must recognize it as already-applied and NOT append twice.
	args2 := &PutAppendArgs{Key: "k", Value: "A", Op: "Append", ClientId: clientId, SeqNum: 1}
	reply2 := &PutAppendReply{}
	kvs[leader].PutAppend(args2, reply2)
	t.Logf("duplicate append (seq 1 again): Err=%s", reply2.Err)

	// A genuinely new append: seq 2, value "B" -> k should be "AB"
	args3 := &PutAppendArgs{Key: "k", Value: "B", Op: "Append", ClientId: clientId, SeqNum: 2}
	reply3 := &PutAppendReply{}
	kvs[leader].PutAppend(args3, reply3)
	t.Logf("new append (seq 2): Err=%s", reply3.Err)

	// Read back via a fresh client.
	ck := MakeClerk(kvs)
	got := ck.Get("k")
	t.Logf("Get(k) = %q", got)

	// If dedup works: "A" (once, not twice) + "B" = "AB".
	// If dedup is broken: "AA" + "B" = "AAB" (the duplicate got applied).
	if got != "AB" {
		t.Fatalf("dedup failed: Get(k) = %q, want %q (got a double-apply if you see 'AAB')", got, "AB")
	}
	t.Logf("SUCCESS: duplicate request was deduplicated (value is AB, not AAB)")
}
