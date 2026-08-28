package kvstore

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestBenchmarkThroughput(t *testing.T) {
	_, ck := makeKVCluster(3)
	time.Sleep(1 * time.Second) // let a leader settle

	const numOps = 500
	latencies := make([]time.Duration, 0, numOps)

	start := time.Now()
	for i := 0; i < numOps; i++ {
		opStart := time.Now()
		ck.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		latencies = append(latencies, time.Since(opStart))
	}
	elapsed := time.Since(start)

	// Throughput.
	throughput := float64(numOps) / elapsed.Seconds()

	// Latency percentiles.
	sort.Slice(latencies, func(a, b int) bool { return latencies[a] < latencies[b] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]

	t.Logf("=== THROUGHPUT & LATENCY (3-node in-process cluster) ===")
	t.Logf("operations:  %d Puts", numOps)
	t.Logf("total time:  %v", elapsed)
	t.Logf("throughput:  %.0f ops/sec", throughput)
	t.Logf("latency p50: %v", p50)
	t.Logf("latency p99: %v", p99)
}

// Failover time: kill the leader, measure how long until the cluster serves again.
func TestBenchmarkFailover(t *testing.T) {
	kvs, ck := makeKVCluster(3)
	time.Sleep(1 * time.Second)

	// Prime the store.
	ck.Put("k", "before")

	// Find and kill the leader.
	leader := -1
	for i := range kvs {
		if _, isLeader := kvs[i].rf.GetState(); isLeader {
			leader = i
			break
		}
	}
	if leader == -1 {
		t.Fatalf("no leader")
	}

	killTime := time.Now()
	kvs[leader].rf.Disconnect()

	// The next successful write measures recovery: the Clerk retries until
	// a new leader is elected and serving.
	ck.Put("k", "after")
	recovery := time.Since(killTime)

	// Verify it actually worked.
	if got := ck.Get("k"); got != "after" {
		t.Fatalf("after failover Get = %q, want %q", got, "after")
	}

	t.Logf("=== FAILOVER TIME (3-node cluster, leader killed) ===")
	t.Logf("recovery time: %v (kill -> next successful write)", recovery)
}
