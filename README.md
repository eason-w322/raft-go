# raft-go

A fault-tolerant, replicated key-value store built from scratch in Go.

Raft handles leader election, log replication, and crash recovery; a key-value store layered on top turns the consensus core into a linearizable, fault-tolerant database. The entire system — consensus, replication, persistence, deduplication, and the client — is implemented and tested without external consensus libraries.

## Features

**Consensus core (`raft/`)**
- Leader election with randomized timeouts
- Log replication with fast conflict-resolution backtracking
- The current-term commit rule (prevents committed-entry loss across leader changes)
- Election restriction (up-to-date log check) guaranteeing Leader Completeness
- Crash recovery via persistent state (`currentTerm`, `votedFor`, `log`)

**Key-value store (`kv_store/`)**
- Linearizable `Put` / `Append` / `Get` routed through the Raft log
- Client-side leader discovery with automatic retry
- Idempotent request deduplication (exactly-once application under retries)
- A client (`Clerk`) that tolerates leader failure transparently

## Architecture
Each server runs a `KVServer` wrapping its own `Raft` instance. Client operations flow down through `Start` into the Raft log; once committed, they flow back up through `applyCh` and are applied to each server's state machine. The two layers communicate only through this narrow interface, keeping consensus and application concerns cleanly separated.

## Testing

All tests run under Go's race detector.

```bash
# consensus core
cd raft && go test -race ./...

# key-value store
cd kv_store && go test -race ./...
```

The suite covers leader election, log replication, follower catch-up after partition, leader failover with committed-data survival, crash-restart persistence, end-to-end key-value operations, fault tolerance, and request deduplication.

## Performance

Benchmarked on a 3-node in-process cluster (no network transport; latency reflects consensus and scheduling overhead only). The optimization work below was driven by profiling: each bottleneck was identified by benchmarking, diagnosed, and fixed.

| Stage | Throughput | p50 latency | p99 latency |
|---|---|---|---|
| Baseline | 10 ops/sec | 98.4 ms | 110.2 ms |
| + Replicate-on-submit | 32,195 ops/sec | 25 µs | 112 µs |
| + Event-driven apply (`sync.Cond`) | 46,629 ops/sec | 18.9 µs | 51.6 µs |

**Optimizations:**

1. **Replicate-on-submit.** The baseline gated log replication on the 100 ms heartbeat interval, so every operation waited up to a full heartbeat to commit. Triggering replication immediately on submission removed this fixed delay — a ~100 ms → sub-millisecond latency drop.

2. **Event-driven apply.** Committed entries were applied by a 10 ms polling loop, capping latency at the poll interval. Replacing the poll with a condition variable that signals on commit made application event-driven, eliminating the remaining fixed delay.

3. **Closing the submit/apply race.** Once latency dropped to microseconds, a latent race surfaced: fast commits could apply *before* the request handler registered its waiter, causing occasional 500 ms timeout tails. Registering the waiter atomically with submission (under a single lock hold) closed the window — visible as the p99 dropping ~2× while p50 held steady.

Leader failover completes in ~1 s, consistent with the 300–600 ms randomized election timeout plus client retry.

## Design notes

- **Linearizable reads.** `Get` operations are routed through the Raft log rather than served from a local map, guaranteeing reads reflect a committed, globally-ordered state.
- **Exactly-once semantics.** Each client stamps requests with a `(clientId, seqNum)` pair; servers track the highest applied sequence number per client and skip duplicates at apply time, so client retries never double-apply an operation.

## Status

Core Raft and the key-value store are complete and tested. Possible extensions: log compaction / snapshotting, and sharding across multiple Raft groups.