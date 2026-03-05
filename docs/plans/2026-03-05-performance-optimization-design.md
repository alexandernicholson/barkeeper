# Performance Optimization Design

**Date:** 2026-03-05
**Status:** Approved
**Goal:** Reach parity with or exceed etcd on single-node throughput and latency.

## Current Performance (single-node, 2 CPU, 512MB RAM)

| Scenario | Barkeeper | etcd | Ratio |
|----------|-----------|------|-------|
| Write c=100 | 91 req/s | 9,645 req/s | 0.01x |
| Read c=100 | 6,937 req/s | 10,082 req/s | 0.69x |
| Connection scaling c=1000 | 9,035 req/s | 78,930 req/s | 0.11x |

## Root Causes

1. **3N fsyncs per N writes** — Raft log, KV store, and applied-index each do separate redb transactions with fsync.
2. **Zero write batching** — Every PUT is its own Raft proposal, log append, redb transaction.
3. **O(N) range scan per write** — `find_latest_kv_in_table()` scans all historical revisions of a key.
4. **4x JSON serialization per write** — Same data encoded/decoded at HTTP, Raft, state machine, and redb layers.
5. **Reads serialized through single actor** — 256-bounded channel, one spawn_blocking per read.
6. **Revision counter read from disk** — Every PUT reads current revision from redb meta table.

## Design

### 1. Write Batch Pipeline

**Raft proposal batching:**
- Raft node collects proposals for a configurable window (default: 1ms or 64 proposals, whichever first).
- Appends entire batch as one log store operation — 1 fsync instead of N.

**State machine batch apply:**
- Applies all entries in a `Vec<LogEntry>` within a single redb write transaction.
- One `begin_write()`, N operations, one `commit()` — 1 fsync instead of 3N.
- Revision counter incremented in-memory, persisted once at end of batch.
- `last_applied_raft_index` persisted once at end of batch.

**Bypass actor for batch applies:**
- State machine gets direct `Arc<KvStore>` access instead of sending N messages through actor channel.
- Calls `store.put()`, `store.delete_range()` directly within the batch transaction.
- Actor channel reserved for non-Raft operations.
- Eliminates N channel hops + N `spawn_blocking` calls per batch.

**Expected impact:** 20-50x write improvement.

### 2. Read Path — Direct Store Access

- Read operations get a direct `Arc<KvStore>` handle.
- Call `store.range()` via `spawn_blocking` on tokio's blocking pool — parallel reads across all cores.
- redb supports concurrent read transactions natively (readers never block writers).
- Actor channel reserved for write coordination only.
- No channel hop, no actor queuing, no 256-entry bottleneck.

**Expected impact:** 5-10x read improvement at high concurrency.

### 3. Serialization & Index Optimization

**Replace JSON with bincode for internal paths:**
- Raft log entries: `KvCommand` serialized with bincode (3-5x faster, smaller payloads).
- redb storage: `InternalKeyValue` stored as bincode.
- HTTP gateway boundary: JSON stays (API contract).

**Latest-key index table:**
- New redb table: `LATEST_TABLE: &[u8] -> u64` mapping key to latest revision.
- Updated in the same batch transaction as the put.
- `find_latest_kv_in_table()` becomes a single point lookup instead of O(N) range scan.
- `range()` queries use the index to jump directly to latest revision per key.
- Compaction updates the index when removing old revisions.

**In-memory revision counter:**
- `AtomicI64` holds current revision.
- Incremented in-memory during batch apply.
- Persisted once at end of batch transaction.
- Initialized from redb on startup.

**Expected impact:** Additional 2-3x on top of batching.

## Combined Expected Impact

| Scenario | Current | Target |
|----------|---------|--------|
| Write c=100 | 91 req/s | 5,000-15,000 req/s |
| Read c=100 | 6,937 req/s | 50,000-80,000 req/s |
| Connection scaling c=1000 | 9,035 req/s | 60,000-100,000 req/s |

## What We're NOT Doing

- **Custom storage engine (LSM/WAL)** — redb is fine once we stop micro-transacting it.
- **Multi-threaded Raft** — Single Raft core is correct; batching is the fix.
- **Connection pooling / HTTP/2** — Not the bottleneck at this stage.
