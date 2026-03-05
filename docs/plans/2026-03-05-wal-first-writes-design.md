# WAL-First Writes with Pre-assigned Revisions

## Problem

Write throughput at high concurrency is 0.44x etcd. The bottleneck: each write batch requires 2 serial fsyncs (Raft log + KV B-tree apply), and the HTTP handler blocks until KV apply completes.

## Solution

Decouple write acknowledgment from KV apply by pre-assigning revisions at Raft proposal time. Writes acknowledge after Raft log commit only. Reads use ReadIndex to maintain linearizability.

## Section 1: Write Path — Pre-assigned Revisions + Early Acknowledge

**Current flow:**
1. Handler → `propose(data)` → Raft log fsync → log index
2. Handler → `broker.wait_for_result(index)` → KV apply fsync → revision
3. Write latency = Raft log fsync + KV apply fsync

**New flow:**
1. Raft actor assigns revision via AtomicI64 (moved from KvStore)
2. Revision embedded in LogEntry: `Command { data, revision }`
3. `RespondToProposal` returns `Success { index, revision }` after Raft log commit
4. Handler returns HTTP response immediately — no broker wait for puts
5. KV apply runs asynchronously using pre-assigned revision from log entry

**Changes:**
- `ClientProposal` carries deserialized `KvCommand` so Raft actor can count revision bumps
- Raft actor owns the AtomicI64 revision counter
- `LogEntryData::Command` includes pre-assigned revision
- `ClientProposalResult::Success` returns real revision (not 0)
- Put handler drops `broker.wait_for_result()` — returns after `propose()`
- `batch_apply_with_index` uses revision from log entry, not its own counter

## Section 2: Read Path — ReadIndex

**Current flow:** Direct `store.range()` with no consistency guarantee.

**New flow:**
1. Handler calls `raft_handle.read_index()` → current commit index
2. Handler waits for `applied_index >= commit_index` via ApplyNotifier
3. Handler does `store.range()` as before

**Single-node optimization:** `read_index()` returns local commit index. No Raft round-trip. Wait is typically zero or microseconds.

**Changes:**
- New `RaftHandle::read_index() -> u64`
- New `ApplyNotifier`: `AtomicU64` + `tokio::sync::watch::Sender<u64>`. State machine updates after each batch. Readers call `wait_for(index)`.
- `handle_range` / `range` call read_index + wait before store access

## Section 3: Response Handling + Edge Cases

**Per-operation behavior:**

| Operation | Fast path? | Details |
|---|---|---|
| Put (no prev_kv) | Yes | Pre-assigned revision, return after Raft commit |
| Put (prev_kv=true) | Yes | Read prev_kv from store before proposing, return after Raft commit |
| DeleteRange | No | Wait on broker — needs deleted count/prev_kvs from apply |
| Txn | No | Wait on broker — needs success/failure results from apply |

**Crash recovery:**
- On restart, load last persisted revision from KV META_TABLE
- Raft actor's AtomicI64 starts from that value
- Committed-but-not-applied entries replay from Raft log with their pre-assigned revisions

**ApplyNotifier (shared):**
- `AtomicU64` for current applied index
- `tokio::sync::watch::Sender<u64>` for notification
- State machine updates after each batch apply
- Readers call `wait_for(commit_index)` — returns immediately if caught up

## TDD

All changes implemented test-first: write failing test, then implement to make it pass.
