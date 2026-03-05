# Write Throughput Optimization Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the 4x write throughput gap with etcd at high concurrency (c=100) by decoupling the KV store fsync from the client response path.

**Architecture:** Currently barkeeper has 2 sequential fsyncs per write batch: WAL (raft.redb) then KV store (kv.redb). etcd responds after WAL fsync only — the KV store (bbolt) batches fsyncs on a 100ms timer. We replicate this pattern: respond to PUT after WAL fsync, apply KV writes in-memory immediately, and flush to redb on a batched timer. Additionally fix low-hanging fruit: move WAL fsync to spawn_blocking, pass entries in-memory instead of re-reading from disk, and use bincode for log entries.

**Tech Stack:** Rust, tokio, redb, bincode, DashMap (concurrent hashmap for write buffer)

---

### Task 1: Add DashMap dependency

**Files:**
- Modify: `Cargo.toml`

**Step 1: Add dashmap to Cargo.toml**

Add `dashmap = "6"` to `[dependencies]` in Cargo.toml, after the `redb` line:

```toml
dashmap = "6"
```

**Step 2: Verify it compiles**

Run: `cargo check 2>&1 | tail -5`
Expected: successful compilation

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "deps: add dashmap for concurrent write buffer"
```

---

### Task 2: Switch log entry serialization from JSON to bincode

The LogStore currently serializes entries with serde_json. bincode is ~5-10x faster and produces smaller output. The state machine already tries bincode first (state_machine.rs:70-73) with JSON fallback, so this is backwards compatible.

**Files:**
- Modify: `src/raft/log_store.rs:33-43` (append method)
- Modify: `src/raft/log_store.rs:47-57` (get method)
- Modify: `src/raft/log_store.rs:61-71` (get_range method)
- Modify: `src/raft/log_store.rs:105-117` (last_term method)
- Modify: `src/raft/log_store.rs:120-122` (term_at → get)
- Modify: `src/raft/log_store.rs:125-134` (save_hard_state)
- Modify: `src/raft/log_store.rs:137-148` (load_hard_state)
- Test: `tests/log_store_test.rs`

**Step 1: Write a test that round-trips a LogEntry through bincode**

In `tests/log_store_test.rs`, add a test that creates a LogStore, appends an entry, reads it back, and verifies the data is intact. This test should already exist — check if it does and verify it passes first.

Run: `cargo test --test log_store_test -- --nocapture 2>&1 | tail -20`

**Step 2: Change LogStore serialization to bincode**

In `src/raft/log_store.rs`, replace all `serde_json::to_vec` calls with `bincode::serialize` and all `serde_json::from_slice` calls with `bincode::deserialize`. Specifically:

- `append()`: `serde_json::to_vec(entry)` → `bincode::serialize(entry)`
- `get()`: `serde_json::from_slice(val.value())` → `bincode::deserialize(val.value())`
- `get_range()`: `serde_json::from_slice(val.value())` → `bincode::deserialize(val.value())`
- `last_term()`: `serde_json::from_slice(v.value())` → `bincode::deserialize(v.value())`
- `save_hard_state()`: `serde_json::to_vec(state)` → `bincode::serialize(state)`
- `load_hard_state()`: `serde_json::from_slice(val.value())` → `bincode::deserialize(val.value())`

Keep `.expect("serialize ...")` / `.expect("deserialize ...")` patterns. bincode is already in Cargo.toml.

**Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass. If log_store_test fails, it means existing data was written with JSON — that's fine since tests use tempdir.

**Step 4: Commit**

```bash
git add src/raft/log_store.rs
git commit -m "perf: switch log entry serialization from JSON to bincode"
```

---

### Task 3: Move log_store.append() to spawn_blocking

Currently `log_store.append()` calls redb `txn.commit()` (which fsyncs) directly on the tokio task running the Raft actor. This blocks the async runtime. Move it to `spawn_blocking`.

**Files:**
- Modify: `src/raft/log_store.rs` — make LogStore `Send + Sync` (wrap db in Arc if needed)
- Modify: `src/raft/node.rs:257-258` — wrap append call in spawn_blocking
- Modify: `src/raft/node.rs:254-256` — wrap save_hard_state in spawn_blocking
- Test: `tests/compat_test.rs` (existing tests cover correctness)

**Step 1: Make LogStore shareable**

Wrap the `db: Database` in `Arc<Database>` so LogStore can be cloned and sent to spawn_blocking. Add `#[derive(Clone)]` to LogStore:

```rust
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}
```

Update `open()` to use `Arc::new(Database::create(path)?)`.

**Step 2: Move blocking calls in execute_actions to spawn_blocking**

In `src/raft/node.rs`, in the `execute_actions` function:

For `Action::PersistHardState`:
```rust
Action::PersistHardState(state) => {
    let ls = log_store.clone();
    let s = state.clone();
    tokio::task::spawn_blocking(move || ls.save_hard_state(&s).unwrap())
        .await
        .unwrap();
}
```

For `Action::AppendToLog`:
```rust
Action::AppendToLog(entries) => {
    let ls = log_store.clone();
    let e = entries.clone();
    tokio::task::spawn_blocking(move || ls.append(&e).unwrap())
        .await
        .unwrap();
}
```

For `Action::TruncateLogAfter`:
```rust
Action::TruncateLogAfter(index) => {
    let ls = log_store.clone();
    let idx = *index;
    tokio::task::spawn_blocking(move || ls.truncate_after(idx).unwrap())
        .await
        .unwrap();
}
```

Do the same in `execute_actions_rebar`.

**Step 3: Run all tests**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass

**Step 4: Commit**

```bash
git add src/raft/log_store.rs src/raft/node.rs
git commit -m "perf: move log_store operations to spawn_blocking"
```

---

### Task 4: Pass entries in-memory to apply pipeline (eliminate disk round-trip)

Currently `Action::ApplyEntries { from, to }` reads entries back from disk via `log_store.get_range(from, to)`. These entries were just written in `AppendToLog`. Cache them in-memory and pass directly.

**Files:**
- Modify: `src/raft/core.rs:19` — change `ApplyEntries` to carry entries
- Modify: `src/raft/node.rs:263-267` — use entries from action instead of reading from disk
- Test: `tests/compat_test.rs` (existing tests)

**Step 1: Change Action::ApplyEntries to carry entries**

In `src/raft/core.rs`, change:
```rust
ApplyEntries { from: u64, to: u64 },
```
to:
```rust
ApplyEntries { entries: Vec<LogEntry> },
```

**Step 2: Update RaftCore to populate entries in ApplyEntries actions**

In `src/raft/core.rs`, the `advance_commit_index` and `handle_append_entries` methods create `Action::ApplyEntries`. They need access to the entries. Since RaftCore is a pure state machine without storage access, we need a different approach:

Add a field to RaftCore to buffer recently appended entries:
```rust
/// Recently appended entries, keyed by index, for passing to apply without disk reads.
recent_entries: Vec<LogEntry>,
```

In `handle_proposal` and `handle_append_entries`, when entries are added via `AppendToLog`, also store them in `recent_entries`.

In `advance_commit_index` and `handle_append_entries` where `ApplyEntries` is created, extract the relevant entries from `recent_entries`:
```rust
let entries: Vec<LogEntry> = self.recent_entries
    .iter()
    .filter(|e| e.index >= from && e.index <= to)
    .cloned()
    .collect();
// Trim entries that are now committed
self.recent_entries.retain(|e| e.index > to);
Action::ApplyEntries { entries }
```

**Step 3: Update execute_actions to use entries from action**

In `src/raft/node.rs`, change:
```rust
Action::ApplyEntries { from, to } => {
    let entries = log_store.get_range(*from, *to).unwrap();
    apply_tx.send(entries).await.ok();
    applied_index.store(*to, Ordering::Relaxed);
    commit_index.store(*to, Ordering::Release);
}
```
to:
```rust
Action::ApplyEntries { entries } => {
    let last_idx = entries.last().map(|e| e.index).unwrap_or(0);
    apply_tx.send(entries.clone()).await.ok();
    applied_index.store(last_idx, Ordering::Relaxed);
    commit_index.store(last_idx, Ordering::Release);
}
```

Do the same in `execute_actions_rebar`.

**Step 4: Update `handle_append_entries` for follower path**

For followers, entries come from `AppendEntriesRequest`. When `handle_append_entries` in core.rs creates `ApplyEntries`, it should include entries from the follower's recent_entries buffer (populated when `AppendToLog` is emitted).

**IMPORTANT**: For followers receiving entries via AppendEntries, the entries to apply may span entries from previous AppendEntries calls that were just committed. In this case, we need a fallback. Keep `log_store` accessible and fall back to `get_range` if `entries` in the action is empty. But for the common single-node leader path, entries will always be available in-memory.

Actually, simpler approach: Instead of modifying the core state machine, handle this at the `execute_actions` level. Keep a `recent_entries: Vec<LogEntry>` buffer in the actor loop. When `AppendToLog(entries)` fires, extend the buffer. When `ApplyEntries { from, to }` fires, drain matching entries from the buffer. Fall back to `log_store.get_range()` only if buffer doesn't have them.

This keeps core.rs untouched:

```rust
// In execute_actions, add recent_entries parameter:
Action::AppendToLog(entries) => {
    recent_entries.extend(entries.iter().cloned());
    // ... spawn_blocking append ...
}
Action::ApplyEntries { from, to } => {
    // Try in-memory first
    let entries: Vec<LogEntry> = recent_entries
        .iter()
        .filter(|e| e.index >= *from && e.index <= *to)
        .cloned()
        .collect();
    let entries = if entries.len() == (*to - *from + 1) as usize {
        // All entries found in memory
        recent_entries.retain(|e| e.index > *to);
        entries
    } else {
        // Fallback to disk
        recent_entries.retain(|e| e.index > *to);
        log_store.get_range(*from, *to).unwrap()
    };
    apply_tx.send(entries).await.ok();
    // ...
}
```

Keep `Action::ApplyEntries { from, to }` unchanged in core.rs.

**Step 5: Run all tests**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass

**Step 6: Commit**

```bash
git add src/raft/node.rs
git commit -m "perf: pass entries in-memory to apply pipeline, skip disk read"
```

---

### Task 5: Create PendingWriteBuffer for in-memory KV state

This is the core of the throughput optimization. Create a write buffer that holds KV mutations before they're flushed to redb. Range queries check the buffer first.

**Files:**
- Create: `src/kv/write_buffer.rs`
- Modify: `src/kv/mod.rs` — add `pub mod write_buffer;`
- Test: `tests/write_buffer_test.rs`

**Step 1: Write the failing test**

Create `tests/write_buffer_test.rs`:

```rust
use barkeeper::kv::write_buffer::WriteBuffer;
use barkeeper::proto::mvccpb::KeyValue;

#[test]
fn test_put_and_get() {
    let buf = WriteBuffer::new();
    let kv = KeyValue {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        lease: 0,
    };
    buf.put(kv.clone());
    let result = buf.get(b"foo");
    assert!(result.is_some());
    assert_eq!(result.unwrap().value, b"bar");
}

#[test]
fn test_delete() {
    let buf = WriteBuffer::new();
    let kv = KeyValue {
        key: b"foo".to_vec(),
        value: b"bar".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        lease: 0,
    };
    buf.put(kv);
    buf.delete(b"foo", 2);
    let result = buf.get(b"foo");
    // After delete, the key should be a tombstone
    assert!(result.is_none());
}

#[test]
fn test_drain() {
    let buf = WriteBuffer::new();
    for i in 0..5 {
        let kv = KeyValue {
            key: format!("key{}", i).into_bytes(),
            value: format!("val{}", i).into_bytes(),
            create_revision: (i + 1) as i64,
            mod_revision: (i + 1) as i64,
            version: 1,
            lease: 0,
        };
        buf.put(kv);
    }
    let entries = buf.drain();
    assert_eq!(entries.len(), 5);
    // After drain, buffer should be empty
    assert!(buf.get(b"key0").is_none());
}

#[test]
fn test_range_prefix() {
    let buf = WriteBuffer::new();
    for key in &["pfx/a", "pfx/b", "pfx/c", "other"] {
        let kv = KeyValue {
            key: key.as_bytes().to_vec(),
            value: b"v".to_vec(),
            create_revision: 1,
            mod_revision: 1,
            version: 1,
            lease: 0,
        };
        buf.put(kv);
    }
    let results = buf.range(b"pfx/", b"pfx0");
    assert_eq!(results.len(), 3);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test write_buffer_test 2>&1 | tail -10`
Expected: compilation error (module doesn't exist)

**Step 3: Implement WriteBuffer**

Create `src/kv/write_buffer.rs`:

```rust
use dashmap::DashMap;
use crate::proto::mvccpb::KeyValue;

/// Buffered KV entry — either a live value or a tombstone (deletion marker).
#[derive(Clone, Debug)]
enum BufferEntry {
    Live(KeyValue),
    Tombstone { key: Vec<u8>, mod_revision: i64 },
}

/// Thread-safe write buffer for KV mutations pending flush to redb.
///
/// Writes go here immediately after WAL commit. Range queries merge
/// buffer contents with redb results. A background timer drains the
/// buffer and flushes to redb periodically.
pub struct WriteBuffer {
    entries: DashMap<Vec<u8>, BufferEntry>,
}

impl WriteBuffer {
    pub fn new() -> Self {
        WriteBuffer {
            entries: DashMap::new(),
        }
    }

    /// Insert or update a key-value pair in the buffer.
    pub fn put(&self, kv: KeyValue) {
        self.entries.insert(kv.key.clone(), BufferEntry::Live(kv));
    }

    /// Mark a key as deleted in the buffer.
    pub fn delete(&self, key: &[u8], mod_revision: i64) {
        self.entries.insert(
            key.to_vec(),
            BufferEntry::Tombstone {
                key: key.to_vec(),
                mod_revision,
            },
        );
    }

    /// Get a single key from the buffer. Returns None if absent or tombstoned.
    pub fn get(&self, key: &[u8]) -> Option<KeyValue> {
        self.entries.get(key).and_then(|entry| match entry.value() {
            BufferEntry::Live(kv) => Some(kv.clone()),
            BufferEntry::Tombstone { .. } => None,
        })
    }

    /// Check if a key is tombstoned in the buffer.
    pub fn is_deleted(&self, key: &[u8]) -> bool {
        self.entries
            .get(key)
            .map(|entry| matches!(entry.value(), BufferEntry::Tombstone { .. }))
            .unwrap_or(false)
    }

    /// Get all live entries in a key range [start, end).
    /// Used to merge with redb range results.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<KeyValue> {
        let mut results = Vec::new();
        for entry in self.entries.iter() {
            let key = entry.key();
            if key.as_slice() >= start && (end.is_empty() || key.as_slice() < end) {
                if let BufferEntry::Live(kv) = entry.value() {
                    results.push(kv.clone());
                }
            }
        }
        results.sort_by(|a, b| a.key.cmp(&b.key));
        results
    }

    /// Drain all entries from the buffer. Returns the entries for flushing.
    pub fn drain(&self) -> Vec<BufferEntry> {
        let mut entries = Vec::new();
        // DashMap doesn't have drain, so we collect keys and remove
        let keys: Vec<Vec<u8>> = self.entries.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            if let Some((_, entry)) = self.entries.remove(&key) {
                entries.push(entry);
            }
        }
        entries
    }

    /// Number of entries in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
```

Add `pub mod write_buffer;` to `src/kv/mod.rs`.

**Step 4: Run test to verify it passes**

Run: `cargo test --test write_buffer_test 2>&1 | tail -10`
Expected: all 4 tests pass

**Step 5: Commit**

```bash
git add src/kv/write_buffer.rs src/kv/mod.rs tests/write_buffer_test.rs
git commit -m "feat: add PendingWriteBuffer for in-memory KV state"
```

---

### Task 6: Decouple KV fsync from write response path

This is the critical change. Instead of PUT waiting for the state machine to apply (which includes redb fsync), PUT responds after WAL commit. The state machine writes to the in-memory WriteBuffer immediately, and a background timer flushes to redb.

**Files:**
- Modify: `src/kv/state_machine.rs` — write to buffer before redb, send broker result after buffer write
- Modify: `src/api/gateway.rs:662-666` — remove broker.wait_for_result from PUT handler
- Modify: `src/api/kv_service.rs` — same for gRPC PUT
- Modify: `src/api/gateway.rs:578-617` — range queries merge buffer with redb
- Modify: `src/server.rs` or wherever state is wired up — create and pass WriteBuffer
- Test: `tests/compat_test.rs` (all existing tests must still pass)

**Step 1: Wire WriteBuffer into GatewayState and StateMachine**

Find where `GatewayState` is constructed (likely `src/server.rs` or `src/api/gateway.rs` `create_router`). Add `write_buffer: Arc<WriteBuffer>` field to GatewayState and pass it through.

Also pass it to `spawn_state_machine` so the state machine can write to it.

**Step 2: State machine writes to buffer first, then flushes to redb**

In `src/kv/state_machine.rs`, split the apply into two phases:

Phase 1 (fast, in-memory): For each command, compute the result and write to WriteBuffer. Send broker result immediately after this.

Phase 2 (background): The redb `batch_apply_with_index` happens after broker notification, so the client doesn't wait for it.

The implementation:

```rust
impl StateMachine {
    async fn apply(&self, entries: Vec<LogEntry>) {
        let last_applied = self.store.last_applied_raft_index().unwrap_or(0);
        let mut commands: Vec<(KvCommand, i64)> = Vec::new();
        let mut entry_indices = Vec::new();

        for entry in &entries {
            if entry.index <= last_applied {
                self.broker.send_result(entry.index, ApplyResult::Noop).await;
                continue;
            }
            match &entry.data {
                LogEntryData::Command { data, revision } => {
                    let cmd_result = bincode::deserialize::<KvCommand>(data)
                        .or_else(|_| {
                            serde_json::from_slice::<KvCommand>(data)
                                .map_err(|e| Box::new(bincode::ErrorKind::Custom(e.to_string())) as Box<bincode::ErrorKind>)
                        });
                    match cmd_result {
                        Ok(cmd) => {
                            commands.push((cmd, *revision));
                            entry_indices.push(entry.index);
                        }
                        Err(_) => {
                            self.broker.send_result(entry.index, ApplyResult::Noop).await;
                        }
                    }
                }
                _ => {
                    self.broker.send_result(entry.index, ApplyResult::Noop).await;
                }
            }
        }

        if commands.is_empty() {
            return;
        }

        let last_index = *entry_indices.last().unwrap();

        // Phase 1: Apply to write buffer (fast, in-memory) and notify broker
        for (i, (cmd, revision)) in commands.iter().enumerate() {
            let index = entry_indices[i];
            let apply_result = self.apply_to_buffer(cmd, *revision);
            // Fire watch notifications from buffer
            // ... (watch events need to be computed here too)
            self.broker.send_result(index, apply_result).await;
        }
        self.notifier.advance(last_index);

        // Phase 2: Flush to redb (background, client doesn't wait)
        let store = Arc::clone(&self.store);
        let cmds = commands;
        tokio::task::spawn_blocking(move || {
            if let Err(e) = store.batch_apply_with_index(&cmds, last_index) {
                tracing::error!(error = %e, "background redb flush failed");
            }
        });
    }
}
```

**IMPORTANT DESIGN DECISION**: The apply_to_buffer method needs to compute the same results (prev_kv, version, create_revision etc.) that batch_apply_with_index computes. This is complex for the full KV semantics.

**Simpler approach**: Keep `batch_apply_with_index` as the source of truth, but split it into two phases:
1. In-memory write transaction (compute results, populate WriteBuffer, DON'T commit/fsync)
2. Background commit/fsync

Actually, redb doesn't support this split — `txn.commit()` is atomic.

**Simplest correct approach**: The state machine does `batch_apply_with_index` as before (inside spawn_blocking), but we **split the broker notification**: send the result to the broker IMMEDIATELY after getting results from batch_apply, without waiting for the redb commit to complete. But this requires changing redb to support deferred commit...

**Actually, the most practical approach**: Keep the state machine as-is, but change the **response path** in the gateway. Instead of PUT waiting for `broker.wait_for_result(index)`, PUT responds as soon as the Raft proposal succeeds (after WAL fsync). The state machine continues to apply in the background. For reads, use ReadIndex (wait for apply to catch up) only when needed, but since most reads will be handled by the write buffer...

Wait — this was the original "early ack" approach that caused the mixed-read regression. The key difference this time: we have the WriteBuffer. Range queries check the buffer first, so they can see pending writes without waiting for apply.

**Final approach:**

1. PUT responds immediately after Raft proposal succeeds (WAL fsync done). No broker wait.
2. The state machine applies entries to both WriteBuffer (fast) and redb (slow fsync).
3. Range queries merge WriteBuffer with redb results.
4. The write buffer is populated by the state machine BEFORE the redb commit, so reads see pending data.

But there's a race: if a PUT response reaches the client and the client immediately does a GET, the state machine might not have populated the buffer yet (it's async via mpsc channel).

**Solution**: Populate the buffer from the Raft actor (in execute_actions), not from the state machine. When `AppendToLog` succeeds, we know the entries are committed (single-node). We can parse the KvCommand and write to the buffer right there, before the proposal response even sends.

Actually, for a single-node cluster, after `AppendToLog` + `advance_commit_index`, the entries ARE committed. We can populate the WriteBuffer at this point, in `execute_actions`, right when `ApplyEntries` fires — but BEFORE sending to the state machine. This guarantees the buffer is populated before the PUT response.

```
PUT → propose → AppendToLog (WAL fsync) → advance_commit →
    ApplyEntries: populate WriteBuffer + send to state machine →
    RespondToProposal → HTTP response

    (async) state machine: batch_apply_with_index in redb →
        on completion: drain matching entries from WriteBuffer
```

This way:
- PUT response fires after WriteBuffer is populated (no race)
- Reads check WriteBuffer first (sees uncommitted-to-redb data)
- State machine flushes to redb in background
- On flush completion, entries are removed from buffer

**Step 3: Implement in execute_actions**

In the `ApplyEntries` handler in `execute_actions`:
1. Parse each entry's KvCommand
2. For Put commands: create KeyValue and call `write_buffer.put()`
3. For DeleteRange: call `write_buffer.delete()` for affected keys
4. Send entries to state machine via apply_tx
5. Don't wait for state machine

In `RespondToProposal` handler: respond immediately (no broker wait needed).

In the PUT gateway handler: remove `broker.wait_for_result(index)`.

**Step 4: Update range queries to merge with buffer**

In `handle_range` (gateway.rs), after getting redb results, merge with WriteBuffer:
- For each key in redb results, check if buffer has a newer version (tombstone → remove, live → replace)
- Add any buffer entries in the range that aren't in redb results
- Re-sort and apply limit

**Step 5: Run all tests**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass

**Step 6: Commit**

```bash
git add src/raft/node.rs src/kv/state_machine.rs src/api/gateway.rs src/api/kv_service.rs src/kv/write_buffer.rs src/server.rs
git commit -m "perf: decouple KV fsync from write response path via WriteBuffer"
```

---

### Task 7: Background flush timer for WriteBuffer → redb

The state machine should drain the WriteBuffer periodically and confirm data is persisted to redb. After flush, remove entries from the buffer.

**Files:**
- Modify: `src/kv/state_machine.rs` — after batch_apply completes, drain flushed entries from buffer
- Test: `tests/compat_test.rs` (existing)

**Step 1: State machine drains buffer after redb flush**

After `batch_apply_with_index` succeeds in the spawn_blocking call, the state machine calls `write_buffer.remove_flushed(entry_indices)` to remove the specific entries that were just persisted.

The WriteBuffer needs a method to remove entries by their keys only if the mod_revision matches (to avoid removing a newer write that arrived while flushing):

```rust
pub fn remove_if_revision_le(&self, key: &[u8], max_revision: i64) {
    self.entries.remove_if(key, |_, entry| {
        match entry {
            BufferEntry::Live(kv) => kv.mod_revision <= max_revision,
            BufferEntry::Tombstone { mod_revision, .. } => *mod_revision <= max_revision,
        }
    });
}
```

**Step 2: Run all tests**

Run: `cargo test 2>&1 | tail -20`
Expected: all tests pass

**Step 3: Commit**

```bash
git add src/kv/state_machine.rs src/kv/write_buffer.rs
git commit -m "feat: state machine drains write buffer after redb flush"
```

---

### Task 8: Update DeleteRange and Txn handlers

DeleteRange and Txn handlers also need to be updated to not wait for broker results for the response path (or handle the write buffer differently).

**Files:**
- Modify: `src/api/gateway.rs` — handle_delete_range, handle_txn
- Modify: `src/api/kv_service.rs` — gRPC equivalents

**Note**: DeleteRange and Txn are more complex because the response includes data computed by the state machine (deleted count, prev_kvs, txn succeeded/failed). For these, we have two options:
1. Continue waiting for broker (simpler, these are less perf-critical)
2. Compute results from the buffer

For now, keep broker.wait_for_result for DeleteRange and Txn — only optimize PUT since that's the benchmark bottleneck.

**Step 1: Verify existing tests pass**

Run: `cargo test 2>&1 | tail -20`
Expected: all pass

**Step 2: Commit (if any changes needed)**

---

### Task 9: Run benchmarks and compare

**Files:**
- Modify: `bench/results/RESULTS.md` (auto-generated)
- Modify: `README.md` — update benchmark table

**Step 1: Build release binary**

Run: `cd barkeeper && cargo build --release 2>&1 | tail -5`

**Step 2: Run benchmark**

Run: `cd barkeeper && bench/harness/run.sh all --native 2>&1`

**Step 3: Update results**

Run: `cd barkeeper && python3 bench/harness/report.py`
Update README benchmark table with new numbers.

**Step 4: Commit**

```bash
git add bench/results/ README.md
git commit -m "bench: update results after write throughput optimization"
```

---

### Task 10: Verify all compat tests pass

Final verification that all 33 compat tests and the full test suite pass.

**Step 1: Run full test suite**

Run: `cargo test 2>&1 | tail -30`
Expected: all tests pass

**Step 2: Run compat tests specifically**

Run: `cargo test --test compat_test 2>&1`
Expected: 33/33 pass
