# Performance Optimization Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reach parity with or exceed etcd on single-node throughput and latency by implementing write batching, direct read access, bincode serialization, and a latest-key index.

**Architecture:** Batch multiple Raft proposals into single log appends and KV transactions (reducing 3N fsyncs to 2 per batch). Bypass the KV actor for both writes (state machine applies directly) and reads (gateway reads directly). Replace JSON with bincode on internal paths. Add a LATEST_TABLE index to eliminate O(N) range scans.

**Tech Stack:** Rust, redb, bincode, tokio

---

### Task 1: Add bincode dependency

**Files:**
- Modify: `Cargo.toml:33-37`

**Step 1: Add bincode to dependencies**

In `Cargo.toml`, add bincode under the Serialization section:

```toml
# Serialization
rmpv = "1"
rmp-serde = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
bincode = "1.3"
base64 = "0.22"
```

**Step 2: Verify it compiles**

Run: `cargo check`
Expected: success

**Step 3: Commit**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: add bincode dependency for internal serialization"
```

---

### Task 2: Add LATEST_TABLE and in-memory revision counter to KvStore

**Files:**
- Modify: `src/kv/store.rs:1-16` (add table definition)
- Modify: `src/kv/store.rs:261-330` (put method)
- Modify: `src/kv/store.rs:335-397` (range method)
- Modify: `src/kv/store.rs:715-736` (find_latest_kv_in_table)
- Test: `tests/kv_store_test.rs`

**Step 1: Write a failing test for LATEST_TABLE**

Add to `tests/kv_store_test.rs`:

```rust
#[test]
fn test_latest_table_point_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    // Put a key 10 times to create 10 revisions
    for _ in 0..10 {
        store.put(b"key1", b"val", 0).unwrap();
    }

    // Range should still be fast (uses LATEST_TABLE, not O(N) scan)
    let result = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].version, 10);
}
```

**Step 2: Run test to verify it passes (baseline)**

Run: `cargo test test_latest_table_point_lookup -- --nocapture`
Expected: PASS (same behavior, just verifying baseline before refactor)

**Step 3: Add LATEST_TABLE definition and AtomicI64 revision counter**

In `src/kv/store.rs`, add after line 16:

```rust
// Latest-key index: user_key → latest revision number (u64)
const LATEST_TABLE: TableDefinition<&[u8], u64> = TableDefinition::new("kv_latest");
```

Add to the `KvStore` struct (find the struct definition):

```rust
use std::sync::atomic::{AtomicI64, Ordering};

pub struct KvStore {
    db: Database,
    revision: AtomicI64,
}
```

Update `KvStore::open()` to initialize the revision counter from disk:

```rust
pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
    let db = Database::create(path.as_ref().join("kv.redb"))?;
    // Create all tables on first open.
    {
        let txn = db.begin_write()?;
        txn.open_table(KV_TABLE)?;
        txn.open_table(REV_TABLE)?;
        txn.open_table(META_TABLE)?;
        txn.open_table(LATEST_TABLE)?;
        txn.commit()?;
    }
    // Load current revision into memory.
    let revision = {
        let txn = db.begin_read()?;
        let meta = txn.open_table(META_TABLE)?;
        match meta.get("revision")? {
            Some(val) => serde_json::from_slice::<i64>(val.value()).unwrap_or(0),
            None => 0,
        }
    };
    Ok(Self {
        db,
        revision: AtomicI64::new(revision),
    })
}
```

**Step 4: Replace find_latest_kv_in_table with LATEST_TABLE lookup**

Replace `find_latest_kv_in_table` (lines 715-736) with:

```rust
fn find_latest_kv_in_table(
    &self,
    kv_table: &redb::Table<&[u8], &[u8]>,
    latest_table: &redb::Table<&[u8], u64>,
    key: &[u8],
    before_rev: u64,
) -> Result<Option<InternalKeyValue>, redb::Error> {
    // Point lookup in LATEST_TABLE.
    let latest_rev = match latest_table.get(key)? {
        Some(val) => val.value(),
        None => return Ok(None),
    };
    if latest_rev > before_rev {
        // Need older revision — fall back to range scan.
        return self.find_latest_kv_in_table_scan(kv_table, key, before_rev);
    }
    let compound = make_compound_key(key, latest_rev);
    match kv_table.get(compound.as_slice())? {
        Some(val) => {
            let ikv: InternalKeyValue = bincode::deserialize(val.value())
                .expect("deserialize kv");
            Ok(Some(ikv))
        }
        None => Ok(None),
    }
}

/// Fallback: scan for latest revision of key before a given revision.
fn find_latest_kv_in_table_scan(
    &self,
    table: &redb::Table<&[u8], &[u8]>,
    key: &[u8],
    before_rev: u64,
) -> Result<Option<InternalKeyValue>, redb::Error> {
    let start = make_compound_key(key, 0);
    let end = make_compound_key(key, before_rev);
    let mut latest: Option<InternalKeyValue> = None;
    for entry in table.range(start.as_slice()..=end.as_slice())? {
        let (ck, val) = entry?;
        if extract_user_key(ck.value()) == key {
            let ikv: InternalKeyValue = bincode::deserialize(val.value())
                .expect("deserialize kv");
            latest = Some(ikv);
        }
    }
    Ok(latest)
}
```

Do the same for `find_latest_kv_at_rev_readonly` — add a `LATEST_TABLE` read-only lookup path.

**Step 5: Update put() to use bincode, LATEST_TABLE, and in-memory revision**

Replace `put()` (lines 261-330):

```rust
pub fn put(
    &self,
    key: impl AsRef<[u8]>,
    value: impl AsRef<[u8]>,
    lease_id: i64,
) -> Result<PutResult, redb::Error> {
    let key = key.as_ref();
    let value = value.as_ref();

    let txn = self.db.begin_write()?;
    let result;
    {
        let mut kv_table = txn.open_table(KV_TABLE)?;
        let mut rev_table = txn.open_table(REV_TABLE)?;
        let mut meta_table = txn.open_table(META_TABLE)?;
        let mut latest_table = txn.open_table(LATEST_TABLE)?;

        // Increment revision atomically in memory.
        let new_rev = self.revision.fetch_add(1, Ordering::SeqCst) + 1;

        // Point lookup for previous value via LATEST_TABLE.
        let prev = self.find_latest_kv_in_table(&kv_table, &latest_table, key, new_rev as u64)?;

        let (create_revision, version) = match &prev {
            Some(prev_kv) if prev_kv.version > 0 => {
                (prev_kv.create_revision, prev_kv.version + 1)
            }
            _ => (new_rev, 1),
        };

        let ikv = InternalKeyValue {
            key: key.to_vec(),
            create_revision,
            mod_revision: new_rev,
            version,
            value: value.to_vec(),
            lease: lease_id,
        };

        let compound = make_compound_key(key, new_rev as u64);
        let serialized = bincode::serialize(&ikv).expect("serialize kv");
        kv_table.insert(compound.as_slice(), serialized.as_slice())?;

        // Update LATEST_TABLE.
        latest_table.insert(key, new_rev as u64)?;

        // Write revision entry (bincode).
        let rev_entries = vec![RevisionEntry {
            key: key.to_vec(),
            event_type: EventType::Put,
        }];
        let rev_bytes = bincode::serialize(&rev_entries).expect("serialize rev entry");
        rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

        // Persist revision to meta (for read queries and restart recovery).
        let rev_bytes = bincode::serialize(&new_rev).expect("serialize rev");
        meta_table.insert("revision", rev_bytes.as_slice())?;

        result = PutResult {
            revision: new_rev,
            prev_kv: prev.map(|p| p.to_proto()),
        };
    }
    txn.commit()?;
    Ok(result)
}
```

**Step 6: Update range() to use bincode deserialization and LATEST_TABLE**

Update `range()` to deserialize with bincode and use `LATEST_TABLE` for single-key lookups. Also update `find_latest_kv_at_rev_readonly` to try the index first.

**Step 7: Update ALL other store methods that serialize/deserialize InternalKeyValue and RevisionEntry**

Search for all occurrences of `serde_json::to_vec` and `serde_json::from_slice` in `store.rs` and replace with `bincode::serialize` / `bincode::deserialize`. This includes:
- `delete_range()`
- `txn()`
- `compact()`
- `changes_since()`
- `collect_unique_keys_in_range()`

Important: Also update `delete_range()` to update `LATEST_TABLE` when a key is deleted (set latest rev to the delete tombstone revision).

**Step 8: Handle migration — support reading old JSON-encoded data**

Add a helper that tries bincode first, falls back to JSON:

```rust
fn deserialize_kv(data: &[u8]) -> InternalKeyValue {
    bincode::deserialize(data)
        .or_else(|_| serde_json::from_slice(data).map_err(|e| e.into()))
        .expect("deserialize kv (bincode or json)")
}
```

Use this in all read paths so existing data doesn't break. New writes use bincode only.

**Step 9: Run all existing tests**

Run: `cargo test`
Expected: all 181+ tests pass

**Step 10: Commit**

```bash
git add src/kv/store.rs tests/kv_store_test.rs
git commit -m "perf: add LATEST_TABLE index, bincode serialization, in-memory revision counter"
```

---

### Task 3: Batch write support in KvStore

**Files:**
- Modify: `src/kv/store.rs`
- Test: `tests/kv_store_test.rs`

**Step 1: Write a failing test for batch puts**

Add to `tests/kv_store_test.rs`:

```rust
#[test]
fn test_batch_put() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    let ops: Vec<_> = (0..100).map(|i| {
        (format!("key{:04}", i).into_bytes(), format!("val{}", i).into_bytes(), 0i64)
    }).collect();

    let results = store.batch_put(&ops).unwrap();
    assert_eq!(results.len(), 100);
    // Revisions should be sequential.
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r.revision, (i as i64) + 1);
    }
    // All keys readable.
    let r = store.range(b"key0000", b"key9999", 0, 0).unwrap();
    assert_eq!(r.kvs.len(), 100);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test test_batch_put`
Expected: FAIL — `batch_put` doesn't exist yet

**Step 3: Implement batch_put**

Add to `KvStore`:

```rust
/// Apply multiple puts in a single redb transaction (1 fsync for N ops).
pub fn batch_put(
    &self,
    ops: &[(Vec<u8>, Vec<u8>, i64)], // (key, value, lease_id)
) -> Result<Vec<PutResult>, redb::Error> {
    let txn = self.db.begin_write()?;
    let mut results = Vec::with_capacity(ops.len());
    {
        let mut kv_table = txn.open_table(KV_TABLE)?;
        let mut rev_table = txn.open_table(REV_TABLE)?;
        let mut meta_table = txn.open_table(META_TABLE)?;
        let mut latest_table = txn.open_table(LATEST_TABLE)?;

        for (key, value, lease_id) in ops {
            let new_rev = self.revision.fetch_add(1, Ordering::SeqCst) + 1;

            let prev = self.find_latest_kv_in_table(
                &kv_table, &latest_table, key, new_rev as u64
            )?;

            let (create_revision, version) = match &prev {
                Some(prev_kv) if prev_kv.version > 0 => {
                    (prev_kv.create_revision, prev_kv.version + 1)
                }
                _ => (new_rev, 1),
            };

            let ikv = InternalKeyValue {
                key: key.to_vec(),
                create_revision,
                mod_revision: new_rev,
                version,
                value: value.to_vec(),
                lease: *lease_id,
            };

            let compound = make_compound_key(key, new_rev as u64);
            let serialized = bincode::serialize(&ikv).expect("serialize kv");
            kv_table.insert(compound.as_slice(), serialized.as_slice())?;
            latest_table.insert(key.as_slice(), new_rev as u64)?;

            let rev_entries = vec![RevisionEntry {
                key: key.to_vec(),
                event_type: EventType::Put,
            }];
            let rev_bytes = bincode::serialize(&rev_entries).expect("serialize rev");
            rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

            results.push(PutResult {
                revision: new_rev,
                prev_kv: prev.map(|p| p.to_proto()),
            });
        }

        // Persist final revision once.
        let final_rev = self.revision.load(Ordering::SeqCst);
        let rev_bytes = bincode::serialize(&final_rev).expect("serialize rev");
        meta_table.insert("revision", rev_bytes.as_slice())?;
    }
    txn.commit()?; // single fsync for all ops
    Ok(results)
}
```

**Step 4: Also add batch_apply for mixed operations**

```rust
/// Apply a batch of mixed KvCommands in a single redb transaction.
/// Returns a Vec of ApplyResult in the same order as input.
pub fn batch_apply(
    &self,
    commands: &[KvCommand],
) -> Result<Vec<BatchApplyResult>, redb::Error> {
    // Similar to batch_put but handles Put, DeleteRange, Compact.
    // Single begin_write(), N operations, single commit().
    // ... (implement all command types inline)
}
```

Where `BatchApplyResult` contains the PutResult/DeleteResult/TxnResult plus the prev/deleted KVs needed for watch notifications.

**Step 5: Run tests**

Run: `cargo test`
Expected: PASS

**Step 6: Commit**

```bash
git add src/kv/store.rs tests/kv_store_test.rs
git commit -m "perf: add batch_put and batch_apply for single-transaction multi-op writes"
```

---

### Task 4: Rewrite state machine to use direct store access and batch apply

**Files:**
- Modify: `src/kv/state_machine.rs`
- Modify: `src/api/server.rs` (pass `Arc<KvStore>` to state machine)
- Test: existing integration tests

**Step 1: Update StateMachine to hold Arc<KvStore> directly**

Replace the `KvStoreActorHandle` with a direct store reference:

```rust
use super::store::KvStore;

struct StateMachine {
    store: Arc<KvStore>,  // direct access, not through actor
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
}
```

**Step 2: Rewrite apply() to batch all entries in one store transaction**

```rust
async fn apply(&self, entries: Vec<LogEntry>) {
    let last_applied = self.store.raft_applied_index()
        .unwrap_or(0);  // direct call, no actor

    // Collect commands to batch.
    let mut commands = Vec::new();
    let mut entry_indices = Vec::new();

    for entry in &entries {
        if entry.index <= last_applied {
            self.broker.send_result(entry.index, ApplyResult::Noop).await;
            continue;
        }
        match &entry.data {
            LogEntryData::Command(data) => {
                if let Ok(cmd) = bincode::deserialize::<KvCommand>(data) {
                    commands.push(cmd);
                    entry_indices.push(entry.index);
                } else {
                    self.broker.send_result(entry.index, ApplyResult::Noop).await;
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

    // Apply all commands in a single redb transaction.
    let results = tokio::task::spawn_blocking({
        let store = Arc::clone(&self.store);
        let cmds = commands.clone();
        let last_index = *entry_indices.last().unwrap();
        move || store.batch_apply_with_index(&cmds, last_index)
    }).await.expect("batch apply panicked");

    match results {
        Ok(batch_results) => {
            // Send results to broker and notify watchers.
            for (i, result) in batch_results.into_iter().enumerate() {
                let index = entry_indices[i];
                let apply_result = self.notify_and_convert(
                    &commands[i], result
                ).await;
                self.broker.send_result(index, apply_result).await;
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "batch apply failed");
            for index in &entry_indices {
                self.broker.send_result(*index, ApplyResult::Noop).await;
            }
        }
    }
}
```

**Step 3: Add batch_apply_with_index to KvStore**

This is `batch_apply` but also persists the `last_applied_raft_index` in the same transaction — collapsing what was previously 3N fsyncs into 1:

```rust
pub fn batch_apply_with_index(
    &self,
    commands: &[KvCommand],
    last_applied_index: u64,
) -> Result<Vec<BatchApplyResult>, redb::Error> {
    let txn = self.db.begin_write()?;
    let mut results = Vec::with_capacity(commands.len());
    {
        let mut kv_table = txn.open_table(KV_TABLE)?;
        let mut rev_table = txn.open_table(REV_TABLE)?;
        let mut meta_table = txn.open_table(META_TABLE)?;
        let mut latest_table = txn.open_table(LATEST_TABLE)?;

        for cmd in commands {
            // apply each command using tables directly
            // (put, delete_range, txn, compact)
            // push result to results vec
        }

        // Persist final revision + applied index once.
        let final_rev = self.revision.load(Ordering::SeqCst);
        meta_table.insert("revision", bincode::serialize(&final_rev)?.as_slice())?;
        meta_table.insert("raft_applied_index",
            bincode::serialize(&last_applied_index)?.as_slice())?;
    }
    txn.commit()?; // SINGLE fsync for entire batch
    Ok(results)
}
```

**Step 4: Update spawn_state_machine signature**

```rust
pub async fn spawn_state_machine(
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
    store: Arc<KvStore>,  // direct store, not actor handle
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
)
```

**Step 5: Update server.rs to pass Arc<KvStore>**

In `src/api/server.rs`, where the state machine is spawned, pass `Arc::clone(&store)` instead of the actor handle.

**Step 6: Run tests**

Run: `cargo test`
Expected: PASS

**Step 7: Commit**

```bash
git add src/kv/state_machine.rs src/kv/store.rs src/api/server.rs
git commit -m "perf: state machine batch applies entries in single redb transaction"
```

---

### Task 5: Raft proposal batching

**Files:**
- Modify: `src/raft/node.rs:165-195` (select loop)
- Test: existing raft tests + integration tests

**Step 1: Add batch collection to Raft node select loop**

Replace the single-proposal handling (lines 174-181) with a drain loop:

```rust
// Client proposals — drain all available
Some(proposal) = proposal_rx.recv() => {
    // Collect this proposal + drain any others waiting.
    let mut proposals = vec![proposal];
    while proposals.len() < 64 {
        match proposal_rx.try_recv() {
            Ok(p) => proposals.push(p),
            Err(_) => break,
        }
    }

    // Step all proposals through Raft core, collecting actions.
    let mut all_actions = Vec::new();
    for p in proposals {
        pending_responses.insert(p.id, p.response_tx);
        let actions = core.step(Event::Proposal { id: p.id, data: p.data });
        all_actions.extend(actions);
    }

    // Deduplicate AppendToLog actions — merge into one.
    let merged = merge_log_actions(all_actions);
    execute_actions(&merged, &log_store, &apply_tx, &mut pending_responses, &mut heartbeat_timer, &config, &applied_ref).await;
    term_ref.store(core.state.persistent.current_term, Ordering::Relaxed);
    leader_ref.store(core.state.leader_id.unwrap_or(0), Ordering::Relaxed);
}
```

**Step 2: Implement merge_log_actions**

```rust
fn merge_log_actions(actions: Vec<Action>) -> Vec<Action> {
    let mut merged_entries = Vec::new();
    let mut other_actions = Vec::new();

    for action in actions {
        match action {
            Action::AppendToLog(entries) => merged_entries.extend(entries),
            other => other_actions.push(other),
        }
    }

    let mut result = Vec::new();
    if !merged_entries.is_empty() {
        result.push(Action::AppendToLog(merged_entries));
    }
    result.extend(other_actions);
    result
}
```

This turns N separate `log_store.append()` calls (N fsyncs) into 1.

**Step 3: Do the same for spawn_raft_node_rebar**

Apply the same batching pattern to the Rebar-based raft node at line 282+.

**Step 4: Run tests**

Run: `cargo test`
Expected: PASS

**Step 5: Commit**

```bash
git add src/raft/node.rs
git commit -m "perf: batch Raft proposals — drain up to 64 per tick, single log append"
```

---

### Task 6: Replace JSON with bincode for Raft proposal serialization

**Files:**
- Modify: `src/api/kv_service.rs:103` (serialize proposal)
- Modify: `src/api/gateway.rs:606-610` (serialize proposal)
- Modify: `src/kv/state_machine.rs:63` (deserialize proposal)

**Step 1: Update kv_service.rs — serialize with bincode**

Replace all `serde_json::to_vec(&cmd)` calls with `bincode::serialize(&cmd)`:

```rust
// kv_service.rs, in put():
let cmd = KvCommand::Put { key, value, lease_id };
let data = bincode::serialize(&cmd)
    .map_err(|e| Status::internal(format!("serialize: {}", e)))?;
```

Same for delete_range, txn, compact handlers.

**Step 2: Update gateway.rs — serialize with bincode**

Same pattern in `handle_put`, `handle_delete_range`, `handle_txn`, `handle_compaction`.

**Step 3: Update state_machine.rs — already done in Task 4**

The state machine already deserializes with bincode from Task 4.

**Step 4: Handle migration — support old JSON entries in Raft log**

In the state machine, try bincode first, fall back to JSON:

```rust
let cmd = bincode::deserialize::<KvCommand>(&data)
    .or_else(|_| serde_json::from_slice::<KvCommand>(&data)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>));
```

This handles replaying old Raft log entries that were JSON-encoded.

**Step 5: Run tests**

Run: `cargo test`
Expected: PASS

**Step 6: Commit**

```bash
git add src/api/kv_service.rs src/api/gateway.rs src/kv/state_machine.rs
git commit -m "perf: use bincode for Raft proposal serialization (4x less overhead)"
```

---

### Task 7: Direct read path — bypass actor for range queries

**Files:**
- Modify: `src/api/gateway.rs:569-594` (handle_range)
- Modify: `src/api/server.rs` (pass `Arc<KvStore>` to gateway)
- Test: existing integration tests

**Step 1: Add Arc<KvStore> to GatewayState**

In `gateway.rs`, add a direct store reference to the shared state:

```rust
pub struct GatewayState {
    pub store_direct: Arc<KvStore>,  // for direct reads
    pub store: KvStoreActorHandle,   // keep for lease/auth operations
    // ... rest of fields
}
```

**Step 2: Update handle_range to use direct store access**

```rust
async fn handle_range(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<RangeRequest>,
) -> impl IntoResponse {
    let store = Arc::clone(&state.store_direct);
    let key = base64_decode(&req.key);
    let range_end = req.range_end.as_deref().map(base64_decode).unwrap_or_default();
    let limit = req.limit.unwrap_or(0);
    let revision = req.revision.unwrap_or(0);

    let result = tokio::task::spawn_blocking(move || {
        store.range(&key, &range_end, limit, revision)
    }).await.expect("range spawn_blocking");

    // ... format response
}
```

This runs range queries directly on tokio's blocking thread pool — multiple reads execute in parallel, no actor bottleneck.

**Step 3: Update server.rs to pass Arc<KvStore>**

Pass `Arc::clone(&store)` when constructing `GatewayState`.

**Step 4: Run tests**

Run: `cargo test`
Expected: PASS

**Step 5: Commit**

```bash
git add src/api/gateway.rs src/api/server.rs
git commit -m "perf: direct store access for range queries — bypass actor, parallel reads"
```

---

### Task 8: Run benchmark and verify improvements

**Files:**
- None (benchmark only)

**Step 1: Build release Docker image**

Run: `docker build -t barkeeper:local .`

**Step 2: Run benchmark**

```bash
export PATH="$HOME/.cargo/bin:$PATH"
cd bench && bash harness/run.sh all
```

**Step 3: Compare results**

Read `bench/results/RESULTS.md` and compare against baseline:
- Write c=100: was 91 req/s, target >5,000 req/s
- Read c=100: was 6,937 req/s, target >50,000 req/s
- Connection scaling c=1000: was 9,035 req/s, target >60,000 req/s

**Step 4: If targets not met, profile with criterion**

Run: `cargo bench --bench kv_bench`

Identify remaining hotspots and iterate.

**Step 5: Commit benchmark results**

```bash
git add bench/results/RESULTS.md
git commit -m "bench: updated results after performance optimization"
```

---

## Execution Order

Tasks 1-7 are sequential — each builds on the previous. Task 8 validates the full stack.

| Task | What | Key Metric |
|------|------|------------|
| 1 | Add bincode dep | Prerequisite |
| 2 | LATEST_TABLE + in-memory revision + bincode in store | Eliminate O(N) scan, 4x less serialization |
| 3 | batch_put / batch_apply | Single transaction for N ops |
| 4 | State machine batch apply with direct store | 3N fsyncs → 1 per batch |
| 5 | Raft proposal batching | N log appends → 1 per batch |
| 6 | Bincode for Raft proposals | Remove JSON from hot path |
| 7 | Direct read path | Parallel reads, no actor bottleneck |
| 8 | Benchmark | Validate targets |
