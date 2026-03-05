# In-Memory KV Store + Apply Batching Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace redb KV state machine with in-memory BTreeMaps to eliminate 38% CPU overhead, and merge per-proposal ApplyEntries actions into single batched applies to reduce per-entry overhead. Together these should dramatically increase write throughput at high concurrency.

**Architecture:** The KV store becomes four in-memory data structures (BTreeMap for kv/revisions, HashMap for latest/meta) behind an RwLock. Durability is guaranteed by the existing WAL — the KV store is a materialized view that can be reconstructed from WAL replay. Periodic snapshots to disk reduce startup replay time. The `merge_log_actions` function is extended to also merge consecutive `ApplyEntries` actions into a single range.

**Tech Stack:** Rust std BTreeMap/HashMap, bincode serialization for snapshots, existing WAL for durability

---

### Task 1: Merge ApplyEntries Actions in merge_log_actions

This is the smallest, most isolated change. Currently `merge_log_actions` only merges `AppendToLog` — after 100 proposals you still get 100 individual `ApplyEntries { from: N, to: N }` actions. Merging them into one `ApplyEntries { from: first, to: last }` eliminates per-entry overhead in the actor loop.

**Files:**
- Modify: `src/raft/node.rs:1188-1203`
- Test: `tests/log_store_test.rs` (existing tests still pass)

**Step 1: Update merge_log_actions to also merge ApplyEntries**

In `src/raft/node.rs`, replace the `merge_log_actions` function (lines 1188-1203) with:

```rust
fn merge_log_actions(actions: Vec<Action>) -> Vec<Action> {
    let mut merged_entries = Vec::new();
    let mut apply_from: Option<u64> = None;
    let mut apply_to: u64 = 0;
    let mut other_actions = Vec::new();
    for action in actions {
        match action {
            Action::AppendToLog(entries) => merged_entries.extend(entries),
            Action::ApplyEntries { from, to } => {
                if apply_from.is_none() {
                    apply_from = Some(from);
                }
                apply_to = to;
            }
            other => other_actions.push(other),
        }
    }
    let mut result = Vec::new();
    if !merged_entries.is_empty() {
        result.push(Action::AppendToLog(merged_entries));
    }
    if let Some(from) = apply_from {
        result.push(Action::ApplyEntries { from, to: apply_to });
    }
    result.extend(other_actions);
    result
}
```

**Step 2: Verify existing tests pass**

Run: `cd /home/alexandernicholson/.pxycrab/workspace/barkeeper && source ~/.cargo/env && cargo test 2>&1 | tail -10`
Expected: All tests pass

**Step 3: Commit**

```bash
cd /home/alexandernicholson/.pxycrab/workspace/barkeeper
git add src/raft/node.rs
git commit -m "perf: merge ApplyEntries actions in group commit batching"
```

---

### Task 2: Rewrite KvStore as In-Memory BTreeMap Store

Replace the redb-backed `KvStore` with in-memory data structures. The public API stays identical — same method signatures, same return types. The error type changes from `redb::Error` to a new `StoreError` enum (wrapping `io::Error` for snapshot operations and `bincode::Error` for serialization).

**Files:**
- Rewrite: `src/kv/store.rs`

**Step 1: Replace the entire file**

Replace `src/kv/store.rs` with the in-memory implementation. Key design:

- `KvInner` struct holds: `kv: BTreeMap<Vec<u8>, InternalKeyValue>`, `revisions: BTreeMap<u64, Vec<RevisionEntry>>`, `latest: HashMap<Vec<u8>, u64>`, `revision: i64`, `raft_applied_index: u64`
- `KvStore` wraps `Arc<RwLock<KvInner>>` + `db_path: PathBuf`
- All existing public methods (`open`, `put`, `range`, `delete_range`, `changes_since`, `compact`, `batch_apply_with_index`, `current_revision`, `last_applied_raft_index`, etc.) keep their exact signatures but operate on in-memory maps
- `open()` loads from snapshot file if it exists (`kv.snapshot` in the data directory), otherwise starts empty
- New `snapshot()` method serializes the entire `KvInner` to `kv.snapshot.tmp` + atomic rename
- Compound key format stays the same: `user_key ++ 0x00 ++ 8-byte BE revision`
- `range()` uses BTreeMap range operations for both point lookups and range queries
- `compact()` removes old compound key entries from the BTreeMap + prunes revisions

```rust
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::proto::mvccpb;
use super::state_machine::KvCommand;

/// Error type for KvStore operations.
#[derive(Debug)]
pub enum StoreError {
    Io(io::Error),
    Serialization(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Io(e) => write!(f, "io error: {}", e),
            StoreError::Serialization(e) => write!(f, "serialization error: {}", e),
        }
    }
}

impl std::error::Error for StoreError {}

impl From<io::Error> for StoreError {
    fn from(e: io::Error) -> Self {
        StoreError::Io(e)
    }
}

impl From<Box<bincode::ErrorKind>> for StoreError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        StoreError::Serialization(e.to_string())
    }
}

// For backward compat with callers that use redb::Error
impl From<redb::Error> for StoreError {
    fn from(e: redb::Error) -> Self {
        StoreError::Io(io::Error::new(io::ErrorKind::Other, e.to_string()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InternalKeyValue {
    pub key: Vec<u8>,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub version: i64,
    pub value: Vec<u8>,
    pub lease: i64,
}

impl InternalKeyValue {
    pub fn to_proto(&self) -> mvccpb::KeyValue {
        mvccpb::KeyValue {
            key: self.key.clone(),
            create_revision: self.create_revision,
            mod_revision: self.mod_revision,
            version: self.version,
            value: self.value.clone(),
            lease: self.lease,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevisionEntry {
    pub key: Vec<u8>,
    pub event_type: EventType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct PutResult {
    pub revision: i64,
    pub prev_kv: Option<mvccpb::KeyValue>,
}

#[derive(Debug)]
pub struct RangeResult {
    pub kvs: Vec<mvccpb::KeyValue>,
    pub count: i64,
    pub more: bool,
}

#[derive(Debug)]
pub struct DeleteRangeResult {
    pub deleted: i64,
    pub prev_kvs: Vec<mvccpb::KeyValue>,
    pub revision: i64,
}

#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub key: Vec<u8>,
    pub event_type: i32,
    pub kv: mvccpb::KeyValue,
    pub prev_kv: Option<mvccpb::KeyValue>,
}

#[derive(Debug, Clone)]
pub struct TxnCompare {
    pub key: Vec<u8>,
    pub target: i32,
    pub result: i32,
    pub target_union: Option<TxnCompareTarget>,
}

#[derive(Debug, Clone)]
pub enum TxnCompareTarget {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

#[derive(Debug, Clone)]
pub enum TxnOp {
    Put { key: Vec<u8>, value: Vec<u8>, lease_id: i64 },
    DeleteRange { key: Vec<u8>, range_end: Vec<u8> },
    Range { key: Vec<u8>, range_end: Vec<u8>, limit: i64, revision: i64 },
}

#[derive(Debug)]
pub enum TxnOpResponse {
    Put(PutResult),
    DeleteRange(DeleteRangeResult),
    Range(RangeResult),
}

#[derive(Debug)]
pub struct TxnResult {
    pub succeeded: bool,
    pub responses: Vec<TxnOpResponse>,
    pub revision: i64,
}

#[derive(Debug, Clone)]
pub enum ApplyResultData {
    Put(PutResult),
    DeleteRange(DeleteRangeResult),
    Txn(TxnResult),
    Compact { revision: i64 },
}

#[derive(Debug)]
pub struct BatchOpResult {
    pub apply_result: ApplyResultData,
    pub watch_events: Vec<WatchEvent>,
}

// --- Snapshot format ---

#[derive(Serialize, Deserialize)]
struct Snapshot {
    kv: Vec<(Vec<u8>, InternalKeyValue)>,
    revisions: Vec<(u64, Vec<RevisionEntry>)>,
    latest: Vec<(Vec<u8>, u64)>,
    revision: i64,
    raft_applied_index: u64,
}

struct KvInner {
    /// Compound key (user_key + 0x00 + 8-byte BE revision) → InternalKeyValue
    kv: BTreeMap<Vec<u8>, InternalKeyValue>,
    /// Revision → list of (key, event_type) pairs
    revisions: BTreeMap<u64, Vec<RevisionEntry>>,
    /// User key → latest revision number
    latest: HashMap<Vec<u8>, u64>,
    /// Current global revision
    revision: i64,
    /// Last applied Raft log index
    raft_applied_index: u64,
}

#[derive(Clone)]
pub struct KvStore {
    inner: Arc<RwLock<KvInner>>,
    revision_atomic: Arc<AtomicI64>,
    db_path: PathBuf,
}

fn make_compound_key(key: &[u8], revision: u64) -> Vec<u8> {
    let mut ck = Vec::with_capacity(key.len() + 1 + 8);
    ck.extend_from_slice(key);
    ck.push(0x00);
    ck.extend_from_slice(&revision.to_be_bytes());
    ck
}

fn extract_user_key(compound: &[u8]) -> &[u8] {
    // Find last 0x00 separator (9 bytes from the end: 0x00 + 8 bytes revision)
    if compound.len() < 9 {
        return compound;
    }
    &compound[..compound.len() - 9]
}

fn extract_revision(compound: &[u8]) -> u64 {
    if compound.len() < 8 {
        return 0;
    }
    let rev_bytes = &compound[compound.len() - 8..];
    u64::from_be_bytes(rev_bytes.try_into().unwrap())
}

impl KvStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let db_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&db_path).ok();

        let snapshot_path = db_path.join("kv.snapshot");
        let inner = if snapshot_path.exists() {
            let bytes = std::fs::read(&snapshot_path)?;
            let snap: Snapshot = bincode::deserialize(&bytes)?;
            KvInner {
                kv: snap.kv.into_iter().collect(),
                revisions: snap.revisions.into_iter().collect(),
                latest: snap.latest.into_iter().collect(),
                revision: snap.revision,
                raft_applied_index: snap.raft_applied_index,
            }
        } else {
            KvInner {
                kv: BTreeMap::new(),
                revisions: BTreeMap::new(),
                latest: HashMap::new(),
                revision: 0,
                raft_applied_index: 0,
            }
        };

        let rev = inner.revision;
        Ok(KvStore {
            inner: Arc::new(RwLock::new(inner)),
            revision_atomic: Arc::new(AtomicI64::new(rev)),
            db_path,
        })
    }

    /// For backward compatibility — open with a file path (ignores the filename, uses parent dir).
    /// New callers should use open() with a directory path.
    pub fn open_legacy(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let p = path.as_ref();
        let dir = p.parent().unwrap_or(p);
        Self::open(dir)
    }

    pub fn current_revision(&self) -> i64 {
        self.revision_atomic.load(Ordering::SeqCst)
    }

    pub fn last_applied_raft_index(&self) -> Result<u64, StoreError> {
        let inner = self.inner.read().unwrap();
        Ok(inner.raft_applied_index)
    }

    pub fn set_last_applied_raft_index(&self, index: u64) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        inner.raft_applied_index = index;
        Ok(())
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        lease_id: i64,
    ) -> Result<PutResult, StoreError> {
        let key = key.as_ref();
        let value = value.as_ref();
        let mut inner = self.inner.write().unwrap();

        let new_rev = inner.revision + 1;
        inner.revision = new_rev;
        self.revision_atomic.store(new_rev, Ordering::SeqCst);

        let prev = find_latest_kv(&inner, key, new_rev as u64);
        let (create_revision, version) = match &prev {
            Some(p) if p.version > 0 => (p.create_revision, p.version + 1),
            _ => (new_rev, 1),
        };

        let ikv = InternalKeyValue {
            key: key.to_vec(), create_revision, mod_revision: new_rev,
            version, value: value.to_vec(), lease: lease_id,
        };
        let compound = make_compound_key(key, new_rev as u64);
        inner.kv.insert(compound, ikv);
        inner.latest.insert(key.to_vec(), new_rev as u64);

        let rev_entries = vec![RevisionEntry { key: key.to_vec(), event_type: EventType::Put }];
        inner.revisions.insert(new_rev as u64, rev_entries);

        Ok(PutResult { revision: new_rev, prev_kv: prev.map(|p| p.to_proto()) })
    }

    pub fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
    ) -> Result<RangeResult, StoreError> {
        let inner = self.inner.read().unwrap();
        let max_rev = inner.revision;
        let query_rev = if revision > 0 { revision } else { max_rev };

        if range_end.is_empty() {
            // Single key lookup
            let kv = find_latest_kv_at_rev(&inner, key, query_rev as u64);
            match kv {
                Some(ikv) if ikv.version > 0 => Ok(RangeResult { kvs: vec![ikv.to_proto()], count: 1, more: false }),
                _ => Ok(RangeResult { kvs: vec![], count: 0, more: false }),
            }
        } else {
            // Range query
            let all_keys = collect_unique_keys_in_range(&inner, key, range_end, query_rev as u64);
            let mut kvs = Vec::new();
            for user_key in &all_keys {
                if let Some(ikv) = find_latest_kv_at_rev(&inner, user_key, query_rev as u64) {
                    if ikv.version > 0 {
                        kvs.push(ikv.to_proto());
                    }
                }
            }
            let total_count = kvs.len() as i64;
            let more = if limit > 0 && total_count > limit {
                kvs.truncate(limit as usize);
                true
            } else {
                false
            };
            Ok(RangeResult { count: total_count, kvs, more })
        }
    }

    pub fn delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<DeleteRangeResult, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let new_rev = inner.revision + 1;
        inner.revision = new_rev;
        self.revision_atomic.store(new_rev, Ordering::SeqCst);

        do_delete_range(&mut inner, key, range_end, new_rev)
    }

    pub fn changes_since(
        &self,
        after_revision: i64,
    ) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue)>, StoreError> {
        let inner = self.inner.read().unwrap();
        let start_rev = (after_revision + 1) as u64;
        let mut results = Vec::new();

        for (&rev, entries) in inner.revisions.range(start_rev..) {
            for re in entries {
                let event_type = match re.event_type {
                    EventType::Put => 0,
                    EventType::Delete => 1,
                };
                let compound = make_compound_key(&re.key, rev);
                let kv = match inner.kv.get(&compound) {
                    Some(ikv) => ikv.to_proto(),
                    None => mvccpb::KeyValue {
                        key: re.key.clone(),
                        mod_revision: rev as i64,
                        ..Default::default()
                    },
                };
                results.push((re.key.clone(), event_type, kv));
            }
        }
        Ok(results)
    }

    pub fn compact(&self, revision: i64) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        do_compact(&mut inner, revision);
        Ok(())
    }

    pub fn compact_db(&self) -> Result<(), StoreError> {
        // No-op for in-memory store
        Ok(())
    }

    pub fn txn(
        &self,
        compares: &[TxnCompare],
        success: &[TxnOp],
        failure: &[TxnOp],
    ) -> Result<TxnResult, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let succeeded = evaluate_compares(&inner, compares);
        let ops = if succeeded { success } else { failure };
        let mut responses = Vec::new();

        for op in ops {
            match op {
                TxnOp::Put { key, value, lease_id } => {
                    let new_rev = inner.revision + 1;
                    inner.revision = new_rev;
                    self.revision_atomic.store(new_rev, Ordering::SeqCst);

                    let prev = find_latest_kv(&inner, key, new_rev as u64);
                    let (create_revision, version) = match &prev {
                        Some(p) if p.version > 0 => (p.create_revision, p.version + 1),
                        _ => (new_rev, 1),
                    };
                    let ikv = InternalKeyValue {
                        key: key.clone(), create_revision, mod_revision: new_rev,
                        version, value: value.clone(), lease: *lease_id,
                    };
                    let compound = make_compound_key(key, new_rev as u64);
                    inner.kv.insert(compound, ikv);
                    inner.latest.insert(key.clone(), new_rev as u64);
                    inner.revisions.insert(new_rev as u64, vec![RevisionEntry { key: key.clone(), event_type: EventType::Put }]);

                    responses.push(TxnOpResponse::Put(PutResult {
                        revision: new_rev,
                        prev_kv: prev.map(|p| p.to_proto()),
                    }));
                }
                TxnOp::DeleteRange { key, range_end } => {
                    let new_rev = inner.revision + 1;
                    inner.revision = new_rev;
                    self.revision_atomic.store(new_rev, Ordering::SeqCst);
                    let result = do_delete_range(&mut inner, key, range_end, new_rev)?;
                    responses.push(TxnOpResponse::DeleteRange(result));
                }
                TxnOp::Range { key, range_end, limit, revision } => {
                    let max_rev = inner.revision;
                    let query_rev = if *revision > 0 { *revision } else { max_rev };
                    if range_end.is_empty() {
                        let kv = find_latest_kv_at_rev(&inner, key, query_rev as u64);
                        match kv {
                            Some(ikv) if ikv.version > 0 => {
                                responses.push(TxnOpResponse::Range(RangeResult { kvs: vec![ikv.to_proto()], count: 1, more: false }));
                            }
                            _ => {
                                responses.push(TxnOpResponse::Range(RangeResult { kvs: vec![], count: 0, more: false }));
                            }
                        }
                    } else {
                        let all_keys = collect_unique_keys_in_range(&inner, key, range_end, query_rev as u64);
                        let mut kvs = Vec::new();
                        for user_key in &all_keys {
                            if let Some(ikv) = find_latest_kv_at_rev(&inner, user_key, query_rev as u64) {
                                if ikv.version > 0 { kvs.push(ikv.to_proto()); }
                            }
                        }
                        let total_count = kvs.len() as i64;
                        let more = if *limit > 0 && total_count > *limit { kvs.truncate(*limit as usize); true } else { false };
                        responses.push(TxnOpResponse::Range(RangeResult { count: total_count, kvs, more }));
                    }
                }
            }
        }

        let rev = inner.revision;
        Ok(TxnResult { succeeded, responses, revision: rev })
    }

    pub fn batch_apply_with_index(
        &self,
        commands: &[(KvCommand, i64)],
        last_applied_index: u64,
    ) -> Result<Vec<BatchOpResult>, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let mut results = Vec::with_capacity(commands.len());

        for (cmd, preassigned_rev) in commands {
            let result = match cmd {
                KvCommand::Put { key, value, lease_id } => {
                    batch_put(&mut inner, &self.revision_atomic, key, value, *lease_id, *preassigned_rev)?
                }
                KvCommand::DeleteRange { key, range_end } => {
                    batch_delete_range(&mut inner, &self.revision_atomic, key, range_end, *preassigned_rev)?
                }
                KvCommand::Txn { compares, success, failure } => {
                    batch_txn(&mut inner, &self.revision_atomic, compares, success, failure, *preassigned_rev)?
                }
                KvCommand::Compact { revision } => {
                    do_compact(&mut inner, *revision);
                    let rev = inner.revision;
                    BatchOpResult {
                        apply_result: ApplyResultData::Compact { revision: rev },
                        watch_events: vec![],
                    }
                }
            };
            results.push(result);
        }

        inner.raft_applied_index = last_applied_index;
        Ok(results)
    }

    /// Serialize the entire store to a snapshot file (atomic via tmp + rename).
    pub fn snapshot(&self) -> Result<(), StoreError> {
        let inner = self.inner.read().unwrap();
        let snap = Snapshot {
            kv: inner.kv.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            revisions: inner.revisions.iter().map(|(k, v)| (*k, v.clone())).collect(),
            latest: inner.latest.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            revision: inner.revision,
            raft_applied_index: inner.raft_applied_index,
        };
        drop(inner);

        let bytes = bincode::serialize(&snap).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let tmp_path = self.db_path.join("kv.snapshot.tmp");
        let snap_path = self.db_path.join("kv.snapshot");
        std::fs::write(&tmp_path, &bytes)?;
        std::fs::rename(&tmp_path, &snap_path)?;
        Ok(())
    }

    /// Return the db_path for compatibility with code that references it.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

// --- Free functions for operations on KvInner ---

fn find_latest_kv(inner: &KvInner, key: &[u8], max_rev: u64) -> Option<InternalKeyValue> {
    // Fast path: check latest index
    if let Some(&latest_rev) = inner.latest.get(key) {
        if latest_rev <= max_rev {
            let compound = make_compound_key(key, latest_rev);
            return inner.kv.get(&compound).cloned();
        }
    }
    // Slow path: scan compound keys for this user key
    find_latest_kv_by_scan(inner, key, max_rev)
}

fn find_latest_kv_at_rev(inner: &KvInner, key: &[u8], rev: u64) -> Option<InternalKeyValue> {
    // Check latest index first — if latest revision <= query revision, use it
    if let Some(&latest_rev) = inner.latest.get(key) {
        if latest_rev <= rev {
            let compound = make_compound_key(key, latest_rev);
            return inner.kv.get(&compound).cloned();
        }
    }
    // Historical query: scan for the latest compound key <= rev
    find_latest_kv_by_scan(inner, key, rev)
}

fn find_latest_kv_by_scan(inner: &KvInner, key: &[u8], max_rev: u64) -> Option<InternalKeyValue> {
    let start = make_compound_key(key, 0);
    let end = make_compound_key(key, max_rev + 1);
    // Iterate compound keys in range, take the last one (highest revision <= max_rev)
    inner.kv.range(start..end).next_back().map(|(_, v)| v.clone())
}

fn collect_unique_keys_in_range(inner: &KvInner, start: &[u8], end: &[u8], max_rev: u64) -> Vec<Vec<u8>> {
    use std::collections::BTreeSet;
    let range_start = make_compound_key(start, 0);
    let range_end = if end == b"\x00" {
        // \x00 means "all keys" prefix scan
        Vec::new()
    } else {
        make_compound_key(end, 0)
    };

    let mut unique_keys = BTreeSet::new();
    let iter: Box<dyn Iterator<Item = _>> = if range_end.is_empty() {
        Box::new(inner.kv.range(range_start..))
    } else {
        Box::new(inner.kv.range(range_start..range_end))
    };

    for (ck, _) in iter {
        let rev = extract_revision(ck);
        if rev <= max_rev {
            let user_key = extract_user_key(ck).to_vec();
            unique_keys.insert(user_key);
        }
    }
    unique_keys.into_iter().collect()
}

fn do_delete_range(inner: &mut KvInner, key: &[u8], range_end: &[u8], new_rev: i64) -> Result<DeleteRangeResult, StoreError> {
    let keys_to_delete = if range_end.is_empty() {
        // Single key
        if find_latest_kv(inner, key, new_rev as u64).map_or(false, |kv| kv.version > 0) {
            vec![key.to_vec()]
        } else {
            vec![]
        }
    } else {
        collect_unique_keys_in_range(inner, key, range_end, new_rev as u64)
            .into_iter()
            .filter(|k| find_latest_kv(inner, k, new_rev as u64).map_or(false, |kv| kv.version > 0))
            .collect()
    };

    let mut prev_kvs = Vec::new();
    let mut rev_entries = Vec::new();
    for k in &keys_to_delete {
        let prev = find_latest_kv(inner, k, new_rev as u64);
        if let Some(p) = &prev {
            prev_kvs.push(p.to_proto());
        }
        // Write tombstone
        let ikv = InternalKeyValue {
            key: k.clone(), create_revision: 0, mod_revision: new_rev,
            version: 0, value: vec![], lease: 0,
        };
        let compound = make_compound_key(k, new_rev as u64);
        inner.kv.insert(compound, ikv);
        inner.latest.insert(k.clone(), new_rev as u64);
        rev_entries.push(RevisionEntry { key: k.clone(), event_type: EventType::Delete });
    }
    if !rev_entries.is_empty() {
        inner.revisions.insert(new_rev as u64, rev_entries);
    }

    Ok(DeleteRangeResult { deleted: keys_to_delete.len() as i64, prev_kvs, revision: new_rev })
}

fn do_compact(inner: &mut KvInner, revision: i64) {
    let mut keys_to_remove = Vec::new();
    let mut latest_per_key: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();

    for (ck, _) in inner.kv.iter() {
        let rev = extract_revision(ck);
        let user_key = extract_user_key(ck).to_vec();

        if rev <= revision as u64 {
            match latest_per_key.get(&user_key) {
                Some((existing_rev, existing_ck)) => {
                    if rev > *existing_rev {
                        keys_to_remove.push(existing_ck.clone());
                        latest_per_key.insert(user_key, (rev, ck.clone()));
                    } else {
                        keys_to_remove.push(ck.clone());
                    }
                }
                None => {
                    latest_per_key.insert(user_key, (rev, ck.clone()));
                }
            }
        }
    }

    for ck in &keys_to_remove {
        inner.kv.remove(ck);
    }

    let revs_to_remove: Vec<u64> = inner.revisions.range(..revision as u64).map(|(&k, _)| k).collect();
    for rev in revs_to_remove {
        inner.revisions.remove(&rev);
    }
}

fn batch_put(
    inner: &mut KvInner,
    revision_atomic: &AtomicI64,
    key: &[u8],
    value: &[u8],
    lease_id: i64,
    preassigned_rev: i64,
) -> Result<BatchOpResult, StoreError> {
    let new_rev = if preassigned_rev > 0 {
        inner.revision = preassigned_rev;
        revision_atomic.store(preassigned_rev, Ordering::SeqCst);
        preassigned_rev
    } else {
        let r = inner.revision + 1;
        inner.revision = r;
        revision_atomic.store(r, Ordering::SeqCst);
        r
    };

    let prev = find_latest_kv(inner, key, new_rev as u64);
    let (create_revision, version) = match &prev {
        Some(p) if p.version > 0 => (p.create_revision, p.version + 1),
        _ => (new_rev, 1),
    };

    let ikv = InternalKeyValue {
        key: key.to_vec(), create_revision, mod_revision: new_rev,
        version, value: value.to_vec(), lease: lease_id,
    };
    let compound = make_compound_key(key, new_rev as u64);
    inner.kv.insert(compound, ikv);
    inner.latest.insert(key.to_vec(), new_rev as u64);
    inner.revisions.insert(new_rev as u64, vec![RevisionEntry { key: key.to_vec(), event_type: EventType::Put }]);

    let notify_kv = mvccpb::KeyValue {
        key: key.to_vec(), create_revision, mod_revision: new_rev,
        version, value: value.to_vec(), lease: lease_id,
    };
    let watch_event = WatchEvent {
        key: key.to_vec(), event_type: 0, kv: notify_kv,
        prev_kv: prev.as_ref().map(|p| p.to_proto()),
    };

    Ok(BatchOpResult {
        apply_result: ApplyResultData::Put(PutResult { revision: new_rev, prev_kv: prev.map(|p| p.to_proto()) }),
        watch_events: vec![watch_event],
    })
}

fn batch_delete_range(
    inner: &mut KvInner,
    revision_atomic: &AtomicI64,
    key: &[u8],
    range_end: &[u8],
    preassigned_rev: i64,
) -> Result<BatchOpResult, StoreError> {
    let new_rev = if preassigned_rev > 0 {
        inner.revision = preassigned_rev;
        revision_atomic.store(preassigned_rev, Ordering::SeqCst);
        preassigned_rev
    } else {
        let r = inner.revision + 1;
        inner.revision = r;
        revision_atomic.store(r, Ordering::SeqCst);
        r
    };

    let keys_to_delete = if range_end.is_empty() {
        if find_latest_kv(inner, key, new_rev as u64).map_or(false, |kv| kv.version > 0) {
            vec![key.to_vec()]
        } else {
            vec![]
        }
    } else {
        collect_unique_keys_in_range(inner, key, range_end, new_rev as u64)
            .into_iter()
            .filter(|k| find_latest_kv(inner, k, new_rev as u64).map_or(false, |kv| kv.version > 0))
            .collect()
    };

    let mut prev_kvs = Vec::new();
    let mut watch_events = Vec::new();
    let mut rev_entries = Vec::new();

    for k in &keys_to_delete {
        let prev = find_latest_kv(inner, k, new_rev as u64);
        let tombstone = InternalKeyValue {
            key: k.clone(), create_revision: 0, mod_revision: new_rev,
            version: 0, value: vec![], lease: 0,
        };
        let compound = make_compound_key(k, new_rev as u64);
        inner.kv.insert(compound, tombstone);
        inner.latest.insert(k.clone(), new_rev as u64);
        rev_entries.push(RevisionEntry { key: k.clone(), event_type: EventType::Delete });

        let notify_kv = mvccpb::KeyValue {
            key: k.clone(), mod_revision: new_rev, ..Default::default()
        };
        watch_events.push(WatchEvent {
            key: k.clone(), event_type: 1, kv: notify_kv,
            prev_kv: prev.as_ref().map(|p| p.to_proto()),
        });
        if let Some(p) = prev { prev_kvs.push(p.to_proto()); }
    }

    if !rev_entries.is_empty() {
        inner.revisions.insert(new_rev as u64, rev_entries);
    }

    Ok(BatchOpResult {
        apply_result: ApplyResultData::DeleteRange(DeleteRangeResult {
            deleted: keys_to_delete.len() as i64, prev_kvs, revision: new_rev,
        }),
        watch_events,
    })
}

fn batch_txn(
    inner: &mut KvInner,
    revision_atomic: &AtomicI64,
    compares: &[TxnCompare],
    success: &[TxnOp],
    failure: &[TxnOp],
    preassigned_rev: i64,
) -> Result<BatchOpResult, StoreError> {
    let succeeded = evaluate_compares(inner, compares);
    let ops = if succeeded { success } else { failure };

    if preassigned_rev > 0 {
        // Set revision to preassigned_rev - 1 so sub-ops auto-increment from there
        inner.revision = preassigned_rev - 1;
        revision_atomic.store(preassigned_rev - 1, Ordering::SeqCst);
    }

    let mut responses = Vec::new();
    let mut all_watch_events = Vec::new();

    for op in ops {
        match op {
            TxnOp::Put { key, value, lease_id } => {
                let result = batch_put(inner, revision_atomic, key, value, *lease_id, 0)?;
                all_watch_events.extend(result.watch_events);
                responses.push(TxnOpResponse::Put(match result.apply_result {
                    ApplyResultData::Put(r) => r,
                    _ => unreachable!(),
                }));
            }
            TxnOp::DeleteRange { key, range_end } => {
                let result = batch_delete_range(inner, revision_atomic, key, range_end, 0)?;
                all_watch_events.extend(result.watch_events);
                responses.push(TxnOpResponse::DeleteRange(match result.apply_result {
                    ApplyResultData::DeleteRange(r) => r,
                    _ => unreachable!(),
                }));
            }
            TxnOp::Range { key, range_end, limit, revision } => {
                let max_rev = inner.revision;
                let query_rev = if *revision > 0 { *revision } else { max_rev };
                if range_end.is_empty() {
                    let kv = find_latest_kv_at_rev(inner, key, query_rev as u64);
                    match kv {
                        Some(ikv) if ikv.version > 0 => {
                            responses.push(TxnOpResponse::Range(RangeResult { kvs: vec![ikv.to_proto()], count: 1, more: false }));
                        }
                        _ => {
                            responses.push(TxnOpResponse::Range(RangeResult { kvs: vec![], count: 0, more: false }));
                        }
                    }
                } else {
                    let all_keys = collect_unique_keys_in_range(inner, key, range_end, query_rev as u64);
                    let mut kvs = Vec::new();
                    for user_key in &all_keys {
                        if let Some(ikv) = find_latest_kv_at_rev(inner, user_key, query_rev as u64) {
                            if ikv.version > 0 { kvs.push(ikv.to_proto()); }
                        }
                    }
                    let total_count = kvs.len() as i64;
                    let more = if *limit > 0 && total_count > *limit { kvs.truncate(*limit as usize); true } else { false };
                    responses.push(TxnOpResponse::Range(RangeResult { count: total_count, kvs, more }));
                }
            }
        }
    }

    let rev = inner.revision;
    Ok(BatchOpResult {
        apply_result: ApplyResultData::Txn(TxnResult { succeeded, responses, revision: rev }),
        watch_events: all_watch_events,
    })
}

fn evaluate_compares(inner: &KvInner, compares: &[TxnCompare]) -> bool {
    for compare in compares {
        let kv = find_latest_kv(inner, &compare.key, inner.revision as u64);
        let actual = kv.as_ref();

        let cmp_result = match &compare.target_union {
            Some(TxnCompareTarget::Version(v)) => {
                let actual_v = actual.map(|kv| kv.version).unwrap_or(0);
                actual_v.cmp(v)
            }
            Some(TxnCompareTarget::CreateRevision(v)) => {
                let actual_v = actual.map(|kv| kv.create_revision).unwrap_or(0);
                actual_v.cmp(v)
            }
            Some(TxnCompareTarget::ModRevision(v)) => {
                let actual_v = actual.map(|kv| kv.mod_revision).unwrap_or(0);
                actual_v.cmp(v)
            }
            Some(TxnCompareTarget::Value(v)) => {
                let actual_v = actual.map(|kv| kv.value.as_slice()).unwrap_or(&[]);
                actual_v.cmp(v.as_slice())
            }
            Some(TxnCompareTarget::Lease(v)) => {
                let actual_v = actual.map(|kv| kv.lease).unwrap_or(0);
                actual_v.cmp(v)
            }
            None => std::cmp::Ordering::Equal,
        };

        let success = match compare.result {
            0 => cmp_result == std::cmp::Ordering::Equal,    // EQUAL
            1 => cmp_result == std::cmp::Ordering::Greater,  // GREATER
            2 => cmp_result == std::cmp::Ordering::Less,     // LESS
            3 => cmp_result != std::cmp::Ordering::Equal,    // NOT_EQUAL
            _ => false,
        };

        if !success {
            return false;
        }
    }
    true
}
```

**Step 2: Verify it compiles**

Run: `cd /home/alexandernicholson/.pxycrab/workspace/barkeeper && source ~/.cargo/env && cargo check 2>&1 | head -30`
Expected: Compilation errors in callers referencing `redb::Error` — the KvStore module itself should compile.

**Step 3: Commit**

```bash
cd /home/alexandernicholson/.pxycrab/workspace/barkeeper
git add src/kv/store.rs
git commit -m "feat: replace redb KvStore with in-memory BTreeMap store"
```

---

### Task 3: Update Callers to Use StoreError

After Task 2, callers that reference `redb::Error` will fail to compile. Update them.

**Files:**
- Modify: `src/kv/state_machine.rs` — change error types from `redb::Error` to `StoreError`
- Modify: `src/kv/actor.rs` — same
- Modify: `src/api/gateway.rs` — any `redb::Error` references
- Modify: `src/api/server.rs` — KvStore::open path changes
- Modify: `Cargo.toml` — remove `redb` from dependencies (or keep if still used elsewhere)
- Modify: any other files that fail `cargo check`

**Step 1: Run cargo check and fix each error**

Run: `cd /home/alexandernicholson/.pxycrab/workspace/barkeeper && source ~/.cargo/env && cargo check 2>&1`

For each compilation error:
- Replace `redb::Error` with `store::StoreError` or `Box<dyn std::error::Error>`
- Update `KvStore::open()` calls — the path should be a directory now
- The `batch_apply_with_index` return type changes from `Result<Vec<BatchOpResult>, redb::Error>` to `Result<Vec<BatchOpResult>, StoreError>`
- The state machine's `spawn_blocking` call just needs the error type adjusted

**Step 2: Verify compilation**

Run: `cargo check`
Expected: Clean compilation (warnings OK)

**Step 3: Commit**

```bash
git add -u
git commit -m "refactor: update callers for in-memory KvStore (StoreError)"
```

---

### Task 4: Update Tests

12 test files reference KvStore. Most use `KvStore::open(tempdir)` which should still work. The main changes:
- Error type assertions (if any reference `redb::Error`)
- Any test that checks on-disk redb files directly

**Step 1: Run the full test suite and fix failures**

Run: `cd /home/alexandernicholson/.pxycrab/workspace/barkeeper && source ~/.cargo/env && cargo test 2>&1 | tail -40`

Fix each failing test:
- Update `KvStore::open()` paths if needed
- Replace any `redb::Error` type annotations
- Tests that verify on-disk persistence need adjustment (in-memory store persists via snapshots, not redb files)

**Step 2: Verify all tests pass**

Run: `cargo test`
Expected: All tests pass

**Step 3: Commit**

```bash
git add -u
git commit -m "test: update tests for in-memory KvStore"
```

---

### Task 5: Remove Tracing Spans and Run Benchmarks

**Files:**
- Modify: `src/raft/node.rs` — remove profiling spans
- Modify: `src/api/gateway.rs` — remove profiling spans
- Modify: `src/main.rs` — revert tracing subscriber to simple `fmt::init()`
- Modify: `bench/results/RESULTS.md` — updated by benchmark
- Modify: `README.md` — update benchmark table

**Step 1: Remove all profiling instrumentation**

Remove all `_gc_t0`, `_put_t0`, `_apply_t0`, `wal_t0`, `resp_t0`, `t0` timing variables and their associated `tracing::info!` calls from:
- `src/api/gateway.rs` (handle_put timing)
- `src/raft/node.rs` (group_commit, wal_flush, apply_entries, send_responses, raft_propose, raft_step timings)

Revert `src/main.rs` tracing subscriber back to:
```rust
tracing_subscriber::fmt::init();
```

**Step 2: Run benchmarks**

Run: `cd /home/alexandernicholson/.pxycrab/workspace/barkeeper && source ~/.cargo/env && bash bench/harness/run.sh barkeeper --native 2>&1`

**Step 3: Update README.md with new numbers**

**Step 4: Commit**

```bash
git add src/raft/node.rs src/api/gateway.rs src/main.rs bench/results/ README.md
git commit -m "bench: remove profiling spans, update results after in-memory KV store"
```
