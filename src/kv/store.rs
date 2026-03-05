use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::proto::mvccpb;
use super::state_machine::KvCommand;

// ── Error type ──────────────────────────────────────────────────────────────

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

// ── Internal serde-friendly KV representation ───────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InternalKeyValue {
    key: Vec<u8>,
    create_revision: i64,
    mod_revision: i64,
    version: i64,
    value: Vec<u8>,
    lease: i64,
}

impl InternalKeyValue {
    fn to_proto(&self) -> mvccpb::KeyValue {
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

/// Event type stored in the revision table.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum EventType {
    Put,
    Delete,
}

// ── Revision entry ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RevisionEntry {
    key: Vec<u8>,
    event_type: EventType,
}

// ── Public result types ─────────────────────────────────────────────────────

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

#[derive(Debug, Clone)]
pub struct DeleteResult {
    pub revision: i64,
    pub deleted: i64,
    pub prev_kvs: Vec<mvccpb::KeyValue>,
}

// ── Txn types ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnCompareTarget {
    Version(i64),
    CreateRevision(i64),
    ModRevision(i64),
    Value(Vec<u8>),
    Lease(i64),
}

/// Compare result (comparison operator) for Txn conditions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnCompareResult {
    Equal,
    Greater,
    Less,
    NotEqual,
}

/// A single compare condition in a Txn.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxnCompare {
    pub key: Vec<u8>,
    pub range_end: Vec<u8>,
    pub target: TxnCompareTarget,
    pub result: TxnCompareResult,
}

/// An operation in a Txn (success or failure branch).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TxnOp {
    Range {
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
    },
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
    },
    DeleteRange {
        key: Vec<u8>,
        range_end: Vec<u8>,
    },
}

/// Result of a single operation in a Txn.
#[derive(Debug)]
pub enum TxnOpResponse {
    Range(RangeResult),
    Put(PutResult),
    DeleteRange(DeleteResult),
}

/// Result of an entire Txn.
#[derive(Debug)]
pub struct TxnResult {
    pub succeeded: bool,
    pub responses: Vec<TxnOpResponse>,
    pub revision: i64,
}

// ── Batch apply types ──────────────────────────────────────────────────────

/// Result of a single operation within a batch apply.
pub struct BatchOpResult {
    pub apply_result: ApplyResultData,
    pub watch_events: Vec<WatchEvent>,
}

pub enum ApplyResultData {
    Put(PutResult),
    DeleteRange(DeleteResult),
    Txn(TxnResult),
    Compact { revision: i64 },
    Noop,
}

pub struct WatchEvent {
    pub key: Vec<u8>,
    pub event_type: i32, // 0=put, 1=delete
    pub kv: crate::proto::mvccpb::KeyValue,
    pub prev_kv: Option<crate::proto::mvccpb::KeyValue>,
}

// ── Compound key helpers ────────────────────────────────────────────────────

fn make_compound_key(key: &[u8], revision: u64) -> Vec<u8> {
    let mut compound = Vec::with_capacity(key.len() + 1 + 8);
    compound.extend_from_slice(key);
    compound.push(0x00);
    compound.extend_from_slice(&revision.to_be_bytes());
    compound
}

fn extract_user_key(compound: &[u8]) -> &[u8] {
    &compound[..compound.len() - 9]
}

fn extract_revision(compound: &[u8]) -> u64 {
    let start = compound.len() - 8;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&compound[start..]);
    u64::from_be_bytes(buf)
}

// ── Snapshot format ─────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct Snapshot {
    kv: Vec<(Vec<u8>, InternalKeyValue)>,
    revisions: Vec<(u64, Vec<RevisionEntry>)>,
    latest: Vec<(Vec<u8>, u64)>,
    revision: i64,
    raft_applied_index: u64,
}

// ── KvInner ─────────────────────────────────────────────────────────────────

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

// ── KvStore ─────────────────────────────────────────────────────────────────

/// MVCC key-value store backed by in-memory BTreeMaps.
///
/// Every mutation creates a new revision. Keys are stored with compound keys
/// that encode the user key and revision for lexicographic ordering.
/// Durability is provided by the WAL — this store is a materialized view.
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<RwLock<KvInner>>,
    revision_atomic: Arc<AtomicI64>,
    db_path: PathBuf,
}

impl KvStore {
    /// Open or create a KvStore at the given directory path.
    /// Loads from snapshot if available, otherwise starts empty.
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

    /// Get the database file size in bytes (snapshot size, or 0 if no snapshot).
    pub fn db_file_size(&self) -> Result<i64, std::io::Error> {
        let snapshot_path = self.db_path.join("kv.snapshot");
        match std::fs::metadata(&snapshot_path) {
            Ok(m) => Ok(m.len() as i64),
            Err(_) => Ok(0),
        }
    }

    /// Read the snapshot file into memory for sending to followers.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, std::io::Error> {
        // Generate a fresh snapshot and return its bytes
        let inner = self.inner.read().unwrap();
        let snap = Snapshot {
            kv: inner.kv.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            revisions: inner.revisions.iter().map(|(k, v)| (*k, v.clone())).collect(),
            latest: inner.latest.iter().map(|(k, v)| (k.clone(), *v)).collect(),
            revision: inner.revision,
            raft_applied_index: inner.raft_applied_index,
        };
        bincode::serialize(&snap)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    }

    /// No-op for in-memory store.
    pub fn compact_db(&self) -> Result<bool, StoreError> {
        Ok(true)
    }

    /// Get the current revision number.
    pub fn current_revision(&self) -> Result<i64, StoreError> {
        Ok(self.revision_atomic.load(Ordering::SeqCst))
    }

    /// Get the last applied Raft log index.
    pub fn last_applied_raft_index(&self) -> Result<u64, StoreError> {
        let inner = self.inner.read().unwrap();
        Ok(inner.raft_applied_index)
    }

    /// Set the last applied Raft log index.
    pub fn set_last_applied_raft_index(&self, index: u64) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        inner.raft_applied_index = index;
        Ok(())
    }

    /// Put a key-value pair into the store. Returns the new revision and the
    /// previous value (if any).
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
        inner.revisions.insert(new_rev as u64, vec![RevisionEntry {
            key: key.to_vec(), event_type: EventType::Put,
        }]);

        Ok(PutResult { revision: new_rev, prev_kv: prev.map(|p| p.to_proto()) })
    }

    /// Range query. If `range_end` is empty, returns a single key.
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
            let kv = find_latest_kv_at_rev(&inner, key, query_rev as u64);
            match kv {
                Some(ikv) if ikv.version > 0 => Ok(RangeResult {
                    kvs: vec![ikv.to_proto()], count: 1, more: false,
                }),
                _ => Ok(RangeResult { kvs: vec![], count: 0, more: false }),
            }
        } else {
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

    /// Delete a key or range of keys. Returns the number deleted and prev values.
    pub fn delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<DeleteResult, StoreError> {
        let mut inner = self.inner.write().unwrap();
        let new_rev = inner.revision + 1;
        inner.revision = new_rev;
        self.revision_atomic.store(new_rev, Ordering::SeqCst);

        do_delete_range(&mut inner, key, range_end, new_rev)
    }

    /// Execute a transaction with compare-and-swap semantics.
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
                    inner.revisions.insert(new_rev as u64, vec![RevisionEntry {
                        key: key.clone(), event_type: EventType::Put,
                    }]);
                    responses.push(TxnOpResponse::Put(PutResult {
                        revision: new_rev, prev_kv: prev.map(|p| p.to_proto()),
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
                                responses.push(TxnOpResponse::Range(RangeResult {
                                    kvs: vec![ikv.to_proto()], count: 1, more: false,
                                }));
                            }
                            _ => {
                                responses.push(TxnOpResponse::Range(RangeResult {
                                    kvs: vec![], count: 0, more: false,
                                }));
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
                        let more = if *limit > 0 && total_count > *limit {
                            kvs.truncate(*limit as usize);
                            true
                        } else {
                            false
                        };
                        responses.push(TxnOpResponse::Range(RangeResult { count: total_count, kvs, more }));
                    }
                }
            }
        }

        let rev = inner.revision;
        Ok(TxnResult { succeeded, responses, revision: rev })
    }

    /// Compact revisions up to and including the given revision.
    pub fn compact(&self, revision: i64) -> Result<(), StoreError> {
        let mut inner = self.inner.write().unwrap();
        do_compact(&mut inner, revision);
        Ok(())
    }

    /// Return watch events (key, event_type, kv) since `after_revision`.
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

    /// Apply a batch of commands atomically within a single write lock.
    /// Updates `raft_applied_index` at the end.
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

    /// Return the db_path for compatibility.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

// ── Free functions for operations on KvInner ────────────────────────────────

fn find_latest_kv(inner: &KvInner, key: &[u8], max_rev: u64) -> Option<InternalKeyValue> {
    if let Some(&latest_rev) = inner.latest.get(key) {
        if latest_rev <= max_rev {
            let compound = make_compound_key(key, latest_rev);
            return inner.kv.get(&compound).cloned();
        }
    }
    find_latest_kv_by_scan(inner, key, max_rev)
}

fn find_latest_kv_at_rev(inner: &KvInner, key: &[u8], rev: u64) -> Option<InternalKeyValue> {
    if let Some(&latest_rev) = inner.latest.get(key) {
        if latest_rev <= rev {
            let compound = make_compound_key(key, latest_rev);
            return inner.kv.get(&compound).cloned();
        }
    }
    find_latest_kv_by_scan(inner, key, rev)
}

fn find_latest_kv_by_scan(inner: &KvInner, key: &[u8], max_rev: u64) -> Option<InternalKeyValue> {
    let start = make_compound_key(key, 0);
    let end = make_compound_key(key, max_rev + 1);
    inner.kv.range(start..end).next_back().map(|(_, v)| v.clone())
}

fn collect_unique_keys_in_range(inner: &KvInner, start: &[u8], end: &[u8], max_rev: u64) -> Vec<Vec<u8>> {
    use std::collections::BTreeSet;
    let range_start = make_compound_key(start, 0);
    let range_end = if end == b"\x00" {
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

fn do_delete_range(inner: &mut KvInner, key: &[u8], range_end: &[u8], new_rev: i64) -> Result<DeleteResult, StoreError> {
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
    let mut rev_entries = Vec::new();
    for k in &keys_to_delete {
        let prev = find_latest_kv(inner, k, new_rev as u64);
        if let Some(p) = &prev {
            prev_kvs.push(p.to_proto());
        }
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

    Ok(DeleteResult { deleted: keys_to_delete.len() as i64, prev_kvs, revision: new_rev })
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
    inner.revisions.insert(new_rev as u64, vec![RevisionEntry {
        key: key.to_vec(), event_type: EventType::Put,
    }]);

    let notify_kv = mvccpb::KeyValue {
        key: key.to_vec(), create_revision, mod_revision: new_rev,
        version, value: value.to_vec(), lease: lease_id,
    };
    let watch_event = WatchEvent {
        key: key.to_vec(), event_type: 0, kv: notify_kv,
        prev_kv: prev.as_ref().map(|p| p.to_proto()),
    };

    Ok(BatchOpResult {
        apply_result: ApplyResultData::Put(PutResult {
            revision: new_rev, prev_kv: prev.map(|p| p.to_proto()),
        }),
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
        apply_result: ApplyResultData::DeleteRange(DeleteResult {
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
                            responses.push(TxnOpResponse::Range(RangeResult {
                                kvs: vec![ikv.to_proto()], count: 1, more: false,
                            }));
                        }
                        _ => {
                            responses.push(TxnOpResponse::Range(RangeResult {
                                kvs: vec![], count: 0, more: false,
                            }));
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
                    let more = if *limit > 0 && total_count > *limit {
                        kvs.truncate(*limit as usize);
                        true
                    } else {
                        false
                    };
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

        let cmp_result = match &compare.target {
            TxnCompareTarget::Version(v) => {
                let actual_v = actual.map(|kv| kv.version).unwrap_or(0);
                actual_v.cmp(v)
            }
            TxnCompareTarget::CreateRevision(v) => {
                let actual_v = actual.map(|kv| kv.create_revision).unwrap_or(0);
                actual_v.cmp(v)
            }
            TxnCompareTarget::ModRevision(v) => {
                let actual_v = actual.map(|kv| kv.mod_revision).unwrap_or(0);
                actual_v.cmp(v)
            }
            TxnCompareTarget::Value(v) => {
                let actual_v = actual.map(|kv| kv.value.as_slice()).unwrap_or(&[]);
                actual_v.cmp(v.as_slice())
            }
            TxnCompareTarget::Lease(v) => {
                let actual_v = actual.map(|kv| kv.lease).unwrap_or(0);
                actual_v.cmp(v)
            }
        };

        let success = match compare.result {
            TxnCompareResult::Equal => cmp_result == std::cmp::Ordering::Equal,
            TxnCompareResult::Greater => cmp_result == std::cmp::Ordering::Greater,
            TxnCompareResult::Less => cmp_result == std::cmp::Ordering::Less,
            TxnCompareResult::NotEqual => cmp_result != std::cmp::Ordering::Equal,
        };

        if !success {
            return false;
        }
    }
    true
}
