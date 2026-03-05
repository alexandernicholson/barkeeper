use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::proto::mvccpb;

// ── redb table definitions ──────────────────────────────────────────────────
// KV table: compound_key (key_bytes + \x00 + 8-byte BE revision) → serialized InternalKeyValue
const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

// Revision table: revision (u64) → serialized list of (key, event_type) pairs
const REV_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("revisions");

// Meta table: string key → bytes value
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("kv_meta");

// Latest table: key → latest revision (index for O(1) lookups)
const LATEST_TABLE: TableDefinition<&[u8], u64> = TableDefinition::new("kv_latest");

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

/// One entry in the revision table: which key was affected and how.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RevisionEntry {
    key: Vec<u8>,
    event_type: EventType,
}

// ── Deserialization helpers (bincode-first with JSON fallback for migration) ─

fn deserialize_kv(data: &[u8]) -> InternalKeyValue {
    bincode::deserialize(data)
        .unwrap_or_else(|_| serde_json::from_slice(data).expect("deserialize kv (json fallback)"))
}

fn deserialize_rev_entries(data: &[u8]) -> Vec<RevisionEntry> {
    bincode::deserialize(data).unwrap_or_else(|_| {
        serde_json::from_slice(data).expect("deserialize rev entries (json fallback)")
    })
}

fn deserialize_i64(data: &[u8]) -> i64 {
    bincode::deserialize(data)
        .unwrap_or_else(|_| serde_json::from_slice(data).expect("deserialize i64 (json fallback)"))
}

fn deserialize_u64(data: &[u8]) -> u64 {
    bincode::deserialize(data)
        .unwrap_or_else(|_| serde_json::from_slice(data).expect("deserialize u64 (json fallback)"))
}

// ── Result types ────────────────────────────────────────────────────────────

/// Result of a put operation.
#[derive(Debug)]
pub struct PutResult {
    pub revision: i64,
    pub prev_kv: Option<mvccpb::KeyValue>,
}

/// Result of a range query.
#[derive(Debug)]
pub struct RangeResult {
    pub kvs: Vec<mvccpb::KeyValue>,
    pub count: i64,
    pub more: bool,
}

/// Result of a delete operation.
#[derive(Debug)]
pub struct DeleteResult {
    pub revision: i64,
    pub deleted: i64,
    pub prev_kvs: Vec<mvccpb::KeyValue>,
}

// ── Txn types ────────────────────────────────────────────────────────────

/// Compare target for Txn conditions.
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

// ── Compound key helpers ────────────────────────────────────────────────────

/// Build a compound key: key_bytes + \x00 + 8-byte big-endian revision.
fn make_compound_key(key: &[u8], revision: u64) -> Vec<u8> {
    let mut compound = Vec::with_capacity(key.len() + 1 + 8);
    compound.extend_from_slice(key);
    compound.push(0x00);
    compound.extend_from_slice(&revision.to_be_bytes());
    compound
}

/// Extract the user key from a compound key (everything before the last 9 bytes).
fn extract_user_key(compound: &[u8]) -> &[u8] {
    &compound[..compound.len() - 9]
}

/// Extract the revision from a compound key (last 8 bytes).
fn extract_revision(compound: &[u8]) -> u64 {
    let start = compound.len() - 8;
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&compound[start..]);
    u64::from_be_bytes(buf)
}

// ── KvStore ─────────────────────────────────────────────────────────────────

/// MVCC key-value store backed by redb.
///
/// Every mutation creates a new revision. Keys are stored with compound keys
/// that encode the user key and revision for lexicographic ordering.
pub struct KvStore {
    db: Database,
    db_path: std::path::PathBuf,
    revision: AtomicI64,
}

impl KvStore {
    /// Open or create a KvStore at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let db_path = path.as_ref().to_path_buf();
        let db = Database::create(path)?;

        // Ensure all tables exist.
        let txn = db.begin_write()?;
        {
            txn.open_table(KV_TABLE)?;
            txn.open_table(REV_TABLE)?;
            txn.open_table(META_TABLE)?;
            txn.open_table(LATEST_TABLE)?;
        }
        txn.commit()?;

        // Load current revision from disk.
        let current_rev = {
            let rtxn = db.begin_read()?;
            let meta_table = rtxn.open_table(META_TABLE)?;
            match meta_table.get("revision")? {
                Some(val) => deserialize_i64(val.value()),
                None => 0,
            }
        };

        // Populate LATEST_TABLE if it's empty but KV_TABLE has data (migration).
        {
            let rtxn = db.begin_read()?;
            let latest_table = rtxn.open_table(LATEST_TABLE)?;
            let kv_table = rtxn.open_table(KV_TABLE)?;
            let latest_empty = latest_table.iter()?.next().is_none();
            let kv_has_data = kv_table.iter()?.next().is_some();

            if latest_empty && kv_has_data {
                drop(latest_table);
                drop(kv_table);
                drop(rtxn);

                // Build the LATEST_TABLE index by scanning KV_TABLE.
                let wtxn = db.begin_write()?;
                {
                    let kv_table = wtxn.open_table(KV_TABLE)?;
                    let mut latest_table = wtxn.open_table(LATEST_TABLE)?;

                    let mut latest_per_key: std::collections::HashMap<Vec<u8>, u64> =
                        std::collections::HashMap::new();

                    for entry in kv_table.iter()? {
                        let (ck, _) = entry?;
                        let ck_bytes = ck.value();
                        let user_key = extract_user_key(ck_bytes).to_vec();
                        let rev = extract_revision(ck_bytes);

                        let current_latest = latest_per_key.get(&user_key).copied().unwrap_or(0);
                        if rev > current_latest {
                            latest_per_key.insert(user_key, rev);
                        }
                    }

                    for (key, rev) in &latest_per_key {
                        latest_table.insert(key.as_slice(), *rev)?;
                    }
                }
                wtxn.commit()?;
            }
        }

        Ok(KvStore {
            db,
            db_path,
            revision: AtomicI64::new(current_rev),
        })
    }

    /// Get the database file size in bytes.
    pub fn db_file_size(&self) -> Result<i64, std::io::Error> {
        let metadata = std::fs::metadata(&self.db_path)?;
        Ok(metadata.len() as i64)
    }

    /// Read the entire database file into memory for snapshotting.
    pub fn snapshot_bytes(&self) -> Result<Vec<u8>, std::io::Error> {
        std::fs::read(&self.db_path)
    }

    /// Trigger internal compaction of the redb database.
    ///
    /// redb manages its own page-level compaction automatically, but
    /// committing an empty write transaction flushes any pending internal
    /// housekeeping and reclaims unused pages. This is the closest analogue
    /// to etcd's "defragment" operation when backed by redb.
    pub fn compact_db(&self) -> Result<bool, redb::Error> {
        let txn = self.db.begin_write()?;
        txn.commit()?;
        Ok(true)
    }

    /// Get the current revision number.
    pub fn current_revision(&self) -> Result<i64, redb::Error> {
        Ok(self.revision.load(Ordering::SeqCst))
    }

    /// Get the last applied Raft log index (persisted in meta table).
    pub fn last_applied_raft_index(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META_TABLE)?;
        match table.get("raft_applied_index")? {
            Some(val) => {
                let idx: u64 = deserialize_u64(val.value());
                Ok(idx)
            }
            None => Ok(0),
        }
    }

    /// Set the last applied Raft log index (persisted in meta table).
    pub fn set_last_applied_raft_index(&self, index: u64) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(META_TABLE)?;
            let val = bincode::serialize(&index).expect("serialize raft index");
            table.insert("raft_applied_index", val.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Put a key-value pair into the store. Returns the new revision and the
    /// previous value (if any).
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

            // Increment revision atomically.
            let new_rev = self.revision.fetch_add(1, Ordering::SeqCst) + 1;

            // Look up previous value for this key.
            let prev =
                self.find_latest_kv_in_table(&kv_table, &latest_table, key, new_rev as u64)?;

            let (create_revision, version) = match &prev {
                Some(prev_kv) if prev_kv.version > 0 => {
                    // Key exists: keep create_revision, bump version.
                    (prev_kv.create_revision, prev_kv.version + 1)
                }
                _ => {
                    // New key (or previously deleted).
                    (new_rev, 1)
                }
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

            // Write revision entry.
            let rev_entries = vec![RevisionEntry {
                key: key.to_vec(),
                event_type: EventType::Put,
            }];
            let rev_bytes = bincode::serialize(&rev_entries).expect("serialize rev entry");
            rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

            // Update meta revision.
            let rev_bytes = bincode::serialize(&new_rev).expect("serialize rev");
            meta_table.insert("revision", rev_bytes.as_slice())?;

            // Update LATEST_TABLE.
            latest_table.insert(key, new_rev as u64)?;

            result = PutResult {
                revision: new_rev,
                prev_kv: prev.map(|p| p.to_proto()),
            };
        }
        txn.commit()?;
        Ok(result)
    }

    /// Query a range of keys. If `range_end` is empty, returns the single key.
    /// If `range_end` is non-empty, returns keys in [key, range_end).
    /// If `revision` is 0 or negative, uses the latest revision.
    pub fn range(
        &self,
        key: &[u8],
        range_end: &[u8],
        limit: i64,
        revision: i64,
    ) -> Result<RangeResult, redb::Error> {
        let txn = self.db.begin_read()?;
        let kv_table = txn.open_table(KV_TABLE)?;
        let meta_table = txn.open_table(META_TABLE)?;
        let latest_table = txn.open_table(LATEST_TABLE)?;

        // For read transactions, read revision from META_TABLE for snapshot consistency.
        let max_rev = match meta_table.get("revision")? {
            Some(val) => deserialize_i64(val.value()),
            None => 0,
        };

        let query_rev = if revision > 0 { revision } else { max_rev };

        if range_end.is_empty() {
            // Single key lookup.
            let kv = self.find_latest_kv_at_rev_readonly(
                &kv_table,
                &latest_table,
                key,
                query_rev as u64,
            )?;
            match kv {
                Some(ikv) if ikv.version > 0 => Ok(RangeResult {
                    kvs: vec![ikv.to_proto()],
                    count: 1,
                    more: false,
                }),
                _ => Ok(RangeResult {
                    kvs: vec![],
                    count: 0,
                    more: false,
                }),
            }
        } else {
            // Range query [key, range_end).
            // We need to find all unique user keys in the range and get their
            // latest values at or before query_rev.
            let all_keys =
                self.collect_unique_keys_in_range(&kv_table, key, range_end, query_rev as u64)?;

            let mut kvs = Vec::new();
            for user_key in &all_keys {
                if let Some(ikv) = self.find_latest_kv_at_rev_readonly(
                    &kv_table,
                    &latest_table,
                    user_key,
                    query_rev as u64,
                )? {
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

            Ok(RangeResult {
                count: total_count,
                kvs,
                more,
            })
        }
    }

    /// Delete keys in range [key, range_end). If range_end is empty, delete the
    /// single key. Returns the new revision, number of deleted keys, and
    /// previous values.
    pub fn delete_range(
        &self,
        key: &[u8],
        range_end: &[u8],
    ) -> Result<DeleteResult, redb::Error> {
        let txn = self.db.begin_write()?;
        let result;
        {
            let mut kv_table = txn.open_table(KV_TABLE)?;
            let mut rev_table = txn.open_table(REV_TABLE)?;
            let mut meta_table = txn.open_table(META_TABLE)?;
            let mut latest_table = txn.open_table(LATEST_TABLE)?;

            let current_rev = self.revision.load(Ordering::SeqCst);

            // Find keys to delete.
            let keys_to_delete = if range_end.is_empty() {
                // Single key.
                match self.find_latest_kv_in_table(
                    &kv_table,
                    &latest_table,
                    key,
                    (current_rev + 1) as u64,
                )? {
                    Some(ikv) if ikv.version > 0 => vec![ikv],
                    _ => vec![],
                }
            } else {
                // Range [key, range_end).
                let user_keys = self.collect_unique_keys_in_range_rw(
                    &kv_table,
                    key,
                    range_end,
                    (current_rev + 1) as u64,
                )?;
                let mut result = Vec::new();
                for ukey in &user_keys {
                    if let Some(ikv) = self.find_latest_kv_in_table(
                        &kv_table,
                        &latest_table,
                        ukey,
                        (current_rev + 1) as u64,
                    )? {
                        if ikv.version > 0 {
                            result.push(ikv);
                        }
                    }
                }
                result
            };

            if keys_to_delete.is_empty() {
                result = DeleteResult {
                    revision: current_rev,
                    deleted: 0,
                    prev_kvs: vec![],
                };
            } else {
                let new_rev = self.revision.fetch_add(1, Ordering::SeqCst) + 1;
                let mut rev_entries = Vec::new();
                let mut prev_kvs = Vec::new();

                for ikv in &keys_to_delete {
                    // Write tombstone.
                    let tombstone = InternalKeyValue {
                        key: ikv.key.clone(),
                        create_revision: 0,
                        mod_revision: new_rev,
                        version: 0,
                        value: vec![],
                        lease: 0,
                    };

                    let compound = make_compound_key(&ikv.key, new_rev as u64);
                    let serialized = bincode::serialize(&tombstone).expect("serialize tombstone");
                    kv_table.insert(compound.as_slice(), serialized.as_slice())?;

                    // Update LATEST_TABLE to point to the tombstone revision.
                    latest_table.insert(ikv.key.as_slice(), new_rev as u64)?;

                    rev_entries.push(RevisionEntry {
                        key: ikv.key.clone(),
                        event_type: EventType::Delete,
                    });

                    prev_kvs.push(ikv.to_proto());
                }

                let rev_bytes = bincode::serialize(&rev_entries).expect("serialize rev entries");
                rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

                let rev_meta = bincode::serialize(&new_rev).expect("serialize rev");
                meta_table.insert("revision", rev_meta.as_slice())?;

                result = DeleteResult {
                    revision: new_rev,
                    deleted: keys_to_delete.len() as i64,
                    prev_kvs,
                };
            }
        }
        txn.commit()?;
        Ok(result)
    }

    // ── Txn (compare-and-swap) ────────────────────────────────────────────

    /// Execute a transaction (compare-and-swap).
    ///
    /// Evaluates all compare conditions against the current state. If all pass,
    /// executes the success ops; otherwise executes the failure ops. All
    /// operations within a branch are applied atomically within a single
    /// revision.
    pub fn txn(
        &self,
        compares: Vec<TxnCompare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
    ) -> Result<TxnResult, redb::Error> {
        // First, evaluate compares using a read-only transaction.
        let succeeded = self.evaluate_compares(&compares)?;

        // Execute the appropriate branch.
        let ops = if succeeded { &success } else { &failure };

        let mut responses = Vec::new();
        let mut final_revision = self.current_revision()?;

        for op in ops {
            match op {
                TxnOp::Range {
                    key,
                    range_end,
                    limit,
                    revision,
                } => {
                    let result = self.range(key, range_end, *limit, *revision)?;
                    responses.push(TxnOpResponse::Range(result));
                }
                TxnOp::Put {
                    key,
                    value,
                    lease_id,
                } => {
                    let result = self.put(key, value, *lease_id)?;
                    final_revision = result.revision;
                    responses.push(TxnOpResponse::Put(result));
                }
                TxnOp::DeleteRange { key, range_end } => {
                    let result = self.delete_range(key, range_end)?;
                    final_revision = result.revision;
                    responses.push(TxnOpResponse::DeleteRange(result));
                }
            }
        }

        Ok(TxnResult {
            succeeded,
            responses,
            revision: final_revision,
        })
    }

    /// Compact the store up to the given revision.
    ///
    /// Removes all KV entries with revision < compact_revision, keeping only
    /// the latest version of each key at or before the compact revision.
    /// Also removes revision table entries for compacted revisions.
    pub fn compact(&self, revision: i64) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut kv_table = txn.open_table(KV_TABLE)?;
            let mut rev_table = txn.open_table(REV_TABLE)?;

            // Collect all compound keys that should be removed.
            // We keep the latest entry for each user key at or before the
            // compact revision, and remove all older entries.
            let mut keys_to_remove: Vec<Vec<u8>> = Vec::new();
            let mut latest_per_key: std::collections::HashMap<Vec<u8>, (u64, Vec<u8>)> =
                std::collections::HashMap::new();

            // Scan all entries up to the compact revision.
            for entry in kv_table.iter()? {
                let (ck, _) = entry?;
                let ck_bytes = ck.value().to_vec();
                let rev = extract_revision(&ck_bytes);
                let user_key = extract_user_key(&ck_bytes).to_vec();

                if rev <= revision as u64 {
                    // Track the latest compound key for each user key.
                    match latest_per_key.get(&user_key) {
                        Some((existing_rev, existing_ck)) => {
                            if rev > *existing_rev {
                                // This is newer; the old one should be removed.
                                keys_to_remove.push(existing_ck.clone());
                                latest_per_key.insert(user_key, (rev, ck_bytes));
                            } else {
                                // This is older; remove it.
                                keys_to_remove.push(ck_bytes);
                            }
                        }
                        None => {
                            latest_per_key.insert(user_key, (rev, ck_bytes));
                        }
                    }
                }
            }

            // Remove old compound keys.
            for ck in &keys_to_remove {
                kv_table.remove(ck.as_slice())?;
            }

            // Remove revision entries for compacted revisions.
            let mut revs_to_remove: Vec<u64> = Vec::new();
            for entry in rev_table.range(0..revision as u64)? {
                let (rev, _) = entry?;
                revs_to_remove.push(rev.value());
            }
            for rev in revs_to_remove {
                rev_table.remove(rev)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Return all mutations since `after_revision` (exclusive).
    /// Each entry is `(key, event_type, kv_at_revision)`.
    /// event_type: 0 = PUT, 1 = DELETE.
    pub fn changes_since(
        &self,
        after_revision: i64,
    ) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue)>, redb::Error> {
        let txn = self.db.begin_read()?;
        let rev_table = txn.open_table(REV_TABLE)?;
        let kv_table = txn.open_table(KV_TABLE)?;

        let start_rev = (after_revision + 1) as u64;
        let mut results = Vec::new();

        let range = rev_table.range(start_rev..)?;
        for entry in range {
            let (rev_key, rev_val) = entry?;
            let revision = rev_key.value();
            let entries: Vec<RevisionEntry> = deserialize_rev_entries(rev_val.value());

            for re in entries {
                let event_type = match re.event_type {
                    EventType::Put => 0,
                    EventType::Delete => 1,
                };

                let compound = make_compound_key(&re.key, revision);
                let kv = match kv_table.get(compound.as_slice())? {
                    Some(val) => {
                        let ikv: InternalKeyValue = deserialize_kv(val.value());
                        ikv.to_proto()
                    }
                    None => mvccpb::KeyValue {
                        key: re.key.clone(),
                        mod_revision: revision as i64,
                        ..Default::default()
                    },
                };

                results.push((re.key, event_type, kv));
            }
        }

        Ok(results)
    }

    // ── Private helpers ─────────────────────────────────────────────────────

    /// Evaluate all compare conditions. Returns `true` if all pass.
    fn evaluate_compares(&self, compares: &[TxnCompare]) -> Result<bool, redb::Error> {
        let txn = self.db.begin_read()?;
        let kv_table = txn.open_table(KV_TABLE)?;
        let meta_table = txn.open_table(META_TABLE)?;
        let latest_table = txn.open_table(LATEST_TABLE)?;

        let current_rev = match meta_table.get("revision")? {
            Some(val) => deserialize_i64(val.value()),
            None => 0,
        };

        for cmp in compares {
            let kv = self.find_latest_kv_at_rev_readonly(
                &kv_table,
                &latest_table,
                &cmp.key,
                current_rev as u64,
            )?;

            let passes = match &cmp.target {
                TxnCompareTarget::Version(target_val) => {
                    let actual = kv.as_ref().map(|k| k.version).unwrap_or(0);
                    compare_i64(actual, *target_val, &cmp.result)
                }
                TxnCompareTarget::CreateRevision(target_val) => {
                    let actual = kv.as_ref().map(|k| k.create_revision).unwrap_or(0);
                    compare_i64(actual, *target_val, &cmp.result)
                }
                TxnCompareTarget::ModRevision(target_val) => {
                    let actual = kv.as_ref().map(|k| k.mod_revision).unwrap_or(0);
                    compare_i64(actual, *target_val, &cmp.result)
                }
                TxnCompareTarget::Value(target_val) => {
                    let actual = kv
                        .as_ref()
                        .map(|k| k.value.as_slice())
                        .unwrap_or(&[]);
                    compare_bytes(actual, target_val, &cmp.result)
                }
                TxnCompareTarget::Lease(target_val) => {
                    let actual = kv.as_ref().map(|k| k.lease).unwrap_or(0);
                    compare_i64(actual, *target_val, &cmp.result)
                }
            };

            if !passes {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Find the latest value for a user key at or before the given revision.
    /// Works with a writable table reference. Uses LATEST_TABLE for O(1) lookups
    /// when possible, falling back to range scan for historical queries.
    fn find_latest_kv_in_table(
        &self,
        table: &redb::Table<&[u8], &[u8]>,
        latest_table: &redb::Table<&[u8], u64>,
        key: &[u8],
        before_rev: u64,
    ) -> Result<Option<InternalKeyValue>, redb::Error> {
        // Try LATEST_TABLE first for O(1) lookup.
        if let Some(latest_rev_guard) = latest_table.get(key)? {
            let latest_rev = latest_rev_guard.value();
            if latest_rev < before_rev {
                // The latest revision is before our cutoff — direct point lookup.
                let compound = make_compound_key(key, latest_rev);
                if let Some(val) = table.get(compound.as_slice())? {
                    return Ok(Some(deserialize_kv(val.value())));
                }
            }
        }

        // Fall back to range scan (for historical queries or if LATEST_TABLE miss).
        let start = make_compound_key(key, 0);
        let end = make_compound_key(key, before_rev);

        let mut latest: Option<InternalKeyValue> = None;
        for entry in table.range(start.as_slice()..=end.as_slice())? {
            let (ck, val) = entry?;
            let ck_bytes = ck.value();
            if extract_user_key(ck_bytes) == key {
                let ikv: InternalKeyValue = deserialize_kv(val.value());
                latest = Some(ikv);
            }
        }
        Ok(latest)
    }

    /// Find the latest value for a user key at or before the given revision.
    /// Works with a read-only table reference. Uses LATEST_TABLE for O(1) lookups
    /// when possible, falling back to range scan for historical queries.
    fn find_latest_kv_at_rev_readonly(
        &self,
        table: &redb::ReadOnlyTable<&[u8], &[u8]>,
        latest_table: &redb::ReadOnlyTable<&[u8], u64>,
        key: &[u8],
        at_rev: u64,
    ) -> Result<Option<InternalKeyValue>, redb::Error> {
        // Try LATEST_TABLE first for O(1) lookup.
        if let Some(latest_rev_guard) = latest_table.get(key)? {
            let latest_rev = latest_rev_guard.value();
            if latest_rev <= at_rev {
                // The latest revision is at or before our cutoff — direct point lookup.
                let compound = make_compound_key(key, latest_rev);
                if let Some(val) = table.get(compound.as_slice())? {
                    return Ok(Some(deserialize_kv(val.value())));
                }
            }
        }

        // Fall back to range scan (for historical queries or if LATEST_TABLE miss).
        let start = make_compound_key(key, 0);
        let end = make_compound_key(key, at_rev);

        let mut latest: Option<InternalKeyValue> = None;
        for entry in table.range(start.as_slice()..=end.as_slice())? {
            let (ck, val) = entry?;
            let ck_bytes = ck.value();
            if extract_user_key(ck_bytes) == key {
                let ikv: InternalKeyValue = deserialize_kv(val.value());
                latest = Some(ikv);
            }
        }
        Ok(latest)
    }

    /// Collect unique user keys in [key, range_end) from writable table, up to max_rev.
    fn collect_unique_keys_in_range_rw(
        &self,
        table: &redb::Table<&[u8], &[u8]>,
        key_start: &[u8],
        range_end: &[u8],
        max_rev: u64,
    ) -> Result<Vec<Vec<u8>>, redb::Error> {
        // Build compound range: from (key_start, rev 0) to the compound key just before range_end.
        let start_compound = make_compound_key(key_start, 0);
        // We need all compound keys whose user key < range_end.
        // The upper bound: range_end + \x00 + max revision.
        let end_compound = make_compound_key(range_end, 0);

        let mut seen = std::collections::BTreeSet::new();
        for entry in table.range(start_compound.as_slice()..end_compound.as_slice())? {
            let (ck, _) = entry?;
            let ck_bytes = ck.value();
            let user_key = extract_user_key(ck_bytes);
            let rev = extract_revision(ck_bytes);
            // Only consider entries within max_rev.
            if rev <= max_rev && user_key >= key_start && user_key < range_end {
                seen.insert(user_key.to_vec());
            }
        }
        Ok(seen.into_iter().collect())
    }

    /// Collect unique user keys in [key, range_end) from read-only table, up to max_rev.
    fn collect_unique_keys_in_range(
        &self,
        table: &redb::ReadOnlyTable<&[u8], &[u8]>,
        key_start: &[u8],
        range_end: &[u8],
        max_rev: u64,
    ) -> Result<Vec<Vec<u8>>, redb::Error> {
        let start_compound = make_compound_key(key_start, 0);
        let end_compound = make_compound_key(range_end, 0);

        let mut seen = std::collections::BTreeSet::new();
        for entry in table.range(start_compound.as_slice()..end_compound.as_slice())? {
            let (ck, _) = entry?;
            let ck_bytes = ck.value();
            let user_key = extract_user_key(ck_bytes);
            let rev = extract_revision(ck_bytes);
            if rev <= max_rev && user_key >= key_start && user_key < range_end {
                seen.insert(user_key.to_vec());
            }
        }
        Ok(seen.into_iter().collect())
    }
}

// ── Compare helpers ──────────────────────────────────────────────────────

fn compare_i64(actual: i64, target: i64, result: &TxnCompareResult) -> bool {
    match result {
        TxnCompareResult::Equal => actual == target,
        TxnCompareResult::Greater => actual > target,
        TxnCompareResult::Less => actual < target,
        TxnCompareResult::NotEqual => actual != target,
    }
}

fn compare_bytes(actual: &[u8], target: &[u8], result: &TxnCompareResult) -> bool {
    match result {
        TxnCompareResult::Equal => actual == target,
        TxnCompareResult::Greater => actual > target,
        TxnCompareResult::Less => actual < target,
        TxnCompareResult::NotEqual => actual != target,
    }
}
