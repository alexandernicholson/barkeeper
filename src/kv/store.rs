use std::path::Path;

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
}

impl KvStore {
    /// Open or create a KvStore at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        // Ensure all tables exist.
        let txn = db.begin_write()?;
        {
            txn.open_table(KV_TABLE)?;
            txn.open_table(REV_TABLE)?;
            txn.open_table(META_TABLE)?;
        }
        txn.commit()?;
        Ok(KvStore { db })
    }

    /// Get the current revision number.
    pub fn current_revision(&self) -> Result<i64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META_TABLE)?;
        match table.get("revision")? {
            Some(val) => {
                let rev: i64 = serde_json::from_slice(val.value()).expect("deserialize revision");
                Ok(rev)
            }
            None => Ok(0),
        }
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

            // Load current revision.
            let current_rev = match meta_table.get("revision")? {
                Some(val) => serde_json::from_slice::<i64>(val.value()).expect("deser rev"),
                None => 0,
            };
            let new_rev = current_rev + 1;

            // Look up previous value for this key.
            let prev = self.find_latest_kv_in_table(&kv_table, key, new_rev as u64)?;

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
            let serialized = serde_json::to_vec(&ikv).expect("serialize kv");
            kv_table.insert(compound.as_slice(), serialized.as_slice())?;

            // Write revision entry.
            let rev_entries = vec![RevisionEntry {
                key: key.to_vec(),
                event_type: EventType::Put,
            }];
            let rev_bytes = serde_json::to_vec(&rev_entries).expect("serialize rev entry");
            rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

            // Update meta revision.
            let rev_bytes = serde_json::to_vec(&new_rev).expect("serialize rev");
            meta_table.insert("revision", rev_bytes.as_slice())?;

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

        let max_rev = match meta_table.get("revision")? {
            Some(val) => serde_json::from_slice::<i64>(val.value()).expect("deser rev"),
            None => 0,
        };

        let query_rev = if revision > 0 { revision } else { max_rev };

        if range_end.is_empty() {
            // Single key lookup.
            let kv = self.find_latest_kv_at_rev_readonly(&kv_table, key, query_rev as u64)?;
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
            let all_keys = self.collect_unique_keys_in_range(&kv_table, key, range_end, query_rev as u64)?;

            let mut kvs = Vec::new();
            for user_key in &all_keys {
                if let Some(ikv) = self.find_latest_kv_at_rev_readonly(&kv_table, user_key, query_rev as u64)? {
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

            let current_rev = match meta_table.get("revision")? {
                Some(val) => serde_json::from_slice::<i64>(val.value()).expect("deser rev"),
                None => 0,
            };

            // Find keys to delete.
            let keys_to_delete = if range_end.is_empty() {
                // Single key.
                match self.find_latest_kv_in_table(&kv_table, key, (current_rev + 1) as u64)? {
                    Some(ikv) if ikv.version > 0 => vec![ikv],
                    _ => vec![],
                }
            } else {
                // Range [key, range_end).
                let user_keys =
                    self.collect_unique_keys_in_range_rw(&kv_table, key, range_end, (current_rev + 1) as u64)?;
                let mut result = Vec::new();
                for ukey in &user_keys {
                    if let Some(ikv) = self.find_latest_kv_in_table(&kv_table, ukey, (current_rev + 1) as u64)? {
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
                let new_rev = current_rev + 1;
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
                    let serialized = serde_json::to_vec(&tombstone).expect("serialize tombstone");
                    kv_table.insert(compound.as_slice(), serialized.as_slice())?;

                    rev_entries.push(RevisionEntry {
                        key: ikv.key.clone(),
                        event_type: EventType::Delete,
                    });

                    prev_kvs.push(ikv.to_proto());
                }

                let rev_bytes = serde_json::to_vec(&rev_entries).expect("serialize rev entries");
                rev_table.insert(new_rev as u64, rev_bytes.as_slice())?;

                let rev_meta = serde_json::to_vec(&new_rev).expect("serialize rev");
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

    // ── Private helpers ─────────────────────────────────────────────────────

    /// Find the latest value for a user key at or before the given revision.
    /// Works with a writable table reference.
    fn find_latest_kv_in_table(
        &self,
        table: &redb::Table<&[u8], &[u8]>,
        key: &[u8],
        before_rev: u64,
    ) -> Result<Option<InternalKeyValue>, redb::Error> {
        // Scan compound keys with prefix = key + \x00, going backwards from before_rev.
        let start = make_compound_key(key, 0);
        let end = make_compound_key(key, before_rev);

        let mut latest: Option<InternalKeyValue> = None;
        for entry in table.range(start.as_slice()..=end.as_slice())? {
            let (ck, val) = entry?;
            let ck_bytes = ck.value();
            if extract_user_key(ck_bytes) == key {
                let ikv: InternalKeyValue =
                    serde_json::from_slice(val.value()).expect("deserialize kv");
                latest = Some(ikv);
            }
        }
        Ok(latest)
    }

    /// Find the latest value for a user key at or before the given revision.
    /// Works with a read-only table reference.
    fn find_latest_kv_at_rev_readonly(
        &self,
        table: &redb::ReadOnlyTable<&[u8], &[u8]>,
        key: &[u8],
        at_rev: u64,
    ) -> Result<Option<InternalKeyValue>, redb::Error> {
        let start = make_compound_key(key, 0);
        let end = make_compound_key(key, at_rev);

        let mut latest: Option<InternalKeyValue> = None;
        for entry in table.range(start.as_slice()..=end.as_slice())? {
            let (ck, val) = entry?;
            let ck_bytes = ck.value();
            if extract_user_key(ck_bytes) == key {
                let ikv: InternalKeyValue =
                    serde_json::from_slice(val.value()).expect("deserialize kv");
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
