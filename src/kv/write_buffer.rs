use dashmap::DashMap;

use crate::proto::mvccpb::KeyValue;

/// Buffered KV entry — either a live value or a tombstone (deletion marker).
#[derive(Clone, Debug)]
pub enum BufferEntry {
    Live(KeyValue),
    Tombstone { key: Vec<u8>, mod_revision: i64 },
}

/// Thread-safe write buffer for KV mutations pending flush to redb.
///
/// Writes go here immediately after WAL commit. Range queries merge
/// buffer contents with redb results. The state machine drains the
/// buffer after flushing to redb.
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

    /// Check if a key has an entry in the buffer (live or tombstone).
    pub fn contains(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    /// Check if a key is tombstoned in the buffer.
    pub fn is_deleted(&self, key: &[u8]) -> bool {
        self.entries
            .get(key)
            .map(|entry| matches!(entry.value(), BufferEntry::Tombstone { .. }))
            .unwrap_or(false)
    }

    /// Get all live entries in a key range [start, end).
    /// If end is empty, matches only exact key `start`.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Vec<KeyValue> {
        let mut results = Vec::new();
        for entry in self.entries.iter() {
            let key = entry.key();
            if end.is_empty() {
                // Exact match
                if key.as_slice() == start {
                    if let BufferEntry::Live(kv) = entry.value() {
                        results.push(kv.clone());
                    }
                }
            } else {
                // Range match [start, end)
                if key.as_slice() >= start && key.as_slice() < end {
                    if let BufferEntry::Live(kv) = entry.value() {
                        results.push(kv.clone());
                    }
                }
            }
        }
        results.sort_by(|a, b| a.key.cmp(&b.key));
        results
    }

    /// Get all tombstoned keys in a range [start, end).
    /// Used to filter out deleted keys from redb results.
    pub fn deleted_keys_in_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        for entry in self.entries.iter() {
            let key = entry.key();
            if let BufferEntry::Tombstone { .. } = entry.value() {
                if end.is_empty() {
                    if key.as_slice() == start {
                        keys.push(key.clone());
                    }
                } else if key.as_slice() >= start && key.as_slice() < end {
                    keys.push(key.clone());
                }
            }
        }
        keys
    }

    /// Remove an entry only if its revision is <= max_revision.
    /// Prevents removing a newer write that arrived while flushing.
    pub fn remove_if_revision_le(&self, key: &[u8], max_revision: i64) {
        self.entries.remove_if(key, |_, entry| match entry {
            BufferEntry::Live(kv) => kv.mod_revision <= max_revision,
            BufferEntry::Tombstone { mod_revision, .. } => *mod_revision <= max_revision,
        });
    }

    /// Number of entries in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
