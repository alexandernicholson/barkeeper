use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use std::path::Path;
use std::sync::Arc;

use super::messages::LogEntry;
use super::state::PersistentState;

/// redb table: log_index (u64) -> serialized LogEntry
const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");

/// redb table: meta key (string) -> value (bytes)
const META_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

/// Durable storage for Raft log entries and hard state.
#[derive(Clone)]
pub struct LogStore {
    db: Arc<Database>,
}

impl LogStore {
    /// Open or create a LogStore at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, redb::Error> {
        let db = Database::create(path)?;
        // Ensure tables exist
        let txn = db.begin_write()?;
        {
            txn.open_table(LOG_TABLE)?;
            txn.open_table(META_TABLE)?;
        }
        txn.commit()?;
        Ok(LogStore { db: Arc::new(db) })
    }

    /// Append entries to the log. Overwrites any existing entries at the same indices.
    pub fn append(&self, entries: &[LogEntry]) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(LOG_TABLE)?;
            for entry in entries {
                let bytes = bincode::serialize(entry).expect("serialize log entry");
                table.insert(entry.index, bytes.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Flush log entries and optionally hard state in a single write transaction.
    /// This is the group-commit path: one fsync covers both log append and state persist.
    pub fn flush(
        &self,
        entries: &[LogEntry],
        hard_state: Option<&PersistentState>,
    ) -> Result<(), redb::Error> {
        if entries.is_empty() && hard_state.is_none() {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            if !entries.is_empty() {
                let mut table = txn.open_table(LOG_TABLE)?;
                for entry in entries {
                    let bytes = bincode::serialize(entry).expect("serialize log entry");
                    table.insert(entry.index, bytes.as_slice())?;
                }
            }
            if let Some(state) = hard_state {
                let mut table = txn.open_table(META_TABLE)?;
                let bytes = bincode::serialize(state).expect("serialize hard state");
                table.insert("hard_state", bytes.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Get a single log entry by index.
    pub fn get(&self, index: u64) -> Result<Option<LogEntry>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOG_TABLE)?;
        match table.get(index)? {
            Some(val) => {
                let entry: LogEntry =
                    bincode::deserialize(val.value()).expect("deserialize");
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Get entries in range [start, end] inclusive.
    pub fn get_range(&self, start: u64, end: u64) -> Result<Vec<LogEntry>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOG_TABLE)?;
        let mut entries = Vec::new();
        for result in table.range(start..=end)? {
            let (_, val) = result?;
            let entry: LogEntry =
                bincode::deserialize(val.value()).expect("deserialize");
            entries.push(entry);
        }
        Ok(entries)
    }

    /// Truncate all entries after the given index (exclusive).
    /// Keeps entries at index and before.
    pub fn truncate_after(&self, after_index: u64) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(LOG_TABLE)?;
            // Collect keys to remove
            let keys: Vec<u64> = table
                .range((after_index + 1)..)?
                .map(|r| r.map(|(k, _)| k.value()))
                .collect::<Result<_, _>>()?;
            for key in keys {
                table.remove(key)?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Get the last log index (0 if empty).
    pub fn last_index(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOG_TABLE)?;
        let result = match table.last()? {
            Some((k, _)) => k.value(),
            None => 0,
        };
        Ok(result)
    }

    /// Get the term of the last log entry (0 if empty).
    pub fn last_term(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOG_TABLE)?;
        let result = match table.last()? {
            Some((_, v)) => {
                let entry: LogEntry =
                    bincode::deserialize(v.value()).expect("deserialize");
                entry.term
            }
            None => 0,
        };
        Ok(result)
    }

    /// Get the term for a specific log index.
    pub fn term_at(&self, index: u64) -> Result<Option<u64>, redb::Error> {
        self.get(index).map(|opt| opt.map(|e| e.term))
    }

    /// Save persistent Raft state (current_term, voted_for).
    pub fn save_hard_state(&self, state: &PersistentState) -> Result<(), redb::Error> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(META_TABLE)?;
            let bytes = bincode::serialize(state).expect("serialize hard state");
            table.insert("hard_state", bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Load persistent Raft state.
    pub fn load_hard_state(&self) -> Result<Option<PersistentState>, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(META_TABLE)?;
        match table.get("hard_state")? {
            Some(val) => {
                let state: PersistentState =
                    bincode::deserialize(val.value()).expect("deserialize");
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Get total number of log entries.
    pub fn len(&self) -> Result<u64, redb::Error> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOG_TABLE)?;
        Ok(table.len()?)
    }

    pub fn is_empty(&self) -> Result<bool, redb::Error> {
        Ok(self.len()? == 0)
    }
}
