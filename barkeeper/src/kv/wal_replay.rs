//! WAL replay logic: restores KV state from unapplied WAL entries after restart.

use crate::kv::state_machine::KvCommand;
use crate::kv::store::KvStore;
use crate::raft::log_store::LogStore;
use crate::raft::messages::LogEntryData;

/// Replay unapplied WAL entries into the KV store.
///
/// Reads entries from `last_applied + 1` through `last_log_index`, deserializes
/// each as a `KvCommand`, and applies them in batch. After replay, a snapshot is
/// saved so future restarts skip already-replayed entries.
///
/// Returns the number of commands applied.
pub fn replay_wal(log_store: &LogStore, kv_store: &KvStore) -> Result<usize, Box<dyn std::error::Error>> {
    let last_applied = kv_store.last_applied_raft_index().unwrap_or(0);
    let last_log_index = log_store.last_index()?;

    if last_applied >= last_log_index {
        return Ok(0);
    }

    let entries = log_store.get_range(last_applied + 1, last_log_index)?;
    let mut commands: Vec<(KvCommand, i64)> = Vec::new();
    for entry in &entries {
        if let LogEntryData::Command { data, revision } = &entry.data {
            if let Ok(cmd) = bincode::deserialize::<KvCommand>(data)
                .or_else(|_| {
                    serde_json::from_slice::<KvCommand>(data)
                        .map_err(|e| Box::new(bincode::ErrorKind::Custom(e.to_string())))
                })
            {
                commands.push((cmd, *revision));
            }
        }
    }

    let count = commands.len();
    if !commands.is_empty() {
        kv_store.batch_apply_with_index(&commands, last_log_index)?;
        kv_store.snapshot()?;
    } else {
        kv_store.set_last_applied_raft_index(last_log_index)?;
    }

    Ok(count)
}
