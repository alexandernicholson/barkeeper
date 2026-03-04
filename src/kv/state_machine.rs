use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::store::KvStore;
use crate::raft::messages::{LogEntry, LogEntryData};

/// Commands that can be applied to the KV store via Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvCommand {
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

/// Applies committed Raft entries to the KV store.
pub struct StateMachine {
    store: KvStore,
}

impl StateMachine {
    pub fn new(store: KvStore) -> Self {
        StateMachine { store }
    }

    pub async fn apply(&self, entries: Vec<LogEntry>) {
        for entry in entries {
            match entry.data {
                LogEntryData::Command(data) => {
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(&data) {
                        self.apply_command(cmd).await;
                    }
                }
                LogEntryData::Noop => {}
                LogEntryData::ConfigChange(_) => {}
            }
        }
    }

    async fn apply_command(&self, cmd: KvCommand) {
        match cmd {
            KvCommand::Put {
                key,
                value,
                lease_id,
            } => match self.store.put(key, value, lease_id) {
                Ok(result) => {
                    tracing::debug!(revision = result.revision, "applied put");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to apply put");
                }
            },
            KvCommand::DeleteRange { key, range_end } => {
                match self.store.delete_range(&key, &range_end) {
                    Ok(result) => {
                        tracing::debug!(
                            revision = result.revision,
                            deleted = result.deleted,
                            "applied delete"
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "failed to apply delete");
                    }
                }
            }
        }
    }

    pub fn store(&self) -> &KvStore {
        &self.store
    }
}

/// Spawn the state machine apply loop.
pub async fn spawn_state_machine(
    store: KvStore,
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
) {
    let sm = StateMachine::new(store);
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            sm.apply(entries).await;
        }
    });
}
