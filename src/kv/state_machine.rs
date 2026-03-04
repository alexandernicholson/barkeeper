use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::actor::KvStoreActorHandle;
use super::store::{TxnCompare, TxnOp};
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
    Txn {
        compares: Vec<TxnCompare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
    },
    Compact {
        revision: i64,
    },
}

/// Applies committed Raft entries to the KV store.
pub struct StateMachine {
    store: KvStoreActorHandle,
}

impl StateMachine {
    pub fn new(store: KvStoreActorHandle) -> Self {
        StateMachine { store }
    }

    pub async fn apply(&self, entries: Vec<LogEntry>) {
        for entry in entries {
            match entry.data {
                LogEntryData::Command(data) => {
                    // Commands are applied by the service layer after Raft commit.
                    // The state machine only logs for observability.
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(&data) {
                        tracing::debug!(?cmd, index = entry.index, "state machine received committed entry (applied by service)");
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
            } => match self.store.put(key, value, lease_id).await {
                Ok(result) => {
                    tracing::debug!(revision = result.revision, "applied put");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to apply put");
                }
            },
            KvCommand::DeleteRange { key, range_end } => {
                match self.store.delete_range(key, range_end).await {
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
            KvCommand::Txn {
                compares,
                success,
                failure,
            } => match self.store.txn(compares, success, failure).await {
                Ok(result) => {
                    tracing::debug!(
                        succeeded = result.succeeded,
                        revision = result.revision,
                        responses = result.responses.len(),
                        "applied txn"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to apply txn");
                }
            },
            KvCommand::Compact { revision } => match self.store.compact(revision).await {
                Ok(()) => {
                    tracing::debug!(revision = revision, "applied compact");
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to apply compact");
                }
            }
        }
    }
}

/// Spawn the state machine apply loop.
pub async fn spawn_state_machine(
    store: KvStoreActorHandle,
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
) {
    let sm = StateMachine::new(store);
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            sm.apply(entries).await;
        }
    });
}
