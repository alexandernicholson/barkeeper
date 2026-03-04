use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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
///
/// Currently the state machine only logs entries for observability.
/// Actual application happens at the service layer after Raft commit.
struct StateMachine;

impl StateMachine {
    async fn apply(&self, entries: Vec<LogEntry>) {
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
}

/// Spawn the state machine apply loop.
pub async fn spawn_state_machine(
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
) {
    let sm = StateMachine;
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            sm.apply(entries).await;
        }
    });
}
