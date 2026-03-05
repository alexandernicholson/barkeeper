use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::apply_broker::{ApplyResult, ApplyResultBroker};
use super::store::{ApplyResultData, KvStore, TxnCompare, TxnOp};
use crate::lease::manager::LeaseManager;
use crate::raft::messages::{LogEntry, LogEntryData};
use crate::watch::actor::WatchHubActorHandle;

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

/// Applies committed Raft entries to the KV store on ALL nodes.
///
/// This is the single source of truth for KV mutations. Both leaders and
/// followers apply entries here, ensuring all nodes have consistent state.
///
/// The state machine holds `Arc<KvStore>` directly and uses
/// `batch_apply_with_index` to apply entire batches in a single redb
/// write transaction (1 fsync per batch).
struct StateMachine {
    store: Arc<KvStore>,
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
}

impl StateMachine {
    async fn apply(&self, entries: Vec<LogEntry>) {
        // Get the last applied index to skip entries that were already applied
        // (e.g. after restart when Raft replays committed entries).
        let last_applied = self.store.last_applied_raft_index().unwrap_or(0);

        // Collect applicable commands.
        let mut commands = Vec::new();
        let mut entry_indices = Vec::new();

        for entry in &entries {
            if entry.index <= last_applied {
                tracing::debug!(index = entry.index, last_applied, "skipping already-applied entry");
                self.broker.send_result(entry.index, ApplyResult::Noop).await;
                continue;
            }

            match &entry.data {
                LogEntryData::Command { data, .. } => {
                    // Try bincode first, fall back to JSON for old entries.
                    let cmd_result = bincode::deserialize::<KvCommand>(data)
                        .or_else(|_| {
                            serde_json::from_slice::<KvCommand>(data)
                                .map_err(|e| Box::new(bincode::ErrorKind::Custom(e.to_string())) as Box<bincode::ErrorKind>)
                        });
                    match cmd_result {
                        Ok(cmd) => {
                            commands.push(cmd);
                            entry_indices.push(entry.index);
                        }
                        Err(_) => {
                            tracing::warn!(index = entry.index, "failed to deserialize KvCommand");
                            self.broker.send_result(entry.index, ApplyResult::Noop).await;
                        }
                    }
                }
                _ => {
                    self.broker.send_result(entry.index, ApplyResult::Noop).await;
                }
            }
        }

        if commands.is_empty() {
            return;
        }

        let last_index = *entry_indices.last().unwrap();

        // Single blocking call for entire batch.
        let results = tokio::task::spawn_blocking({
            let store = Arc::clone(&self.store);
            let cmds = commands.clone();
            move || store.batch_apply_with_index(&cmds, last_index)
        })
        .await
        .expect("batch apply panicked");

        match results {
            Ok(batch_results) => {
                for (i, batch_op) in batch_results.into_iter().enumerate() {
                    let index = entry_indices[i];

                    // Fire watch notifications.
                    for event in &batch_op.watch_events {
                        self.watch_hub
                            .notify(
                                event.key.clone(),
                                event.event_type,
                                event.kv.clone(),
                                event.prev_kv.clone(),
                            )
                            .await;
                    }

                    // Attach keys to leases.
                    if let KvCommand::Put { ref key, lease_id, .. } = commands[i] {
                        if lease_id != 0 {
                            self.lease_manager.attach_key(lease_id, key.clone()).await;
                        }
                    }

                    // Convert to ApplyResult and send to broker.
                    let apply_result = match batch_op.apply_result {
                        ApplyResultData::Put(r) => ApplyResult::Put(r),
                        ApplyResultData::DeleteRange(r) => ApplyResult::DeleteRange(r),
                        ApplyResultData::Txn(r) => ApplyResult::Txn(r),
                        ApplyResultData::Compact { revision } => ApplyResult::Compact { revision },
                        ApplyResultData::Noop => ApplyResult::Noop,
                    };
                    self.broker.send_result(index, apply_result).await;
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "batch apply failed");
                for index in &entry_indices {
                    self.broker.send_result(*index, ApplyResult::Noop).await;
                }
            }
        }
    }
}

/// Spawn the state machine apply loop.
pub async fn spawn_state_machine(
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
    store: Arc<KvStore>,
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
) {
    let sm = StateMachine {
        store,
        watch_hub,
        lease_manager,
        broker,
    };
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            sm.apply(entries).await;
        }
    });
}
