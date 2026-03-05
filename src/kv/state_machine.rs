use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use super::actor::KvStoreActorHandle;
use super::apply_broker::{ApplyResult, ApplyResultBroker};
use super::store::{TxnCompare, TxnOp};
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
struct StateMachine {
    store: KvStoreActorHandle,
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
}

impl StateMachine {
    async fn apply(&self, entries: Vec<LogEntry>) {
        // Get the last applied index to skip entries that were already applied
        // (e.g. after restart when Raft replays committed entries).
        let last_applied = self.store.last_applied_raft_index().await.unwrap_or(0);

        for entry in entries {
            let index = entry.index;

            if index <= last_applied {
                tracing::debug!(index, last_applied, "skipping already-applied entry");
                // Still send a Noop result so any waiting service handler doesn't hang.
                self.broker.send_result(index, ApplyResult::Noop).await;
                continue;
            }

            let result = match entry.data {
                LogEntryData::Command(data) => {
                    if let Ok(cmd) = serde_json::from_slice::<KvCommand>(&data) {
                        self.apply_command(cmd).await
                    } else {
                        tracing::warn!(index, "failed to deserialize KvCommand");
                        ApplyResult::Noop
                    }
                }
                LogEntryData::Noop => ApplyResult::Noop,
                LogEntryData::ConfigChange(_) => ApplyResult::Noop,
            };

            // Persist the applied index so we can skip on restart.
            if let Err(e) = self.store.set_last_applied_raft_index(index).await {
                tracing::error!(index, error = %e, "failed to persist applied raft index");
            }

            self.broker.send_result(index, result).await;
        }
    }

    async fn apply_command(&self, cmd: KvCommand) -> ApplyResult {
        match cmd {
            KvCommand::Put { key, value, lease_id } => {
                match self.store.put(key.clone(), value.clone(), lease_id).await {
                    Ok(result) => {
                        // Notify watchers.
                        let (create_rev, ver) = match &result.prev_kv {
                            Some(prev) => (prev.create_revision, prev.version + 1),
                            None => (result.revision, 1),
                        };
                        let notify_kv = crate::proto::mvccpb::KeyValue {
                            key: key.clone(),
                            create_revision: create_rev,
                            mod_revision: result.revision,
                            version: ver,
                            value: value.clone(),
                            lease: lease_id,
                        };
                        self.watch_hub.notify(key.clone(), 0, notify_kv, result.prev_kv.clone()).await;

                        // Attach key to lease.
                        if lease_id != 0 {
                            self.lease_manager.attach_key(lease_id, key).await;
                        }

                        ApplyResult::Put(result)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "state machine: put failed");
                        ApplyResult::Noop
                    }
                }
            }
            KvCommand::DeleteRange { key, range_end } => {
                match self.store.delete_range(key.clone(), range_end.clone()).await {
                    Ok(result) => {
                        // Notify watchers for each deleted key.
                        for prev in &result.prev_kvs {
                            let tombstone = crate::proto::mvccpb::KeyValue {
                                key: prev.key.clone(),
                                create_revision: 0,
                                mod_revision: result.revision,
                                version: 0,
                                value: vec![],
                                lease: 0,
                            };
                            self.watch_hub.notify(prev.key.clone(), 1, tombstone, Some(prev.clone())).await;
                        }
                        ApplyResult::DeleteRange(result)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "state machine: delete_range failed");
                        ApplyResult::Noop
                    }
                }
            }
            KvCommand::Txn { compares, success, failure } => {
                match self.store.txn(compares, success.clone(), failure.clone()).await {
                    Ok(result) => {
                        // Notify watchers for txn mutations.
                        let executed_ops = if result.succeeded { &success } else { &failure };
                        for (op, resp) in executed_ops.iter().zip(result.responses.iter()) {
                            match (op, resp) {
                                (TxnOp::Put { key, value, lease_id }, crate::kv::store::TxnOpResponse::Put(r)) => {
                                    let (create_rev, ver) = match &r.prev_kv {
                                        Some(prev) => (prev.create_revision, prev.version + 1),
                                        None => (r.revision, 1),
                                    };
                                    let notify_kv = crate::proto::mvccpb::KeyValue {
                                        key: key.clone(),
                                        create_revision: create_rev,
                                        mod_revision: r.revision,
                                        version: ver,
                                        value: value.clone(),
                                        lease: *lease_id,
                                    };
                                    self.watch_hub.notify(key.clone(), 0, notify_kv, r.prev_kv.clone()).await;
                                }
                                (TxnOp::DeleteRange { .. }, crate::kv::store::TxnOpResponse::DeleteRange(r)) => {
                                    for prev in &r.prev_kvs {
                                        let tombstone = crate::proto::mvccpb::KeyValue {
                                            key: prev.key.clone(),
                                            create_revision: 0,
                                            mod_revision: r.revision,
                                            version: 0,
                                            value: vec![],
                                            lease: 0,
                                        };
                                        self.watch_hub.notify(prev.key.clone(), 1, tombstone, Some(prev.clone())).await;
                                    }
                                }
                                _ => {} // Range ops don't need notifications
                            }
                        }
                        ApplyResult::Txn(result)
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "state machine: txn failed");
                        ApplyResult::Noop
                    }
                }
            }
            KvCommand::Compact { revision } => {
                match self.store.compact(revision).await {
                    Ok(_) => {
                        let rev = self.store.current_revision().await.unwrap_or(0);
                        ApplyResult::Compact { revision: rev }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "state machine: compact failed");
                        ApplyResult::Noop
                    }
                }
            }
        }
    }
}

/// Spawn the state machine apply loop.
pub async fn spawn_state_machine(
    mut apply_rx: mpsc::Receiver<Vec<LogEntry>>,
    store: KvStoreActorHandle,
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    broker: Arc<ApplyResultBroker>,
) {
    let sm = StateMachine { store, watch_hub, lease_manager, broker };
    tokio::spawn(async move {
        while let Some(entries) = apply_rx.recv().await {
            sm.apply(entries).await;
        }
    });
}
