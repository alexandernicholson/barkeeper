//! Rebar actor for KV store management.
//!
//! Converts the shared `Arc<KvStore>` into a message-passing actor that
//! serializes all access through a `tokio::sync::mpsc` command channel.
//! Since redb is a blocking/sync database, all store operations run via
//! `tokio::task::spawn_blocking`.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use rebar_core::runtime::Runtime;

use crate::actors::commands::KvStoreCmd;
use crate::kv::store::{
    DeleteResult, KvStore, PutResult, RangeResult, TxnCompare, TxnOp, TxnResult,
};
use crate::proto::mvccpb;

/// Spawn the KV store actor on the Rebar runtime.
///
/// The actor takes ownership of the `KvStore` (wrapping it in `Arc` internally
/// for `spawn_blocking`), and processes commands sequentially from the `cmd_rx`
/// channel. Returns a lightweight handle for sending commands.
pub async fn spawn_kv_store_actor(runtime: &Runtime, store: Arc<KvStore>) -> KvStoreActorHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<KvStoreCmd>(256);

    runtime.spawn(move |mut ctx| async move {
        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        KvStoreCmd::Put { key, value, lease_id, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.put(&key, &value, lease_id).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::Range { key, range_end, limit, revision, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.range(&key, &range_end, limit, revision).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::DeleteRange { key, range_end, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.delete_range(&key, &range_end).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::Txn { compares, success, failure, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.txn(&compares, &success, &failure).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::Compact { revision, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.compact(revision).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::CurrentRevision { reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.current_revision().map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::ChangesSince { after_revision, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.changes_since(after_revision).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::ChangesSinceWithPrev { after_revision, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.changes_since_with_prev(after_revision).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::DbFileSize { reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.db_file_size().map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::SnapshotBytes { reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.snapshot_bytes().map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::CompactDb { reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.compact_db().map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::LastAppliedRaftIndex { reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.last_applied_raft_index().map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                        KvStoreCmd::SetLastAppliedRaftIndex { index, reply } => {
                            let s = Arc::clone(&store);
                            let result = tokio::task::spawn_blocking(move || {
                                s.set_last_applied_raft_index(index).map_err(|e| e.to_string())
                            }).await.expect("kv store spawn_blocking panicked");
                            let _ = reply.send(result);
                        }
                    }
                }
                // Also listen on Rebar mailbox for future distributed messages.
                Some(_msg) = ctx.recv() => {
                    // Reserved for future distributed KV coordination.
                }
                else => break,
            }
        }
    }).await;

    KvStoreActorHandle { cmd_tx }
}

/// Lightweight handle for communicating with the KV store actor.
///
/// This replaces `Arc<KvStore>` throughout the codebase. It is
/// cheaply cloneable and exposes the same async API as the old store.
#[derive(Clone)]
pub struct KvStoreActorHandle {
    cmd_tx: mpsc::Sender<KvStoreCmd>,
}

impl KvStoreActorHandle {
    /// Put a key-value pair into the store.
    pub async fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
    ) -> Result<PutResult, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::Put {
                key,
                value,
                lease_id,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Query a range of keys.
    pub async fn range(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
    ) -> Result<RangeResult, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::Range {
                key,
                range_end,
                limit,
                revision,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Delete keys in range [key, range_end).
    pub async fn delete_range(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
    ) -> Result<DeleteResult, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::DeleteRange {
                key,
                range_end,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Execute a transaction (compare-and-swap).
    pub async fn txn(
        &self,
        compares: Vec<TxnCompare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
    ) -> Result<TxnResult, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::Txn {
                compares,
                success,
                failure,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Compact the store up to the given revision.
    pub async fn compact(&self, revision: i64) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::Compact { revision, reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Get the current revision number.
    pub async fn current_revision(&self) -> Result<i64, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::CurrentRevision { reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Return all mutations since `after_revision` (exclusive).
    pub async fn changes_since(
        &self,
        after_revision: i64,
    ) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue)>, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::ChangesSince {
                after_revision,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Return all mutations since `after_revision` (exclusive), including previous key-values.
    pub async fn changes_since_with_prev(
        &self,
        after_revision: i64,
    ) -> Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue, Option<mvccpb::KeyValue>)>, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::ChangesSinceWithPrev {
                after_revision,
                reply,
            })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Get the database file size in bytes.
    pub async fn db_file_size(&self) -> Result<i64, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::DbFileSize { reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Read the entire database file into memory for snapshotting.
    pub async fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::SnapshotBytes { reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Trigger internal compaction of the redb database.
    pub async fn compact_db(&self) -> Result<bool, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::CompactDb { reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Get the last applied Raft log index.
    pub async fn last_applied_raft_index(&self) -> Result<u64, String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::LastAppliedRaftIndex { reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }

    /// Set the last applied Raft log index.
    pub async fn set_last_applied_raft_index(&self, index: u64) -> Result<(), String> {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(KvStoreCmd::SetLastAppliedRaftIndex { index, reply })
            .await
            .expect("kv store actor dead");
        rx.await.expect("kv store actor dropped")
    }
}
