//! Rebar actor for watch hub management.
//!
//! Converts the shared `Arc<WatchHub>` into a message-passing actor that
//! serializes all access through a `tokio::sync::mpsc` command channel.
//! The key difference from other actors: `CreateWatch` returns an
//! `mpsc::Receiver<WatchEvent>` through the reply channel, enabling
//! streaming event delivery to subscribers.

use std::collections::HashMap;

use tokio::sync::{mpsc, oneshot};

use rebar_core::runtime::Runtime;

use crate::actors::commands::WatchHubCmd;
use crate::kv::actor::KvStoreActorHandle;
use crate::proto::mvccpb;
use crate::watch::hub::{key_matches, WatchEvent};

/// A single watcher watching a key or range (private to the actor).
struct Watcher {
    id: i64,
    key: Vec<u8>,
    range_end: Vec<u8>,
    filters: Vec<i32>,
    prev_kv: bool,
    tx: mpsc::Sender<WatchEvent>,
}

/// Spawn the watch hub actor on the Rebar runtime.
///
/// The actor owns all watcher state internally and processes commands
/// sequentially from the `cmd_rx` channel. An optional `KvStoreActorHandle`
/// enables historical replay for `CreateWatch` with `start_revision > 0`.
/// Returns a lightweight handle for sending commands.
pub async fn spawn_watch_hub_actor(
    runtime: &Runtime,
    store: Option<KvStoreActorHandle>,
) -> WatchHubActorHandle {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<WatchHubCmd>(256);

    runtime.spawn(move |mut ctx| async move {
        let mut watchers: HashMap<i64, Watcher> = HashMap::new();
        let mut next_id: i64 = 1;

        loop {
            tokio::select! {
                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        WatchHubCmd::CreateWatch { key, range_end, start_revision, filters, prev_kv, requested_watch_id, reply } => {
                            let (tx, rx) = mpsc::channel(256);

                            // Determine watch ID: use requested if non-zero, else auto-assign.
                            let id = if requested_watch_id != 0 {
                                // Check for collision with existing watchers.
                                if watchers.contains_key(&requested_watch_id) {
                                    // Collision: send reply with the id but drop tx so rx closes immediately.
                                    let _ = reply.send((requested_watch_id, rx));
                                    // Drop tx here (it goes out of scope since we don't store it).
                                    continue;
                                }
                                // Bump next_id if the requested id >= next_id.
                                if requested_watch_id >= next_id {
                                    next_id = requested_watch_id + 1;
                                }
                                requested_watch_id
                            } else {
                                let id = next_id;
                                next_id += 1;
                                id
                            };

                            // Replay historical events BEFORE sending the reply
                            // so the subscriber doesn't miss events.
                            if start_revision > 0 {
                                if let Some(ref store) = store {
                                    // changes_since is exclusive, so pass start_revision - 1
                                    // to include events AT start_revision.
                                    match store.changes_since(start_revision - 1).await {
                                        Ok(changes) => {
                                            for (change_key, event_type, kv) in changes {
                                                if !key_matches(&key, &range_end, &change_key) {
                                                    continue;
                                                }

                                                // Apply filters during historical replay.
                                                if filters.contains(&event_type) {
                                                    continue;
                                                }

                                                let event = mvccpb::Event {
                                                    r#type: event_type,
                                                    kv: Some(kv),
                                                    prev_kv: None,
                                                };

                                                let watch_event = WatchEvent {
                                                    watch_id: id,
                                                    events: vec![event],
                                                    compact_revision: 0,
                                                };

                                                // If the receiver is gone, stop replaying.
                                                if tx.send(watch_event).await.is_err() {
                                                    break;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("failed to replay watch history: {}", e);
                                        }
                                    }
                                }
                            }

                            let watcher = Watcher {
                                id,
                                key,
                                range_end,
                                filters,
                                prev_kv,
                                tx,
                            };
                            watchers.insert(id, watcher);

                            let _ = reply.send((id, rx));
                        }
                        WatchHubCmd::CancelWatch { watch_id, reply } => {
                            let existed = watchers.remove(&watch_id).is_some();
                            let _ = reply.send(existed);
                        }
                        WatchHubCmd::Notify { key, event_type, kv, prev_kv } => {
                            let mut dead_ids = Vec::new();

                            for (id, watcher) in watchers.iter() {
                                if !key_matches(&watcher.key, &watcher.range_end, &key) {
                                    continue;
                                }

                                // Skip delivery when event type matches a filter.
                                if watcher.filters.contains(&event_type) {
                                    continue;
                                }

                                // Only include prev_kv for watchers that requested it.
                                let event_prev_kv = if watcher.prev_kv {
                                    prev_kv.clone()
                                } else {
                                    None
                                };

                                let event = mvccpb::Event {
                                    r#type: event_type,
                                    kv: Some(kv.clone()),
                                    prev_kv: event_prev_kv,
                                };

                                let watch_event = WatchEvent {
                                    watch_id: watcher.id,
                                    events: vec![event],
                                    compact_revision: 0,
                                };

                                // If the receiver is gone, mark the watcher for cleanup.
                                if watcher.tx.send(watch_event).await.is_err() {
                                    dead_ids.push(*id);
                                }
                            }

                            // Clean up dead watchers.
                            for id in dead_ids {
                                watchers.remove(&id);
                            }
                        }
                    }
                }
                // Also listen on Rebar mailbox for future distributed messages.
                Some(_msg) = ctx.recv() => {
                    // Reserved for future distributed watch coordination.
                }
                else => break,
            }
        }
    }).await;

    WatchHubActorHandle { cmd_tx }
}

/// Lightweight handle for communicating with the watch hub actor.
///
/// This replaces `Arc<WatchHub>` throughout the codebase. It is
/// cheaply cloneable and exposes the same async API as the old hub.
#[derive(Clone)]
pub struct WatchHubActorHandle {
    cmd_tx: mpsc::Sender<WatchHubCmd>,
}

impl WatchHubActorHandle {
    /// Create a watch and return (watch_id, event_receiver).
    ///
    /// If `start_revision > 0` and the actor has a store reference, historical
    /// events from that revision onward are replayed to the watcher before it
    /// begins receiving live events.
    pub async fn create_watch(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
        filters: Vec<i32>,
        prev_kv: bool,
    ) -> (i64, mpsc::Receiver<WatchEvent>) {
        self.create_watch_with_id(key, range_end, start_revision, filters, prev_kv, 0)
            .await
    }

    /// Create a watch with a specific requested watch ID.
    ///
    /// If `requested_watch_id` is non-zero, the actor will attempt to use that
    /// ID. If it collides with an existing watcher, the returned receiver will
    /// be closed immediately. If `requested_watch_id` is 0, an ID is
    /// auto-assigned.
    pub async fn create_watch_with_id(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
        filters: Vec<i32>,
        prev_kv: bool,
        requested_watch_id: i64,
    ) -> (i64, mpsc::Receiver<WatchEvent>) {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(WatchHubCmd::CreateWatch {
                key,
                range_end,
                start_revision,
                filters,
                prev_kv,
                requested_watch_id,
                reply,
            })
            .await
            .expect("watch hub actor dead");
        rx.await.expect("watch hub actor dropped reply")
    }

    /// Cancel a watch. Returns true if the watcher existed and was removed.
    pub async fn cancel_watch(&self, watch_id: i64) -> bool {
        let (reply, rx) = oneshot::channel();
        self.cmd_tx
            .send(WatchHubCmd::CancelWatch { watch_id, reply })
            .await
            .expect("watch hub actor dead");
        rx.await.expect("watch hub actor dropped reply")
    }

    /// Notify all matching watchers of an event (fire-and-forget).
    ///
    /// The handle sends the command with backpressure (`.send().await`) but does
    /// not wait for the actor to finish processing the notification. This is
    /// important for performance since `notify` is called on every KV mutation.
    pub async fn notify(
        &self,
        key: Vec<u8>,
        event_type: i32,
        kv: mvccpb::KeyValue,
        prev_kv: Option<mvccpb::KeyValue>,
    ) {
        self.cmd_tx
            .send(WatchHubCmd::Notify {
                key,
                event_type,
                kv,
                prev_kv,
            })
            .await
            .expect("watch hub actor dead");
    }
}
