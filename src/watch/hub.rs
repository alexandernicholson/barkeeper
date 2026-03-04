use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::kv::actor::KvStoreActorHandle;
use crate::proto::mvccpb;

/// A single watcher watching a key or range.
struct Watcher {
    id: i64,
    key: Vec<u8>,
    range_end: Vec<u8>,
    tx: mpsc::Sender<WatchEvent>,
    start_revision: i64,
}

/// Event to send to a watcher.
#[derive(Debug, Clone)]
pub struct WatchEvent {
    pub watch_id: i64,
    pub events: Vec<mvccpb::Event>,
    pub compact_revision: i64,
}

/// The WatchHub maintains a set of watchers and fans out KV mutation events
/// to all matching watchers.
pub struct WatchHub {
    watchers: Arc<Mutex<HashMap<i64, Watcher>>>,
    next_id: Arc<Mutex<i64>>,
    store: Option<KvStoreActorHandle>,
}

impl WatchHub {
    pub fn new() -> Self {
        WatchHub {
            watchers: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            store: None,
        }
    }

    /// Create a WatchHub with an attached KvStore actor handle for historical replay.
    pub fn with_store(store: KvStoreActorHandle) -> Self {
        WatchHub {
            watchers: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(1)),
            store: Some(store),
        }
    }

    /// Create a watch and return (watch_id, event_receiver).
    ///
    /// If `start_revision > 0` and the hub has a store reference, historical
    /// events from that revision onward are replayed to the watcher before it
    /// begins receiving live events.
    pub async fn create_watch(
        &self,
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
    ) -> (i64, mpsc::Receiver<WatchEvent>) {
        let (tx, rx) = mpsc::channel(256);

        let mut next_id = self.next_id.lock().await;
        let id = *next_id;
        *next_id += 1;
        drop(next_id);

        // Clone key/range_end/tx for replay before the watcher takes ownership.
        let replay_key = key.clone();
        let replay_range_end = range_end.clone();
        let replay_tx = tx.clone();

        let watcher = Watcher {
            id,
            key,
            range_end,
            tx,
            start_revision,
        };

        self.watchers.lock().await.insert(id, watcher);

        // Replay historical events if start_revision > 0 and we have a store.
        if start_revision > 0 {
            if let Some(ref store) = self.store {
                // changes_since is exclusive, so pass start_revision - 1
                // to include events AT start_revision.
                match store.changes_since(start_revision - 1).await {
                    Ok(changes) => {
                        for (change_key, event_type, kv) in changes {
                            if !key_matches(&replay_key, &replay_range_end, &change_key) {
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
                            if replay_tx.send(watch_event).await.is_err() {
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

        (id, rx)
    }

    /// Cancel a watch. Returns true if the watcher existed and was removed.
    pub async fn cancel_watch(&self, watch_id: i64) -> bool {
        self.watchers.lock().await.remove(&watch_id).is_some()
    }

    /// Notify all matching watchers of an event.
    ///
    /// This checks each watcher's key/range_end against the event key and sends
    /// the event to all matching watchers.
    pub async fn notify(
        &self,
        key: &[u8],
        event_type: i32,
        kv: mvccpb::KeyValue,
        prev_kv: Option<mvccpb::KeyValue>,
    ) {
        let watchers = self.watchers.lock().await;
        let mut dead_ids = Vec::new();

        for (id, watcher) in watchers.iter() {
            if !key_matches(&watcher.key, &watcher.range_end, key) {
                continue;
            }

            let event = mvccpb::Event {
                r#type: event_type,
                kv: Some(kv.clone()),
                prev_kv: prev_kv.clone(),
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

        // We cannot remove from the map while holding the lock from the
        // iteration above — but we already hold it. Drop and re-acquire.
        drop(watchers);

        if !dead_ids.is_empty() {
            let mut watchers = self.watchers.lock().await;
            for id in dead_ids {
                watchers.remove(&id);
            }
        }
    }
}

/// Check whether a key matches a watcher's key/range_end filter.
///
/// - If `range_end` is empty, the watcher matches only the exact key.
/// - If `range_end` is `[0]` (`\x00`), the watcher matches all keys >= key.
/// - Otherwise the watcher matches keys in the half-open range [key, range_end).
pub fn key_matches(watcher_key: &[u8], range_end: &[u8], event_key: &[u8]) -> bool {
    if range_end.is_empty() {
        // Exact match.
        event_key == watcher_key
    } else if range_end == b"\x00" {
        // All keys >= watcher_key.
        event_key >= watcher_key
    } else {
        // Range [key, range_end).
        event_key >= watcher_key && event_key < range_end
    }
}
