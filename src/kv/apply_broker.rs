use std::collections::HashMap;
use tokio::sync::{Mutex, oneshot};

use super::store::{DeleteResult, PutResult, TxnResult};

/// Result of applying a committed Raft entry to the KV store.
pub enum ApplyResult {
    Put(PutResult),
    DeleteRange(DeleteResult),
    Txn(TxnResult),
    Compact { revision: i64 },
    Noop,
}

/// Broker that connects the state machine (producer) with service handlers (consumers).
///
/// The state machine applies committed entries and sends results here.
/// Service handlers register waiters before proposing and receive results
/// after the state machine applies their entry.
pub struct ApplyResultBroker {
    inner: Mutex<BrokerInner>,
}

struct BrokerInner {
    waiters: HashMap<u64, oneshot::Sender<ApplyResult>>,
    results: HashMap<u64, ApplyResult>,
}

impl ApplyResultBroker {
    pub fn new() -> Self {
        ApplyResultBroker {
            inner: Mutex::new(BrokerInner {
                waiters: HashMap::new(),
                results: HashMap::new(),
            }),
        }
    }

    /// Called by the state machine after applying an entry.
    pub async fn send_result(&self, index: u64, result: ApplyResult) {
        let mut inner = self.inner.lock().await;
        if let Some(tx) = inner.waiters.remove(&index) {
            let _ = tx.send(result);
        } else {
            // Buffer the result — the service handler may not have registered yet.
            inner.results.insert(index, result);
        }
    }

    /// Called by service handlers to wait for the apply result at a given Raft log index.
    pub async fn wait_for_result(&self, index: u64) -> ApplyResult {
        let rx = {
            let mut inner = self.inner.lock().await;
            // Check if the state machine already applied this entry.
            if let Some(result) = inner.results.remove(&index) {
                return result;
            }
            // Register a waiter.
            let (tx, rx) = oneshot::channel();
            inner.waiters.insert(index, tx);
            rx
        };
        // Wait outside the lock.
        rx.await.expect("state machine dropped result sender")
    }
}
