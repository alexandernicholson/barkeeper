use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone)]
pub struct ApplyNotifier {
    inner: Arc<ApplyNotifierInner>,
}

struct ApplyNotifierInner {
    applied: AtomicU64,
    tx: watch::Sender<u64>,
    rx: watch::Receiver<u64>,
}

impl ApplyNotifier {
    pub fn new(initial: u64) -> Self {
        let (tx, rx) = watch::channel(initial);
        Self {
            inner: Arc::new(ApplyNotifierInner {
                applied: AtomicU64::new(initial),
                tx,
                rx,
            }),
        }
    }

    /// Wait until applied index >= target. Returns immediately if already there.
    pub async fn wait_for(&self, target: u64) {
        if self.inner.applied.load(Ordering::Acquire) >= target {
            return;
        }
        let mut rx = self.inner.rx.clone();
        while *rx.borrow_and_update() < target {
            if rx.changed().await.is_err() {
                return; // sender dropped
            }
        }
    }

    /// Advance the applied index and wake all waiters.
    pub fn advance(&self, index: u64) {
        self.inner.applied.store(index, Ordering::Release);
        let _ = self.inner.tx.send(index);
    }

    /// Return current applied index.
    pub fn current(&self) -> u64 {
        self.inner.applied.load(Ordering::Acquire)
    }
}
