//! Tests that watch notifications fire for mutations inside transactions.

use std::sync::Arc;
use tokio::time::{timeout, Duration};

use barkeeper::kv::store::KvStore;
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;

/// A put inside a txn should fire a watch notification.
#[tokio::test]
async fn test_txn_put_fires_watch_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path()).unwrap());
    let runtime = Runtime::new(1);
    let hub = spawn_watch_hub_actor(&runtime, None).await;

    // Watch key "txnkey".
    let (_wid, mut rx) = hub.create_watch(b"txnkey".to_vec(), vec![], 0, vec![], false).await;

    // Execute a txn that puts "txnkey".
    let result = store.txn(
        &[], // no compares
        &[barkeeper::kv::store::TxnOp::Put {
            key: b"txnkey".to_vec(),
            value: b"txnval".to_vec(),
            lease_id: 0,
        }],
        &[],
    ).unwrap();

    // Fire notifications for the txn results.
    for resp in &result.responses {
        match resp {
            barkeeper::kv::store::TxnOpResponse::Put(r) => {
                let (create_rev, ver) = match &r.prev_kv {
                    Some(prev) => (prev.create_revision, prev.version + 1),
                    None => (r.revision, 1),
                };
                let kv = barkeeper::proto::mvccpb::KeyValue {
                    key: b"txnkey".to_vec(),
                    create_revision: create_rev,
                    mod_revision: r.revision,
                    version: ver,
                    value: b"txnval".to_vec(),
                    lease: 0,
                };
                hub.notify(b"txnkey".to_vec(), 0, kv, r.prev_kv.clone()).await;
            }
            _ => {}
        }
    }

    let event = timeout(Duration::from_secs(1), rx.recv()).await
        .expect("should receive event within 1s")
        .expect("channel should not be closed");

    assert_eq!(event.events.len(), 1);
    assert_eq!(event.events[0].r#type, 0); // PUT
    let kv = event.events[0].kv.as_ref().unwrap();
    assert_eq!(kv.key, b"txnkey");
    assert_eq!(kv.value, b"txnval");
}

/// A delete inside a txn should fire a watch notification.
#[tokio::test]
async fn test_txn_delete_fires_watch_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path()).unwrap());
    let runtime = Runtime::new(1);
    let hub = spawn_watch_hub_actor(&runtime, None).await;

    // Pre-populate key.
    store.put(b"delkey", b"val", 0).unwrap();

    // Watch key "delkey".
    let (_wid, mut rx) = hub.create_watch(b"delkey".to_vec(), vec![], 0, vec![], false).await;

    // Execute a txn that deletes "delkey".
    let result = store.txn(
        &[],
        &[barkeeper::kv::store::TxnOp::DeleteRange {
            key: b"delkey".to_vec(),
            range_end: vec![],
        }],
        &[],
    ).unwrap();

    // Fire notifications for the txn results.
    for resp in &result.responses {
        match resp {
            barkeeper::kv::store::TxnOpResponse::DeleteRange(r) => {
                for prev in &r.prev_kvs {
                    let tombstone = barkeeper::proto::mvccpb::KeyValue {
                        key: prev.key.clone(),
                        create_revision: 0,
                        mod_revision: r.revision,
                        version: 0,
                        value: vec![],
                        lease: 0,
                    };
                    hub.notify(prev.key.clone(), 1, tombstone, Some(prev.clone())).await;
                }
            }
            _ => {}
        }
    }

    let event = timeout(Duration::from_secs(1), rx.recv()).await
        .expect("should receive event within 1s")
        .expect("channel should not be closed");

    assert_eq!(event.events.len(), 1);
    assert_eq!(event.events[0].r#type, 1); // DELETE
}
