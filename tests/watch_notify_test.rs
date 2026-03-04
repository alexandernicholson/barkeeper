//! Tests that KV mutations trigger WatchHub notifications.

use std::sync::Arc;
use tokio::time::{timeout, Duration};

use barkeeper::kv::store::KvStore;
use barkeeper::watch::hub::WatchHub;
use barkeeper::proto::mvccpb;

/// After a put, the WatchHub should receive a notification for watchers on that key.
#[tokio::test]
async fn test_watch_hub_receives_put_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());
    let hub = Arc::new(WatchHub::new());

    // Create a watch on key "foo".
    let (watch_id, mut event_rx) = hub.create_watch(b"foo".to_vec(), vec![], 0).await;
    assert!(watch_id > 0);

    // Simulate what StoreProcess should do after a put: call hub.notify().
    let result = store.put(b"foo", b"bar", 0).unwrap();
    let kv = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        create_revision: result.revision,
        mod_revision: result.revision,
        version: 1,
        value: b"bar".to_vec(),
        lease: 0,
    };
    hub.notify(b"foo", 0, kv, None).await; // 0 = PUT

    // The watcher should receive the event.
    let event = timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .expect("timeout waiting for watch event")
        .expect("watch channel closed");

    assert_eq!(event.watch_id, watch_id);
    assert_eq!(event.events.len(), 1);
    assert_eq!(event.events[0].r#type, 0); // PUT
    let ev_kv = event.events[0].kv.as_ref().unwrap();
    assert_eq!(ev_kv.key, b"foo");
    assert_eq!(ev_kv.value, b"bar");
}

/// After a delete, the WatchHub should receive a DELETE notification.
#[tokio::test]
async fn test_watch_hub_receives_delete_notification() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());
    let hub = Arc::new(WatchHub::new());

    // Put then create watch.
    store.put(b"delme", b"val", 0).unwrap();
    let (_watch_id, mut event_rx) = hub.create_watch(b"delme".to_vec(), vec![], 0).await;

    // Delete and notify.
    let result = store.delete_range(b"delme", b"").unwrap();
    assert_eq!(result.deleted, 1);

    let kv = mvccpb::KeyValue {
        key: b"delme".to_vec(),
        create_revision: 0,
        mod_revision: result.revision,
        version: 0,
        value: vec![],
        lease: 0,
    };
    hub.notify(b"delme", 1, kv, None).await; // 1 = DELETE

    let event = timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .expect("timeout")
        .expect("closed");

    assert_eq!(event.events[0].r#type, 1); // DELETE
}

/// Watch with prefix range_end should match multiple keys.
#[tokio::test]
async fn test_watch_hub_prefix_notification() {
    let hub = Arc::new(WatchHub::new());

    // Watch prefix "pfx/" — range_end is "pfx0" (next byte after '/')
    let (_watch_id, mut event_rx) = hub
        .create_watch(b"pfx/".to_vec(), b"pfx0".to_vec(), 0)
        .await;

    // Notify for "pfx/a" — should match.
    let kv = mvccpb::KeyValue {
        key: b"pfx/a".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val".to_vec(),
        lease: 0,
    };
    hub.notify(b"pfx/a", 0, kv, None).await;

    let event = timeout(Duration::from_secs(2), event_rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(event.events[0].kv.as_ref().unwrap().key, b"pfx/a");

    // Notify for "other" — should NOT match.
    let kv2 = mvccpb::KeyValue {
        key: b"other".to_vec(),
        create_revision: 2,
        mod_revision: 2,
        version: 1,
        value: b"nope".to_vec(),
        lease: 0,
    };
    hub.notify(b"other", 0, kv2, None).await;

    // Give it a moment — should NOT receive anything.
    let result = timeout(Duration::from_millis(200), event_rx.recv()).await;
    assert!(result.is_err(), "should not receive event for non-matching key");
}
