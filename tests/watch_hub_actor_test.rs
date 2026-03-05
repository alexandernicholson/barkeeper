//! Tests for the WatchHub Rebar actor.

use tokio::time::{timeout, Duration};

use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::store::KvStore;
use barkeeper::proto::mvccpb;
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;

/// Helper: create a WatchHubActorHandle without a store (no historical replay).
fn make_runtime() -> Runtime {
    Runtime::new(1)
}

/// create_watch should return a working receiver that gets events from notify.
#[tokio::test]
async fn test_create_watch_returns_working_receiver() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (watch_id, mut rx) = hub.create_watch(b"foo".to_vec(), vec![], 0).await;
    assert!(watch_id > 0, "watch_id should be positive");

    let kv = mvccpb::KeyValue {
        key: b"foo".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"bar".to_vec(),
        lease: 0,
    };
    hub.notify(b"foo".to_vec(), 0, kv, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
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

/// notify should deliver events to matching watchers.
#[tokio::test]
async fn test_notify_delivers_to_matching_watcher() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (_wid, mut rx) = hub.create_watch(b"key1".to_vec(), vec![], 0).await;

    let kv = mvccpb::KeyValue {
        key: b"key1".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val1".to_vec(),
        lease: 0,
    };
    hub.notify(b"key1".to_vec(), 0, kv, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(event.events[0].kv.as_ref().unwrap().key, b"key1");
}

/// notify should NOT deliver events to non-matching watchers.
#[tokio::test]
async fn test_notify_does_not_deliver_to_non_matching_watcher() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (_wid, mut rx) = hub.create_watch(b"apple".to_vec(), vec![], 0).await;

    let kv = mvccpb::KeyValue {
        key: b"banana".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"yellow".to_vec(),
        lease: 0,
    };
    hub.notify(b"banana".to_vec(), 0, kv, None).await;

    // Give the actor time to process; should NOT receive anything.
    let result = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(result.is_err(), "should not receive event for non-matching key");
}

/// cancel_watch should stop event delivery.
#[tokio::test]
async fn test_cancel_watch_stops_delivery() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (watch_id, mut rx) = hub.create_watch(b"cancel_me".to_vec(), vec![], 0).await;

    // Cancel the watch.
    let existed = hub.cancel_watch(watch_id).await;
    assert!(existed, "watch should have existed");

    // Notify — should not arrive since the watch is cancelled.
    let kv = mvccpb::KeyValue {
        key: b"cancel_me".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val".to_vec(),
        lease: 0,
    };
    hub.notify(b"cancel_me".to_vec(), 0, kv, None).await;

    let result = timeout(Duration::from_millis(200), rx.recv()).await;
    // Either timeout (no event) or channel closed (None) — both are acceptable.
    match result {
        Err(_) => {} // timeout — good
        Ok(None) => {} // channel closed — also good
        Ok(Some(_)) => panic!("should not receive event after cancel"),
    }

    // Cancel again — should return false.
    let existed_again = hub.cancel_watch(watch_id).await;
    assert!(!existed_again, "second cancel should return false");
}

/// Range watcher (key + range_end) should match keys in range.
#[tokio::test]
async fn test_range_watcher() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    // Watch prefix "pfx/" — range_end is "pfx0" (next byte after '/')
    let (_wid, mut rx) = hub
        .create_watch(b"pfx/".to_vec(), b"pfx0".to_vec(), 0)
        .await;

    // Notify for "pfx/a" — should match.
    let kv1 = mvccpb::KeyValue {
        key: b"pfx/a".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val".to_vec(),
        lease: 0,
    };
    hub.notify(b"pfx/a".to_vec(), 0, kv1, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
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
    hub.notify(b"other".to_vec(), 0, kv2, None).await;

    let result = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(result.is_err(), "should not receive event for non-matching key");
}

/// All-keys watcher (range_end = "\x00") should match every key.
#[tokio::test]
async fn test_all_keys_watcher() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (_wid, mut rx) = hub
        .create_watch(b"\x00".to_vec(), b"\x00".to_vec(), 0)
        .await;

    let kv = mvccpb::KeyValue {
        key: b"anything".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val".to_vec(),
        lease: 0,
    };
    hub.notify(b"anything".to_vec(), 0, kv, None).await;

    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout")
        .expect("closed");
    assert_eq!(event.events[0].kv.as_ref().unwrap().key, b"anything");
}

/// Historical replay: create_watch with start_revision should replay past events.
#[tokio::test]
async fn test_historical_replay_with_kv_store() {
    let dir = tempfile::tempdir().unwrap();
    let kv_store = KvStore::open(dir.path().join("kv.redb")).unwrap();
    let kv_runtime = Runtime::new(1);
    let store_handle = spawn_kv_store_actor(&kv_runtime, std::sync::Arc::new(kv_store)).await;

    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, Some(store_handle.clone())).await;

    // Create some history via the actor handle.
    store_handle.put(b"hist".to_vec(), b"v1".to_vec(), 0).await.unwrap(); // rev 1
    store_handle.put(b"hist".to_vec(), b"v2".to_vec(), 0).await.unwrap(); // rev 2
    store_handle.put(b"other".to_vec(), b"v3".to_vec(), 0).await.unwrap(); // rev 3

    // Watch "hist" from revision 1 — should replay rev 1 and 2.
    let (_wid, mut rx) = hub.create_watch(b"hist".to_vec(), vec![], 1).await;

    // Should receive 2 historical events.
    let e1 = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout for e1")
        .expect("closed for e1");
    assert_eq!(e1.events[0].kv.as_ref().unwrap().value, b"v1");

    let e2 = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout for e2")
        .expect("closed for e2");
    assert_eq!(e2.events[0].kv.as_ref().unwrap().value, b"v2");
}

/// Dead watcher cleanup: dropping the receiver should cause the actor to
/// remove the watcher on the next notify.
#[tokio::test]
async fn test_dead_watcher_cleanup() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (watch_id, rx) = hub.create_watch(b"dead".to_vec(), vec![], 0).await;

    // Drop the receiver — this makes the watcher "dead".
    drop(rx);

    // Notify — the actor should detect the dead watcher and clean it up.
    let kv = mvccpb::KeyValue {
        key: b"dead".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"val".to_vec(),
        lease: 0,
    };
    hub.notify(b"dead".to_vec(), 0, kv, None).await;

    // Give the actor time to process the notify and clean up.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Cancelling should return false because the watcher was already cleaned up.
    let existed = hub.cancel_watch(watch_id).await;
    assert!(!existed, "watcher should have been cleaned up by notify");
}

/// Multiple concurrent watchers should each receive their own events.
#[tokio::test]
async fn test_concurrent_watchers() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    // Create 3 watchers on different keys.
    let (wid1, mut rx1) = hub.create_watch(b"k1".to_vec(), vec![], 0).await;
    let (wid2, mut rx2) = hub.create_watch(b"k2".to_vec(), vec![], 0).await;
    let (wid3, mut rx3) = hub.create_watch(b"k1".to_vec(), vec![], 0).await; // also watches k1

    // Notify k1 — should reach watcher 1 and 3, not 2.
    let kv = mvccpb::KeyValue {
        key: b"k1".to_vec(),
        create_revision: 1,
        mod_revision: 1,
        version: 1,
        value: b"v1".to_vec(),
        lease: 0,
    };
    hub.notify(b"k1".to_vec(), 0, kv, None).await;

    let e1 = timeout(Duration::from_secs(2), rx1.recv()).await.unwrap().unwrap();
    assert_eq!(e1.watch_id, wid1);

    let e3 = timeout(Duration::from_secs(2), rx3.recv()).await.unwrap().unwrap();
    assert_eq!(e3.watch_id, wid3);

    // rx2 should NOT receive.
    let r2 = timeout(Duration::from_millis(200), rx2.recv()).await;
    assert!(r2.is_err(), "watcher 2 should not receive k1 event");

    // Notify k2 — should reach watcher 2 only.
    let kv2 = mvccpb::KeyValue {
        key: b"k2".to_vec(),
        create_revision: 2,
        mod_revision: 2,
        version: 1,
        value: b"v2".to_vec(),
        lease: 0,
    };
    hub.notify(b"k2".to_vec(), 0, kv2, None).await;

    let e2 = timeout(Duration::from_secs(2), rx2.recv()).await.unwrap().unwrap();
    assert_eq!(e2.watch_id, wid2);
}

/// Watch IDs should be unique and increasing.
#[tokio::test]
async fn test_watch_ids_are_unique() {
    let rt = make_runtime();
    let hub = spawn_watch_hub_actor(&rt, None).await;

    let (id1, _rx1) = hub.create_watch(b"a".to_vec(), vec![], 0).await;
    let (id2, _rx2) = hub.create_watch(b"b".to_vec(), vec![], 0).await;
    let (id3, _rx3) = hub.create_watch(b"c".to_vec(), vec![], 0).await;

    assert!(id1 < id2, "IDs should be increasing");
    assert!(id2 < id3, "IDs should be increasing");
}
