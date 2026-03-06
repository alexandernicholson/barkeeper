//! Tests for revision-based watching.

use std::sync::Arc;

use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::store::KvStore;
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;
use tokio::time::{timeout, Duration};

/// changes_since should return mutations after the given revision.
#[tokio::test]
async fn test_changes_since_returns_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path()).unwrap());

    // Create 3 revisions.
    store.put(b"a", b"v1", 0).unwrap(); // rev 1
    store.put(b"b", b"v2", 0).unwrap(); // rev 2
    store.put(b"c", b"v3", 0).unwrap(); // rev 3

    // Get changes since revision 1 (should return revs 2 and 3).
    let changes = store.changes_since(1).unwrap();
    assert_eq!(changes.len(), 2, "should have 2 changes after rev 1");
    assert_eq!(changes[0].0, b"b"); // key
    assert_eq!(changes[1].0, b"c"); // key
}

/// changes_since(0) should return all mutations.
#[tokio::test]
async fn test_changes_since_zero_returns_all() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path()).unwrap());

    store.put(b"x", b"v1", 0).unwrap();
    store.put(b"y", b"v2", 0).unwrap();

    let changes = store.changes_since(0).unwrap();
    assert_eq!(changes.len(), 2);
}

/// changes_since should include delete events.
#[tokio::test]
async fn test_changes_since_includes_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path()).unwrap());

    store.put(b"del", b"v1", 0).unwrap(); // rev 1
    store.delete_range(b"del", b"").unwrap(); // rev 2

    let changes = store.changes_since(1).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0, b"del");
    assert_eq!(changes[0].1, 1); // DELETE event type
}

/// WatchHub with start_revision should replay historical events.
#[tokio::test]
async fn test_watchhub_replays_history() {
    let dir = tempfile::tempdir().unwrap();
    let kv_store = KvStore::open(dir.path()).unwrap();
    let kv_runtime = Runtime::new(1);
    let handle = spawn_kv_store_actor(&kv_runtime, std::sync::Arc::new(kv_store)).await;
    let watch_runtime = Runtime::new(1);
    let hub = spawn_watch_hub_actor(&watch_runtime, Some(handle.clone())).await;

    // Create some history via the actor handle.
    handle.put(b"hist".to_vec(), b"v1".to_vec(), 0).await.unwrap(); // rev 1
    handle.put(b"hist".to_vec(), b"v2".to_vec(), 0).await.unwrap(); // rev 2
    handle.put(b"other".to_vec(), b"v3".to_vec(), 0).await.unwrap(); // rev 3

    // Watch "hist" from revision 1 — should replay rev 1 and 2.
    let (_wid, mut rx) = hub.create_watch(b"hist".to_vec(), vec![], 1, vec![], false).await;

    // Should receive 2 historical events.
    let e1 = timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
    assert_eq!(e1.events[0].kv.as_ref().unwrap().value, b"v1");

    let e2 = timeout(Duration::from_secs(1), rx.recv()).await.unwrap().unwrap();
    assert_eq!(e2.events[0].kv.as_ref().unwrap().value, b"v2");
}
