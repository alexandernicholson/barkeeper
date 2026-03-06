//! Tests verifying barkeeper meets kube-apiserver's etcd expectations.
//!
//! These tests exercise the exact operations kube-apiserver performs against etcd,
//! based on analysis of kubernetes/apiserver pkg/storage/etcd3/.

use std::sync::Arc;
use tokio::time::{timeout, Duration};

use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::store::KvStore;
use barkeeper::watch::actor::spawn_watch_hub_actor;
use barkeeper::watch::hub::WatchEvent;
use rebar_core::runtime::Runtime;

fn make_runtime() -> Runtime {
    Runtime::new(1)
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. Range: CountOnly
//
// kube-apiserver calls `client.KV.Get(ctx, prefix, WithPrefix(), WithCountOnly())`
// to poll the etcd_object_counts metric. CountOnly should return count without kvs.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_range_count_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"/registry/pods/a", b"data-a", 0).unwrap();
    store.put(b"/registry/pods/b", b"data-b", 0).unwrap();
    store.put(b"/registry/pods/c", b"data-c", 0).unwrap();

    // CountOnly: should return count=3, kvs=[], more=false
    let result = store
        .range_with_options(b"/registry/pods/", b"/registry/pods0", 0, 0, true, false)
        .unwrap();
    assert_eq!(result.count, 3, "count should be 3");
    assert!(result.kvs.is_empty(), "kvs should be empty with count_only");
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. Range: KeysOnly
//
// kube-apiserver calls `client.KV.Get(ctx, prefix, WithPrefix(), WithKeysOnly())`
// for corrupted object deletion scan and key enumeration. KeysOnly returns keys
// with empty values.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_range_keys_only() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"/registry/pods/a", b"large-pod-data", 0).unwrap();
    store.put(b"/registry/pods/b", b"large-pod-data", 0).unwrap();

    // KeysOnly: should return keys with empty values
    let result = store
        .range_with_options(b"/registry/pods/", b"/registry/pods0", 0, 0, false, true)
        .unwrap();
    assert_eq!(result.count, 2, "count should be 2");
    assert_eq!(result.kvs.len(), 2, "should return 2 kvs");
    assert_eq!(result.kvs[0].key, b"/registry/pods/a");
    assert!(
        result.kvs[0].value.is_empty(),
        "value should be empty with keys_only"
    );
    assert_eq!(result.kvs[1].key, b"/registry/pods/b");
    assert!(
        result.kvs[1].value.is_empty(),
        "value should be empty with keys_only"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. Watch compaction error
//
// kube-apiserver expects the error "mvcc: required revision has been compacted"
// when starting a watch at a revision that was already compacted. The watch
// cache detects the closed watch and initiates a full re-list.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_watch_at_compacted_revision_returns_error() {
    let rt = make_runtime();
    let dir = tempfile::tempdir().unwrap();
    let store_raw = KvStore::open(dir.path()).unwrap();

    // Create some revisions.
    store_raw.put(b"key1", b"v1", 0).unwrap(); // rev 1
    store_raw.put(b"key2", b"v2", 0).unwrap(); // rev 2
    store_raw.put(b"key3", b"v3", 0).unwrap(); // rev 3

    // Compact up to revision 2 — revisions 1 and 2 are pruned.
    store_raw.compact(2).unwrap();

    let store = spawn_kv_store_actor(&rt, Arc::new(store_raw)).await;
    let hub = spawn_watch_hub_actor(&rt, Some(store)).await;

    // Start a watch at revision 1, which has been compacted.
    let (watch_id, mut rx) = hub
        .create_watch(b"key1".to_vec(), vec![], 1, vec![], false)
        .await;

    // Should receive a compaction error event (watch canceled with compact_revision).
    let event = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("timeout waiting for compaction error")
        .expect("channel closed");

    assert!(
        event.compact_revision > 0,
        "compact_revision should be set when watch starts at compacted revision, got: {}",
        event.compact_revision
    );
    // The channel should close after the error (watch is canceled).
    let next = timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        matches!(next, Ok(None)) || next.is_err(),
        "channel should close after compaction error"
    );
    let _ = watch_id;
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. Watch progress notifications (server-initiated periodic)
//
// kube-apiserver creates watches with WithProgressNotify(). The server must
// periodically send empty WatchResponse with the current revision to keep
// the watch cache's resource version current during quiet periods.
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_watch_progress_notify_periodic() {
    let rt = make_runtime();
    let dir = tempfile::tempdir().unwrap();
    let store_raw = KvStore::open(dir.path()).unwrap();
    store_raw.put(b"key", b"val", 0).unwrap(); // rev 1

    let store = spawn_kv_store_actor(&rt, Arc::new(store_raw)).await;
    let hub = spawn_watch_hub_actor(&rt, Some(store)).await;

    // Create a watch with progress_notify=true. No mutations happen.
    let (_wid, mut rx): (i64, tokio::sync::mpsc::Receiver<WatchEvent>) = hub
        .create_watch_with_progress(b"key".to_vec(), vec![], 0, vec![], false, true)
        .await;

    // Should receive a progress notification within a reasonable time (10s).
    // etcd's default progress interval is 10 minutes, but for kube-apiserver
    // compatibility we need some interval. Using a short timeout for testing.
    let event = timeout(Duration::from_secs(15), rx.recv())
        .await
        .expect("timeout waiting for progress notification")
        .expect("channel closed");

    assert!(
        event.events.is_empty(),
        "progress notification should have no events"
    );
    assert!(
        event.watch_id > 0,
        "progress notification should have valid watch_id"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 5. Lease attachment for Txn Put
//
// kube-apiserver attaches leases to keys via Txn Put (e.g. event objects with TTL).
// The LeaseManager must track keys written via Txn so that lease expiry/revoke
// deletes those keys.
// ─────────────────────────────────────────────────────────────────────────────

// This test operates at the KvStore level to verify the lease_id is stored.
// The state machine integration (LeaseManager.attach_key) is tested separately.

#[test]
fn test_txn_put_stores_lease_id() {
    use barkeeper::kv::store::TxnOp;

    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    let result = store
        .txn(
            &[],
            &[TxnOp::Put {
                key: b"/registry/events/ev1".to_vec(),
                value: b"event-data".to_vec(),
                lease_id: 12345,
            }],
            &[],
        )
        .unwrap();

    assert!(result.succeeded);

    // The stored key should have the lease_id set.
    let range = store.range(b"/registry/events/ev1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(
        range.kvs[0].lease, 12345,
        "lease should be attached to key written via Txn Put"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 6. Maintenance.Status version string
//
// kube-apiserver uses the version string from Status to construct storage-versions
// capability map. It expects an etcd-format version like "3.5.x".
// ─────────────────────────────────────────────────────────────────────────────

// This requires an integration test against the actual gRPC service, so we test
// at the function level. The version should start with "3.5" for K8s compat.

#[test]
fn test_etcd_version_string() {
    // The barkeeper binary should report an etcd-compatible version string.
    // This is set in maintenance_service.rs. For now, verify the expected format.
    let version = barkeeper::etcd_version();
    assert!(
        version.starts_with("3.5"),
        "version should be etcd-compatible (3.5.x), got: {}",
        version
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// 7. Compact tracks compacted revision floor
//
// After compact(rev), the store should track that revision as the compacted floor.
// Watches starting below this floor should be rejected.
// changes_since below this floor should return an error.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn test_compact_tracks_floor_revision() {
    let dir = tempfile::tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"a", b"1", 0).unwrap(); // rev 1
    store.put(b"b", b"2", 0).unwrap(); // rev 2
    store.put(b"c", b"3", 0).unwrap(); // rev 3

    store.compact(2).unwrap();

    // compacted_revision() should return 2.
    assert_eq!(
        store.compacted_revision().unwrap(),
        2,
        "compacted_revision should track the floor"
    );

    // changes_since(0) should return an error since revision 1 was compacted.
    let result = store.changes_since(0);
    assert!(
        result.is_err(),
        "changes_since below compacted floor should return error"
    );
}
