//! Tests for the KvStoreActor — the Rebar actor replacement for Arc<KvStore>.
//!
//! Written TDD-style: tests define expected behavior, then the actor
//! implementation makes them pass.

use tempfile::TempDir;

use barkeeper::kv::actor::{spawn_kv_store_actor, KvStoreActorHandle};
use barkeeper::kv::store::{
    KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp,
};

/// Helper: create a temp dir, open a KvStore, spawn the actor, return handle.
async fn make_handle() -> (KvStoreActorHandle, TempDir) {
    let dir = TempDir::new().expect("create temp dir");
    let db_path = dir.path().join("test.redb");
    let store = KvStore::open(&db_path).expect("open kv store");

    let runtime = rebar_core::runtime::Runtime::new(1);
    let handle = spawn_kv_store_actor(&runtime, store).await;
    (handle, dir)
}

// ---------------------------------------------------------------------------
// Put and Range (single key)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_put_and_range_single_key() {
    let (handle, _dir) = make_handle().await;

    let result = handle
        .put(b"hello".to_vec(), b"world".to_vec(), 0)
        .await
        .expect("put should succeed");
    assert_eq!(result.revision, 1);
    assert!(result.prev_kv.is_none());

    let range = handle
        .range(b"hello".to_vec(), vec![], 0, 0)
        .await
        .expect("range should succeed");
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].key, b"hello");
    assert_eq!(range.kvs[0].value, b"world");
    assert_eq!(range.count, 1);
}

// ---------------------------------------------------------------------------
// Put and Range (range query)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_put_and_range_query() {
    let (handle, _dir) = make_handle().await;

    handle.put(b"a".to_vec(), b"1".to_vec(), 0).await.unwrap();
    handle.put(b"b".to_vec(), b"2".to_vec(), 0).await.unwrap();
    handle.put(b"c".to_vec(), b"3".to_vec(), 0).await.unwrap();

    // Range [a, d) should return all three keys.
    let range = handle
        .range(b"a".to_vec(), b"d".to_vec(), 0, 0)
        .await
        .expect("range should succeed");
    assert_eq!(range.kvs.len(), 3);
    assert_eq!(range.count, 3);

    // Range [a, b) should return only "a".
    let range = handle
        .range(b"a".to_vec(), b"b".to_vec(), 0, 0)
        .await
        .expect("range should succeed");
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].key, b"a");
}

// ---------------------------------------------------------------------------
// DeleteRange
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_delete_range() {
    let (handle, _dir) = make_handle().await;

    handle.put(b"foo".to_vec(), b"bar".to_vec(), 0).await.unwrap();

    let del = handle
        .delete_range(b"foo".to_vec(), vec![])
        .await
        .expect("delete should succeed");
    assert_eq!(del.deleted, 1);
    assert_eq!(del.prev_kvs.len(), 1);
    assert_eq!(del.prev_kvs[0].key, b"foo");

    // Key should be gone.
    let range = handle
        .range(b"foo".to_vec(), vec![], 0, 0)
        .await
        .expect("range should succeed");
    assert_eq!(range.kvs.len(), 0);
}

// ---------------------------------------------------------------------------
// Txn (compare-and-swap)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_txn_compare_and_swap() {
    let (handle, _dir) = make_handle().await;

    handle.put(b"key".to_vec(), b"val1".to_vec(), 0).await.unwrap();

    // Txn: if version == 1, put new value; else range.
    let compares = vec![TxnCompare {
        key: b"key".to_vec(),
        range_end: vec![],
        target: TxnCompareTarget::Version(1),
        result: TxnCompareResult::Equal,
    }];

    let success = vec![TxnOp::Put {
        key: b"key".to_vec(),
        value: b"val2".to_vec(),
        lease_id: 0,
    }];

    let failure = vec![TxnOp::Range {
        key: b"key".to_vec(),
        range_end: vec![],
        limit: 0,
        revision: 0,
    }];

    let result = handle
        .txn(compares, success, failure)
        .await
        .expect("txn should succeed");
    assert!(result.succeeded);
    assert_eq!(result.responses.len(), 1);

    // Verify the put went through.
    let range = handle
        .range(b"key".to_vec(), vec![], 0, 0)
        .await
        .unwrap();
    assert_eq!(range.kvs[0].value, b"val2");
}

// ---------------------------------------------------------------------------
// Compact
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compact() {
    let (handle, _dir) = make_handle().await;

    // Create multiple revisions.
    handle.put(b"k".to_vec(), b"v1".to_vec(), 0).await.unwrap();
    handle.put(b"k".to_vec(), b"v2".to_vec(), 0).await.unwrap();
    handle.put(b"k".to_vec(), b"v3".to_vec(), 0).await.unwrap();

    // Compact up to revision 2 — removes old entries.
    handle.compact(2).await.expect("compact should succeed");

    // Latest value should still be accessible.
    let range = handle
        .range(b"k".to_vec(), vec![], 0, 0)
        .await
        .unwrap();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].value, b"v3");
}

// ---------------------------------------------------------------------------
// CurrentRevision
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_current_revision() {
    let (handle, _dir) = make_handle().await;

    let rev = handle.current_revision().await.expect("should succeed");
    assert_eq!(rev, 0);

    handle.put(b"a".to_vec(), b"1".to_vec(), 0).await.unwrap();
    let rev = handle.current_revision().await.expect("should succeed");
    assert_eq!(rev, 1);

    handle.put(b"b".to_vec(), b"2".to_vec(), 0).await.unwrap();
    let rev = handle.current_revision().await.expect("should succeed");
    assert_eq!(rev, 2);
}

// ---------------------------------------------------------------------------
// ChangesSince
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_changes_since() {
    let (handle, _dir) = make_handle().await;

    handle.put(b"x".to_vec(), b"1".to_vec(), 0).await.unwrap();
    handle.put(b"y".to_vec(), b"2".to_vec(), 0).await.unwrap();
    handle.put(b"z".to_vec(), b"3".to_vec(), 0).await.unwrap();

    // Changes since revision 1 (exclusive) should return revisions 2 and 3.
    let changes = handle
        .changes_since(1)
        .await
        .expect("changes_since should succeed");
    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].0, b"y");
    assert_eq!(changes[0].1, 0); // PUT event
    assert_eq!(changes[1].0, b"z");
}

// ---------------------------------------------------------------------------
// Concurrent access (multiple handles)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_access() {
    let (handle, _dir) = make_handle().await;

    // Spawn 10 concurrent put operations.
    let mut tasks = Vec::new();
    for i in 0..10u8 {
        let h = handle.clone();
        tasks.push(tokio::spawn(async move {
            h.put(vec![i], vec![i], 0).await.expect("put should succeed")
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    // All 10 keys should exist.
    let rev = handle.current_revision().await.unwrap();
    assert_eq!(rev, 10);

    // Range [0x00, 0xFF) should have all 10.
    let range = handle
        .range(vec![0x00], vec![0xFF], 0, 0)
        .await
        .unwrap();
    assert_eq!(range.count, 10);
}

// ---------------------------------------------------------------------------
// DbFileSize, SnapshotBytes, CompactDb
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_db_file_size() {
    let (handle, _dir) = make_handle().await;

    let size = handle.db_file_size().await.expect("should succeed");
    assert!(size > 0, "db file should have non-zero size");
}

#[tokio::test]
async fn test_snapshot_bytes() {
    let (handle, _dir) = make_handle().await;

    handle.put(b"k".to_vec(), b"v".to_vec(), 0).await.unwrap();

    let bytes = handle.snapshot_bytes().await.expect("should succeed");
    assert!(!bytes.is_empty(), "snapshot should have data");
}

#[tokio::test]
async fn test_compact_db() {
    let (handle, _dir) = make_handle().await;

    let ok = handle.compact_db().await.expect("should succeed");
    assert!(ok);
}

// ---------------------------------------------------------------------------
// Handle is Clone and Send
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_handle_is_clone_and_send() {
    let (handle, _dir) = make_handle().await;
    let handle2 = handle.clone();

    // Both handles should work.
    handle.put(b"a".to_vec(), b"1".to_vec(), 0).await.unwrap();
    let range = handle2
        .range(b"a".to_vec(), vec![], 0, 0)
        .await
        .unwrap();
    assert_eq!(range.kvs.len(), 1);
}
