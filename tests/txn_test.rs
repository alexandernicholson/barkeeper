use barkeeper::kv::store::{
    KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse,
};
use tempfile::tempdir;

#[test]
fn test_txn_success_path() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    // Put a key so it exists at version 1.
    store.put(b"key1", b"val1", 0).unwrap();

    // Txn: if version("key1") == 1, then put key1=val2.
    let compares = vec![TxnCompare {
        key: b"key1".to_vec(),
        range_end: vec![],
        target: TxnCompareTarget::Version(1),
        result: TxnCompareResult::Equal,
    }];
    let success = vec![TxnOp::Put {
        key: b"key1".to_vec(),
        value: b"val2".to_vec(),
        lease_id: 0,
    }];
    let failure = vec![];

    let result = store.txn(&compares, &success, &failure).unwrap();
    assert!(result.succeeded, "txn should succeed when version matches");
    assert_eq!(result.responses.len(), 1);

    // Verify the new value is set.
    let range = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].value, b"val2");
    assert_eq!(range.kvs[0].version, 2);
}

#[test]
fn test_txn_failure_path() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    // Put a key so it exists at version 1.
    store.put(b"key1", b"val1", 0).unwrap();

    // Txn: if version("key1") == 99, then put key1=A, else put key1=B.
    let compares = vec![TxnCompare {
        key: b"key1".to_vec(),
        range_end: vec![],
        target: TxnCompareTarget::Version(99),
        result: TxnCompareResult::Equal,
    }];
    let success = vec![TxnOp::Put {
        key: b"key1".to_vec(),
        value: b"A".to_vec(),
        lease_id: 0,
    }];
    let failure = vec![TxnOp::Put {
        key: b"key1".to_vec(),
        value: b"B".to_vec(),
        lease_id: 0,
    }];

    let result = store.txn(&compares, &success, &failure).unwrap();
    assert!(
        !result.succeeded,
        "txn should fail when version does not match"
    );
    assert_eq!(result.responses.len(), 1);

    // Verify the failure path value is set (B, not A).
    let range = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 1);
    assert_eq!(range.kvs[0].value, b"B");
}

#[test]
fn test_txn_with_range_op() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    store.put(b"foo", b"bar", 0).unwrap();

    // Txn: if key "foo" exists (version > 0), then range it.
    let compares = vec![TxnCompare {
        key: b"foo".to_vec(),
        range_end: vec![],
        target: TxnCompareTarget::Version(0),
        result: TxnCompareResult::Greater,
    }];
    let success = vec![TxnOp::Range {
        key: b"foo".to_vec(),
        range_end: vec![],
        limit: 0,
        revision: 0,
    }];
    let failure = vec![];

    let result = store.txn(&compares, &success, &failure).unwrap();
    assert!(result.succeeded);
    assert_eq!(result.responses.len(), 1);

    match &result.responses[0] {
        TxnOpResponse::Range(r) => {
            assert_eq!(r.kvs.len(), 1);
            assert_eq!(r.kvs[0].key, b"foo");
            assert_eq!(r.kvs[0].value, b"bar");
        }
        _ => panic!("expected Range response"),
    }
}

#[test]
fn test_txn_with_delete_op() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    store.put(b"key1", b"val1", 0).unwrap();

    // Txn: unconditional (no compares), delete key1.
    let compares = vec![];
    let success = vec![TxnOp::DeleteRange {
        key: b"key1".to_vec(),
        range_end: vec![],
    }];
    let failure = vec![];

    let result = store.txn(&compares, &success, &failure).unwrap();
    assert!(result.succeeded, "empty compares should always succeed");
    assert_eq!(result.responses.len(), 1);

    match &result.responses[0] {
        TxnOpResponse::DeleteRange(r) => {
            assert_eq!(r.deleted, 1);
        }
        _ => panic!("expected DeleteRange response"),
    }

    // Verify key is gone.
    let range = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 0);
}

#[test]
fn test_txn_value_compare() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    store.put(b"key1", b"hello", 0).unwrap();

    // Txn: if value("key1") == "hello", then put key1="world".
    let compares = vec![TxnCompare {
        key: b"key1".to_vec(),
        range_end: vec![],
        target: TxnCompareTarget::Value(b"hello".to_vec()),
        result: TxnCompareResult::Equal,
    }];
    let success = vec![TxnOp::Put {
        key: b"key1".to_vec(),
        value: b"world".to_vec(),
        lease_id: 0,
    }];
    let failure = vec![];

    let result = store.txn(&compares, &success, &failure).unwrap();
    assert!(result.succeeded);

    let range = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs[0].value, b"world");
}

#[test]
fn test_compact() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    // Put key at rev 1, update at rev 2, update at rev 3.
    store.put(b"key", b"v1", 0).unwrap(); // rev 1
    store.put(b"key", b"v2", 0).unwrap(); // rev 2
    store.put(b"key", b"v3", 0).unwrap(); // rev 3

    // Before compact: old revisions are accessible.
    let r1 = store.range(b"key", b"", 0, 1).unwrap();
    assert_eq!(r1.kvs.len(), 1);
    assert_eq!(r1.kvs[0].value, b"v1");

    // Compact at revision 2 (remove entries with revision < 2).
    store.compact(2).unwrap();

    // After compact: revision 1 should no longer be accessible
    // (the old entry at rev 1 was removed).
    let r1_after = store.range(b"key", b"", 0, 1).unwrap();
    assert_eq!(
        r1_after.kvs.len(),
        0,
        "old revision should be compacted away"
    );

    // Revision 2 should still be accessible (it was the latest at compact point).
    let r2 = store.range(b"key", b"", 0, 2).unwrap();
    assert_eq!(r2.kvs.len(), 1);
    assert_eq!(r2.kvs[0].value, b"v2");

    // Latest revision (3) should still be accessible.
    let r3 = store.range(b"key", b"", 0, 0).unwrap();
    assert_eq!(r3.kvs.len(), 1);
    assert_eq!(r3.kvs[0].value, b"v3");
}

#[test]
fn test_compact_multiple_keys() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();

    store.put(b"a", b"1", 0).unwrap(); // rev 1
    store.put(b"b", b"2", 0).unwrap(); // rev 2
    store.put(b"a", b"3", 0).unwrap(); // rev 3

    // Compact at revision 2: keeps latest for "a" (rev 1 since rev 3 > 2)
    // and latest for "b" (rev 2).
    store.compact(2).unwrap();

    // Key "a" at revision 1 should still be there (it's the latest at or before rev 2).
    let ra = store.range(b"a", b"", 0, 2).unwrap();
    assert_eq!(ra.kvs.len(), 1);
    assert_eq!(ra.kvs[0].value, b"1");

    // Key "b" at revision 2 should still be there.
    let rb = store.range(b"b", b"", 0, 2).unwrap();
    assert_eq!(rb.kvs.len(), 1);
    assert_eq!(rb.kvs[0].value, b"2");

    // Key "a" at latest (rev 3) should still be accessible.
    let ra_latest = store.range(b"a", b"", 0, 0).unwrap();
    assert_eq!(ra_latest.kvs.len(), 1);
    assert_eq!(ra_latest.kvs[0].value, b"3");
}
