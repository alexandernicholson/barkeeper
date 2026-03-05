use barkeeper::kv::store::KvStore;
use tempfile::tempdir;

#[test]
fn test_empty_store() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();
    assert_eq!(store.current_revision().unwrap(), 0);

    let result = store.range(b"any", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 0);
    assert_eq!(result.count, 0);
}

#[test]
fn test_put_and_range() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    let put_result = store.put(b"hello", b"world", 0).unwrap();
    assert_eq!(put_result.revision, 1);
    assert!(put_result.prev_kv.is_none());

    let range_result = store.range(b"hello", b"", 0, 0).unwrap();
    assert_eq!(range_result.kvs.len(), 1);
    assert_eq!(range_result.kvs[0].key, b"hello");
    assert_eq!(range_result.kvs[0].value, b"world");
    assert_eq!(range_result.kvs[0].create_revision, 1);
    assert_eq!(range_result.kvs[0].mod_revision, 1);
    assert_eq!(range_result.kvs[0].version, 1);
}

#[test]
fn test_put_overwrite() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    // First put.
    store.put(b"key1", b"val1", 0).unwrap();

    // Overwrite.
    let result = store.put(b"key1", b"val2", 0).unwrap();
    assert_eq!(result.revision, 2);
    assert!(result.prev_kv.is_some());

    let prev = result.prev_kv.unwrap();
    assert_eq!(prev.value, b"val1");

    // Check the stored value.
    let range = store.range(b"key1", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 1);
    let kv = &range.kvs[0];
    assert_eq!(kv.value, b"val2");
    assert_eq!(kv.create_revision, 1); // create_revision stays the same
    assert_eq!(kv.mod_revision, 2); // mod_revision updated
    assert_eq!(kv.version, 2); // version incremented
}

#[test]
fn test_delete_range_single() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"foo", b"bar", 0).unwrap();
    assert_eq!(store.current_revision().unwrap(), 1);

    let del = store.delete_range(b"foo", b"").unwrap();
    assert_eq!(del.deleted, 1);
    assert_eq!(del.revision, 2);
    assert_eq!(del.prev_kvs.len(), 1);
    assert_eq!(del.prev_kvs[0].value, b"bar");

    // Key should no longer be visible.
    let range = store.range(b"foo", b"", 0, 0).unwrap();
    assert_eq!(range.kvs.len(), 0);
}

#[test]
fn test_delete_nonexistent() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    let del = store.delete_range(b"nokey", b"").unwrap();
    assert_eq!(del.deleted, 0);
    assert_eq!(del.prev_kvs.len(), 0);
    // Revision should not change.
    assert_eq!(store.current_revision().unwrap(), 0);
}

#[test]
fn test_range_prefix() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"aa", b"1", 0).unwrap();
    store.put(b"ab", b"2", 0).unwrap();
    store.put(b"ac", b"3", 0).unwrap();
    store.put(b"b", b"4", 0).unwrap();

    // Range [a, b) should return aa, ab, ac but not b.
    let result = store.range(b"a", b"b", 0, 0).unwrap();
    assert_eq!(result.count, 3);
    assert_eq!(result.kvs.len(), 3);
    assert_eq!(result.kvs[0].key, b"aa");
    assert_eq!(result.kvs[1].key, b"ab");
    assert_eq!(result.kvs[2].key, b"ac");
}

#[test]
fn test_revision_query() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"key", b"v1", 0).unwrap(); // rev 1
    store.put(b"key", b"v2", 0).unwrap(); // rev 2
    store.put(b"key", b"v3", 0).unwrap(); // rev 3

    // Query at revision 1: should see v1.
    let r1 = store.range(b"key", b"", 0, 1).unwrap();
    assert_eq!(r1.kvs.len(), 1);
    assert_eq!(r1.kvs[0].value, b"v1");

    // Query at revision 2: should see v2.
    let r2 = store.range(b"key", b"", 0, 2).unwrap();
    assert_eq!(r2.kvs.len(), 1);
    assert_eq!(r2.kvs[0].value, b"v2");

    // Query at latest (revision 0 means latest).
    let r_latest = store.range(b"key", b"", 0, 0).unwrap();
    assert_eq!(r_latest.kvs.len(), 1);
    assert_eq!(r_latest.kvs[0].value, b"v3");
}

#[test]
fn test_range_limit() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path()).unwrap();

    store.put(b"a", b"1", 0).unwrap();
    store.put(b"b", b"2", 0).unwrap();
    store.put(b"c", b"3", 0).unwrap();

    // Range [a, d) with limit 2.
    let result = store.range(b"a", b"d", 2, 0).unwrap();
    assert_eq!(result.kvs.len(), 2);
    assert_eq!(result.count, 3); // total count is 3
    assert!(result.more); // there are more keys
    assert_eq!(result.kvs[0].key, b"a");
    assert_eq!(result.kvs[1].key, b"b");
}
