//! Tests for revision-based watching.

use std::sync::Arc;

use barkeeper::kv::store::KvStore;

/// changes_since should return mutations after the given revision.
#[tokio::test]
async fn test_changes_since_returns_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

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
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

    store.put(b"x", b"v1", 0).unwrap();
    store.put(b"y", b"v2", 0).unwrap();

    let changes = store.changes_since(0).unwrap();
    assert_eq!(changes.len(), 2);
}

/// changes_since should include delete events.
#[tokio::test]
async fn test_changes_since_includes_deletes() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("kv.redb")).unwrap());

    store.put(b"del", b"v1", 0).unwrap(); // rev 1
    store.delete_range(b"del", b"").unwrap(); // rev 2

    let changes = store.changes_since(1).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].0, b"del");
    assert_eq!(changes[0].1, 1); // DELETE event type
}
