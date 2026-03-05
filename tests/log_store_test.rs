use barkeeper::raft::log_store::LogStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use barkeeper::raft::state::PersistentState;
use tempfile::tempdir;

fn make_entry(term: u64, index: u64, data: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        data: LogEntryData::Command { data: data.as_bytes().to_vec(), revision: 0 },
    }
}

#[test]
fn test_open_and_empty() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();
    assert!(store.is_empty().unwrap());
    assert_eq!(store.last_index().unwrap(), 0);
    assert_eq!(store.last_term().unwrap(), 0);
}

#[test]
fn test_append_and_get() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let entries = vec![
        make_entry(1, 1, "put a 1"),
        make_entry(1, 2, "put b 2"),
        make_entry(2, 3, "put c 3"),
    ];
    store.append(&entries).unwrap();

    assert_eq!(store.len().unwrap(), 3);
    assert_eq!(store.last_index().unwrap(), 3);
    assert_eq!(store.last_term().unwrap(), 2);

    let entry = store.get(2).unwrap().unwrap();
    assert_eq!(entry.term, 1);
    assert_eq!(entry.index, 2);
}

#[test]
fn test_get_range() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let entries = vec![
        make_entry(1, 1, "a"),
        make_entry(1, 2, "b"),
        make_entry(2, 3, "c"),
        make_entry(2, 4, "d"),
    ];
    store.append(&entries).unwrap();

    let range = store.get_range(2, 3).unwrap();
    assert_eq!(range.len(), 2);
    assert_eq!(range[0].index, 2);
    assert_eq!(range[1].index, 3);
}

#[test]
fn test_truncate_after() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let entries = vec![
        make_entry(1, 1, "a"),
        make_entry(1, 2, "b"),
        make_entry(2, 3, "c"),
        make_entry(2, 4, "d"),
    ];
    store.append(&entries).unwrap();

    store.truncate_after(2).unwrap();
    assert_eq!(store.len().unwrap(), 2);
    assert_eq!(store.last_index().unwrap(), 2);
    assert!(store.get(3).unwrap().is_none());
}

#[test]
fn test_term_at() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    store.append(&[make_entry(5, 1, "x")]).unwrap();
    assert_eq!(store.term_at(1).unwrap(), Some(5));
    assert_eq!(store.term_at(99).unwrap(), None);
}

#[test]
fn test_hard_state_persistence() {
    let dir = tempdir().unwrap();

    {
        let store = LogStore::open(dir.path()).unwrap();
        assert!(store.load_hard_state().unwrap().is_none());

        let state = PersistentState {
            current_term: 5,
            voted_for: Some(3),
        };
        store.save_hard_state(&state).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = LogStore::open(dir.path()).unwrap();
        let state = store.load_hard_state().unwrap().unwrap();
        assert_eq!(state.current_term, 5);
        assert_eq!(state.voted_for, Some(3));
    }
}

#[test]
fn test_overwrite_entries() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    store.append(&[make_entry(1, 1, "original")]).unwrap();
    store.append(&[make_entry(2, 1, "overwritten")]).unwrap();

    let entry = store.get(1).unwrap().unwrap();
    assert_eq!(entry.term, 2);
}

#[test]
fn test_flush_entries_and_hard_state() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let entries = vec![
        make_entry(1, 1, "a"),
        make_entry(1, 2, "b"),
    ];
    let state = PersistentState {
        current_term: 3,
        voted_for: Some(1),
    };

    store.flush(&entries, Some(&state)).unwrap();

    assert_eq!(store.len().unwrap(), 2);
    assert_eq!(store.last_index().unwrap(), 2);
    let entry = store.get(1).unwrap().unwrap();
    assert_eq!(entry.term, 1);

    let loaded = store.load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 3);
    assert_eq!(loaded.voted_for, Some(1));
}

#[test]
fn test_flush_entries_only() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let entries = vec![make_entry(1, 1, "x")];
    store.flush(&entries, None).unwrap();

    assert_eq!(store.len().unwrap(), 1);
    assert!(store.load_hard_state().unwrap().is_none());
}

#[test]
fn test_flush_hard_state_only() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    let state = PersistentState {
        current_term: 7,
        voted_for: None,
    };
    store.flush(&[], Some(&state)).unwrap();

    assert_eq!(store.len().unwrap(), 0);
    let loaded = store.load_hard_state().unwrap().unwrap();
    assert_eq!(loaded.current_term, 7);
}

#[test]
fn test_wal_persistence_across_reopen() {
    let dir = tempdir().unwrap();

    {
        let store = LogStore::open(dir.path()).unwrap();
        store.append(&[
            make_entry(1, 1, "a"),
            make_entry(1, 2, "b"),
            make_entry(2, 3, "c"),
        ]).unwrap();
    }

    // Reopen and verify WAL entries survived
    {
        let store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.len().unwrap(), 3);
        assert_eq!(store.last_index().unwrap(), 3);
        assert_eq!(store.last_term().unwrap(), 2);
        let entry = store.get(2).unwrap().unwrap();
        assert_eq!(entry.index, 2);
        assert_eq!(entry.term, 1);
    }
}

#[test]
fn test_truncate_then_append() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path()).unwrap();

    store.append(&[
        make_entry(1, 1, "a"),
        make_entry(1, 2, "b"),
        make_entry(1, 3, "c"),
    ]).unwrap();

    // Simulate follower log conflict: truncate and append new entries
    store.truncate_after(1).unwrap();
    store.append(&[
        make_entry(2, 2, "new-b"),
        make_entry(2, 3, "new-c"),
    ]).unwrap();

    assert_eq!(store.len().unwrap(), 3);
    assert_eq!(store.last_term().unwrap(), 2);
    let entry = store.get(2).unwrap().unwrap();
    assert_eq!(entry.term, 2);
}
