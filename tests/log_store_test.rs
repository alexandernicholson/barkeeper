use barkeeper::raft::log_store::LogStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use barkeeper::raft::state::PersistentState;
use tempfile::tempdir;

fn make_entry(term: u64, index: u64, data: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        data: LogEntryData::Command(data.as_bytes().to_vec()),
    }
}

#[test]
fn test_open_and_empty() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();
    assert!(store.is_empty().unwrap());
    assert_eq!(store.last_index().unwrap(), 0);
    assert_eq!(store.last_term().unwrap(), 0);
}

#[test]
fn test_append_and_get() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();

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
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();

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
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();

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
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();

    store.append(&[make_entry(5, 1, "x")]).unwrap();
    assert_eq!(store.term_at(1).unwrap(), Some(5));
    assert_eq!(store.term_at(99).unwrap(), None);
}

#[test]
fn test_hard_state_persistence() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.redb");

    {
        let store = LogStore::open(&path).unwrap();
        assert!(store.load_hard_state().unwrap().is_none());

        let state = PersistentState {
            current_term: 5,
            voted_for: Some(3),
        };
        store.save_hard_state(&state).unwrap();
    }

    // Reopen and verify persistence
    {
        let store = LogStore::open(&path).unwrap();
        let state = store.load_hard_state().unwrap().unwrap();
        assert_eq!(state.current_term, 5);
        assert_eq!(state.voted_for, Some(3));
    }
}

#[test]
fn test_overwrite_entries() {
    let dir = tempdir().unwrap();
    let store = LogStore::open(dir.path().join("test.redb")).unwrap();

    store.append(&[make_entry(1, 1, "original")]).unwrap();
    store.append(&[make_entry(2, 1, "overwritten")]).unwrap();

    let entry = store.get(1).unwrap().unwrap();
    assert_eq!(entry.term, 2);
}
