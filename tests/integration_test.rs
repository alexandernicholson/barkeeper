use std::sync::Arc;

use barkeeper::kv::state_machine::{KvCommand, StateMachine};
use barkeeper::kv::store::KvStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use tempfile::tempdir;

/// State machine now delegates application to the service layer.
/// It should accept entries without panicking and leave the store unchanged
/// (the service layer applies after Raft commit).
#[tokio::test]
async fn test_state_machine_apply_put() {
    let dir = tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let sm = StateMachine::new(Arc::clone(&store));

    let cmd = KvCommand::Put {
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
        lease_id: 0,
    };

    let entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command(serde_json::to_vec(&cmd).unwrap()),
    };

    // Should not panic — state machine accepts entries for logging.
    sm.apply(vec![entry]).await;

    // Store remains empty: the service layer applies after Raft commit,
    // not the state machine.
    let result = sm.store().range(b"hello", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 0);
}

#[tokio::test]
async fn test_state_machine_apply_delete() {
    let dir = tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let sm = StateMachine::new(Arc::clone(&store));

    let put = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command(
            serde_json::to_vec(&KvCommand::Put {
                key: b"key".to_vec(),
                value: b"val".to_vec(),
                lease_id: 0,
            })
            .unwrap(),
        ),
    };

    let delete = LogEntry {
        term: 1,
        index: 2,
        data: LogEntryData::Command(
            serde_json::to_vec(&KvCommand::DeleteRange {
                key: b"key".to_vec(),
                range_end: b"".to_vec(),
            })
            .unwrap(),
        ),
    };

    sm.apply(vec![put, delete]).await;

    let result = sm.store().range(b"key", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 0);
}

/// State machine no longer applies to store (service layer does).
/// Verify the state machine holds a reference to the shared store.
#[tokio::test]
async fn test_state_machine_shared_store() {
    let dir = tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let sm = StateMachine::new(Arc::clone(&store));

    // Write directly to the store (simulating the service layer).
    store.put(b"shared", b"data", 0).unwrap();

    // State machine's store reference should see the same data.
    let result = sm.store().range(b"shared", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"data");

    // Also verify through the Arc reference.
    let result = store.range(b"shared", b"", 0, 0).unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"data");
}

#[tokio::test]
async fn test_state_machine_noop_and_config_change_ignored() {
    let dir = tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let sm = StateMachine::new(Arc::clone(&store));

    let noop_entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Noop,
    };

    // Apply should not panic on noop.
    sm.apply(vec![noop_entry]).await;

    // Store should still be empty.
    assert_eq!(store.current_revision().unwrap(), 0);
}

/// State machine now delegates application to the service layer.
/// Verify it accepts multiple entries without panicking and the store
/// remains empty (service layer handles application).
#[tokio::test]
async fn test_state_machine_multiple_puts() {
    let dir = tempdir().unwrap();
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let sm = StateMachine::new(Arc::clone(&store));

    let entries: Vec<LogEntry> = (1..=5)
        .map(|i| LogEntry {
            term: 1,
            index: i,
            data: LogEntryData::Command(
                serde_json::to_vec(&KvCommand::Put {
                    key: format!("key{}", i).into_bytes(),
                    value: format!("val{}", i).into_bytes(),
                    lease_id: 0,
                })
                .unwrap(),
            ),
        })
        .collect();

    // Should not panic — state machine logs entries for observability.
    sm.apply(entries).await;

    // Store remains empty: the service layer applies after Raft commit.
    assert_eq!(store.current_revision().unwrap(), 0);
}
