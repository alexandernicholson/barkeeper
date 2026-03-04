use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::state_machine::{KvCommand, StateMachine};
use barkeeper::kv::store::KvStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use rebar_core::runtime::Runtime;
use tempfile::tempdir;

/// Helper: open a KvStore and spawn the actor, returning the handle.
async fn make_store_handle(
    dir: &tempfile::TempDir,
) -> barkeeper::kv::actor::KvStoreActorHandle {
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();
    let runtime = Runtime::new(1);
    spawn_kv_store_actor(&runtime, store).await
}

/// State machine now delegates application to the service layer.
/// It should accept entries without panicking and leave the store unchanged
/// (the service layer applies after Raft commit).
#[tokio::test]
async fn test_state_machine_apply_put() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let sm = StateMachine::new(handle.clone());

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

    // Should not panic -- state machine accepts entries for logging.
    sm.apply(vec![entry]).await;

    // Store remains empty: the service layer applies after Raft commit,
    // not the state machine.
    let result = handle.range(b"hello".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 0);
}

#[tokio::test]
async fn test_state_machine_apply_delete() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let sm = StateMachine::new(handle.clone());

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

    let result = handle.range(b"key".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 0);
}

/// State machine no longer applies to store (service layer does).
/// Verify the handle can write and read back data.
#[tokio::test]
async fn test_state_machine_shared_store() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let _sm = StateMachine::new(handle.clone());

    // Write directly to the store via handle (simulating the service layer).
    handle.put(b"shared".to_vec(), b"data".to_vec(), 0).await.unwrap();

    // The handle should see the same data.
    let result = handle.range(b"shared".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"data");
}

#[tokio::test]
async fn test_state_machine_noop_and_config_change_ignored() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let sm = StateMachine::new(handle.clone());

    let noop_entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Noop,
    };

    // Apply should not panic on noop.
    sm.apply(vec![noop_entry]).await;

    // Store should still be empty.
    assert_eq!(handle.current_revision().await.unwrap(), 0);
}

/// State machine now delegates application to the service layer.
/// Verify it accepts multiple entries without panicking and the store
/// remains empty (service layer handles application).
#[tokio::test]
async fn test_state_machine_multiple_puts() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let sm = StateMachine::new(handle.clone());

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

    // Should not panic -- state machine logs entries for observability.
    sm.apply(entries).await;

    // Store remains empty: the service layer applies after Raft commit.
    assert_eq!(handle.current_revision().await.unwrap(), 0);
}
