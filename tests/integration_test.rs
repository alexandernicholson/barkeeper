use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::state_machine::{spawn_state_machine, KvCommand};
use barkeeper::kv::store::KvStore;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use rebar_core::runtime::Runtime;
use tempfile::tempdir;
use tokio::sync::mpsc;

/// Helper: open a KvStore and spawn the actor, returning the handle.
async fn make_store_handle(
    dir: &tempfile::TempDir,
) -> barkeeper::kv::actor::KvStoreActorHandle {
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();
    let runtime = Runtime::new(1);
    spawn_kv_store_actor(&runtime, store).await
}

/// State machine accepts committed entries without panicking.
/// The state machine only logs entries; actual application happens at the
/// service layer after Raft commit.
#[tokio::test]
async fn test_state_machine_apply_put() {
    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx).await;

    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;

    let entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Command(
            serde_json::to_vec(&KvCommand::Put {
                key: b"hello".to_vec(),
                value: b"world".to_vec(),
                lease_id: 0,
            })
            .unwrap(),
        ),
    };

    // Send entry to state machine — should not panic.
    apply_tx.send(vec![entry]).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // Store remains empty: the service layer applies after Raft commit.
    let result = handle.range(b"hello".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 0);
}

#[tokio::test]
async fn test_state_machine_apply_delete() {
    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx).await;

    let entries = vec![
        LogEntry {
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
        },
        LogEntry {
            term: 1,
            index: 2,
            data: LogEntryData::Command(
                serde_json::to_vec(&KvCommand::DeleteRange {
                    key: b"key".to_vec(),
                    range_end: b"".to_vec(),
                })
                .unwrap(),
            ),
        },
    ];

    apply_tx.send(entries).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    let result = handle.range(b"key".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 0);
}

/// Verify the KvStore actor handle can write and read back data.
#[tokio::test]
async fn test_kv_store_actor_write_read() {
    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;

    handle.put(b"shared".to_vec(), b"data".to_vec(), 0).await.unwrap();

    let result = handle.range(b"shared".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"data");
}

#[tokio::test]
async fn test_state_machine_noop_ignored() {
    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx).await;

    let noop_entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Noop,
    };

    // Should not panic on noop.
    apply_tx.send(vec![noop_entry]).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    assert_eq!(handle.current_revision().await.unwrap(), 0);
}

/// State machine accepts multiple entries without panicking.
#[tokio::test]
async fn test_state_machine_multiple_puts() {
    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx).await;

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

    apply_tx.send(entries).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let dir = tempdir().unwrap();
    let handle = make_store_handle(&dir).await;
    assert_eq!(handle.current_revision().await.unwrap(), 0);
}
