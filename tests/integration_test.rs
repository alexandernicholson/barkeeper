use std::sync::Arc;

use barkeeper::kv::actor::spawn_kv_store_actor;
use barkeeper::kv::apply_broker::ApplyResultBroker;
use barkeeper::kv::state_machine::{spawn_state_machine, KvCommand};
use barkeeper::kv::store::KvStore;
use barkeeper::lease::manager::LeaseManager;
use barkeeper::raft::messages::{LogEntry, LogEntryData};
use barkeeper::watch::actor::spawn_watch_hub_actor;
use rebar_core::runtime::Runtime;
use tempfile::tempdir;
use tokio::sync::mpsc;

/// Helper: open a KvStore and spawn the actor + state machine, returning the
/// actor handle and the apply channel sender.
async fn make_store_and_sm(
    dir: &tempfile::TempDir,
) -> (
    barkeeper::kv::actor::KvStoreActorHandle,
    mpsc::Sender<Vec<LogEntry>>,
) {
    let store = Arc::new(KvStore::open(dir.path().join("test.redb")).unwrap());
    let runtime = Runtime::new(1);
    let handle = spawn_kv_store_actor(&runtime, Arc::clone(&store)).await;

    let watch_runtime = Runtime::new(1);
    let watch_hub = spawn_watch_hub_actor(&watch_runtime, Some(handle.clone())).await;
    let lease_manager = Arc::new(LeaseManager::new());
    let broker = Arc::new(ApplyResultBroker::new());

    let (apply_tx, apply_rx) = mpsc::channel(256);
    spawn_state_machine(apply_rx, store, watch_hub, lease_manager, broker).await;

    (handle, apply_tx)
}

/// State machine applies a put entry to the store.
#[tokio::test]
async fn test_state_machine_apply_put() {
    let dir = tempdir().unwrap();
    let (handle, apply_tx) = make_store_and_sm(&dir).await;

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

    apply_tx.send(vec![entry]).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // State machine now applies to the store.
    let result = handle.range(b"hello".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"world");
}

#[tokio::test]
async fn test_state_machine_apply_delete() {
    let dir = tempdir().unwrap();
    let (handle, apply_tx) = make_store_and_sm(&dir).await;

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
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Key should be deleted.
    let result = handle.range(b"key".to_vec(), vec![], 0, 0).await.unwrap();
    assert_eq!(result.kvs.len(), 0);
}

/// Verify the KvStore actor handle can write and read back data.
#[tokio::test]
async fn test_kv_store_actor_write_read() {
    let dir = tempdir().unwrap();
    let store = KvStore::open(dir.path().join("test.redb")).unwrap();
    let runtime = Runtime::new(1);
    let handle = spawn_kv_store_actor(&runtime, Arc::new(store)).await;

    handle
        .put(b"shared".to_vec(), b"data".to_vec(), 0)
        .await
        .unwrap();

    let result = handle
        .range(b"shared".to_vec(), vec![], 0, 0)
        .await
        .unwrap();
    assert_eq!(result.kvs.len(), 1);
    assert_eq!(result.kvs[0].value, b"data");
}

#[tokio::test]
async fn test_state_machine_noop_ignored() {
    let dir = tempdir().unwrap();
    let (handle, apply_tx) = make_store_and_sm(&dir).await;

    let noop_entry = LogEntry {
        term: 1,
        index: 1,
        data: LogEntryData::Noop,
    };

    // Should not panic on noop.
    apply_tx.send(vec![noop_entry]).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    assert_eq!(handle.current_revision().await.unwrap(), 0);
}

/// State machine applies multiple puts correctly.
#[tokio::test]
async fn test_state_machine_multiple_puts() {
    let dir = tempdir().unwrap();
    let (handle, apply_tx) = make_store_and_sm(&dir).await;

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
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // All 5 keys should exist.
    assert_eq!(handle.current_revision().await.unwrap(), 5);
}
