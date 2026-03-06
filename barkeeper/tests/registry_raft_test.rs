use rebar_core::runtime::Runtime;
use rebar_cluster::registry::orset::Registry;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[tokio::test]
async fn test_raft_process_registers_in_registry() {
    let runtime = Runtime::new(1);
    let registry = Arc::new(Mutex::new(Registry::new()));

    let reg = Arc::clone(&registry);
    let pid = runtime
        .spawn(move |ctx| {
            let reg = reg;
            async move {
                // Register self as "raft:1"
                let name = format!("raft:{}", ctx.self_pid().node_id());
                reg.lock().unwrap().register(
                    &name,
                    ctx.self_pid(),
                    ctx.self_pid().node_id(),
                    1, // timestamp
                );
                // Keep alive briefly
                let mut ctx = ctx;
                ctx.recv_timeout(Duration::from_millis(200)).await;
            }
        })
        .await;

    // Allow time for spawn
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify registration
    let reg = registry.lock().unwrap();
    let entry = reg.lookup("raft:1");
    assert!(entry.is_some(), "raft:1 should be registered");
    assert_eq!(entry.unwrap().pid, pid);
}

#[tokio::test]
async fn test_registry_lookup_returns_none_for_unregistered() {
    let registry = Registry::new();
    assert!(
        registry.lookup("raft:99").is_none(),
        "unregistered name should return None"
    );
}

#[tokio::test]
async fn test_rebar_process_receives_messages() {
    let runtime = Runtime::new(1);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let receiver = runtime
        .spawn(move |mut ctx| async move {
            let msg = ctx.recv().await.unwrap();
            let payload = msg.payload().as_str().unwrap().to_string();
            done_tx.send(payload).unwrap();
        })
        .await;

    runtime
        .send(receiver, rmpv::Value::String("hello-raft".into()))
        .await
        .unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, "hello-raft");
}

#[tokio::test]
async fn test_raft_message_serialization_via_rebar() {
    use barkeeper::raft::messages::*;

    let runtime = Runtime::new(1);
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let receiver = runtime
        .spawn(move |mut ctx| async move {
            let msg = ctx.recv().await.unwrap();
            let raft_msg = raft_message_from_value(msg.payload()).unwrap();
            match raft_msg {
                RaftMessage::RequestVoteReq(req) => {
                    done_tx.send(req.term).unwrap();
                }
                _ => panic!("expected RequestVoteReq"),
            }
        })
        .await;

    let vote_req = RaftMessage::RequestVoteReq(RequestVoteRequest {
        term: 42,
        candidate_id: 1,
        last_log_index: 10,
        last_log_term: 5,
    });
    let payload = raft_message_to_value(&vote_req);

    runtime.send(receiver, payload).await.unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, 42);
}
