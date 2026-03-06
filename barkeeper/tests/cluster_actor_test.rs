//! Tests for the ClusterActor — the Rebar actor replacement for ClusterManager.
//!
//! Written TDD-style: tests define expected behavior, then the actor
//! implementation makes them pass.

use std::sync::{Arc, Mutex};

use rebar_core::runtime::Runtime;

use barkeeper::cluster::actor::{spawn_cluster_actor, ClusterActorHandle};
use barkeeper::cluster::membership_sync::MembershipSync;

/// Helper: create a Rebar runtime and spawn a cluster actor with the given ID.
async fn make_handle(cluster_id: u64) -> ClusterActorHandle {
    let runtime = Runtime::new(1);
    spawn_cluster_actor(&runtime, cluster_id).await
}

// ---------------------------------------------------------------------------
// Basic cluster ID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_cluster_id() {
    let handle = make_handle(42).await;
    assert_eq!(handle.cluster_id(), 42);
}

// ---------------------------------------------------------------------------
// AddInitialMember + MemberList
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_add_initial_member_and_list() {
    let handle = make_handle(1).await;

    handle
        .add_initial_member(
            10,
            "node-10".into(),
            vec!["http://127.0.0.1:2380".into()],
            vec!["http://127.0.0.1:2379".into()],
        )
        .await;

    let members = handle.member_list().await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].id, 10);
    assert_eq!(members[0].name, "node-10");
    assert_eq!(members[0].peer_urls, vec!["http://127.0.0.1:2380"]);
    assert_eq!(members[0].client_urls, vec!["http://127.0.0.1:2379"]);
    assert!(!members[0].is_learner);
}

// ---------------------------------------------------------------------------
// MemberAdd
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_member_add() {
    let handle = make_handle(1).await;

    let member = handle
        .member_add(vec!["http://peer:2380".into()], false)
        .await;

    assert_eq!(member.id, 100); // first auto-generated ID
    assert_eq!(member.peer_urls, vec!["http://peer:2380"]);
    assert!(!member.is_learner);

    // Second add should get id 101.
    let member2 = handle
        .member_add(vec!["http://peer2:2380".into()], true)
        .await;
    assert_eq!(member2.id, 101);
    assert!(member2.is_learner);

    // Member list should have both.
    let members = handle.member_list().await;
    assert_eq!(members.len(), 2);
}

// ---------------------------------------------------------------------------
// MemberRemove
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_member_remove() {
    let handle = make_handle(1).await;

    let member = handle
        .member_add(vec!["http://peer:2380".into()], false)
        .await;

    assert!(handle.member_remove(member.id).await);
    assert!(!handle.member_remove(member.id).await); // already removed

    let members = handle.member_list().await;
    assert!(members.is_empty());
}

// ---------------------------------------------------------------------------
// MemberUpdate
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_member_update() {
    let handle = make_handle(1).await;

    let member = handle
        .member_add(vec!["http://old:2380".into()], false)
        .await;

    assert!(handle
        .member_update(member.id, vec!["http://new:2380".into()])
        .await);

    let members = handle.member_list().await;
    assert_eq!(members.len(), 1);
    assert_eq!(members[0].peer_urls, vec!["http://new:2380"]);
}

#[tokio::test]
async fn test_member_update_nonexistent() {
    let handle = make_handle(1).await;
    assert!(!handle
        .member_update(999, vec!["http://new:2380".into()])
        .await);
}

// ---------------------------------------------------------------------------
// MemberPromote
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_member_promote() {
    let handle = make_handle(1).await;

    let learner = handle
        .member_add(vec!["http://learner:2380".into()], true)
        .await;
    assert!(learner.is_learner);

    // Promote should succeed.
    assert!(handle.member_promote(learner.id).await);

    // After promotion, member should not be a learner.
    let members = handle.member_list().await;
    let promoted = members.iter().find(|m| m.id == learner.id).unwrap();
    assert!(!promoted.is_learner);

    // Promoting again should fail (already a voter).
    assert!(!handle.member_promote(learner.id).await);
}

#[tokio::test]
async fn test_member_promote_nonexistent() {
    let handle = make_handle(1).await;
    assert!(!handle.member_promote(999).await);
}

// ---------------------------------------------------------------------------
// AddInitialMember advances next_id
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_add_initial_member_advances_next_id() {
    let handle = make_handle(1).await;

    // Add initial member with a high ID.
    handle
        .add_initial_member(500, "node-500".into(), vec![], vec![])
        .await;

    // Next auto-generated ID should be > 500.
    let member = handle.member_add(vec![], false).await;
    assert!(member.id > 500, "expected id > 500, got {}", member.id);
}

// ---------------------------------------------------------------------------
// Concurrent access (multiple handles sending simultaneously)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_access() {
    let handle = make_handle(1).await;

    // Spawn 10 concurrent member_add operations.
    let mut tasks = Vec::new();
    for i in 0..10 {
        let h = handle.clone();
        tasks.push(tokio::spawn(async move {
            h.member_add(vec![format!("http://peer-{}:2380", i)], false)
                .await
        }));
    }

    let mut ids = Vec::new();
    for task in tasks {
        let member = task.await.unwrap();
        ids.push(member.id);
    }

    // All IDs should be unique.
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 10, "expected 10 unique IDs, got {:?}", ids);

    // Member list should have all 10.
    let members = handle.member_list().await;
    assert_eq!(members.len(), 10);
}

// ---------------------------------------------------------------------------
// SetMembershipSync — SWIM peers appear in member_list
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_set_membership_sync() {
    let handle = make_handle(1).await;

    // Add one explicit member.
    handle
        .add_initial_member(1, "node-1".into(), vec![], vec![])
        .await;

    // Create a MembershipSync with a SWIM-discovered peer.
    let sync = Arc::new(Mutex::new(MembershipSync::new()));
    {
        let mut s = sync.lock().unwrap();
        s.on_alive(2, "10.0.0.2:2380".parse().unwrap());
    }

    handle.set_membership_sync(Arc::clone(&sync)).await;

    // Member list should now include the SWIM-discovered peer.
    let members = handle.member_list().await;
    assert_eq!(members.len(), 2);

    let swim_member = members.iter().find(|m| m.id == 2).unwrap();
    assert_eq!(swim_member.peer_urls, vec!["http://10.0.0.2:2380"]);
}

// ---------------------------------------------------------------------------
// Handle is Clone and Send
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_handle_is_clone_and_send() {
    let handle = make_handle(1).await;
    let handle2 = handle.clone();

    // Both handles should work.
    handle
        .add_initial_member(1, "node-1".into(), vec![], vec![])
        .await;
    let members = handle2.member_list().await;
    assert_eq!(members.len(), 1);
}
