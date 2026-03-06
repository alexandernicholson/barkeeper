//! Tests for the AuthActor — the Rebar actor replacement for AuthManager.
//!
//! Written TDD-style: tests define expected behavior, then the actor
//! implementation makes them pass.

use rebar_core::runtime::Runtime;

use barkeeper::auth::actor::{spawn_auth_actor, AuthActorHandle};
use barkeeper::auth::manager::Permission;

/// Helper: create a Rebar runtime and spawn an auth actor.
async fn make_handle() -> AuthActorHandle {
    let runtime = Runtime::new(1);
    spawn_auth_actor(&runtime).await
}

// ---------------------------------------------------------------------------
// Enable / Disable toggle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_auth_disabled_by_default() {
    let handle = make_handle().await;
    assert!(!handle.is_enabled().await);
}

#[tokio::test]
async fn test_auth_enable_disable() {
    let handle = make_handle().await;

    handle.auth_enable().await;
    assert!(handle.is_enabled().await);

    handle.auth_disable().await;
    assert!(!handle.is_enabled().await);
}

// ---------------------------------------------------------------------------
// User CRUD (add, get, delete, list)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_user_add_and_get() {
    let handle = make_handle().await;

    assert!(handle.user_add("alice".into(), "pass123".into()).await);
    let user = handle.user_get("alice").await;
    assert!(user.is_some());
    assert_eq!(user.unwrap().name, "alice");
}

#[tokio::test]
async fn test_user_add_duplicate() {
    let handle = make_handle().await;

    assert!(handle.user_add("alice".into(), "pass123".into()).await);
    assert!(!handle.user_add("alice".into(), "other".into()).await);
}

#[tokio::test]
async fn test_user_get_nonexistent() {
    let handle = make_handle().await;
    assert!(handle.user_get("ghost").await.is_none());
}

#[tokio::test]
async fn test_user_delete() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "pass123".into()).await;
    assert!(handle.user_delete("alice").await);
    assert!(!handle.user_delete("alice").await); // already deleted
    assert!(handle.user_get("alice").await.is_none());
}

#[tokio::test]
async fn test_user_list() {
    let handle = make_handle().await;

    handle.user_add("charlie".into(), "p".into()).await;
    handle.user_add("alice".into(), "p".into()).await;
    handle.user_add("bob".into(), "p".into()).await;

    let users = handle.user_list().await;
    assert_eq!(users, vec!["alice", "bob", "charlie"]); // sorted
}

// ---------------------------------------------------------------------------
// Authenticate with valid and invalid credentials
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_authenticate_valid() {
    let handle = make_handle().await;

    handle.user_add("root".into(), "secret".into()).await;
    let token = handle.authenticate("root", "secret").await;
    assert!(token.is_some());
    // JWT tokens have 3 dot-separated parts: header.payload.signature
    let t = token.unwrap();
    let parts: Vec<&str> = t.split('.').collect();
    assert_eq!(parts.len(), 3, "token should be a JWT with 3 parts, got: {}", t);
}

#[tokio::test]
async fn test_authenticate_wrong_password() {
    let handle = make_handle().await;

    handle.user_add("root".into(), "secret".into()).await;
    let token = handle.authenticate("root", "wrong").await;
    assert!(token.is_none());
}

#[tokio::test]
async fn test_authenticate_nonexistent_user() {
    let handle = make_handle().await;

    let token = handle.authenticate("ghost", "any").await;
    assert!(token.is_none());
}

// ---------------------------------------------------------------------------
// Validate token
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_validate_token() {
    let handle = make_handle().await;

    handle.user_add("root".into(), "secret".into()).await;
    let token = handle
        .authenticate("root", "secret")
        .await
        .unwrap();

    let username = handle.validate_token(&token).await;
    assert_eq!(username, Some("root".to_string()));

    // Invalid token should return None.
    assert!(handle.validate_token("bogus").await.is_none());
}

// ---------------------------------------------------------------------------
// User change password
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_user_change_password() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "old".into()).await;
    assert!(handle.user_change_password("alice", "new".into()).await);

    // Old password should fail.
    assert!(handle.authenticate("alice", "old").await.is_none());

    // New password should succeed.
    assert!(handle.authenticate("alice", "new").await.is_some());
}

#[tokio::test]
async fn test_user_change_password_nonexistent() {
    let handle = make_handle().await;
    assert!(!handle.user_change_password("ghost", "pass".into()).await);
}

// ---------------------------------------------------------------------------
// Role CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_role_add_and_get() {
    let handle = make_handle().await;

    assert!(handle.role_add("admin".into()).await);
    let role = handle.role_get("admin").await;
    assert!(role.is_some());
    assert_eq!(role.unwrap().name, "admin");
}

#[tokio::test]
async fn test_role_add_duplicate() {
    let handle = make_handle().await;

    assert!(handle.role_add("admin".into()).await);
    assert!(!handle.role_add("admin".into()).await);
}

#[tokio::test]
async fn test_role_delete() {
    let handle = make_handle().await;

    handle.role_add("admin".into()).await;
    assert!(handle.role_delete("admin").await);
    assert!(!handle.role_delete("admin").await); // already deleted
    assert!(handle.role_get("admin").await.is_none());
}

#[tokio::test]
async fn test_role_list() {
    let handle = make_handle().await;

    handle.role_add("writer".into()).await;
    handle.role_add("admin".into()).await;
    handle.role_add("reader".into()).await;

    let roles = handle.role_list().await;
    assert_eq!(roles, vec!["admin", "reader", "writer"]); // sorted
}

// ---------------------------------------------------------------------------
// User grant / revoke role
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_user_grant_role() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "p".into()).await;
    handle.role_add("admin".into()).await;

    assert!(handle.user_grant_role("alice", "admin").await);

    let user = handle.user_get("alice").await.unwrap();
    assert!(user.roles.contains(&"admin".to_string()));
}

#[tokio::test]
async fn test_user_grant_role_duplicate() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "p".into()).await;
    handle.role_add("admin".into()).await;

    assert!(handle.user_grant_role("alice", "admin").await);
    assert!(!handle.user_grant_role("alice", "admin").await); // already granted
}

#[tokio::test]
async fn test_user_grant_role_nonexistent_user() {
    let handle = make_handle().await;
    assert!(!handle.user_grant_role("ghost", "admin").await);
}

#[tokio::test]
async fn test_user_revoke_role() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "p".into()).await;
    handle.user_grant_role("alice", "admin").await;

    assert!(handle.user_revoke_role("alice", "admin").await);

    let user = handle.user_get("alice").await.unwrap();
    assert!(!user.roles.contains(&"admin".to_string()));
}

#[tokio::test]
async fn test_user_revoke_role_not_granted() {
    let handle = make_handle().await;

    handle.user_add("alice".into(), "p".into()).await;
    assert!(!handle.user_revoke_role("alice", "admin").await);
}

// ---------------------------------------------------------------------------
// Role grant / revoke permission
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_role_grant_permission() {
    let handle = make_handle().await;

    handle.role_add("reader".into()).await;

    let perm = Permission {
        perm_type: 0, // READ
        key: b"foo".to_vec(),
        range_end: b"fop".to_vec(),
    };
    assert!(handle.role_grant_permission("reader", perm).await);

    let role = handle.role_get("reader").await.unwrap();
    assert_eq!(role.permissions.len(), 1);
    assert_eq!(role.permissions[0].key, b"foo");
}

#[tokio::test]
async fn test_role_grant_permission_nonexistent() {
    let handle = make_handle().await;

    let perm = Permission {
        perm_type: 0,
        key: b"foo".to_vec(),
        range_end: b"fop".to_vec(),
    };
    assert!(!handle.role_grant_permission("ghost", perm).await);
}

#[tokio::test]
async fn test_role_revoke_permission() {
    let handle = make_handle().await;

    handle.role_add("reader".into()).await;
    let perm = Permission {
        perm_type: 0,
        key: b"foo".to_vec(),
        range_end: b"fop".to_vec(),
    };
    handle.role_grant_permission("reader", perm).await;

    assert!(handle
        .role_revoke_permission("reader", b"foo", b"fop")
        .await);

    let role = handle.role_get("reader").await.unwrap();
    assert!(role.permissions.is_empty());
}

#[tokio::test]
async fn test_role_revoke_permission_not_granted() {
    let handle = make_handle().await;

    handle.role_add("reader".into()).await;
    assert!(!handle
        .role_revoke_permission("reader", b"foo", b"fop")
        .await);
}

#[tokio::test]
async fn test_role_revoke_permission_nonexistent_role() {
    let handle = make_handle().await;
    assert!(!handle
        .role_revoke_permission("ghost", b"foo", b"fop")
        .await);
}

// ---------------------------------------------------------------------------
// Concurrent access
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_concurrent_access() {
    let handle = make_handle().await;

    // Spawn 10 concurrent user_add operations.
    let mut tasks = Vec::new();
    for i in 0..10 {
        let h = handle.clone();
        tasks.push(tokio::spawn(async move {
            h.user_add(format!("user-{}", i), "pass".into()).await
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    let users = handle.user_list().await;
    assert_eq!(users.len(), 10, "expected 10 users, got {:?}", users);
}

// ---------------------------------------------------------------------------
// Handle is Clone and Send
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_handle_is_clone_and_send() {
    let handle = make_handle().await;
    let handle2 = handle.clone();

    // Both handles should work.
    handle.user_add("alice".into(), "p".into()).await;
    let users = handle2.user_list().await;
    assert_eq!(users.len(), 1);
}

// ---------------------------------------------------------------------------
// JWT token format
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_authenticate_returns_jwt() {
    let handle = make_handle().await;
    handle.user_add("alice".into(), "password123".into()).await;

    let token = handle.authenticate("alice", "password123").await
        .expect("authenticate should succeed");

    // JWT tokens have 3 dot-separated parts: header.payload.signature
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3, "token should be a JWT with 3 parts, got: {}", token);
}
