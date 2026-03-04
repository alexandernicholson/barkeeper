//! Tests that leases expire and attached keys are cleaned up.

use std::sync::Arc;
use tokio::time::{sleep, Duration};

use barkeeper::lease::manager::LeaseManager;

/// A lease that expires should be removed from the lease list.
#[tokio::test]
async fn test_lease_expires_from_list() {
    let manager = Arc::new(LeaseManager::new());

    // Grant a lease with TTL=1 second.
    let id = manager.grant(0, 1).await;
    assert!(manager.list().await.contains(&id));

    // Wait for expiry.
    sleep(Duration::from_secs(2)).await;

    // Check expiry — this requires LeaseManager to have a check_expired() method.
    let expired = manager.check_expired().await;
    assert!(expired.iter().any(|e| e.lease_id == id));
}

/// Keys attached to an expired lease should be deletable.
#[tokio::test]
async fn test_expired_lease_returns_attached_keys() {
    let manager = Arc::new(LeaseManager::new());

    let id = manager.grant(0, 1).await;
    manager.attach_key(id, b"ephemeral".to_vec()).await;

    sleep(Duration::from_secs(2)).await;

    let expired = manager.check_expired().await;
    let lease_entry = expired.iter().find(|e| e.lease_id == id).unwrap();
    assert_eq!(lease_entry.keys, vec![b"ephemeral".to_vec()]);
}

/// Keepalive should prevent expiry.
#[tokio::test]
async fn test_keepalive_prevents_expiry() {
    let manager = Arc::new(LeaseManager::new());

    let id = manager.grant(0, 2).await;

    sleep(Duration::from_secs(1)).await;
    manager.keepalive(id).await;

    sleep(Duration::from_secs(1)).await;
    manager.keepalive(id).await;

    // 3 seconds total, but keepalive resets TTL each time.
    sleep(Duration::from_secs(1)).await;

    let expired = manager.check_expired().await;
    assert!(!expired.iter().any(|e| e.lease_id == id));
}
