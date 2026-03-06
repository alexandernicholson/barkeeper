//! Tests for kube-apiserver boot compatibility.
//!
//! These verify the exact behaviors kube-apiserver expects when connecting to
//! an etcd-compatible server: mTLS, MemberList health checks, LeaseKeepAlive
//! under load, watch reconnection reliability, and concurrent compaction safety.

// ── Gap 1: mTLS ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tls_mtls {
    use barkeeper::tls;

    /// kube-apiserver passes --etcd-cafile to validate the server, and
    /// --etcd-certfile/--etcd-keyfile so the server can validate the client.
    /// When client_cert_auth=true and trusted_ca_file is set, the gRPC server
    /// MUST require and validate client certificates.
    #[test]
    fn test_build_tonic_tls_with_client_auth() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = tls::generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();

        // Use the server's own CA as the trusted CA (self-signed).
        let ca_cert = std::fs::read_to_string(&cert).unwrap();

        let tls_config = tls::build_tonic_tls_with_client_auth(&cert, &key, &ca_cert);
        assert!(tls_config.is_ok(), "mTLS config should build successfully");
    }

    /// The HTTP gateway (axum) must also support mTLS when configured.
    #[test]
    fn test_build_tls_acceptor_with_client_auth() {
        let dir = tempfile::tempdir().unwrap();
        let (cert, key) = tls::generate_self_signed(dir.path().to_str().unwrap(), 1).unwrap();

        let ca_cert_pem = std::fs::read(&cert).unwrap();

        let acceptor = tls::build_tls_acceptor_with_client_auth(&cert, &key, &ca_cert_pem);
        assert!(acceptor.is_ok(), "mTLS acceptor should build successfully");
    }
}

// ── Gap 2: MemberList returns client URLs ──────────────────────────────────

#[cfg(test)]
mod memberlist_client_urls {
    use barkeeper::cluster::actor::spawn_cluster_actor;
    use rebar_core::runtime::Runtime;

    /// kube-apiserver calls MemberList at startup and checks that at least one
    /// member has non-empty clientURLs matching the --etcd-servers address.
    #[tokio::test]
    async fn test_member_list_returns_client_urls() {
        let rt = Runtime::new(1);
        let cluster = spawn_cluster_actor(&rt, 12345).await;

        cluster
            .add_initial_member(
                1,
                "barkeeper-0".to_string(),
                vec!["http://127.0.0.1:2380".to_string()],
                vec!["http://127.0.0.1:2379".to_string()],
            )
            .await;

        let members = cluster.member_list().await;
        assert_eq!(members.len(), 1);
        assert!(!members[0].client_urls.is_empty(), "client_urls must not be empty");
        assert!(
            members[0].client_urls[0].contains("2379"),
            "client URL should contain the client port"
        );
    }
}

// ── Gap 3: LeaseKeepAlive under concurrent load ────────────────────────────

#[cfg(test)]
mod lease_keepalive_concurrent {
    use std::sync::Arc;
    use barkeeper::lease::manager::LeaseManager;

    /// kube-apiserver keeps multiple leases alive concurrently (leader election,
    /// endpoints). All keepalives must succeed without data corruption.
    #[tokio::test]
    async fn test_concurrent_keepalives_no_corruption() {
        let manager = Arc::new(LeaseManager::new());

        // Grant 10 leases with 30s TTL.
        let mut lease_ids = Vec::new();
        for _ in 0..10 {
            let id = manager.grant(0, 30).await;
            lease_ids.push(id);
        }

        // Spawn 10 concurrent keepalive tasks, each sending 100 keepalives.
        let mut handles = Vec::new();
        for &id in &lease_ids {
            let mgr = Arc::clone(&manager);
            handles.push(tokio::spawn(async move {
                for _ in 0..100 {
                    let ttl = mgr.keepalive(id).await;
                    assert!(ttl.is_some(), "keepalive must succeed for lease {}", id);
                    assert!(ttl.unwrap() > 0, "keepalive must return positive TTL");
                }
            }));
        }

        // All tasks must complete without panic.
        for h in handles {
            h.await.unwrap();
        }

        // All leases must still exist.
        let expired = manager.check_expired().await;
        for &id in &lease_ids {
            assert!(
                !expired.iter().any(|e| e.lease_id == id),
                "lease {} should not be expired after keepalives",
                id
            );
        }
    }
}

// ── Gap 4: Watch reconnect delivers exactly expected events ────────────────

#[cfg(test)]
mod watch_reconnect_reliability {
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};
    use barkeeper::kv::actor::spawn_kv_store_actor;
    use barkeeper::kv::store::KvStore;
    use barkeeper::watch::actor::spawn_watch_hub_actor;
    use rebar_core::runtime::Runtime;

    /// kube-apiserver reconnects watches with the last seen revision. The
    /// watch must deliver events from exactly that revision onwards without
    /// gaps or duplicates.
    #[tokio::test]
    async fn test_watch_reconnect_no_gaps_no_duplicates() {
        let rt = Runtime::new(1);
        let dir = tempfile::tempdir().unwrap();
        let store_raw = KvStore::open(dir.path()).unwrap();

        // Write 5 revisions.
        for i in 1..=5 {
            store_raw
                .put(format!("/registry/pods/pod{}", i).as_bytes(), b"data", 0)
                .unwrap();
        }

        let store = spawn_kv_store_actor(&rt, Arc::new(store_raw)).await;
        let hub = spawn_watch_hub_actor(&rt, Some(store)).await;

        // "Reconnect" from revision 3 — should see revisions 3, 4, 5.
        let (_wid, mut rx) = hub
            .create_watch(
                b"/registry/pods/".to_vec(),
                b"/registry/pods0".to_vec(),
                3,
                vec![],
                false,
            )
            .await;

        let mut seen_revisions = Vec::new();
        for _ in 0..3 {
            let event = timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("should receive event within timeout")
                .expect("channel should not close");
            for ev in &event.events {
                if let Some(ref kv) = ev.kv {
                    seen_revisions.push(kv.mod_revision);
                }
            }
        }

        seen_revisions.sort();
        assert_eq!(seen_revisions, vec![3, 4, 5], "should see exactly revisions 3, 4, 5");

        // No more events should be pending (no duplicates).
        let extra = rx.try_recv();
        assert!(
            extra.is_err(),
            "should have no extra events after replaying 3 revisions"
        );
    }
}

// ── Gap 5: Concurrent compaction safety ────────────────────────────────────

#[cfg(test)]
mod concurrent_compaction {
    use barkeeper::kv::store::KvStore;

    /// kube-apiserver runs periodic compaction. Two concurrent compaction calls
    /// must both succeed and the compacted_revision must be monotonically
    /// non-decreasing.
    #[test]
    fn test_concurrent_compact_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let store = KvStore::open(dir.path()).unwrap();

        // Write 100 revisions.
        for i in 0..100 {
            store
                .put(format!("key{}", i).as_bytes(), b"val", 0)
                .unwrap();
        }

        // Spawn threads that compact to different revisions concurrently.
        let store = std::sync::Arc::new(store);
        let mut handles = Vec::new();

        for rev in [20, 50, 80, 30, 70, 10, 90, 40, 60] {
            let s = std::sync::Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                s.compact(rev).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // compacted_revision should be the maximum of all compacted revisions.
        let compacted = store.compacted_revision().unwrap();
        assert!(
            compacted >= 90,
            "compacted_revision should be at least 90 (the max), got {}",
            compacted
        );

        // Store should still function — range at current revision works.
        let result = store.range(b"key99", b"", 0, 0).unwrap();
        assert_eq!(result.count, 1, "latest key should still be readable");
    }
}
