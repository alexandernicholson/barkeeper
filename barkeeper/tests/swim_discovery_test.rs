// Tests for the --initial-cluster parsing and DNS resolution logic.

use barkeeper::cluster::discovery::{
    self, ClusterMode, detect_mode, derive_node_id, parse_hostname_ordinal,
    parse_initial_cluster, resolve_url_to_socket_addr, strip_url_scheme, url_to_socket_addr,
};

// ─── detect_mode ────────────────────────────────────────────────────────────

#[test]
fn test_detect_static_mode() {
    // Contains '=' -> static mode
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380";
    let result = parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, ClusterMode::Static);
    assert_eq!(peers.len(), 2);
}

#[test]
fn test_detect_dns_mode() {
    // No '=' -> DNS mode (will fail resolution in unit test, but detection works)
    let raw = "barkeeper.default.svc.cluster.local";
    let mode = detect_mode(raw);
    assert_eq!(mode, ClusterMode::Dns);
}

#[test]
fn test_detect_mode_single_entry_static() {
    assert_eq!(detect_mode("1=http://10.0.0.1:2380"), ClusterMode::Static);
}

#[test]
fn test_detect_mode_bare_hostname() {
    assert_eq!(detect_mode("my-service.ns.svc.cluster.local"), ClusterMode::Dns);
}

// ─── parse_hostname_ordinal ─────────────────────────────────────────────────

#[test]
fn test_hostname_ordinal_parsing() {
    assert_eq!(parse_hostname_ordinal("barkeeper-0"), Some(1));
    assert_eq!(parse_hostname_ordinal("barkeeper-2"), Some(3));
    assert_eq!(parse_hostname_ordinal("my-app-42"), Some(43));
    assert_eq!(parse_hostname_ordinal("no-ordinal"), None);
    assert_eq!(parse_hostname_ordinal(""), None);
}

#[test]
fn test_hostname_ordinal_complex_names() {
    // Multi-hyphen names: last segment is ordinal
    assert_eq!(parse_hostname_ordinal("my-cool-app-0"), Some(1));
    assert_eq!(parse_hostname_ordinal("a-b-c-d-5"), Some(6));
}

// ─── derive_node_id ─────────────────────────────────────────────────────────

#[test]
fn test_derive_node_id_from_hostname() {
    let id = derive_node_id(None, Some("barkeeper-2"));
    assert_eq!(id, Ok(3));
}

#[test]
fn test_explicit_node_id_overrides_hostname() {
    let id = derive_node_id(Some(7), Some("barkeeper-2"));
    assert_eq!(id, Ok(7));
}

#[test]
fn test_no_node_id_no_hostname_ordinal_errors() {
    let id = derive_node_id(None, Some("no-ordinal"));
    assert!(id.is_err());
}

#[test]
fn test_derive_node_id_no_hostname() {
    let id = derive_node_id(None, None);
    assert!(id.is_err());
}

// ─── url_to_socket_addr (sync, IP-only) ─────────────────────────────────────

#[test]
fn test_url_to_socket_addr_ip_with_port() {
    let addr = url_to_socket_addr("http://10.0.0.1:2380", 2380).unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
    assert_eq!(addr.port(), 2380);
}

#[test]
fn test_url_to_socket_addr_ip_without_port() {
    let addr = url_to_socket_addr("http://10.0.0.1", 2380).unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
    assert_eq!(addr.port(), 2380);
}

#[test]
fn test_url_to_socket_addr_https() {
    let addr = url_to_socket_addr("https://192.168.1.1:2380", 2380).unwrap();
    assert_eq!(addr.ip().to_string(), "192.168.1.1");
    assert_eq!(addr.port(), 2380);
}

#[test]
fn test_url_to_socket_addr_bare_ip_port() {
    let addr = url_to_socket_addr("10.0.0.1:2380", 2380).unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
    assert_eq!(addr.port(), 2380);
}

#[test]
fn test_url_to_socket_addr_ipv6() {
    let addr = url_to_socket_addr("[::1]:2380", 2380).unwrap();
    assert_eq!(addr.port(), 2380);
}

#[test]
fn test_url_to_socket_addr_rejects_dns_hostname() {
    // Sync version cannot resolve DNS hostnames
    let result = url_to_socket_addr("http://barkeeper-0.barkeeper.default.svc.cluster.local:2381", 2380);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("invalid address"));
}

#[test]
fn test_url_to_socket_addr_rejects_bare_dns() {
    let result = url_to_socket_addr("http://my-host", 2380);
    assert!(result.is_err());
}

// ─── strip_url_scheme ───────────────────────────────────────────────────────

#[test]
fn test_strip_url_scheme_http() {
    assert_eq!(strip_url_scheme("http://10.0.0.1:2380"), "10.0.0.1:2380");
}

#[test]
fn test_strip_url_scheme_https() {
    assert_eq!(strip_url_scheme("https://10.0.0.1:2380"), "10.0.0.1:2380");
}

#[test]
fn test_strip_url_scheme_none() {
    assert_eq!(strip_url_scheme("10.0.0.1:2380"), "10.0.0.1:2380");
}

#[test]
fn test_strip_url_scheme_dns_hostname() {
    assert_eq!(
        strip_url_scheme("http://barkeeper-0.barkeeper.default.svc.cluster.local:2381"),
        "barkeeper-0.barkeeper.default.svc.cluster.local:2381"
    );
}

// ─── parse_initial_cluster (sync) ───────────────────────────────────────────

#[test]
fn test_static_parse_with_urls() {
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380,3=http://10.0.0.3:2380";
    let result = parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
    let (_, peers) = result.unwrap();
    assert_eq!(peers.len(), 3);
    assert!(peers.contains_key(&1));
    assert!(peers.contains_key(&2));
    assert!(peers.contains_key(&3));
}

#[test]
fn test_static_parse_preserves_port() {
    let raw = "1=http://10.0.0.1:2381,2=http://10.0.0.2:2381";
    let (_, peers) = parse_initial_cluster(raw, None, 2380).unwrap();
    assert_eq!(peers[&1].port(), 2381);
    assert_eq!(peers[&2].port(), 2381);
}

#[test]
fn test_static_parse_default_port() {
    let raw = "1=http://10.0.0.1,2=http://10.0.0.2";
    let (_, peers) = parse_initial_cluster(raw, None, 2380).unwrap();
    assert_eq!(peers[&1].port(), 2380);
    assert_eq!(peers[&2].port(), 2380);
}

#[test]
fn test_static_parse_empty_entries_skipped() {
    let raw = "1=http://10.0.0.1:2380,,2=http://10.0.0.2:2380,";
    let (_, peers) = parse_initial_cluster(raw, None, 2380).unwrap();
    assert_eq!(peers.len(), 2);
}

#[test]
fn test_static_parse_invalid_node_id() {
    let raw = "abc=http://10.0.0.1:2380";
    let result = parse_initial_cluster(raw, None, 2380);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("invalid node id"));
}

#[test]
fn test_static_parse_missing_equals() {
    // Has '=' in URLs but not in entry format — still detected as static
    // because it contains '='
    let raw = "1=http://10.0.0.1:2380";
    let result = parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
}

#[test]
fn test_dns_mode_returns_empty_peers() {
    let raw = "barkeeper.default.svc.cluster.local";
    let (mode, peers) = parse_initial_cluster(raw, None, 2380).unwrap();
    assert_eq!(mode, ClusterMode::Dns);
    assert!(peers.is_empty());
}

#[test]
fn test_static_parse_rejects_dns_hostnames() {
    // Sync parse_initial_cluster cannot resolve DNS hostnames
    let raw = "1=http://barkeeper-0.barkeeper.default.svc.cluster.local:2381";
    let result = parse_initial_cluster(raw, None, 2380);
    assert!(result.is_err());
}

// ─── resolve_url_to_socket_addr (async) ─────────────────────────────────────

#[tokio::test]
async fn test_resolve_url_ip_with_port() {
    let addr = resolve_url_to_socket_addr("http://10.0.0.1:2380", 2380).await.unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
    assert_eq!(addr.port(), 2380);
}

#[tokio::test]
async fn test_resolve_url_ip_without_port() {
    let addr = resolve_url_to_socket_addr("http://10.0.0.1", 2380).await.unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
    assert_eq!(addr.port(), 2380);
}

#[tokio::test]
async fn test_resolve_url_bare_ip() {
    let addr = resolve_url_to_socket_addr("10.0.0.1:2380", 2380).await.unwrap();
    assert_eq!(addr.ip().to_string(), "10.0.0.1");
}

#[tokio::test]
async fn test_resolve_url_localhost() {
    // localhost should resolve via DNS
    let addr = resolve_url_to_socket_addr("http://localhost:2380", 2380).await.unwrap();
    assert_eq!(addr.port(), 2380);
    // Should be 127.0.0.1 or ::1
    assert!(addr.ip().is_loopback());
}

#[tokio::test]
async fn test_resolve_url_localhost_default_port() {
    let addr = resolve_url_to_socket_addr("http://localhost", 2381).await.unwrap();
    assert_eq!(addr.port(), 2381);
    assert!(addr.ip().is_loopback());
}

#[tokio::test]
async fn test_resolve_url_nonexistent_host_fails() {
    let result = resolve_url_to_socket_addr(
        "http://this-host-definitely-does-not-exist.invalid:2380",
        2380,
    ).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("DNS resolution failed"));
}

// ─── parse_initial_cluster_async ────────────────────────────────────────────

#[tokio::test]
async fn test_async_parse_ip_addresses() {
    // Async version should work fine with IP addresses (fast path)
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380,3=http://10.0.0.3:2380";
    let result = discovery::parse_initial_cluster_async(raw, None, 2380).await;
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, ClusterMode::Static);
    assert_eq!(peers.len(), 3);
}

#[tokio::test]
async fn test_async_parse_localhost_entries() {
    // DNS hostname entries should resolve
    let raw = "1=http://localhost:2381,2=http://localhost:2382,3=http://localhost:2383";
    let result = discovery::parse_initial_cluster_async(raw, None, 2380).await;
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, ClusterMode::Static);
    assert_eq!(peers.len(), 3);
    assert!(peers[&1].ip().is_loopback());
    assert_eq!(peers[&1].port(), 2381);
    assert_eq!(peers[&2].port(), 2382);
    assert_eq!(peers[&3].port(), 2383);
}

#[tokio::test]
async fn test_async_parse_mixed_ip_and_dns() {
    // Mix of IP addresses and DNS hostnames
    let raw = "1=http://10.0.0.1:2381,2=http://localhost:2382";
    let result = discovery::parse_initial_cluster_async(raw, None, 2380).await;
    assert!(result.is_ok());
    let (_, peers) = result.unwrap();
    assert_eq!(peers.len(), 2);
    assert_eq!(peers[&1].ip().to_string(), "10.0.0.1");
    assert!(peers[&2].ip().is_loopback());
}

#[tokio::test]
async fn test_async_parse_nonexistent_dns_fails() {
    let raw = "1=http://this-host-does-not-exist.invalid:2381";
    let result = discovery::parse_initial_cluster_async(raw, None, 2380).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_async_parse_dns_mode_localhost() {
    // DNS mode with localhost — should resolve and return peers
    let result = discovery::parse_initial_cluster_async("localhost", None, 2380).await;
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, ClusterMode::Dns);
    assert!(!peers.is_empty());
    // Should have at least one address
    let first_addr = peers.values().next().unwrap();
    assert!(first_addr.ip().is_loopback());
    assert_eq!(first_addr.port(), 2380);
}

// ─── resolve_dns_seeds ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_resolve_dns_seeds_localhost() {
    let peers = discovery::resolve_dns_seeds("localhost", 2380).await.unwrap();
    assert!(!peers.is_empty());
    for addr in peers.values() {
        assert!(addr.ip().is_loopback());
        assert_eq!(addr.port(), 2380);
    }
}

#[tokio::test]
async fn test_resolve_dns_seeds_deterministic_order() {
    // Calling resolve_dns_seeds twice should produce the same node_id mapping
    let peers1 = discovery::resolve_dns_seeds("localhost", 2380).await.unwrap();
    let peers2 = discovery::resolve_dns_seeds("localhost", 2380).await.unwrap();
    assert_eq!(peers1.len(), peers2.len());
    for (id, addr) in &peers1 {
        assert_eq!(peers2.get(id), Some(addr), "node_id {} mismatch", id);
    }
}

#[tokio::test]
async fn test_resolve_dns_seeds_nonexistent_fails() {
    let result = discovery::resolve_dns_seeds(
        "this-host-does-not-exist.invalid",
        2380,
    ).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("DNS resolution failed"));
}
