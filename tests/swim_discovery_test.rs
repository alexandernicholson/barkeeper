// Tests for the --initial-cluster parsing logic.

#[test]
fn test_detect_static_mode() {
    // Contains '=' -> static mode
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380";
    let result = barkeeper::cluster::discovery::parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, barkeeper::cluster::discovery::ClusterMode::Static);
    assert_eq!(peers.len(), 2);
}

#[test]
fn test_detect_dns_mode() {
    // No '=' -> DNS mode (will fail resolution in unit test, but detection works)
    let raw = "barkeeper.default.svc.cluster.local";
    let mode = barkeeper::cluster::discovery::detect_mode(raw);
    assert_eq!(mode, barkeeper::cluster::discovery::ClusterMode::Dns);
}

#[test]
fn test_hostname_ordinal_parsing() {
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal("barkeeper-0"),
        Some(1) // ordinal 0 -> node_id 1
    );
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal("barkeeper-2"),
        Some(3)
    );
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal("my-app-42"),
        Some(43)
    );
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal("no-ordinal"),
        None
    );
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal(""),
        None
    );
}

#[test]
fn test_static_parse_with_urls() {
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380,3=http://10.0.0.3:2380";
    let result = barkeeper::cluster::discovery::parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
    let (_, peers) = result.unwrap();
    assert_eq!(peers.len(), 3);
    assert!(peers.contains_key(&1));
    assert!(peers.contains_key(&2));
    assert!(peers.contains_key(&3));
}

#[test]
fn test_derive_node_id_from_hostname() {
    // When --node-id is not explicitly set, derive from HOSTNAME
    let id = barkeeper::cluster::discovery::derive_node_id(None, Some("barkeeper-2"));
    assert_eq!(id, Ok(3));
}

#[test]
fn test_explicit_node_id_overrides_hostname() {
    // --node-id takes precedence over hostname
    let id = barkeeper::cluster::discovery::derive_node_id(Some(7), Some("barkeeper-2"));
    assert_eq!(id, Ok(7));
}

#[test]
fn test_no_node_id_no_hostname_ordinal_errors() {
    let id = barkeeper::cluster::discovery::derive_node_id(None, Some("no-ordinal"));
    assert!(id.is_err());
}
