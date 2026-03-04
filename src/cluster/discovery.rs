use std::collections::HashMap;
use std::net::SocketAddr;

/// How the cluster was specified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterMode {
    /// Static: explicit ID=URL pairs.
    Static,
    /// DNS: bare hostname, resolve via DNS.
    Dns,
}

/// Detect whether the raw --initial-cluster value is static (ID=URL) or DNS.
pub fn detect_mode(raw: &str) -> ClusterMode {
    if raw.contains('=') {
        ClusterMode::Static
    } else {
        ClusterMode::Dns
    }
}

/// Parse the hostname to extract a StatefulSet ordinal.
/// "barkeeper-0" -> Some(1), "barkeeper-2" -> Some(3), "no-ordinal" -> None
pub fn parse_hostname_ordinal(hostname: &str) -> Option<u64> {
    let parts: Vec<&str> = hostname.rsplitn(2, '-').collect();
    if parts.len() == 2 {
        parts[0].parse::<u64>().ok().map(|n| n + 1)
    } else {
        None
    }
}

/// Derive node ID: explicit --node-id wins, otherwise parse from hostname.
pub fn derive_node_id(
    explicit: Option<u64>,
    hostname: Option<&str>,
) -> Result<u64, String> {
    if let Some(id) = explicit {
        return Ok(id);
    }
    if let Some(host) = hostname {
        if let Some(id) = parse_hostname_ordinal(host) {
            return Ok(id);
        }
    }
    Err("cannot derive node ID: set --node-id or use a hostname with an ordinal suffix (e.g. barkeeper-0)".into())
}

/// Parse --initial-cluster. Returns the mode and a map of node_id -> SocketAddr.
///
/// Static mode: "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380"
/// DNS mode: "barkeeper.default.svc.cluster.local" (async resolution not done here)
pub fn parse_initial_cluster(
    raw: &str,
    _hostname: Option<&str>,
    default_port: u16,
) -> Result<(ClusterMode, HashMap<u64, SocketAddr>), String> {
    let mode = detect_mode(raw);
    match mode {
        ClusterMode::Static => {
            let mut peers = HashMap::new();
            for entry in raw.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                let (id_str, url) = entry
                    .split_once('=')
                    .ok_or_else(|| format!("invalid cluster entry: '{}'", entry))?;
                let id: u64 = id_str
                    .trim()
                    .parse()
                    .map_err(|e| format!("invalid node id '{}': {}", id_str, e))?;
                let addr = url_to_socket_addr(url.trim(), default_port)?;
                peers.insert(id, addr);
            }
            Ok((ClusterMode::Static, peers))
        }
        ClusterMode::Dns => {
            // DNS resolution is async and happens at startup, not here.
            // Return empty peers; caller will resolve.
            Ok((ClusterMode::Dns, HashMap::new()))
        }
    }
}

/// Resolve DNS seeds asynchronously. Returns node_id -> SocketAddr.
///
/// For SRV records: uses port from the record.
/// For A records: uses default_port.
/// Node IDs are derived from hostname ordinals.
pub async fn resolve_dns_seeds(
    hostname: &str,
    default_port: u16,
) -> Result<HashMap<u64, SocketAddr>, String> {
    use tokio::net;

    // Try A record resolution (hostname:port).
    let lookup = format!("{}:{}", hostname, default_port);
    let addrs: Vec<SocketAddr> = net::lookup_host(&lookup)
        .await
        .map_err(|e| format!("DNS resolution failed for '{}': {}", hostname, e))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("no addresses found for '{}'", hostname));
    }

    // For A record results, we assign sequential node IDs starting at 1.
    // In production K8s, SRV records would give us hostnames with ordinals.
    let mut peers = HashMap::new();
    for (i, addr) in addrs.iter().enumerate() {
        peers.insert((i + 1) as u64, *addr);
    }

    Ok(peers)
}

/// Extract SocketAddr from a URL like "http://10.0.0.1:2380".
fn url_to_socket_addr(url: &str, default_port: u16) -> Result<SocketAddr, String> {
    let host_port = url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    host_port
        .parse::<SocketAddr>()
        .or_else(|_| {
            // Try adding default port
            format!("{}:{}", host_port, default_port)
                .parse::<SocketAddr>()
        })
        .map_err(|e| format!("invalid address '{}': {}", url, e))
}
