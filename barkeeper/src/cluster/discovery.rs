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

/// Parse --initial-cluster synchronously. Only works with IP addresses.
///
/// Static mode: "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380"
/// DNS mode: "barkeeper.default.svc.cluster.local" (returns empty peers; caller must resolve)
///
/// For DNS hostnames in static mode entries, use `parse_initial_cluster_async` instead.
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

/// Parse --initial-cluster with async DNS resolution.
///
/// Like `parse_initial_cluster`, but resolves DNS hostnames in static mode entries
/// via `tokio::net::lookup_host`. This allows entries like:
/// `1=http://barkeeper-0.barkeeper.default.svc.cluster.local:2381`
///
/// For DNS mode (bare hostname without `=`), calls `resolve_dns_seeds`.
pub async fn parse_initial_cluster_async(
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
                let addr = resolve_url_to_socket_addr(url.trim(), default_port).await?;
                peers.insert(id, addr);
            }
            Ok((ClusterMode::Static, peers))
        }
        ClusterMode::Dns => {
            let peers = resolve_dns_seeds(raw, default_port).await?;
            Ok((ClusterMode::Dns, peers))
        }
    }
}

/// Resolve DNS seeds asynchronously. Returns node_id -> SocketAddr.
///
/// For A records: uses default_port.
/// Addresses are sorted deterministically so that all nodes in the cluster
/// agree on the node_id -> address mapping.
pub async fn resolve_dns_seeds(
    hostname: &str,
    default_port: u16,
) -> Result<HashMap<u64, SocketAddr>, String> {
    use tokio::net;

    // Try A record resolution (hostname:port).
    let lookup = format!("{}:{}", hostname, default_port);
    let mut addrs: Vec<SocketAddr> = net::lookup_host(&lookup)
        .await
        .map_err(|e| format!("DNS resolution failed for '{}': {}", hostname, e))?
        .collect();

    if addrs.is_empty() {
        return Err(format!("no addresses found for '{}'", hostname));
    }

    // Sort deterministically so all nodes assign the same node_id to each address.
    // Sort by IP then port to ensure consistent ordering regardless of DNS response order.
    addrs.sort_by(|a, b| a.ip().cmp(&b.ip()).then(a.port().cmp(&b.port())));

    let mut peers = HashMap::new();
    for (i, addr) in addrs.iter().enumerate() {
        peers.insert((i + 1) as u64, *addr);
    }

    Ok(peers)
}

/// Resolve a URL to a SocketAddr asynchronously.
///
/// Tries to parse as a numeric IP first (fast path). If that fails, performs
/// DNS resolution via `tokio::net::lookup_host`.
///
/// Accepts formats:
/// - `http://10.0.0.1:2380` (IP with port)
/// - `http://10.0.0.1` (IP, uses default_port)
/// - `http://barkeeper-0.barkeeper.default.svc.cluster.local:2381` (DNS with port)
/// - `http://barkeeper-0.barkeeper.default.svc.cluster.local` (DNS, uses default_port)
pub async fn resolve_url_to_socket_addr(
    url: &str,
    default_port: u16,
) -> Result<SocketAddr, String> {
    let host_port = strip_url_scheme(url);

    // Fast path: try parsing as a numeric IP:port.
    if let Ok(addr) = host_port.parse::<SocketAddr>() {
        return Ok(addr);
    }

    // Try adding default port if no port present.
    let with_port = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{}:{}", host_port, default_port)
    };

    // Second attempt: numeric IP with default port.
    if let Ok(addr) = with_port.parse::<SocketAddr>() {
        return Ok(addr);
    }

    // DNS resolution fallback.
    use tokio::net;
    let addr = net::lookup_host(&with_port)
        .await
        .map_err(|e| format!("DNS resolution failed for '{}': {}", url, e))?
        .next()
        .ok_or_else(|| format!("no addresses found for '{}'", url))?;

    Ok(addr)
}

/// Extract host:port from a URL like "http://10.0.0.1:2380".
pub fn strip_url_scheme(url: &str) -> &str {
    url.trim_start_matches("http://")
        .trim_start_matches("https://")
}

/// Extract SocketAddr from a URL like "http://10.0.0.1:2380".
///
/// Only works with numeric IP addresses. For DNS hostnames, use
/// `resolve_url_to_socket_addr` instead.
pub fn url_to_socket_addr(url: &str, default_port: u16) -> Result<SocketAddr, String> {
    let host_port = strip_url_scheme(url);
    host_port
        .parse::<SocketAddr>()
        .or_else(|_| {
            // Try adding default port
            format!("{}:{}", host_port, default_port)
                .parse::<SocketAddr>()
        })
        .map_err(|e| format!("invalid address '{}': {}", url, e))
}
