# SWIM Cluster Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace barkeeper's custom gRPC cluster transport with Rebar's DistributedRuntime, SWIM membership, and TCP frame transport — making `--initial-cluster "barkeeper.default.svc.cluster.local"` work via DNS autodiscovery.

**Architecture:** SWIM handles cluster membership discovery and failure detection. Raft stays for data consensus. All actors become proper Rebar distributed processes using `DistributedRuntime`. Raft messages flow as actor messages via `DistributedRouter` over TCP frames instead of gRPC.

**Tech Stack:** Rebar (rebar-core, rebar-cluster), SWIM protocol, TCP transport, DNS SRV/A resolution, msgpack serialization, redb storage (unchanged).

**Design doc:** `docs/plans/2026-03-04-swim-cluster-integration-design.md`

---

## Task 1: DNS Seed Discovery Module

Parse `--initial-cluster` to detect DNS mode vs static mode. Resolve hostnames to peer addresses. Extract node IDs from hostname ordinals.

**Files:**
- Create: `src/cluster/discovery.rs`
- Modify: `src/cluster/mod.rs`
- Test: `tests/swim_discovery_test.rs`

### Step 1: Write failing tests for discovery

Create `tests/swim_discovery_test.rs`:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;

// Tests for the --initial-cluster parsing logic.

#[test]
fn test_detect_static_mode() {
    // Contains '=' → static mode
    let raw = "1=http://10.0.0.1:2380,2=http://10.0.0.2:2380";
    let result = barkeeper::cluster::discovery::parse_initial_cluster(raw, None, 2380);
    assert!(result.is_ok());
    let (mode, peers) = result.unwrap();
    assert_eq!(mode, barkeeper::cluster::discovery::ClusterMode::Static);
    assert_eq!(peers.len(), 2);
}

#[test]
fn test_detect_dns_mode() {
    // No '=' → DNS mode (will fail resolution in unit test, but detection works)
    let raw = "barkeeper.default.svc.cluster.local";
    let mode = barkeeper::cluster::discovery::detect_mode(raw);
    assert_eq!(mode, barkeeper::cluster::discovery::ClusterMode::Dns);
}

#[test]
fn test_hostname_ordinal_parsing() {
    assert_eq!(
        barkeeper::cluster::discovery::parse_hostname_ordinal("barkeeper-0"),
        Some(1) // ordinal 0 → node_id 1
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
```

### Step 2: Run tests to verify they fail

Run: `cargo test --test swim_discovery_test -- --nocapture`
Expected: FAIL — `barkeeper::cluster::discovery` module does not exist.

### Step 3: Implement the discovery module

Create `src/cluster/discovery.rs`:

```rust
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
/// "barkeeper-0" → Some(1), "barkeeper-2" → Some(3), "no-ordinal" → None
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

/// Parse --initial-cluster. Returns the mode and a map of node_id → SocketAddr.
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

/// Resolve DNS seeds asynchronously. Returns node_id → SocketAddr.
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
```

### Step 4: Add module declaration

In `src/cluster/mod.rs`, add:

```rust
pub mod discovery;
```

If `src/cluster/mod.rs` doesn't exist, check if `src/cluster.rs` exists or if the module is declared differently. The module tree in `src/lib.rs` already has `pub mod cluster;`.

### Step 5: Run tests to verify they pass

Run: `cargo test --test swim_discovery_test -- --nocapture`
Expected: All 7 tests PASS.

### Step 6: Commit

```bash
git add src/cluster/discovery.rs src/cluster/mod.rs tests/swim_discovery_test.rs
git commit -m "feat: add DNS seed discovery module for --initial-cluster magic"
```

---

## Task 2: Raft Message Serialization Over Rebar Frames

Implement round-trip serialization of RaftMessage to/from `rmpv::Value` for transport over Rebar's frame protocol. This replaces the JSON-over-gRPC serialization.

**Files:**
- Modify: `src/raft/messages.rs` (add msgpack helpers)
- Test: `tests/rebar_raft_transport_test.rs`

### Step 1: Write failing tests

Create `tests/rebar_raft_transport_test.rs`:

```rust
use barkeeper::raft::messages::*;

#[test]
fn test_raft_message_roundtrip_append_entries_req() {
    let msg = RaftMessage::AppendEntriesReq(AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 10,
        prev_log_term: 2,
        entries: vec![LogEntry {
            term: 3,
            index: 11,
            data: LogEntryData::Command(b"hello".to_vec()),
        }],
        leader_commit: 10,
    });

    let value = barkeeper::raft::messages::raft_message_to_value(&msg);
    let decoded = barkeeper::raft::messages::raft_message_from_value(&value).unwrap();

    match decoded {
        RaftMessage::AppendEntriesReq(req) => {
            assert_eq!(req.term, 3);
            assert_eq!(req.leader_id, 1);
            assert_eq!(req.entries.len(), 1);
        }
        _ => panic!("expected AppendEntriesReq"),
    }
}

#[test]
fn test_raft_message_roundtrip_request_vote() {
    let msg = RaftMessage::RequestVoteReq(RequestVoteRequest {
        term: 5,
        candidate_id: 2,
        last_log_index: 20,
        last_log_term: 4,
    });

    let value = barkeeper::raft::messages::raft_message_to_value(&msg);
    let decoded = barkeeper::raft::messages::raft_message_from_value(&value).unwrap();

    match decoded {
        RaftMessage::RequestVoteReq(req) => {
            assert_eq!(req.term, 5);
            assert_eq!(req.candidate_id, 2);
        }
        _ => panic!("expected RequestVoteReq"),
    }
}

#[test]
fn test_raft_message_roundtrip_all_variants() {
    let messages = vec![
        RaftMessage::AppendEntriesReq(AppendEntriesRequest {
            term: 1, leader_id: 1, prev_log_index: 0, prev_log_term: 0,
            entries: vec![], leader_commit: 0,
        }),
        RaftMessage::AppendEntriesResp(AppendEntriesResponse {
            term: 1, success: true, match_index: 5, node_id: 2,
        }),
        RaftMessage::RequestVoteReq(RequestVoteRequest {
            term: 2, candidate_id: 1, last_log_index: 5, last_log_term: 1,
        }),
        RaftMessage::RequestVoteResp(RequestVoteResponse {
            term: 2, vote_granted: true, node_id: 2,
        }),
    ];

    for msg in messages {
        let value = barkeeper::raft::messages::raft_message_to_value(&msg);
        let decoded = barkeeper::raft::messages::raft_message_from_value(&value).unwrap();
        // Verify roundtrip by re-serializing
        let value2 = barkeeper::raft::messages::raft_message_to_value(&decoded);
        assert_eq!(
            format!("{:?}", value),
            format!("{:?}", value2),
            "roundtrip failed for message"
        );
    }
}
```

### Step 2: Run tests to verify they fail

Run: `cargo test --test rebar_raft_transport_test -- --nocapture`
Expected: FAIL — `raft_message_to_value` and `raft_message_from_value` don't exist.

### Step 3: Implement msgpack serialization

In `src/raft/messages.rs`, add these functions (RaftMessage already derives `Serialize, Deserialize`):

```rust
/// Serialize a RaftMessage to an rmpv::Value for Rebar frame transport.
pub fn raft_message_to_value(msg: &RaftMessage) -> rmpv::Value {
    let bytes = rmp_serde::to_vec(msg).expect("serialize raft message");
    rmpv::Value::Binary(bytes)
}

/// Deserialize a RaftMessage from an rmpv::Value.
pub fn raft_message_from_value(value: &rmpv::Value) -> Result<RaftMessage, String> {
    match value {
        rmpv::Value::Binary(bytes) => {
            rmp_serde::from_slice(bytes).map_err(|e| format!("deserialize raft message: {}", e))
        }
        _ => Err("expected Binary value".into()),
    }
}
```

Add `rmp-serde` and `rmpv` to `Cargo.toml` if not already present:

```toml
rmpv = "1"
rmp-serde = "1"
```

### Step 4: Run tests to verify they pass

Run: `cargo test --test rebar_raft_transport_test -- --nocapture`
Expected: All 3 tests PASS.

### Step 5: Commit

```bash
git add src/raft/messages.rs tests/rebar_raft_transport_test.rs Cargo.toml
git commit -m "feat: add msgpack serialization for Raft messages over Rebar frames"
```

---

## Task 3: RaftProcess as a Rebar Distributed Actor

Convert the Raft node from a raw `tokio::spawn` task to a Rebar process spawned via `DistributedRuntime.runtime().spawn()`. This is the core migration.

**Files:**
- Modify: `src/raft/node.rs` (rewrite spawn_raft_node)
- Modify: `src/api/server.rs` (use DistributedRuntime)
- Modify: `src/config.rs` (add SWIM config)
- Modify: `src/lib.rs` (if needed)
- Test: `tests/registry_raft_test.rs`

### Step 1: Write failing test for Raft actor registration

Create `tests/registry_raft_test.rs`:

```rust
use rebar::DistributedRuntime;
use rebar_cluster::connection::manager::ConnectionManager;
use rebar_cluster::registry::orset::Registry;
use rebar_cluster::transport::tcp::TcpTransport;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn test_raft_process_registers_in_registry() {
    // Create a single-node DistributedRuntime
    let tcp = TcpTransport::new();
    let connector = Box::new(tcp);
    let cm = ConnectionManager::new(connector);
    let mut dr = DistributedRuntime::new(1, cm);
    let registry = Arc::new(Mutex::new(Registry::new()));

    let reg = Arc::clone(&registry);
    let pid = dr.runtime().spawn(move |ctx| {
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
            // Keep alive
            loop {
                if ctx.recv_timeout(std::time::Duration::from_millis(100)).await.is_none() {
                    break;
                }
            }
        }
    }).await;

    // Allow time for spawn
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Verify registration
    let reg = registry.lock().unwrap();
    let entry = reg.lookup("raft:1");
    assert!(entry.is_some(), "raft:1 should be registered");
    assert_eq!(entry.unwrap().pid, pid);
}
```

### Step 2: Run test to verify it fails

Run: `cargo test --test registry_raft_test -- --nocapture`
Expected: FAIL — may compile but helps validate the Rebar API works with barkeeper's dependency setup.

### Step 3: Update Cargo.toml dependencies

Ensure these are in `Cargo.toml`:

```toml
rebar = { git = "https://github.com/alexandernicholson/rebar.git", branch = "main" }
rebar-core = { git = "https://github.com/alexandernicholson/rebar.git", branch = "main" }
rebar-cluster = { git = "https://github.com/alexandernicholson/rebar.git", branch = "main" }
rmpv = "1"
rmp-serde = "1"
```

### Step 4: Run test to verify it passes

Run: `cargo test --test registry_raft_test -- --nocapture`
Expected: PASS.

### Step 5: Rewrite spawn_raft_node to use Rebar ProcessContext

Modify `src/raft/node.rs`. The core change: instead of `tokio::spawn`, use `runtime.spawn()` which gives a `ProcessContext` with `recv()` and `send()`.

The RaftHandle API stays the same externally (proposal_tx, current_term, applied_index) — the change is internal to how the actor runs and receives peer messages.

Key changes in `spawn_raft_node`:
- Accept `runtime: &Runtime` instead of `transport: Option<Arc<dyn RaftTransport>>`
- Accept `registry: Arc<Mutex<Registry>>` for self-registration
- Accept `peers: Arc<Mutex<HashMap<u64, ProcessId>>>` for peer PID lookup
- The actor loop uses `ctx.recv_timeout()` for combined timer + message handling
- Outbound Raft messages use `ctx.send(peer_pid, raft_message_to_value(&msg))`
- Inbound messages arrive via `ctx.recv()` and are deserialized with `raft_message_from_value`

The full implementation is complex — see the design doc for the data flow. The key pattern:

```rust
pub async fn spawn_raft_node(
    config: RaftConfig,
    apply_tx: mpsc::Sender<Vec<LogEntry>>,
    runtime: &Runtime,
    registry: Arc<Mutex<Registry>>,
    peers: Arc<Mutex<HashMap<u64, ProcessId>>>,
) -> RaftHandle {
    // ... setup channels, log store, core init ...

    let pid = runtime.spawn(move |mut ctx| async move {
        // Register in global registry
        let name = format!("raft:{}", config.node_id);
        registry.lock().unwrap().register(
            &name, ctx.self_pid(), config.node_id, 1,
        );

        loop {
            // Use recv_timeout for election timer behavior
            match ctx.recv_timeout(election_timeout).await {
                None => {
                    // Timeout → election timeout
                    let actions = core.step(Event::ElectionTimeout);
                    // execute actions...
                }
                Some(msg) => {
                    // Inbound message from peer or proposal channel
                    // Deserialize and feed to core.step()
                }
            }
        }
    }).await;

    handle
}
```

**Important:** Client proposals still use the mpsc channel (proposal_tx) because they come from gRPC services, not from other Rebar actors. The Rebar mailbox is only for inter-node Raft messages.

### Step 6: Run all existing tests

Run: `cargo test`
Expected: All 99 existing tests PASS (single-node tests should work since DistributedRuntime with no peers behaves identically).

### Step 7: Commit

```bash
git add src/raft/node.rs src/api/server.rs src/config.rs Cargo.toml
git commit -m "feat: migrate RaftProcess to Rebar distributed actor with registry"
```

---

## Task 4: SWIM Service Integration

Wire up SWIM membership protocol to discover peers and feed membership changes to the Raft voter set.

**Files:**
- Create: `src/cluster/swim_service.rs`
- Create: `src/cluster/membership_sync.rs`
- Modify: `src/cluster/mod.rs`
- Modify: `src/api/server.rs`
- Test: `tests/swim_membership_test.rs`

### Step 1: Write failing tests

Create `tests/swim_membership_test.rs`:

```rust
use rebar_cluster::swim::*;
use std::net::SocketAddr;

#[test]
fn test_swim_alive_adds_to_membership() {
    let mut members = MembershipList::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    members.add(Member::new(2, addr));

    assert_eq!(members.alive_count(), 1);
    assert!(members.get(2).is_some());
    assert_eq!(members.get(2).unwrap().state, NodeState::Alive);
}

#[test]
fn test_swim_dead_marks_member() {
    let mut members = MembershipList::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    members.add(Member::new(2, addr));
    members.mark_dead(2);

    assert_eq!(members.get(2).unwrap().state, NodeState::Dead);
    assert_eq!(members.alive_count(), 0);
}

#[test]
fn test_gossip_queue_propagates_alive() {
    let mut queue = GossipQueue::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();
    queue.add(GossipUpdate::Alive {
        node_id: 2,
        addr,
        incarnation: 1,
        cert_hash: None,
    });

    let updates = queue.drain(10);
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        GossipUpdate::Alive { node_id, .. } => assert_eq!(*node_id, 2),
        _ => panic!("expected Alive"),
    }
}

#[test]
fn test_membership_sync_alive_event_adds_voter() {
    use barkeeper::cluster::membership_sync::MembershipSync;
    use std::collections::HashSet;

    let mut sync = MembershipSync::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();

    sync.on_alive(2, addr);
    let voters = sync.current_voters();
    assert!(voters.contains(&2));
}

#[test]
fn test_membership_sync_dead_event_removes_peer() {
    use barkeeper::cluster::membership_sync::MembershipSync;

    let mut sync = MembershipSync::new();
    let addr: SocketAddr = "127.0.0.1:2380".parse().unwrap();

    sync.on_alive(2, addr);
    assert!(sync.current_voters().contains(&2));

    sync.on_dead(2);
    assert!(!sync.has_peer(2));
}
```

### Step 2: Run tests to verify they fail

Run: `cargo test --test swim_membership_test -- --nocapture`
Expected: FAIL — `MembershipSync` doesn't exist.

### Step 3: Implement MembershipSync

Create `src/cluster/membership_sync.rs`:

```rust
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;

/// Bridges SWIM membership events to Raft voter set management.
pub struct MembershipSync {
    /// Active peers: node_id → SocketAddr.
    peers: HashMap<u64, SocketAddr>,
}

impl MembershipSync {
    pub fn new() -> Self {
        MembershipSync {
            peers: HashMap::new(),
        }
    }

    /// A peer is alive (SWIM Alive event).
    pub fn on_alive(&mut self, node_id: u64, addr: SocketAddr) {
        self.peers.insert(node_id, addr);
    }

    /// A peer is dead (SWIM Dead event).
    pub fn on_dead(&mut self, node_id: u64) {
        self.peers.remove(&node_id);
    }

    /// A peer is leaving (SWIM Leave event).
    pub fn on_leave(&mut self, node_id: u64) {
        self.peers.remove(&node_id);
    }

    /// Current set of voter node IDs.
    pub fn current_voters(&self) -> HashSet<u64> {
        self.peers.keys().copied().collect()
    }

    /// Check if a peer is tracked.
    pub fn has_peer(&self, node_id: u64) -> bool {
        self.peers.contains_key(&node_id)
    }

    /// Get all peer addresses.
    pub fn peer_addrs(&self) -> &HashMap<u64, SocketAddr> {
        &self.peers
    }
}
```

Create `src/cluster/swim_service.rs`:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rebar_cluster::swim::*;

/// Runs the SWIM protocol loop for cluster membership discovery.
pub struct SwimService {
    pub config: SwimConfig,
    pub members: MembershipList,
    pub detector: FailureDetector,
    pub gossip: GossipQueue,
    pub node_id: u64,
}

impl SwimService {
    pub fn new(node_id: u64, config: SwimConfig) -> Self {
        SwimService {
            config,
            members: MembershipList::new(),
            detector: FailureDetector::new(),
            gossip: GossipQueue::new(),
            node_id,
        }
    }

    /// Add a seed peer to the membership list.
    pub fn add_seed(&mut self, node_id: u64, addr: SocketAddr) {
        self.members.add(Member::new(node_id, addr));
        self.gossip.add(GossipUpdate::Alive {
            node_id,
            addr,
            incarnation: 0,
            cert_hash: None,
        });
    }

    /// Run one SWIM protocol tick. Returns the node to probe (if any).
    pub fn tick(&mut self) -> Option<u64> {
        let now = Instant::now();

        // Check for suspect → dead transitions
        let newly_dead = self.detector.check_suspect_timeouts(
            &mut self.members,
            &self.config,
            now,
        );
        for id in &newly_dead {
            let addr = self.members.get(*id).map(|m| m.addr).unwrap_or_else(|| {
                "0.0.0.0:0".parse().unwrap()
            });
            self.gossip.add(GossipUpdate::Dead { node_id: *id, addr });
        }

        // Remove expired dead nodes
        self.detector.remove_expired_dead(&mut self.members, &self.config, now);

        // Select a node to probe
        self.detector.tick(&self.members, self.node_id)
    }

    /// Record a successful probe response.
    pub fn record_ack(&mut self, node_id: u64) {
        self.detector.record_ack(&mut self.members, node_id);
    }

    /// Record a failed probe.
    pub fn record_nack(&mut self, node_id: u64) {
        self.detector.record_nack(&mut self.members, node_id, Instant::now());
    }

    /// Get pending gossip updates.
    pub fn drain_gossip(&mut self, max: usize) -> Vec<GossipUpdate> {
        self.gossip.drain(max)
    }

    /// Get all alive members.
    pub fn alive_members(&self) -> Vec<&Member> {
        self.members
            .all_members()
            .filter(|m| m.state == NodeState::Alive)
            .collect()
    }
}
```

### Step 4: Update module declarations

In `src/cluster/mod.rs`:

```rust
pub mod discovery;
pub mod manager;
pub mod membership_sync;
pub mod swim_service;
```

### Step 5: Run tests to verify they pass

Run: `cargo test --test swim_membership_test -- --nocapture`
Expected: All 5 tests PASS.

### Step 6: Commit

```bash
git add src/cluster/swim_service.rs src/cluster/membership_sync.rs src/cluster/mod.rs tests/swim_membership_test.rs
git commit -m "feat: add SWIM service and membership sync bridge"
```

---

## Task 5: Wire DistributedRuntime Into Server Bootstrap

Replace the `Runtime::new()` in `src/api/server.rs` with `DistributedRuntime::new()`. Wire up TCP listener, SWIM bootstrap, and the outbound message processing loop.

**Files:**
- Modify: `src/api/server.rs` (major rewrite of start())
- Modify: `src/main.rs` (new --initial-cluster parsing)
- Modify: `src/config.rs` (updated ClusterConfig)

### Step 1: Update ClusterConfig

Modify `src/config.rs`:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;

use crate::cluster::discovery::ClusterMode;

/// Configuration for cluster bootstrapping.
#[derive(Debug, Clone, Default)]
pub struct ClusterConfig {
    /// How the cluster was specified.
    pub mode: Option<ClusterMode>,
    /// Resolved peer addresses: node_id → SocketAddr.
    pub peers: HashMap<u64, SocketAddr>,
    /// Address to listen on for peer traffic.
    pub listen_peer_addr: Option<SocketAddr>,
    /// Raw --initial-cluster value (for DNS re-resolution).
    pub raw_initial_cluster: Option<String>,
    /// Initial cluster state: "new" or "existing".
    pub initial_cluster_state: String,
}

impl ClusterConfig {
    pub fn is_clustered(&self) -> bool {
        !self.peers.is_empty() || self.raw_initial_cluster.is_some()
    }
}
```

### Step 2: Update main.rs parsing

Modify `src/main.rs` to use the new discovery module:

The `parse_initial_cluster` function is replaced by `cluster::discovery::parse_initial_cluster`. DNS resolution happens async in `main()` before calling `BarkeepServer::start()`. Node ID is derived from hostname when in DNS mode.

### Step 3: Rewrite server.rs to use DistributedRuntime

Key changes in `BarkeepServer::start()`:

1. Replace `Runtime::new(node_id)` with `DistributedRuntime::new(node_id, connection_manager)`
2. Start TCP listener for inbound peer connections
3. Bootstrap SWIM with seed peers from ClusterConfig
4. Spawn the outbound message processing loop (`process_outbound()`)
5. Remove `GrpcRaftTransport` and peer gRPC listener
6. Pass `runtime` to `spawn_raft_node` instead of `transport`

### Step 4: Run all existing tests

Run: `cargo test`
Expected: All 99 existing tests PASS. Single-node tests work because DistributedRuntime with no peers behaves identically to the old Runtime.

### Step 5: Commit

```bash
git add src/api/server.rs src/main.rs src/config.rs
git commit -m "feat: wire DistributedRuntime into server bootstrap with SWIM"
```

---

## Task 6: Replace ClusterManager with SWIM-Backed Implementation

The etcd Cluster gRPC service (MemberList, MemberAdd, MemberRemove) currently uses the in-memory `ClusterManager`. Replace it with an implementation that reads from SWIM's `MembershipList`.

**Files:**
- Modify: `src/cluster/manager.rs` (rewrite to wrap SWIM)
- Modify: `src/api/cluster_service.rs` (if API changes needed)
- Existing tests in `tests/compat_test.rs` must still pass

### Step 1: Verify existing cluster API tests pass before changes

Run: `cargo test --test compat_test test_cluster`
Note which tests exercise the cluster API.

### Step 2: Rewrite ClusterManager to wrap SWIM MembershipList

The `ClusterManager` struct now wraps a reference to the SWIM `MembershipList` and translates between SWIM's `Member` type and etcd's member representation.

### Step 3: Run cluster-related compat tests

Run: `cargo test --test compat_test`
Expected: All pass.

### Step 4: Commit

```bash
git add src/cluster/manager.rs src/api/cluster_service.rs
git commit -m "feat: replace ClusterManager with SWIM-backed membership"
```

---

## Task 7: Update Multi-Node Integration Tests

Migrate `tests/cluster_test.rs` and `tests/replication_test.rs` from `LocalTransport` to `DistributedRuntime` with in-process TCP.

**Files:**
- Modify: `tests/cluster_test.rs`
- Modify: `tests/replication_test.rs`

### Step 1: Read existing cluster_test.rs and replication_test.rs

Understand the current test patterns using `LocalTransport`.

### Step 2: Rewrite tests to use DistributedRuntime

Replace `LocalTransport::new()` with per-node `DistributedRuntime` instances communicating over localhost TCP. Each node:
1. Creates its own `DistributedRuntime`
2. Starts TCP listener on a random port
3. Registers seeds from other nodes
4. SWIM discovers all peers
5. Raft forms consensus

### Step 3: Run modified tests

Run: `cargo test --test cluster_test --test replication_test -- --nocapture`
Expected: All 6 tests PASS.

### Step 4: Commit

```bash
git add tests/cluster_test.rs tests/replication_test.rs
git commit -m "feat: migrate multi-node tests to DistributedRuntime with SWIM"
```

---

## Task 8: Remove Deprecated Transport Code

Remove the old gRPC-based Raft transport now that everything uses Rebar.

**Files:**
- Delete: `src/raft/grpc_transport.rs`
- Delete: `src/raft/transport.rs`
- Delete: `proto/raftpb/raft_transport.proto`
- Modify: `src/raft/mod.rs` (remove module declarations)
- Modify: `build.rs` (remove raft_transport.proto from compile list)
- Modify: `src/lib.rs` (remove raftpb proto module if unused)

### Step 1: Remove files

```bash
rm src/raft/grpc_transport.rs
rm src/raft/transport.rs
rm proto/raftpb/raft_transport.proto
```

### Step 2: Update module declarations

In `src/raft/mod.rs`, remove:
```rust
pub mod grpc_transport;
pub mod transport;
```

In `build.rs`, remove `"proto/raftpb/raft_transport.proto"` from the compile list.

In `src/lib.rs`, remove the `raftpb` proto module if nothing else uses it.

### Step 3: Run full test suite

Run: `cargo test`
Expected: All tests PASS. No compilation errors.

### Step 4: Commit

```bash
git add -A
git commit -m "chore: remove deprecated gRPC Raft transport"
```

---

## Task 9: Graceful Shutdown with NodeDrain

Implement graceful shutdown using Rebar's three-phase drain protocol.

**Files:**
- Modify: `src/api/server.rs` (add shutdown handler)
- Test: manual test (start cluster, Ctrl+C one node, verify others detect departure)

### Step 1: Add signal handler in server.rs

```rust
// In BarkeepServer::start(), after starting all services:
let drain = NodeDrain::new(DrainConfig::default());
tokio::signal::ctrl_c().await?;
tracing::info!("shutting down gracefully");
let result = drain.drain(node_id, listen_addr, &mut gossip, &mut registry, &mut remote_rx, &mut connection_manager, process_count).await;
tracing::info!(?result, "drain complete");
```

### Step 2: Run and verify

Start a 3-node cluster locally, Ctrl+C one node, verify the other two detect the Leave gossip and continue operating.

### Step 3: Commit

```bash
git add src/api/server.rs
git commit -m "feat: add graceful shutdown with NodeDrain protocol"
```

---

## Task 10: Update Documentation

Update all documentation to reflect the new SWIM-based architecture.

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/deployment.md`
- Modify: `docs/developer-guide.md`
- Modify: `docs/extending.md`

### Step 1: Update README.md

- Add SWIM to the features list
- Update architecture diagram (Mermaid) to show SWIM
- Update CLI flags table (no new flags, but document DNS mode for --initial-cluster)
- Add Kubernetes deployment example

### Step 2: Update architecture.md

- Replace ClusterManager references with SWIM MembershipList
- Update actor layout diagrams
- Add SWIM membership flow diagram
- Update transport section (TCP frames, not gRPC)

### Step 3: Update deployment.md

- Add Kubernetes StatefulSet + headless service example
- Show the magic `--initial-cluster "barkeeper.default.svc.cluster.local"`
- Update Docker Compose example to use DNS hostnames

### Step 4: Update developer-guide.md and extending.md

- Update references to transport layer
- Update command flow diagram
- Remove references to GrpcRaftTransport

### Step 5: Commit

```bash
git add README.md docs/
git commit -m "docs: update all documentation for SWIM cluster integration"
```

---

## Task 11: Final Verification

Run the complete test suite and verify nothing is broken.

### Step 1: Run full test suite

Run: `cargo test`
Expected: All tests PASS (99 existing + new tests).

### Step 2: Run clippy

Run: `cargo clippy -- -D warnings`
Expected: No warnings.

### Step 3: Build release binary

Run: `cargo build --release`
Expected: Clean build.

### Step 4: Manual smoke test

```bash
# Single node
./target/release/barkeeper
curl -s http://127.0.0.1:2380/v3/maintenance/status -d '{}' | jq .

# Multi-node (3 terminals)
./target/release/barkeeper --node-id 1 --listen-client-urls 127.0.0.1:2379 --listen-peer-urls http://127.0.0.1:12380 --initial-cluster "1=http://127.0.0.1:12380,2=http://127.0.0.1:22380,3=http://127.0.0.1:32380" --data-dir /tmp/bk1

./target/release/barkeeper --node-id 2 --listen-client-urls 127.0.0.1:2479 --listen-peer-urls http://127.0.0.1:22380 --initial-cluster "1=http://127.0.0.1:12380,2=http://127.0.0.1:22380,3=http://127.0.0.1:32380" --data-dir /tmp/bk2

./target/release/barkeeper --node-id 3 --listen-client-urls 127.0.0.1:2579 --listen-peer-urls http://127.0.0.1:32380 --initial-cluster "1=http://127.0.0.1:12380,2=http://127.0.0.1:22380,3=http://127.0.0.1:32380" --data-dir /tmp/bk3

# Write to any node, read from another
etcdctl --endpoints=127.0.0.1:2379 put hello world
etcdctl --endpoints=127.0.0.1:2479 get hello
```

### Step 5: Final commit

```bash
git add -A
git commit -m "feat: SWIM cluster integration complete"
```
