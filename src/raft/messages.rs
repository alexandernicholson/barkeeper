use rmpv::Value;
use serde::{Deserialize, Serialize};

/// A single entry in the Raft log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: LogEntryData,
}

/// What a log entry contains.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogEntryData {
    /// A client KV operation to apply to the state machine.
    Command(Vec<u8>),
    /// A cluster membership change.
    ConfigChange(ConfigChange),
    /// No-op entry (used after leader election).
    Noop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChange {
    pub change_type: ConfigChangeType,
    pub node_id: u64,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChangeType {
    AddLearner,
    PromoteVoter,
    RemoveNode,
}

// --- Raft RPCs ---

/// AppendEntries RPC (leader -> followers).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
    /// Optimization: if rejected, the follower's last log index for faster backtracking.
    pub last_log_index: u64,
}

/// RequestVote RPC (candidate -> all nodes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteRequest {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

/// InstallSnapshot RPC (leader -> lagging follower).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    pub term: u64,
    pub leader_id: u64,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
    pub done: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    pub term: u64,
}

// --- Internal messages (within a node) ---

/// A client proposal to be replicated through Raft.
#[derive(Debug)]
pub struct ClientProposal {
    pub id: u64,
    pub data: Vec<u8>,
    pub response_tx: tokio::sync::oneshot::Sender<ClientProposalResult>,
}

#[derive(Debug)]
pub enum ClientProposalResult {
    Success { index: u64, revision: i64 },
    NotLeader { leader_id: Option<u64> },
    Error(String),
}

/// Encodes a Raft message into rmpv::Value for Rebar messaging.
pub fn encode_raft_message(msg: &RaftMessage) -> Value {
    let bytes = serde_json::to_vec(msg).expect("raft message serialization");
    Value::Binary(bytes)
}

/// Decodes a Raft message from rmpv::Value.
pub fn decode_raft_message(val: &Value) -> Option<RaftMessage> {
    match val {
        Value::Binary(bytes) => serde_json::from_slice(bytes).ok(),
        _ => None,
    }
}

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

/// Wrapper enum for all Raft RPC messages sent between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntriesReq(AppendEntriesRequest),
    AppendEntriesResp(AppendEntriesResponse),
    RequestVoteReq(RequestVoteRequest),
    RequestVoteResp(RequestVoteResponse),
    InstallSnapshotReq(InstallSnapshotRequest),
    InstallSnapshotResp(InstallSnapshotResponse),
}
