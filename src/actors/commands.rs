use std::sync::Arc;

use tokio::sync::oneshot;

use crate::cluster::manager::Member;
use crate::cluster::membership_sync::MembershipSync;
use crate::kv::state_machine::KvCommand;
use crate::kv::store::{DeleteResult, PutResult, TxnResult};
use crate::proto::mvccpb;

/// Commands sent to the RaftProcess actor.
pub enum RaftCmd {
    /// Propose a KV mutation through Raft consensus.
    Propose {
        command: KvCommand,
        reply: oneshot::Sender<Result<ProposeResult, String>>,
    },
}

/// Result of a successful Raft proposal, returned after commit + apply.
pub struct ProposeResult {
    pub revision: i64,
    pub raft_term: u64,
    pub put_result: Option<PutResult>,
    pub delete_result: Option<DeleteResult>,
    pub txn_result: Option<TxnResult>,
}

/// Commands sent to the StoreProcess actor.
pub enum StoreCmd {
    /// Apply a committed KV command to the store.
    Apply {
        command: KvCommand,
        reply: oneshot::Sender<Result<ApplyResult, String>>,
    },
}

/// Result of applying a command to the store.
pub enum ApplyResult {
    Put(PutResult),
    Delete(DeleteResult),
    Txn(TxnResult),
    Compact,
}

/// Commands sent to the WatchProcess actor.
pub enum WatchCmd {
    /// Notify watchers of a mutation event.
    Notify {
        key: Vec<u8>,
        event_type: i32, // 0 = PUT, 1 = DELETE
        kv: mvccpb::KeyValue,
        prev_kv: Option<mvccpb::KeyValue>,
    },
}

/// Commands sent to the LeaseProcess actor.
pub enum LeaseCmd {
    /// Check for expired leases (called by timer tick).
    CheckExpiry {
        reply: oneshot::Sender<Vec<ExpiredLease>>,
    },
}

pub use crate::lease::manager::ExpiredLease;

/// Commands sent to the ClusterActor.
pub enum ClusterCmd {
    /// Query the cluster ID.
    ClusterId {
        reply: oneshot::Sender<u64>,
    },
    /// Add the initial local member (bootstrap).
    AddInitialMember {
        id: u64,
        name: String,
        peer_urls: Vec<String>,
        client_urls: Vec<String>,
        reply: oneshot::Sender<()>,
    },
    /// List all cluster members.
    MemberList {
        reply: oneshot::Sender<Vec<Member>>,
    },
    /// Add a new member to the cluster.
    MemberAdd {
        peer_urls: Vec<String>,
        is_learner: bool,
        reply: oneshot::Sender<Member>,
    },
    /// Remove a member by ID.
    MemberRemove {
        id: u64,
        reply: oneshot::Sender<bool>,
    },
    /// Update a member's peer URLs.
    MemberUpdate {
        id: u64,
        peer_urls: Vec<String>,
        reply: oneshot::Sender<bool>,
    },
    /// Promote a learner to a voting member.
    MemberPromote {
        id: u64,
        reply: oneshot::Sender<bool>,
    },
    /// Attach a SWIM membership sync for peer discovery.
    SetMembershipSync {
        sync: Arc<std::sync::Mutex<MembershipSync>>,
        reply: oneshot::Sender<()>,
    },
}
