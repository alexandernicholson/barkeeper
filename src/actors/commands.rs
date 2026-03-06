use std::sync::Arc;

use tokio::sync::oneshot;

use crate::cluster::manager::Member;
use crate::cluster::membership_sync::MembershipSync;
use crate::kv::state_machine::KvCommand;
use crate::kv::store::{DeleteResult, PutResult, RangeResult, TxnCompare, TxnOp, TxnResult};
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

/// Commands sent to the WatchHubActor.
pub enum WatchHubCmd {
    /// Create a new watch on a key or range. Returns the watch_id and an event
    /// receiver through the reply channel.
    CreateWatch {
        key: Vec<u8>,
        range_end: Vec<u8>,
        start_revision: i64,
        filters: Vec<i32>,
        prev_kv: bool,
        requested_watch_id: i64,
        reply: oneshot::Sender<(i64, tokio::sync::mpsc::Receiver<crate::watch::hub::WatchEvent>)>,
    },
    /// Cancel an existing watch. Returns true if the watcher existed and was removed.
    CancelWatch {
        watch_id: i64,
        reply: oneshot::Sender<bool>,
    },
    /// Notify all matching watchers of a mutation event. Fire-and-forget (no reply).
    Notify {
        key: Vec<u8>,
        event_type: i32, // 0 = PUT, 1 = DELETE
        kv: mvccpb::KeyValue,
        prev_kv: Option<mvccpb::KeyValue>,
    },
}

use crate::auth::manager::{Permission, Role, User};

/// Commands sent to the AuthActor.
pub enum AuthCmd {
    /// Enable authentication.
    AuthEnable {
        reply: oneshot::Sender<()>,
    },
    /// Disable authentication.
    AuthDisable {
        reply: oneshot::Sender<()>,
    },
    /// Check whether authentication is enabled.
    IsEnabled {
        reply: oneshot::Sender<bool>,
    },
    /// Authenticate a user by name and password.
    Authenticate {
        name: String,
        password: String,
        reply: oneshot::Sender<Option<String>>,
    },
    /// Validate a token. Returns the username if valid.
    ValidateToken {
        token: String,
        reply: oneshot::Sender<Option<String>>,
    },
    /// Add a new user.
    UserAdd {
        name: String,
        password: String,
        reply: oneshot::Sender<bool>,
    },
    /// Delete a user by name.
    UserDelete {
        name: String,
        reply: oneshot::Sender<bool>,
    },
    /// Get a user by name.
    UserGet {
        name: String,
        reply: oneshot::Sender<Option<User>>,
    },
    /// List all user names (sorted).
    UserList {
        reply: oneshot::Sender<Vec<String>>,
    },
    /// Change a user's password.
    UserChangePassword {
        name: String,
        password: String,
        reply: oneshot::Sender<bool>,
    },
    /// Grant a role to a user.
    UserGrantRole {
        name: String,
        role: String,
        reply: oneshot::Sender<bool>,
    },
    /// Revoke a role from a user.
    UserRevokeRole {
        name: String,
        role: String,
        reply: oneshot::Sender<bool>,
    },
    /// Add a new role.
    RoleAdd {
        name: String,
        reply: oneshot::Sender<bool>,
    },
    /// Delete a role by name.
    RoleDelete {
        name: String,
        reply: oneshot::Sender<bool>,
    },
    /// Get a role by name.
    RoleGet {
        name: String,
        reply: oneshot::Sender<Option<Role>>,
    },
    /// List all role names (sorted).
    RoleList {
        reply: oneshot::Sender<Vec<String>>,
    },
    /// Grant a permission to a role.
    RoleGrantPermission {
        name: String,
        permission: Permission,
        reply: oneshot::Sender<bool>,
    },
    /// Revoke a permission from a role by matching key and range_end.
    RoleRevokePermission {
        name: String,
        key: Vec<u8>,
        range_end: Vec<u8>,
        reply: oneshot::Sender<bool>,
    },
}

/// Commands sent to the ClusterActor.
pub enum ClusterCmd {
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

/// Commands sent to the KvStoreActor.
pub enum KvStoreCmd {
    /// Put a key-value pair.
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: i64,
        reply: oneshot::Sender<Result<PutResult, String>>,
    },
    /// Query a range of keys.
    Range {
        key: Vec<u8>,
        range_end: Vec<u8>,
        limit: i64,
        revision: i64,
        reply: oneshot::Sender<Result<RangeResult, String>>,
    },
    /// Delete keys in a range.
    DeleteRange {
        key: Vec<u8>,
        range_end: Vec<u8>,
        reply: oneshot::Sender<Result<DeleteResult, String>>,
    },
    /// Execute a transaction (compare-and-swap).
    Txn {
        compares: Vec<TxnCompare>,
        success: Vec<TxnOp>,
        failure: Vec<TxnOp>,
        reply: oneshot::Sender<Result<TxnResult, String>>,
    },
    /// Compact the store up to a revision.
    Compact {
        revision: i64,
        reply: oneshot::Sender<Result<(), String>>,
    },
    /// Get the current revision number.
    CurrentRevision {
        reply: oneshot::Sender<Result<i64, String>>,
    },
    /// Return all mutations since a revision (exclusive).
    ChangesSince {
        after_revision: i64,
        reply: oneshot::Sender<Result<Vec<(Vec<u8>, i32, mvccpb::KeyValue)>, String>>,
    },
    /// Get the database file size in bytes.
    DbFileSize {
        reply: oneshot::Sender<Result<i64, String>>,
    },
    /// Read the entire database file for snapshotting.
    SnapshotBytes {
        reply: oneshot::Sender<Result<Vec<u8>, String>>,
    },
    /// Trigger internal compaction of the redb database.
    CompactDb {
        reply: oneshot::Sender<Result<bool, String>>,
    },
    /// Get the last applied Raft log index.
    LastAppliedRaftIndex {
        reply: oneshot::Sender<Result<u64, String>>,
    },
    /// Set the last applied Raft log index.
    SetLastAppliedRaftIndex {
        index: u64,
        reply: oneshot::Sender<Result<(), String>>,
    },
}
