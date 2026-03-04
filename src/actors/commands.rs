use tokio::sync::oneshot;

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

/// An expired lease and its attached keys.
pub struct ExpiredLease {
    pub lease_id: i64,
    pub keys: Vec<Vec<u8>>,
}
