use serde::{Deserialize, Serialize};

/// Metadata describing the state captured in a snapshot.
///
/// This records the last log entry that was included in the snapshot
/// (so that entries before it can be discarded) and the cluster
/// membership at that point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMeta {
    /// The index of the last log entry included in the snapshot.
    pub last_included_index: u64,
    /// The term of the last log entry included in the snapshot.
    pub last_included_term: u64,
    /// The set of voter node IDs at the time of the snapshot.
    pub voters: Vec<u64>,
    /// The set of learner node IDs at the time of the snapshot.
    pub learners: Vec<u64>,
}

/// A complete snapshot of the state machine.
///
/// Contains the metadata describing the point-in-time of the snapshot
/// and the opaque serialized state machine data.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub meta: SnapshotMeta,
    pub data: Vec<u8>,
}

impl Snapshot {
    /// Create a new snapshot from metadata and serialized state machine data.
    pub fn new(meta: SnapshotMeta, data: Vec<u8>) -> Self {
        Snapshot { meta, data }
    }
}
