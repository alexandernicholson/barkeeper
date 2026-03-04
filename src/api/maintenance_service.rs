use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::kv::store::KvStore;
use crate::proto::etcdserverpb::maintenance_server::Maintenance;
use crate::proto::etcdserverpb::{
    AlarmRequest, AlarmResponse, DefragmentRequest, DefragmentResponse, DowngradeRequest,
    DowngradeResponse, HashKvRequest, HashKvResponse, HashRequest, HashResponse,
    MoveLeaderRequest, MoveLeaderResponse, ResponseHeader, SnapshotRequest, SnapshotResponse,
    StatusRequest, StatusResponse,
};

/// gRPC Maintenance service implementing the etcd Maintenance API.
pub struct MaintenanceService {
    store: Arc<KvStore>,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}

impl MaintenanceService {
    pub fn new(store: Arc<KvStore>, cluster_id: u64, member_id: u64, raft_term: Arc<AtomicU64>) -> Self {
        MaintenanceService {
            store,
            cluster_id,
            member_id,
            raft_term,
        }
    }

    fn make_header(&self) -> Option<ResponseHeader> {
        let revision = self.store.current_revision().unwrap_or(0);
        Some(ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: self.raft_term.load(Ordering::Relaxed),
        })
    }
}

#[tonic::async_trait]
impl Maintenance for MaintenanceService {
    async fn alarm(
        &self,
        _request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        // Stub: no alarms active
        Ok(Response::new(AlarmResponse {
            header: self.make_header(),
            alarms: vec![],
        }))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let db_size = self.store.db_file_size().unwrap_or(0);

        Ok(Response::new(StatusResponse {
            header: self.make_header(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            db_size,
            leader: self.member_id,
            raft_index: 0,
            raft_term: self.raft_term.load(Ordering::Relaxed),
            raft_applied_index: 0,
            errors: vec![],
            db_size_in_use: db_size,
            is_learner: false,
            storage_version: "1".to_string(),
            db_size_quota: 0,
            downgrade_info: None,
        }))
    }

    async fn defragment(
        &self,
        _request: Request<DefragmentRequest>,
    ) -> Result<Response<DefragmentResponse>, Status> {
        // Stub: redb handles compaction internally
        Ok(Response::new(DefragmentResponse {
            header: self.make_header(),
        }))
    }

    async fn hash(
        &self,
        _request: Request<HashRequest>,
    ) -> Result<Response<HashResponse>, Status> {
        // Stub: return zero hash
        Ok(Response::new(HashResponse {
            header: self.make_header(),
            hash: 0,
        }))
    }

    async fn hash_kv(
        &self,
        _request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        // Stub: return zero hash
        Ok(Response::new(HashKvResponse {
            header: self.make_header(),
            hash: 0,
            compact_revision: 0,
            hash_revision: 0,
        }))
    }

    type SnapshotStream = ReceiverStream<Result<SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        // Stub: send an empty snapshot with a single chunk
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let header = self.make_header();
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(SnapshotResponse {
                    header,
                    remaining_bytes: 0,
                    blob: vec![],
                    version: env!("CARGO_PKG_VERSION").to_string(),
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn move_leader(
        &self,
        _request: Request<MoveLeaderRequest>,
    ) -> Result<Response<MoveLeaderResponse>, Status> {
        // Stub: single-node, no leader transfer possible
        Err(Status::unimplemented(
            "leader transfer not yet supported in single-node mode",
        ))
    }

    async fn downgrade(
        &self,
        _request: Request<DowngradeRequest>,
    ) -> Result<Response<DowngradeResponse>, Status> {
        // Stub: downgrade not supported
        Err(Status::unimplemented("downgrade not supported"))
    }
}
