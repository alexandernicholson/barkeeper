use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::kv::actor::KvStoreActorHandle;
use crate::proto::etcdserverpb::maintenance_server::Maintenance;
use crate::proto::etcdserverpb::{
    alarm_request::AlarmAction, AlarmMember, AlarmRequest, AlarmResponse, AlarmType,
    DefragmentRequest, DefragmentResponse, DowngradeRequest, DowngradeResponse, HashKvRequest,
    HashKvResponse, HashRequest, HashResponse, MoveLeaderRequest, MoveLeaderResponse,
    ResponseHeader, SnapshotRequest, SnapshotResponse, StatusRequest, StatusResponse,
};
use crate::raft::node::RaftHandle;

/// gRPC Maintenance service implementing the etcd Maintenance API.
pub struct MaintenanceService {
    store: KvStoreActorHandle,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
    raft_handle: RaftHandle,
    alarms: Arc<Mutex<Vec<AlarmMember>>>,
}

impl MaintenanceService {
    pub fn new(
        store: KvStoreActorHandle,
        cluster_id: u64,
        member_id: u64,
        raft_term: Arc<AtomicU64>,
        raft_handle: RaftHandle,
    ) -> Self {
        MaintenanceService {
            store,
            cluster_id,
            member_id,
            raft_term,
            raft_handle,
            alarms: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a shared reference to the alarm store for use by the HTTP gateway.
    pub fn alarms(&self) -> Arc<Mutex<Vec<AlarmMember>>> {
        Arc::clone(&self.alarms)
    }

    async fn make_header(&self) -> Option<ResponseHeader> {
        let revision = self.store.current_revision().await.unwrap_or(0);
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
        request: Request<AlarmRequest>,
    ) -> Result<Response<AlarmResponse>, Status> {
        let req = request.into_inner();
        let action = AlarmAction::try_from(req.action).unwrap_or(AlarmAction::Get);
        let alarm_type = AlarmType::try_from(req.alarm).unwrap_or(AlarmType::None);
        let member_id = if req.member_id == 0 {
            self.member_id
        } else {
            req.member_id
        };

        // Fetch header before acquiring the std::sync::Mutex to avoid
        // holding a MutexGuard across an await point.
        let header = self.make_header().await;

        let mut alarms = self.alarms.lock().unwrap();

        match action {
            AlarmAction::Get => {
                // Return all active alarms, optionally filtered by type.
                let result: Vec<AlarmMember> = if alarm_type == AlarmType::None {
                    alarms.clone()
                } else {
                    alarms
                        .iter()
                        .filter(|a| a.alarm == req.alarm)
                        .cloned()
                        .collect()
                };
                Ok(Response::new(AlarmResponse {
                    header,
                    alarms: result,
                }))
            }
            AlarmAction::Activate => {
                // Add the alarm if not already present.
                let new_alarm = AlarmMember {
                    member_id,
                    alarm: req.alarm,
                };
                let already_exists = alarms
                    .iter()
                    .any(|a| a.member_id == member_id && a.alarm == req.alarm);
                if !already_exists {
                    tracing::warn!(
                        member_id,
                        alarm = req.alarm,
                        "alarm activated"
                    );
                    alarms.push(new_alarm.clone());
                }
                Ok(Response::new(AlarmResponse {
                    header,
                    alarms: vec![new_alarm],
                }))
            }
            AlarmAction::Deactivate => {
                // Remove matching alarms.
                let before_len = alarms.len();
                alarms.retain(|a| !(a.member_id == member_id && a.alarm == req.alarm));
                if alarms.len() < before_len {
                    tracing::info!(
                        member_id,
                        alarm = req.alarm,
                        "alarm deactivated"
                    );
                }
                Ok(Response::new(AlarmResponse {
                    header,
                    alarms: vec![],
                }))
            }
        }
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let db_size = self.store.db_file_size().await.unwrap_or(0);

        Ok(Response::new(StatusResponse {
            header: self.make_header().await,
            version: crate::etcd_version().to_string(),
            db_size,
            leader: self.raft_handle.leader_id(),
            raft_index: 0,
            raft_term: self.raft_term.load(Ordering::Relaxed),
            raft_applied_index: self.raft_handle.applied_index(),
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
        tracing::info!("defragment requested");

        // Trigger a write transaction commit which causes redb to reclaim
        // unused pages internally. redb manages its own page-level compaction
        // so an explicit "defragment" reduces to flushing pending work.
        match self.store.compact_db().await {
            Ok(_) => {
                tracing::info!("defragment completed successfully");
                Ok(Response::new(DefragmentResponse {
                    header: self.make_header().await,
                }))
            }
            Err(e) => {
                tracing::error!(error = %e, "defragment failed");
                Err(Status::internal(format!("defragment failed: {}", e)))
            }
        }
    }

    async fn hash(
        &self,
        _request: Request<HashRequest>,
    ) -> Result<Response<HashResponse>, Status> {
        let hash = self.store.hash().await
            .map_err(|e| Status::internal(format!("hash: {}", e)))?;
        Ok(Response::new(HashResponse {
            header: self.make_header().await,
            hash,
        }))
    }

    async fn hash_kv(
        &self,
        request: Request<HashKvRequest>,
    ) -> Result<Response<HashKvResponse>, Status> {
        let revision = request.into_inner().revision;
        let (hash, compact_revision) = self.store.hash_kv(revision).await
            .map_err(|e| Status::internal(format!("hash_kv: {}", e)))?;
        Ok(Response::new(HashKvResponse {
            header: self.make_header().await,
            hash,
            compact_revision,
            hash_revision: revision,
        }))
    }

    type SnapshotStream = ReceiverStream<Result<SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let data = self
            .store
            .snapshot_bytes()
            .await
            .map_err(|e| Status::internal(format!("snapshot: {}", e)))?;

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let header = self.make_header().await;
        let version = crate::etcd_version().to_string();

        tokio::spawn(async move {
            let total = data.len();
            let chunk_size = 64 * 1024; // 64KB chunks
            let mut offset = 0;
            let mut first = true;

            while offset < total {
                let end = (offset + chunk_size).min(total);
                let chunk = data[offset..end].to_vec();
                let remaining = (total - end) as u64;

                let resp = SnapshotResponse {
                    header: if first { header.clone() } else { None },
                    remaining_bytes: remaining,
                    blob: chunk,
                    version: if first { version.clone() } else { String::new() },
                };

                first = false;
                offset = end;

                if tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
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
