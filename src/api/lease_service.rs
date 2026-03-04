use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::lease_server::Lease;
use crate::proto::etcdserverpb::{
    LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest, LeaseKeepAliveResponse,
    LeaseStatus as ProtoLeaseStatus, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
    LeaseRevokeResponse, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse, ResponseHeader,
};

/// gRPC Lease service implementing the etcd Lease API.
pub struct LeaseService {
    manager: Arc<LeaseManager>,
    cluster_id: u64,
    member_id: u64,
}

impl LeaseService {
    pub fn new(manager: Arc<LeaseManager>, cluster_id: u64, member_id: u64) -> Self {
        LeaseService {
            manager,
            cluster_id,
            member_id,
        }
    }

    fn make_header(&self, revision: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 0,
        })
    }
}

#[tonic::async_trait]
impl Lease for LeaseService {
    async fn lease_grant(
        &self,
        request: Request<LeaseGrantRequest>,
    ) -> Result<Response<LeaseGrantResponse>, Status> {
        let req = request.into_inner();

        let id = self.manager.grant(req.id, req.ttl).await;

        Ok(Response::new(LeaseGrantResponse {
            header: self.make_header(0),
            id,
            ttl: req.ttl,
            error: String::new(),
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<LeaseRevokeRequest>,
    ) -> Result<Response<LeaseRevokeResponse>, Status> {
        let req = request.into_inner();

        let existed = self.manager.revoke(req.id).await;
        if !existed {
            return Err(Status::not_found(format!("lease {} not found", req.id)));
        }

        Ok(Response::new(LeaseRevokeResponse {
            header: self.make_header(0),
        }))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut in_stream = request.into_inner();
        let (resp_tx, resp_rx) = mpsc::channel(256);

        let manager = Arc::clone(&self.manager);
        let cluster_id = self.cluster_id;
        let member_id = self.member_id;

        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                let req = match result {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!("lease keep alive stream error: {}", e);
                        break;
                    }
                };

                let ttl = manager.keepalive(req.id).await.unwrap_or(-1);

                let resp = LeaseKeepAliveResponse {
                    header: Some(ResponseHeader {
                        cluster_id,
                        member_id,
                        revision: 0,
                        raft_term: 0,
                    }),
                    id: req.id,
                    ttl,
                };

                if resp_tx.send(Ok(resp)).await.is_err() {
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<LeaseTimeToLiveRequest>,
    ) -> Result<Response<LeaseTimeToLiveResponse>, Status> {
        let req = request.into_inner();

        match self.manager.time_to_live(req.id).await {
            Some((granted_ttl, remaining, keys)) => {
                let response_keys = if req.keys { keys } else { vec![] };

                Ok(Response::new(LeaseTimeToLiveResponse {
                    header: self.make_header(0),
                    id: req.id,
                    ttl: remaining,
                    granted_ttl,
                    keys: response_keys,
                }))
            }
            None => Err(Status::not_found(format!("lease {} not found", req.id))),
        }
    }

    async fn lease_leases(
        &self,
        _request: Request<LeaseLeasesRequest>,
    ) -> Result<Response<LeaseLeasesResponse>, Status> {
        let ids = self.manager.list().await;

        let leases = ids
            .into_iter()
            .map(|id| ProtoLeaseStatus { id })
            .collect();

        Ok(Response::new(LeaseLeasesResponse {
            header: self.make_header(0),
            leases,
        }))
    }
}
