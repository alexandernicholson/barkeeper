use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::kv::state_machine::KvCommand;
use crate::kv::store::KvStore;
use crate::proto::etcdserverpb::kv_server::Kv;
use crate::proto::etcdserverpb::{
    CompactionRequest, CompactionResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
    PutResponse, RangeRequest, RangeResponse, ResponseHeader, TxnRequest, TxnResponse,
};
use crate::raft::messages::ClientProposalResult;
use crate::raft::node::RaftHandle;

/// gRPC KV service implementing the etcd KV API.
pub struct KvService {
    raft: RaftHandle,
    store: Arc<KvStore>,
    cluster_id: u64,
    member_id: u64,
}

impl KvService {
    pub fn new(raft: RaftHandle, store: Arc<KvStore>, cluster_id: u64, member_id: u64) -> Self {
        KvService {
            raft,
            store,
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
impl Kv for KvService {
    async fn range(
        &self,
        request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        let req = request.into_inner();

        let result = self
            .store
            .range(&req.key, &req.range_end, req.limit, req.revision)
            .map_err(|e| Status::internal(format!("range failed: {}", e)))?;

        let revision = self
            .store
            .current_revision()
            .map_err(|e| Status::internal(format!("revision failed: {}", e)))?;

        Ok(Response::new(RangeResponse {
            header: self.make_header(revision),
            kvs: result.kvs,
            more: result.more,
            count: result.count,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();

        let cmd = KvCommand::Put {
            key: req.key.clone(),
            value: req.value.clone(),
            lease_id: req.lease,
        };

        let data =
            serde_json::to_vec(&cmd).map_err(|e| Status::internal(format!("serialize: {}", e)))?;

        let result = self
            .raft
            .propose(data)
            .await
            .map_err(|e| Status::internal(format!("propose failed: {}", e)))?;

        match result {
            ClientProposalResult::Success { revision, .. } => {
                // If prev_kv was requested, read the previous value from the store.
                // Note: the put has already been applied by the time we get here,
                // so the prev_kv is the value at revision - 1.
                let prev_kv = if req.prev_kv && revision > 1 {
                    let prev_result = self
                        .store
                        .range(&req.key, b"", 0, revision - 1)
                        .map_err(|e| Status::internal(format!("prev_kv read: {}", e)))?;
                    prev_result.kvs.into_iter().next()
                } else {
                    None
                };

                Ok(Response::new(PutResponse {
                    header: self.make_header(revision),
                    prev_kv,
                }))
            }
            ClientProposalResult::NotLeader { leader_id } => Err(Status::unavailable(format!(
                "not leader, leader is {:?}",
                leader_id
            ))),
            ClientProposalResult::Error(e) => Err(Status::internal(e)),
        }
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let req = request.into_inner();

        let cmd = KvCommand::DeleteRange {
            key: req.key.clone(),
            range_end: req.range_end.clone(),
        };

        let data =
            serde_json::to_vec(&cmd).map_err(|e| Status::internal(format!("serialize: {}", e)))?;

        let result = self
            .raft
            .propose(data)
            .await
            .map_err(|e| Status::internal(format!("propose failed: {}", e)))?;

        match result {
            ClientProposalResult::Success { revision, .. } => {
                // Read the delete result from the store to get deleted count.
                // The delete has already been applied. We can look at the revision
                // to determine the delete result. For now, we return the revision
                // from Raft and read back the deleted count.
                let deleted_result = self
                    .store
                    .current_revision()
                    .map_err(|e| Status::internal(format!("revision read: {}", e)))?;

                // For prev_kv support, we'd need to read before the delete.
                // Since the delete was applied through Raft, prev_kvs aren't easily
                // available here. Return empty for now unless prev_kv is requested.
                let prev_kvs = vec![];

                // We don't have direct access to the deleted count from the Raft
                // proposal result. The state machine applied the delete but didn't
                // report back the count through the proposal channel. We use the
                // revision to construct the response.
                Ok(Response::new(DeleteRangeResponse {
                    header: self.make_header(revision),
                    deleted: if deleted_result == revision { 1 } else { 0 },
                    prev_kvs,
                }))
            }
            ClientProposalResult::NotLeader { leader_id } => Err(Status::unavailable(format!(
                "not leader, leader is {:?}",
                leader_id
            ))),
            ClientProposalResult::Error(e) => Err(Status::internal(e)),
        }
    }

    async fn txn(&self, _request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        Err(Status::unimplemented("txn not yet implemented"))
    }

    async fn compact(
        &self,
        _request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        Err(Status::unimplemented("compact not yet implemented"))
    }
}
