use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::kv::state_machine::KvCommand;
use crate::kv::store::{
    KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse,
};
use crate::proto::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
use crate::proto::etcdserverpb::kv_server::Kv;
use crate::proto::etcdserverpb::{
    request_op, response_op, CompactionRequest, CompactionResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp,
    ResponseHeader, ResponseOp, TxnRequest, TxnResponse,
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

    async fn txn(&self, request: Request<TxnRequest>) -> Result<Response<TxnResponse>, Status> {
        let req = request.into_inner();

        // Convert proto compares to internal types.
        let compares = req
            .compare
            .iter()
            .map(|c| convert_compare(c))
            .collect::<Result<Vec<_>, _>>()?;

        // Convert proto ops to internal types.
        let success = req
            .success
            .iter()
            .map(|op| convert_request_op(op))
            .collect::<Result<Vec<_>, _>>()?;

        let failure = req
            .failure
            .iter()
            .map(|op| convert_request_op(op))
            .collect::<Result<Vec<_>, _>>()?;

        // Execute the transaction directly on the store.
        let result = self
            .store
            .txn(compares, success, failure)
            .map_err(|e| Status::internal(format!("txn failed: {}", e)))?;

        // Convert results to proto responses.
        let responses: Vec<ResponseOp> = result
            .responses
            .into_iter()
            .map(|resp| convert_txn_op_response(resp, self.cluster_id, self.member_id))
            .collect();

        Ok(Response::new(TxnResponse {
            header: self.make_header(result.revision),
            succeeded: result.succeeded,
            responses,
        }))
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let req = request.into_inner();

        self.store
            .compact(req.revision)
            .map_err(|e| Status::internal(format!("compact failed: {}", e)))?;

        let revision = self
            .store
            .current_revision()
            .map_err(|e| Status::internal(format!("revision failed: {}", e)))?;

        Ok(Response::new(CompactionResponse {
            header: self.make_header(revision),
        }))
    }
}

// ── Proto conversion helpers ────────────────────────────────────────────────

fn convert_compare(c: &crate::proto::etcdserverpb::Compare) -> Result<TxnCompare, Status> {
    let target = match &c.target_union {
        Some(TargetUnion::Version(v)) => TxnCompareTarget::Version(*v),
        Some(TargetUnion::CreateRevision(v)) => TxnCompareTarget::CreateRevision(*v),
        Some(TargetUnion::ModRevision(v)) => TxnCompareTarget::ModRevision(*v),
        Some(TargetUnion::Value(v)) => TxnCompareTarget::Value(v.clone()),
        Some(TargetUnion::Lease(v)) => TxnCompareTarget::Lease(*v),
        None => {
            // Default based on target field.
            let target_enum = CompareTarget::try_from(c.target).unwrap_or(CompareTarget::Version);
            match target_enum {
                CompareTarget::Version => TxnCompareTarget::Version(0),
                CompareTarget::Create => TxnCompareTarget::CreateRevision(0),
                CompareTarget::Mod => TxnCompareTarget::ModRevision(0),
                CompareTarget::Value => TxnCompareTarget::Value(vec![]),
                CompareTarget::Lease => TxnCompareTarget::Lease(0),
            }
        }
    };

    let result_enum = CompareResult::try_from(c.result).unwrap_or(CompareResult::Equal);
    let result = match result_enum {
        CompareResult::Equal => TxnCompareResult::Equal,
        CompareResult::Greater => TxnCompareResult::Greater,
        CompareResult::Less => TxnCompareResult::Less,
        CompareResult::NotEqual => TxnCompareResult::NotEqual,
    };

    Ok(TxnCompare {
        key: c.key.clone(),
        range_end: c.range_end.clone(),
        target,
        result,
    })
}

fn convert_request_op(op: &RequestOp) -> Result<TxnOp, Status> {
    match &op.request {
        Some(request_op::Request::RequestRange(r)) => Ok(TxnOp::Range {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
            revision: r.revision,
        }),
        Some(request_op::Request::RequestPut(r)) => Ok(TxnOp::Put {
            key: r.key.clone(),
            value: r.value.clone(),
            lease_id: r.lease,
        }),
        Some(request_op::Request::RequestDeleteRange(r)) => Ok(TxnOp::DeleteRange {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
        }),
        Some(request_op::Request::RequestTxn(_)) => {
            Err(Status::unimplemented("nested txn not supported"))
        }
        None => Err(Status::invalid_argument("empty request op")),
    }
}

fn convert_txn_op_response(
    resp: TxnOpResponse,
    cluster_id: u64,
    member_id: u64,
) -> ResponseOp {
    let make_header = |revision: i64| {
        Some(ResponseHeader {
            cluster_id,
            member_id,
            revision,
            raft_term: 0,
        })
    };

    match resp {
        TxnOpResponse::Range(r) => ResponseOp {
            response: Some(response_op::Response::ResponseRange(RangeResponse {
                header: make_header(0),
                kvs: r.kvs,
                more: r.more,
                count: r.count,
            })),
        },
        TxnOpResponse::Put(r) => ResponseOp {
            response: Some(response_op::Response::ResponsePut(PutResponse {
                header: make_header(r.revision),
                prev_kv: r.prev_kv,
            })),
        },
        TxnOpResponse::DeleteRange(r) => ResponseOp {
            response: Some(response_op::Response::ResponseDeleteRange(
                DeleteRangeResponse {
                    header: make_header(r.revision),
                    deleted: r.deleted,
                    prev_kvs: r.prev_kvs,
                },
            )),
        },
    }
}
