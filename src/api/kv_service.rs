use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::kv::actor::KvStoreActorHandle;
use crate::kv::apply_broker::{ApplyResult, ApplyResultBroker};
use crate::kv::apply_notifier::ApplyNotifier;
use crate::kv::state_machine::KvCommand;
use crate::kv::store::{
    KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse,
};
use crate::lease::manager::LeaseManager;
use crate::proto::etcdserverpb::compare::{CompareResult, CompareTarget, TargetUnion};
use crate::proto::etcdserverpb::kv_server::Kv;
use crate::proto::etcdserverpb::{
    request_op, response_op, CompactionRequest, CompactionResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, RequestOp,
    ResponseHeader, ResponseOp, TxnRequest, TxnResponse,
};
use crate::raft::node::RaftHandle;
use crate::raft::messages::ClientProposalResult;

/// gRPC KV service implementing the etcd KV API.
pub struct KvService {
    store: KvStoreActorHandle,
    store_direct: Arc<KvStore>,
    #[allow(dead_code)]
    lease_manager: Arc<LeaseManager>,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
    raft_handle: RaftHandle,
    broker: Arc<ApplyResultBroker>,
    apply_notifier: ApplyNotifier,
}

impl KvService {
    pub fn new(
        store: KvStoreActorHandle,
        store_direct: Arc<KvStore>,
        lease_manager: Arc<LeaseManager>,
        cluster_id: u64,
        member_id: u64,
        raft_term: Arc<AtomicU64>,
        raft_handle: RaftHandle,
        broker: Arc<ApplyResultBroker>,
        apply_notifier: ApplyNotifier,
    ) -> Self {
        KvService {
            store,
            store_direct,
            lease_manager,
            cluster_id,
            member_id,
            raft_term,
            raft_handle,
            broker,
            apply_notifier,
        }
    }

    fn make_header(&self, revision: i64) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: self.raft_term.load(Ordering::Relaxed),
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

        // Since PUT waits for apply before responding, the store is always
        // consistent for reads without needing ReadIndex in single-node mode.

        let result = self
            .store
            .range(req.key.clone(), req.range_end.clone(), req.limit, req.revision)
            .await
            .map_err(|e| Status::internal(format!("range failed: {}", e)))?;

        let revision = self
            .store
            .current_revision()
            .await
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

        // If prev_kv requested, read it BEFORE proposing the new write.
        // Since PUT waits for apply, all previous writes are already applied.
        let prev_kv = if req.prev_kv {
            let store = self.store_direct.clone();
            let key = req.key.clone();
            tokio::task::spawn_blocking(move || {
                store.range(&key, &[], 0, 0).ok()
                    .and_then(|r| r.kvs.into_iter().next())
            })
            .await
            .ok()
            .flatten()
        } else {
            None
        };

        // Serialize command and propose through Raft.
        let cmd = KvCommand::Put {
            key: req.key.clone(),
            value: req.value.clone(),
            lease_id: req.lease,
        };
        let data = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;

        let proposal_result = self.raft_handle.propose(data).await
            .map_err(|e| Status::unavailable(format!("raft: {}", e)))?;

        match proposal_result {
            ClientProposalResult::Success { revision, .. } => {
                // No broker wait needed — WriteBuffer populated before this response.
                Ok(Response::new(PutResponse {
                    header: self.make_header(revision),
                    prev_kv,
                }))
            }
            ClientProposalResult::NotLeader { .. } => {
                Err(Status::unavailable("not leader"))
            }
            ClientProposalResult::Error(e) => {
                Err(Status::internal(e))
            }
        }
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let req = request.into_inner();

        // Serialize command and propose through Raft.
        let cmd = KvCommand::DeleteRange {
            key: req.key.clone(),
            range_end: req.range_end.clone(),
        };
        let data = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;

        let proposal_result = self.raft_handle.propose(data).await
            .map_err(|e| Status::unavailable(format!("raft: {}", e)))?;

        match proposal_result {
            ClientProposalResult::Success { index, .. } => {
                // Wait for the state machine to apply this entry.
                let result = self.broker.wait_for_result(index).await;
                match result {
                    ApplyResult::DeleteRange(del_result) => {
                        let prev_kvs = if req.prev_kv {
                            del_result.prev_kvs
                        } else {
                            vec![]
                        };

                        Ok(Response::new(DeleteRangeResponse {
                            header: self.make_header(del_result.revision),
                            deleted: del_result.deleted,
                            prev_kvs,
                        }))
                    }
                    _ => Err(Status::internal("unexpected apply result for delete_range")),
                }
            }
            ClientProposalResult::NotLeader { .. } => {
                Err(Status::unavailable("not leader"))
            }
            ClientProposalResult::Error(e) => {
                Err(Status::internal(e))
            }
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

        // Convert proto ops to internal types (skip empty request ops).
        let success = req
            .success
            .iter()
            .filter_map(|op| convert_request_op(op).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        let failure = req
            .failure
            .iter()
            .filter_map(|op| convert_request_op(op).transpose())
            .collect::<Result<Vec<_>, _>>()?;

        // Serialize command and propose through Raft.
        let cmd = KvCommand::Txn {
            compares,
            success,
            failure,
        };
        let data = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;

        let proposal_result = self.raft_handle.propose(data).await
            .map_err(|e| Status::unavailable(format!("raft: {}", e)))?;

        match proposal_result {
            ClientProposalResult::Success { index, .. } => {
                // Wait for the state machine to apply this entry.
                let result = self.broker.wait_for_result(index).await;
                match result {
                    ApplyResult::Txn(txn_result) => {
                        // Convert results to proto responses.
                        let responses: Vec<ResponseOp> = txn_result
                            .responses
                            .into_iter()
                            .map(|resp| convert_txn_op_response(resp, self.cluster_id, self.member_id, self.raft_term.load(Ordering::Relaxed)))
                            .collect();

                        Ok(Response::new(TxnResponse {
                            header: self.make_header(txn_result.revision),
                            succeeded: txn_result.succeeded,
                            responses,
                        }))
                    }
                    _ => Err(Status::internal("unexpected apply result for txn")),
                }
            }
            ClientProposalResult::NotLeader { .. } => {
                Err(Status::unavailable("not leader"))
            }
            ClientProposalResult::Error(e) => {
                Err(Status::internal(e))
            }
        }
    }

    async fn compact(
        &self,
        request: Request<CompactionRequest>,
    ) -> Result<Response<CompactionResponse>, Status> {
        let req = request.into_inner();

        // Serialize command and propose through Raft.
        let cmd = KvCommand::Compact {
            revision: req.revision,
        };
        let data = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;

        let proposal_result = self.raft_handle.propose(data).await
            .map_err(|e| Status::unavailable(format!("raft: {}", e)))?;

        match proposal_result {
            ClientProposalResult::Success { index, .. } => {
                // Wait for the state machine to apply this entry.
                let result = self.broker.wait_for_result(index).await;
                match result {
                    ApplyResult::Compact { revision } => {
                        Ok(Response::new(CompactionResponse {
                            header: self.make_header(revision),
                        }))
                    }
                    _ => {
                        // Compact still succeeded even if result type is unexpected.
                        let revision = self
                            .store
                            .current_revision()
                            .await
                            .map_err(|e| Status::internal(format!("revision failed: {}", e)))?;
                        Ok(Response::new(CompactionResponse {
                            header: self.make_header(revision),
                        }))
                    }
                }
            }
            ClientProposalResult::NotLeader { .. } => {
                Err(Status::unavailable("not leader"))
            }
            ClientProposalResult::Error(e) => {
                Err(Status::internal(e))
            }
        }
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

fn convert_request_op(op: &RequestOp) -> Result<Option<TxnOp>, Status> {
    match &op.request {
        Some(request_op::Request::RequestRange(r)) => Ok(Some(TxnOp::Range {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
            limit: r.limit,
            revision: r.revision,
        })),
        Some(request_op::Request::RequestPut(r)) => Ok(Some(TxnOp::Put {
            key: r.key.clone(),
            value: r.value.clone(),
            lease_id: r.lease,
        })),
        Some(request_op::Request::RequestDeleteRange(r)) => Ok(Some(TxnOp::DeleteRange {
            key: r.key.clone(),
            range_end: r.range_end.clone(),
        })),
        Some(request_op::Request::RequestTxn(_)) => {
            Err(Status::unimplemented("nested txn not supported"))
        }
        None => Ok(None), // etcdctl may send empty request ops; skip them
    }
}

fn convert_txn_op_response(
    resp: TxnOpResponse,
    cluster_id: u64,
    member_id: u64,
    raft_term: u64,
) -> ResponseOp {
    let make_header = |revision: i64| {
        Some(ResponseHeader {
            cluster_id,
            member_id,
            revision,
            raft_term,
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
                prev_kv: None, // prev_kv only included when explicitly requested
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
