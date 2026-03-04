use std::sync::Arc;

use tonic::{Request, Response, Status};

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
use crate::watch::hub::WatchHub;

/// gRPC KV service implementing the etcd KV API.
pub struct KvService {
    store: Arc<KvStore>,
    watch_hub: Arc<WatchHub>,
    cluster_id: u64,
    member_id: u64,
}

impl KvService {
    pub fn new(store: Arc<KvStore>, watch_hub: Arc<WatchHub>, cluster_id: u64, member_id: u64) -> Self {
        KvService {
            store,
            watch_hub,
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

        let result = self
            .store
            .put(&req.key, &req.value, req.lease)
            .map_err(|e| Status::internal(format!("put failed: {}", e)))?;

        // Notify watchers of the put event.
        let (create_rev, ver) = match &result.prev_kv {
            Some(prev) => (prev.create_revision, prev.version + 1),
            None => (result.revision, 1),
        };
        let notify_kv = crate::proto::mvccpb::KeyValue {
            key: req.key.clone(),
            create_revision: create_rev,
            mod_revision: result.revision,
            version: ver,
            value: req.value.clone(),
            lease: req.lease,
        };
        self.watch_hub.notify(&req.key, 0, notify_kv, result.prev_kv.clone()).await;

        let prev_kv = if req.prev_kv {
            result.prev_kv
        } else {
            None
        };

        Ok(Response::new(PutResponse {
            header: self.make_header(result.revision),
            prev_kv,
        }))
    }

    async fn delete_range(
        &self,
        request: Request<DeleteRangeRequest>,
    ) -> Result<Response<DeleteRangeResponse>, Status> {
        let req = request.into_inner();

        let result = self
            .store
            .delete_range(&req.key, &req.range_end)
            .map_err(|e| Status::internal(format!("delete failed: {}", e)))?;

        // Notify watchers for each deleted key.
        for prev in &result.prev_kvs {
            let tombstone = crate::proto::mvccpb::KeyValue {
                key: prev.key.clone(),
                create_revision: 0,
                mod_revision: result.revision,
                version: 0,
                value: vec![],
                lease: 0,
            };
            self.watch_hub.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
        }

        let prev_kvs = if req.prev_kv {
            result.prev_kvs
        } else {
            vec![]
        };

        Ok(Response::new(DeleteRangeResponse {
            header: self.make_header(result.revision),
            deleted: result.deleted,
            prev_kvs,
        }))
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

        // TODO: Wire txn watch notifications. Individual put/delete operations
        // within a txn should notify watchers, but this requires iterating over
        // the TxnOpResponse results and extracting the mutated keys. This will
        // be wired in the StoreProcess layer later.

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
