//! HTTP/JSON gateway that maps JSON POST requests to the KV store, Lease
//! manager, and Cluster manager -- matching etcd's grpc-gateway endpoints.
//!
//! Byte fields (key, value) are base64-encoded in JSON, mirroring etcd's
//! HTTP gateway behaviour. Numeric int64/uint64 fields are serialized as
//! strings following the proto3 canonical JSON mapping. Fields with default
//! values (0, false, empty) are omitted from responses.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize, Serializer};

use crate::cluster::manager::ClusterManager;
use crate::kv::store::{KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse};
use crate::lease::manager::LeaseManager;
use crate::watch::hub::WatchHub;

// ── Proto3 JSON serialization helpers ──────────────────────────────────────

fn ser_str_u64<S: Serializer>(val: &u64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&val.to_string())
}

fn ser_str_i64<S: Serializer>(val: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&val.to_string())
}

fn is_zero_u64(val: &u64) -> bool {
    *val == 0
}

fn is_zero_i64(val: &i64) -> bool {
    *val == 0
}

fn is_false(val: &bool) -> bool {
    !*val
}

fn is_empty_vec<T>(val: &Vec<T>) -> bool {
    val.is_empty()
}

fn is_empty_str(val: &str) -> bool {
    val.is_empty()
}

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GatewayState {
    pub store: Arc<KvStore>,
    pub watch_hub: Arc<WatchHub>,
    pub lease_manager: Arc<LeaseManager>,
    pub cluster_manager: Arc<ClusterManager>,
    pub cluster_id: u64,
    pub member_id: u64,
}

// ── JSON request / response types ───────────────────────────────────────────
// All int64/uint64 fields serialize as strings per proto3 JSON mapping.
// Fields with default values are omitted per proto3 convention.

#[derive(Debug, Deserialize, Default)]
struct RangeRequest {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    range_end: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    revision: Option<i64>,
}

#[derive(Debug, Serialize)]
struct RangeResponse {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "is_empty_vec")]
    kvs: Vec<JsonKeyValue>,
    #[serde(skip_serializing_if = "is_false")]
    more: bool,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    count: i64,
}

#[derive(Debug, Deserialize, Default)]
struct PutRequest {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    lease: Option<i64>,
    #[serde(default)]
    prev_kv: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PutResponse {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_kv: Option<JsonKeyValue>,
}

#[derive(Debug, Deserialize, Default)]
struct DeleteRangeRequest {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    range_end: Option<String>,
    #[serde(default)]
    prev_kv: Option<bool>,
}

#[derive(Debug, Serialize)]
struct DeleteRangeResponse {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    deleted: i64,
    #[serde(skip_serializing_if = "is_empty_vec")]
    prev_kvs: Vec<JsonKeyValue>,
}

#[derive(Debug, Deserialize, Default)]
struct LeaseGrantRequest {
    #[serde(alias = "TTL", default)]
    ttl: Option<i64>,
    #[serde(alias = "ID", default)]
    id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct LeaseGrantResponse {
    header: JsonResponseHeader,
    #[serde(rename = "ID", serialize_with = "ser_str_i64")]
    id: i64,
    #[serde(rename = "TTL", serialize_with = "ser_str_i64")]
    ttl: i64,
    #[serde(skip_serializing_if = "is_empty_str")]
    error: String,
}

#[derive(Debug, Deserialize, Default)]
struct LeaseRevokeRequest {
    #[serde(alias = "ID", default)]
    id: Option<i64>,
}

#[derive(Debug, Serialize)]
struct LeaseRevokeResponse {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct LeaseTimeToLiveRequest {
    #[serde(alias = "ID", default)]
    id: Option<i64>,
    #[serde(default)]
    keys: Option<bool>,
}

#[derive(Debug, Serialize)]
struct LeaseTimeToLiveResponse {
    header: JsonResponseHeader,
    #[serde(rename = "ID", serialize_with = "ser_str_i64")]
    id: i64,
    #[serde(rename = "TTL", serialize_with = "ser_str_i64")]
    ttl: i64,
    #[serde(rename = "grantedTTL", serialize_with = "ser_str_i64")]
    granted_ttl: i64,
    #[serde(skip_serializing_if = "is_empty_vec")]
    keys: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct LeaseLeasesRequest {}

#[derive(Debug, Serialize)]
struct LeaseLeasesResponse {
    header: JsonResponseHeader,
    leases: Vec<JsonLeaseStatus>,
}

#[derive(Debug, Serialize)]
struct JsonLeaseStatus {
    #[serde(rename = "ID", serialize_with = "ser_str_i64")]
    id: i64,
}

#[derive(Debug, Deserialize, Default)]
struct MemberListRequest {
    #[serde(default)]
    linearizable: Option<bool>,
}

#[derive(Debug, Serialize)]
struct MemberListResponse {
    header: JsonResponseHeader,
    members: Vec<JsonMember>,
}

#[derive(Debug, Serialize)]
struct JsonMember {
    #[serde(rename = "ID", serialize_with = "ser_str_u64")]
    id: u64,
    name: String,
    #[serde(rename = "peerURLs")]
    peer_urls: Vec<String>,
    #[serde(rename = "clientURLs")]
    client_urls: Vec<String>,
    #[serde(rename = "isLearner", skip_serializing_if = "is_false")]
    is_learner: bool,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    header: JsonResponseHeader,
    version: String,
    #[serde(rename = "dbSize", serialize_with = "ser_str_i64")]
    db_size: i64,
    #[serde(serialize_with = "ser_str_u64")]
    leader: u64,
    #[serde(rename = "raftIndex", serialize_with = "ser_str_u64")]
    raft_index: u64,
    #[serde(rename = "raftTerm", serialize_with = "ser_str_u64")]
    raft_term: u64,
    #[serde(rename = "raftAppliedIndex", serialize_with = "ser_str_u64")]
    raft_applied_index: u64,
    #[serde(rename = "dbSizeInUse", serialize_with = "ser_str_i64")]
    db_size_in_use: i64,
}

// ── Txn types ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct TxnRequest {
    #[serde(default)]
    compare: Vec<TxnCompareJson>,
    #[serde(default)]
    success: Vec<TxnRequestOp>,
    #[serde(default)]
    failure: Vec<TxnRequestOp>,
}

#[derive(Debug, Deserialize)]
struct TxnCompareJson {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    result: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    version: Option<i64>,
    #[serde(default)]
    create_revision: Option<i64>,
    #[serde(default)]
    mod_revision: Option<i64>,
    #[serde(default)]
    lease: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct TxnRequestOp {
    #[serde(rename = "requestRange", default)]
    request_range: Option<RangeRequest>,
    #[serde(rename = "requestPut", default)]
    request_put: Option<PutRequest>,
    #[serde(rename = "requestDeleteRange", default)]
    request_delete_range: Option<DeleteRangeRequest>,
}

#[derive(Debug, Serialize)]
struct TxnResponse {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "is_false")]
    succeeded: bool,
    #[serde(skip_serializing_if = "is_empty_vec")]
    responses: Vec<TxnResponseOp>,
}

#[derive(Debug, Serialize)]
struct TxnResponseOp {
    #[serde(skip_serializing_if = "Option::is_none")]
    response_range: Option<RangeResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_put: Option<TxnPutResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_delete_range: Option<DeleteRangeResponse>,
}

#[derive(Debug, Serialize)]
struct TxnPutResponse {
    header: TxnInnerHeader,
    #[serde(skip_serializing_if = "Option::is_none")]
    prev_kv: Option<JsonKeyValue>,
}

#[derive(Debug, Serialize)]
struct TxnInnerHeader {
    #[serde(serialize_with = "ser_str_i64")]
    revision: i64,
}

// Shared types

#[derive(Debug, Serialize, Clone)]
struct JsonResponseHeader {
    #[serde(serialize_with = "ser_str_u64")]
    cluster_id: u64,
    #[serde(serialize_with = "ser_str_u64")]
    member_id: u64,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    revision: i64,
    #[serde(serialize_with = "ser_str_u64")]
    raft_term: u64,
}

#[derive(Debug, Serialize, Clone)]
struct JsonKeyValue {
    key: String,
    #[serde(serialize_with = "ser_str_i64")]
    create_revision: i64,
    #[serde(serialize_with = "ser_str_i64")]
    mod_revision: i64,
    #[serde(serialize_with = "ser_str_i64")]
    version: i64,
    value: String,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    lease: i64,
}

// ── Error helper ────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
    code: u16,
    message: String,
}

fn json_error(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    let msg = msg.into();
    (
        status,
        axum::Json(ErrorBody {
            error: msg.clone(),
            code: status.as_u16(),
            message: msg,
        }),
    )
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn decode_b64(s: &Option<String>) -> Vec<u8> {
    match s {
        Some(v) if !v.is_empty() => B64.decode(v).unwrap_or_default(),
        _ => vec![],
    }
}

fn encode_b64(bytes: &[u8]) -> String {
    B64.encode(bytes)
}

fn proto_kv_to_json(kv: &crate::proto::mvccpb::KeyValue) -> JsonKeyValue {
    JsonKeyValue {
        key: encode_b64(&kv.key),
        create_revision: kv.create_revision,
        mod_revision: kv.mod_revision,
        version: kv.version,
        value: encode_b64(&kv.value),
        lease: kv.lease,
    }
}

impl GatewayState {
    fn make_header(&self, revision: i64) -> JsonResponseHeader {
        JsonResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision,
            raft_term: 0,
        }
    }
}

/// Parse JSON body without requiring Content-Type header (matching etcd behavior).
fn parse_json<T: serde::de::DeserializeOwned + Default>(body: &[u8]) -> T {
    if body.is_empty() {
        return T::default();
    }
    serde_json::from_slice(body).unwrap_or_default()
}

// ── Router constructor ──────────────────────────────────────────────────────

pub fn create_router(
    _raft: crate::raft::node::RaftHandle,
    store: Arc<KvStore>,
    watch_hub: Arc<WatchHub>,
    lease_manager: Arc<LeaseManager>,
    cluster_manager: Arc<ClusterManager>,
    cluster_id: u64,
    member_id: u64,
) -> Router {
    let state = GatewayState {
        store,
        watch_hub,
        lease_manager,
        cluster_manager,
        cluster_id,
        member_id,
    };

    Router::new()
        .route("/v3/kv/range", post(handle_range))
        .route("/v3/kv/put", post(handle_put))
        .route("/v3/kv/deleterange", post(handle_delete_range))
        .route("/v3/kv/txn", post(handle_txn))
        .route("/v3/kv/compaction", post(handle_compaction_stub))
        .route("/v3/lease/grant", post(handle_lease_grant))
        .route("/v3/lease/revoke", post(handle_lease_revoke))
        .route("/v3/lease/timetolive", post(handle_lease_timetolive))
        .route("/v3/lease/leases", post(handle_lease_leases))
        .route("/v3/cluster/member/list", post(handle_cluster_member_list))
        .route("/v3/maintenance/status", post(handle_maintenance_status))
        .with_state(state)
}

// ── Handlers ────────────────────────────────────────────────────────────────
// All handlers accept raw bytes to avoid the Content-Type: application/json
// requirement. etcd's grpc-gateway doesn't require the header.

async fn handle_range(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: RangeRequest = parse_json(&body);
    let key = decode_b64(&req.key);
    let range_end = decode_b64(&req.range_end);
    let limit = req.limit.unwrap_or(0);
    let revision = req.revision.unwrap_or(0);

    match state.store.range(&key, &range_end, limit, revision) {
        Ok(result) => {
            let rev = state.store.current_revision().unwrap_or(0);
            let kvs: Vec<JsonKeyValue> = result.kvs.iter().map(proto_kv_to_json).collect();
            axum::Json(RangeResponse {
                header: state.make_header(rev),
                kvs,
                more: result.more,
                count: result.count,
            })
            .into_response()
        }
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, format!("range: {}", e))
            .into_response(),
    }
}

async fn handle_put(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: PutRequest = parse_json(&body);
    let key = decode_b64(&req.key);
    let value = decode_b64(&req.value);
    let lease_id = req.lease.unwrap_or(0);

    match state.store.put(&key, &value, lease_id) {
        Ok(result) => {
            // Notify watchers of the put event.
            let (create_rev, ver) = match &result.prev_kv {
                Some(prev) => (prev.create_revision, prev.version + 1),
                None => (result.revision, 1),
            };
            let notify_kv = crate::proto::mvccpb::KeyValue {
                key: key.clone(),
                create_revision: create_rev,
                mod_revision: result.revision,
                version: ver,
                value: value.clone(),
                lease: lease_id,
            };
            state.watch_hub.notify(&key, 0, notify_kv, result.prev_kv.clone()).await;

            let prev_kv = if req.prev_kv.unwrap_or(false) {
                result.prev_kv.as_ref().map(proto_kv_to_json)
            } else {
                None
            };

            axum::Json(PutResponse {
                header: state.make_header(result.revision),
                prev_kv,
            })
            .into_response()
        }
        Err(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, format!("put: {}", e)).into_response()
        }
    }
}

async fn handle_delete_range(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: DeleteRangeRequest = parse_json(&body);
    let key = decode_b64(&req.key);
    let range_end = decode_b64(&req.range_end);

    match state.store.delete_range(&key, &range_end) {
        Ok(result) => {
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
                state.watch_hub.notify(&prev.key, 1, tombstone, Some(prev.clone())).await;
            }

            let prev_kvs = if req.prev_kv.unwrap_or(false) {
                result.prev_kvs.iter().map(proto_kv_to_json).collect()
            } else {
                vec![]
            };

            axum::Json(DeleteRangeResponse {
                header: state.make_header(result.revision),
                deleted: result.deleted,
                prev_kvs,
            })
            .into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("delete: {}", e),
        )
        .into_response(),
    }
}

async fn handle_txn(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: TxnRequest = parse_json(&body);

    // Convert JSON compares to internal types.
    let compares: Vec<TxnCompare> = req
        .compare
        .iter()
        .map(|c| {
            let key = decode_b64(&c.key);
            let target = match c.target.as_deref() {
                Some("VALUE") | Some("value") => {
                    TxnCompareTarget::Value(decode_b64(&c.value))
                }
                Some("VERSION") | Some("version") => {
                    TxnCompareTarget::Version(c.version.unwrap_or(0))
                }
                Some("CREATE") | Some("create") => {
                    TxnCompareTarget::CreateRevision(c.create_revision.unwrap_or(0))
                }
                Some("MOD") | Some("mod") => {
                    TxnCompareTarget::ModRevision(c.mod_revision.unwrap_or(0))
                }
                Some("LEASE") | Some("lease") => {
                    TxnCompareTarget::Lease(c.lease.unwrap_or(0))
                }
                _ => TxnCompareTarget::Value(decode_b64(&c.value)),
            };
            let result = match c.result.as_deref() {
                Some("EQUAL") | Some("equal") => TxnCompareResult::Equal,
                Some("GREATER") | Some("greater") => TxnCompareResult::Greater,
                Some("LESS") | Some("less") => TxnCompareResult::Less,
                Some("NOT_EQUAL") | Some("not_equal") => TxnCompareResult::NotEqual,
                _ => TxnCompareResult::Equal,
            };
            TxnCompare {
                key,
                range_end: vec![],
                target,
                result,
            }
        })
        .collect();

    let convert_ops = |ops: &[TxnRequestOp]| -> Vec<TxnOp> {
        ops.iter()
            .filter_map(|op| {
                if let Some(ref r) = op.request_put {
                    Some(TxnOp::Put {
                        key: decode_b64(&r.key),
                        value: decode_b64(&r.value),
                        lease_id: r.lease.unwrap_or(0),
                    })
                } else if let Some(ref r) = op.request_range {
                    Some(TxnOp::Range {
                        key: decode_b64(&r.key),
                        range_end: decode_b64(&r.range_end),
                        limit: r.limit.unwrap_or(0),
                        revision: r.revision.unwrap_or(0),
                    })
                } else if let Some(ref r) = op.request_delete_range {
                    Some(TxnOp::DeleteRange {
                        key: decode_b64(&r.key),
                        range_end: decode_b64(&r.range_end),
                    })
                } else {
                    None
                }
            })
            .collect()
    };

    let success = convert_ops(&req.success);
    let failure = convert_ops(&req.failure);

    match state.store.txn(compares, success, failure) {
        Ok(result) => {
            // TODO: Wire txn watch notifications. Individual put/delete operations
            // within a txn should notify watchers, but this requires iterating over
            // the TxnOpResponse results and extracting the mutated keys. This will
            // be wired in the StoreProcess layer later.

            let responses: Vec<TxnResponseOp> = result
                .responses
                .into_iter()
                .map(|resp| match resp {
                    TxnOpResponse::Range(r) => TxnResponseOp {
                        response_range: Some(RangeResponse {
                            header: state.make_header(0),
                            kvs: r.kvs.iter().map(proto_kv_to_json).collect(),
                            more: r.more,
                            count: r.count,
                        }),
                        response_put: None,
                        response_delete_range: None,
                    },
                    TxnOpResponse::Put(r) => TxnResponseOp {
                        response_range: None,
                        response_put: Some(TxnPutResponse {
                            header: TxnInnerHeader {
                                revision: r.revision,
                            },
                            prev_kv: None, // prev_kv only when explicitly requested
                        }),
                        response_delete_range: None,
                    },
                    TxnOpResponse::DeleteRange(r) => TxnResponseOp {
                        response_range: None,
                        response_put: None,
                        response_delete_range: Some(DeleteRangeResponse {
                            header: state.make_header(r.revision),
                            deleted: r.deleted,
                            prev_kvs: r.prev_kvs.iter().map(proto_kv_to_json).collect(),
                        }),
                    },
                })
                .collect();

            axum::Json(TxnResponse {
                header: state.make_header(result.revision),
                succeeded: result.succeeded,
                responses,
            })
            .into_response()
        }
        Err(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, format!("txn: {}", e)).into_response()
        }
    }
}

async fn handle_compaction_stub() -> impl IntoResponse {
    json_error(
        StatusCode::NOT_IMPLEMENTED,
        "compaction not yet implemented",
    )
}

async fn handle_lease_grant(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: LeaseGrantRequest = parse_json(&body);
    let ttl = req.ttl.unwrap_or(0);
    let id = req.id.unwrap_or(0);

    let lease_id = state.lease_manager.grant(id, ttl).await;
    let rev = state.store.current_revision().unwrap_or(0);

    axum::Json(LeaseGrantResponse {
        header: state.make_header(rev),
        id: lease_id,
        ttl,
        error: String::new(),
    })
}

async fn handle_lease_revoke(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: LeaseRevokeRequest = parse_json(&body);
    let id = req.id.unwrap_or(0);
    let existed = state.lease_manager.revoke(id).await;
    if !existed {
        return json_error(
            StatusCode::NOT_FOUND,
            format!("lease {} not found", id),
        )
        .into_response();
    }

    axum::Json(LeaseRevokeResponse {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_lease_timetolive(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: LeaseTimeToLiveRequest = parse_json(&body);
    let id = req.id.unwrap_or(0);
    let want_keys = req.keys.unwrap_or(false);

    match state.lease_manager.time_to_live(id).await {
        Some((granted_ttl, remaining, key_bytes)) => {
            let keys = if want_keys {
                key_bytes.iter().map(|k| encode_b64(k)).collect()
            } else {
                vec![]
            };

            axum::Json(LeaseTimeToLiveResponse {
                header: state.make_header(0),
                id,
                ttl: remaining,
                granted_ttl,
                keys,
            })
            .into_response()
        }
        None => json_error(
            StatusCode::NOT_FOUND,
            format!("lease {} not found", id),
        )
        .into_response(),
    }
}

async fn handle_lease_leases(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _req: LeaseLeasesRequest = parse_json(&body);
    let ids = state.lease_manager.list().await;
    let leases = ids.into_iter().map(|id| JsonLeaseStatus { id }).collect();
    let rev = state.store.current_revision().unwrap_or(0);

    axum::Json(LeaseLeasesResponse {
        header: state.make_header(rev),
        leases,
    })
}

async fn handle_cluster_member_list(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _req: MemberListRequest = parse_json(&body);
    let members = state.cluster_manager.member_list().await;
    let json_members = members
        .iter()
        .map(|m| JsonMember {
            id: m.id,
            name: m.name.clone(),
            peer_urls: m.peer_urls.clone(),
            client_urls: m.client_urls.clone(),
            is_learner: m.is_learner,
        })
        .collect();

    axum::Json(MemberListResponse {
        header: state.make_header(0),
        members: json_members,
    })
}

async fn handle_maintenance_status(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: serde_json::Value = parse_json(&body);
    let rev = state.store.current_revision().unwrap_or(0);
    let db_size = state.store.db_file_size().unwrap_or(0);

    axum::Json(StatusResponse {
        header: state.make_header(rev),
        version: env!("CARGO_PKG_VERSION").to_string(),
        db_size,
        leader: state.member_id,
        raft_index: 0,
        raft_term: 0,
        raft_applied_index: 0,
        db_size_in_use: db_size,
    })
}
