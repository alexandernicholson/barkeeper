//! HTTP/JSON gateway that maps JSON POST requests to the KV store, Lease
//! manager, and Cluster manager -- matching etcd's grpc-gateway endpoints.
//!
//! Byte fields (key, value) are base64-encoded in JSON, mirroring etcd's
//! HTTP gateway behaviour. Numeric int64/uint64 fields are serialized as
//! strings following the proto3 canonical JSON mapping. Fields with default
//! values (0, false, empty) are omitted from responses.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::sse::{Event as SseEvent, Sse};
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize, Serializer};

use crate::auth::actor::AuthActorHandle;
use crate::proto::etcdserverpb::{alarm_request::AlarmAction, AlarmMember, AlarmType};
use crate::cluster::actor::ClusterActorHandle;
use crate::kv::actor::KvStoreActorHandle;
use crate::kv::apply_broker::{ApplyResult, ApplyResultBroker};
use crate::kv::state_machine::KvCommand;
use crate::kv::store::{KvStore, TxnCompare, TxnCompareResult, TxnCompareTarget, TxnOp, TxnOpResponse};
use crate::lease::manager::LeaseManager;
use crate::raft::messages::ClientProposalResult;
use crate::raft::node::RaftHandle;
use crate::watch::actor::WatchHubActorHandle;

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
    pub store: KvStoreActorHandle,
    pub store_direct: Arc<KvStore>,
    pub watch_hub: WatchHubActorHandle,
    pub lease_manager: Arc<LeaseManager>,
    pub cluster_manager: ClusterActorHandle,
    pub auth_manager: AuthActorHandle,
    pub cluster_id: u64,
    pub member_id: u64,
    pub raft_term: Arc<AtomicU64>,
    pub raft_handle: RaftHandle,
    pub alarms: Arc<Mutex<Vec<AlarmMember>>>,
    pub broker: Arc<ApplyResultBroker>,
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

// ── Compaction types ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct CompactionRequest {
    #[serde(default)]
    revision: Option<String>, // proto3 JSON: int64 as string
}

#[derive(Debug, Serialize)]
struct CompactionResponse {
    header: JsonResponseHeader,
}

// ── Watch types ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct WatchCreateRequest {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    range_end: Option<String>,
    #[serde(default)]
    start_revision: Option<i64>,
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
            raft_term: self.raft_term.load(Ordering::Relaxed),
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

// ── Auth middleware ─────────────────────────────────────────────────────────

/// Axum middleware that enforces authentication when auth is enabled.
///
/// - Auth endpoints (`/v3/auth/*`) always pass through so clients can
///   authenticate, enable/disable auth, and manage users and roles.
/// - When auth is disabled, all requests pass through.
/// - When auth is enabled, non-auth requests must include a valid token in
///   the `Authorization` header. Requests without a valid token are rejected
///   with 401 Unauthorized.
async fn auth_middleware(
    State(state): State<GatewayState>,
    request: Request,
    next: Next,
) -> axum::response::Response {
    let path = request.uri().path().to_string();

    // Auth endpoints are always accessible.
    if path.starts_with("/v3/auth/") {
        return next.run(request).await;
    }

    // If auth is not enabled, pass through.
    if !state.auth_manager.is_enabled().await {
        return next.run(request).await;
    }

    // Auth is enabled — check for a valid token.
    let token = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match token {
        Some(token_str) => {
            if state.auth_manager.validate_token(token_str).await.is_some() {
                next.run(request).await
            } else {
                json_error(StatusCode::UNAUTHORIZED, "invalid auth token").into_response()
            }
        }
        None => {
            json_error(StatusCode::UNAUTHORIZED, "auth token is not provided").into_response()
        }
    }
}

// ── Router constructor ──────────────────────────────────────────────────────

pub fn create_router(
    raft_handle: RaftHandle,
    store: KvStoreActorHandle,
    store_direct: Arc<KvStore>,
    watch_hub: WatchHubActorHandle,
    lease_manager: Arc<LeaseManager>,
    cluster_manager: ClusterActorHandle,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
    auth_manager: AuthActorHandle,
    alarms: Arc<Mutex<Vec<AlarmMember>>>,
    broker: Arc<ApplyResultBroker>,
) -> Router {
    let state = GatewayState {
        store,
        store_direct,
        watch_hub,
        lease_manager,
        cluster_manager,
        auth_manager,
        cluster_id,
        member_id,
        raft_term,
        raft_handle,
        alarms,
        broker,
    };

    Router::new()
        .route("/v3/kv/range", post(handle_range))
        .route("/v3/kv/put", post(handle_put))
        .route("/v3/kv/deleterange", post(handle_delete_range))
        .route("/v3/kv/txn", post(handle_txn))
        .route("/v3/kv/compaction", post(handle_compaction))
        .route("/v3/lease/grant", post(handle_lease_grant))
        .route("/v3/lease/revoke", post(handle_lease_revoke))
        .route("/v3/lease/timetolive", post(handle_lease_timetolive))
        .route("/v3/lease/leases", post(handle_lease_leases))
        .route("/v3/cluster/member/list", post(handle_cluster_member_list))
        .route("/v3/maintenance/status", post(handle_maintenance_status))
        .route("/v3/maintenance/defragment", post(handle_maintenance_defragment))
        .route("/v3/maintenance/alarm", post(handle_maintenance_alarm))
        .route("/v3/maintenance/snapshot", post(handle_maintenance_snapshot))
        // Watch SSE endpoint
        .route("/v3/watch", post(handle_watch_sse))
        // Auth endpoints
        .route("/v3/auth/enable", post(handle_auth_enable))
        .route("/v3/auth/disable", post(handle_auth_disable))
        .route("/v3/auth/status", post(handle_auth_status))
        .route("/v3/auth/authenticate", post(handle_auth_authenticate))
        .route("/v3/auth/user/add", post(handle_auth_user_add))
        .route("/v3/auth/user/get", post(handle_auth_user_get))
        .route("/v3/auth/user/list", post(handle_auth_user_list))
        .route("/v3/auth/user/delete", post(handle_auth_user_delete))
        .route("/v3/auth/user/changepw", post(handle_auth_user_change_password))
        .route("/v3/auth/user/grant", post(handle_auth_user_grant_role))
        .route("/v3/auth/user/revoke", post(handle_auth_user_revoke_role))
        .route("/v3/auth/role/add", post(handle_auth_role_add))
        .route("/v3/auth/role/get", post(handle_auth_role_get))
        .route("/v3/auth/role/list", post(handle_auth_role_list))
        .route("/v3/auth/role/delete", post(handle_auth_role_delete))
        .route("/v3/auth/role/grant", post(handle_auth_role_grant_permission))
        .route("/v3/auth/role/revoke", post(handle_auth_role_revoke_permission))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
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

    // Bypass the actor channel — run the range query directly on the
    // blocking thread pool.  redb supports concurrent read transactions
    // so multiple reads execute in parallel without serialising through
    // the single-threaded actor.
    let store = Arc::clone(&state.store_direct);
    let result = tokio::task::spawn_blocking(move || {
        store.range(&key, &range_end, limit, revision)
    })
    .await
    .expect("range spawn_blocking");

    match result {
        Ok(result) => {
            let rev = state.store_direct.current_revision().unwrap_or(0);
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

    // Serialize command and propose through Raft.
    let cmd = KvCommand::Put {
        key: key.clone(),
        value: value.clone(),
        lease_id,
    };
    let data = match bincode::serialize(&cmd) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let proposal_result = match state.raft_handle.propose(data).await {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::SERVICE_UNAVAILABLE, format!("raft: {}", e)).into_response(),
    };

    match proposal_result {
        ClientProposalResult::Success { index, .. } => {
            // Wait for the state machine to apply this entry.
            let result = state.broker.wait_for_result(index).await;
            match result {
                ApplyResult::Put(put_result) => {
                    let prev_kv = if req.prev_kv.unwrap_or(false) {
                        put_result.prev_kv.as_ref().map(proto_kv_to_json)
                    } else {
                        None
                    };

                    axum::Json(PutResponse {
                        header: state.make_header(put_result.revision),
                        prev_kv,
                    })
                    .into_response()
                }
                _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected apply result").into_response(),
            }
        }
        ClientProposalResult::NotLeader { .. } => {
            json_error(StatusCode::SERVICE_UNAVAILABLE, "not leader").into_response()
        }
        ClientProposalResult::Error(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
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

    // Serialize command and propose through Raft.
    let cmd = KvCommand::DeleteRange {
        key: key.clone(),
        range_end: range_end.clone(),
    };
    let data = match bincode::serialize(&cmd) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let proposal_result = match state.raft_handle.propose(data).await {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::SERVICE_UNAVAILABLE, format!("raft: {}", e)).into_response(),
    };

    match proposal_result {
        ClientProposalResult::Success { index, .. } => {
            // Wait for the state machine to apply this entry.
            let result = state.broker.wait_for_result(index).await;
            match result {
                ApplyResult::DeleteRange(del_result) => {
                    let prev_kvs = if req.prev_kv.unwrap_or(false) {
                        del_result.prev_kvs.iter().map(proto_kv_to_json).collect()
                    } else {
                        vec![]
                    };

                    axum::Json(DeleteRangeResponse {
                        header: state.make_header(del_result.revision),
                        deleted: del_result.deleted,
                        prev_kvs,
                    })
                    .into_response()
                }
                _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected apply result").into_response(),
            }
        }
        ClientProposalResult::NotLeader { .. } => {
            json_error(StatusCode::SERVICE_UNAVAILABLE, "not leader").into_response()
        }
        ClientProposalResult::Error(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
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

    // Serialize command and propose through Raft.
    let cmd = KvCommand::Txn {
        compares: compares.clone(),
        success: success.clone(),
        failure: failure.clone(),
    };
    let data = match bincode::serialize(&cmd) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let proposal_result = match state.raft_handle.propose(data).await {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::SERVICE_UNAVAILABLE, format!("raft: {}", e)).into_response(),
    };

    match proposal_result {
        ClientProposalResult::Success { index, .. } => {
            // Wait for the state machine to apply this entry.
            let apply_result = state.broker.wait_for_result(index).await;
            match apply_result {
                ApplyResult::Txn(result) => {
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
                                    prev_kv: None,
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
                _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, "unexpected apply result").into_response(),
            }
        }
        ClientProposalResult::NotLeader { .. } => {
            json_error(StatusCode::SERVICE_UNAVAILABLE, "not leader").into_response()
        }
        ClientProposalResult::Error(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

async fn handle_compaction(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: CompactionRequest = parse_json(&body);
    let revision: i64 = req.revision
        .as_deref()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);

    // Serialize command and propose through Raft.
    let cmd = KvCommand::Compact { revision };
    let data = match bincode::serialize(&cmd) {
        Ok(d) => d,
        Err(e) => return json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let proposal_result = match state.raft_handle.propose(data).await {
        Ok(r) => r,
        Err(e) => return json_error(StatusCode::SERVICE_UNAVAILABLE, format!("raft: {}", e)).into_response(),
    };

    match proposal_result {
        ClientProposalResult::Success { index, .. } => {
            // Wait for the state machine to apply this entry.
            let result = state.broker.wait_for_result(index).await;
            let rev = match result {
                ApplyResult::Compact { revision } => revision,
                _ => state.store.current_revision().await.unwrap_or(0),
            };
            axum::Json(CompactionResponse {
                header: state.make_header(rev),
            })
            .into_response()
        }
        ClientProposalResult::NotLeader { .. } => {
            json_error(StatusCode::SERVICE_UNAVAILABLE, "not leader").into_response()
        }
        ClientProposalResult::Error(e) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
    }
}

async fn handle_lease_grant(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: LeaseGrantRequest = parse_json(&body);
    let ttl = req.ttl.unwrap_or(0);
    let id = req.id.unwrap_or(0);

    let lease_id = state.lease_manager.grant(id, ttl).await;
    let rev = state.store.current_revision().await.unwrap_or(0);

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
    let keys = match state.lease_manager.revoke(id).await {
        Some(keys) => keys,
        None => {
            return json_error(
                StatusCode::NOT_FOUND,
                format!("lease {} not found", id),
            )
            .into_response();
        }
    };

    // Delete all keys attached to the revoked lease.
    for key in keys {
        let _ = state.store.delete_range(key, vec![]).await;
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
    let rev = state.store.current_revision().await.unwrap_or(0);

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
    let rev = state.store.current_revision().await.unwrap_or(0);
    let db_size = state.store.db_file_size().await.unwrap_or(0);

    axum::Json(StatusResponse {
        header: state.make_header(rev),
        version: env!("CARGO_PKG_VERSION").to_string(),
        db_size,
        leader: state.raft_handle.leader_id(),
        raft_index: 0,
        raft_term: state.raft_term.load(Ordering::Relaxed),
        raft_applied_index: state.raft_handle.applied_index(),
        db_size_in_use: db_size,
    })
}

async fn handle_maintenance_snapshot(
    State(state): State<GatewayState>,
) -> impl IntoResponse {
    match state.store.snapshot_bytes().await {
        Ok(data) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response(),
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("snapshot: {}", e),
        )
        .into_response(),
    }
}

// ── Defragment types ───────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct DefragmentReq {}

#[derive(Debug, Serialize)]
struct DefragmentResp {
    header: JsonResponseHeader,
}

async fn handle_maintenance_defragment(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: DefragmentReq = parse_json(&body);

    match state.store.compact_db().await {
        Ok(_) => {
            let rev = state.store.current_revision().await.unwrap_or(0);
            axum::Json(DefragmentResp {
                header: state.make_header(rev),
            })
            .into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("defragment failed: {}", e),
        )
        .into_response(),
    }
}

// ── Alarm types ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct AlarmReq {
    #[serde(default)]
    action: Option<i32>,
    #[serde(default, rename = "memberID")]
    member_id: Option<String>, // proto3 JSON: uint64 as string
    #[serde(default)]
    alarm: Option<i32>,
}

#[derive(Debug, Serialize)]
struct AlarmResp {
    header: JsonResponseHeader,
    #[serde(skip_serializing_if = "is_empty_vec")]
    alarms: Vec<JsonAlarmMember>,
}

#[derive(Debug, Serialize)]
struct JsonAlarmMember {
    #[serde(rename = "memberID", skip_serializing_if = "is_zero_u64", serialize_with = "ser_str_u64")]
    member_id: u64,
    #[serde(skip_serializing_if = "is_zero_i64", serialize_with = "ser_str_i64")]
    alarm: i64,
}

async fn handle_maintenance_alarm(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AlarmReq = parse_json(&body);
    let action = req.action.unwrap_or(0);
    let alarm_value = req.alarm.unwrap_or(0);
    let member_id: u64 = req
        .member_id
        .as_deref()
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let effective_member_id = if member_id == 0 {
        state.member_id
    } else {
        member_id
    };

    let rev = state.store.current_revision().await.unwrap_or(0);

    let alarm_action = AlarmAction::try_from(action).unwrap_or(AlarmAction::Get);
    let mut alarms_lock = state.alarms.lock().unwrap();

    let result_alarms: Vec<JsonAlarmMember> = match alarm_action {
        AlarmAction::Get => {
            let alarm_type = AlarmType::try_from(alarm_value).unwrap_or(AlarmType::None);
            if alarm_type == AlarmType::None {
                alarms_lock
                    .iter()
                    .map(|a| JsonAlarmMember {
                        member_id: a.member_id,
                        alarm: a.alarm as i64,
                    })
                    .collect()
            } else {
                alarms_lock
                    .iter()
                    .filter(|a| a.alarm == alarm_value)
                    .map(|a| JsonAlarmMember {
                        member_id: a.member_id,
                        alarm: a.alarm as i64,
                    })
                    .collect()
            }
        }
        AlarmAction::Activate => {
            let new_alarm = AlarmMember {
                member_id: effective_member_id,
                alarm: alarm_value,
            };
            let already_exists = alarms_lock
                .iter()
                .any(|a| a.member_id == effective_member_id && a.alarm == alarm_value);
            if !already_exists {
                alarms_lock.push(new_alarm);
            }
            vec![JsonAlarmMember {
                member_id: effective_member_id,
                alarm: alarm_value as i64,
            }]
        }
        AlarmAction::Deactivate => {
            alarms_lock
                .retain(|a| !(a.member_id == effective_member_id && a.alarm == alarm_value));
            vec![]
        }
    };

    axum::Json(AlarmResp {
        header: state.make_header(rev),
        alarms: result_alarms,
    })
}

// ── Auth JSON request / response types ──────────────────────────────────────

#[derive(Debug, Deserialize, Default)]
struct AuthEnableReq {}

#[derive(Debug, Serialize)]
struct AuthEnableResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthDisableReq {}

#[derive(Debug, Serialize)]
struct AuthDisableResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthStatusReq {}

#[derive(Debug, Serialize)]
struct AuthStatusResp {
    header: JsonResponseHeader,
    enabled: bool,
    #[serde(rename = "authRevision", serialize_with = "ser_str_u64")]
    auth_revision: u64,
}

#[derive(Debug, Deserialize, Default)]
struct AuthenticateReq {
    #[serde(default)]
    name: String,
    #[serde(default)]
    password: String,
}

#[derive(Debug, Serialize)]
struct AuthenticateResp {
    header: JsonResponseHeader,
    token: String,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserAddReq {
    #[serde(default)]
    name: String,
    #[serde(default)]
    password: String,
}

#[derive(Debug, Serialize)]
struct AuthUserAddResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserGetReq {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Serialize)]
struct AuthUserGetResp {
    header: JsonResponseHeader,
    roles: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserListReq {}

#[derive(Debug, Serialize)]
struct AuthUserListResp {
    header: JsonResponseHeader,
    users: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserDeleteReq {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Serialize)]
struct AuthUserDeleteResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserChangePasswordReq {
    #[serde(default)]
    name: String,
    #[serde(default)]
    password: String,
}

#[derive(Debug, Serialize)]
struct AuthUserChangePasswordResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserGrantRoleReq {
    #[serde(default)]
    user: String,
    #[serde(default)]
    role: String,
}

#[derive(Debug, Serialize)]
struct AuthUserGrantRoleResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthUserRevokeRoleReq {
    #[serde(default)]
    name: String,
    #[serde(default)]
    role: String,
}

#[derive(Debug, Serialize)]
struct AuthUserRevokeRoleResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleAddReq {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Serialize)]
struct AuthRoleAddResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleGetReq {
    #[serde(default)]
    role: String,
}

#[derive(Debug, Serialize)]
struct AuthRoleGetResp {
    header: JsonResponseHeader,
    perm: Vec<JsonPermission>,
}

#[derive(Debug, Serialize)]
struct JsonPermission {
    #[serde(rename = "permType")]
    perm_type: i32,
    key: String,
    range_end: String,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleListReq {}

#[derive(Debug, Serialize)]
struct AuthRoleListResp {
    header: JsonResponseHeader,
    roles: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleDeleteReq {
    #[serde(default)]
    role: String,
}

#[derive(Debug, Serialize)]
struct AuthRoleDeleteResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleGrantPermissionReq {
    #[serde(default)]
    name: String,
    #[serde(default)]
    perm: Option<JsonPermissionReq>,
}

#[derive(Debug, Deserialize, Default)]
struct JsonPermissionReq {
    #[serde(rename = "permType", default, deserialize_with = "deserialize_perm_type")]
    perm_type: i32,
    #[serde(default)]
    key: String,
    #[serde(default)]
    range_end: String,
}

/// Deserialize perm_type from either a numeric value or a string enum name.
/// etcd's grpc-gateway accepts both: 0/1/2 or "READ"/"WRITE"/"READWRITE".
fn deserialize_perm_type<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<i32, D::Error> {
    use serde::de;

    struct PermTypeVisitor;
    impl<'de> de::Visitor<'de> for PermTypeVisitor {
        type Value = i32;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an integer or permission string (READ, WRITE, READWRITE)")
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<i32, E> { Ok(v as i32) }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<i32, E> { Ok(v as i32) }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<i32, E> {
            match v {
                "READ" => Ok(0),
                "WRITE" => Ok(1),
                "READWRITE" => Ok(2),
                other => other.parse::<i32>().map_err(|_| de::Error::unknown_variant(other, &["READ", "WRITE", "READWRITE"])),
            }
        }
    }
    deserializer.deserialize_any(PermTypeVisitor)
}

#[derive(Debug, Serialize)]
struct AuthRoleGrantPermissionResp {
    header: JsonResponseHeader,
}

#[derive(Debug, Deserialize, Default)]
struct AuthRoleRevokePermissionReq {
    #[serde(default)]
    role: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    range_end: String,
}

#[derive(Debug, Serialize)]
struct AuthRoleRevokePermissionResp {
    header: JsonResponseHeader,
}

// ── Auth Handlers ───────────────────────────────────────────────────────────

async fn handle_auth_enable(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: AuthEnableReq = parse_json(&body);
    state.auth_manager.auth_enable().await;
    axum::Json(AuthEnableResp {
        header: state.make_header(0),
    })
}

async fn handle_auth_disable(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: AuthDisableReq = parse_json(&body);
    state.auth_manager.auth_disable().await;
    axum::Json(AuthDisableResp {
        header: state.make_header(0),
    })
}

async fn handle_auth_status(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: AuthStatusReq = parse_json(&body);
    let enabled = state.auth_manager.is_enabled().await;
    axum::Json(AuthStatusResp {
        header: state.make_header(0),
        enabled,
        auth_revision: 0,
    })
}

async fn handle_auth_authenticate(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthenticateReq = parse_json(&body);
    match state.auth_manager.authenticate(&req.name, &req.password).await {
        Some(token) => axum::Json(AuthenticateResp {
            header: state.make_header(0),
            token,
        })
        .into_response(),
        None => json_error(StatusCode::UNAUTHORIZED, "invalid credentials").into_response(),
    }
}

async fn handle_auth_user_add(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserAddReq = parse_json(&body);
    if req.name.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "user name cannot be empty").into_response();
    }
    let added = state.auth_manager.user_add(req.name, req.password).await;
    if !added {
        return json_error(StatusCode::CONFLICT, "user already exists").into_response();
    }
    axum::Json(AuthUserAddResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_user_get(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserGetReq = parse_json(&body);
    match state.auth_manager.user_get(&req.name).await {
        Some(user) => axum::Json(AuthUserGetResp {
            header: state.make_header(0),
            roles: user.roles,
        })
        .into_response(),
        None => json_error(StatusCode::NOT_FOUND, format!("user {} not found", req.name))
            .into_response(),
    }
}

async fn handle_auth_user_list(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: AuthUserListReq = parse_json(&body);
    let users = state.auth_manager.user_list().await;
    axum::Json(AuthUserListResp {
        header: state.make_header(0),
        users,
    })
}

async fn handle_auth_user_delete(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserDeleteReq = parse_json(&body);
    let deleted = state.auth_manager.user_delete(&req.name).await;
    if !deleted {
        return json_error(StatusCode::NOT_FOUND, format!("user {} not found", req.name))
            .into_response();
    }
    axum::Json(AuthUserDeleteResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_user_change_password(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserChangePasswordReq = parse_json(&body);
    let changed = state.auth_manager.user_change_password(&req.name, req.password).await;
    if !changed {
        return json_error(StatusCode::NOT_FOUND, format!("user {} not found", req.name))
            .into_response();
    }
    axum::Json(AuthUserChangePasswordResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_user_grant_role(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserGrantRoleReq = parse_json(&body);
    let granted = state.auth_manager.user_grant_role(&req.user, &req.role).await;
    if !granted {
        return json_error(
            StatusCode::BAD_REQUEST,
            "user not found or role already granted",
        )
        .into_response();
    }
    axum::Json(AuthUserGrantRoleResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_user_revoke_role(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthUserRevokeRoleReq = parse_json(&body);
    let revoked = state.auth_manager.user_revoke_role(&req.name, &req.role).await;
    if !revoked {
        return json_error(
            StatusCode::BAD_REQUEST,
            "user not found or role not granted",
        )
        .into_response();
    }
    axum::Json(AuthUserRevokeRoleResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_role_add(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthRoleAddReq = parse_json(&body);
    if req.name.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "role name cannot be empty").into_response();
    }
    let added = state.auth_manager.role_add(req.name).await;
    if !added {
        return json_error(StatusCode::CONFLICT, "role already exists").into_response();
    }
    axum::Json(AuthRoleAddResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_role_get(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthRoleGetReq = parse_json(&body);
    match state.auth_manager.role_get(&req.role).await {
        Some(role) => {
            let perm = role
                .permissions
                .into_iter()
                .map(|p| JsonPermission {
                    perm_type: p.perm_type,
                    key: encode_b64(&p.key),
                    range_end: encode_b64(&p.range_end),
                })
                .collect();
            axum::Json(AuthRoleGetResp {
                header: state.make_header(0),
                perm,
            })
            .into_response()
        }
        None => json_error(StatusCode::NOT_FOUND, format!("role {} not found", req.role))
            .into_response(),
    }
}

async fn handle_auth_role_list(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let _: AuthRoleListReq = parse_json(&body);
    let roles = state.auth_manager.role_list().await;
    axum::Json(AuthRoleListResp {
        header: state.make_header(0),
        roles,
    })
}

async fn handle_auth_role_delete(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthRoleDeleteReq = parse_json(&body);
    let deleted = state.auth_manager.role_delete(&req.role).await;
    if !deleted {
        return json_error(StatusCode::NOT_FOUND, format!("role {} not found", req.role))
            .into_response();
    }
    axum::Json(AuthRoleDeleteResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_role_grant_permission(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthRoleGrantPermissionReq = parse_json(&body);
    let perm = match req.perm {
        Some(p) => crate::auth::manager::Permission {
            perm_type: p.perm_type,
            key: decode_b64(&Some(p.key)),
            range_end: decode_b64(&Some(p.range_end)),
        },
        None => {
            return json_error(StatusCode::BAD_REQUEST, "permission is required").into_response()
        }
    };
    let granted = state.auth_manager.role_grant_permission(&req.name, perm).await;
    if !granted {
        return json_error(StatusCode::NOT_FOUND, format!("role {} not found", req.name))
            .into_response();
    }
    axum::Json(AuthRoleGrantPermissionResp {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_auth_role_revoke_permission(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let req: AuthRoleRevokePermissionReq = parse_json(&body);
    let key = decode_b64(&Some(req.key));
    let range_end = decode_b64(&Some(req.range_end));
    let revoked = state
        .auth_manager
        .role_revoke_permission(&req.role, &key, &range_end)
        .await;
    if !revoked {
        return json_error(
            StatusCode::BAD_REQUEST,
            "role not found or permission not granted",
        )
        .into_response();
    }
    axum::Json(AuthRoleRevokePermissionResp {
        header: state.make_header(0),
    })
    .into_response()
}

// ── Watch SSE handler ───────────────────────────────────────────────────────

fn proto_kv_to_json_value(kv: &crate::proto::mvccpb::KeyValue) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert("key".into(), serde_json::Value::String(encode_b64(&kv.key)));
    if kv.create_revision != 0 {
        m.insert(
            "create_revision".into(),
            serde_json::Value::String(kv.create_revision.to_string()),
        );
    }
    if kv.mod_revision != 0 {
        m.insert(
            "mod_revision".into(),
            serde_json::Value::String(kv.mod_revision.to_string()),
        );
    }
    if kv.version != 0 {
        m.insert(
            "version".into(),
            serde_json::Value::String(kv.version.to_string()),
        );
    }
    m.insert(
        "value".into(),
        serde_json::Value::String(encode_b64(&kv.value)),
    );
    if kv.lease != 0 {
        m.insert(
            "lease".into(),
            serde_json::Value::String(kv.lease.to_string()),
        );
    }
    serde_json::Value::Object(m)
}

fn make_header_json(state: &GatewayState, revision: i64) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "cluster_id".into(),
        serde_json::Value::String(state.cluster_id.to_string()),
    );
    m.insert(
        "member_id".into(),
        serde_json::Value::String(state.member_id.to_string()),
    );
    if revision != 0 {
        m.insert(
            "revision".into(),
            serde_json::Value::String(revision.to_string()),
        );
    }
    m.insert(
        "raft_term".into(),
        serde_json::Value::String(state.raft_term.load(Ordering::Relaxed).to_string()),
    );
    serde_json::Value::Object(m)
}

async fn handle_watch_sse(
    State(state): State<GatewayState>,
    body: axum::body::Bytes,
) -> Sse<impl futures::Stream<Item = Result<SseEvent, std::convert::Infallible>>> {
    let req: WatchCreateRequest = parse_json(&body);
    let key = decode_b64(&req.key);
    let range_end = decode_b64(&req.range_end);
    let start_revision = req.start_revision.unwrap_or(0);

    let (watch_id, mut event_rx) = state
        .watch_hub
        .create_watch(key, range_end, start_revision)
        .await;

    let stream = async_stream::stream! {
        // Send initial "created" event.
        let created = serde_json::json!({
            "result": {
                "header": make_header_json(&state, 0),
                "watch_id": watch_id.to_string(),
                "created": true,
                "events": []
            }
        });
        yield Ok::<_, std::convert::Infallible>(
            SseEvent::default().data(created.to_string()),
        );

        // Forward watch events as they arrive.
        while let Some(watch_event) = event_rx.recv().await {
            let events: Vec<serde_json::Value> = watch_event
                .events
                .iter()
                .map(|e| {
                    let mut ev = serde_json::json!({
                        "type": if e.r#type == 1 { "DELETE" } else { "PUT" },
                    });
                    if let Some(ref kv) = e.kv {
                        ev["kv"] = proto_kv_to_json_value(kv);
                    }
                    if let Some(ref prev) = e.prev_kv {
                        ev["prev_kv"] = proto_kv_to_json_value(prev);
                    }
                    ev
                })
                .collect();

            let resp = serde_json::json!({
                "result": {
                    "header": make_header_json(&state, 0),
                    "watch_id": watch_event.watch_id.to_string(),
                    "events": events
                }
            });
            yield Ok::<_, std::convert::Infallible>(
                SseEvent::default().data(resp.to_string()),
            );
        }
    };

    Sse::new(stream)
}
