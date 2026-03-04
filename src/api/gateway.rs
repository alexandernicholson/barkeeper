//! HTTP/JSON gateway that maps JSON POST requests to the KV store, Lease
//! manager, and Cluster manager -- matching etcd's grpc-gateway endpoints.
//!
//! Byte fields (key, value) are base64-encoded in JSON, mirroring etcd's
//! HTTP gateway behaviour.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize};

use crate::cluster::manager::ClusterManager;
use crate::kv::state_machine::KvCommand;
use crate::kv::store::KvStore;
use crate::lease::manager::LeaseManager;
use crate::raft::messages::ClientProposalResult;
use crate::raft::node::RaftHandle;

// ── Shared state ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct GatewayState {
    pub raft: RaftHandle,
    pub store: Arc<KvStore>,
    pub lease_manager: Arc<LeaseManager>,
    pub cluster_manager: Arc<ClusterManager>,
    pub cluster_id: u64,
    pub member_id: u64,
}

// ── JSON request / response types ───────────────────────────────────────────

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
    kvs: Vec<JsonKeyValue>,
    more: bool,
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
    deleted: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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
    #[serde(rename = "ID")]
    id: i64,
    #[serde(rename = "TTL")]
    ttl: i64,
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
    #[serde(rename = "ID")]
    id: i64,
    #[serde(rename = "TTL")]
    ttl: i64,
    #[serde(rename = "grantedTTL")]
    granted_ttl: i64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
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
    #[serde(rename = "ID")]
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
    #[serde(rename = "ID")]
    id: u64,
    name: String,
    #[serde(rename = "peerURLs")]
    peer_urls: Vec<String>,
    #[serde(rename = "clientURLs")]
    client_urls: Vec<String>,
    #[serde(rename = "isLearner")]
    is_learner: bool,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    header: JsonResponseHeader,
    version: String,
    #[serde(rename = "dbSize")]
    db_size: i64,
    leader: u64,
    #[serde(rename = "raftIndex")]
    raft_index: u64,
    #[serde(rename = "raftTerm")]
    raft_term: u64,
}

// Shared types

#[derive(Debug, Serialize, Clone)]
struct JsonResponseHeader {
    cluster_id: u64,
    member_id: u64,
    revision: i64,
    raft_term: u64,
}

#[derive(Debug, Serialize, Clone)]
struct JsonKeyValue {
    key: String,
    create_revision: i64,
    mod_revision: i64,
    version: i64,
    value: String,
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
        Json(ErrorBody {
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

// ── Router constructor ──────────────────────────────────────────────────────

pub fn create_router(
    raft: RaftHandle,
    store: Arc<KvStore>,
    lease_manager: Arc<LeaseManager>,
    cluster_manager: Arc<ClusterManager>,
    cluster_id: u64,
    member_id: u64,
) -> Router {
    let state = GatewayState {
        raft,
        store,
        lease_manager,
        cluster_manager,
        cluster_id,
        member_id,
    };

    Router::new()
        .route("/v3/kv/range", post(handle_range))
        .route("/v3/kv/put", post(handle_put))
        .route("/v3/kv/deleterange", post(handle_delete_range))
        .route("/v3/kv/txn", post(handle_txn_stub))
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

async fn handle_range(
    State(state): State<GatewayState>,
    Json(req): Json<RangeRequest>,
) -> impl IntoResponse {
    let key = decode_b64(&req.key);
    let range_end = decode_b64(&req.range_end);
    let limit = req.limit.unwrap_or(0);
    let revision = req.revision.unwrap_or(0);

    match state.store.range(&key, &range_end, limit, revision) {
        Ok(result) => {
            let rev = state.store.current_revision().unwrap_or(0);
            let kvs: Vec<JsonKeyValue> = result.kvs.iter().map(proto_kv_to_json).collect();
            Json(RangeResponse {
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
    Json(req): Json<PutRequest>,
) -> impl IntoResponse {
    let key = decode_b64(&req.key);
    let value = decode_b64(&req.value);
    let lease_id = req.lease.unwrap_or(0);

    let cmd = KvCommand::Put {
        key: key.clone(),
        value: value.clone(),
        lease_id,
    };

    let data = match serde_json::to_vec(&cmd) {
        Ok(d) => d,
        Err(e) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("serialize: {}", e),
            )
            .into_response()
        }
    };

    match state.raft.propose(data).await {
        Ok(ClientProposalResult::Success { revision, .. }) => {
            let prev_kv = if req.prev_kv.unwrap_or(false) && revision > 1 {
                state
                    .store
                    .range(&key, b"", 0, revision - 1)
                    .ok()
                    .and_then(|r| r.kvs.into_iter().next())
                    .map(|kv| proto_kv_to_json(&kv))
            } else {
                None
            };

            Json(PutResponse {
                header: state.make_header(revision),
                prev_kv,
            })
            .into_response()
        }
        Ok(ClientProposalResult::NotLeader { leader_id }) => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not leader, leader is {:?}", leader_id),
        )
        .into_response(),
        Ok(ClientProposalResult::Error(e)) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("propose: {}", e),
        )
        .into_response(),
    }
}

async fn handle_delete_range(
    State(state): State<GatewayState>,
    Json(req): Json<DeleteRangeRequest>,
) -> impl IntoResponse {
    let key = decode_b64(&req.key);
    let range_end = decode_b64(&req.range_end);

    let cmd = KvCommand::DeleteRange {
        key: key.clone(),
        range_end: range_end.clone(),
    };

    let data = match serde_json::to_vec(&cmd) {
        Ok(d) => d,
        Err(e) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("serialize: {}", e),
            )
            .into_response()
        }
    };

    match state.raft.propose(data).await {
        Ok(ClientProposalResult::Success { revision, .. }) => {
            let rev = state.store.current_revision().unwrap_or(revision);

            Json(DeleteRangeResponse {
                header: state.make_header(revision),
                deleted: if rev == revision { 1 } else { 0 },
                prev_kvs: vec![],
            })
            .into_response()
        }
        Ok(ClientProposalResult::NotLeader { leader_id }) => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not leader, leader is {:?}", leader_id),
        )
        .into_response(),
        Ok(ClientProposalResult::Error(e)) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response()
        }
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("propose: {}", e),
        )
        .into_response(),
    }
}

async fn handle_txn_stub() -> impl IntoResponse {
    json_error(StatusCode::NOT_IMPLEMENTED, "txn not yet implemented")
}

async fn handle_compaction_stub() -> impl IntoResponse {
    json_error(
        StatusCode::NOT_IMPLEMENTED,
        "compaction not yet implemented",
    )
}

async fn handle_lease_grant(
    State(state): State<GatewayState>,
    Json(req): Json<LeaseGrantRequest>,
) -> impl IntoResponse {
    let ttl = req.ttl.unwrap_or(0);
    let id = req.id.unwrap_or(0);

    let lease_id = state.lease_manager.grant(id, ttl).await;

    Json(LeaseGrantResponse {
        header: state.make_header(0),
        id: lease_id,
        ttl,
        error: String::new(),
    })
}

async fn handle_lease_revoke(
    State(state): State<GatewayState>,
    Json(req): Json<LeaseRevokeRequest>,
) -> impl IntoResponse {
    let id = req.id.unwrap_or(0);
    let existed = state.lease_manager.revoke(id).await;
    if !existed {
        return json_error(
            StatusCode::NOT_FOUND,
            format!("lease {} not found", id),
        )
        .into_response();
    }

    Json(LeaseRevokeResponse {
        header: state.make_header(0),
    })
    .into_response()
}

async fn handle_lease_timetolive(
    State(state): State<GatewayState>,
    Json(req): Json<LeaseTimeToLiveRequest>,
) -> impl IntoResponse {
    let id = req.id.unwrap_or(0);
    let want_keys = req.keys.unwrap_or(false);

    match state.lease_manager.time_to_live(id).await {
        Some((granted_ttl, remaining, key_bytes)) => {
            let keys = if want_keys {
                key_bytes.iter().map(|k| encode_b64(k)).collect()
            } else {
                vec![]
            };

            Json(LeaseTimeToLiveResponse {
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
    Json(_req): Json<LeaseLeasesRequest>,
) -> impl IntoResponse {
    let ids = state.lease_manager.list().await;
    let leases = ids.into_iter().map(|id| JsonLeaseStatus { id }).collect();

    Json(LeaseLeasesResponse {
        header: state.make_header(0),
        leases,
    })
}

async fn handle_cluster_member_list(
    State(state): State<GatewayState>,
    Json(_req): Json<MemberListRequest>,
) -> impl IntoResponse {
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

    Json(MemberListResponse {
        header: state.make_header(0),
        members: json_members,
    })
}

async fn handle_maintenance_status(State(state): State<GatewayState>) -> impl IntoResponse {
    Json(StatusResponse {
        header: state.make_header(0),
        version: env!("CARGO_PKG_VERSION").to_string(),
        db_size: 0,
        leader: state.member_id,
        raft_index: 0,
        raft_term: 0,
    })
}
