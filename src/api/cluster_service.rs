use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::cluster::manager::ClusterManager;
use crate::proto::etcdserverpb::cluster_server::Cluster;
use crate::proto::etcdserverpb::{
    Member as ProtoMember, MemberAddRequest, MemberAddResponse, MemberListRequest,
    MemberListResponse, MemberPromoteRequest, MemberPromoteResponse, MemberRemoveRequest,
    MemberRemoveResponse, MemberUpdateRequest, MemberUpdateResponse, ResponseHeader,
};

/// gRPC Cluster service implementing the etcd Cluster API.
pub struct ClusterService {
    manager: Arc<ClusterManager>,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}

impl ClusterService {
    pub fn new(manager: Arc<ClusterManager>, cluster_id: u64, member_id: u64, raft_term: Arc<AtomicU64>) -> Self {
        ClusterService {
            manager,
            cluster_id,
            member_id,
            raft_term,
        }
    }

    fn make_header(&self) -> Option<ResponseHeader> {
        Some(ResponseHeader {
            cluster_id: self.cluster_id,
            member_id: self.member_id,
            revision: 0,
            raft_term: self.raft_term.load(Ordering::Relaxed),
        })
    }

    /// Convert an internal Member to the proto Member.
    fn to_proto_member(m: &crate::cluster::manager::Member) -> ProtoMember {
        ProtoMember {
            id: m.id,
            name: m.name.clone(),
            peer_ur_ls: m.peer_urls.clone(),
            client_ur_ls: m.client_urls.clone(),
            is_learner: m.is_learner,
        }
    }

    async fn proto_member_list(&self) -> Vec<ProtoMember> {
        self.manager
            .member_list()
            .await
            .iter()
            .map(Self::to_proto_member)
            .collect()
    }
}

#[tonic::async_trait]
impl Cluster for ClusterService {
    async fn member_add(
        &self,
        request: Request<MemberAddRequest>,
    ) -> Result<Response<MemberAddResponse>, Status> {
        let req = request.into_inner();

        let member = self
            .manager
            .member_add(req.peer_ur_ls, req.is_learner)
            .await;

        let proto_member = Self::to_proto_member(&member);
        let members = self.proto_member_list().await;

        Ok(Response::new(MemberAddResponse {
            header: self.make_header(),
            member: Some(proto_member),
            members,
        }))
    }

    async fn member_remove(
        &self,
        request: Request<MemberRemoveRequest>,
    ) -> Result<Response<MemberRemoveResponse>, Status> {
        let req = request.into_inner();

        let existed = self.manager.member_remove(req.id).await;
        if !existed {
            return Err(Status::not_found(format!("member {} not found", req.id)));
        }

        let members = self.proto_member_list().await;

        Ok(Response::new(MemberRemoveResponse {
            header: self.make_header(),
            members,
        }))
    }

    async fn member_update(
        &self,
        request: Request<MemberUpdateRequest>,
    ) -> Result<Response<MemberUpdateResponse>, Status> {
        let req = request.into_inner();

        let existed = self.manager.member_update(req.id, req.peer_ur_ls).await;
        if !existed {
            return Err(Status::not_found(format!("member {} not found", req.id)));
        }

        let members = self.proto_member_list().await;

        Ok(Response::new(MemberUpdateResponse {
            header: self.make_header(),
            members,
        }))
    }

    async fn member_list(
        &self,
        _request: Request<MemberListRequest>,
    ) -> Result<Response<MemberListResponse>, Status> {
        let members = self.proto_member_list().await;

        Ok(Response::new(MemberListResponse {
            header: self.make_header(),
            members,
        }))
    }

    async fn member_promote(
        &self,
        request: Request<MemberPromoteRequest>,
    ) -> Result<Response<MemberPromoteResponse>, Status> {
        let req = request.into_inner();

        let promoted = self.manager.member_promote(req.id).await;
        if !promoted {
            return Err(Status::failed_precondition(format!(
                "member {} not found or not a learner",
                req.id
            )));
        }

        let members = self.proto_member_list().await;

        Ok(Response::new(MemberPromoteResponse {
            header: self.make_header(),
            members,
        }))
    }
}
