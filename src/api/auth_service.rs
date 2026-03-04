use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::auth::manager::{AuthManager, Permission};
use crate::proto::etcdserverpb::auth_server::Auth;
use crate::proto::etcdserverpb::{
    AuthDisableRequest, AuthDisableResponse, AuthEnableRequest, AuthEnableResponse,
    AuthRoleAddRequest, AuthRoleAddResponse, AuthRoleDeleteRequest, AuthRoleDeleteResponse,
    AuthRoleGetRequest, AuthRoleGetResponse, AuthRoleGrantPermissionRequest,
    AuthRoleGrantPermissionResponse, AuthRoleListRequest, AuthRoleListResponse,
    AuthRoleRevokePermissionRequest, AuthRoleRevokePermissionResponse, AuthStatusRequest,
    AuthStatusResponse, AuthUserAddRequest, AuthUserAddResponse, AuthUserChangePasswordRequest,
    AuthUserChangePasswordResponse, AuthUserDeleteRequest, AuthUserDeleteResponse,
    AuthUserGetRequest, AuthUserGetResponse, AuthUserGrantRoleRequest, AuthUserGrantRoleResponse,
    AuthUserListRequest, AuthUserListResponse, AuthUserRevokeRoleRequest,
    AuthUserRevokeRoleResponse, AuthenticateRequest, AuthenticateResponse, ResponseHeader,
};

/// gRPC Auth service implementing the etcd Auth API.
pub struct AuthService {
    manager: Arc<AuthManager>,
    cluster_id: u64,
    member_id: u64,
    raft_term: Arc<AtomicU64>,
}

impl AuthService {
    pub fn new(manager: Arc<AuthManager>, cluster_id: u64, member_id: u64, raft_term: Arc<AtomicU64>) -> Self {
        AuthService {
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
}

#[tonic::async_trait]
impl Auth for AuthService {
    async fn auth_enable(
        &self,
        _request: Request<AuthEnableRequest>,
    ) -> Result<Response<AuthEnableResponse>, Status> {
        self.manager.auth_enable().await;
        Ok(Response::new(AuthEnableResponse {
            header: self.make_header(),
        }))
    }

    async fn auth_disable(
        &self,
        _request: Request<AuthDisableRequest>,
    ) -> Result<Response<AuthDisableResponse>, Status> {
        self.manager.auth_disable().await;
        Ok(Response::new(AuthDisableResponse {
            header: self.make_header(),
        }))
    }

    async fn auth_status(
        &self,
        _request: Request<AuthStatusRequest>,
    ) -> Result<Response<AuthStatusResponse>, Status> {
        let enabled = self.manager.is_enabled().await;
        Ok(Response::new(AuthStatusResponse {
            header: self.make_header(),
            enabled,
            auth_revision: 0,
        }))
    }

    async fn authenticate(
        &self,
        request: Request<AuthenticateRequest>,
    ) -> Result<Response<AuthenticateResponse>, Status> {
        let req = request.into_inner();
        match self.manager.authenticate(&req.name, &req.password).await {
            Some(token) => Ok(Response::new(AuthenticateResponse {
                header: self.make_header(),
                token,
            })),
            None => Err(Status::unauthenticated("invalid credentials")),
        }
    }

    async fn user_add(
        &self,
        request: Request<AuthUserAddRequest>,
    ) -> Result<Response<AuthUserAddResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("user name cannot be empty"));
        }
        let added = self.manager.user_add(req.name, req.password).await;
        if !added {
            return Err(Status::already_exists("user already exists"));
        }
        Ok(Response::new(AuthUserAddResponse {
            header: self.make_header(),
        }))
    }

    async fn user_get(
        &self,
        request: Request<AuthUserGetRequest>,
    ) -> Result<Response<AuthUserGetResponse>, Status> {
        let req = request.into_inner();
        match self.manager.user_get(&req.name).await {
            Some(user) => Ok(Response::new(AuthUserGetResponse {
                header: self.make_header(),
                roles: user.roles,
            })),
            None => Err(Status::not_found(format!("user {} not found", req.name))),
        }
    }

    async fn user_list(
        &self,
        _request: Request<AuthUserListRequest>,
    ) -> Result<Response<AuthUserListResponse>, Status> {
        let users = self.manager.user_list().await;
        Ok(Response::new(AuthUserListResponse {
            header: self.make_header(),
            users,
        }))
    }

    async fn user_delete(
        &self,
        request: Request<AuthUserDeleteRequest>,
    ) -> Result<Response<AuthUserDeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self.manager.user_delete(&req.name).await;
        if !deleted {
            return Err(Status::not_found(format!("user {} not found", req.name)));
        }
        Ok(Response::new(AuthUserDeleteResponse {
            header: self.make_header(),
        }))
    }

    async fn user_change_password(
        &self,
        request: Request<AuthUserChangePasswordRequest>,
    ) -> Result<Response<AuthUserChangePasswordResponse>, Status> {
        let req = request.into_inner();
        let changed = self.manager.user_change_password(&req.name, req.password).await;
        if !changed {
            return Err(Status::not_found(format!("user {} not found", req.name)));
        }
        Ok(Response::new(AuthUserChangePasswordResponse {
            header: self.make_header(),
        }))
    }

    async fn user_grant_role(
        &self,
        request: Request<AuthUserGrantRoleRequest>,
    ) -> Result<Response<AuthUserGrantRoleResponse>, Status> {
        let req = request.into_inner();
        let granted = self.manager.user_grant_role(&req.user, &req.role).await;
        if !granted {
            return Err(Status::failed_precondition(
                "user not found or role already granted",
            ));
        }
        Ok(Response::new(AuthUserGrantRoleResponse {
            header: self.make_header(),
        }))
    }

    async fn user_revoke_role(
        &self,
        request: Request<AuthUserRevokeRoleRequest>,
    ) -> Result<Response<AuthUserRevokeRoleResponse>, Status> {
        let req = request.into_inner();
        let revoked = self.manager.user_revoke_role(&req.name, &req.role).await;
        if !revoked {
            return Err(Status::failed_precondition(
                "user not found or role not granted",
            ));
        }
        Ok(Response::new(AuthUserRevokeRoleResponse {
            header: self.make_header(),
        }))
    }

    async fn role_add(
        &self,
        request: Request<AuthRoleAddRequest>,
    ) -> Result<Response<AuthRoleAddResponse>, Status> {
        let req = request.into_inner();
        if req.name.is_empty() {
            return Err(Status::invalid_argument("role name cannot be empty"));
        }
        let added = self.manager.role_add(req.name).await;
        if !added {
            return Err(Status::already_exists("role already exists"));
        }
        Ok(Response::new(AuthRoleAddResponse {
            header: self.make_header(),
        }))
    }

    async fn role_get(
        &self,
        request: Request<AuthRoleGetRequest>,
    ) -> Result<Response<AuthRoleGetResponse>, Status> {
        let req = request.into_inner();
        match self.manager.role_get(&req.role).await {
            Some(role) => {
                let perm = role
                    .permissions
                    .into_iter()
                    .map(|p| crate::proto::authpb::Permission {
                        perm_type: p.perm_type,
                        key: p.key,
                        range_end: p.range_end,
                    })
                    .collect();
                Ok(Response::new(AuthRoleGetResponse {
                    header: self.make_header(),
                    perm,
                }))
            }
            None => Err(Status::not_found(format!("role {} not found", req.role))),
        }
    }

    async fn role_list(
        &self,
        _request: Request<AuthRoleListRequest>,
    ) -> Result<Response<AuthRoleListResponse>, Status> {
        let roles = self.manager.role_list().await;
        Ok(Response::new(AuthRoleListResponse {
            header: self.make_header(),
            roles,
        }))
    }

    async fn role_delete(
        &self,
        request: Request<AuthRoleDeleteRequest>,
    ) -> Result<Response<AuthRoleDeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self.manager.role_delete(&req.role).await;
        if !deleted {
            return Err(Status::not_found(format!("role {} not found", req.role)));
        }
        Ok(Response::new(AuthRoleDeleteResponse {
            header: self.make_header(),
        }))
    }

    async fn role_grant_permission(
        &self,
        request: Request<AuthRoleGrantPermissionRequest>,
    ) -> Result<Response<AuthRoleGrantPermissionResponse>, Status> {
        let req = request.into_inner();
        let proto_perm = req
            .perm
            .ok_or_else(|| Status::invalid_argument("permission is required"))?;
        let perm = Permission {
            perm_type: proto_perm.perm_type,
            key: proto_perm.key,
            range_end: proto_perm.range_end,
        };
        let granted = self.manager.role_grant_permission(&req.name, perm).await;
        if !granted {
            return Err(Status::not_found(format!("role {} not found", req.name)));
        }
        Ok(Response::new(AuthRoleGrantPermissionResponse {
            header: self.make_header(),
        }))
    }

    async fn role_revoke_permission(
        &self,
        request: Request<AuthRoleRevokePermissionRequest>,
    ) -> Result<Response<AuthRoleRevokePermissionResponse>, Status> {
        let req = request.into_inner();
        let revoked = self
            .manager
            .role_revoke_permission(&req.role, &req.key, &req.range_end)
            .await;
        if !revoked {
            return Err(Status::failed_precondition(
                "role not found or permission not granted",
            ));
        }
        Ok(Response::new(AuthRoleRevokePermissionResponse {
            header: self.make_header(),
        }))
    }
}
