//! Tower layer that enforces authentication on gRPC requests.
//!
//! When auth is enabled, all gRPC requests except those to the Auth service
//! must include a valid token in the `authorization` metadata field.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use http::{HeaderValue, Request, Response};
use tonic::body::BoxBody;
use tower::{Layer, Service};

use super::manager::AuthManager;

/// Tower layer that wraps services with auth enforcement.
#[derive(Clone)]
pub struct GrpcAuthLayer {
    auth_manager: Arc<AuthManager>,
}

impl GrpcAuthLayer {
    pub fn new(auth_manager: Arc<AuthManager>) -> Self {
        GrpcAuthLayer { auth_manager }
    }
}

impl<S> Layer<S> for GrpcAuthLayer {
    type Service = GrpcAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcAuthService {
            inner,
            auth_manager: Arc::clone(&self.auth_manager),
        }
    }
}

/// Tower service that checks auth before forwarding to the inner service.
#[derive(Clone)]
pub struct GrpcAuthService<S> {
    inner: S,
    auth_manager: Arc<AuthManager>,
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

impl<S, ReqBody> Service<Request<ReqBody>> for GrpcAuthService<S>
where
    S: Service<Request<ReqBody>, Response = Response<BoxBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        // Swap the clone so we keep the ready state.
        std::mem::swap(&mut self.inner, &mut inner);

        let auth_manager = Arc::clone(&self.auth_manager);

        Box::pin(async move {
            let path = req.uri().path().to_string();

            // Auth service endpoints are always accessible (users need to be
            // able to authenticate, enable/disable auth, etc.)
            if path.contains("/etcdserverpb.Auth/") {
                return inner.call(req).await;
            }

            // If auth is not enabled, pass through.
            if !auth_manager.is_enabled().await {
                return inner.call(req).await;
            }

            // Auth is enabled -- check for a valid token in the `authorization`
            // metadata (HTTP/2 header).
            let token: Option<String> = req
                .headers()
                .get("authorization")
                .and_then(|v: &HeaderValue| v.to_str().ok())
                .map(|s: &str| s.to_string());

            match token {
                Some(ref token_str) => {
                    if auth_manager.validate_token(token_str).await.is_some() {
                        inner.call(req).await
                    } else {
                        let response = tonic::Status::unauthenticated("invalid auth token");
                        Ok(response.into_http())
                    }
                }
                None => {
                    let response =
                        tonic::Status::unauthenticated("auth token is not provided");
                    Ok(response.into_http())
                }
            }
        })
    }
}
