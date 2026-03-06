pub mod raft;
pub mod kv;
pub mod watch;
pub mod lease;
pub mod cluster;
pub mod auth;
pub mod api;
pub mod config;
pub mod actors;
pub mod tls;

/// Return an etcd-compatible version string for kube-apiserver compatibility.
/// kube-apiserver uses this to construct the storage-versions capability map.
pub fn etcd_version() -> &'static str {
    "3.5.0"
}

pub mod proto {
    pub mod mvccpb {
        tonic::include_proto!("mvccpb");
    }
    pub mod etcdserverpb {
        tonic::include_proto!("etcdserverpb");
    }
    pub mod authpb {
        tonic::include_proto!("authpb");
    }
}
