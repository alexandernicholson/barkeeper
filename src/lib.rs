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
