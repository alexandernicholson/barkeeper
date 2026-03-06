pub mod client;
pub mod app;
pub mod ui;
pub mod event;

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
