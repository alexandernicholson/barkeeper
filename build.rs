fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile etcd-compatible protos (server-only, no client needed).
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(
            &[
                "proto/etcdserverpb/rpc.proto",
                "proto/etcdserverpb/kv.proto",
                "proto/authpb/auth.proto",
            ],
            &["proto/"],
        )?;

    // Compile raft transport proto (both server and client needed for
    // inter-node communication).
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/raftpb/raft_transport.proto"], &["proto/"])?;

    Ok(())
}
