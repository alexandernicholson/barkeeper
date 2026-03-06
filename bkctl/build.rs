fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Generate gRPC client stubs from the shared proto definitions.
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                "../barkeeper/proto/etcdserverpb/rpc.proto",
                "../barkeeper/proto/etcdserverpb/kv.proto",
                "../barkeeper/proto/authpb/auth.proto",
            ],
            &["../barkeeper/proto/"],
        )?;
    Ok(())
}
