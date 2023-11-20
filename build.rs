fn main() -> Result<(), Box<dyn std::error::Error>> {
    //tonic_build::compile_protos("proto/message.proto")?;
    tonic_build::configure()
        .build_server(true)
        .compile(
            &[
                "proto/message.proto",
                // "proto/storage.proto",
                // "proto/consensus.proto",
            ],
            &["proto"],
        )
        .unwrap();
    Ok(())
}
