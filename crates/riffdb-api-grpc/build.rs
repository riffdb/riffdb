#![forbid(unsafe_code)]

//! Generates only the Tonic service boundary from the checked descriptor set.

use std::error::Error;
use std::path::PathBuf;

use prost::Message;
use prost_types::FileDescriptorSet;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR")
            .ok_or("CARGO_MANIFEST_DIR is unavailable while generating gRPC services")?,
    );
    let descriptor_path =
        manifest_dir.join("../../fixtures/proto/descriptors/riffdb-v1-descriptor-set.bin");
    println!("cargo:rerun-if-changed={}", descriptor_path.display());

    let descriptor_bytes = std::fs::read(descriptor_path)?;
    let descriptors = FileDescriptorSet::decode(descriptor_bytes.as_slice())?;
    let build_client = std::env::var_os("CARGO_FEATURE_CLIENT").is_some();
    let build_server = std::env::var_os("CARGO_FEATURE_SERVER").is_some();
    if !build_client && !build_server {
        let output = PathBuf::from(
            std::env::var_os("OUT_DIR")
                .ok_or("OUT_DIR is unavailable while generating gRPC services")?,
        );
        std::fs::write(output.join("riffdb.v1.rs"), [])?;
        std::fs::write(output.join("riffdb.app.v1.rs"), [])?;
        return Ok(());
    }
    tonic_prost_build::configure()
        .build_client(build_client)
        .build_server(build_server)
        .build_transport(build_client)
        .extern_path(".riffdb.v1", "::riffdb_proto::v1")
        .extern_path(".riffdb.app.v1", "::riffdb_proto::app::v1")
        .codec_path("crate::codec::StrictProstCodec")
        .compile_fds(descriptors)?;
    Ok(())
}
