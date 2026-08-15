#![forbid(unsafe_code)]

//! Generates only the Tonic service boundary from the checked descriptor set.

use std::error::Error;
use std::path::PathBuf;

use prost::Message;
use prost_types::FileDescriptorSet;

fn main() -> Result<(), Box<dyn Error>> {
    let descriptors = FileDescriptorSet::decode(riffdb_proto::PRODUCTION_FILE_DESCRIPTOR_SET)?;
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
