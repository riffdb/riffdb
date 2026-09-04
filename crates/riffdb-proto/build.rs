#![forbid(unsafe_code)]

//! Generates client-only Tonic service bindings from the frozen descriptors.

use std::error::Error;
use std::path::PathBuf;

use prost::Message;
use prost_types::FileDescriptorSet;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest = PathBuf::from(
        std::env::var_os("CARGO_MANIFEST_DIR").ok_or("CARGO_MANIFEST_DIR is unavailable")?,
    );
    let descriptors = FileDescriptorSet::decode(
        std::fs::read(manifest.join("fixtures/descriptors/riffdb-v1-descriptor-set.bin"))?
            .as_slice(),
    )?;
    let output = PathBuf::from(
        std::env::var_os("OUT_DIR").ok_or("OUT_DIR is unavailable while generating clients")?,
    );
    if std::env::var_os("CARGO_FEATURE_CLIENT").is_none() {
        std::fs::write(output.join("riffdb.v1.rs"), [])?;
        std::fs::write(output.join("riffdb.app.v1.rs"), [])?;
        return Ok(());
    }
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .build_transport(true)
        .extern_path(".riffdb.v1", "::riffdb_proto::v1")
        .extern_path(".riffdb.app.v1", "::riffdb_proto::app::v1")
        .codec_path("crate::client_codec::StrictProstCodec")
        .compile_fds(descriptors)?;
    Ok(())
}
