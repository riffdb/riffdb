#![forbid(unsafe_code)]
//! Regenerate or check the value-free ADR-0186 inventory from its sole owner.

use std::{env, fs, path::Path};

use riffdb_storage_api::AuthoritativeStateCatalogV1;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments != ["--check"] && arguments != ["--write"] {
        return Err("usage: generate_authoritative_catalog --check|--write".into());
    }
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/replication/authoritative-state-catalog-v1.txt");
    let expected = AuthoritativeStateCatalogV1.canonical_fixture();
    if arguments == ["--write"] {
        fs::create_dir_all(path.parent().ok_or("fixture parent missing")?)?;
        fs::write(path, expected)?;
    } else if fs::read_to_string(path)? != expected {
        return Err("authoritative state catalog fixture differs from its owner".into());
    }
    Ok(())
}
