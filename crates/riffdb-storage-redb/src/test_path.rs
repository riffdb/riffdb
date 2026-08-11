#![forbid(unsafe_code)]

use std::fs;
use std::path::PathBuf;

pub(crate) fn root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/riffdb-test-data/storage-redb-unit");
    fs::create_dir_all(&root).expect("create repository-local redb test root");
    fs::canonicalize(root).expect("canonicalize repository-local redb test root")
}
