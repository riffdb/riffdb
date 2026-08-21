#![forbid(unsafe_code)]

//! Regenerates the fixed WP-596 remote vector-exit Rust client fixture.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_rust_application_client,
};

const CONTRACT: &str = include_str!("../../../fixtures/vector-exit/documents.riff");
const QUERY: &str = include_str!("../../../fixtures/vector-exit/similar_documents.riffq");

fn main() {
    let output = env::args_os().nth(1).map_or_else(
        || {
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../fixtures/vector-exit/generated-client.rs")
        },
        PathBuf::from,
    );
    let contract = compile_contract_source(CONTRACT).expect("compile vector exit contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("vector_documents").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("SimilarDocuments", QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("compile vector query module");
    let generated = generate_rust_application_client(&module, &contract, &[]);
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent).expect("create vector fixture directory");
    }
    fs::write(output, generated).expect("write vector exit client");
}
