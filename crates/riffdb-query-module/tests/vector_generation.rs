#![forbid(unsafe_code)]

//! Generators over a vector-bearing contract (M4 fix round).
//!
//! Before this round the Rust/Go/TS/Python generators panicked on
//! contract-declared input: `vector_field` emits a vector-typed field into
//! the entity record, and the record recursion hit
//! `ValueTypeTag::Vector => unreachable!()`. These tests red on any
//! reintroduced panic and pin the chosen behavior: vector fields are
//! excluded from generated client models until the wire protocol carries a
//! distinct vector variant.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    generate_go_application_client, generate_python_application_client,
    generate_rust_application_client, generate_typescript_application_client,
};

const CONTRACT: &str = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(128, cosine, (title, body), staleness_slo 60)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

const QUERY: &str = r#"
query GetDocument(
    $org_id: Document.org_id,
    $doc_id: Document.doc_id,
) {
    one document from Document
        where org_id == $org_id
            && doc_id == $doc_id
        else NotFound

    return Found {
        document: document { title body }
    }

    outcomes Found | NotFound
}
"#;

fn application() -> (riffdb_contract_ir::ContractBundle, QueryModule) {
    let contract = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("docs_vectors").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("GetDocument", QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("query module");
    (contract, module)
}

/// Every generator must produce an artifact for a vector-bearing contract
/// without panicking, and the non-vector fields must survive.
#[test]
fn generators_do_not_panic_on_a_vector_bearing_entity() {
    let (contract, module) = application();

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("pub struct Document"));
    assert!(rust.contains("title"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("type Document struct"));
    assert!(go.contains("Title"));

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains("GetDocument"));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("title"));
}

/// The chosen fail-closed shape: vector fields do not appear in generated
/// entity models (no wire variant exists to carry them), and no generator
/// emits a punned or panicking accessor for them.
#[test]
fn vector_fields_are_excluded_from_generated_entity_models() {
    let (contract, module) = application();

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(
        !rust.contains("embedding"),
        "generated Rust client must not model the vector field"
    );

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(
        !go.contains("Embedding"),
        "generated Go client must not model the vector field"
    );
    assert!(
        !go.contains("riffdbUnsupportedVectorField"),
        "the undefined-identifier guard must never reach an emitted artifact"
    );

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(
        !typescript.contains("embedding"),
        "generated TypeScript client must not model the vector field"
    );

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(
        !python.contains("embedding"),
        "generated Python client must not model the vector field"
    );
}
