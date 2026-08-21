#![forbid(unsafe_code)]

//! Generated application facades over ADR-0136's production embedding write.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_PROJECTED_VECTOR_V1, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, generate_go_application_client,
    generate_python_application_client, generate_rust_application_client,
    generate_typescript_application_client,
};

const CONTRACT: &str = r#"
contract Docs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<256>
    field body: string<65536>
    vector_field embedding(4, cosine, (title, body), staleness_slo 60,
        model "embed-v1", current_version "2026-08-21",
        replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
  command SetDocumentEmbedding {
    input request_id: string<128>
    input org_id: uuid
    input doc_id: uuid
    input embedding: vector<4>
    input submitted_model: string<256>
    input submitted_version: string<256>
    idempotency_key request_id
    mutate Document(org_id, doc_id) as document else Missing {}
    embed document.embedding = embedding from (submitted_model, submitted_version)
    return Embedded { document: document }
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

const NEAREST_QUERY: &str = r#"
query SimilarDocuments(
    $org_id: Document.org_id,
    $query_vector: Document.embedding,
    $k: Limit,
) {
    source projected Document.embedding
    freshness causal inherit_session_commit true max_wait_ms 500

    many documents from Document
        where org_id == $org_id
        nearest(embedding, $query_vector, $k)
    return Found { documents: documents { title } }
    outcomes Found
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

#[test]
fn projected_nearest_module_round_trips_with_its_successor_identity() {
    let contract = compile_contract_source(CONTRACT).expect("vector contract compiles");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("docs_nearest").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("SimilarDocuments", NEAREST_QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("projected module compiles");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_PROJECTED_VECTOR_V1
    );
    let query = module.query("SimilarDocuments").expect("named query");
    let source = query
        .plan()
        .representative_program()
        .projected_source()
        .expect("projected source");
    assert_eq!(source.name(), "Document.embedding");
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("successor module decodes");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
}

/// Every generator emits a typed canonical vector rather than omitting or
/// punning the production field.
#[test]
fn generators_emit_typed_vector_entity_and_command_models() {
    let (contract, module) = application();

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("pub struct Document"));
    assert!(rust.contains("pub embedding: CanonicalVector"));
    assert!(rust.contains("pub struct SetDocumentEmbeddingInput"));
    assert!(rust.contains("wire_vector(&self.embedding, 4)?"));
    assert!(rust.contains("pub const EMBEDDING_MODEL_IDENTITY: &'static str = \"embed-v1\""));
    assert!(rust.contains("pub fn for_embedding("));
    assert!(!rust.contains("for_embedding(request_id: String, org_id: ApplicationUuid, doc_id: ApplicationUuid, embedding: CanonicalVector, submitted_model"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("type Document struct"));
    assert!(go.contains("Embedding []float32"));
    assert!(go.contains("riffdb.VectorFrom(input.Embedding)"));
    assert!(go.contains("len(input.Embedding) != 4"));
    assert!(go.contains("const SetDocumentEmbeddingEmbeddingModelIdentity = \"embed-v1\""));
    assert!(go.contains(
        "func NewSetDocumentEmbeddingForEmbedding(input SetDocumentEmbeddingForEmbeddingInput)"
    ));

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains("GetDocument"));
    assert!(typescript.contains("readonly embedding: ReadonlyArray<number>"));
    assert!(typescript.contains(r#""dimension":4,"kind":"vector""#));
    assert!(typescript.contains("SET_DOCUMENT_EMBEDDING_EMBEDDING_MODEL_IDENTITY = \"embed-v1\""));
    assert!(typescript.contains("setDocumentEmbeddingForEmbedding(input: Omit<SetDocumentEmbeddingInput, \"submitted_model\" | \"submitted_version\">)"));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("title"));
    assert!(python.contains("embedding: Annotated[tuple[float, ...], \"vector<4>\"]"));
    assert!(
        python
            .contains("SET_DOCUMENT_EMBEDDING_EMBEDDING_MODEL_IDENTITY: Final[str] = \"embed-v1\"")
    );
    assert!(python.contains("def set_document_embedding_for_embedding(*,"));
}
