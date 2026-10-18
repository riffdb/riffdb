#![forbid(unsafe_code)]

//! Generated application facades over ADR-0136's production embedding write.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_BOUNDED_LIMIT_V1, QueryModule,
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
    $k: Limit<499>,
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

const VECTOR_EXIT_CONTRACT: &str = include_str!("../../../fixtures/vector-exit/documents.riff");
const VECTOR_EXIT_QUERY: &str =
    include_str!("../../../fixtures/vector-exit/similar_documents.riffq");
const VECTOR_EXIT_GENERATED: &str =
    include_str!("../../../fixtures/vector-exit/generated-client.rs");

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
fn remote_vector_exit_client_fixture_is_exactly_generator_owned() {
    let contract =
        compile_contract_source(VECTOR_EXIT_CONTRACT).expect("vector exit contract compiles");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("vector_documents").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("SimilarDocuments", VECTOR_EXIT_QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("vector exit module");
    assert_eq!(
        generate_rust_application_client(&module, &contract, &[]),
        VECTOR_EXIT_GENERATED
    );
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
    // A parameterised nearest K is a typed limit, and the only typed limit is
    // now Limit<MAX>, so this module carries the bounded-limit codec.
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_BOUNDED_LIMIT_V1
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

// req: PRJ-001, PRJ-002, PRJ-003, PRJ-004, PRJ-005, OQ-019, OQ-020, OQ-022, OQ-045
#[test]
fn riffql_source_projected_executes_event_and_columnar_projections_across_sdks() {
    let contract_src = r#"
contract TraceObservability version 1 {
  entity Span {
    key (org_id: string<64>, span_id: string<64>)
    field model: string<128>
    field status: string<32>
    field latency_ms: i64
    field total_tokens: i64
    field started_at: timestamp
    delete_policy no_inbound
  }
  event SpanLogged {
    org_id: string<64>
    model: string<128>
    latency_ms: i64
    total_tokens: i64
    started_at: timestamp
  }
  aggregate Spans {
    root Span
    partition_by (org_id)
    conflict_key (org_id, span_id)
  }
  projection HourlyModelRollup {
    source event SpanLogged
    key (org_id, model, hour(started_at))
    measure span_count = count()
    measure tokens_sum = sum(total_tokens)
    measure latency_min = min(latency_ms)
    measure latency_max = max(latency_ms)
    measure latency_mean = mean(latency_ms)
    frontier transactionally_ordered
  }
  columnar_projection SpanAnalytics {
    source entity Span
    fields (model, status, latency_ms, total_tokens, started_at)
  }
}
"#;
    let event_query = r#"
query QueryHourlyRollup(
    $org_id: Span.org_id,
    $model: Span.model,
    $start_bucket: Span.started_at,
    $end_bucket: Span.started_at
) {
    source projected HourlyModelRollup
    freshness available

    many rollups from HourlyModelRollup
        where org_id == $org_id
          && model == $model
          && started_at >= $start_bucket
          && started_at < $end_bucket
        order by started_at asc
        take 24

    return Found {
        rollups: rollups {
            started_at
            span_count
            tokens_sum
            latency_min
            latency_max
        }
    }
    outcomes Found
}
"#;
    let columnar_query = r#"
query QuerySpanAnalytics(
    $org_id: Span.org_id,
    $start_at: Span.started_at,
    $end_at: Span.started_at
) {
    source projected SpanAnalytics
    freshness bounded max_lag_ms 5000

    many spans from Span
        where org_id == $org_id
          && started_at >= $start_at
          && started_at < $end_at
        take 100

    aggregate by_model from spans {
        group by model, hour(started_at) as hour_bucket
        count() as span_count
        sum(total_tokens) as total_tokens
        p95(latency_ms) as p95_latency_ms
        order by total_tokens desc, model asc, hour_bucket asc
        take 20
    }

    return Found {
        by_model: by_model {
            model
            hour_bucket
            span_count
            total_tokens
            p95_latency_ms
        }
    }
    outcomes Found
}
"#;
    let contract = compile_contract_source(contract_src).expect("trace contract compiles");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("trace_analytics").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![
            NamedQuerySource::new("QueryHourlyRollup", event_query).expect("event query"),
            NamedQuerySource::new("QuerySpanAnalytics", columnar_query).expect("columnar query"),
        ],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &contract).expect("projected module compiles");
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &contract)
        .expect("projected module round-trips");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());

    let rust = generate_rust_application_client(&module, &contract, &[]);
    assert!(rust.contains("QueryHourlyRollup"));
    assert!(rust.contains("QuerySpanAnalytics"));

    let go = generate_go_application_client(&module, &contract, &[]);
    assert!(go.contains("QueryHourlyRollup"));
    assert!(go.contains("QuerySpanAnalytics"));

    let typescript = generate_typescript_application_client(&module, &contract, &[]);
    assert!(typescript.contains("QueryHourlyRollup"));
    assert!(typescript.contains("QuerySpanAnalytics"));

    let python =
        generate_python_application_client(&module, &contract, &[]).expect("Python client");
    assert!(python.contains("query_hourly_rollup"));
    assert!(python.contains("query_span_analytics"));
}
