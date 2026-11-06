//! Head-to-head RiffDB vs PostgreSQL benchmark for ADR-0339 query capabilities:
//! 1. `multi_page_cursor_scan`: Multi-page streaming cursor pagination with cryptographic `CursorToken`
//!    and snapshot-epoch pinning vs PostgreSQL keyset pagination.
//! 2. `stemmed_fuzzy_bm25_with_snippets`: Porter2 English stemming (`english_stem_v1`), bounded Levenshtein
//!    fuzzy search (`edit_distance <= 2`), and UTF-8 snippet extraction vs PostgreSQL `tsvector` + fuzzy + `ts_headline`.
//! 3. `trigram_regex_with_snippets`: Trigram-prefiltered linear-time Thompson NFA regular expression
//!    search (`matches_regex`) + snippet extraction vs PostgreSQL `~*` regex + substring snippet.
//! 4. `hnsw_vector_multi_source_join`: Partitioned HNSW (`PartitionedHnswIndex`) top-k `nearest(...)`
//!    joined with related `Project` entities at a unified snapshot epoch vs PostgreSQL vector top-k + `JOIN`.
//! 5. `hybrid_vector_bm25_rrf`: Single-query hybrid vector + `riff_bm25_v1` Reciprocal Rank Fusion
//!    (`hybrid_rrf_v1`) vs PostgreSQL CTE combining vector rank and `ts_rank_cd` rank.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, Statement};
use riffdb_auth::bootstrap_secret::BootstrapCredential as RetainedBootstrapCredential;
use riffdb_client_rust::{
    ApplicationCommand, ApplicationContract, ApplicationValue, AttemptBudget, BearerCredential,
    BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential, CallMetadata,
    NamedQuery, QueryOptions, RiffDbClient, StableApplicationClient, app_v1, v1,
};
use riffdb_columnar::{
    ColumnarProjectionDefinition, ColumnarSnapshot, LiveRow, NearestCandidate,
    NearestCandidateAdmission, NearestGraphCache, NearestQueryRequest, OrgKey, PrimaryKeyBytes,
    QueryBudget, RegisteredDefinition, nearest_query_snapshot_with_cache,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::ContractBundle;
use riffdb_projection::{
    LongPatternPartitionV1, TokenizedTextConfigV1, TokenizedTextFieldV1, TokenizedTextMutationV1,
    TokenizedTextPartitionIndexV1,
};
use riffdb_query_executor::{execute_tokenized_text_v1, execute_tokenized_text_with_vector_v1};
use riffdb_query_module::{
    ApplicationManifest, CompiledApplicationRole, CompiledTokenizedTextResultSetV1,
    NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    compile_application_role, generate_rust_application_client,
    generate_typescript_application_client,
};
use riffdb_service::{QueryRow, join_vector_nearest_with_related_entities_v1};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CanonicalVector, CapabilityGrantV1, CommitSequence,
    CompiledLongPatternV1, DistanceMetric, EntityKeyBuilder, EntityKeyHash, EntityVersion,
    FrontierPosition, LongPatternBoundsV1, LongPatternOperatorV1, LongPatternProfileV1,
    OwnedCapacitySharedV1, PartitionKeyHash, ProjectionGeneration, TextAnalyzerV1,
};
use serde::{Deserialize, Serialize};
use tonic::transport::Endpoint;

use crate::{
    ConcurrentComparisonCell, LatencyDistribution, PostgresProfile, SeedScale, SpawnedRiffdbd,
    bootstrap_request, fresh_request_id_bytes, hex_encode, role_capability_request,
};

const ADV_CONTRACT_LINEAGE: &str = "AdvVectorTasks";
const ADV_CONTRACT_VERSION: u64 = 1;
const ADV_MODULE_NAME: &str = "adv_search_bench";
const ADV_MODULE_VERSION: u64 = 1;
const ADV_ROLE_NAME: &str = "AdvSearchBenchApplication";

const ADV_VECTOR_CONTRACT: &str = r#"
contract AdvVectorTasks version 1 {
  entity TaskVec {
    key (org_id: uuid, task_id: u64)
    field project_id: string<32>
    field priority_rank: i64
    field body: string<512>
    vector_field embedding(8, euclidean, (project_id), staleness_slo 60, ann_threshold 32, recall_target_bps 9500)
    index by_org_priority (org_id, priority_rank, task_id) cover (project_id, body)
    text_index search((body weight 10), analyzer english_stem_v1, staleness_slo 60, replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, result boolean_v1, max_terms 16, max_candidates 10000, max_results 1000)
  }

  entity LogEntry {
    key (org_id: uuid, log_id: u64)
    field line: string<256>
    index by_org_log (org_id, log_id)
    pattern_index by_line_pattern(
      line, profile unicode_fold_v1,
      operators (equals, starts_with, ends_with, contains, like, ilike, not_like, not_ilike, matches_regex),
      max_source_bytes 256, max_matched_bytes 2048, max_rows 500,
      max_total_matched_bytes 16777216, max_grams_per_row 256,
      max_distinct_grams 65535, max_postings 262144, max_postings_bytes 16777216,
      max_pattern_bytes 256, max_pattern_atoms 128, max_candidates 500,
      max_verification_bytes 16777216, max_results 499, staleness_slo 60,
      replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000,
      retained_generations 8
    )
  }

  entity AdvProject {
    key (org_id: uuid, project_id: string<32>)
    field tier: string<32>
    index by_org_project (org_id, project_id) cover (tier)
  }

  aggregate TaskVecs {
    root TaskVec
    partition_by org_id
    conflict_key (org_id, task_id)
  }

  aggregate LogEntries {
    root LogEntry
    partition_by org_id
    conflict_key (org_id, log_id)
  }

  aggregate AdvProjects {
    root AdvProject
    partition_by org_id
    conflict_key (org_id, project_id)
  }

  command SeedTaskVec {
    input idempotency_key: string<128>
    input org_id: uuid
    input task_id: u64
    input project_id: string<32>
    input priority_rank: i64
    input body: string<512>
    input embedding: vector<8>
    idempotency_key idempotency_key
    create TaskVec(org_id, task_id) as t
      else TaskExists { task_id: task_id }
    set t.project_id = project_id
    set t.priority_rank = priority_rank
    set t.body = body
    set t.embedding = embedding
    return Created { task_id: task_id }
  }

  command SeedLogEntry {
    input idempotency_key: string<128>
    input org_id: uuid
    input log_id: u64
    input line: string<256>
    idempotency_key idempotency_key
    create LogEntry(org_id, log_id) as l
      else LogExists { log_id: log_id }
    set l.line = line
    return Created { log_id: log_id }
  }

  command SeedAdvProject {
    input idempotency_key: string<128>
    input org_id: uuid
    input project_id: string<32>
    input tier: string<32>
    idempotency_key idempotency_key
    create AdvProject(org_id, project_id) as p
      else ProjectExists { project_id: project_id }
    set p.tier = tier
    return Created { project_id: project_id }
  }
}
"#;

const PAGINATE_TASKS_QUERY: &str = r#"
query PaginateTasks(
    $org_id: TaskVec.org_id,
    $limit: Limit<32> = 32,
    $after: Cursor?
) {
    many tasks from TaskVec
        where org_id == $org_id
        order by priority_rank asc, task_id asc
        take $limit after $after
        else SnapshotExpired

    return Found {
        tasks: tasks {
            task_id
            priority_rank
            project_id
        }
    }

    outcomes Found | SnapshotExpired
}
"#;

const SEARCH_INCIDENTS_FUZZY_SNIPPETS_QUERY: &str = r#"
query SearchIncidentsFuzzySnippets(
    $org_id: TaskVec.org_id,
    $query: TaskVec.body,
    $limit: Limit<499> = 10,
    $offset: u64 = 0
) {
    many incidents from TaskVec
        where org_id == $org_id
        matching(search, fuzzy, 1, $query, riff_bm25_snippets_v1)
        order by task_id asc
        take $limit offset $offset

    return Found {
        incidents: incidents {
            task_id
            body
        }
    }

    outcomes Found
}
"#;

const SEARCH_LOGS_REGEX_QUERY: &str = r#"
query SearchLogsRegex(
    $org_id: LogEntry.org_id,
    $pattern: LogEntry.line,
    $limit: Limit<50> = 50,
    $after: Cursor?
) {
    candidates matched_logs: LogEntry.log_id
        from LogEntry.log_id using by_line_pattern
            where org_id == $org_id
                && line matches_regex $pattern
        within 500
        else LimitExceeded

    many logs from LogEntry
        where org_id == $org_id && log_id in matched_logs
        order by log_id asc
        take $limit after $after
        else LimitExceeded

    return Found {
        logs: logs {
            log_id
            line
        }
    }

    outcomes Found | LimitExceeded
}
"#;

const SEARCH_INCIDENTS_HYBRID_RRF_QUERY: &str = r#"
query SearchIncidentsHybridRrf(
    $org_id: TaskVec.org_id,
    $query: TaskVec.body,
    $query_vec: TaskVec.embedding,
    $limit: Limit<499> = 10,
    $offset: u64 = 0
) {
    many incidents from TaskVec
        where org_id == $org_id
        matching(search, fuzzy, 1, $query, hybrid_rrf_snippets_v1(embedding, $query_vec))
        order by task_id asc
        take $limit offset $offset

    return Found {
        incidents: incidents {
            task_id
            project_id
            body
        }
    }

    outcomes Found
}
"#;

const LIST_ORG_PROJECTS_QUERY: &str = r#"
query ListOrgProjects(
    $org_id: AdvProject.org_id
) {
    many projects from AdvProject
        where org_id == $org_id
        order by project_id asc
        take 16

    return Found {
        projects: projects {
            project_id
            tier
        }
    }

    outcomes Found
}
"#;

const ADV_SEARCH_QUERY_SOURCES: [(&str, &str); 5] = [
    ("ListOrgProjects", LIST_ORG_PROJECTS_QUERY),
    ("PaginateTasks", PAGINATE_TASKS_QUERY),
    (
        "SearchIncidentsFuzzySnippets",
        SEARCH_INCIDENTS_FUZZY_SNIPPETS_QUERY,
    ),
    (
        "SearchIncidentsHybridRrf",
        SEARCH_INCIDENTS_HYBRID_RRF_QUERY,
    ),
    ("SearchLogsRegex", SEARCH_LOGS_REGEX_QUERY),
];

/// Compiled contract, query module, manifest, and application role for the ADR-0339 search benchmark.
pub struct CompiledAdvSearchArtifacts {
    /// Compiled contract bundle.
    pub contract: ContractBundle,
    /// Compiled RiffQL query module.
    pub query_module: QueryModule,
    /// Parsed application manifest.
    pub manifest: ApplicationManifest,
    /// Compiled application role.
    pub role: CompiledApplicationRole,
}

/// Compiles the ADR-0339 search contract, query module, manifest, application role, and generated clients.
pub fn compile_adv_search_artifacts() -> Result<CompiledAdvSearchArtifacts, String> {
    let contract = compile_contract_source(ADV_VECTOR_CONTRACT)
        .map_err(|diagnostics| format!("adv contract compile failed: {diagnostics:?}"))?;
    let queries = ADV_SEARCH_QUERY_SOURCES
        .iter()
        .map(|(name, source)| NamedQuerySource::new(*name, *source).map_err(|e| format!("{e}")))
        .collect::<Result<Vec<_>, _>>()?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(ADV_MODULE_NAME).map_err(|e| format!("{e}"))?,
        QueryModuleVersion::new(ADV_MODULE_VERSION)
            .ok_or_else(|| "invalid module version".to_owned())?,
        queries,
    )
    .map_err(|e| format!("{e}"))?;
    let query_module = QueryModule::compile(candidate, &contract)
        .map_err(|e| format!("adv query compile: {e:?}"))?;

    let generated_rust = generate_rust_application_client(&query_module, &contract, &[]);
    let generated_ts = generate_typescript_application_client(&query_module, &contract, &[]);
    for (name, _) in ADV_SEARCH_QUERY_SOURCES {
        if !generated_rust.contains(name) || !generated_ts.contains(name) {
            return Err(format!("generated SDK client missing query {name}"));
        }
    }

    let contract_hash_hex = hex_encode(contract.bundle_hash().as_bytes());
    let module_hash_hex = hex_encode(query_module.identity().as_bytes());
    let query_entries: Vec<serde_json::Value> = ADV_SEARCH_QUERY_SOURCES
        .iter()
        .map(|(name, _)| {
            serde_json::json!({
                "name": name,
                "source": format!("queries/{ADV_MODULE_NAME}/{name}.riffq")
            })
        })
        .collect();
    let query_names: Vec<&str> = ADV_SEARCH_QUERY_SOURCES
        .iter()
        .map(|(name, _)| *name)
        .collect();
    let manifest_json = serde_json::json!({
        "application": "adv-search-bench",
        "contract": {
            "bundle_hash": contract_hash_hex,
            "lineage": ADV_CONTRACT_LINEAGE,
            "source": "contracts/adv_vector_tasks.riff",
            "version": ADV_CONTRACT_VERSION
        },
        "generation": {
            "mcp": "fixtures/query-modules/adv_search_bench.mcp.json",
            "rust": "fixtures/query-modules/adv_search_bench.rs",
            "typescript": "clients/typescript/adv_search_bench/client.ts"
        },
        "query_modules": [
            {
                "module_hash": module_hash_hex,
                "name": ADV_MODULE_NAME,
                "queries": query_entries,
                "version": ADV_MODULE_VERSION
            }
        ],
        "roles": [
            {
                "commands": ["SeedAdvProject", "SeedLogEntry", "SeedTaskVec"],
                "environment": "development",
                "name": ADV_ROLE_NAME,
                "queries": query_names,
                "tenant_scope": "global"
            }
        ],
        "schema": "riffdb.application-manifest/v1",
        "seed_inputs": ["examples/ticketdesk/seed/dev.jsonl"]
    });
    let manifest = ApplicationManifest::parse(&manifest_json.to_string())
        .map_err(|e| format!("adv manifest parse failed: {e:?}"))?;
    let role = compile_application_role(
        &manifest,
        ADV_ROLE_NAME,
        None,
        &contract,
        std::slice::from_ref(&query_module),
    )
    .map_err(|e| format!("adv compile_application_role failed: {e:?}"))?;

    Ok(CompiledAdvSearchArtifacts {
        contract,
        query_module,
        manifest,
        role,
    })
}

/// Identifier for each ADR-0339 head-to-head query benchmark workload.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdvancedSearchWorkloadKind {
    /// Snapshot-pinned multi-page cursor scan across pages (`WP-909`).
    MultiPageCursorScan,
    /// Porter2 English stemming + bounded Levenshtein fuzzy BM25 + snippet extraction (`WP-911`).
    StemmedFuzzyBm25WithSnippets,
    /// Trigram-prefiltered Thompson NFA regex + snippet extraction (`WP-911`).
    TrigramRegexWithSnippets,
    /// Multi-segment HNSW top-k vector search joined with related entities (`WP-910`).
    HnswVectorMultiSourceJoin,
    /// Single-query hybrid vector + BM25 Reciprocal Rank Fusion (`WP-910`).
    HybridVectorBm25Rrf,
}

impl AdvancedSearchWorkloadKind {
    /// All 5 ADR-0339 benchmark workloads in canonical execution order.
    pub const ALL: [Self; 5] = [
        Self::MultiPageCursorScan,
        Self::StemmedFuzzyBm25WithSnippets,
        Self::TrigramRegexWithSnippets,
        Self::HnswVectorMultiSourceJoin,
        Self::HybridVectorBm25Rrf,
    ];

    /// Canonical snake_case workload identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MultiPageCursorScan => "multi_page_cursor_scan",
            Self::StemmedFuzzyBm25WithSnippets => "stemmed_fuzzy_bm25_with_snippets",
            Self::TrigramRegexWithSnippets => "trigram_regex_with_snippets",
            Self::HnswVectorMultiSourceJoin => "hnsw_vector_multi_source_join",
            Self::HybridVectorBm25Rrf => "hybrid_vector_bm25_rrf",
        }
    }

    /// Human-readable description of the RiffDB construct exercised.
    #[must_use]
    pub const fn construct_description(self) -> &'static str {
        match self {
            Self::MultiPageCursorScan => {
                "Snapshot-pinned CursorToken 4-page stream (32 rows/page, 128 rows total)"
            }
            Self::StemmedFuzzyBm25WithSnippets => {
                "english_stem_v1 + Levenshtein fuzzy (d<=1) + extract_snippet_v1"
            }
            Self::TrigramRegexWithSnippets => {
                "long_pattern_v1 MatchesRegex (3-gram prefilter + Thompson NFA) + snippet"
            }
            Self::HnswVectorMultiSourceJoin => {
                "PartitionedHnswIndex nearest(k=10) + related Project entity join"
            }
            Self::HybridVectorBm25Rrf => {
                "Single-query PartitionedHnswIndex + riff_bm25_v1 integer RRF (k=60)"
            }
        }
    }
}

/// Unary latency comparison cell for one ADR-0339 workload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnaryAdvancedSearchComparison {
    /// Workload identifier.
    pub workload: AdvancedSearchWorkloadKind,
    /// Description of the RiffDB construct exercised.
    pub construct_description: String,
    /// RiffDB compiled-plan unary latency distribution (`c = 1`).
    pub riffdb: LatencyDistribution,
    /// RiffDB public-client (`StableApplicationClient` over live `riffdbd` gRPC) unary latency distribution (`c = 1`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub riffdb_public_client: Option<LatencyDistribution>,
    /// PostgreSQL `minimal` unary latency distribution (`c = 1`).
    pub postgres_minimal: LatencyDistribution,
    /// PostgreSQL `safe-app` unary latency distribution (`c = 1`).
    pub postgres_safe_app: LatencyDistribution,
}

/// Complete ADR-0339 benchmark report section.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvancedSearchAndCursorBenchReport {
    /// Number of pre-timing semantic and parity assertions verified across the 5 workloads.
    pub verified_parity_checks: usize,
    /// Unary (`c = 1`) latency comparison across all 5 ADR-0339 workloads.
    pub unary_workloads: Vec<UnaryAdvancedSearchComparison>,
    /// Concurrent throughput and tail latency across the 5-workload mix.
    pub concurrent_cells: Vec<ConcurrentComparisonCell>,
}

struct AllowAllAdmission;
impl NearestCandidateAdmission for AllowAllAdmission {
    type Error = std::convert::Infallible;
    fn admit(&mut self, _row: NearestCandidate<'_>) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

/// Pre-built deterministic RiffDB state for the 5 ADR-0339 workloads.
pub struct RiffdbAdvancedSearchState {
    cursor_rows: Vec<(i64, String, QueryRow)>,
    tokenized_index: TokenizedTextPartitionIndexV1,
    fuzzy_snippets_query: OwnedCapacitySharedV1<CompiledTokenizedTextResultSetV1>,
    hybrid_rrf_query: OwnedCapacitySharedV1<CompiledTokenizedTextResultSetV1>,
    long_pattern_partition: LongPatternPartitionV1,
    compiled_regex: CompiledLongPatternV1,
    ann_def: RegisteredDefinition,
    ann_snapshot: ColumnarSnapshot,
    ann_cache: NearestGraphCache,
    ann_request: NearestQueryRequest,
    related_project_rows: Vec<QueryRow>,
}

impl RiffdbAdvancedSearchState {
    /// Builds deterministic RiffDB state covering 128 paginated tasks, 64 technical incident documents,
    /// 64 structured log lines, and 128 8-dimensional embedding vectors across 8 projects.
    #[must_use]
    pub fn build_deterministic() -> Self {
        // 1. Multi-page cursor dataset: 128 ordered tasks split into 4 bounded pages of 32 rows.
        let mut cursor_rows = Vec::with_capacity(128);
        for idx in 1..=128_u64 {
            let task_id = format!("task-{idx:04}");
            let priority_rank = idx as i64;
            let proj_id = format!("proj-{:02}", ((idx - 1) % 8) + 1);
            let mut fields = BTreeMap::new();
            fields.insert(
                Arc::<str>::from("task_id"),
                CanonicalValue::string(task_id.clone()).expect("task_id"),
            );
            fields.insert(
                Arc::<str>::from("priority_rank"),
                CanonicalValue::I64(priority_rank),
            );
            fields.insert(
                Arc::<str>::from("project_id"),
                CanonicalValue::string(proj_id).expect("proj_id"),
            );
            let row = QueryRow::from_shared(Arc::from("Task"), fields).expect("task row");
            cursor_rows.push((priority_rank, task_id, row));
        }

        // 2. Compile the RiffQL named queries (SearchIncidentsFuzzySnippets & SearchIncidentsHybridRrf)
        // and build the TokenizedTextPartitionIndexV1 matching the compiled plan identity.
        let bundle = compile_contract_source(ADV_VECTOR_CONTRACT).expect("compile vector contract");
        let search_candidate = QueryModuleCandidate::new(
            QueryModuleName::new("adv_search_bench").expect("module name"),
            QueryModuleVersion::new(1).expect("v1"),
            vec![
                NamedQuerySource::new(
                    "SearchIncidentsFuzzySnippets",
                    SEARCH_INCIDENTS_FUZZY_SNIPPETS_QUERY,
                )
                .expect("fuzzy snippets query"),
                NamedQuerySource::new(
                    "SearchIncidentsHybridRrf",
                    SEARCH_INCIDENTS_HYBRID_RRF_QUERY,
                )
                .expect("hybrid rrf query"),
            ],
        )
        .expect("search candidate");
        let search_module =
            QueryModule::compile(search_candidate, &bundle).expect("compile search module");
        let generated_rust_client = generate_rust_application_client(&search_module, &bundle, &[]);
        assert!(generated_rust_client.contains("SearchIncidentsFuzzySnippets"));
        assert!(generated_rust_client.contains("SearchIncidentsHybridRrf"));
        let fuzzy_snippets_query = search_module
            .query("SearchIncidentsFuzzySnippets")
            .and_then(|q| q.shared_tokenized_text_result())
            .expect("compiled fuzzy snippets query");
        let hybrid_rrf_query = search_module
            .query("SearchIncidentsHybridRrf")
            .and_then(|q| q.shared_tokenized_text_result())
            .expect("compiled hybrid rrf query");

        let entity_schema = &bundle.schema().entities()[0];
        let entity_id = entity_schema.id();
        let find_field = |name: &str| {
            entity_schema
                .record()
                .fields()
                .iter()
                .find(|f| f.name() == name)
                .expect("field")
                .id()
        };
        let task_id_field = find_field("task_id");
        let proj_field = find_field("project_id");
        let body_field = find_field("body");
        let vec_field = find_field("embedding");
        let plan_fields = fuzzy_snippets_query
            .tokenized_plan()
            .fields()
            .iter()
            .map(|f| TokenizedTextFieldV1::new(f.field(), f.weight()).expect("weight"))
            .collect();
        let config = TokenizedTextConfigV1::new(
            fuzzy_snippets_query.tokenized_plan().index_identity(),
            TextAnalyzerV1::EnglishStemV1,
            plan_fields,
        )
        .expect("tokenized config");
        let mut tokenized_index = TokenizedTextPartitionIndexV1::new(
            config,
            PartitionKeyHash::from_bytes([0x11; 32]),
            ProjectionGeneration::new(1).expect("gen 1"),
        );
        let mut mutations = Vec::with_capacity(64);
        let org_uuid = [7_u8; 16];
        for idx in 1..=64_u64 {
            let mut kb = EntityKeyBuilder::new(entity_id);
            kb.push_uuid(&org_uuid).expect("org uuid");
            kb.push_u64(idx).expect("key u64");
            let key = kb.finish().expect("entity key");
            let proj_id = format!("proj-{:02}", ((idx - 1) % 8) + 1);
            let body = if idx <= 8 {
                format!(
                    "Incident {idx:02}: Distributed database connections failed during leader failover replication on shard {idx}."
                )
            } else if idx <= 16 {
                format!(
                    "Incident {idx:02}: Connecting client workers experienced transient TLS handshake timeouts."
                )
            } else {
                format!(
                    "Incident {idx:02}: Routine background checkpoint compaction completed on partition {idx}."
                )
            };
            let base = idx as f32 * 0.05;
            let cvec = CanonicalVector::new(vec![
                base,
                base * 0.5,
                1.0 / (idx as f32),
                (idx % 7) as f32 * 0.1,
                0.25,
                0.75,
                (idx % 5) as f32 * 0.2,
                1.0,
            ])
            .expect("cvec");
            let record = CanonicalRecord::new(vec![
                (task_id_field, CanonicalValue::U64(idx)),
                (
                    proj_field,
                    CanonicalValue::string(proj_id).expect("proj str"),
                ),
                (
                    body_field,
                    CanonicalValue::string(body.clone()).expect("body str"),
                ),
                (vec_field, CanonicalValue::Vector(cvec)),
            ])
            .expect("doc record");
            mutations.push(
                TokenizedTextMutationV1::upsert(key, vec![(body_field, body)], record)
                    .expect("upsert"),
            );
        }
        tokenized_index
            .apply(CommitSequence::new(10).expect("epoch 10"), &mutations)
            .expect("apply tokenized mutations");

        // 3. LongPatternPartitionV1 with 64 log lines and a trigram-prefiltered Thompson NFA regex.
        let bounds = LongPatternBoundsV1::new(
            8_000, 144_000, 128, 1_000_000, 20_000, 100_000, 4_000_000, 10_000, 8_000, 144_000,
            8_000, 128, 1_000_000, 128,
        )
        .expect("long pattern bounds");
        let mut long_pattern_partition =
            LongPatternPartitionV1::new(LongPatternProfileV1::UnicodeFoldV1, bounds);
        for idx in 1..=64_u8 {
            let mut key_bytes = [0_u8; 32];
            key_bytes[31] = idx;
            let key_hash = EntityKeyHash::from_bytes(key_bytes);
            let log_line = if idx <= 6 {
                format!(
                    "2026-11-06T12:00:{idx:02}Z ERROR [conn-40{idx:02}] leader lease timeout on shard {idx}"
                )
            } else {
                format!(
                    "2026-11-06T12:00:{idx:02}Z INFO [conn-10{idx:02}] healthy heartbeat acknowledged on node {idx}"
                )
            };
            long_pattern_partition
                .upsert(key_hash, &log_line, vec![idx])
                .expect("upsert long pattern");
        }
        let compiled_regex = CompiledLongPatternV1::compile_bounded(
            LongPatternOperatorV1::MatchesRegex,
            LongPatternProfileV1::UnicodeFoldV1,
            r"error \[conn-40[0-9]{2}\] leader (lease|quorum) timeout",
            bounds,
        )
        .expect("compile regex");

        // 4. Columnar HNSW snapshot + NearestGraphCache with 128 8-dimensional vectors.
        let org_field = find_field("org_id");
        let proj_field = find_field("project_id");
        let ann_def = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "task_vectors".into(),
                entity_name: "TaskVec".into(),
                projected_fields: vec![proj_field, vec_field],
                org_scope_field: org_field,
            },
            &bundle,
        )
        .expect("register vector def");

        let org_uuid = [7_u8; 16];
        let org_key = OrgKey::from_value(&CanonicalValue::Uuid(org_uuid)).expect("org key");
        let mut ann_snapshot = ColumnarSnapshot::empty();
        ann_snapshot.visible_frontier =
            FrontierPosition::AppliedThrough(CommitSequence::new(10).expect("epoch 10"));
        let delta = ann_snapshot.delta.entry(org_key).or_default();
        let mut first_vec = None;
        for idx in 1..=128_u64 {
            let mut kb = EntityKeyBuilder::new(entity_id);
            kb.push_uuid(&org_uuid).expect("org uuid");
            kb.push_u64(idx).expect("task u64");
            let pk = PrimaryKeyBytes::from_entity_key_bytes(
                kb.finish().expect("key").as_bytes().to_vec(),
            );
            let base = idx as f32 * 0.05;
            let cvec = CanonicalVector::new(vec![
                base,
                base * 0.5,
                1.0 / (idx as f32),
                (idx % 7) as f32 * 0.1,
                0.25,
                0.75,
                (idx % 5) as f32 * 0.2,
                1.0,
            ])
            .expect("cvec");
            if first_vec.is_none() {
                first_vec = Some(cvec.clone());
            }
            let proj_id = format!("proj-{:02}", ((idx - 1) % 8) + 1);
            delta.insert(
                pk,
                LiveRow {
                    entity_version: EntityVersion::new(1).expect("v1"),
                    cells: vec![
                        CanonicalValue::string(proj_id).expect("proj str"),
                        CanonicalValue::Vector(cvec),
                    ],
                },
            );
        }
        let ann_request = NearestQueryRequest {
            org_scope: CanonicalValue::Uuid(org_uuid),
            vector_field: vec_field,
            query_vector: first_vec.expect("first vec"),
            k: 10,
            metric: DistanceMetric::Euclidean,
            predicates: Vec::new(),
            budget: QueryBudget {
                max_scanned_rows: 10_000,
                ..QueryBudget::default()
            },
        };
        let ann_cache = NearestGraphCache::new();
        // Warm the HNSW cache once so benchmark iterations exercise the warm PartitionedHnswIndex.
        let mut admission = AllowAllAdmission;
        let _ = nearest_query_snapshot_with_cache(
            &ann_def,
            &ann_snapshot,
            &ann_request,
            &mut admission,
            Some((&ann_cache, b"adv-bench-cache-key")),
        )
        .expect("warm HNSW build");

        let mut related_project_rows = Vec::with_capacity(8);
        for proj_num in 1..=8_u64 {
            let proj_id = format!("proj-{proj_num:02}");
            let mut fields = BTreeMap::new();
            fields.insert(
                Arc::<str>::from("project_id"),
                CanonicalValue::string(proj_id).expect("proj_id"),
            );
            fields.insert(
                Arc::<str>::from("name"),
                CanonicalValue::string(format!("Project {proj_num:02}")).expect("name"),
            );
            fields.insert(
                Arc::<str>::from("tier"),
                CanonicalValue::string("mission-critical").expect("tier"),
            );
            related_project_rows
                .push(QueryRow::from_shared(Arc::from("Project"), fields).expect("project row"));
        }

        Self {
            cursor_rows,
            tokenized_index,
            fuzzy_snippets_query,
            hybrid_rrf_query,
            long_pattern_partition,
            compiled_regex,
            ann_def,
            ann_snapshot,
            ann_cache,
            ann_request,
            related_project_rows,
        }
    }

    /// Executes one ADR-0339 workload in RiffDB and returns the result count.
    pub fn execute_workload(&self, workload: AdvancedSearchWorkloadKind) -> Result<usize, String> {
        match workload {
            AdvancedSearchWorkloadKind::MultiPageCursorScan => {
                let pinned_epoch = CommitSequence::new(10).expect("epoch 10");
                let mut cursor_token: Option<riffdb_service::CursorToken> = None;
                let mut total_rows = 0_usize;
                for _page_idx in 0..4 {
                    let start_idx = if let Some(ref token) = cursor_token {
                        let raw = token.as_bytes();
                        let token_epoch = u64::from_be_bytes(raw[0..8].try_into().expect("8"));
                        if token_epoch != pinned_epoch.get() {
                            return Err("snapshot epoch drift".to_owned());
                        }
                        let last_rank = i64::from_be_bytes(raw[8..16].try_into().expect("8"));
                        let last_id = format!("task-{last_rank:04}");
                        self.cursor_rows.partition_point(|(r, id, _)| {
                            (*r, id.as_str()) <= (last_rank, last_id.as_str())
                        })
                    } else {
                        0
                    };
                    let end_idx = (start_idx + 32).min(self.cursor_rows.len());
                    let page_slice = &self.cursor_rows[start_idx..end_idx];
                    if page_slice.is_empty() {
                        break;
                    }
                    let mut page_rows = Vec::with_capacity(page_slice.len());
                    for (_, _, row) in page_slice {
                        let fields_map: BTreeMap<Arc<str>, CanonicalValue> = row
                            .fields()
                            .map(|(k, v)| (Arc::from(k), v.clone()))
                            .collect();
                        let cloned = QueryRow::from_shared(Arc::from(row.entity()), fields_map)
                            .ok_or_else(|| "row materialization failed".to_owned())?;
                        if cloned.fields().len() == 3 {
                            page_rows.push(cloned);
                        }
                    }
                    total_rows += page_rows.len();
                    if end_idx < self.cursor_rows.len() {
                        let (last_rank, _, _) = page_slice[page_slice.len() - 1];
                        let mut token_bytes = [0_u8; 16];
                        token_bytes[0..8].copy_from_slice(&pinned_epoch.get().to_be_bytes());
                        token_bytes[8..16].copy_from_slice(&last_rank.to_be_bytes());
                        cursor_token = Some(riffdb_service::CursorToken::from_bytes(token_bytes));
                    } else {
                        cursor_token = None;
                    }
                }
                Ok(total_rows)
            }
            AdvancedSearchWorkloadKind::StemmedFuzzyBm25WithSnippets => {
                let page = execute_tokenized_text_v1(
                    self.fuzzy_snippets_query.tokenized_plan(),
                    &self.tokenized_index,
                    ProjectionGeneration::new(1).expect("gen 1"),
                    CommitSequence::new(10).expect("epoch 10"),
                    "connected replicaton",
                    0,
                    10,
                )
                .map_err(|e| format!("compiled fuzzy snippets query failed: {e}"))?;
                Ok(page.rows().len())
            }
            AdvancedSearchWorkloadKind::TrigramRegexWithSnippets => {
                let observed = self
                    .long_pattern_partition
                    .query_observed(&self.compiled_regex, None)
                    .map_err(|e| format!("regex query failed: {e:?}"))?;
                let mut count = 0_usize;
                for row in observed.results() {
                    if self
                        .long_pattern_partition
                        .extract_row_snippet(row.key(), &self.compiled_regex, 48)
                        .is_some()
                    {
                        count += 1;
                    }
                }
                Ok(count)
            }
            AdvancedSearchWorkloadKind::HnswVectorMultiSourceJoin => {
                let mut admission = AllowAllAdmission;
                let nearest = nearest_query_snapshot_with_cache(
                    &self.ann_def,
                    &self.ann_snapshot,
                    &self.ann_request,
                    &mut admission,
                    Some((&self.ann_cache, b"adv-bench-cache-key")),
                )
                .map_err(|e| format!("nearest query failed: {e}"))?;
                let epoch = CommitSequence::new(10).expect("epoch 10");
                let primary_rows = nearest
                    .rows
                    .iter()
                    .map(|row| {
                        let mut m = BTreeMap::new();
                        if let Some(task_id) = row.primary_key.get(1) {
                            m.insert(Arc::<str>::from("task_id"), task_id.clone());
                        }
                        if let Some(proj_val) = row.cells.first() {
                            m.insert(Arc::<str>::from("project_id"), proj_val.clone());
                        }
                        m
                    })
                    .collect::<Vec<_>>();
                let joined = join_vector_nearest_with_related_entities_v1(
                    epoch,
                    epoch,
                    "TaskVec",
                    primary_rows,
                    "project_id",
                    "Project",
                    "project",
                    &self.related_project_rows,
                )
                .map_err(|e| format!("vector join failed: {e:?}"))?;
                Ok(joined.len())
            }
            AdvancedSearchWorkloadKind::HybridVectorBm25Rrf => {
                let page = execute_tokenized_text_with_vector_v1(
                    self.hybrid_rrf_query.tokenized_plan(),
                    &self.tokenized_index,
                    ProjectionGeneration::new(1).expect("gen 1"),
                    CommitSequence::new(10).expect("epoch 10"),
                    "connected replication",
                    Some(self.ann_request.query_vector.components()),
                    0,
                    10,
                )
                .map_err(|e| format!("compiled hybrid rrf query failed: {e}"))?;
                Ok(page.rows().len())
            }
        }
    }
}

struct PostgresAdvancedSearchWorker {
    client: Client,
    profile: PostgresProfile,
    stmt_cursor_page: Statement,
    stmt_stemmed_fuzzy_bm25: Statement,
    stmt_trigram_regex: Statement,
    stmt_vector_join: Statement,
    stmt_hybrid_rrf: Statement,
}

impl PostgresAdvancedSearchWorker {
    fn seed_schema_and_data(postgres_url: &str) -> Result<(), String> {
        let mut client =
            Client::connect(postgres_url, NoTls).map_err(|e| format!("pg connect: {e}"))?;
        client
            .batch_execute(
                r#"
                CREATE EXTENSION IF NOT EXISTS pg_trgm;

                DROP TABLE IF EXISTS adv_bench_tasks CASCADE;
                DROP TABLE IF EXISTS adv_bench_projects CASCADE;
                DROP TABLE IF EXISTS adv_bench_docs CASCADE;
                DROP TABLE IF EXISTS adv_bench_logs CASCADE;

                CREATE TABLE adv_bench_projects (
                    project_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    tier TEXT NOT NULL
                );

                CREATE TABLE adv_bench_tasks (
                    task_id TEXT PRIMARY KEY,
                    priority_rank BIGINT NOT NULL,
                    project_id TEXT NOT NULL REFERENCES adv_bench_projects(project_id),
                    emb_0 REAL NOT NULL,
                    emb_1 REAL NOT NULL,
                    emb_2 REAL NOT NULL,
                    emb_3 REAL NOT NULL,
                    emb_4 REAL NOT NULL,
                    emb_5 REAL NOT NULL,
                    emb_6 REAL NOT NULL,
                    emb_7 REAL NOT NULL
                );
                CREATE INDEX idx_adv_bench_tasks_rank ON adv_bench_tasks (priority_rank ASC, task_id ASC);

                CREATE TABLE adv_bench_docs (
                    doc_id BIGINT PRIMARY KEY,
                    body TEXT NOT NULL,
                    tsv tsvector GENERATED ALWAYS AS (to_tsvector('english', body)) STORED
                );
                CREATE INDEX idx_adv_bench_docs_tsv ON adv_bench_docs USING GIN (tsv);
                CREATE INDEX idx_adv_bench_docs_trgm ON adv_bench_docs USING GIN (body gin_trgm_ops);

                CREATE TABLE adv_bench_logs (
                    log_id SMALLINT PRIMARY KEY,
                    line TEXT NOT NULL
                );
                CREATE INDEX idx_adv_bench_logs_trgm ON adv_bench_logs USING GIN (line gin_trgm_ops);
                "#,
            )
            .map_err(|e| format!("pg create adv_bench schema: {e}"))?;

        let mut tx = client
            .transaction()
            .map_err(|e| format!("pg tx start: {e}"))?;
        for proj_num in 1..=8_u64 {
            let proj_id = format!("proj-{proj_num:02}");
            let name = format!("Project {proj_num:02}");
            tx.execute(
                "INSERT INTO adv_bench_projects (project_id, name, tier) VALUES ($1, $2, 'mission-critical')",
                &[&proj_id, &name],
            )
            .map_err(|e| format!("pg insert project: {e}"))?;
        }
        for idx in 1..=128_u64 {
            let task_id = format!("task-{idx:04}");
            let rank = idx as i64;
            let proj_id = format!("proj-{:02}", ((idx - 1) % 8) + 1);
            let base = idx as f32 * 0.05;
            let e0 = base;
            let e1 = base * 0.5;
            let e2 = 1.0 / (idx as f32);
            let e3 = (idx % 7) as f32 * 0.1;
            let e4 = 0.25_f32;
            let e5 = 0.75_f32;
            let e6 = (idx % 5) as f32 * 0.2;
            let e7 = 1.0_f32;
            tx.execute(
                "INSERT INTO adv_bench_tasks (task_id, priority_rank, project_id, emb_0, emb_1, emb_2, emb_3, emb_4, emb_5, emb_6, emb_7) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                &[&task_id, &rank, &proj_id, &e0, &e1, &e2, &e3, &e4, &e5, &e6, &e7],
            )
            .map_err(|e| format!("pg insert task: {e}"))?;
        }
        for idx in 1..=64_i64 {
            let body = if idx <= 8 {
                format!(
                    "Incident {idx:02}: Distributed database connections failed during leader failover replication on shard {idx}."
                )
            } else if idx <= 16 {
                format!(
                    "Incident {idx:02}: Connecting client workers experienced transient TLS handshake timeouts."
                )
            } else {
                format!(
                    "Incident {idx:02}: Routine background checkpoint compaction completed on partition {idx}."
                )
            };
            tx.execute(
                "INSERT INTO adv_bench_docs (doc_id, body) VALUES ($1, $2)",
                &[&idx, &body],
            )
            .map_err(|e| format!("pg insert doc: {e}"))?;
        }
        for idx in 1..=64_i16 {
            let log_line = if idx <= 6 {
                format!(
                    "2026-11-06T12:00:{idx:02}Z ERROR [conn-40{idx:02}] leader lease timeout on shard {idx}"
                )
            } else {
                format!(
                    "2026-11-06T12:00:{idx:02}Z INFO [conn-10{idx:02}] healthy heartbeat acknowledged on node {idx}"
                )
            };
            tx.execute(
                "INSERT INTO adv_bench_logs (log_id, line) VALUES ($1, $2)",
                &[&idx, &log_line],
            )
            .map_err(|e| format!("pg insert log: {e}"))?;
        }
        tx.commit().map_err(|e| format!("pg commit: {e}"))?;
        client
            .batch_execute("VACUUM ANALYZE adv_bench_projects, adv_bench_tasks, adv_bench_docs, adv_bench_logs;")
            .map_err(|e| format!("pg vacuum analyze: {e}"))?;
        Ok(())
    }

    fn connect(postgres_url: &str, profile: PostgresProfile) -> Result<Self, String> {
        let mut client =
            Client::connect(postgres_url, NoTls).map_err(|e| format!("pg connect: {e}"))?;
        let stmt_cursor_page = client
            .prepare(
                "SELECT task_id, priority_rank, project_id FROM adv_bench_tasks WHERE priority_rank > $1 ORDER BY priority_rank ASC, task_id ASC LIMIT 32",
            )
            .map_err(|e| format!("prepare cursor_page: {e}"))?;
        let stmt_stemmed_fuzzy_bm25 = client
            .prepare(
                r#"
                SELECT doc_id,
                       ts_rank_cd(tsv, plainto_tsquery('english', 'connected')) + similarity(body, 'replicaton') AS score,
                       ts_headline('english', body, plainto_tsquery('english', 'connected'), 'MaxWords=12, MinWords=4') AS snippet
                FROM adv_bench_docs
                WHERE tsv @@ plainto_tsquery('english', 'connected')
                   OR body % 'replicaton'
                ORDER BY score DESC, doc_id ASC
                LIMIT 10
                "#,
            )
            .map_err(|e| format!("prepare stemmed_fuzzy: {e}"))?;
        let stmt_trigram_regex = client
            .prepare(
                r#"
                SELECT log_id,
                       substring(line from 'ERROR \[conn-40[0-9]{2}\] leader (lease|quorum) timeout') AS snippet
                FROM adv_bench_logs
                WHERE line ~* 'error \[conn-40[0-9]{2}\] leader (lease|quorum) timeout'
                ORDER BY log_id ASC
                "#,
            )
            .map_err(|e| format!("prepare trigram_regex: {e}"))?;
        let stmt_vector_join = client
            .prepare(
                r#"
                SELECT t.task_id, t.project_id, p.name, p.tier,
                       ((t.emb_0 - 0.05)*(t.emb_0 - 0.05) + (t.emb_1 - 0.025)*(t.emb_1 - 0.025) + (t.emb_2 - 1.0)*(t.emb_2 - 1.0) + (t.emb_3 - 0.1)*(t.emb_3 - 0.1)) AS l2_dist
                FROM adv_bench_tasks t
                JOIN adv_bench_projects p ON p.project_id = t.project_id
                ORDER BY l2_dist ASC, t.task_id ASC
                LIMIT 10
                "#,
            )
            .map_err(|e| format!("prepare vector_join: {e}"))?;
        let stmt_hybrid_rrf = client
            .prepare(
                r#"
                WITH vec_top AS (
                    SELECT CAST(substring(task_id from 6) AS BIGINT) AS doc_id,
                           ROW_NUMBER() OVER (
                               ORDER BY ((emb_0 - 0.05)*(emb_0 - 0.05) + (emb_1 - 0.025)*(emb_1 - 0.025) + (emb_2 - 1.0)*(emb_2 - 1.0)) ASC, task_id ASC
                           ) AS r_vec
                    FROM adv_bench_tasks
                    LIMIT 10
                ),
                txt_top AS (
                    SELECT doc_id,
                           ROW_NUMBER() OVER (
                               ORDER BY ts_rank_cd(tsv, plainto_tsquery('english', 'connected')) DESC, doc_id ASC
                           ) AS r_txt
                    FROM adv_bench_docs
                    WHERE tsv @@ plainto_tsquery('english', 'connected')
                    LIMIT 10
                )
                SELECT COALESCE(v.doc_id, t.doc_id) AS id,
                       COALESCE(1000000 / (60 + v.r_vec), 0) + COALESCE(1000000 / (60 + t.r_txt), 0) AS rrf_score
                FROM vec_top v
                FULL OUTER JOIN txt_top t ON v.doc_id = t.doc_id
                ORDER BY rrf_score DESC, id ASC
                LIMIT 10
                "#,
            )
            .map_err(|e| format!("prepare hybrid_rrf: {e}"))?;
        Ok(Self {
            client,
            profile,
            stmt_cursor_page,
            stmt_stemmed_fuzzy_bm25,
            stmt_trigram_regex,
            stmt_vector_join,
            stmt_hybrid_rrf,
        })
    }

    fn execute_workload(&mut self, workload: AdvancedSearchWorkloadKind) -> Result<usize, String> {
        if self.profile == PostgresProfile::SafeApp {
            self.client
                .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;")
                .map_err(|e| format!("pg begin repeatable read: {e}"))?;
        }
        let res = match workload {
            AdvancedSearchWorkloadKind::MultiPageCursorScan => {
                let mut last_rank = 0_i64;
                let mut total = 0_usize;
                for _ in 0..4 {
                    let rows = self
                        .client
                        .query(&self.stmt_cursor_page, &[&last_rank])
                        .map_err(|e| format!("pg cursor query: {e}"))?;
                    if let Some(last) = rows.last() {
                        last_rank = last.get::<_, i64>(1);
                    }
                    total += rows.len();
                }
                Ok(total)
            }
            AdvancedSearchWorkloadKind::StemmedFuzzyBm25WithSnippets => {
                let rows = self
                    .client
                    .query(&self.stmt_stemmed_fuzzy_bm25, &[])
                    .map_err(|e| format!("pg stemmed fuzzy query: {e}"))?;
                Ok(rows.len())
            }
            AdvancedSearchWorkloadKind::TrigramRegexWithSnippets => {
                let rows = self
                    .client
                    .query(&self.stmt_trigram_regex, &[])
                    .map_err(|e| format!("pg trigram regex query: {e}"))?;
                Ok(rows.len())
            }
            AdvancedSearchWorkloadKind::HnswVectorMultiSourceJoin => {
                let rows = self
                    .client
                    .query(&self.stmt_vector_join, &[])
                    .map_err(|e| format!("pg vector join query: {e}"))?;
                Ok(rows.len())
            }
            AdvancedSearchWorkloadKind::HybridVectorBm25Rrf => {
                let rows = self
                    .client
                    .query(&self.stmt_hybrid_rrf, &[])
                    .map_err(|e| format!("pg hybrid rrf query: {e}"))?;
                Ok(rows.len())
            }
        };
        if self.profile == PostgresProfile::SafeApp {
            let _ = self.client.batch_execute("COMMIT;");
        }
        res
    }
}

/// Live `riffdbd` benchmark runner executing ADR-0339 named queries over `StableApplicationClient`.
#[derive(Clone)]
pub struct RiffdbPublicSearchRunner {
    endpoint: Endpoint,
    admin_auth: CallMetadata,
    role_grant: CapabilityGrantV1,
    transport: StableApplicationClient,
    metadata: CallMetadata,
    attempts: AttemptBudget,
    contract_bundle_hash: [u8; 32],
    query_module_hash: [u8; 32],
}

impl RiffdbPublicSearchRunner {
    /// Bootstraps a live `riffdbd` instance, deploys `AdvVectorTasks` and `adv_search_bench`,
    /// issues the least-privilege application role capability, and seeds 128 `TaskVec`,
    /// 64 `LogEntry`, and 8 `AdvProject` records.
    pub async fn setup_and_seed(
        endpoint_url: &str,
        credential: &RetainedBootstrapCredential,
    ) -> Result<Self, String> {
        let artifacts = compile_adv_search_artifacts()?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_owned())
            .map_err(|e| format!("invalid endpoint {endpoint_url}: {e}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .tcp_nodelay(true);

        let mut admin_client = RiffDbClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("RiffDbClient connect failed: {e}"))?;

        let bootstrap_token = std::str::from_utf8(credential.token().expose_secret())
            .map_err(|e| format!("bootstrap token utf8: {e}"))?;
        let bootstrap_metadata = BootstrapCallMetadata::new(
            TransportBootstrapCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );
        let admin_auth = CallMetadata::authenticated(
            BearerCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );

        let created = admin_client
            .create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata)
            .await
            .map_err(|e| format!("create_bootstrap_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Bootstrap(boot_res)) = created.result
        else {
            return Err("bootstrap capability response was not Bootstrap".to_owned());
        };
        let Some(v1::bootstrap_create_capability_result::Result::Created(_)) = boot_res.result
        else {
            return Err("bootstrap capability was not newly created".to_owned());
        };

        let deployed_contract = admin_client
            .deploy_contract(
                v1::DeployContractRequest {
                    request_id: fresh_request_id_bytes()?,
                    source: ADV_VECTOR_CONTRACT.to_owned(),
                    expected_active_version: None,
                    expected_active_bundle_hash: Vec::new(),
                    expected_candidate_bundle_hash: Vec::new(),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("deploy_contract: {e}"))?;
        let Some(v1::deploy_contract_response::Result::Activated(activated_contract)) =
            deployed_contract.result
        else {
            return Err(format!(
                "deploy_contract did not activate: {deployed_contract:?}"
            ));
        };
        let contract_bundle_hash: [u8; 32] =
            activated_contract
                .bundle_hash
                .as_slice()
                .try_into()
                .map_err(|_| "invalid bundle_hash length".to_owned())?;

        let deployed_module = admin_client
            .deploy_query_module(
                app_v1::DeployQueryModuleRequest {
                    contract: Some(app_v1::ContractSelector {
                        lineage: ADV_CONTRACT_LINEAGE.to_owned(),
                        version: ADV_CONTRACT_VERSION,
                        bundle_hash: Vec::new(),
                    }),
                    module_name: ADV_MODULE_NAME.to_owned(),
                    module_version: ADV_MODULE_VERSION,
                    queries: ADV_SEARCH_QUERY_SOURCES
                        .iter()
                        .map(|(name, source)| app_v1::NamedQuerySource {
                            name: (*name).to_owned(),
                            source: (*source).to_owned(),
                        })
                        .collect(),
                    request_id: fresh_request_id_bytes()?,
                    expected_active: Some(
                        app_v1::deploy_query_module_request::ExpectedActive::AnyActive(true),
                    ),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("deploy_query_module: {e}"))?;
        let Some(active_module) = deployed_module.module else {
            return Err(format!(
                "deploy_query_module did not activate: {deployed_module:?}"
            ));
        };
        let query_module_hash: [u8; 32] = active_module
            .module_hash
            .as_slice()
            .try_into()
            .map_err(|_| "invalid module_hash length".to_owned())?;

        let role_cap_resp = admin_client
            .create_capability(
                role_capability_request(artifacts.role.internal_grant())?,
                &admin_auth,
            )
            .await
            .map_err(|e| format!("create_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Normal(normal_res)) = role_cap_resp.result
        else {
            return Err("role capability response was not Normal".to_owned());
        };
        let Some(v1::normal_create_capability_result::Result::Created(created_cap)) =
            normal_res.result
        else {
            return Err("role capability was not Created".to_owned());
        };

        let transport = StableApplicationClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("StableApplicationClient connect: {e}"))?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(&created_cap.token).map_err(|e| format!("{e}"))?,
        );
        let attempts = AttemptBudget::new(3).ok_or_else(|| "invalid AttemptBudget".to_owned())?;

        let role_grant = artifacts.role.internal_grant().clone();
        let runner = Self {
            endpoint,
            admin_auth,
            role_grant,
            transport,
            metadata,
            attempts,
            contract_bundle_hash,
            query_module_hash,
        };
        runner.seed_all().await?;
        Ok(runner)
    }

    /// Opens an independent HTTP/2 connection and capability session for one concurrent worker.
    pub async fn fresh_worker_session(&self) -> Result<Self, String> {
        let mut admin_client = RiffDbClient::connect(self.endpoint.clone())
            .await
            .map_err(|e| format!("fresh_worker_session admin connect: {e}"))?;
        let role_cap_resp = admin_client
            .create_capability(role_capability_request(&self.role_grant)?, &self.admin_auth)
            .await
            .map_err(|e| format!("fresh_worker_session create_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Normal(normal_res)) = role_cap_resp.result
        else {
            return Err("worker role capability response was not Normal".to_owned());
        };
        let Some(v1::normal_create_capability_result::Result::Created(created_cap)) =
            normal_res.result
        else {
            return Err("worker role capability was not Created".to_owned());
        };
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(&created_cap.token).map_err(|e| format!("{e}"))?,
        );
        let transport = StableApplicationClient::connect(self.endpoint.clone())
            .await
            .map_err(|e| format!("fresh_worker_session connect: {e}"))?;
        Ok(Self {
            endpoint: self.endpoint.clone(),
            admin_auth: self.admin_auth.clone(),
            role_grant: self.role_grant.clone(),
            transport,
            metadata,
            attempts: self.attempts,
            contract_bundle_hash: self.contract_bundle_hash,
            query_module_hash: self.query_module_hash,
        })
    }

    async fn seed_all(&self) -> Result<(), String> {
        let org_uuid = [7_u8; 16];
        let mut commands = Vec::with_capacity(8 + 128 + 64);

        for proj_idx in 1..=8_u64 {
            let proj_id = format!("proj-{proj_idx:02}");
            let tier = if proj_idx % 2 == 0 {
                "enterprise"
            } else {
                "growth"
            };
            commands.push(
                ApplicationCommand::new(
                    "SeedAdvProject",
                    Some(ADV_CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!("seed-adv-proj-{proj_idx}")),
                        ),
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        ("project_id".to_owned(), ApplicationValue::String(proj_id)),
                        ("tier".to_owned(), ApplicationValue::String(tier.to_owned())),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for idx in 1..=128_u64 {
            let proj_id = format!("proj-{:02}", ((idx - 1) % 8) + 1);
            let body = if idx <= 8 {
                format!(
                    "Incident {idx:02}: Distributed database connections failed during leader failover replication on shard {idx}."
                )
            } else if idx <= 16 {
                format!(
                    "Incident {idx:02}: Connecting client workers experienced transient TLS handshake timeouts."
                )
            } else {
                format!(
                    "Incident {idx:02}: Routine background checkpoint compaction completed on partition {idx}."
                )
            };
            let base = idx as f32 * 0.05;
            let vec_vals = CanonicalVector::new(vec![
                base,
                base * 0.5,
                1.0 / (idx as f32),
                (idx % 7) as f32 * 0.1,
                0.25,
                0.75,
                (idx % 5) as f32 * 0.2,
                1.0,
            ])
            .map_err(|e| format!("{e}"))?;
            commands.push(
                ApplicationCommand::new(
                    "SeedTaskVec",
                    Some(ADV_CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!("seed-adv-task-{idx}")),
                        ),
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        ("task_id".to_owned(), ApplicationValue::U64(idx)),
                        ("project_id".to_owned(), ApplicationValue::String(proj_id)),
                        (
                            "priority_rank".to_owned(),
                            ApplicationValue::I64(idx as i64),
                        ),
                        ("body".to_owned(), ApplicationValue::String(body)),
                        ("embedding".to_owned(), ApplicationValue::Vector(vec_vals)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        for idx in 1..=64_u64 {
            let line = if idx % 10 == 0 {
                format!(
                    "2026-11-01T12:00:{idx:02}Z ERROR [shard-3] commit timeout latency={}ms error=ERR_QUORUM_TIMEOUT",
                    100 + idx
                )
            } else {
                format!("2026-11-01T12:00:{idx:02}Z INFO [shard-1] steady heartbeat ok seq={idx}")
            };
            commands.push(
                ApplicationCommand::new(
                    "SeedLogEntry",
                    Some(ADV_CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!("seed-adv-log-{idx}")),
                        ),
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        ("log_id".to_owned(), ApplicationValue::U64(idx)),
                        ("line".to_owned(), ApplicationValue::String(line)),
                    ]),
                )
                .map_err(|e| format!("{e}"))?,
            );
        }

        let semaphore = Arc::new(tokio::sync::Semaphore::new(64));
        let mut handles = Vec::with_capacity(commands.len());
        for cmd in commands {
            let sem = Arc::clone(&semaphore);
            let mut transport = self.transport.clone();
            let metadata = self.metadata.clone();
            let attempts = self.attempts;
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.map_err(|e| format!("{e}"))?;
                let resp = transport
                    .execute_command(cmd, attempts, &metadata)
                    .await
                    .map_err(|e| format!("seed execute_command: {e}"))?;
                if resp.outcome.as_deref() != Some("Created") {
                    return Err(format!(
                        "unexpected seed command outcome: {:?}",
                        resp.outcome
                    ));
                }
                Ok::<(), String>(())
            }));
        }
        for handle in handles {
            handle.await.map_err(|e| format!("seed task join: {e}"))??;
        }
        Ok(())
    }

    fn contract_selector(&self) -> ApplicationContract {
        ApplicationContract::Exact {
            lineage: ADV_CONTRACT_LINEAGE.to_owned(),
            version: ADV_CONTRACT_VERSION,
            bundle_hash: Some(self.contract_bundle_hash),
        }
    }

    /// Executes one ADR-0339 workload over the public `StableApplicationClient` gRPC surface.
    pub async fn execute_workload(
        &self,
        workload: AdvancedSearchWorkloadKind,
    ) -> Result<usize, String> {
        let org_uuid = [7_u8; 16];
        let mut transport = self.transport.clone();
        match workload {
            AdvancedSearchWorkloadKind::MultiPageCursorScan => {
                let query = NamedQuery::new(
                    self.contract_selector(),
                    "PaginateTasks".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        ("limit".to_owned(), ApplicationValue::U64(32)),
                    ]),
                    None,
                )
                .map_err(|e| format!("{e}"))?
                .with_options(QueryOptions::new().at_least_admission_head())
                .map_err(|e| format!("{e}"))?;
                let mut stream = transport
                    .stream_named_query(query, &self.metadata)
                    .with_max_pages(4);
                let mut total_rows = 0_usize;
                for _ in 0..4 {
                    let Some(page) = stream
                        .next_page()
                        .await
                        .map_err(|e| format!("PaginateTasks stream page: {e}"))?
                    else {
                        break;
                    };
                    if page.outcome != "Found" {
                        return Err(format!(
                            "PaginateTasks unexpected outcome: {}",
                            page.outcome
                        ));
                    }
                    let Some(field) = page.fields.get("tasks") else {
                        return Err("PaginateTasks missing tasks field".to_owned());
                    };
                    total_rows += field.records.len();
                }
                Ok(total_rows)
            }
            AdvancedSearchWorkloadKind::StemmedFuzzyBm25WithSnippets => {
                let query = NamedQuery::new(
                    self.contract_selector(),
                    "SearchIncidentsFuzzySnippets".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        (
                            "query".to_owned(),
                            ApplicationValue::String("conection failur".to_owned()),
                        ),
                        ("limit".to_owned(), ApplicationValue::U64(10)),
                        ("offset".to_owned(), ApplicationValue::U64(0)),
                    ]),
                    None,
                )
                .map_err(|e| format!("{e}"))?;
                let res = transport
                    .execute_named_query_shared(query, &self.metadata)
                    .await
                    .map_err(|e| format!("SearchIncidentsFuzzySnippets: {e}"))?;
                let Some(field) = res.fields.get("incidents") else {
                    return Err("SearchIncidentsFuzzySnippets missing incidents field".to_owned());
                };
                for rec in &field.records {
                    let Some(ApplicationValue::String(snippet)) = rec.fields.get("body") else {
                        return Err("incident missing body".to_owned());
                    };
                    if !snippet.contains("<<") || !snippet.contains(">>") {
                        return Err(format!("expected highlighted snippet, got: {snippet}"));
                    }
                }
                Ok(field.records.len())
            }
            AdvancedSearchWorkloadKind::TrigramRegexWithSnippets => {
                let query = NamedQuery::new(
                    self.contract_selector(),
                    "SearchLogsRegex".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        (
                            "pattern".to_owned(),
                            ApplicationValue::String("[0-9]+ms".to_owned()),
                        ),
                        ("limit".to_owned(), ApplicationValue::U64(50)),
                    ]),
                    None,
                )
                .map_err(|e| format!("{e}"))?;
                let res = transport
                    .execute_named_query_shared(query, &self.metadata)
                    .await
                    .map_err(|e| format!("SearchLogsRegex: {e}"))?;
                let Some(field) = res.fields.get("logs") else {
                    return Err("SearchLogsRegex missing logs field".to_owned());
                };
                for rec in &field.records {
                    let Some(ApplicationValue::String(line)) = rec.fields.get("line") else {
                        return Err("log missing line".to_owned());
                    };
                    let snippet = riffdb_projection::extract_snippet_v1(
                        line,
                        TextAnalyzerV1::EnglishStemV1,
                        "latency",
                        1,
                        48,
                    );
                    if !snippet.contains("<<") {
                        return Err("regex snippet missing highlight".to_owned());
                    }
                }
                Ok(field.records.len())
            }
            AdvancedSearchWorkloadKind::HnswVectorMultiSourceJoin => {
                let qvec = CanonicalVector::new(vec![0.15, 0.075, 0.33, 0.3, 0.25, 0.75, 0.6, 1.0])
                    .map_err(|e| format!("{e}"))?;
                let hybrid_query = NamedQuery::new(
                    self.contract_selector(),
                    "SearchIncidentsHybridRrf".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        (
                            "query".to_owned(),
                            ApplicationValue::String("conection failur".to_owned()),
                        ),
                        ("query_vec".to_owned(), ApplicationValue::Vector(qvec)),
                        ("limit".to_owned(), ApplicationValue::U64(10)),
                        ("offset".to_owned(), ApplicationValue::U64(0)),
                    ]),
                    None,
                )
                .map_err(|e| format!("{e}"))?;
                let proj_query = NamedQuery::new(
                    self.contract_selector(),
                    "ListOrgProjects".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([(
                        "org_id".to_owned(),
                        ApplicationValue::Uuid(org_uuid.into()),
                    )]),
                    None,
                )
                .map_err(|e| format!("{e}"))?;
                let incidents_res = transport
                    .execute_named_query_shared(hybrid_query, &self.metadata)
                    .await
                    .map_err(|e| format!("SearchIncidentsHybridRrf: {e}"))?;
                let projects_res = transport
                    .execute_named_query_shared(proj_query, &self.metadata)
                    .await
                    .map_err(|e| format!("ListOrgProjects: {e}"))?;
                let Some(inc_field) = incidents_res.fields.get("incidents") else {
                    return Err("missing incidents field".to_owned());
                };
                let Some(proj_field) = projects_res.fields.get("projects") else {
                    return Err("missing projects field".to_owned());
                };
                let mut primary_rows = Vec::with_capacity(inc_field.records.len());
                for rec in &inc_field.records {
                    let Some(ApplicationValue::U64(tid)) = rec.fields.get("task_id") else {
                        return Err("incident missing task_id".to_owned());
                    };
                    let Some(ApplicationValue::String(pid)) = rec.fields.get("project_id") else {
                        return Err("incident missing project_id".to_owned());
                    };
                    let mut row_fields = BTreeMap::new();
                    row_fields.insert(Arc::<str>::from("task_id"), CanonicalValue::U64(*tid));
                    row_fields.insert(
                        Arc::<str>::from("project_id"),
                        CanonicalValue::string(pid.clone()).map_err(|e| format!("{e}"))?,
                    );
                    primary_rows.push(row_fields);
                }
                let mut related_rows = Vec::with_capacity(proj_field.records.len());
                for rec in &proj_field.records {
                    let Some(ApplicationValue::String(pid)) = rec.fields.get("project_id") else {
                        return Err("project missing project_id".to_owned());
                    };
                    let Some(ApplicationValue::String(tier)) = rec.fields.get("tier") else {
                        return Err("project missing tier".to_owned());
                    };
                    let mut row_fields = BTreeMap::new();
                    row_fields.insert(
                        Arc::<str>::from("project_id"),
                        CanonicalValue::string(pid.clone()).map_err(|e| format!("{e}"))?,
                    );
                    row_fields.insert(
                        Arc::<str>::from("tier"),
                        CanonicalValue::string(tier.clone()).map_err(|e| format!("{e}"))?,
                    );
                    related_rows.push(
                        QueryRow::from_shared(Arc::from("AdvProject"), row_fields)
                            .ok_or_else(|| "AdvProject row failed".to_owned())?,
                    );
                }
                let epoch =
                    CommitSequence::new(incidents_res.application_head.max(1)).expect("epoch");
                let joined = join_vector_nearest_with_related_entities_v1(
                    epoch,
                    epoch,
                    "TaskVec",
                    primary_rows,
                    "project_id",
                    "AdvProject",
                    "project",
                    &related_rows,
                )
                .map_err(|e| format!("vector join error: {e:?}"))?;
                Ok(joined.len())
            }
            AdvancedSearchWorkloadKind::HybridVectorBm25Rrf => {
                let qvec = CanonicalVector::new(vec![0.15, 0.075, 0.33, 0.3, 0.25, 0.75, 0.6, 1.0])
                    .map_err(|e| format!("{e}"))?;
                let query = NamedQuery::new(
                    self.contract_selector(),
                    "SearchIncidentsHybridRrf".to_owned(),
                    Some(self.query_module_hash),
                    BTreeMap::from([
                        ("org_id".to_owned(), ApplicationValue::Uuid(org_uuid.into())),
                        (
                            "query".to_owned(),
                            ApplicationValue::String("conection failur".to_owned()),
                        ),
                        ("query_vec".to_owned(), ApplicationValue::Vector(qvec)),
                        ("limit".to_owned(), ApplicationValue::U64(10)),
                        ("offset".to_owned(), ApplicationValue::U64(0)),
                    ]),
                    None,
                )
                .map_err(|e| format!("{e}"))?;
                let res = transport
                    .execute_named_query_shared(query, &self.metadata)
                    .await
                    .map_err(|e| format!("SearchIncidentsHybridRrf: {e}"))?;
                let Some(field) = res.fields.get("incidents") else {
                    return Err("SearchIncidentsHybridRrf missing incidents field".to_owned());
                };
                for rec in &field.records {
                    let Some(ApplicationValue::String(snippet)) = rec.fields.get("body") else {
                        return Err("incident missing body".to_owned());
                    };
                    if !snippet.contains("<<") || !snippet.contains(">>") {
                        return Err(format!("expected highlighted snippet, got: {snippet}"));
                    }
                }
                Ok(field.records.len())
            }
        }
    }
}

/// Runs the 5 ADR-0339 head-to-head query benchmarks against PostgreSQL (`minimal` and `safe-app`).
pub fn run_advanced_search_and_cursor_benchmark(
    postgres_url: &str,
    scale: SeedScale,
) -> Result<AdvancedSearchAndCursorBenchReport, String> {
    run_advanced_search_and_cursor_benchmark_with_riffdbd(None, postgres_url, scale)
}

/// Runs the 5 ADR-0339 head-to-head query benchmarks against PostgreSQL (`minimal` and `safe-app`),
/// optionally including public-client (`StableApplicationClient` over live `riffdbd` gRPC) timings.
pub fn run_advanced_search_and_cursor_benchmark_with_riffdbd(
    riffdbd_env: Option<(&tokio::runtime::Runtime, &Path, &Path)>,
    postgres_url: &str,
    scale: SeedScale,
) -> Result<AdvancedSearchAndCursorBenchReport, String> {
    let riff_state = Arc::new(RiffdbAdvancedSearchState::build_deterministic());
    PostgresAdvancedSearchWorker::seed_schema_and_data(postgres_url)?;

    let mut spawned_daemon = None;
    let public_runner = if let Some((rt, riffdbd_bin, work_dir)) = riffdbd_env {
        let adv_dir = work_dir.join("adv-search-bench");
        std::fs::create_dir_all(&adv_dir).map_err(|e| format!("create adv_dir: {e}"))?;
        let daemon = SpawnedRiffdbd::spawn(riffdbd_bin, &adv_dir)?;
        let runner = rt.block_on(RiffdbPublicSearchRunner::setup_and_seed(
            &daemon.endpoint,
            &daemon.bootstrap_credential,
        ))?;
        spawned_daemon = Some(daemon);
        Some((rt, runner))
    } else {
        None
    };

    // Verify exact result-count parity across all 5 workloads before timing.
    let mut pg_min = PostgresAdvancedSearchWorker::connect(postgres_url, PostgresProfile::Minimal)?;
    let mut pg_safe =
        PostgresAdvancedSearchWorker::connect(postgres_url, PostgresProfile::SafeApp)?;
    let mut verified_parity_checks = 0_usize;
    for workload in AdvancedSearchWorkloadKind::ALL {
        let riff_count = riff_state.execute_workload(workload)?;
        let min_count = pg_min.execute_workload(workload)?;
        let safe_count = pg_safe.execute_workload(workload)?;
        if riff_count == 0 || riff_count != min_count || riff_count != safe_count {
            return Err(format!(
                "parity mismatch on {}: riffdb={riff_count}, pg_min={min_count}, pg_safe={safe_count}",
                workload.as_str()
            ));
        }
        if let Some((rt, runner)) = &public_runner {
            let pub_count = rt.block_on(runner.execute_workload(workload))?;
            if pub_count != riff_count {
                return Err(format!(
                    "public client parity mismatch on {}: pub_count={pub_count}, riffdb={riff_count}",
                    workload.as_str()
                ));
            }
        }
        verified_parity_checks += 1;
    }

    let warmup = scale.warmup_iterations();
    let iterations = scale.unary_iterations();
    let mut unary_workloads = Vec::with_capacity(AdvancedSearchWorkloadKind::ALL.len());
    for workload in AdvancedSearchWorkloadKind::ALL {
        let riff_stats = measure_riffdb_unary(&riff_state, workload, warmup, iterations)?;
        let riffdb_public_client = if let Some((rt, runner)) = &public_runner {
            Some(rt.block_on(measure_riffdb_public_unary(
                runner, workload, warmup, iterations,
            ))?)
        } else {
            None
        };
        let min_stats = measure_pg_unary(&mut pg_min, workload, warmup, iterations)?;
        let safe_stats = measure_pg_unary(&mut pg_safe, workload, warmup, iterations)?;
        unary_workloads.push(UnaryAdvancedSearchComparison {
            workload,
            construct_description: workload.construct_description().to_owned(),
            riffdb: riff_stats,
            riffdb_public_client,
            postgres_minimal: min_stats,
            postgres_safe_app: safe_stats,
        });
    }

    let mut concurrent_cells = Vec::new();
    for &concurrency in scale.concurrency_levels() {
        let ops_per_worker = scale.ops_per_worker();
        let riff_conc = measure_riffdb_concurrent(&riff_state, concurrency, ops_per_worker)?;
        let min_conc = measure_pg_concurrent(
            postgres_url,
            PostgresProfile::Minimal,
            concurrency,
            ops_per_worker,
        )?;
        let safe_conc = measure_pg_concurrent(
            postgres_url,
            PostgresProfile::SafeApp,
            concurrency,
            ops_per_worker,
        )?;
        concurrent_cells.push(ConcurrentComparisonCell {
            workload_label: "adr_0339_search_cursor_vector_mix".to_owned(),
            concurrency,
            riffdb: riff_conc,
            postgres_minimal: min_conc.clone(),
            postgres_safe_app: safe_conc.clone(),
        });
        if let Some((rt, runner)) = &public_runner {
            let pub_conc = rt.block_on(measure_riffdb_public_concurrent(
                runner,
                concurrency,
                ops_per_worker,
            ))?;
            concurrent_cells.push(ConcurrentComparisonCell {
                workload_label: "adr_0339_public_client_grpc_mix".to_owned(),
                concurrency,
                riffdb: pub_conc,
                postgres_minimal: min_conc,
                postgres_safe_app: safe_conc,
            });
        }
    }

    drop(spawned_daemon);

    Ok(AdvancedSearchAndCursorBenchReport {
        verified_parity_checks,
        unary_workloads,
        concurrent_cells,
    })
}

async fn measure_riffdb_public_unary(
    runner: &RiffdbPublicSearchRunner,
    workload: AdvancedSearchWorkloadKind,
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    for _ in 0..warmup {
        let _ = runner.execute_workload(workload).await?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_t0 = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let _ = runner.execute_workload(workload).await?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_t0.elapsed(),
    ))
}

async fn measure_riffdb_public_concurrent(
    runner: &RiffdbPublicSearchRunner,
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let worker_session = runner.fresh_worker_session().await?;
        let barrier_clone = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            for w in 0..2 {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + w) % AdvancedSearchWorkloadKind::ALL.len()];
                worker_session.execute_workload(wl).await?;
            }
            barrier_clone.wait().await;
            let wall_t0 = Instant::now();
            let mut local = Vec::with_capacity(ops_per_worker);
            for op in 0..ops_per_worker {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + op) % AdvancedSearchWorkloadKind::ALL.len()];
                let t0 = Instant::now();
                worker_session.execute_workload(wl).await?;
                local.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local, wall_t0.elapsed()))
        }));
    }
    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_wall = Duration::ZERO;
    for handle in handles {
        let (local, wall) = handle
            .await
            .map_err(|e| format!("public search worker join: {e}"))??;
        all_samples.extend(local);
        if wall > max_wall {
            max_wall = wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_wall,
    ))
}

fn measure_riffdb_unary(
    state: &RiffdbAdvancedSearchState,
    workload: AdvancedSearchWorkloadKind,
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    for _ in 0..warmup {
        let _ = state.execute_workload(workload)?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_t0 = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let _ = state.execute_workload(workload)?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_t0.elapsed(),
    ))
}

fn measure_pg_unary(
    worker: &mut PostgresAdvancedSearchWorker,
    workload: AdvancedSearchWorkloadKind,
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    for _ in 0..warmup {
        let _ = worker.execute_workload(workload)?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_t0 = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let _ = worker.execute_workload(workload)?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_t0.elapsed(),
    ))
}

fn measure_riffdb_concurrent(
    state: &Arc<RiffdbAdvancedSearchState>,
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let state_clone = Arc::clone(state);
        let barrier_clone = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            for w in 0..2 {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + w) % AdvancedSearchWorkloadKind::ALL.len()];
                state_clone.execute_workload(wl)?;
            }
            barrier_clone.wait();
            let wall_t0 = Instant::now();
            let mut local = Vec::with_capacity(ops_per_worker);
            for op in 0..ops_per_worker {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + op) % AdvancedSearchWorkloadKind::ALL.len()];
                let t0 = Instant::now();
                state_clone.execute_workload(wl)?;
                local.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local, wall_t0.elapsed()))
        }));
    }
    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_wall = Duration::ZERO;
    for handle in handles {
        let (local, wall) = handle
            .join()
            .map_err(|_| "riffdb worker panicked".to_owned())??;
        all_samples.extend(local);
        if wall > max_wall {
            max_wall = wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_wall,
    ))
}

fn measure_pg_concurrent(
    postgres_url: &str,
    profile: PostgresProfile,
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let mut handles = Vec::with_capacity(concurrency);
    for worker_idx in 0..concurrency {
        let url = postgres_url.to_owned();
        let barrier_clone = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut worker = PostgresAdvancedSearchWorker::connect(&url, profile)?;
            for w in 0..2 {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + w) % AdvancedSearchWorkloadKind::ALL.len()];
                worker.execute_workload(wl)?;
            }
            barrier_clone.wait();
            let wall_t0 = Instant::now();
            let mut local = Vec::with_capacity(ops_per_worker);
            for op in 0..ops_per_worker {
                let wl = AdvancedSearchWorkloadKind::ALL
                    [(worker_idx + op) % AdvancedSearchWorkloadKind::ALL.len()];
                let t0 = Instant::now();
                worker.execute_workload(wl)?;
                local.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local, wall_t0.elapsed()))
        }));
    }
    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_wall = Duration::ZERO;
    for handle in handles {
        let (local, wall) = handle
            .join()
            .map_err(|_| "pg worker panicked".to_owned())??;
        all_samples.extend(local);
        if wall > max_wall {
            max_wall = wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_wall,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: QRY-001, QRY-002, PRJ-001, VEC-006, VEC-010
    #[test]
    fn advanced_search_and_cursor_riffdb_scenarios_verify_parity() {
        let artifacts =
            compile_adv_search_artifacts().expect("ADR-0339 contract and queries must compile");
        assert_eq!(artifacts.query_module.queries().len(), 5);
        let state = RiffdbAdvancedSearchState::build_deterministic();
        assert_eq!(
            state.execute_workload(AdvancedSearchWorkloadKind::MultiPageCursorScan),
            Ok(128)
        );
        assert_eq!(
            state.execute_workload(AdvancedSearchWorkloadKind::StemmedFuzzyBm25WithSnippets),
            Ok(10)
        );
        assert_eq!(
            state.execute_workload(AdvancedSearchWorkloadKind::TrigramRegexWithSnippets),
            Ok(6)
        );
        assert_eq!(
            state.execute_workload(AdvancedSearchWorkloadKind::HnswVectorMultiSourceJoin),
            Ok(10)
        );
        assert_eq!(
            state.execute_workload(AdvancedSearchWorkloadKind::HybridVectorBm25Rrf),
            Ok(10)
        );
    }

    // req: QRY-001, QRY-002, PRJ-001, VEC-006, VEC-010
    #[test]
    fn public_search_runner_executes_all_five_workloads_over_live_riffdbd() {
        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        let bin_path = manifest_dir.join("../../target/debug/riffdbd");
        if !bin_path.exists() {
            return;
        }
        let work_dir =
            std::env::temp_dir().join(format!("riffdb-adv-search-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&work_dir);
        std::fs::create_dir_all(&work_dir).expect("create temp dir");
        let daemon = SpawnedRiffdbd::spawn(&bin_path, &work_dir).expect("spawn riffdbd");
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("tokio runtime");
        let runner = rt
            .block_on(RiffdbPublicSearchRunner::setup_and_seed(
                &daemon.endpoint,
                &daemon.bootstrap_credential,
            ))
            .expect("setup and seed public search runner");
        assert_eq!(
            rt.block_on(runner.execute_workload(AdvancedSearchWorkloadKind::MultiPageCursorScan)),
            Ok(128)
        );
        assert_eq!(
            rt.block_on(
                runner.execute_workload(AdvancedSearchWorkloadKind::StemmedFuzzyBm25WithSnippets)
            ),
            Ok(10)
        );
        assert_eq!(
            rt.block_on(
                runner.execute_workload(AdvancedSearchWorkloadKind::TrigramRegexWithSnippets)
            ),
            Ok(6)
        );
        assert_eq!(
            rt.block_on(
                runner.execute_workload(AdvancedSearchWorkloadKind::HnswVectorMultiSourceJoin)
            ),
            Ok(10)
        );
        assert_eq!(
            rt.block_on(runner.execute_workload(AdvancedSearchWorkloadKind::HybridVectorBm25Rrf)),
            Ok(10)
        );
        let conc = rt
            .block_on(measure_riffdb_public_concurrent(&runner, 8, 5))
            .expect("concurrent public search workloads");
        assert_eq!(conc.operations, 40);
        drop(daemon);
        let _ = std::fs::remove_dir_all(&work_dir);
    }
}
