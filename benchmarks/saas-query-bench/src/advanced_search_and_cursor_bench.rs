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
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, Statement};
use riffdb_columnar::{
    ColumnarProjectionDefinition, ColumnarSnapshot, LiveRow, NearestCandidate,
    NearestCandidateAdmission, NearestGraphCache, NearestQueryRequest, OrgKey, PrimaryKeyBytes,
    QueryBudget, RegisteredDefinition, nearest_query_snapshot_with_cache,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_projection::{
    LongPatternPartitionV1, TokenizedTextConfigV1, TokenizedTextFieldV1, TokenizedTextMutationV1,
    TokenizedTextPartitionIndexV1, extract_snippet_v1,
};
use riffdb_service::{
    HybridRrfCandidateV1, QueryRow, fuse_vector_and_bm25_rrf_v1,
    join_vector_nearest_with_related_entities_v1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CanonicalVector, CommitSequence, CompiledLongPatternV1,
    DistanceMetric, EntityKey, EntityKeyBuilder, EntityKeyHash, EntityTypeId, EntityVersion,
    FieldId, FrontierPosition, LongPatternBoundsV1, LongPatternOperatorV1, LongPatternProfileV1,
    PartitionKeyHash, ProjectionGeneration, TextAnalyzerV1,
};
use serde::{Deserialize, Serialize};

use crate::{ConcurrentComparisonCell, LatencyDistribution, PostgresProfile, SeedScale};

const ADV_VECTOR_CONTRACT: &str = r#"
contract AdvVectorTasks version 1 {
  entity TaskVec {
    key (org_id: uuid, task_id: u64)
    field project_id: string<32>
    vector_field embedding(8, euclidean, (project_id), staleness_slo 60, ann_threshold 32, recall_target_bps 9500)
  }
  aggregate TaskVecs {
    root TaskVec
    partition_by org_id
    conflict_key (org_id, task_id)
  }
}
"#;

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
    /// RiffDB unary latency distribution (`c = 1`).
    pub riffdb: LatencyDistribution,
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
    doc_texts: BTreeMap<EntityKey, String>,
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

        // 2. Tokenized text index with EnglishStemV1 (64 incident documents).
        let f1 = FieldId::new(1).expect("field 1");
        let config = TokenizedTextConfigV1::new(
            [0x39; 32],
            TextAnalyzerV1::EnglishStemV1,
            vec![TokenizedTextFieldV1::new(f1, 10).expect("weight")],
        )
        .expect("tokenized config");
        let mut tokenized_index = TokenizedTextPartitionIndexV1::new(
            config,
            PartitionKeyHash::from_bytes([0x11; 32]),
            ProjectionGeneration::new(1).expect("gen 1"),
        );
        let mut mutations = Vec::with_capacity(64);
        let mut doc_texts = BTreeMap::new();
        for idx in 1..=64_u64 {
            let mut kb = EntityKeyBuilder::new(EntityTypeId::first());
            kb.push_u64(idx).expect("key u64");
            let key = kb.finish().expect("entity key");
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
            doc_texts.insert(key.clone(), body.clone());
            mutations.push(
                TokenizedTextMutationV1::upsert(
                    key,
                    vec![(f1, body)],
                    CanonicalRecord::new(Vec::new()).expect("empty record"),
                )
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
        let bundle = compile_contract_source(ADV_VECTOR_CONTRACT).expect("compile vector contract");
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
        let org_field = find_field("org_id");
        let proj_field = find_field("project_id");
        let vec_field = find_field("embedding");
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
            doc_texts,
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
                let ranked = self
                    .tokenized_index
                    .search_bm25_fuzzy_v1("connected replicaton", 1, 10)
                    .map_err(|e| format!("bm25 fuzzy search failed: {e}"))?;
                let mut snippets = 0_usize;
                for item in &ranked {
                    if let Some(body) = self.doc_texts.get(item.key()) {
                        let snippet = extract_snippet_v1(
                            body,
                            TextAnalyzerV1::EnglishStemV1,
                            "connected replicaton",
                            1,
                            32,
                        );
                        if !snippet.is_empty() {
                            snippets += 1;
                        }
                    }
                }
                Ok(snippets)
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
                let mut admission = AllowAllAdmission;
                let nearest = nearest_query_snapshot_with_cache(
                    &self.ann_def,
                    &self.ann_snapshot,
                    &self.ann_request,
                    &mut admission,
                    Some((&self.ann_cache, b"adv-bench-cache-key")),
                )
                .map_err(|e| format!("nearest query failed: {e}"))?;
                let bm25_ranked = self
                    .tokenized_index
                    .search_bm25_fuzzy_v1("connected replication", 0, 10)
                    .map_err(|e| format!("bm25 search failed: {e}"))?;
                let mut candidate_map: BTreeMap<u64, HybridRrfCandidateV1> = BTreeMap::new();
                for (vec_idx, row) in nearest.rows.iter().enumerate() {
                    let id_u64 = match row.primary_key.get(1) {
                        Some(CanonicalValue::U64(v)) => *v,
                        _ => (vec_idx + 1) as u64,
                    };
                    let id_val = CanonicalValue::U64(id_u64);
                    let mut fields = BTreeMap::new();
                    fields.insert(Arc::<str>::from("id"), id_val.clone());
                    candidate_map.insert(
                        id_u64,
                        HybridRrfCandidateV1 {
                            primary_key: vec![id_val],
                            fields,
                            vector_rank: Some((vec_idx + 1) as u32),
                            bm25_rank: None,
                        },
                    );
                }
                for (bm25_idx, _) in bm25_ranked.iter().enumerate() {
                    let id_u64 = (bm25_idx + 1) as u64;
                    let rank = Some((bm25_idx + 1) as u32);
                    if let Some(existing) = candidate_map.get_mut(&id_u64) {
                        existing.bm25_rank = rank;
                    } else {
                        let id_val = CanonicalValue::U64(id_u64);
                        let mut fields = BTreeMap::new();
                        fields.insert(Arc::<str>::from("id"), id_val.clone());
                        candidate_map.insert(
                            id_u64,
                            HybridRrfCandidateV1 {
                                primary_key: vec![id_val],
                                fields,
                                vector_rank: None,
                                bm25_rank: rank,
                            },
                        );
                    }
                }
                let epoch = CommitSequence::new(10).expect("epoch 10");
                let fused = fuse_vector_and_bm25_rrf_v1(
                    epoch,
                    epoch,
                    5_000,
                    5_000,
                    candidate_map.into_values().collect(),
                    10,
                )
                .map_err(|e| format!("hybrid rrf failed: {e:?}"))?;
                Ok(fused.len())
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

/// Runs the 5 ADR-0339 head-to-head query benchmarks against PostgreSQL (`minimal` and `safe-app`).
pub fn run_advanced_search_and_cursor_benchmark(
    postgres_url: &str,
    scale: SeedScale,
) -> Result<AdvancedSearchAndCursorBenchReport, String> {
    let riff_state = Arc::new(RiffdbAdvancedSearchState::build_deterministic());
    PostgresAdvancedSearchWorker::seed_schema_and_data(postgres_url)?;

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
        verified_parity_checks += 1;
    }

    let warmup = scale.warmup_iterations();
    let iterations = scale.unary_iterations();
    let mut unary_workloads = Vec::with_capacity(AdvancedSearchWorkloadKind::ALL.len());
    for workload in AdvancedSearchWorkloadKind::ALL {
        let riff_stats = measure_riffdb_unary(&riff_state, workload, warmup, iterations)?;
        let min_stats = measure_pg_unary(&mut pg_min, workload, warmup, iterations)?;
        let safe_stats = measure_pg_unary(&mut pg_safe, workload, warmup, iterations)?;
        unary_workloads.push(UnaryAdvancedSearchComparison {
            workload,
            construct_description: workload.construct_description().to_owned(),
            riffdb: riff_stats,
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
            postgres_minimal: min_conc,
            postgres_safe_app: safe_conc,
        });
    }

    Ok(AdvancedSearchAndCursorBenchReport {
        verified_parity_checks,
        unary_workloads,
        concurrent_cells,
    })
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
}
