//! Compiler-owned tokenized text plan lowering (ADR-0173).

use std::num::NonZeroU32;

use riffdb_query_ir::{
    OperationalQueryFamilyV1, SymbolicCatalog, TokenizedMatchKindV1, TokenizedRankingV1,
    TokenizedTextPlanFieldV1, TokenizedTextPlanV1,
};
use riffdb_riffql_syntax::{
    Cardinality, Document, RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1, Span, TokenizedMatchKind,
    TypeReference,
};
use riffdb_types::{
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
};

use crate::{PlannerDiagnosticCode, PlannerDiagnostics};

/// Complete compiled named boolean tokenized operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledTokenizedTextQueryV1 {
    /// Provider plan with no runtime structural choices.
    pub plan: TokenizedTextPlanV1,
    /// Existing typed selection, partition, policy, and window metadata.
    pub metadata: OperationalQueryFamilyV1,
    /// Typed bounded string parameter.
    pub query_parameter: String,
}

/// Compiles exactly one safe tokenized match clause.
pub fn compile_tokenized_text_query_v1(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<CompiledTokenizedTextQueryV1, PlannerDiagnostics> {
    if document.language_version != RIFFQL_LANGUAGE_VERSION_TOKENIZED_TEXT_V1
        || document.body.bindings.len() != 1
        || !document.body.aggregates.is_empty()
    {
        return Err(diagnostic(
            document
                .name
                .as_ref()
                .map_or(Span { start: 0, end: 0 }, |name| name.span),
            "tokenized query requires one bounded many binding",
        ));
    }
    let binding = &document.body.bindings[0];
    let clause = binding.tokenized_match.as_ref().ok_or_else(|| {
        diagnostic(
            binding.cardinality.span,
            "tokenized query is missing its compiler-owned match clause",
        )
    })?;
    if binding.cardinality.value != Cardinality::Many
        || binding.nearest.is_some()
        || binding.take.is_none()
    {
        return Err(diagnostic(
            binding.cardinality.span,
            "tokenized query requires a bounded many binding without nearest",
        ));
    }
    let entity = catalog
        .entity(binding.entity.value.as_str())
        .ok_or_else(|| diagnostic(binding.entity.span, "tokenized query entity is unknown"))?;
    let index = entity
        .text_index(clause.index.value.as_str())
        .ok_or_else(|| diagnostic(clause.index.span, "tokenized text index is unknown"))?;
    let parameter = document
        .parameters
        .iter()
        .find(|parameter| parameter.name.value == clause.query.value)
        .ok_or_else(|| diagnostic(clause.query.span, "tokenized query parameter is unknown"))?;
    let TypeReference::Named(parameter_type) = &parameter.ty.value else {
        return Err(diagnostic(
            parameter.ty.span,
            "tokenized query parameter must be a bounded string field type",
        ));
    };
    let segments = &parameter_type.0;
    if segments.len() != 2 {
        return Err(diagnostic(
            parameter.ty.span,
            "tokenized query parameter must name Entity.field",
        ));
    }
    let parameter_entity = catalog
        .entity(segments[0].value.as_str())
        .and_then(|entity| entity.field(segments[1].value.as_str()))
        .ok_or_else(|| {
            diagnostic(
                parameter.ty.span,
                "tokenized query parameter type is unknown",
            )
        })?;
    if parameter_entity.value_type().tag() != riffdb_contract_ir::ValueTypeTag::String {
        return Err(diagnostic(
            parameter.ty.span,
            "tokenized query parameter must be a bounded string",
        ));
    }
    let kind = match clause.kind.value {
        TokenizedMatchKind::Conjunction => TokenizedMatchKindV1::Conjunction,
        TokenizedMatchKind::Disjunction => TokenizedMatchKindV1::Disjunction,
        TokenizedMatchKind::Phrase => TokenizedMatchKindV1::Phrase,
        TokenizedMatchKind::Proximity(distance) => TokenizedMatchKindV1::Proximity(distance),
    };
    let ranking = match clause.ranking {
        riffdb_riffql_syntax::TokenizedRanking::Boolean => TokenizedRankingV1::Boolean,
        riffdb_riffql_syntax::TokenizedRanking::RiffBm25V1 => TokenizedRankingV1::RiffBm25V1,
    };
    let policy_mode = if catalog
        .row_policies()
        .any(|policy| policy.entity() == entity.name())
    {
        ProjectionProviderPolicyModeV1::BoundedRowAdmission
    } else {
        ProjectionProviderPolicyModeV1::PartitionAligned
    };
    let descriptor = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::TokenizedText,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        policy_mode,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: index.max_candidates(),
            max_output_rows: index.max_results(),
            max_measures: 0,
            max_input_bytes: 65_535,
            max_work_units: u64::from(index.max_terms())
                .saturating_mul(u64::from(index.max_candidates())),
            max_state_bytes_per_row: 4_194_304,
            max_diagnostic_bytes: 4_096,
            retained_epochs: 8,
            max_catchup_lag: 100,
            max_epoch_lease_steps: 1_024,
        },
        ProjectionProviderStateIdentityV1::new(
            NonZeroU32::new(1).expect("state version is nonzero"),
            TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
        ),
    )
    .map_err(|_| diagnostic(clause.span, "tokenized provider descriptor is inconsistent"))?;
    let fields = index
        .source_fields()
        .iter()
        .map(|(field, weight)| TokenizedTextPlanFieldV1::new(*field, *weight))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| diagnostic(clause.span, "tokenized source weight is invalid"))?;
    let plan = TokenizedTextPlanV1::new(
        entity.internal_id(),
        index.id(),
        index.identity(),
        index.analyzer(),
        fields,
        kind,
        ranking,
        index.max_terms(),
        index.max_candidates(),
        index.max_results(),
        descriptor,
        clause.span,
    )
    .map_err(|_| diagnostic(clause.span, "tokenized text plan is inconsistent"))?;
    let mut metadata_document = document.clone();
    metadata_document.body.bindings[0].tokenized_match = None;
    metadata_document.language_version =
        riffdb_riffql_syntax::document_query_shape_language_version(&metadata_document);
    let metadata_catalog = catalog
        .with_tokenized_metadata_index(entity.name(), index.name())
        .ok_or_else(|| diagnostic(clause.index.span, "tokenized metadata binding is invalid"))?;
    let metadata = crate::compile_operational_query_family(&metadata_document, &metadata_catalog)?;
    Ok(CompiledTokenizedTextQueryV1 {
        plan,
        metadata,
        query_parameter: clause.query.value.as_str().to_owned(),
    })
}

fn diagnostic(span: Span, summary: &'static str) -> PlannerDiagnostics {
    crate::one(
        PlannerDiagnosticCode::ExactTextProvider,
        span,
        Vec::new(),
        summary,
        None,
    )
}
