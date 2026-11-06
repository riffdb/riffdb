//! Cryptographic `CursorToken` continuation helpers for projected-table,
//! projected-vector, exact-predicate, and exact-text named queries (`ADR-0339`).

use std::collections::BTreeMap;
use std::num::NonZeroU16;
use std::sync::Arc;

use riffdb_errors::{ApplicationErrorCode, PublicError, ValidationCode};
use riffdb_query_executor::{
    MAX_QUERY_SCANNED_ROWS, QueryOwnedSnapshot, QueryParameters, QueryResultValue, QueryRow,
};
use riffdb_query_ir::{QueryAccessProgramV1, QueryRowLimit};
use riffdb_types::{
    CanonicalValue, CommitSequence, FrontierPosition, ProjectionGeneration, QueryModuleHash,
    QueryPlanHash, ServiceOperationV1,
};

use crate::symbolic_query::{
    application_validation_failure, execution_failure, query_cursor_parameter_hash,
    validation_failure,
};
use crate::{
    CursorAccessError, CursorContractIdentity, CursorPublicationGuard, CursorToken,
    ExecuteSymbolicQueryResult, InternalDefect, QueryConsistencyV1, QueryCursorLookup,
    RequestContext, RiffDbServiceInner, ServiceFailure, ServiceResult, ServiceTelemetryEvent,
    SymbolicResultField, TokenizedTextCursorState, VectorProjectionResult,
};

pub(crate) type ResolvedOffsetCursor = (
    QueryCursorLookup,
    Option<Arc<TokenizedTextCursorState>>,
    u32,
    NonZeroU16,
    Option<u64>,
);

#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_exact_provider_cursor(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    program: &QueryAccessProgramV1,
    module_hash: QueryModuleHash,
    plan_hash: QueryPlanHash,
    parameters: &QueryParameters,
    cursor: Option<CursorToken>,
    result_ceiling: u32,
    mut offset: u32,
    mut limit: NonZeroU16,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ResolvedOffsetCursor> {
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        Some(module_hash),
        plan_hash,
        query_cursor_parameter_hash(program, parameters)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let prior = match cursor {
        Some(token) => match service.cursors.resolve_tokenized_text(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                ));
            }
            Err(CursorAccessError::Unavailable) => {
                return Err(PublicError::storage_unavailable().into());
            }
        },
        None => None,
    };
    let resolved_head = if let Some(prior_state) = prior.as_deref() {
        if prior_state.result_ceiling() != result_ceiling
            || minimum_application_head
                .is_some_and(|floor| floor > prior_state.minimum_application_head())
            || consistency.is_some_and(|value| value != QueryConsistencyV1::AdmissionHead)
        {
            return Err(application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::CursorInvalid,
            ));
        }
        offset = prior_state.next_offset();
        let remaining = result_ceiling
            .checked_sub(offset)
            .and_then(|rem| u16::try_from(rem.min(u32::from(u16::MAX))).ok())
            .and_then(NonZeroU16::new)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
        limit = NonZeroU16::new(limit.get().min(remaining.get()))
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?;
        Some(prior_state.minimum_application_head())
    } else {
        minimum_application_head
    };
    Ok((cursor_lookup, prior, offset, limit, resolved_head))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn register_exact_provider_cursor<'a>(
    service: &'a RiffDbServiceInner,
    context: &RequestContext,
    cursor_lookup: QueryCursorLookup,
    prior: Option<&TokenizedTextCursorState>,
    offset: u32,
    returned_rows: usize,
    exact_total: u64,
    result_ceiling: u32,
    max_offset: u32,
    epoch: CommitSequence,
    generation: ProjectionGeneration,
    history_incarnation: u64,
    provider_digest: [u8; 32],
    minimum_application_head: Option<u64>,
) -> Result<Option<CursorPublicationGuard<'a>>, ServiceFailure> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    if prior.is_some_and(|prior_state| {
        prior_state.epoch() != epoch
            || prior_state.generation() != generation
            || prior_state.history_incarnation() != history_incarnation
            || prior_state.statistics_identity() != provider_digest
    }) {
        return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    }
    let returned = u32::try_from(returned_rows)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let next_offset = offset
        .checked_add(returned)
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let has_more = returned > 0
        && u64::from(next_offset) < exact_total
        && next_offset < result_ceiling
        && next_offset <= max_offset;
    if !has_more {
        return Ok(None);
    }
    let floor = minimum_application_head.unwrap_or_else(|| epoch.get());
    match service.cursors.register_tokenized_text_unpublished(
        context.principal().principal_id(),
        cursor_lookup,
        TokenizedTextCursorState::new(
            next_offset,
            epoch,
            generation,
            floor,
            history_incarnation,
            provider_digest,
            result_ceiling,
        ),
    ) {
        Ok(guard) => {
            if guard.capacity_evicted() {
                service
                    .providers
                    .telemetry
                    .record(ServiceTelemetryEvent::CursorEvicted);
            }
            Ok(Some(guard))
        }
        Err(_) => Err(PublicError::storage_unavailable().into()),
    }
}

pub(crate) type ResolvedVectorCursor = (
    QueryCursorLookup,
    Option<Arc<TokenizedTextCursorState>>,
    u32,
    u32,
    Option<u64>,
);

#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_projected_vector_cursor(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    program: &QueryAccessProgramV1,
    module_hash: QueryModuleHash,
    plan_hash: QueryPlanHash,
    parameters: &QueryParameters,
    cursor: Option<CursorToken>,
    k: u32,
    maximum_k: u32,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ResolvedVectorCursor> {
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        Some(module_hash),
        plan_hash,
        query_cursor_parameter_hash(program, parameters)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let prior = match cursor {
        Some(token) => match service.cursors.resolve_tokenized_text(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                ));
            }
            Err(CursorAccessError::Unavailable) => {
                return Err(PublicError::storage_unavailable().into());
            }
        },
        None => None,
    };
    let (prior_offset, resolved_head) = if let Some(prior_state) = prior.as_deref() {
        if prior_state.result_ceiling() != maximum_k
            || prior_state.next_offset() >= maximum_k
            || minimum_application_head
                .is_some_and(|floor| floor > prior_state.minimum_application_head())
            || consistency.is_some_and(|value| value != QueryConsistencyV1::AdmissionHead)
        {
            return Err(application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::CursorInvalid,
            ));
        }
        (
            prior_state.next_offset(),
            Some(prior_state.minimum_application_head()),
        )
    } else {
        (0, minimum_application_head)
    };
    let fetch_k = prior_offset
        .saturating_add(k)
        .saturating_add(1)
        .min(maximum_k);
    Ok((cursor_lookup, prior, prior_offset, fetch_k, resolved_head))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn paginate_projected_vector_result<'a>(
    service: &'a RiffDbServiceInner,
    context: &RequestContext,
    observed: VectorProjectionResult,
    cursor_lookup: QueryCursorLookup,
    prior: Option<&TokenizedTextCursorState>,
    prior_offset: u32,
    k: u32,
    maximum_k: u32,
    minimum_application_head: Option<u64>,
    plan_hash: QueryPlanHash,
) -> Result<(VectorProjectionResult, Option<CursorPublicationGuard<'a>>), ServiceFailure> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let (mut nearest, frontier, head) = observed.into_parts();
    let FrontierPosition::AppliedThrough(epoch) = frontier.position() else {
        return Err(service.internal_failure(OPERATION, InternalDefect::ProofMismatch));
    };
    let history_incarnation = frontier.history_incarnation();
    let digest = *plan_hash.as_bytes();
    if prior.is_some_and(|prior_state| {
        prior_state.epoch() != epoch
            || prior_state.history_incarnation() != history_incarnation
            || prior_state.statistics_identity() != digest
    }) {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }
    let offset_usize = usize::try_from(prior_offset)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let k_usize = usize::try_from(k)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let total_fetched = nearest.rows.len();
    let page_rows: Vec<_> = nearest
        .rows
        .into_iter()
        .skip(offset_usize)
        .take(k_usize)
        .collect();
    let returned = u32::try_from(page_rows.len())
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let next_offset = prior_offset.saturating_add(returned);
    let has_more = returned > 0
        && total_fetched > offset_usize.saturating_add(page_rows.len())
        && next_offset < maximum_k;
    nearest.rows = page_rows;
    let cursor_guard = if has_more {
        let floor = minimum_application_head.unwrap_or_else(|| epoch.get());
        match service.cursors.register_tokenized_text_unpublished(
            context.principal().principal_id(),
            cursor_lookup,
            TokenizedTextCursorState::new(
                next_offset,
                epoch,
                ProjectionGeneration::first(),
                floor,
                history_incarnation,
                digest,
                maximum_k,
            ),
        ) {
            Ok(guard) => {
                if guard.capacity_evicted() {
                    service
                        .providers
                        .telemetry
                        .record(ServiceTelemetryEvent::CursorEvicted);
                }
                Some(guard)
            }
            Err(_) => return Err(PublicError::storage_unavailable().into()),
        }
    } else {
        None
    };
    Ok((
        VectorProjectionResult::new(nearest, frontier, head),
        cursor_guard,
    ))
}

pub(crate) type ResolvedTableCursor = (
    QueryCursorLookup,
    Option<Arc<TokenizedTextCursorState>>,
    Option<u64>,
);

#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_projected_table_cursor(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    program: &QueryAccessProgramV1,
    module_hash: QueryModuleHash,
    plan_hash: QueryPlanHash,
    parameters: &QueryParameters,
    cursor: Option<CursorToken>,
    minimum_application_head: Option<u64>,
    consistency: Option<QueryConsistencyV1>,
) -> ServiceResult<ResolvedTableCursor> {
    let cursor_lookup = QueryCursorLookup::new(
        CursorContractIdentity::new(
            program.contract().lineage().clone(),
            program.contract().version(),
            program.contract().bundle_hash(),
        ),
        Some(module_hash),
        plan_hash,
        query_cursor_parameter_hash(program, parameters)
            .ok_or_else(|| validation_failure(ValidationCode::InvalidValue))?,
        context.principal().capability_id(),
        context.principal().capability_revision(),
    );
    let prior = match cursor {
        Some(token) => match service.cursors.resolve_tokenized_text(
            token,
            context.principal().principal_id(),
            &cursor_lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(application_validation_failure(
                    ValidationCode::InvalidValue,
                    ApplicationErrorCode::CursorInvalid,
                ));
            }
            Err(CursorAccessError::Unavailable) => {
                return Err(PublicError::storage_unavailable().into());
            }
        },
        None => None,
    };
    let resolved_head = if let Some(prior_state) = prior.as_deref() {
        if minimum_application_head
            .is_some_and(|floor| floor > prior_state.minimum_application_head())
            || consistency.is_some_and(|value| value != QueryConsistencyV1::AdmissionHead)
        {
            return Err(application_validation_failure(
                ValidationCode::InvalidValue,
                ApplicationErrorCode::CursorInvalid,
            ));
        }
        Some(prior_state.minimum_application_head())
    } else {
        minimum_application_head
    };
    Ok((cursor_lookup, prior, resolved_head))
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_projected_table_page<'a>(
    service: &'a RiffDbServiceInner,
    context: &RequestContext,
    program: &QueryAccessProgramV1,
    parameters: &QueryParameters,
    candidate_rows: Vec<QueryRow>,
    application_head: u64,
    epoch_key: String,
    epoch: u64,
    cursor_lookup: QueryCursorLookup,
    prior: Option<&TokenizedTextCursorState>,
    plan_hash: QueryPlanHash,
) -> Result<
    (
        QueryOwnedSnapshot,
        u32,
        u32,
        Option<CursorPublicationGuard<'a>>,
    ),
    ServiceFailure,
> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::ExecuteQuery;
    let step = program
        .steps()
        .first()
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let (page_limit, limit_param_name, result_ceiling) = match step.row_limit() {
        QueryRowLimit::Literal(value) => (
            u32::try_from(*value).unwrap_or(u32::MAX),
            None,
            u32::try_from(*value).unwrap_or(u32::MAX),
        ),
        QueryRowLimit::Parameter { name, default } => {
            let val = parameters
                .get(name)
                .and_then(|v| match v {
                    CanonicalValue::U64(n) => Some(*n),
                    _ => None,
                })
                .or(*default)
                .unwrap_or(1);
            let ceiling = u32::try_from(MAX_QUERY_SCANNED_ROWS).unwrap_or(65_534);
            (
                u32::try_from(val).unwrap_or(ceiling),
                Some(name.as_str()),
                ceiling,
            )
        }
        QueryRowLimit::BoundedParameter {
            name,
            maximum,
            default,
        } => {
            let val = parameters
                .get(name)
                .and_then(|v| match v {
                    CanonicalValue::U64(n) => Some(*n),
                    _ => None,
                })
                .or(*default)
                .unwrap_or(1);
            let ceiling = u32::try_from(*maximum).unwrap_or(65_534);
            (
                u32::try_from(val).unwrap_or(ceiling),
                Some(name.as_str()),
                ceiling,
            )
        }
        QueryRowLimit::CandidateComplete { maximum } => {
            let ceiling = u32::from(*maximum);
            (ceiling, None, ceiling)
        }
    };
    let prior_offset = prior.map_or(0, TokenizedTextCursorState::next_offset);
    let digest = *plan_hash.as_bytes();
    let commit_epoch = CommitSequence::new(epoch.max(1))
        .ok_or_else(|| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    if prior.is_some_and(|prior_state| {
        prior_state.epoch() != commit_epoch
            || prior_state.statistics_identity() != digest
            || prior_state.result_ceiling() != result_ceiling
    }) {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::CursorInvalid,
        ));
    }

    let mut adjusted_params = parameters.clone();
    if let Some(param_name) = limit_param_name {
        let probe_limit = u64::from(
            prior_offset
                .saturating_add(page_limit)
                .saturating_add(1)
                .min(result_ceiling),
        );
        let mut map: BTreeMap<String, CanonicalValue> = adjusted_params
            .iter()
            .map(|(k, v)| (k.to_owned(), v.clone()))
            .collect();
        map.insert(param_name.to_owned(), CanonicalValue::U64(probe_limit));
        if let Some(updated) = QueryParameters::checked(map) {
            adjusted_params = updated;
        }
    }

    let snapshot = riffdb_query_executor::execute_projected_table_rows(
        program,
        &adjusted_params,
        candidate_rows,
        application_head,
        epoch_key,
        epoch,
    )
    .map_err(|error| execution_failure(service, OPERATION, error))?;

    let offset_usize = usize::try_from(prior_offset)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;
    let limit_usize = usize::try_from(page_limit)
        .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?;

    let mut returned = 0u32;
    let mut has_more = false;
    for value in snapshot.fields().values() {
        if let QueryResultValue::Many(rows) = value {
            let total_matched = rows.len();
            let page_len = total_matched.saturating_sub(offset_usize).min(limit_usize);
            returned = u32::try_from(page_len).unwrap_or(0);
            let next_offset = prior_offset.saturating_add(returned);
            has_more = returned > 0
                && total_matched > offset_usize.saturating_add(page_len)
                && next_offset < result_ceiling;
        }
    }
    let next_offset = prior_offset.saturating_add(returned);
    let cursor_guard = if has_more {
        match service.cursors.register_tokenized_text_unpublished(
            context.principal().principal_id(),
            cursor_lookup,
            TokenizedTextCursorState::new(
                next_offset,
                commit_epoch,
                ProjectionGeneration::first(),
                application_head,
                0,
                digest,
                result_ceiling,
            ),
        ) {
            Ok(guard) => {
                if guard.capacity_evicted() {
                    service
                        .providers
                        .telemetry
                        .record(ServiceTelemetryEvent::CursorEvicted);
                }
                Some(guard)
            }
            Err(_) => return Err(PublicError::storage_unavailable().into()),
        }
    } else {
        None
    };
    Ok((snapshot, prior_offset, page_limit, cursor_guard))
}

pub(crate) fn apply_projected_table_page_window(
    result: &mut ExecuteSymbolicQueryResult,
    prior_offset: u32,
    page_limit: u32,
) {
    let offset_usize = usize::try_from(prior_offset).unwrap_or(usize::MAX);
    let limit_usize = usize::try_from(page_limit).unwrap_or(usize::MAX);
    let fields = Arc::make_mut(result.fields_mut());
    for value in fields.values_mut() {
        if let SymbolicResultField::Many(rows) = value {
            *rows = rows
                .drain(..)
                .skip(offset_usize)
                .take(limit_usize)
                .collect();
        }
    }
}

/// Reciprocal Rank Fusion smoothing constant $k_{\text{rrf}} = 60$ (`ADR-0339`, `hybrid_rrf_v1`).
pub const HYBRID_RRF_K_CONSTANT_V1: u64 = 60;
/// Fixed-point integer scale ($10^6$) for deterministic RRF rank contributions (`ADR-0339`).
pub const HYBRID_RRF_SCALE_MICRO_V1: u64 = 1_000_000;

/// One pre-admitted candidate entering deterministic Reciprocal Rank Fusion (`hybrid_rrf_v1`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridRrfCandidateV1 {
    /// Canonical primary-key values of the entity row (used for exact tie-breaking).
    pub primary_key: Vec<CanonicalValue>,
    /// Selected field values of the entity row.
    pub fields: BTreeMap<Arc<str>, CanonicalValue>,
    /// Optional 1-based rank from the vector `nearest(...)` leg.
    pub vector_rank: Option<u32>,
    /// Optional 1-based rank from the `riff_bm25_v1` tokenized-text leg.
    pub bm25_rank: Option<u32>,
}

/// One deterministically fused result row produced by `hybrid_rrf_v1` (`ADR-0339`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HybridRrfRankedRowV1 {
    /// Canonical primary-key values.
    pub primary_key: Vec<CanonicalValue>,
    /// Selected field values.
    pub fields: BTreeMap<Arc<str>, CanonicalValue>,
    /// Exact integer fixed-point RRF score (`vec_weight_bps * (1_000_000 / (60 + rank_vec)) + text_weight_bps * (1_000_000 / (60 + rank_bm25))`).
    pub rrf_score_ug: u64,
    /// 1-based rank in the vector leg, if present.
    pub vector_rank: Option<u32>,
    /// 1-based rank in the BM25 leg, if present.
    pub bm25_rank: Option<u32>,
}

/// Executes deterministic fixed-point integer Reciprocal Rank Fusion (`hybrid_rrf_v1`) over
/// vector `nearest(...)` and `riff_bm25_v1` ranked candidates at a verified unified snapshot epoch.
///
/// Fails closed if `vector_epoch != bm25_epoch` or if the basis-point weights are invalid.
pub fn fuse_vector_and_bm25_rrf_v1(
    vector_epoch: CommitSequence,
    bm25_epoch: CommitSequence,
    vec_weight_bps: u32,
    text_weight_bps: u32,
    candidates: Vec<HybridRrfCandidateV1>,
    limit: usize,
) -> Result<Vec<HybridRrfRankedRowV1>, ServiceFailure> {
    if vector_epoch != bm25_epoch {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::FreshnessUnsatisfied,
        ));
    }
    if vec_weight_bps == 0 && text_weight_bps == 0
        || vec_weight_bps.saturating_add(text_weight_bps) > 20_000
        || limit == 0
    {
        return Err(validation_failure(ValidationCode::InvalidValue));
    }
    let mut scored = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let vec_term = candidate.vector_rank.filter(|r| *r > 0).map_or(0_u64, |r| {
            u64::from(vec_weight_bps).saturating_mul(
                HYBRID_RRF_SCALE_MICRO_V1 / (HYBRID_RRF_K_CONSTANT_V1 + u64::from(r)),
            )
        });
        let bm25_term = candidate.bm25_rank.filter(|r| *r > 0).map_or(0_u64, |r| {
            u64::from(text_weight_bps).saturating_mul(
                HYBRID_RRF_SCALE_MICRO_V1 / (HYBRID_RRF_K_CONSTANT_V1 + u64::from(r)),
            )
        });
        let rrf_score_ug = vec_term.saturating_add(bm25_term);
        if rrf_score_ug == 0 {
            continue;
        }
        scored.push(HybridRrfRankedRowV1 {
            primary_key: candidate.primary_key,
            fields: candidate.fields,
            rrf_score_ug,
            vector_rank: candidate.vector_rank,
            bm25_rank: candidate.bm25_rank,
        });
    }
    scored.sort_by(|left, right| {
        right
            .rrf_score_ug
            .cmp(&left.rrf_score_ug)
            .then_with(|| compare_canonical_key_slice(&left.primary_key, &right.primary_key))
    });
    scored.truncate(limit);
    Ok(scored)
}

fn compare_canonical_key_slice(
    left: &[CanonicalValue],
    right: &[CanonicalValue],
) -> std::cmp::Ordering {
    for (a, b) in left.iter().zip(right.iter()) {
        let ord = match (a, b) {
            (CanonicalValue::String(la), CanonicalValue::String(rb)) => {
                la.as_str().cmp(rb.as_str())
            }
            (CanonicalValue::U64(la), CanonicalValue::U64(rb)) => la.cmp(rb),
            (CanonicalValue::I64(la), CanonicalValue::I64(rb)) => la.cmp(rb),
            (CanonicalValue::Uuid(la), CanonicalValue::Uuid(rb)) => la.cmp(rb),
            _ => riffdb_types::encode_canonical_value(a)
                .unwrap_or_default()
                .cmp(&riffdb_types::encode_canonical_value(b).unwrap_or_default()),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    left.len().cmp(&right.len())
}

/// Joins ranked `nearest(...)` rows from step 0 with related entity rows from dependent steps
/// at a unified snapshot epoch (`ADR-0339`).
#[allow(clippy::too_many_arguments)]
pub fn join_vector_nearest_with_related_entities_v1(
    vector_epoch: CommitSequence,
    related_epoch: CommitSequence,
    primary_entity_name: &str,
    primary_rows: Vec<BTreeMap<Arc<str>, CanonicalValue>>,
    join_key_field: &str,
    related_entity_name: &str,
    nested_field_name: &str,
    related_rows: &[QueryRow],
) -> Result<Vec<crate::SymbolicResultRecord>, ServiceFailure> {
    if vector_epoch != related_epoch {
        return Err(application_validation_failure(
            ValidationCode::InvalidValue,
            ApplicationErrorCode::FreshnessUnsatisfied,
        ));
    }
    let mut related_index: BTreeMap<Vec<u8>, Vec<crate::SymbolicResultRecord>> = BTreeMap::new();
    for row in related_rows {
        let Some(join_val) = row.field(join_key_field) else {
            continue;
        };
        let Ok(encoded_key) = riffdb_types::encode_canonical_value(join_val) else {
            continue;
        };
        let fields = row
            .fields()
            .map(|(k, v)| (Arc::<str>::from(k), v.clone()))
            .collect::<BTreeMap<_, _>>();
        related_index.entry(encoded_key).or_default().push(
            crate::SymbolicResultRecord::new_with_nested(
                Arc::from(related_entity_name),
                fields,
                BTreeMap::new(),
            ),
        );
    }

    let mut joined = Vec::with_capacity(primary_rows.len());
    for row_fields in primary_rows {
        let mut nested = BTreeMap::new();
        if let Some(join_val) = row_fields.get(join_key_field)
            && let Ok(encoded_key) = riffdb_types::encode_canonical_value(join_val)
            && let Some(matches) = related_index.get(&encoded_key)
        {
            nested.insert(Arc::<str>::from(nested_field_name), matches.clone());
        }
        joined.push(crate::SymbolicResultRecord::new_with_nested(
            Arc::from(primary_entity_name),
            row_fields,
            nested,
        ));
    }
    Ok(joined)
}

#[cfg(test)]
mod projected_table_query_execution_tests {
    use std::collections::BTreeMap;

    use riffdb_query_executor::{QueryParameters, QueryRow};
    use riffdb_query_module::{
        NamedQuerySource, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };
    use riffdb_types::{CanonicalValue, Timestamp};

    use crate::{ExecuteSymbolicQueryResult, SharedEnumVariantNames, SymbolicResultField};

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
        let bundle =
            riffdb_contract_compiler::compile_contract_source(contract_src).expect("contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("trace_analytics").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![
                    NamedQuerySource::new("QueryHourlyRollup", event_query).expect("event query"),
                    NamedQuerySource::new("QuerySpanAnalytics", columnar_query)
                        .expect("columnar query"),
                ],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect("module");

        // 1. Execute event projection query via execute_projected_table_rows + snapshot_to_result
        let event_compiled = module.query("QueryHourlyRollup").expect("event query");
        let event_program = event_compiled.plan().representative_program();
        let t_h1 = Timestamp::new(1_700_000_000 - (1_700_000_000 % 3600), 0).expect("t1");
        let t_h2 = Timestamp::new(t_h1.seconds() + 3600, 0).expect("t2");
        let t_h3 = Timestamp::new(t_h1.seconds() + 7200, 0).expect("t3");
        let event_params = QueryParameters::checked(BTreeMap::from([
            (
                "org_id".to_owned(),
                CanonicalValue::string("org-1").expect("org"),
            ),
            (
                "model".to_owned(),
                CanonicalValue::string("gpt-4o").expect("model"),
            ),
            ("start_bucket".to_owned(), CanonicalValue::Timestamp(t_h1)),
            ("end_bucket".to_owned(), CanonicalValue::Timestamp(t_h3)),
        ]))
        .expect("event params");
        let event_candidates = vec![
            QueryRow::checked(
                "HourlyModelRollup".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "model".to_owned(),
                        CanonicalValue::string("gpt-4o").expect("model"),
                    ),
                    ("started_at".to_owned(), CanonicalValue::Timestamp(t_h2)),
                    ("span_count".to_owned(), CanonicalValue::U64(5)),
                    ("tokens_sum".to_owned(), CanonicalValue::I64(2500)),
                    ("latency_min".to_owned(), CanonicalValue::I64(80)),
                    ("latency_max".to_owned(), CanonicalValue::I64(420)),
                ]),
            )
            .expect("row 2"),
            QueryRow::checked(
                "HourlyModelRollup".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "model".to_owned(),
                        CanonicalValue::string("gpt-4o").expect("model"),
                    ),
                    ("started_at".to_owned(), CanonicalValue::Timestamp(t_h1)),
                    ("span_count".to_owned(), CanonicalValue::U64(3)),
                    ("tokens_sum".to_owned(), CanonicalValue::I64(1200)),
                    ("latency_min".to_owned(), CanonicalValue::I64(100)),
                    ("latency_max".to_owned(), CanonicalValue::I64(300)),
                ]),
            )
            .expect("row 1"),
        ];
        let event_snapshot = riffdb_query_executor::execute_projected_table_rows(
            event_program,
            &event_params,
            event_candidates,
            42,
            "HourlyModelRollup".to_owned(),
            42,
        )
        .expect("execute event projection query");
        let event_result = ExecuteSymbolicQueryResult::from_snapshot(
            event_program,
            event_snapshot,
            SharedEnumVariantNames::from(BTreeMap::new()),
            false,
            false,
        );
        assert_eq!(event_result.outcome(), "Found");
        let SymbolicResultField::Many(rollups) = &event_result.fields()["rollups"] else {
            panic!("expected rollups list");
        };
        assert_eq!(rollups.len(), 2);
        assert_eq!(
            rollups[0].fields()["started_at"],
            CanonicalValue::Timestamp(t_h1)
        );
        assert_eq!(rollups[0].fields()["tokens_sum"], CanonicalValue::I64(1200));
        assert_eq!(
            rollups[1].fields()["started_at"],
            CanonicalValue::Timestamp(t_h2)
        );
        assert_eq!(rollups[1].fields()["tokens_sum"], CanonicalValue::I64(2500));

        // 2. Execute columnar projection query with temporal bucket group by and p95
        let col_compiled = module.query("QuerySpanAnalytics").expect("columnar query");
        let col_program = col_compiled.plan().representative_program();
        let col_params = QueryParameters::checked(BTreeMap::from([
            (
                "org_id".to_owned(),
                CanonicalValue::string("org-1").expect("org"),
            ),
            ("start_at".to_owned(), CanonicalValue::Timestamp(t_h1)),
            ("end_at".to_owned(), CanonicalValue::Timestamp(t_h3)),
        ]))
        .expect("columnar params");
        let col_candidates = vec![
            QueryRow::checked(
                "Span".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "span_id".to_owned(),
                        CanonicalValue::string("s-1").expect("span"),
                    ),
                    (
                        "model".to_owned(),
                        CanonicalValue::string("gpt-4o").expect("model"),
                    ),
                    (
                        "status".to_owned(),
                        CanonicalValue::string("OK").expect("status"),
                    ),
                    ("latency_ms".to_owned(), CanonicalValue::I64(100)),
                    ("total_tokens".to_owned(), CanonicalValue::I64(400)),
                    (
                        "started_at".to_owned(),
                        CanonicalValue::Timestamp(
                            Timestamp::new(t_h1.seconds() + 120, 0).expect("ts"),
                        ),
                    ),
                ]),
            )
            .expect("span 1"),
            QueryRow::checked(
                "Span".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "span_id".to_owned(),
                        CanonicalValue::string("s-2").expect("span"),
                    ),
                    (
                        "model".to_owned(),
                        CanonicalValue::string("gpt-4o").expect("model"),
                    ),
                    (
                        "status".to_owned(),
                        CanonicalValue::string("OK").expect("status"),
                    ),
                    ("latency_ms".to_owned(), CanonicalValue::I64(300)),
                    ("total_tokens".to_owned(), CanonicalValue::I64(800)),
                    (
                        "started_at".to_owned(),
                        CanonicalValue::Timestamp(
                            Timestamp::new(t_h1.seconds() + 600, 0).expect("ts"),
                        ),
                    ),
                ]),
            )
            .expect("span 2"),
        ];
        let col_snapshot = riffdb_query_executor::execute_projected_table_rows(
            col_program,
            &col_params,
            col_candidates,
            55,
            "SpanAnalytics".to_owned(),
            55,
        )
        .expect("execute columnar projection query");
        let col_result = ExecuteSymbolicQueryResult::from_snapshot(
            col_program,
            col_snapshot,
            SharedEnumVariantNames::from(BTreeMap::new()),
            false,
            false,
        );
        assert_eq!(col_result.outcome(), "Found");
        let SymbolicResultField::Many(by_model) = &col_result.fields()["by_model"] else {
            panic!("expected by_model groups");
        };
        assert_eq!(by_model.len(), 1);
        assert_eq!(
            by_model[0].fields()["model"],
            CanonicalValue::string("gpt-4o").expect("model")
        );
        assert_eq!(
            by_model[0].fields()["hour_bucket"],
            CanonicalValue::Timestamp(t_h1)
        );
        assert_eq!(by_model[0].fields()["span_count"], CanonicalValue::U64(2));
        assert_eq!(
            by_model[0].exact_decimals()["total_tokens"].coefficient(),
            1200
        );
        assert_eq!(
            by_model[0].fields()["p95_latency_ms"],
            CanonicalValue::I64(300)
        );
    }

    // req: QRY-001, QRY-002, QRY-004
    #[test]
    fn streaming_cursor_paginates_across_all_named_query_providers_with_pinned_snapshot() {
        use std::num::NonZeroU16;
        use std::time::{Duration, Instant};

        use riffdb_columnar::{NearestQueryResult, NearestResultRow, NearestSearchKind};
        use riffdb_types::{
            CommitSequence, FrontierPosition, ProjectionFrontier, ProjectionGeneration,
        };

        use crate::primary_admission_test_support::ServiceHarness;
        use crate::{
            VectorProjectionResult,
            symbolic_query_continuation::{
                apply_projected_table_page_window, execute_projected_table_page,
                paginate_projected_vector_result, register_exact_provider_cursor,
                resolve_exact_provider_cursor, resolve_projected_table_cursor,
                resolve_projected_vector_cursor,
            },
        };

        let harness = ServiceHarness::columnar_board();
        let inner = &harness.service.inner;
        let (context, _cancel) = harness.context_with_deadline(
            1,
            Instant::now()
                .checked_add(Duration::from_secs(5))
                .expect("deadline"),
        );

        let contract_src = r#"
contract CursorProviders version 1 {
  entity Doc {
    key (org_id: string<64>, doc_id: string<64>)
    field title: string<128>
    field category: string<32>
    delete_policy no_inbound
  }
  event DocLogged {
    org_id: string<64>
    category: string<32>
    doc_id: string<64>
  }
  aggregate Docs {
    root Doc
    partition_by (org_id)
    conflict_key (org_id, doc_id)
  }
  projection CategoryDocs {
    source event DocLogged
    key (org_id, category, doc_id)
    measure doc_count = count()
    frontier transactionally_ordered
  }
}
"#;
        let query_src = r#"
query ListCategoryDocs(
    $org_id: Doc.org_id,
    $category: Doc.category,
    $limit: Limit<10> = 2
) {
    source projected CategoryDocs
    freshness available

    many docs from CategoryDocs
        where org_id == $org_id
          && category == $category
        order by doc_id asc
        take $limit

    return Found {
        docs: docs {
            doc_id
            doc_count
        }
    }
    outcomes Found
}
"#;
        let bundle =
            riffdb_contract_compiler::compile_contract_source(contract_src).expect("contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("cursor_providers").expect("module"),
                QueryModuleVersion::new(1).expect("version"),
                vec![NamedQuerySource::new("ListCategoryDocs", query_src).expect("query")],
            )
            .expect("candidate"),
            &bundle,
        )
        .expect("compiled module");
        let compiled = module.query("ListCategoryDocs").expect("query");
        let program = compiled.plan().representative_program();
        let module_hash = module.identity();
        let plan_hash = compiled.plan().identity();
        let params = QueryParameters::checked(BTreeMap::from([
            (
                "org_id".to_owned(),
                CanonicalValue::string("org-1").expect("org"),
            ),
            (
                "category".to_owned(),
                CanonicalValue::string("guide").expect("cat"),
            ),
            ("limit".to_owned(), CanonicalValue::U64(2)),
        ]))
        .expect("params");

        // 1. Projected-table multi-page cursor continuation with snapshot fence
        let make_candidates = || {
            (1..=5)
                .map(|idx| {
                    QueryRow::checked(
                        "CategoryDocs".to_owned(),
                        BTreeMap::from([
                            (
                                "org_id".to_owned(),
                                CanonicalValue::string("org-1").expect("org"),
                            ),
                            (
                                "category".to_owned(),
                                CanonicalValue::string("guide").expect("cat"),
                            ),
                            (
                                "doc_id".to_owned(),
                                CanonicalValue::string(format!("doc-{idx}")).expect("doc"),
                            ),
                            ("doc_count".to_owned(), CanonicalValue::U64(1)),
                        ]),
                    )
                    .expect("row")
                })
                .collect::<Vec<_>>()
        };

        let mut cursor = None;
        let mut collected_docs = Vec::new();
        for expected_page in 0..3 {
            let (lookup, prior, resolved_head) = resolve_projected_table_cursor(
                inner,
                &context,
                program,
                module_hash,
                plan_hash,
                &params,
                cursor,
                Some(42),
                None,
            )
            .expect("resolve table cursor");
            assert_eq!(resolved_head, Some(42));
            let (snapshot, prior_offset, page_limit, guard) = execute_projected_table_page(
                inner,
                &context,
                program,
                &params,
                make_candidates(),
                42,
                "CategoryDocs".to_owned(),
                42,
                lookup,
                prior.as_deref(),
                plan_hash,
            )
            .expect("execute projected table page");
            let mut res = ExecuteSymbolicQueryResult::from_snapshot(
                program,
                snapshot,
                SharedEnumVariantNames::from(BTreeMap::new()),
                false,
                false,
            );
            apply_projected_table_page_window(&mut res, prior_offset, page_limit);
            let SymbolicResultField::Many(rows) = &res.fields()["docs"] else {
                panic!("expected docs");
            };
            for r in rows {
                if let CanonicalValue::String(id) = &r.fields()["doc_id"] {
                    collected_docs.push(id.as_str().to_owned());
                }
            }
            cursor = guard.map(crate::CursorPublicationGuard::publish);
            if expected_page < 2 {
                assert!(cursor.is_some(), "page {expected_page} must mint cursor");
            } else {
                assert!(cursor.is_none(), "final page must not mint cursor");
            }
        }
        assert_eq!(
            collected_docs,
            vec!["doc-1", "doc-2", "doc-3", "doc-4", "doc-5"]
        );

        // 2. Projected-vector multi-page cursor continuation with snapshot epoch fence
        let epoch = CommitSequence::new(42).expect("epoch");
        let frontier = ProjectionFrontier::new(7, FrontierPosition::AppliedThrough(epoch));
        let make_vector_observed = || {
            let rows = (1..=5_u8)
                .map(|idx| NearestResultRow {
                    primary_key: vec![
                        CanonicalValue::string("org-1").expect("org"),
                        CanonicalValue::string(format!("vec-{idx}")).expect("id"),
                    ],
                    cells: Vec::new(),
                    distance: f32::from(idx),
                })
                .collect();
            VectorProjectionResult::new(
                NearestQueryResult {
                    primary_key_fields: Vec::new(),
                    projected_fields: Vec::new(),
                    rows,
                    scanned_rows: 5,
                    search_kind: NearestSearchKind::Exact,
                    ann_stats: None,
                },
                frontier.clone(),
                frontier.clone(),
            )
        };
        let mut vec_cursor = None;
        let mut vec_ids = Vec::new();
        for page_idx in 0..3 {
            let (lookup, prior, prior_offset, fetch_k, _) = resolve_projected_vector_cursor(
                inner,
                &context,
                program,
                module_hash,
                plan_hash,
                &params,
                vec_cursor,
                2,
                10,
                Some(42),
                None,
            )
            .expect("resolve vector cursor");
            assert!(fetch_k >= prior_offset + 2);
            let (paginated, guard) = paginate_projected_vector_result(
                inner,
                &context,
                make_vector_observed(),
                lookup,
                prior.as_deref(),
                prior_offset,
                2,
                10,
                Some(42),
                plan_hash,
            )
            .expect("paginate vector result");
            let (nearest, _, _) = paginated.into_parts();
            for row in nearest.rows {
                if let CanonicalValue::String(s) = &row.primary_key[1] {
                    vec_ids.push(s.as_str().to_owned());
                }
            }
            vec_cursor = guard.map(crate::CursorPublicationGuard::publish);
            if page_idx < 2 {
                assert!(vec_cursor.is_some());
            } else {
                assert!(vec_cursor.is_none());
            }
        }
        assert_eq!(vec_ids, vec!["vec-1", "vec-2", "vec-3", "vec-4", "vec-5"]);

        // 3. Exact-predicate & exact-text provider cursor continuation and epoch drift rejection
        let provider_digest = [0x55; 32];
        let (lookup1, prior1, off1, lim1, _) = resolve_exact_provider_cursor(
            inner,
            &context,
            program,
            module_hash,
            plan_hash,
            &params,
            None,
            10,
            0,
            NonZeroU16::new(2).expect("2"),
            Some(42),
            None,
        )
        .expect("page 1 cursor");
        assert!(prior1.is_none());
        assert_eq!(off1, 0);
        assert_eq!(lim1.get(), 2);
        let guard1 = register_exact_provider_cursor(
            inner,
            &context,
            lookup1,
            None,
            off1,
            2,
            5,
            10,
            8,
            epoch,
            ProjectionGeneration::first(),
            7,
            provider_digest,
            Some(42),
        )
        .expect("register page 1")
        .expect("has more");
        let token1 = guard1.publish();

        let (lookup2, prior2, off2, lim2, _) = resolve_exact_provider_cursor(
            inner,
            &context,
            program,
            module_hash,
            plan_hash,
            &params,
            Some(token1),
            10,
            0,
            NonZeroU16::new(2).expect("2"),
            Some(42),
            None,
        )
        .expect("page 2 cursor");
        assert_eq!(off2, 2);
        assert_eq!(lim2.get(), 2);

        // Epoch drift on page 2 is rejected deterministically
        let drifted_epoch = CommitSequence::new(43).expect("drifted");
        assert!(
            register_exact_provider_cursor(
                inner,
                &context,
                lookup2,
                prior2.as_deref(),
                off2,
                2,
                5,
                10,
                8,
                drifted_epoch,
                ProjectionGeneration::first(),
                7,
                provider_digest,
                Some(42),
            )
            .is_err(),
            "epoch drift across exact provider continuation must be refused"
        );
    }

    // req: VEC-001, VEC-006, VEC-007, VEC-009, QRY-001, QRY-002
    #[test]
    fn vector_nearest_query_joins_related_entities_and_executes_hybrid_bm25_rrf() {
        use crate::symbolic_query_continuation::{
            HYBRID_RRF_K_CONSTANT_V1, HYBRID_RRF_SCALE_MICRO_V1, HybridRrfCandidateV1,
            fuse_vector_and_bm25_rrf_v1, join_vector_nearest_with_related_entities_v1,
        };
        use riffdb_types::CommitSequence;
        use std::sync::Arc;

        let epoch = CommitSequence::new(100).expect("epoch");
        let drifted = CommitSequence::new(101).expect("drifted");

        // 1. Hybrid RRF fusion (vector nearest + riff_bm25_v1) at unified epoch
        // doc-b is rank 1 in vector and rank 2 in BM25 -> highest combined RRF score
        // doc-a is rank 2 in vector and rank 1 in BM25 -> tied RRF score with doc-b, tie-broken by primary key asc ("doc-a" < "doc-b")
        // doc-c is rank 3 in vector only
        let candidates = vec![
            HybridRrfCandidateV1 {
                primary_key: vec![
                    CanonicalValue::string("org-1").expect("org"),
                    CanonicalValue::string("doc-b").expect("id"),
                ],
                fields: BTreeMap::from([
                    (
                        Arc::<str>::from("doc_id"),
                        CanonicalValue::string("doc-b").expect("id"),
                    ),
                    (
                        Arc::<str>::from("author_id"),
                        CanonicalValue::string("auth-2").expect("author"),
                    ),
                ]),
                vector_rank: Some(1),
                bm25_rank: Some(2),
            },
            HybridRrfCandidateV1 {
                primary_key: vec![
                    CanonicalValue::string("org-1").expect("org"),
                    CanonicalValue::string("doc-a").expect("id"),
                ],
                fields: BTreeMap::from([
                    (
                        Arc::<str>::from("doc_id"),
                        CanonicalValue::string("doc-a").expect("id"),
                    ),
                    (
                        Arc::<str>::from("author_id"),
                        CanonicalValue::string("auth-1").expect("author"),
                    ),
                ]),
                vector_rank: Some(2),
                bm25_rank: Some(1),
            },
            HybridRrfCandidateV1 {
                primary_key: vec![
                    CanonicalValue::string("org-1").expect("org"),
                    CanonicalValue::string("doc-c").expect("id"),
                ],
                fields: BTreeMap::from([
                    (
                        Arc::<str>::from("doc_id"),
                        CanonicalValue::string("doc-c").expect("id"),
                    ),
                    (
                        Arc::<str>::from("author_id"),
                        CanonicalValue::string("auth-1").expect("author"),
                    ),
                ]),
                vector_rank: Some(3),
                bm25_rank: None,
            },
        ];

        // Epoch mismatch across vector and BM25 legs must fail closed
        assert!(
            fuse_vector_and_bm25_rrf_v1(epoch, drifted, 5_000, 5_000, candidates.clone(), 3)
                .is_err()
        );

        let fused = fuse_vector_and_bm25_rrf_v1(epoch, epoch, 5_000, 5_000, candidates, 3)
            .expect("hybrid RRF fusion succeeds at unified epoch");
        assert_eq!(fused.len(), 3);
        let expected_top_score = 5_000
            * (HYBRID_RRF_SCALE_MICRO_V1 / (HYBRID_RRF_K_CONSTANT_V1 + 1))
            + 5_000 * (HYBRID_RRF_SCALE_MICRO_V1 / (HYBRID_RRF_K_CONSTANT_V1 + 2));
        assert_eq!(fused[0].rrf_score_ug, expected_top_score);
        assert_eq!(fused[1].rrf_score_ug, expected_top_score);
        // Tie-broken deterministically by canonical primary key ascending ("doc-a" before "doc-b")
        assert_eq!(
            fused[0].fields["doc_id"],
            CanonicalValue::string("doc-a").expect("id")
        );
        assert_eq!(
            fused[1].fields["doc_id"],
            CanonicalValue::string("doc-b").expect("id")
        );
        assert_eq!(
            fused[2].fields["doc_id"],
            CanonicalValue::string("doc-c").expect("id")
        );

        // 2. Post-nearest dependent entity join at unified snapshot epoch
        let author_rows = vec![
            QueryRow::checked(
                "Author".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "author_id".to_owned(),
                        CanonicalValue::string("auth-1").expect("auth"),
                    ),
                    (
                        "display_name".to_owned(),
                        CanonicalValue::string("Ada Lovelace").expect("name"),
                    ),
                ]),
            )
            .expect("author 1"),
            QueryRow::checked(
                "Author".to_owned(),
                BTreeMap::from([
                    (
                        "org_id".to_owned(),
                        CanonicalValue::string("org-1").expect("org"),
                    ),
                    (
                        "author_id".to_owned(),
                        CanonicalValue::string("auth-2").expect("auth"),
                    ),
                    (
                        "display_name".to_owned(),
                        CanonicalValue::string("Alan Turing").expect("name"),
                    ),
                ]),
            )
            .expect("author 2"),
        ];

        let primary_rows = fused.into_iter().map(|r| r.fields).collect::<Vec<_>>();
        assert!(
            join_vector_nearest_with_related_entities_v1(
                epoch,
                drifted,
                "Document",
                primary_rows.clone(),
                "author_id",
                "Author",
                "author",
                &author_rows,
            )
            .is_err(),
            "epoch drift between vector nearest leg and joined entity leg must fail closed"
        );

        let joined = join_vector_nearest_with_related_entities_v1(
            epoch,
            epoch,
            "Document",
            primary_rows,
            "author_id",
            "Author",
            "author",
            &author_rows,
        )
        .expect("post-nearest join succeeds at unified epoch");
        assert_eq!(joined.len(), 3);
        let author_0 = &joined[0].nested()["author"][0];
        assert_eq!(
            author_0.fields()["display_name"],
            CanonicalValue::string("Ada Lovelace").expect("name")
        );
        let author_1 = &joined[1].nested()["author"][0];
        assert_eq!(
            author_1.fields()["display_name"],
            CanonicalValue::string("Alan Turing").expect("name")
        );
    }
}
