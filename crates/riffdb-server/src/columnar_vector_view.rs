//! Shared vector evidence, freshness, policy and nearest-query semantics.
use super::*;

pub(super) fn execute(
    request: VectorProjectionRequest,
    observation: ColumnarObservation,
    storage: &(impl AuthoritativePointReader + ?Sized),
    executor: &(impl riffdb_query_executor::QueryExecutionPort + ?Sized),
) -> Result<VectorProjectionResult, VectorProjectionPortError> {
    match observation.lifecycle() {
        Some(ColumnarLifecycle::Building) => return Err(VectorProjectionPortError::Building),
        Some(ColumnarLifecycle::Rebuilding { .. }) => {
            return Err(VectorProjectionPortError::Rebuilding);
        }
        Some(ColumnarLifecycle::Degraded { .. }) => {
            return Err(VectorProjectionPortError::Degraded);
        }
        Some(ColumnarLifecycle::Invalid { .. }) => {
            return Err(VectorProjectionPortError::Integrity);
        }
        Some(ColumnarLifecycle::Ready) | None => {}
    }
    if !observation.has_published()
        || observation.definition().entity_type_id() != request.entity()
        || !observation
            .definition()
            .projected_fields()
            .contains(&request.field())
    {
        return Err(VectorProjectionPortError::Integrity);
    }
    let FrontierPosition::AppliedThrough(epoch) = observation.published_frontier().position()
    else {
        return Err(VectorProjectionPortError::Building);
    };
    if request
        .minimum_epoch()
        .is_some_and(|minimum| epoch < minimum)
    {
        return Err(VectorProjectionPortError::FreshnessUnsatisfied);
    }
    if let Some(max_lag_ms) = request.max_lag_ms() {
        let FrontierPosition::AppliedThrough(head) = observation.head().position() else {
            return Err(VectorProjectionPortError::FreshnessUnsatisfied);
        };
        if trusted_commit_lag_ms(storage, epoch, head)? > max_lag_ms {
            return Err(VectorProjectionPortError::FreshnessUnsatisfied);
        }
    }
    let target = riffdb_query_executor::VectorInspectionTargetV1::new(
        request.lineage().clone(),
        request.partition().clone(),
        request.entity(),
        request.field(),
        None,
        std::num::NonZeroU16::new(500).expect("fixed inspection bound is nonzero"),
    );
    let evidence = riffdb_query_executor::QueryExecutionPort::inspect_vector_evidence(
        executor,
        &target,
        request.row_policy().map(Arc::as_ref),
    )
    .map_err(map_vector_execution_error)?;
    if !evidence.exact_end()
        || evidence.frontier() != Some(epoch)
        || evidence.stale_entities() > request.stale_entity_threshold()
    {
        return Err(VectorProjectionPortError::FreshnessUnsatisfied);
    }
    let candidates = evidence
        .candidates()
        .iter()
        .map(|candidate| candidate.entity_key().clone())
        .collect::<BTreeSet<_>>();
    if let Some(admission) = evidence.admission()
        && !admission.covers(request.entity(), &candidates)
    {
        return Err(VectorProjectionPortError::Integrity);
    }
    let current = evidence
        .candidates()
        .iter()
        .filter(|candidate| {
            candidate
                .embedding_write()
                .is_some_and(|(_, metadata)| metadata == request.current_model())
        })
        .map(|candidate| candidate.entity_key().clone())
        .collect::<BTreeSet<_>>();
    let mut admission = ProductionVectorAdmission {
        entity: request.entity(),
        key_schema: observation.definition().primary_key_schema(),
        current,
        policy: evidence.admission(),
    };
    let nearest_request = NearestQueryRequest {
        org_scope: request.partition_value().clone(),
        vector_field: request.field(),
        query_vector: request.query_vector().clone(),
        k: request.k(),
        metric: request.metric(),
        predicates: request.predicates().to_vec(),
        budget: QueryBudget {
            max_scanned_rows: 500,
            max_group_cardinality: 1,
        },
    };
    let nearest = riffdb_columnar::nearest_query_snapshot_with_admission(
        observation.definition(),
        observation.snapshot().as_ref(),
        &nearest_request,
        &mut admission,
    )
    .map_err(map_vector_nearest_error)?;
    Ok(VectorProjectionResult::new(
        nearest,
        observation.published_frontier().clone(),
        observation.head().clone(),
    ))
}

fn trusted_commit_lag_ms(
    storage: &(impl AuthoritativePointReader + ?Sized),
    frontier: CommitSequence,
    head: CommitSequence,
) -> Result<u64, VectorProjectionPortError> {
    if head < frontier {
        return Err(VectorProjectionPortError::Integrity);
    }
    if head == frontier {
        return Ok(0);
    }
    let frontier_time = AuthoritativePointReader::read_commit(storage, frontier)
        .map_err(|_| VectorProjectionPortError::Unavailable)?
        .ok_or(VectorProjectionPortError::Integrity)?
        .logical_time()
        .timestamp();
    let head_time = AuthoritativePointReader::read_commit(storage, head)
        .map_err(|_| VectorProjectionPortError::Unavailable)?
        .ok_or(VectorProjectionPortError::Integrity)?
        .logical_time()
        .timestamp();
    if head_time < frontier_time {
        return Err(VectorProjectionPortError::Integrity);
    }
    trusted_timestamp_lag_ms(frontier_time, head_time)
}
