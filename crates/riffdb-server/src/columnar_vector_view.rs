//! Shared vector evidence, freshness, policy and nearest-query semantics.
use super::*;

pub(super) fn execute(
    request: VectorProjectionRequest,
    observation: ColumnarObservation,
    storage: &(impl AuthoritativePointReader + ?Sized),
    executor: &(impl riffdb_query_executor::QueryExecutionPort + ?Sized),
    cache: Option<(
        &riffdb_columnar::NearestGraphCache,
        riffdb_types::ProjectionGeneration,
    )>,
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
            max_scanned_rows: production_vector_candidate_ceiling(
                observation.definition(),
                request.field(),
            ),
            max_group_cardinality: 1,
        },
    };
    let context =
        cache.and_then(|(_, generation)| graph_context(&request, &observation, generation));
    let nearest = riffdb_columnar::nearest_query_snapshot_with_cache(
        observation.definition(),
        observation.snapshot().as_ref(),
        &nearest_request,
        &mut admission,
        cache
            .zip(context.as_deref())
            .map(|((cache, _), context)| (cache, context)),
    )
    .map_err(map_vector_nearest_error)?;
    Ok(VectorProjectionResult::new(
        nearest,
        observation.published_frontier().clone(),
        observation.head().clone(),
    ))
}

/// Rows one organization may merge before a production vector query refuses,
/// for a field that declares no approximate tier.
///
/// A field with no ANN declaration has asked for exact search and nothing else,
/// so this bound is the server's own and is deliberately small.
const UNDECLARED_VECTOR_CANDIDATE_CEILING: usize = 500;

/// Rows one organization may merge before a production vector query refuses.
///
/// A declared ANN tier engages only *above* the contract's `row_threshold`, and
/// the merge happens before that test, so a ceiling below the declared
/// threshold would refuse the query first and the declared tier could never run
/// (ADR-0255). A field that declares one is therefore served up to
/// `MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG`, which is the per-organization row
/// count the declaration's own bound is expressed in: the contract author chose
/// to be measured against it.
fn production_vector_candidate_ceiling(
    definition: &riffdb_columnar::RegisteredDefinition,
    field: FieldId,
) -> usize {
    if definition.vector_ann_config(field).is_some() {
        riffdb_contract_ir::MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG as usize
    } else {
        UNDECLARED_VECTOR_CANDIDATE_CEILING
    }
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

// Generation-local cache ownership additionally binds the exact provider/source.
// This is an ephemeral equality key, never a persisted encoding or hash proof.
fn graph_context(
    request: &VectorProjectionRequest,
    observation: &ColumnarObservation,
    generation: riffdb_types::ProjectionGeneration,
) -> Option<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut append = |value: &[u8]| -> Option<()> {
        if bytes.len().checked_add(value.len())?.checked_add(8)? > 64 * 1024 {
            return None;
        }
        bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
        bytes.extend_from_slice(value);
        Some(())
    };
    append(request.source_name().as_bytes())?;
    append(request.lineage().as_str().as_bytes())?;
    append(request.partition().as_bytes())?;
    append(&generation.get().to_be_bytes())?;
    append(
        &observation
            .published_frontier()
            .history_incarnation()
            .to_be_bytes(),
    )?;
    let FrontierPosition::AppliedThrough(frontier) = observation.published_frontier().position()
    else {
        return None;
    };
    append(&frontier.to_be_bytes())?;
    append(observation.definition().fingerprint().as_bytes())?;
    append(request.current_model().model_identity().as_bytes())?;
    append(request.current_model().model_version().as_bytes())?;
    if let Some(policy) = request.row_policy() {
        append(&policy.internal_vector_cache_identity(request.entity())?)?;
    } else {
        append(&[])?;
    }
    Some(bytes)
}

#[cfg(test)]
mod ceiling_tests {
    use super::*;
    use riffdb_columnar::ColumnarProjectionDefinition;
    use riffdb_contract_compiler::compile_contract_source;

    const DECLARED: &str = r#"
contract AnnDocs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<64>
    vector_field embedding(12, cosine, (title), staleness_slo 60, ann_threshold 4096, recall_target_bps 9500)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

    const UNDECLARED: &str = r#"
contract AnnDocs version 1 {
  entity Document {
    key (org_id: uuid, doc_id: uuid)
    field title: string<64>
    vector_field embedding(12, cosine, (title), staleness_slo 60)
  }
  aggregate Documents {
    root Document
    partition_by org_id
    conflict_key (org_id, doc_id)
  }
}
"#;

    fn definition(source: &str) -> (riffdb_columnar::RegisteredDefinition, FieldId) {
        let bundle = compile_contract_source(source).expect("contract compiles");
        let field = |name: &str| {
            bundle.schema().entities()[0]
                .record()
                .fields()
                .iter()
                .find(|candidate| candidate.name() == name)
                .expect("field")
                .id()
        };
        let embedding = field("embedding");
        let registered = riffdb_columnar::RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: "document_vectors".into(),
                entity_name: "Document".into(),
                projected_fields: vec![field("title"), embedding],
                org_scope_field: field("org_id"),
            },
            &bundle,
        )
        .expect("register");
        (registered, embedding)
    }

    /// A declared approximate tier engages only *above* the contract's
    /// `ann_threshold`, and the merge that feeds it happens first. A ceiling
    /// below the declared threshold refuses the query before the tier can run,
    /// which is what made a declaration of 500 or more inert (ADR-0255).
    // req: VEC-010
    #[test]
    fn a_declared_approximate_tier_is_served_up_to_its_declarable_bound() {
        let (registered, field) = definition(DECLARED);
        let ceiling = production_vector_candidate_ceiling(&registered, field);
        let declared = registered
            .vector_ann_config(field)
            .expect("the contract declares an approximate tier")
            .row_threshold() as usize;
        assert!(
            ceiling > declared,
            "a declared threshold of {declared} must be reachable, not refused first"
        );
        assert_eq!(
            ceiling,
            riffdb_contract_ir::MAX_VECTOR_ANN_THRESHOLD_ROWS_PER_ORG as usize,
            "the bound is the per-organization row count the declaration is expressed in"
        );
    }

    /// A field that declares no approximate tier asked for exact search and
    /// nothing else, so the server keeps its own smaller bound.
    // req: VEC-010
    #[test]
    fn an_undeclared_field_keeps_the_servers_own_ceiling() {
        let (registered, field) = definition(UNDECLARED);
        assert!(registered.vector_ann_config(field).is_none());
        assert_eq!(
            production_vector_candidate_ceiling(&registered, field),
            UNDECLARED_VECTOR_CANDIDATE_CEILING
        );
    }
}
