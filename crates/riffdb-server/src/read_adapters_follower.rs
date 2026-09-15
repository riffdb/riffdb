//! Least-authority follower composition of shared semantic read operations.
use super::*;
use crate::replication_bootstrap::FollowerReadSnapshots;
use riffdb_storage_api::SnapshotFenceReader;
pub(super) fn read_vector_observation(
    storage: &impl VectorObservationRepository,
    target: AuthoritativeVectorTarget,
) -> Result<Option<AuthoritativeVectorObservation>, AuthoritativeReadError> {
    let lower = VectorObservationTargetV1::new(
        target.lineage().clone(),
        target.partition_key().clone(),
        target.entity_type(),
        target.vector_field(),
    );
    storage
        .read_vector_observation(&lower)
        .map_err(map_storage_error)
        .map(|value| {
            value.map(|value| {
                AuthoritativeVectorObservation::new(
                    target,
                    value.total_entities(),
                    value.source_stale_entities(),
                    value
                        .model_counts()
                        .map(|(metadata, count)| (metadata.clone(), count))
                        .collect(),
                    value.revision(),
                )
            })
        })
}
pub(super) fn read_vector_evidence(
    storage: &impl VectorEvidenceIndexRepository,
    request: AuthoritativeVectorEvidenceRequest,
) -> Result<AuthoritativeVectorEvidencePage, AuthoritativeReadError> {
    let target = VectorObservationTargetV1::new(
        request.target().lineage().clone(),
        request.target().partition_key().clone(),
        request.target().entity_type(),
        request.target().vector_field(),
    );
    let limit = StorageScanLimit::new(request.limit().get().get())
        .ok_or(AuthoritativeReadError::Integrity)?;
    let lower = VectorEvidenceIndexScanRequestV1::new(target, request.after().cloned(), limit)
        .map_err(|_| AuthoritativeReadError::Integrity)?;
    let page = storage
        .scan_vector_evidence_index(&lower)
        .map_err(map_storage_error)?;
    let rows = page
        .entries()
        .iter()
        .map(|entry| {
            AuthoritativeVectorEvidenceRow::new(
                entry.entity_key().clone(),
                entry.newest_source_write(),
                entry
                    .embedding_write()
                    .map(|write| (write.sequence(), write.metadata().clone())),
            )
        })
        .collect();
    Ok(AuthoritativeVectorEvidencePage::new(
        rows,
        page.continuation().cloned(),
        page.exact_end(),
    ))
}
impl ServerAuthoritativeReadPort {
    pub(crate) fn follower(reads: FollowerReadSnapshots, driver: &BlockingPortDriver) -> Self {
        let source = reads.clone();
        let vector_observation = driver.executor(move |target| {
            read_vector_observation(
                source.latest().map_err(map_storage_error)?.snapshot(),
                target,
            )
        });
        let source = reads.clone();
        let vector_evidence = driver.executor(move |request| {
            read_vector_evidence(
                source.latest().map_err(map_storage_error)?.snapshot(),
                request,
            )
        });
        let source = reads.clone();
        let application_head = driver.executor(move |()| {
            source
                .latest()
                .map_err(map_storage_error)?
                .snapshot()
                .application_frontier()
                .map(|head| {
                    head.map_or(
                        FrontierPosition::BeforeFirst,
                        FrontierPosition::AppliedThrough,
                    )
                })
                .map_err(map_storage_error)
        });
        let source = reads.clone();
        let entity = driver.executor(move |request| {
            read_entity(
                source.latest().map_err(map_storage_error)?.snapshot(),
                request,
            )
        });
        let source = reads;
        let index = driver.executor(move |request| {
            scan_index(
                source.latest().map_err(map_storage_error)?.snapshot(),
                request,
            )
        });
        // These families require durable audit or primary admission and are
        // refused by the shared follower service before any provider reservation.
        // Keeping their lower ports closed prevents accidental future bypasses.
        Self {
            vector_observation,
            vector_evidence,
            application_head,
            entity,
            index,
            event_replay: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            reactive_event_window: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            outcome: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            commit: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            commit_scan: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            subscription: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            provenance: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
            revoke_target: driver.executor(|_| Err(AuthoritativeReadError::Unavailable)),
        }
    }
}
