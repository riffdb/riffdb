//! Canonical reconstruction of the complete immutable primary-fence receipt.
use super::*;

pub(super) fn reconstruct(
    record: &StoredPrimaryFenceAdministrationV1,
) -> Result<AuthoritativeTransactionV3, StorageError> {
    let lineage = record.lineage();
    let allocator = AdministrationSequenceAllocator::next(record.administration_sequence());
    let allocated = allocator.allocate_one().map_err(|_| corrupt())?;
    let encoded = encode_primary_fence_administration_v1(record).map_err(codec_error)?;
    let before = encode_administration_sequence_allocator_v1(allocator).map_err(codec_error)?;
    let after =
        encode_administration_sequence_allocator_v1(allocated.next()).map_err(codec_error)?;
    let mut mutations = vec![
        AuthoritativeMutationV3::put(
            N::Audit,
            &crate::keys::encode_audit_key(record.administration_sequence()),
            None,
            encoded.as_bytes(),
        )
        .map_err(|_| corrupt())?,
        AuthoritativeMutationV3::replace(
            N::NextAdministrationSequence,
            crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
            before.as_bytes(),
            after.as_bytes(),
        )
        .map_err(|_| corrupt())?,
    ];
    mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: Some(record.observed().sequence()),
            sequence: record
                .observed()
                .sequence()
                .checked_next()
                .ok_or_else(corrupt)?,
            predecessor_frontier: record.observed().frontier(),
            covered_frontier: DualFrontier::new(
                record.observed().frontier().application(),
                Some(record.administration_sequence()),
            ),
            prior_history_hash: record.observed().history_hash(),
        },
        ChangelogAttributionV3::PrimaryFence,
        mutations,
        lineage.catalog_digest(),
    )
    .map_err(|_| corrupt())
}
