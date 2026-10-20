//! Closed FreshV3 canonical origin/anchor proof for one actual startup owner.
//! This proves fixed control evidence only, never population or Source closure.
use super::*;
use crate::{gate::ExclusiveLease, store::SharedRedb};
use redb::ReadableDatabase;

#[path = "cold/materialized.rs"]
mod materialized;
pub(crate) use materialized::install_actual_origin_complement;

/// Move-only complete bounded proof, retaining this owner's real mutation lease.
/// Only its consuming finish can install the fixed evidence into that owner.
pub(crate) struct ValidatedColdOriginV1<'engine, 'lease> {
    prepared: Option<PreparedOriginContextV1<'engine, Database>>,
    _lease: &'lease ExclusiveLease,
}
impl ValidatedColdOriginV1<'_, '_> {
    pub(crate) fn install_after_complete_bounded_validation(mut self) -> Result<(), StorageError> {
        match self.prepared.take() {
            Some(prepared) => context::install_prepared(prepared),
            None => Ok(()),
        }
    }
}

// The explicit destructor keeps the actual lease borrow live even when the
// caller never consumes or otherwise uses a successfully prepared candidate.
impl Drop for ValidatedColdOriginV1<'_, '_> {
    fn drop(&mut self) {}
}

/// A fields-private retained read minted only by the actual native engine.
/// No constructor accepts a caller-selected engine/read pair.
pub(crate) struct StartupOriginReadV1<'engine, 'capture_lease> {
    transaction: Option<redb::ReadTransaction>,
    actual_shared: Option<&'engine SharedRedb>,
    _capture_lease: Option<&'capture_lease ExclusiveLease>,
}
impl<'engine, 'capture_lease> StartupOriginReadV1<'engine, 'capture_lease> {
    pub(crate) fn from_engine(
        shared: &'engine SharedRedb,
        lease: &'capture_lease ExclusiveLease,
    ) -> Result<Self, StorageError> {
        shared.verify_mutation_lease_affinity(lease)?;
        let transaction = shared
            .database
            .begin_read()
            .map_err(crate::error::transaction_error)?;
        Ok(Self {
            transaction: Some(transaction),
            actual_shared: Some(shared),
            _capture_lease: Some(lease),
        })
    }
    // Background validation may retain the actual engine read without a
    // mutation lease, but that observation cannot mint a missing origin proof.
    pub(crate) fn from_engine_without_origin_authority(
        shared: &SharedRedb,
    ) -> Result<Self, StorageError> {
        let transaction = shared
            .database
            .begin_read()
            .map_err(crate::error::transaction_error)?;
        Ok(Self {
            transaction: Some(transaction),
            actual_shared: None,
            _capture_lease: None,
        })
    }
    // Existing verified materialization keeps its own meaning. It never gains
    // an actual-engine origin brand merely from equal IDs, hashes or bytes.
    pub(crate) fn from_materialized(history: crate::journal::VerifiedMaterializedHistory) -> Self {
        Self {
            transaction: Some(history.into_read()),
            actual_shared: None,
            _capture_lease: None,
        }
    }
    pub(crate) fn read(&self) -> Result<&redb::ReadTransaction, StorageError> {
        self.transaction.as_ref().ok_or_else(invariant)
    }
    pub(crate) fn into_read(mut self) -> Result<redb::ReadTransaction, StorageError> {
        self.transaction.take().ok_or_else(invariant)
    }
}
impl Drop for StartupOriginReadV1<'_, '_> {
    fn drop(&mut self) {}
}

pub(crate) fn validate_bounded_cold_origin<'engine, 'lease>(
    observation: &StartupOriginReadV1<'engine, '_>,
    lease: &'lease ExclusiveLease,
) -> Result<ValidatedColdOriginV1<'engine, 'lease>, StorageError> {
    let transaction = observation.read()?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let declared = crate::catalog_dispatch::declared_catalog(&meta)?;
    if declared != Some(AuthoritativeStateCatalogV3.digest()) {
        if let Some(shared) = observation.actual_shared {
            shared.verify_mutation_lease_affinity(lease)?;
            validate_same_owner_cold_origin(&shared.database, transaction)?;
        }
        return Ok(ValidatedColdOriginV1 {
            prepared: None,
            _lease: lease,
        });
    }
    let shared = observation
        .actual_shared
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
    shared.verify_mutation_lease_affinity(lease)?;
    let owner = &shared.database;
    if owner.native_origin_context().anchor_key().is_some() {
        validate_same_owner_cold_origin(owner, transaction)?;
        return Ok(ValidatedColdOriginV1 {
            prepared: None,
            _lease: lease,
        });
    }
    // These existing exact roots and the following fact share the actual native
    // owner's retained read. No caller selects an alternate owner or resolver.
    let history = crate::changelog_v3_roots::read_checkpoint_roots(transaction)?
        .ok_or_else(|| value_error(()))?;
    let lineage = history.lineage();
    if lineage.catalog_digest() != AuthoritativeStateCatalogV3.digest()
        || lineage.history_incarnation() != 1
        || lineage.leadership_epoch() != LeadershipEpochV1::initial()
    {
        // Cut and inheritance need their actual native cold owners; neither a
        // decoded fact nor a familiar database identity substitutes for them.
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    let administration = history
        .anchor()
        .frontier()
        .administration()
        .ok_or_else(|| value_error(()))?;
    let key = crate::keys::encode_audit_key(administration);
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let encoded = audit
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let budget = owner.source_working_budget();
    let decoded = decode_native_catalog_transition_admitted_v1(encoded.value(), budget)
        .map_err(source_decode_error)?;
    let origin = decoded.value();
    let parts = origin.parts();
    if !matches!(&parts.origin, NativeCatalogTransitionOriginV1::FreshV3) {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    if parts.database_id != lineage.database_id()
        || parts.publication_binding.history_incarnation != lineage.history_incarnation()
        || parts.administration_sequence != administration
        || parts.target_catalog_digest != lineage.catalog_digest()
    {
        return Err(value_error(()));
    }
    // The same closed producer recipe retains its complete graph reservation
    // until after all metadata, mutations and the reconstructed receipt drop.
    // The original registry digest belongs to the fact, not this binary.
    let _work = budget.reserve(work_bound()?).map_err(unavailable)?;
    let metadata = fresh_initial_metadata(origin)?;
    let receipt = fresh_activation_receipt(origin, &metadata, encoded.value())?;
    let anchor = ChangelogHistoryPointV3::from_receipt(&receipt).map_err(value_error)?;
    if anchor != history.anchor() {
        return Err(value_error(()));
    }
    // The original frame may be reclaimed. The exact canonical fact plus the
    // sole net-ten recipe reconstructs the immutable anchor checked above.
    // Existing full retained-history and population scrub remain unchanged.
    let allocator_bytes = meta
        .get("projection_checkpoint_ordinal/v1")
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let allocator =
        decode_projection_checkpoint_ordinal_allocator_inline_v1(allocator_bytes.value())
            .map_err(value_error)?;
    if allocator.value().allocation_history_incarnation() != lineage.history_incarnation() {
        return Err(value_error(()));
    }
    // Do not pin the current allocator ordinal, current tail or current registry
    // to their historical initial values: all may advance after installation.
    let catalog = meta
        .get(metadata_key(
            AuthoritativeNamespaceV1::AuthoritativeStateCatalog,
        )?)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let incarnation = meta
        .get(META_HISTORY_INCARNATION)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let leadership = meta
        .get(metadata_key(AuthoritativeNamespaceV1::LeadershipEpoch)?)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let stamps = LineageStampsV1::from_checked_postimages(
        catalog.value(),
        incarnation.value(),
        leadership.value(),
    )?;
    let digest = native_catalog_origin_envelope_digest_admitted_v1(encoded.value(), budget)
        .map_err(source_decode_error)?;
    let prepared = context::prepare(
        owner,
        CheckedOriginPostimagesV1 {
            lineage,
            original_origin: NativeCatalogOriginReferenceV1::new(administration, digest),
            anchor,
            predecessor_cut: None,
            stamps,
            administration,
            key,
            role: ControlRoleV1::CatalogOrigin,
            encoded: encoded.value(),
        },
    )?;
    Ok(ValidatedColdOriginV1 {
        prepared: Some(prepared),
        _lease: lease,
    })
}

fn source_decode_error(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            storage_error(StorageErrorKind::Unavailable)
        }
        ProjectionSourceRecordDecodeErrorV1::Codec(error) => codec_error(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::DatabaseId;
    // req: REP-002, REP-003, REC-001, STO-012, PRJ-004
    fn reopen_genuine_fresh(root: &crate::test_path::ScopedDirectory) -> crate::RedbStore {
        let path = root.join("database.redb");
        {
            let mut store = crate::RedbStore::open(&path).unwrap();
            let database =
                DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10])
                    .unwrap();
            store
                .initialize_projection_source_fixture(database)
                .unwrap();
        }
        crate::RedbStore::open(path).unwrap()
    }

    #[test]
    // req: REP-002, REC-001, STO-012, PRJ-004
    fn new_engine_origin_candidate_is_nonauthoritative_until_complete_bounded_finish() {
        let root = crate::test_path::ScopedDirectory::new("cold-origin-consumption");
        let store = reopen_genuine_fresh(&root);
        let owner = &store.shared.database;
        let lease = store.acquire_mutation_lease().unwrap();
        let observation = StartupOriginReadV1::from_engine(&store.shared, &lease).unwrap();
        let read = observation.read().unwrap();
        assert!(owner.native_origin_context().anchor_key().is_none());
        {
            let candidate = validate_bounded_cold_origin(&observation, &lease).unwrap();
            assert!(owner.native_origin_context().anchor_key().is_none());
            drop(candidate);
            assert!(owner.native_origin_context().anchor_key().is_none());
        }
        validate_bounded_cold_origin(&observation, &lease)
            .unwrap()
            .install_after_complete_bounded_validation()
            .unwrap();
        validate_same_owner_cold_origin(owner, read).unwrap();
        let budget = owner.source_working_budget();
        let before = budget.observation().unwrap();
        let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.owned_bytes
            - before.reserved_bytes;
        let pressure = budget.reserve(remaining).unwrap();
        let saturated = budget.observation().unwrap();
        validate_same_owner_cold_origin(owner, read).unwrap();
        assert_eq!(budget.observation().unwrap(), saturated);
        drop(pressure);
    }

    #[test]
    // req: REP-002, REC-001, STO-012, PRJ-004
    fn cold_origin_refuses_foreign_mutation_lease_without_installing_context() {
        let first = crate::test_path::ScopedDirectory::new("cold-origin-first-lease");
        let second = crate::test_path::ScopedDirectory::new("cold-origin-foreign-lease");
        let store = reopen_genuine_fresh(&first);
        let foreign = crate::RedbStore::open(second.join("database.redb")).unwrap();
        let foreign_lease = foreign.acquire_mutation_lease().unwrap();
        let owner = &store.shared.database;
        let lease = store.acquire_mutation_lease().unwrap();
        let observation = StartupOriginReadV1::from_engine(&store.shared, &lease).unwrap();
        let before = owner.source_working_budget().observation().unwrap();
        assert!(
            matches!(StartupOriginReadV1::from_engine(&store.shared, &foreign_lease),
            Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
        );
        assert!(
            matches!(validate_bounded_cold_origin(&observation, &foreign_lease),
            Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
        );
        assert!(owner.native_origin_context().anchor_key().is_none());
        assert_eq!(owner.source_working_budget().observation().unwrap(), before);
    }

    #[test]
    // req: REP-002, REC-001, STO-012, PRJ-004
    fn cold_origin_capacity_refusal_never_installs_partial_authority() {
        let root = crate::test_path::ScopedDirectory::new("cold-origin-capacity-refusal");
        let store = reopen_genuine_fresh(&root);
        let owner = &store.shared.database;
        let lease = store.acquire_mutation_lease().unwrap();
        let observation = StartupOriginReadV1::from_engine(&store.shared, &lease).unwrap();
        let budget = owner.source_working_budget();
        let before = budget.observation().unwrap();
        let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.owned_bytes
            - before.reserved_bytes;
        let pressure = budget.reserve(remaining).unwrap();
        let saturated = budget.observation().unwrap();
        assert!(matches!(validate_bounded_cold_origin(&observation, &lease),
            Err(error) if error.kind() == StorageErrorKind::Unavailable));
        assert!(owner.native_origin_context().anchor_key().is_none());
        assert_eq!(budget.observation().unwrap(), saturated);
        drop(pressure);
        assert_eq!(budget.observation().unwrap(), before);
        validate_bounded_cold_origin(&observation, &lease)
            .unwrap()
            .install_after_complete_bounded_validation()
            .unwrap();
    }
    #[test]
    // req: REP-002, REC-001, STO-012, PRJ-004
    fn fresh_reconstruction_uses_original_registry_digest_and_pending_envelope() {
        let root = crate::test_path::ScopedDirectory::new("cold-origin-original-registry");
        let store = reopen_genuine_fresh(&root);
        let owner = &store.shared.database;
        let lease = store.acquire_mutation_lease().unwrap();
        let observation = StartupOriginReadV1::from_engine(&store.shared, &lease).unwrap();
        let read = observation.read().unwrap();
        let key = crate::keys::encode_audit_key(AdministrationSequence::first());
        let audit = read.open_table(AUDIT).unwrap();
        let bytes = audit.get(key.as_slice()).unwrap().unwrap();
        let original = decode_native_catalog_transition_admitted_v1(
            bytes.value(),
            owner.source_working_budget(),
        )
        .unwrap();
        let _work = owner
            .source_working_budget()
            .reserve(work_bound().unwrap())
            .unwrap();
        let original = original.value().parts();
        let historical_digest = riffdb_types::SchemaHash::from_bytes([0x79; 32]);
        assert_ne!(historical_digest, current_record_registry_digest());
        // Synthetic structural input tests the sole recipe, not a persisted
        // transition, origin installer or authority-acquisition constructor.
        let historical = NativeCatalogTransitionV1::new(NativeCatalogTransitionPartsV1 {
            database_id: original.database_id,
            administration_sequence: original.administration_sequence,
            publication_binding: original.publication_binding,
            target_catalog_digest: original.target_catalog_digest,
            target_registry_digest: historical_digest,
            initial_source_allocator: original.initial_source_allocator,
            origin: NativeCatalogTransitionOriginV1::FreshV3,
        })
        .unwrap();
        let metadata = fresh_initial_metadata(&historical).unwrap();
        let expected =
            encode_record_registry_v3(StoredRecordRegistry::pending(historical_digest)).unwrap();
        assert_eq!(metadata[4].as_slice(), expected.as_bytes());
        let current = encode_record_registry_v3(StoredRecordRegistry::pending(
            current_record_registry_digest(),
        ))
        .unwrap();
        assert_ne!(metadata[4].as_slice(), current.as_bytes());
        assert!(owner.native_origin_context().anchor_key().is_none());
    }
}
