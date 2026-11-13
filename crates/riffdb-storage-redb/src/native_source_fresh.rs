//! Explicit Empty-owned V3 fixture installation. Production initialization stays
//! on its existing branch until complete native lifecycle qualification.
use crate::{
    error::{codec_error, precommit_storage_error, storage_error, table_error},
    layout::*,
};
use redb::{Database, TableDefinition};
use riffdb_storage_api::{proto_codec::*, *};
use riffdb_types::{
    AdministrationSequence, DualFrontier, LeadershipEpochV1,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

mod cold;
mod context;
mod source_observation;
pub(crate) use cold::{
    StartupOriginReadV1, install_actual_origin_complement, validate_bounded_cold_origin,
};
pub(crate) use context::OriginContextCellV1;
use context::{CheckedOriginPostimagesV1, ControlRoleV1, LineageStampsV1, PreparedOriginContextV1};
pub(crate) use source_observation::SourceOriginObservationV1;

const SCALAR_CAP: usize = 512;
const ORIGIN_CAP: usize = 2048;
const INITIAL_KEYS: [&str; 9] = [
    META_FORMAT_VERSION,
    META_DATABASE_ID,
    META_APPLICATION_SEQUENCE,
    META_ADMINISTRATION_SEQUENCE,
    META_RECORD_REGISTRY,
    META_HISTORY_INCARNATION,
    META_CHANGELOG_V2_ROTATION_RECEIPT,
    META_INDEX_EPOCH_ROWS_REPAIRED,
    "projection_checkpoint_ordinal/v1",
];

/// This result retains the original actual-engine borrow and admitted context.
/// The Empty owner installs it only after its existing durable commit succeeds.
pub(crate) struct StagedEmptyV3<'engine> {
    prepared: PreparedOriginContextV1<'engine, Database>,
    source_usage: crate::source_usage_domain::PreparedFreshSourceUsage<'engine>,
}
impl StagedEmptyV3<'_> {
    pub(crate) fn install_after_durable_commit(self) -> Result<(), StorageError> {
        // Installation checks actual engine affinity again; no detached proof.
        context::install_prepared(self.prepared)?;
        self.source_usage.install_after_durable_commit()
    }
}

fn scalar(value: CanonicalStoredEnvelopeV1) -> Result<Vec<u8>, StorageError> {
    if value.as_bytes().len() > SCALAR_CAP {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    Ok(value.into_bytes())
}
fn scalar_result(
    value: Result<CanonicalStoredEnvelopeV1, DurableCodecError>,
) -> Result<Vec<u8>, StorageError> {
    scalar(value.map_err(codec_error)?)
}
fn value_error(_: impl Sized) -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}
fn unavailable(_: impl Sized) -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn namespace(key: &str) -> Result<AuthoritativeNamespaceV3, StorageError> {
    AuthoritativeStateCatalogV3
        .lookup("meta", key.as_bytes())
        .ok_or_else(invariant)
}
fn metadata_key(namespace: AuthoritativeNamespaceV1) -> Result<&'static str, StorageError> {
    namespace.metadata_key().ok_or_else(invariant)
}

// Complete closed inventory before constructing any Fresh-specific heap graph.
// Existing compact encoding emits one final Vec; receipt hashing streams bytes.
fn work_bound() -> Result<u64, StorageError> {
    let mut bytes = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1;
    let mut add = |requested: usize, alignment: usize| -> Result<(), StorageError> {
        bytes = bytes
            .checked_add(
                owned_allocation_capacity_bound_v1(requested, alignment).map_err(value_error)?,
            )
            .ok_or_else(invariant)?;
        Ok(())
    };
    // Eight encoded initial metadata values (one raw byte is static), six
    // nonrecursive roots. These owners may coexist until transaction staging.
    for _ in 0..14 {
        add(SCALAR_CAP, align_of::<u8>())?;
    }
    add(1, align_of::<u8>())?; // Raw index-repair marker Vec.
    // Largest one-at-a-time generated scalar/root mapper: current history has
    // database16, catalog32, and three position hashes32. Other mappers are
    // strict subsets (rotation16+32, V3 Active16+32, registry/catalog32).
    add(16, align_of::<u8>())?;
    for _ in 0..4 {
        add(32, align_of::<u8>())?;
    }
    add(
        10 * size_of::<AuthoritativeMutationV3>(),
        align_of::<AuthoritativeMutationV3>(),
    )?;
    let mut receipt_max = 160usize + 44 * 10;
    for key in INITIAL_KEYS {
        add(key.len(), align_of::<u8>())?;
        add(SCALAR_CAP, align_of::<u8>())?;
        receipt_max = receipt_max
            .checked_add(key.len() + SCALAR_CAP)
            .ok_or_else(invariant)?;
    }
    add(9, align_of::<u8>())?;
    add(ORIGIN_CAP, align_of::<u8>())?;
    receipt_max = receipt_max
        .checked_add(9 + ORIGIN_CAP)
        .ok_or_else(invariant)?;
    add(receipt_max, align_of::<u8>())?;
    // All array/message/receipt owners are stack-resident, counted explicitly.
    bytes
        .checked_add(
            u64::try_from(
                size_of::<riffdb_proto::storage::v1::StoredChangelogHistoryStateV3>()
                    + size_of::<AuthoritativeTransactionV3>()
                    + size_of::<[Vec<u8>; 9]>()
                    + size_of::<[(&str, Vec<u8>); 6]>(),
            )
            .map_err(value_error)?,
        )
        .ok_or_else(invariant)
}

/// The private store token names the same transaction classified Empty under
/// its mutation lease; no database, catalog selector or replacement tx is taken.
#[cfg_attr(not(any(test, feature = "test-fixtures")), allow(dead_code))]
pub(crate) fn stage_empty_v3<'engine>(
    empty: &crate::store::ProvenEmptyInitializationV1<'engine, '_>,
) -> Result<StagedEmptyV3<'engine>, StorageError> {
    use AuthoritativeNamespaceV1 as Old;
    let transaction = empty.transaction();
    let owner = empty.owner();
    let database = empty.database_id();
    let budget = owner.source_working_budget();
    let _work = budget.reserve(work_bound()?).map_err(unavailable)?;
    // Empty classification precedes table creation, and this branch writes each
    // final initial stamp once. No transient PRE registry is a durable preimage.
    create_all_tables(transaction).map_err(table_error)?;
    for source in [
        AuthoritativeNamespaceV3::ProjectionSourceRows,
        AuthoritativeNamespaceV3::ProjectionSourceObjects,
        AuthoritativeNamespaceV3::ProjectionSourceRoots,
        AuthoritativeNamespaceV3::ProjectionSourceMaintenance,
    ] {
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(source.table());
        transaction.open_table(definition).map_err(table_error)?;
    }
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .map_err(table_error)?;
    transaction
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .map_err(table_error)?;
    let lineage = ChangelogLineageV3::new_with_catalog(
        database,
        1,
        LeadershipEpochV1::initial(),
        AuthoritativeStateCatalogV3.digest(),
    )
    .map_err(value_error)?;
    let administration = AdministrationSequence::first();
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: database,
        history_incarnation: 1,
        predecessor: None,
        sequence: ChangelogTransactionSequence::new(1).ok_or_else(invariant)?,
        predecessor_frontier: DualFrontier::INITIAL,
        covered_frontier: DualFrontier::new(None, Some(administration)),
        prior_history_hash: [0; 32],
    };
    let source_allocator =
        ProjectionCheckpointOrdinalAllocatorV1::initial(1).map_err(value_error)?;
    type Allocation =
        riffdb_types::exact_provider_budget_v1::ProviderAllocation<NativeCatalogTransitionV1>;
    let input_bound = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
        + u64::try_from(
            size_of::<NativeCatalogTransitionV1>()
                + 2 * size_of::<Allocation>()
                + 2 * align_of::<Allocation>()
                + 1024,
        )
        .map_err(value_error)?;
    let input_work = budget.reserve(input_bound).map_err(unavailable)?;
    let origin = NativeCatalogTransitionV1::new(NativeCatalogTransitionPartsV1 {
        database_id: database,
        administration_sequence: administration,
        publication_binding: binding,
        target_catalog_digest: lineage.catalog_digest(),
        target_registry_digest: current_record_registry_digest(),
        initial_source_allocator: source_allocator,
        origin: NativeCatalogTransitionOriginV1::FreshV3,
    })
    .map_err(value_error)?;
    let origin = input_work.settle_owned_value(origin).map_err(unavailable)?;
    let origin_bytes =
        encode_native_catalog_transition_admitted_v1(&origin, budget).map_err(value_error)?;
    let origin_digest =
        native_catalog_origin_envelope_digest_admitted_v1(origin_bytes.as_bytes(), budget)
            .map_err(value_error)?;
    let audit_key = crate::keys::encode_audit_key(administration);
    let metadata = fresh_initial_metadata(&origin)?;
    let receipt = fresh_activation_receipt(&origin, &metadata, origin_bytes.as_bytes())?;

    let point = ChangelogHistoryPointV3::from_receipt(&receipt).map_err(value_error)?;
    let history =
        ChangelogHistoryStateV3::new(lineage, point, point, point).map_err(value_error)?;
    let (_, physical_next) = ChangelogTransactionAllocator::initial()
        .allocate_one()
        .map_err(value_error)?;
    let admission =
        ReplicationPrimaryAdmissionV1::active_catalog_bound(lineage).map_err(value_error)?;
    let roots = [
        (
            metadata_key(Old::AuthoritativeStateCatalog)?,
            scalar_result(encode_authoritative_state_catalog_v3(
                AuthoritativeStateCatalogV3,
            ))?,
        ),
        (
            metadata_key(Old::LeadershipEpoch)?,
            scalar_result(encode_leadership_epoch_v1(lineage.leadership_epoch()))?,
        ),
        (
            metadata_key(Old::ChangelogHistoryState)?,
            scalar_result(encode_changelog_history_state_v3(history))?,
        ),
        (
            metadata_key(Old::NextChangelogTransaction)?,
            scalar_result(encode_changelog_transaction_allocator_v3(physical_next))?,
        ),
        (
            metadata_key(Old::ReplicationFollowerState)?,
            scalar_result(encode_replication_follower_state_v3(
                ReplicationFollowerStateV3::detached(),
            ))?,
        ),
        (
            crate::primary_admission_roots::key()?,
            scalar_result(encode_replication_primary_admission(&admission))?,
        ),
    ];
    let stamps = LineageStampsV1::from_checked_postimages(&roots[0].1, &metadata[5], &roots[1].1)?;
    let prepared = context::prepare(
        owner,
        CheckedOriginPostimagesV1 {
            lineage,
            original_origin: NativeCatalogOriginReferenceV1::new(administration, origin_digest),
            anchor: point,
            predecessor_cut: None,
            stamps,
            administration,
            key: audit_key,
            role: ControlRoleV1::CatalogOrigin,
            encoded: origin_bytes.as_bytes(),
        },
    )?;
    let encoded_receipt = receipt.encode().map_err(value_error)?;
    // Exact net10 validation already established the closed after bytes. The
    // genuine Empty transaction proves Source absence before any Source insert.
    let source_usage = crate::source_usage_domain::prepare_fresh_source_usage(empty, &metadata[8])?;
    for mutation in receipt.mutations() {
        crate::changelog_v3_write::apply_mutation(transaction, mutation)?;
    }
    let mut meta = transaction.open_table(META).map_err(table_error)?;
    for (key, value) in &roots {
        meta.insert(*key, value.as_slice())
            .map_err(precommit_storage_error)?;
    }
    drop(meta);
    transaction
        .open_table(crate::changelog_v3_activation::HISTORY)
        .map_err(table_error)?
        .insert(
            binding.sequence.get().to_be_bytes().as_slice(),
            encoded_receipt.as_slice(),
        )
        .map_err(precommit_storage_error)?;
    Ok(StagedEmptyV3 {
        prepared,
        source_usage,
    })
}

// The sole closed Fresh postimage recipe is shared by proven-Empty publication
// and bounded cold anchor reconstruction. A decoded fact is not write authority.
fn fresh_initial_metadata(
    origin: &NativeCatalogTransitionV1,
) -> Result<[Vec<u8>; 9], StorageError> {
    let parts = origin.parts();
    if !matches!(&parts.origin, NativeCatalogTransitionOriginV1::FreshV3)
        || parts.publication_binding.history_incarnation != 1
        || parts.administration_sequence != AdministrationSequence::first()
    {
        return Err(value_error(()));
    }
    let database = parts.database_id;
    let administration = parts.administration_sequence;
    let rotation = ChangelogV2RotationReceipt::new(database, 1, DualFrontier::INITIAL, [0; 32])
        .map_err(value_error)?;
    Ok([
        scalar_result(encode_storage_format_version_v1(StorageFormatVersion::V2))?,
        scalar_result(encode_database_identity_v1(database))?,
        scalar_result(encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::initial(),
        ))?,
        scalar_result(encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::Next(
                administration.checked_next().ok_or_else(invariant)?,
            ),
        ))?,
        scalar_result(encode_record_registry_v3(StoredRecordRegistry::pending(
            parts.target_registry_digest,
        )))?,
        scalar_result(encode_history_incarnation_v1(1))?,
        scalar_result(encode_changelog_v2_rotation_receipt_v1(rotation))?,
        // Raw initial index-repair marker has its frozen one-byte encoding.
        vec![1],
        scalar_result(encode_projection_checkpoint_ordinal_allocator_v1(
            &parts.initial_source_allocator,
        ))?,
    ])
}

fn fresh_activation_receipt(
    origin: &NativeCatalogTransitionV1,
    metadata: &[Vec<u8>; 9],
    origin_encoded: &[u8],
) -> Result<AuthoritativeTransactionV3, StorageError> {
    use AuthoritativeNamespaceV1 as Old;
    let parts = origin.parts();
    let binding = parts.publication_binding;
    let audit_key = crate::keys::encode_audit_key(parts.administration_sequence);
    let mut mutations = Vec::with_capacity(10);
    for (key, value) in INITIAL_KEYS.into_iter().zip(metadata) {
        mutations.push(
            AuthoritativeMutationV3::put(namespace(key)?, key.as_bytes(), None, value)
                .map_err(value_error)?,
        );
    }
    mutations.push(
        AuthoritativeMutationV3::put(Old::Audit, audit_key.as_slice(), None, origin_encoded)
            .map_err(value_error)?,
    );
    mutations.sort_unstable_by(|left, right| {
        (left.namespace(), left.key()).cmp(&(right.namespace(), right.key()))
    });
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        binding,
        ChangelogAttributionV3::V3Activation,
        mutations,
        parts.target_catalog_digest,
    )
    .map_err(value_error)?;
    validate_fresh_recipe(&receipt, metadata, origin_encoded)?;
    Ok(receipt)
}

// Explicit closed Fresh recipe: generic missing-record inference is forbidden.
fn validate_fresh_recipe(
    receipt: &AuthoritativeTransactionV3,
    metadata: &[Vec<u8>; 9],
    origin: &[u8],
) -> Result<(), StorageError> {
    let binding = receipt.binding();
    let first = AdministrationSequence::first();
    if receipt.attribution() != ChangelogAttributionV3::V3Activation
        || receipt.mutations().len() != 10
        || binding.predecessor.is_some()
        || binding.sequence.get() != 1
        || binding.prior_history_hash != [0; 32]
        || binding.predecessor_frontier != DualFrontier::INITIAL
        || binding.covered_frontier != DualFrontier::new(None, Some(first))
    {
        return Err(value_error(()));
    }
    let audit_key = crate::keys::encode_audit_key(first);
    for mutation in receipt.mutations() {
        if !mutation.matches_prior(None) || mutation.expected_hash().is_some() {
            return Err(value_error(()));
        }
        if mutation.namespace() == AuthoritativeNamespaceV1::Audit {
            if mutation.key() != audit_key || mutation.value() != Some(origin) {
                return Err(value_error(()));
            }
        } else {
            let exact = INITIAL_KEYS.into_iter().zip(metadata).any(|(key, value)| {
                namespace(key).is_ok_and(|namespace| namespace == mutation.namespace())
                    && mutation.key() == key.as_bytes()
                    && mutation.value() == Some(value.as_slice())
            });
            if !exact {
                return Err(value_error(()));
            }
        }
    }
    Ok(())
}

/// Cold traversal may reuse canonical evidence only on the exact actual engine
/// where Fresh installation minted it. Reopen has no context and must refuse.
pub(crate) fn validate_same_owner_cold_origin(
    owner: &crate::engine_owner::EngineOwner<Database>,
    transaction: &redb::ReadTransaction,
) -> Result<(), StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let declared = crate::catalog_dispatch::declared_catalog(&meta)?;
    if declared != Some(AuthoritativeStateCatalogV3.digest()) {
        return if owner.native_origin_context().anchor_key().is_none() {
            Ok(())
        } else {
            Err(value_error(()))
        };
    }
    let key = owner
        .native_origin_context()
        .anchor_key()
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let encoded = audit
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    if !checked_origin_row(owner, transaction, &key, encoded.value())? {
        return Err(value_error(()));
    }
    Ok(())
}

/// Recognizes exactly the immutable, already-canonical Fresh origin artifact.
/// Other records retain their ordinary decoder. No context transfers to a new
/// engine, and this function never mints cold or writer authority.
pub(crate) fn checked_origin_row(
    owner: &crate::engine_owner::EngineOwner<Database>,
    transaction: &redb::ReadTransaction,
    key: &[u8],
    encoded: &[u8],
) -> Result<bool, StorageError> {
    if !checked_origin_role(encoded)? {
        return Ok(false);
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
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
    checked_origin_controls(
        owner,
        key,
        encoded,
        catalog.value(),
        incarnation.value(),
        leadership.value(),
    )
}

// Select only the already-installed context's fixed Audit key on this actual
// engine. The scalar is neither origin validation nor Source authority; callers
// must still verify the exact retained bytes and lineage through the sole checker.
pub(crate) fn source_origin_anchor_key_v1(
    owner: &crate::engine_owner::EngineOwner<Database>,
) -> Result<[u8; 9], StorageError> {
    owner
        .native_origin_context()
        .anchor_key()
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))
}

// Only the fields-private originating Source read context calls this with its
// own actual ports and retained root read. This fixed reference grants no Source
// selection/pin by itself and never mints or transfers an engine hot proof.
#[cfg(any(test, feature = "projection-source-driver"))]
pub(crate) fn checked_captured_source_origin_v1(
    owner: &crate::engine_owner::EngineOwner<Database>,
    transaction: &redb::ReadTransaction,
) -> Result<NativeCatalogOriginReferenceV1, StorageError> {
    validate_same_owner_cold_origin(owner, transaction)?;
    let key = owner
        .native_origin_context()
        .anchor_key()
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
    let audit = transaction.open_table(AUDIT).map_err(table_error)?;
    let encoded = audit
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(|| value_error(()))?;
    let meta = transaction.open_table(META).map_err(table_error)?;
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
    owner.native_origin_context().checked_origin_reference(
        &key,
        encoded.value(),
        catalog.value(),
        incarnation.value(),
        leadership.value(),
    )
}

fn checked_origin_role(encoded: &[u8]) -> Result<bool, StorageError> {
    let Ok(role) = riffdb_proto::envelope::inspect_readable_record_role_v1(encoded) else {
        return Ok(false);
    };
    if role == "riffdb.storage.v1.StoredSourceInheritanceAdministrationV1" {
        return Err(storage_error(StorageErrorKind::Unavailable));
    }
    if role != "riffdb.storage.v1.StoredAuthoritativeCatalogTransitionV1" {
        return Ok(false);
    }
    Ok(true)
}
fn checked_origin_controls(
    owner: &crate::engine_owner::EngineOwner<Database>,
    key: &[u8],
    encoded: &[u8],
    catalog: &[u8],
    incarnation: &[u8],
    leadership: &[u8],
) -> Result<bool, StorageError> {
    let sequence = owner.native_origin_context().check_control_head(
        key,
        encoded,
        catalog,
        incarnation,
        leadership,
    )?;
    if crate::keys::decode_audit_key(key).map_err(value_error)? != sequence {
        return Err(value_error(()));
    }
    Ok(true)
}
// Called only by the fields-private actual logical Source read. Fixed reference
// inspection reuses the same policy and existing owner proof; it cannot mint one.
pub(crate) fn checked_source_origin_controls_v1(
    owner: &crate::engine_owner::EngineOwner<Database>,
    key: &[u8],
    encoded: &[u8],
    catalog: &[u8],
    incarnation: &[u8],
    leadership: &[u8],
) -> Result<NativeCatalogOriginReferenceV1, StorageError> {
    if !checked_origin_role(encoded)?
        || !checked_origin_controls(owner, key, encoded, catalog, incarnation, leadership)?
    {
        return Err(value_error(()));
    }
    owner.native_origin_context().checked_origin_reference(
        key,
        encoded,
        catalog,
        incarnation,
        leadership,
    )
}

#[cfg(test)]
#[path = "native_source_fresh/tests.rs"]
mod tests;
