//! Private durable clean-close lifecycle state machine (ADR-0157).

use redb::{ReadTransaction, ReadableTable, ReadableTableMetadata};
use riffdb_proto::storage::v1::{
    StoredCleanCloseLifecycleStateV1 as WireState, StoredCleanCloseLifecycleV1,
};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, MAX_RETAINED_QUERY_MODULES, StorageError, StoredContractBundleV1,
};
use riffdb_types::{DatabaseId, HashDomain, SchemaHash, hash, hash_query_module};

use crate::codec;
use crate::error::{codec_error, precommit_storage_error, storage_error, table_error};
use crate::keys;
use crate::layout::{
    CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, CONTRACT_BUNDLES, META, META_ADMINISTRATION_SEQUENCE,
    META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP, META_CHANGELOG_V2_ROTATION_RECEIPT,
    META_DATABASE_ID, META_FORMAT_VERSION, META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED, META_RECORD_REGISTRY, META_RETENTION_HOLDS,
    META_RETENTION_WATERMARK, QUERY_MODULE_ACTIVE, QUERY_MODULES,
};

const LIFECYCLE_HASH_LABEL: &[u8] = b"riffdb-clean-close-lifecycle-v1\0";
const LIFECYCLE_HASH_VERSION: u32 = 1;
const BOUNDED_ROOT_HASH_LABEL: &[u8] = b"riffdb-clean-close-bounded-roots-v1\0";
const BOUNDED_ROOT_HASH_VERSION: u32 = 1;
const MAX_BINDING_PREIMAGE_BYTES: usize = 16 * 1024 * 1024;
const META_TABLE_TAG: u8 = 0x01;
const CATALOG_TABLE_TAG: u8 = 0x02;
const QUERY_ACTIVE_TABLE_TAG: u8 = 0x03;

const BOUNDED_META_KEYS: [&str; 11] = [
    META_FORMAT_VERSION,
    META_DATABASE_ID,
    META_APPLICATION_SEQUENCE,
    META_ADMINISTRATION_SEQUENCE,
    META_CAPABILITY_BOOTSTRAP,
    META_RECORD_REGISTRY,
    META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED,
    META_RETENTION_WATERMARK,
    META_RETENTION_HOLDS,
    META_CHANGELOG_V2_ROTATION_RECEIPT,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanCloseCodecError {
    Invalid,
    GenerationExhausted,
}

/// Why the ADR-0157 clean-close certificate did not admit bounded startup.
///
/// Every variant costs exactly one thing — the next open takes the complete
/// validation pass — and every variant used to produce that outcome silently.
/// On a large database the complete pass is tens of minutes of SHA-256 and
/// record decoding, so "slow start" was the only observable and every
/// precondition was indistinguishable from outside the engine. This enum
/// carries no path, key, value, or identity, so naming it leaks nothing.
///
/// Observability only: declining still means full validation, and nothing here
/// may widen what bounded startup accepts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanCloseDeclineReason {
    /// This open created the durable unit, so redb necessarily rebuilt
    /// allocator state for a file that did not exist. Benign and unavoidable:
    /// there is no history to fast-path over. Split out from
    /// [`Self::EngineRepairedAtOpen`] so a healthy first boot does not report
    /// the same reason as a pre-existing database whose previous close left no
    /// usable allocator state — otherwise the signal cries wolf on every new
    /// database, which is how a diagnostic stops being read.
    EngineInitializedAtOpen,
    /// redb ran its own repair pass at open over a pre-existing file; no prior
    /// certificate can bind the repaired roots.
    ///
    /// This is the one decline whose cost is doubled: redb's repair rebuilds
    /// allocator state over the whole file (up to three full scans) and THEN
    /// the complete RiffDB validation pass runs. Both are silent.
    ///
    /// In redb 4.2.0 the repair is skipped only when the open finds a persisted
    /// `allocator_state` system table, which is written by `close_database`
    /// from `redb::Database::drop` under `quick_repair`. RiffDB never enables
    /// `quick_repair` on its own writes (see `open_after_format_preflight`), so
    /// that drop-time commit is the only thing that leaves one — and its failure
    /// is swallowed by redb, invisible here because redb's `logging` feature is
    /// off. Read this reason as "the previous handle's close left no usable
    /// allocator state".
    ///
    /// redb's repair has two branches: one that only rebuilds in-memory
    /// allocator state over unchanged roots, and one that rolls back a
    /// partially committed transaction. They are not distinguished here, so
    /// this declines for both.
    EngineRepairedAtOpen,
    /// No lifecycle record at all (never gracefully closed, or a shutdown that
    /// failed to write it — the write side is `let _ =` at its call site).
    RecordAbsent,
    /// A lifecycle record exists but does not decode.
    RecordDecodeFailed,
    /// The record binds a different database identity.
    DatabaseIdMismatch,
    /// The record binds a different history incarnation.
    IncarnationMismatch,
    /// The record decodes and matches identity, but its state is `Dirty` — an
    /// open consumed the certificate and no clean close replaced it.
    StateNotClean,
    /// The final journal boundary did not verify: a non-authoritative extent
    /// name is present, the active extent is missing, its header disagrees with
    /// the durable application/administration frontier, or its tail is not the
    /// empty clean-close tail.
    JournalBoundaryUnverified,
    /// The bounded meta/catalog/query-module roots could not be re-hashed
    /// (corrupt row or a bound exceeded).
    BoundedRootsUnavailable,
    /// Everything decoded and verified, but the recomputed bounded-state
    /// binding differs from the one the certificate recorded.
    BindingMismatch,
    /// The certificate verified, but an in-flight `Delivering` outbox entry was
    /// positively observed.
    ///
    /// A clean close means every writer and delivery lane stopped, so no
    /// delivery attempt can still hold a lease. Observing one contradicts the
    /// certificate about state the certificate does not cover, which is exactly
    /// the case ADR-0156 resolves by taking the complete path. Distinguished
    /// from "could not tell": the probe reports ignorance separately and
    /// ignorance does not decline.
    OutboxDeliveringObserved,
}

impl CleanCloseDeclineReason {
    /// Every reason, in counter-index order (see [`Self::index`]).
    pub(crate) const ALL: [Self; 11] = [
        Self::EngineInitializedAtOpen,
        Self::EngineRepairedAtOpen,
        Self::RecordAbsent,
        Self::RecordDecodeFailed,
        Self::DatabaseIdMismatch,
        Self::IncarnationMismatch,
        Self::StateNotClean,
        Self::JournalBoundaryUnverified,
        Self::BoundedRootsUnavailable,
        Self::BindingMismatch,
        Self::OutboxDeliveringObserved,
    ];

    /// Stable counter index for per-store decline-reason counting.
    #[must_use]
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::EngineInitializedAtOpen => 0,
            Self::EngineRepairedAtOpen => 1,
            Self::RecordAbsent => 2,
            Self::RecordDecodeFailed => 3,
            Self::DatabaseIdMismatch => 4,
            Self::IncarnationMismatch => 5,
            Self::StateNotClean => 6,
            Self::JournalBoundaryUnverified => 7,
            Self::BoundedRootsUnavailable => 8,
            Self::BindingMismatch => 9,
            Self::OutboxDeliveringObserved => 10,
        }
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::EngineInitializedAtOpen => "engine_initialized_at_open",
            Self::EngineRepairedAtOpen => "engine_repaired_at_open",
            Self::RecordAbsent => "record_absent",
            Self::RecordDecodeFailed => "record_decode_failed",
            Self::DatabaseIdMismatch => "database_id_mismatch",
            Self::IncarnationMismatch => "incarnation_mismatch",
            Self::StateNotClean => "state_not_clean",
            Self::JournalBoundaryUnverified => "journal_boundary_unverified",
            Self::BoundedRootsUnavailable => "bounded_roots_unavailable",
            Self::BindingMismatch => "binding_mismatch",
            Self::OutboxDeliveringObserved => "outbox_delivering_observed",
        }
    }
}

/// The one closed answer to "may this open take the bounded path?".
///
/// Replaces an `Option` whose `None` collapsed every distinct precondition
/// into one anonymous answer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanCloseVerdict {
    /// The certificate verified; bounded startup is admitted.
    Verified(CleanCloseLifecycle),
    /// The certificate did not verify; full validation is required.
    Declined(CleanCloseDeclineReason),
}

impl CleanCloseVerdict {
    /// The verified certificate, or `None` when bounded startup was declined.
    #[must_use]
    pub(crate) const fn verified(self) -> Option<CleanCloseLifecycle> {
        match self {
            Self::Verified(lifecycle) => Some(lifecycle),
            Self::Declined(_) => None,
        }
    }

    /// Why bounded startup was declined, or `None` when it was admitted.
    #[must_use]
    pub(crate) const fn declined(self) -> Option<CleanCloseDeclineReason> {
        match self {
            Self::Verified(_) => None,
            Self::Declined(reason) => Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanCloseState {
    Dirty,
    Clean([u8; 32]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CleanCloseLifecycle {
    database_id: DatabaseId,
    history_incarnation: u64,
    record_registry_digest: SchemaHash,
    lifecycle_generation: u64,
    state: CleanCloseState,
}

impl CleanCloseLifecycle {
    pub(crate) fn dirty(
        database_id: DatabaseId,
        history_incarnation: u64,
        lifecycle_generation: u64,
    ) -> Result<Self, CleanCloseCodecError> {
        Self::new(
            database_id,
            history_incarnation,
            lifecycle_generation,
            CleanCloseState::Dirty,
        )
    }

    pub(crate) fn clean(
        database_id: DatabaseId,
        history_incarnation: u64,
        lifecycle_generation: u64,
        binding_hash: [u8; 32],
    ) -> Result<Self, CleanCloseCodecError> {
        Self::new(
            database_id,
            history_incarnation,
            lifecycle_generation,
            CleanCloseState::Clean(binding_hash),
        )
    }

    fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        lifecycle_generation: u64,
        state: CleanCloseState,
    ) -> Result<Self, CleanCloseCodecError> {
        if history_incarnation == 0 || lifecycle_generation == 0 {
            return Err(CleanCloseCodecError::Invalid);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            record_registry_digest: riffdb_storage_api::proto_codec::current_record_registry_digest(
            ),
            lifecycle_generation,
            state,
        })
    }

    pub(crate) fn successor_dirty(self) -> Result<Self, CleanCloseCodecError> {
        Self::dirty(
            self.database_id,
            self.history_incarnation,
            self.lifecycle_generation
                .checked_add(1)
                .ok_or(CleanCloseCodecError::GenerationExhausted)?,
        )
    }

    pub(crate) fn successor_clean(
        self,
        binding_hash: [u8; 32],
    ) -> Result<Self, CleanCloseCodecError> {
        Self::clean(
            self.database_id,
            self.history_incarnation,
            self.lifecycle_generation
                .checked_add(1)
                .ok_or(CleanCloseCodecError::GenerationExhausted)?,
            binding_hash,
        )
    }

    pub(crate) const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    pub(crate) const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    #[cfg(test)]
    pub(crate) const fn lifecycle_generation(self) -> u64 {
        self.lifecycle_generation
    }

    pub(crate) const fn state(self) -> CleanCloseState {
        self.state
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, CleanCloseCodecError> {
        let (state, clean_state_binding_hash) = match self.state {
            CleanCloseState::Dirty => (WireState::Dirty as i32, Vec::new()),
            CleanCloseState::Clean(binding) => (WireState::Clean as i32, binding.to_vec()),
        };
        let message = StoredCleanCloseLifecycleV1 {
            database_id: self.database_id.as_bytes().to_vec(),
            history_incarnation: self.history_incarnation,
            record_registry_digest: self.record_registry_digest.as_bytes().to_vec(),
            lifecycle_generation: self.lifecycle_generation,
            state,
            clean_state_binding_hash,
            lifecycle_hash: lifecycle_hash(self).to_vec(),
        };
        riffdb_proto::durable::encode_current_message(&message)
            .map_err(|_| CleanCloseCodecError::Invalid)
    }

    pub(crate) fn decode(encoded: &[u8]) -> Result<Self, CleanCloseCodecError> {
        let message =
            riffdb_proto::durable::decode_readable_message::<StoredCleanCloseLifecycleV1>(encoded)
                .map_err(|_| CleanCloseCodecError::Invalid)?;
        let database_id = DatabaseId::from_bytes(
            message
                .database_id
                .try_into()
                .map_err(|_| CleanCloseCodecError::Invalid)?,
        )
        .map_err(|_| CleanCloseCodecError::Invalid)?;
        let registry_bytes: [u8; 32] = message
            .record_registry_digest
            .try_into()
            .map_err(|_| CleanCloseCodecError::Invalid)?;
        let record_registry_digest = SchemaHash::from_bytes(registry_bytes);
        if record_registry_digest
            != riffdb_storage_api::proto_codec::current_record_registry_digest()
        {
            return Err(CleanCloseCodecError::Invalid);
        }
        let state =
            match WireState::try_from(message.state).map_err(|_| CleanCloseCodecError::Invalid)? {
                WireState::Dirty if message.clean_state_binding_hash.is_empty() => {
                    CleanCloseState::Dirty
                }
                WireState::Clean => CleanCloseState::Clean(
                    message
                        .clean_state_binding_hash
                        .try_into()
                        .map_err(|_| CleanCloseCodecError::Invalid)?,
                ),
                WireState::Unspecified | WireState::Dirty => {
                    return Err(CleanCloseCodecError::Invalid);
                }
            };
        let lifecycle = Self::new(
            database_id,
            message.history_incarnation,
            message.lifecycle_generation,
            state,
        )?;
        let observed_hash: [u8; 32] = message
            .lifecycle_hash
            .try_into()
            .map_err(|_| CleanCloseCodecError::Invalid)?;
        if observed_hash != lifecycle_hash(lifecycle) {
            return Err(CleanCloseCodecError::Invalid);
        }
        Ok(lifecycle)
    }
}

fn lifecycle_hash(lifecycle: CleanCloseLifecycle) -> [u8; 32] {
    let mut preimage = Vec::with_capacity(LIFECYCLE_HASH_LABEL.len() + 101);
    preimage.extend_from_slice(LIFECYCLE_HASH_LABEL);
    preimage.extend_from_slice(&LIFECYCLE_HASH_VERSION.to_be_bytes());
    preimage.extend_from_slice(lifecycle.database_id.as_bytes());
    preimage.extend_from_slice(&lifecycle.history_incarnation.to_be_bytes());
    preimage.extend_from_slice(lifecycle.record_registry_digest.as_bytes());
    preimage.extend_from_slice(&lifecycle.lifecycle_generation.to_be_bytes());
    match lifecycle.state {
        CleanCloseState::Dirty => {
            preimage.push(0x01);
            preimage.push(0x00);
        }
        CleanCloseState::Clean(binding) => {
            preimage.push(0x02);
            preimage.push(0x01);
            preimage.extend_from_slice(&binding);
        }
    }
    *hash(HashDomain::Schema, &preimage).as_bytes()
}

pub(crate) fn bounded_state_binding_hash(
    transaction: &ReadTransaction,
    journal_header_digest: [u8; 32],
) -> Result<[u8; 32], riffdb_storage_api::StorageError> {
    let mut preimage = Vec::with_capacity(4096);
    append_bytes(&mut preimage, BOUNDED_ROOT_HASH_LABEL)?;
    append_bytes(&mut preimage, &BOUNDED_ROOT_HASH_VERSION.to_be_bytes())?;

    let meta = transaction.open_table(META).map_err(table_error)?;
    for key in BOUNDED_META_KEYS {
        let value = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .map(|value| value.value().to_vec());
        validate_bounded_meta_value(key, value.as_deref())?;
        append_item(
            &mut preimage,
            META_TABLE_TAG,
            key.as_bytes(),
            value.as_deref(),
        )?;
    }
    drop(meta);

    let catalog = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    if catalog.len().map_err(precommit_storage_error)? > 1 {
        return Err(corrupt());
    }
    let active_catalog = catalog
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .map(|value| value.value().to_vec());
    if let Some(value) = &active_catalog {
        codec::decode_active_catalog_pointer_v1(value)?;
    }
    append_item(
        &mut preimage,
        CATALOG_TABLE_TAG,
        &CATALOG_ACTIVE_KEY,
        active_catalog.as_deref(),
    )?;
    drop(catalog);

    let active_modules = transaction
        .open_table(QUERY_MODULE_ACTIVE)
        .map_err(table_error)?;
    let count = usize::try_from(active_modules.len().map_err(precommit_storage_error)?)
        .map_err(|_| limit_exceeded())?;
    if count > MAX_RETAINED_QUERY_MODULES {
        return Err(limit_exceeded());
    }
    let count = u32::try_from(count).map_err(|_| limit_exceeded())?;
    append_bytes(&mut preimage, &count.to_be_bytes())?;
    let module_bodies = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
    let mut previous_key: Option<Vec<u8>> = None;
    for entry in active_modules.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let key = key.value();
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous >= key)
        {
            return Err(corrupt());
        }
        previous_key = Some(key.to_vec());
        let record = codec::decode_query_module_administration_v1(value.value())?;
        let pointer = record.value().activated();
        let expected = keys::encode_active_query_module_key(
            pointer.contract_lineage(),
            pointer.contract_version(),
            pointer.contract_bundle_hash(),
        )
        .map_err(|_| corrupt())?;
        if key != expected.as_slice() {
            return Err(corrupt());
        }
        let module_key = keys::encode_query_module_key(pointer.module_hash());
        let stored_module = module_bodies
            .get(module_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let module = codec::decode_query_module_v1(stored_module.value())?;
        if !pointer.matches_module(module.value())
            || hash_query_module(module.value().canonical_bytes()) != pointer.module_hash()
        {
            return Err(corrupt());
        }
        append_item(
            &mut preimage,
            QUERY_ACTIVE_TABLE_TAG,
            key,
            Some(value.value()),
        )?;
    }
    drop(module_bodies);
    drop(active_modules);
    append_bytes(&mut preimage, &journal_header_digest)?;
    Ok(*hash(HashDomain::Schema, &preimage).as_bytes())
}

/// Write-snapshot form used by graceful close so checkpoint classification,
/// bounded-root reread, and CLEAN share one final transaction snapshot.
pub(crate) fn bounded_state_binding_hash_for_write(
    transaction: &redb::WriteTransaction,
    journal_header_digest: [u8; 32],
) -> Result<[u8; 32], riffdb_storage_api::StorageError> {
    let mut preimage = Vec::with_capacity(4096);
    append_bytes(&mut preimage, BOUNDED_ROOT_HASH_LABEL)?;
    append_bytes(&mut preimage, &BOUNDED_ROOT_HASH_VERSION.to_be_bytes())?;

    let meta = transaction.open_table(META).map_err(table_error)?;
    for key in BOUNDED_META_KEYS {
        let value = meta
            .get(key)
            .map_err(precommit_storage_error)?
            .map(|value| value.value().to_vec());
        validate_bounded_meta_value(key, value.as_deref())?;
        append_item(
            &mut preimage,
            META_TABLE_TAG,
            key.as_bytes(),
            value.as_deref(),
        )?;
    }
    drop(meta);

    let catalog = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    if catalog.len().map_err(precommit_storage_error)? > 1 {
        return Err(corrupt());
    }
    let active_catalog = catalog
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .map(|value| value.value().to_vec());
    if let Some(value) = &active_catalog {
        codec::decode_active_catalog_pointer_v1(value)?;
    }
    append_item(
        &mut preimage,
        CATALOG_TABLE_TAG,
        &CATALOG_ACTIVE_KEY,
        active_catalog.as_deref(),
    )?;
    drop(catalog);

    let active_modules = transaction
        .open_table(QUERY_MODULE_ACTIVE)
        .map_err(table_error)?;
    let count = usize::try_from(active_modules.len().map_err(precommit_storage_error)?)
        .map_err(|_| limit_exceeded())?;
    if count > MAX_RETAINED_QUERY_MODULES {
        return Err(limit_exceeded());
    }
    let count = u32::try_from(count).map_err(|_| limit_exceeded())?;
    append_bytes(&mut preimage, &count.to_be_bytes())?;
    let module_bodies = transaction.open_table(QUERY_MODULES).map_err(table_error)?;
    let mut previous_key: Option<Vec<u8>> = None;
    for entry in active_modules.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let key = key.value();
        if previous_key
            .as_deref()
            .is_some_and(|previous| previous >= key)
        {
            return Err(corrupt());
        }
        previous_key = Some(key.to_vec());
        let record = codec::decode_query_module_administration_v1(value.value())?;
        let pointer = record.value().activated();
        let expected = keys::encode_active_query_module_key(
            pointer.contract_lineage(),
            pointer.contract_version(),
            pointer.contract_bundle_hash(),
        )
        .map_err(|_| corrupt())?;
        if key != expected.as_slice() {
            return Err(corrupt());
        }
        let module_key = keys::encode_query_module_key(pointer.module_hash());
        let stored_module = module_bodies
            .get(module_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        let module = codec::decode_query_module_v1(stored_module.value())?;
        if !pointer.matches_module(module.value())
            || hash_query_module(module.value().canonical_bytes()) != pointer.module_hash()
        {
            return Err(corrupt());
        }
        append_item(
            &mut preimage,
            QUERY_ACTIVE_TABLE_TAG,
            key,
            Some(value.value()),
        )?;
    }
    drop(module_bodies);
    drop(active_modules);
    append_bytes(&mut preimage, &journal_header_digest)?;
    Ok(*hash(HashDomain::Schema, &preimage).as_bytes())
}

/// Reloads the exact active catalog root already proved by bounded clean
/// startup. Unlike the ordinary operational catalog read, this does not walk
/// population audit history whose integrity was intentionally deferred.
pub(crate) fn load_bound_active_catalog(
    transaction: &ReadTransaction,
) -> Result<Option<(ActiveCatalogPointerV1, StoredContractBundleV1)>, StorageError> {
    let active_table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    let active_count = active_table.len().map_err(precommit_storage_error)?;
    if active_count == 0 {
        return Ok(None);
    }
    if active_count != 1 {
        return Err(corrupt());
    }
    let active_bytes = active_table
        .get(CATALOG_ACTIVE_KEY.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let active = codec::decode_active_catalog_pointer_v1(active_bytes.value())?
        .into_parts()
        .0;
    drop(active_bytes);
    drop(active_table);

    let bundle_key = keys::encode_contract_bundle_key(active.lineage(), active.contract_version())
        .map_err(|_| corrupt())?;
    let bundles = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let bundle_bytes = bundles
        .get(bundle_key.as_slice())
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?;
    let bundle = codec::decode_contract_bundle_v1(bundle_bytes.value())?
        .into_parts()
        .0;
    if bundle.lineage() != active.lineage()
        || bundle.contract_version() != active.contract_version()
        || !active.matches_bundle(&bundle)
    {
        return Err(corrupt());
    }
    Ok(Some((active, bundle)))
}

fn validate_bounded_meta_value(
    key: &str,
    value: Option<&[u8]>,
) -> Result<(), riffdb_storage_api::StorageError> {
    let Some(value) = value else {
        return Ok(());
    };
    match key {
        META_FORMAT_VERSION => {
            codec::decode_storage_format_version_v1(value)?;
        }
        META_DATABASE_ID => {
            codec::decode_database_identity_v1(value)?;
        }
        META_APPLICATION_SEQUENCE => {
            codec::decode_application_sequence_allocator_v1(value)?;
        }
        META_ADMINISTRATION_SEQUENCE => {
            codec::decode_administration_sequence_allocator_v1(value)?;
        }
        META_CAPABILITY_BOOTSTRAP => {
            codec::decode_capability_bootstrap_marker_v1(value)?;
        }
        META_RECORD_REGISTRY => {
            let digest = codec::decode_record_registry_v2(value)?;
            if digest.value() != &riffdb_storage_api::proto_codec::current_record_registry_digest()
            {
                return Err(corrupt());
            }
        }
        META_HISTORY_INCARNATION => {
            if *codec::decode_history_incarnation_v1(value)?.value() == 0 {
                return Err(corrupt());
            }
        }
        META_INDEX_EPOCH_ROWS_REPAIRED => {
            if value != [1_u8] {
                return Err(corrupt());
            }
        }
        META_RETENTION_WATERMARK => {
            riffdb_storage_api::proto_codec::decode_retention_watermark_v1(value)
                .map_err(codec_error)?;
        }
        META_RETENTION_HOLDS => {
            riffdb_storage_api::proto_codec::decode_retention_holds_v1(value)
                .map_err(codec_error)?;
        }
        META_CHANGELOG_V2_ROTATION_RECEIPT => {
            riffdb_storage_api::decode_changelog_v2_rotation_receipt_v1(value)
                .map_err(codec_error)?;
        }
        _ => return Err(corrupt()),
    }
    Ok(())
}

fn append_item(
    preimage: &mut Vec<u8>,
    table_tag: u8,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<(), riffdb_storage_api::StorageError> {
    append_bytes(preimage, &[table_tag])?;
    append_length(preimage, key.len())?;
    append_bytes(preimage, key)?;
    match value {
        None => append_bytes(preimage, &[0x00]),
        Some(value) => {
            append_bytes(preimage, &[0x01])?;
            append_length(preimage, value.len())?;
            append_bytes(preimage, value)
        }
    }
}

fn append_length(
    preimage: &mut Vec<u8>,
    length: usize,
) -> Result<(), riffdb_storage_api::StorageError> {
    append_bytes(
        preimage,
        &u32::try_from(length)
            .map_err(|_| limit_exceeded())?
            .to_be_bytes(),
    )
}

fn append_bytes(
    preimage: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), riffdb_storage_api::StorageError> {
    let next = preimage
        .len()
        .checked_add(bytes.len())
        .ok_or_else(limit_exceeded)?;
    if next > MAX_BINDING_PREIMAGE_BYTES {
        return Err(limit_exceeded());
    }
    preimage.extend_from_slice(bytes);
    Ok(())
}

fn corrupt() -> riffdb_storage_api::StorageError {
    storage_error(riffdb_storage_api::StorageErrorKind::CorruptData)
}

fn limit_exceeded() -> riffdb_storage_api::StorageError {
    storage_error(riffdb_storage_api::StorageErrorKind::LimitExceeded)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_id() -> DatabaseId {
        let mut bytes = [0x11; 16];
        bytes[6] = 0x71;
        bytes[8] = 0x81;
        DatabaseId::from_bytes(bytes).expect("valid UUIDv7 database ID")
    }

    #[test]
    fn dirty_and_clean_records_round_trip_canonically() {
        let dirty = CleanCloseLifecycle::dirty(database_id(), 7, 11).expect("dirty lifecycle");
        let encoded_dirty = dirty.encode().expect("dirty encodes");
        assert_eq!(CleanCloseLifecycle::decode(&encoded_dirty), Ok(dirty));

        let clean = dirty.successor_clean([0x5a; 32]).expect("clean successor");
        let encoded_clean = clean.encode().expect("clean encodes");
        assert_eq!(CleanCloseLifecycle::decode(&encoded_clean), Ok(clean));
        assert_ne!(encoded_dirty, encoded_clean);
    }

    #[test]
    fn generation_transitions_are_checked_and_never_wrap() {
        let clean =
            CleanCloseLifecycle::clean(database_id(), 1, 4, [0x33; 32]).expect("clean lifecycle");
        let dirty = clean.successor_dirty().expect("dirty successor");
        assert_eq!(dirty.lifecycle_generation(), 5);
        assert_eq!(dirty.state(), CleanCloseState::Dirty);

        let exhausted =
            CleanCloseLifecycle::dirty(database_id(), 1, u64::MAX).expect("terminal generation");
        assert_eq!(
            exhausted.successor_dirty(),
            Err(CleanCloseCodecError::GenerationExhausted)
        );
        assert_eq!(
            exhausted.successor_clean([0; 32]),
            Err(CleanCloseCodecError::GenerationExhausted)
        );
    }

    #[test]
    fn zero_incarnation_and_generation_are_rejected() {
        assert_eq!(
            CleanCloseLifecycle::dirty(database_id(), 0, 1),
            Err(CleanCloseCodecError::Invalid)
        );
        assert_eq!(
            CleanCloseLifecycle::dirty(database_id(), 1, 0),
            Err(CleanCloseCodecError::Invalid)
        );
    }

    #[test]
    fn self_hash_binds_every_semantic_field() {
        let base = CleanCloseLifecycle::dirty(database_id(), 9, 13).expect("base lifecycle");
        assert_ne!(
            lifecycle_hash(base),
            lifecycle_hash(base.successor_dirty().unwrap())
        );
        assert_ne!(
            lifecycle_hash(base),
            lifecycle_hash(CleanCloseLifecycle::dirty(database_id(), 10, 13).unwrap())
        );
        assert_ne!(
            lifecycle_hash(base),
            lifecycle_hash(CleanCloseLifecycle::clean(database_id(), 9, 13, [0; 32]).unwrap())
        );
    }
}
