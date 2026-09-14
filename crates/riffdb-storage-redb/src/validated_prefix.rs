//! Validated-prefix startup checkpoint build/write (ADR-0085 Amendment 1).
//!
//! # What a wrong recorded count costs
//!
//! The eight recorded below-S counts are never taken on trust. At the next open
//! they are checked twice: [`load_active_checkpoint`] refuses a checkpoint whose
//! counts exceed redb's own row counts (`CountImpossible`, a fallback to full
//! validation), and the session's `verify_checkpoint_prefix_counts` then requires,
//! at structural ExactEnd, that `row_count − walked_suffix` equal every recorded
//! range-skipped count and that every classified prefix row of the full-walk
//! tables equal its recorded count. That second check does NOT fall back: per
//! ADR-0019 A1 a divergence after verified bindings is authoritative corruption
//! and the open REFUSES.
//!
//! So a wrong count can never be silently trusted — but the price of one is a
//! refused open, not a slow one. That refusal is recoverable with existing
//! offline tooling rather than from backup (see the recovery note on
//! `verify_checkpoint_prefix_counts`), and it is still an outage. That is why the
//! O(1) count derivation
//! below checks what it can check cheaply, falls back to the reference walk the
//! moment an assumption does not hold, and keeps its one maintained input (the
//! terminal execution-failure census) to a single seeding site and a single
//! counting site.

use redb::{Durability, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata};

use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityChainFingerprint, EntityTarget,
    EntityTransitionFingerprint, StorageError, StorageErrorKind, StoredValidatedPrefixCheckpointV1,
    StoredValidatedPrefixCheckpointV2, ValidatedPrefixEntityTransitionCounts,
    ValidatedPrefixRetainedSnapshot, ValidatedPrefixSequenceCounts,
    proto_codec::{
        current_record_registry_digest, decode_validated_prefix_checkpoint_v2,
        encode_validated_prefix_checkpoint_v2,
    },
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, EventId};

use crate::codec;
use crate::command_authority::command_authority_head;
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::hooks::RedbTestOperation;
use crate::keys;
use crate::layout::{
    AUDIT, AUDIT_BY_REQUEST, COMMITS, ENTITIES, ENTITY_CHAIN_HEADS, EVENT_ROUTES, EVENTS,
    IDEMPOTENCY, META, META_VALIDATED_PREFIX_CHECKPOINT, OUTBOX, OUTBOX_STATUS,
    VALIDATED_PREFIX_ENTITY_HEADS,
};
use crate::store::SharedRedb;

#[cfg(test)]
#[path = "validated_prefix_streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
#[path = "validated_prefix_receipt_tests.rs"]
mod receipt_tests;

/// Number of deterministic sample windows derived from the checkpoint self-hash.
pub(crate) const SAMPLE_WINDOW_COUNT: usize = 8;
/// Rows inspected per sample window (clipped to ≤ S).
pub(crate) const SAMPLE_WINDOW_SIZE: u64 = 128;

/// Active checkpoint binding used by a checkpointed structural evidence session.
#[derive(Clone, Debug)]
pub(crate) struct ActiveCheckpoint {
    pub checkpoint_commit_sequence: u64,
    pub audit_sequence_bound: u64,
    /// Allocator state proven with the validated prefix. Startup advances from
    /// this exact state across the suffix rather than replaying retained
    /// administration history from sequence one.
    pub retained: ValidatedPrefixRetainedSnapshot,
    pub counts: ValidatedPrefixSequenceCounts,
    /// Fingerprint-verified exact entity-chain heads at S; consumed once to
    /// seed suffix entity-chain advancement (`None` after consumption).
    pub entity_heads_at_s:
        Option<std::collections::BTreeMap<EntityTarget, riffdb_storage_api::EntityChainHeadV1>>,
    pub checkpoint_hash: [u8; 32],
}

/// Reasons a checkpoint was ignored (fail-closed → full validation). Never blocks open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointIgnoreReason {
    Absent,
    DecodeFailed,
    SelfHashMismatch,
    DatabaseIdMismatch,
    IncarnationMismatch,
    RegistryDigestMismatch,
    SequenceBeyondHead,
    CountImpossible,
    EntityChainMismatch,
    WatermarkMismatch,
}

impl CheckpointIgnoreReason {
    /// Every reason, in counter-index order (see [`Self::index`]).
    pub(crate) const ALL: [Self; 10] = [
        Self::Absent,
        Self::DecodeFailed,
        Self::SelfHashMismatch,
        Self::DatabaseIdMismatch,
        Self::IncarnationMismatch,
        Self::RegistryDigestMismatch,
        Self::SequenceBeyondHead,
        Self::CountImpossible,
        Self::EntityChainMismatch,
        Self::WatermarkMismatch,
    ];

    /// Stable counter index for per-store ignore-reason counting.
    #[must_use]
    pub(crate) const fn index(self) -> usize {
        match self {
            Self::Absent => 0,
            Self::DecodeFailed => 1,
            Self::SelfHashMismatch => 2,
            Self::DatabaseIdMismatch => 3,
            Self::IncarnationMismatch => 4,
            Self::RegistryDigestMismatch => 5,
            Self::SequenceBeyondHead => 6,
            Self::CountImpossible => 7,
            Self::EntityChainMismatch => 8,
            Self::WatermarkMismatch => 9,
        }
    }

    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::DecodeFailed => "decode_failed",
            Self::SelfHashMismatch => "self_hash_mismatch",
            Self::DatabaseIdMismatch => "database_id_mismatch",
            Self::IncarnationMismatch => "incarnation_mismatch",
            Self::RegistryDigestMismatch => "registry_digest_mismatch",
            Self::SequenceBeyondHead => "sequence_beyond_head",
            Self::CountImpossible => "count_impossible",
            Self::EntityChainMismatch => "entity_chain_mismatch",
            Self::WatermarkMismatch => "watermark_mismatch",
        }
    }
}

/// How one checkpoint's eight below-S row counts are obtained.
///
/// Both variants define the SAME eight numbers, and therefore the same
/// checkpoint bytes, for every database this crate can write; the checkpoint
/// record, its semantics and its bindings are untouched by the choice.
/// [`Self::Walked`] is the reference definition — one full pass per count class.
/// [`Self::DurableLengths`] is the O(1) derivation the production write uses;
/// `checkpoint_counts_from_durable_lengths_are_byte_identical_to_the_walk` pins
/// the equality on randomized histories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointCountSource {
    /// Reference implementation: one full walk per count class — O(history).
    ///
    /// The production write never selects it; `build_checkpoint_from_snapshot`
    /// reaches the same code directly when a `DurableLengths` precondition does
    /// not hold. Selecting it is how the byte-identity property test states the
    /// reference, and how the falsifiability transcript re-points the shutdown
    /// write at the walk.
    #[allow(
        dead_code,
        reason = "reference count source selected by the byte-identity property test"
    )]
    Walked,
    /// O(1): redb's per-table row counts plus this process's terminal
    /// execution-failure census (the one quantity a row count cannot express).
    DurableLengths { execution_failed_rows: u64 },
}

/// Closed lifecycle purpose for one checkpoint write attempt.
///
/// Startup retains its established proof-publication schedule. The fixture
/// purpose exists only to preserve falsifiability tests for the removed
/// shutdown writer; production graceful close never selects it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointPurpose {
    StartupValidation,
    TestFixture,
}

/// Closed result of the bounded graceful-close checkpoint inspection.
///
/// This is process evidence, never durable authority. The four successful
/// classes describe bytes that were observed and left untouched; the two
/// failure classes describe how far graceful close progressed.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GracefulCheckpointDispositionV1 {
    BarrierFailed,
    RetainedExactCurrent,
    LeftAbsent,
    LeftStale,
    LeftIneligible,
    ClassificationFailed,
}

impl GracefulCheckpointDispositionV1 {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BarrierFailed => "barrier_failed",
            Self::RetainedExactCurrent => "retained_exact_current",
            Self::LeftAbsent => "left_absent",
            Self::LeftStale => "left_stale",
            Self::LeftIneligible => "left_ineligible",
            Self::ClassificationFailed => "classification_failed",
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_successful(self) -> bool {
        matches!(
            self,
            Self::RetainedExactCurrent | Self::LeftAbsent | Self::LeftStale | Self::LeftIneligible
        )
    }
}

/// Successful classifier states, excluding both failure dispositions by type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SuccessfulGracefulCheckpointDisposition {
    RetainedExactCurrent,
    LeftAbsent,
    LeftStale,
    LeftIneligible,
}

impl SuccessfulGracefulCheckpointDisposition {
    const fn public(self) -> GracefulCheckpointDispositionV1 {
        match self {
            Self::RetainedExactCurrent => GracefulCheckpointDispositionV1::RetainedExactCurrent,
            Self::LeftAbsent => GracefulCheckpointDispositionV1::LeftAbsent,
            Self::LeftStale => GracefulCheckpointDispositionV1::LeftStale,
            Self::LeftIneligible => GracefulCheckpointDispositionV1::LeftIneligible,
        }
    }
}

/// Closed result of the final lifecycle transition.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GracefulLifecycleOutcomeV1 {
    CleanCommitted,
    CleanNotAttempted,
    CleanFailed,
    CleanUnknown,
}

impl GracefulLifecycleOutcomeV1 {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CleanCommitted => "clean_committed",
            Self::CleanNotAttempted => "clean_not_attempted",
            Self::CleanFailed => "clean_failed",
            Self::CleanUnknown => "clean_unknown",
        }
    }
}

/// Attempted CLEAN outcomes, excluding `CleanNotAttempted` by type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AttemptedGracefulLifecycleOutcome {
    Committed,
    Failed,
    Unknown,
}

impl AttemptedGracefulLifecycleOutcome {
    const fn public(self) -> GracefulLifecycleOutcomeV1 {
        match self {
            Self::Committed => GracefulLifecycleOutcomeV1::CleanCommitted,
            Self::Failed => GracefulLifecycleOutcomeV1::CleanFailed,
            Self::Unknown => GracefulLifecycleOutcomeV1::CleanUnknown,
        }
    }
}

/// Fixed-cardinality, redaction-safe graceful-close process receipt.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GracefulCheckpointCloseReceiptV1 {
    disposition: GracefulCheckpointDispositionV1,
    lifecycle: GracefulLifecycleOutcomeV1,
    /// Barrier, classification, and CLEAN stage durations in microseconds.
    elapsed_us: [u64; 3],
}

impl GracefulCheckpointCloseReceiptV1 {
    const fn from_parts(
        disposition: GracefulCheckpointDispositionV1,
        lifecycle: GracefulLifecycleOutcomeV1,
        elapsed_us: [u64; 3],
    ) -> Self {
        Self {
            disposition,
            lifecycle,
            elapsed_us,
        }
    }

    #[cfg(test)]
    pub(crate) fn new(
        disposition: GracefulCheckpointDispositionV1,
        lifecycle: GracefulLifecycleOutcomeV1,
        elapsed_us: [u64; 3],
    ) -> Result<Self, StorageError> {
        let valid = if disposition.is_successful() {
            !matches!(lifecycle, GracefulLifecycleOutcomeV1::CleanNotAttempted)
        } else {
            matches!(lifecycle, GracefulLifecycleOutcomeV1::CleanNotAttempted)
        };
        if !valid {
            return Err(StorageError::new(
                StorageErrorKind::InvariantViolation,
                None,
            ));
        }
        Ok(Self::from_parts(disposition, lifecycle, elapsed_us))
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn barrier_failed(elapsed_us: [u64; 3]) -> Self {
        Self::from_parts(
            GracefulCheckpointDispositionV1::BarrierFailed,
            GracefulLifecycleOutcomeV1::CleanNotAttempted,
            elapsed_us,
        )
    }

    pub(crate) const fn classification_failed(elapsed_us: [u64; 3]) -> Self {
        Self::from_parts(
            GracefulCheckpointDispositionV1::ClassificationFailed,
            GracefulLifecycleOutcomeV1::CleanNotAttempted,
            elapsed_us,
        )
    }

    pub(crate) const fn completed(
        disposition: SuccessfulGracefulCheckpointDisposition,
        lifecycle: AttemptedGracefulLifecycleOutcome,
        elapsed_us: [u64; 3],
    ) -> Self {
        Self::from_parts(disposition.public(), lifecycle.public(), elapsed_us)
    }

    #[must_use]
    pub const fn disposition(self) -> GracefulCheckpointDispositionV1 {
        self.disposition
    }

    #[must_use]
    pub const fn lifecycle(self) -> GracefulLifecycleOutcomeV1 {
        self.lifecycle
    }

    #[must_use]
    pub const fn elapsed_us(self) -> [u64; 3] {
        self.elapsed_us
    }

    /// One bounded line containing only closed tags and saturating durations.
    #[must_use]
    pub fn format_v1_line(self) -> String {
        format!(
            "riffdb-graceful-checkpoint-close-v1\t{}\t{}\t{},{},{}",
            self.disposition.as_str(),
            self.lifecycle.as_str(),
            self.elapsed_us[0],
            self.elapsed_us[1],
            self.elapsed_us[2]
        )
    }
}

/// Classifies the retained checkpoint from one immutable post-barrier view.
///
/// Only fixed metadata, redb table cardinalities, and the process-generation
/// witness are consulted. No population key, row, or companion value is decoded.
pub(crate) fn classify_graceful_checkpoint(
    transaction: &ReadTransaction,
    execution_failed_rows: u64,
    process_generation_witness: bool,
) -> Result<SuccessfulGracefulCheckpointDisposition, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let database_id = *codec::decode_database_identity_v1(
        meta.get(crate::layout::META_DATABASE_ID)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .value(),
    )?
    .value();
    let history_incarnation = *codec::decode_history_incarnation_v1(
        meta.get(crate::layout::META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .value(),
    )?
    .value();
    let application = *codec::decode_application_sequence_allocator_v1(
        meta.get(crate::layout::META_APPLICATION_SEQUENCE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .value(),
    )?
    .value();
    let administration = *codec::decode_administration_sequence_allocator_v1(
        meta.get(crate::layout::META_ADMINISTRATION_SEQUENCE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .value(),
    )?
    .value();
    let checkpoint_bytes = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?;
    let snapshot_rows = transaction
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;
    let Some(encoded) = checkpoint_bytes else {
        return Ok(if snapshot_rows == 0 {
            SuccessfulGracefulCheckpointDisposition::LeftAbsent
        } else {
            SuccessfulGracefulCheckpointDisposition::LeftIneligible
        });
    };
    let checkpoint = match decode_validated_prefix_checkpoint_v2(encoded.value()) {
        Ok(checkpoint) => checkpoint.into_parts().0,
        Err(_) => return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible),
    };
    drop(encoded);
    drop(meta);

    // An exhausted allocator has no bounded fixed-metadata predecessor witness:
    // deriving its exact tail would require consulting population values. The
    // retained proof therefore remains untouched but cannot be retained as
    // exact-current by the graceful classifier.
    if matches!(application, ApplicationSequenceAllocator::Exhausted)
        || matches!(
            administration,
            riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
        )
    {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    }

    let entity_counts = checkpoint.entity_counts();
    let Some(recorded_chain_rows) = entity_counts
        .live_entity_count
        .checked_add(entity_counts.deleted_entity_count)
    else {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    };
    if snapshot_rows != recorded_chain_rows {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    }
    if !process_generation_witness {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftStale);
    }

    let base = checkpoint.base();
    if base.retained().application_sequence_exhausted
        || base.retained().administration_sequence_exhausted
    {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    }
    let ApplicationSequenceAllocator::Next(application_next) = application else {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    };
    let riffdb_storage_api::AdministrationSequenceAllocator::Next(administration_next) =
        administration
    else {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    };
    let head = application_next.get().saturating_sub(1);
    let administration_tail = administration_next.get().saturating_sub(1);
    let watermark = load_retention_watermark_sequence(transaction)?;
    let Some(counts) = counts_from_durable_cardinalities(
        transaction,
        head,
        base.audit_sequence_bound(),
        execution_failed_rows,
    )?
    else {
        return Ok(SuccessfulGracefulCheckpointDisposition::LeftIneligible);
    };
    let entity_rows = transaction
        .open_table(ENTITIES)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;
    let chain_rows = transaction
        .open_table(ENTITY_CHAIN_HEADS)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;

    let exact = base.database_id() == database_id
        && base.history_incarnation() == history_incarnation
        && base.registry_digest() == current_record_registry_digest()
        && base.checkpoint_commit_sequence() == head
        // The physical AUDIT bound can trail the logical administration tail:
        // segmented command audit records live inside COMMITS. Its own bound,
        // counts, and self-hash were verified at startup; the retained logical
        // allocator separately proves no administration mutation followed.
        && base.audit_sequence_bound() <= administration_tail
        && base.retention_watermark_sequence() == watermark
        && base.retained() == allocator_snapshot(application, administration)
        && base.counts() == counts
        && entity_counts.live_entity_count == entity_rows
        && recorded_chain_rows == chain_rows;
    Ok(if exact {
        SuccessfulGracefulCheckpointDisposition::RetainedExactCurrent
    } else {
        SuccessfulGracefulCheckpointDisposition::LeftStale
    })
}

/// Builds and durably writes one validated-prefix checkpoint under an exclusive writer.
pub(crate) fn write_validated_prefix_checkpoint(
    shared: &SharedRedb,
    retained: &riffdb_storage_api::RetainedMetadataV1,
    purpose: CheckpointPurpose,
) -> Result<(), StorageError> {
    let transaction = shared.database.begin_read().map_err(transaction_error)?;
    // Transitional routing, not an activation permit. Any V3 control requires
    // strict roots even when the test-only exact-checkpoint path is a no-op.
    let v3 = crate::changelog_v3_journal::has_recovery_roots(&transaction)?;
    if v3 {
        crate::changelog_v3_roots::read_checkpoint_roots(&transaction)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    }
    let mut rows_walked = 0_u64;
    // Counts come from table metadata, never from a history pass: at graceful
    // shutdown this is the whole difference between O(1) and O(history), and
    // the write happens after the writer lane has drained.
    let source = CheckpointCountSource::DurableLengths {
        execution_failed_rows: shared.terminal_execution_failure_rows(),
    };
    if purpose == CheckpointPurpose::TestFixture
        && exact_current_checkpoint_exists(
            &transaction,
            retained,
            shared.terminal_execution_failure_rows(),
        )?
    {
        shared.note_checkpoint_count_rows_walked(rows_walked);
        return Ok(());
    }
    let checkpoint =
        build_checkpoint_from_snapshot(&transaction, retained, source, &mut rows_walked)?;
    let receipt = if v3 {
        plan_checkpoint_receipt(&transaction, &checkpoint)?
    } else {
        None
    };
    drop(transaction);
    shared.note_checkpoint_count_rows_walked(rows_walked);

    if v3 {
        if let Some(receipt) = receipt {
            // This sealed owner opens the one existing hardened transaction,
            // checks every expected value and history predecessor before any
            // mutation, then stages the planned rows and exact receipt together.
            // It replaces the raw writer below, never runs alongside it.
            let prepared = crate::changelog_v3_write::PreparedImmediateReceipt::apply(
                &shared.database,
                crate::store::RedbCommitProfile::Hardened,
                &receipt,
            )?;
            shared.before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
            prepared.commit(shared)?;
            shared.after_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
        }
        return Ok(());
    }

    let encoded =
        encode_validated_prefix_checkpoint_v2(&checkpoint).map_err(crate::error::codec_error)?;
    let mut write = shared.database.begin_write().map_err(transaction_error)?;
    write.set_two_phase_commit(true);
    write
        .set_durability(Durability::Immediate)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    replace_checkpoint_entity_heads(&write, &checkpoint)?;
    {
        let mut meta = write.open_table(META).map_err(table_error)?;
        meta.insert(META_VALIDATED_PREFIX_CHECKPOINT, encoded.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    shared.before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)?;
    shared.commit_durable(write)?;
    shared.after_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
}

/// Returns true only when the already validated V2 proof describes the exact
/// current durable view. Startup established the snapshot contents and
/// fingerprints for this process generation; frontiers and complete O(1)
/// table counts prove that no authoritative transition occurred since.
fn exact_current_checkpoint_exists(
    transaction: &ReadTransaction,
    retained: &riffdb_storage_api::RetainedMetadataV1,
    execution_failed_rows: u64,
) -> Result<bool, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
    else {
        return Ok(false);
    };
    let checkpoint = match decode_validated_prefix_checkpoint_v2(encoded.value()) {
        Ok(checkpoint) => checkpoint.into_parts().0,
        Err(_) => return Ok(false),
    };
    let base = checkpoint.base();
    let head = last_commit_sequence(transaction)?
        .map(CommitSequence::get)
        .unwrap_or(0);
    let audit = last_audit_sequence(transaction)?
        .map(AdministrationSequence::get)
        .unwrap_or(0);
    let watermark = load_retention_watermark_sequence(transaction)?;
    let counts = counts_from_durable_lengths(transaction, head, audit, execution_failed_rows)?;
    let Some(counts) = counts else {
        return Ok(false);
    };
    let entity_rows = transaction
        .open_table(ENTITIES)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;
    let chain_rows = transaction
        .open_table(ENTITY_CHAIN_HEADS)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;
    let snapshot_rows = transaction
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)?;
    let entity_counts = checkpoint.entity_counts();
    let recorded_chain_rows = entity_counts
        .live_entity_count
        .checked_add(entity_counts.deleted_entity_count)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    Ok(base.database_id() == retained.database_id()
        && base.history_incarnation() == retained.history_incarnation()
        && base.registry_digest() == current_record_registry_digest()
        && base.checkpoint_commit_sequence() == head
        && base.audit_sequence_bound() == audit
        && base.retention_watermark_sequence() == watermark
        && base.retained() == retained_snapshot(retained)
        && base.counts() == counts
        && entity_counts.live_entity_count == entity_rows
        && recorded_chain_rows == chain_rows
        && snapshot_rows == chain_rows)
}

/// Replaces the exact at-S entity-head snapshot in the same transaction that
/// publishes the checkpoint binding it. The copied rows are decoded and
/// re-proved against the checkpoint before the meta row can become visible.
fn replace_checkpoint_entity_heads(
    transaction: &redb::WriteTransaction,
    checkpoint: &StoredValidatedPrefixCheckpointV2,
) -> Result<(), StorageError> {
    let source = transaction
        .open_table(ENTITY_CHAIN_HEADS)
        .map_err(table_error)?;
    let mut snapshot = transaction
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .map_err(table_error)?;

    let mut live_entity_count = 0_u64;
    let mut deleted_entity_count = 0_u64;
    let mut entity_transition_count = 0_u64;
    for row in source.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let decoded = riffdb_storage_api::decode_entity_chain_head_v1(value.value())
            .map_err(crate::error::codec_error)?;
        let head = decoded.value();
        if head.target().key().as_bytes() != key.value() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        entity_transition_count = entity_transition_count
            .checked_add(head.chain_revision())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        match head.state() {
            riffdb_storage_api::EntityChainStateV1::Live { .. } => {
                live_entity_count = live_entity_count
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            riffdb_storage_api::EntityChainStateV1::Deleted => {
                deleted_entity_count = deleted_entity_count
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            riffdb_storage_api::EntityChainStateV1::NeverExisted => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
        let unchanged = snapshot
            .get(key.value())
            .map_err(precommit_storage_error)?
            .is_some_and(|prior| prior.value() == value.value());
        if !unchanged {
            snapshot
                .insert(key.value(), value.value())
                .map_err(precommit_storage_error)?;
        }
    }
    let counts = ValidatedPrefixEntityTransitionCounts {
        live_entity_count,
        deleted_entity_count,
        entity_transition_count,
    };
    let fingerprint = fingerprint_from_head_table(&source)?;
    if counts != checkpoint.entity_counts()
        || fingerprint != checkpoint.entity_transition_fingerprint()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let mut failure = None;
    snapshot
        .retain(|key, _| {
            if failure.is_some() {
                return true;
            }
            match source.get(key) {
                Ok(row) => row.is_some(),
                Err(error) => {
                    failure = Some(precommit_storage_error(error));
                    true
                }
            }
        })
        .map_err(precommit_storage_error)?;
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(())
}

/// Plans only the exact net checkpoint-head changes from one immutable read
/// view. No-op population rows never enter the bounded receipt accumulator.
/// The sealed writer rechecks its V3 predecessor and all physical preconditions
/// in its fresh transaction; this read-only plan is not a permit.
pub(crate) fn plan_checkpoint_head_changes(
    transaction: &ReadTransaction,
    checkpoint: &StoredValidatedPrefixCheckpointV2,
) -> Result<Vec<riffdb_storage_api::AuthoritativeMutationV3>, StorageError> {
    use riffdb_storage_api::{
        AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N,
        ChangelogV3Error,
    };
    let (counts, fingerprint) = entity_transition_proof(transaction)?;
    if counts != checkpoint.entity_counts()
        || fingerprint != checkpoint.entity_transition_fingerprint()
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let source = transaction
        .open_table(ENTITY_CHAIN_HEADS)
        .map_err(table_error)?;
    let snapshot = transaction
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .map_err(table_error)?;
    let invalid = |error| {
        storage_error(match error {
            ChangelogV3Error::LimitExceeded => StorageErrorKind::LimitExceeded,
            _ => StorageErrorKind::CorruptData,
        })
    };
    let mut changes = AuthoritativeMutationAccumulatorV3::default();
    for row in source.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let prior = snapshot.get(key.value()).map_err(precommit_storage_error)?;
        let change = match prior {
            Some(prior) if prior.value() == value.value() => continue,
            Some(prior) => AuthoritativeMutationV3::replace(
                N::ValidatedPrefixEntityHeads,
                key.value(),
                prior.value(),
                value.value(),
            ),
            None => AuthoritativeMutationV3::put(
                N::ValidatedPrefixEntityHeads,
                key.value(),
                None,
                value.value(),
            ),
        }
        .map_err(invalid)?;
        changes.record(change).map_err(invalid)?;
    }
    for row in snapshot.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        // Never turn a malformed old snapshot row into a deletion/repair hint.
        let head = riffdb_storage_api::decode_entity_chain_head_v1(value.value())
            .map_err(crate::error::codec_error)?;
        if head.value().target().key().as_bytes() != key.value() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        if source
            .get(key.value())
            .map_err(precommit_storage_error)?
            .is_none()
        {
            let change = AuthoritativeMutationV3::delete_matching(
                N::ValidatedPrefixEntityHeads,
                key.value(),
                value.value(),
            )
            .map_err(invalid)?;
            changes.record(change).map_err(invalid)?;
        }
    }
    changes.finish().map_err(invalid)
}

/// Forms the complete checkpoint operation's receipt from the same pin used by
/// the existing validated checkpoint builder. Prefix/history count evidence and
/// the exclusive drained checkpoint gate remain the existing caller's duties.
/// An exact existing checkpoint is a no-op, not an empty physical transaction.
pub(crate) fn plan_checkpoint_receipt(
    transaction: &ReadTransaction,
    checkpoint: &StoredValidatedPrefixCheckpointV2,
) -> Result<Option<riffdb_storage_api::AuthoritativeTransactionV3>, StorageError> {
    use riffdb_storage_api::{
        AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
        AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogV3Error,
    };
    let history = crate::changelog_v3_roots::read_checkpoint_roots(transaction)?
        .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
    let base = checkpoint.base();
    let frontier = history.tail().frontier();
    if base.database_id() != history.lineage().database_id()
        || base.history_incarnation() != history.lineage().history_incarnation()
        || base.registry_digest() != current_record_registry_digest()
        || base.checkpoint_commit_sequence()
            != frontier.application().map_or(0, |sequence| sequence.get())
        // The retained V2 certificate bounds the physical AUDIT namespace.
        // Command-owned audit rows live inside COMMITS and can advance V3's
        // administration frontier beyond that physical bound (ADR-0102).
        // Recheck the exact bound from this SAME pin; a mere <= comparison
        // would wrongly accept a substituted lower certificate bound.
        || base.audit_sequence_bound()
            != last_audit_sequence(transaction)?.map_or(0, AdministrationSequence::get)
        || base.audit_sequence_bound()
            > frontier
                .administration()
                .map_or(0, |sequence| sequence.get())
    {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let mut changes = plan_checkpoint_head_changes(transaction, checkpoint)?;
    let encoded =
        encode_validated_prefix_checkpoint_v2(checkpoint).map_err(crate::error::codec_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let prior = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?;
    let invalid = |error| {
        storage_error(match error {
            ChangelogV3Error::LimitExceeded => StorageErrorKind::LimitExceeded,
            ChangelogV3Error::SequenceExhausted => StorageErrorKind::SequenceExhausted,
            _ => StorageErrorKind::CorruptData,
        })
    };
    let mutation = if let Some(prior) = prior {
        if prior.value() == encoded.as_bytes() {
            if changes.is_empty() {
                return Ok(None);
            }
            // Reusing the identical proof cannot authorize repairing its rows.
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        match decode_validated_prefix_checkpoint_v2(prior.value()) {
            Ok(previous) => {
                let previous = previous.value().base();
                if base.previous_checkpoint_hash() != Some(previous.checkpoint_hash())
                    || previous.database_id() != base.database_id()
                    || previous.history_incarnation() != base.history_incarnation()
                    || previous.checkpoint_commit_sequence() > base.checkpoint_commit_sequence()
                    || previous.audit_sequence_bound() > base.audit_sequence_bound()
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
            Err(_) => {
                // ADR-0019 A1: an invalid optional proof is ignored, not trusted.
                // The existing validated builder starts a new proof chain, but
                // these exact invalid bytes remain the replacement precondition.
                if base.previous_checkpoint_hash().is_some() {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
            }
        }
        AuthoritativeMutationV3::replace(
            N::ValidatedPrefixCheckpoint,
            META_VALIDATED_PREFIX_CHECKPOINT.as_bytes(),
            prior.value(),
            encoded.as_bytes(),
        )
    } else {
        // ADR-0085 retention deletes the singleton first, retaining its old
        // snapshot until complete validation rebuilds the optional proof. The
        // bounded head planner above validates and receipts every net change.
        if base.previous_checkpoint_hash().is_some() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        AuthoritativeMutationV3::put(
            N::ValidatedPrefixCheckpoint,
            META_VALIDATED_PREFIX_CHECKPOINT.as_bytes(),
            None,
            encoded.as_bytes(),
        )
    }
    .map_err(invalid)?;
    // Head namespace34 precedes the singleton checkpoint namespace109. The
    // shared constructor checks the whole receipt's frame-adjusted byte limit.
    changes.push(mutation);
    let (sequence, _) = history
        .expected_allocator()
        .allocate_one()
        .map_err(invalid)?;
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: frontier,
            covered_frontier: frontier,
            prior_history_hash: history.tail().history_hash(),
        },
        ChangelogAttributionV3::ValidatedPrefixCheckpoint,
        changes,
    )
    .map_err(invalid)?;
    Ok(Some(receipt))
}

/// Physical key order equals `(entity type, entity key)` order: canonical keys
/// begin with the fixed version/purpose prefix and big-endian entity type.
/// Both passes borrow this exact transaction-bound table, never a latest view.
fn fingerprint_from_head_table(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
) -> Result<EntityTransitionFingerprint, StorageError> {
    EntityTransitionFingerprint::from_sorted_head_reader(|| {
        Ok(table.iter().map_err(precommit_storage_error)?.map(|row| {
            let (key, value) = row.map_err(precommit_storage_error)?;
            let head = riffdb_storage_api::decode_entity_chain_head_v1(value.value())
                .map_err(crate::error::codec_error)?
                .into_parts()
                .0;
            if head.target().key().as_bytes() != key.value() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            Ok(head)
        }))
    })
}

/// Builds one checkpoint from an immutable snapshot.
///
/// `rows_walked` accumulates every history row the COUNT classes iterate, so a
/// test can pin that the production path touches none of them
/// (`the_shutdown_checkpoint_write_iterates_no_history_rows`). The ENTITIES pass
/// behind the entity-chain fingerprint is deliberately not tallied there: it is
/// current-state, not history, and is bounded by the live entity set rather than
/// by history length. The checkpoint write also snapshots exact chain heads in
/// the same transaction that publishes this proof.
pub(crate) fn build_checkpoint_from_snapshot(
    transaction: &ReadTransaction,
    retained: &riffdb_storage_api::RetainedMetadataV1,
    source: CheckpointCountSource,
    rows_walked: &mut u64,
) -> Result<StoredValidatedPrefixCheckpointV2, StorageError> {
    let database_id = retained.database_id();
    let history_incarnation = retained.history_incarnation();
    let registry_digest = current_record_registry_digest();

    let head_commit = last_commit_sequence(transaction)?;
    let checkpoint_commit_sequence = head_commit.map(CommitSequence::get).unwrap_or(0);
    let audit_sequence_bound = last_audit_sequence(transaction)?
        .map(AdministrationSequence::get)
        .unwrap_or(0);

    let s = checkpoint_commit_sequence;
    let counts = match source {
        CheckpointCountSource::Walked => {
            walked_counts(transaction, s, audit_sequence_bound, rows_walked)?
        }
        CheckpointCountSource::DurableLengths {
            execution_failed_rows,
        } => match counts_from_durable_lengths(
            transaction,
            s,
            audit_sequence_bound,
            execution_failed_rows,
        )? {
            Some(counts) => counts,
            // A census larger than the table it counts within can only mean the
            // census drifted. Fall back to the reference walk rather than write
            // counts the next open would refuse (see the module note on why a
            // wrong count refuses instead of falling back at open).
            None => walked_counts(transaction, s, audit_sequence_bound, rows_walked)?,
        },
    };
    let entity_chain_fingerprint = entity_chain_fingerprint_from_entities(transaction)?;
    let retained_snap = retained_snapshot(retained);

    let previous = load_previous_hash(transaction)?;

    let retention_watermark_sequence = load_retention_watermark_sequence(transaction)?;

    let base = StoredValidatedPrefixCheckpointV1::new(
        database_id,
        history_incarnation,
        registry_digest,
        checkpoint_commit_sequence,
        audit_sequence_bound,
        counts,
        entity_chain_fingerprint,
        retained_snap,
        previous,
        retention_watermark_sequence,
    )
    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let (entity_counts, entity_transition_fingerprint) = entity_transition_proof(transaction)?;
    StoredValidatedPrefixCheckpointV2::new(base, entity_counts, entity_transition_fingerprint)
        .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
}

fn entity_transition_proof(
    transaction: &ReadTransaction,
) -> Result<
    (
        ValidatedPrefixEntityTransitionCounts,
        EntityTransitionFingerprint,
    ),
    StorageError,
> {
    let table = transaction
        .open_table(ENTITY_CHAIN_HEADS)
        .map_err(table_error)?;
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    let mut live_entity_count = 0_u64;
    let mut deleted_entity_count = 0_u64;
    let mut entity_transition_count = 0_u64;
    for row in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = row.map_err(precommit_storage_error)?;
        let head = riffdb_storage_api::decode_entity_chain_head_v1(value.value())
            .map_err(crate::error::codec_error)?
            .into_parts()
            .0;
        if head.target().key().as_bytes() != key.value() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        entity_transition_count = entity_transition_count
            .checked_add(head.chain_revision())
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        match head.state() {
            riffdb_storage_api::EntityChainStateV1::Live { .. } => {
                if entities
                    .get(key.value())
                    .map_err(precommit_storage_error)?
                    .is_none()
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                live_entity_count = live_entity_count
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            riffdb_storage_api::EntityChainStateV1::Deleted => {
                if entities
                    .get(key.value())
                    .map_err(precommit_storage_error)?
                    .is_some()
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                deleted_entity_count = deleted_entity_count
                    .checked_add(1)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            }
            riffdb_storage_api::EntityChainStateV1::NeverExisted => {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
    }
    if entities.len().map_err(precommit_storage_error)? != live_entity_count {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let fingerprint = fingerprint_from_head_table(&table)?;
    Ok((
        ValidatedPrefixEntityTransitionCounts {
            live_entity_count,
            deleted_entity_count,
            entity_transition_count,
        },
        fingerprint,
    ))
}

/// Reference definition of the eight below-S counts: one full pass per class.
fn walked_counts(
    transaction: &ReadTransaction,
    s: u64,
    audit_sequence_bound: u64,
    rows_walked: &mut u64,
) -> Result<ValidatedPrefixSequenceCounts, StorageError> {
    Ok(ValidatedPrefixSequenceCounts {
        commits_count: count_commits_le(transaction, s, rows_walked)?,
        events_count: count_event_keys_le(transaction, EVENTS, s, rows_walked)?,
        event_routes_count: count_event_routes_le(transaction, s, rows_walked)?,
        outbox_count: count_event_keys_le(transaction, OUTBOX, s, rows_walked)?,
        outbox_status_count: count_event_keys_le(transaction, OUTBOX_STATUS, s, rows_walked)?,
        idempotency_count: count_idempotency_le(transaction, s, rows_walked)?,
        audit_count: count_audit_le(transaction, audit_sequence_bound, rows_walked)?,
        audit_by_request_count: count_audit_by_request_le(
            transaction,
            audit_sequence_bound,
            rows_walked,
        )?,
    })
}

/// Derives the eight below-S counts from redb's per-table row counts — O(1).
///
/// Sound because of how S and the audit bound are chosen in
/// [`build_checkpoint_from_snapshot`]: they are the LAST keys of `COMMITS` and
/// `AUDIT` in this same immutable snapshot. Every sequence-linked row in the
/// eight counted tables is born in the one durable transaction that writes its
/// `COMMITS` row (or, for `OUTBOX_STATUS` and `AUDIT_BY_REQUEST`, keyed off a row
/// that already exists), so no row can carry a sequence above those last keys:
/// "rows with sequence ≤ S" is "every row in the table". Retention pruning only
/// deletes from below, which lowers both sides identically. `COMMITS` and `AUDIT`
/// carry that property by the definition of S and the bound; the three
/// event-keyed tables have it CHECKED here from their last key, an O(log n)
/// probe; `EVENT_ROUTES`, `AUDIT_BY_REQUEST` and `IDEMPOTENCY` are not ordered by
/// sequence and rest on the birth argument alone.
///
/// Two departures from a plain row count are explicit:
///   * S == 0 (empty application history) and bound == 0 (empty administration
///     history): the walks return 0 without classifying a row, so the classes
///     bounded by that sequence must be 0 whatever the table holds.
///   * `IDEMPOTENCY` stores both terminal classes and `idempotency_count` admits
///     only `StoredOutcome` rows; the `ExecutionFailed` census is subtracted.
///
/// Returns `None` — caller falls back to the reference walk — when the census
/// exceeds the table it counts within, or when a checked table holds a row above
/// S. Both mean an assumption this derivation rests on does not hold here, and a
/// walk is always correct.
fn counts_from_durable_lengths(
    transaction: &ReadTransaction,
    s: u64,
    audit_sequence_bound: u64,
    execution_failed_rows: u64,
) -> Result<Option<ValidatedPrefixSequenceCounts>, StorageError> {
    let idempotency = table_row_count(transaction, IDEMPOTENCY)?;
    let Some(terminal_outcomes) = idempotency.checked_sub(execution_failed_rows) else {
        return Ok(None);
    };
    for definition in [EVENTS, OUTBOX, OUTBOX_STATUS] {
        if !event_keyed_table_ends_at_or_below(transaction, definition, s)? {
            return Ok(None);
        }
    }
    let below_s = |count: u64| if s == 0 { 0 } else { count };
    let below_bound = |count: u64| if audit_sequence_bound == 0 { 0 } else { count };
    Ok(Some(ValidatedPrefixSequenceCounts {
        commits_count: below_s(table_row_count(transaction, COMMITS)?),
        events_count: below_s(table_row_count(transaction, EVENTS)?),
        event_routes_count: below_s(table_row_count(transaction, EVENT_ROUTES)?),
        outbox_count: below_s(table_row_count(transaction, OUTBOX)?),
        outbox_status_count: below_s(table_row_count(transaction, OUTBOX_STATUS)?),
        idempotency_count: below_s(terminal_outcomes),
        audit_count: below_bound(table_row_count(transaction, AUDIT)?),
        audit_by_request_count: below_bound(table_row_count(transaction, AUDIT_BY_REQUEST)?),
    }))
}

/// Whether an event-keyed table's LAST key sits at or below S — one B-tree
/// descent that turns "every row is at or below S" from an assumption about the
/// write lane into a checked fact for the sequence-ordered tables. Empty tables
/// trivially qualify; an undecodable last key does not.
fn event_keyed_table_ends_at_or_below(
    transaction: &ReadTransaction,
    definition: redb::TableDefinition<'_, &'static [u8], &'static [u8]>,
    s: u64,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let Some((key, _)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(true);
    };
    Ok(keys::decode_event_key(key.value())
        .is_ok_and(|id| id.commit_sequence().get() <= s && s != 0))
}

/// Reads one table's row count from redb's table metadata (no row is touched).
fn table_row_count(
    transaction: &ReadTransaction,
    definition: redb::TableDefinition<'_, &'static [u8], &'static [u8]>,
) -> Result<u64, StorageError> {
    transaction
        .open_table(definition)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)
}

/// Reads the live retention watermark sequence (0 when the meta key is absent).
fn load_retention_watermark_sequence(transaction: &ReadTransaction) -> Result<u64, StorageError> {
    Ok(crate::retention::load_watermark(transaction)?
        .map(|w| w.watermark_sequence())
        .unwrap_or(0))
}

/// Read-view counterpart used only by graceful classification. Unlike the
/// historical checkpoint builder, it deliberately performs no last-key probe:
/// the fixed allocator metadata is the post-barrier tail witness.
fn counts_from_durable_cardinalities(
    transaction: &ReadTransaction,
    s: u64,
    audit_sequence_bound: u64,
    execution_failed_rows: u64,
) -> Result<Option<ValidatedPrefixSequenceCounts>, StorageError> {
    let idempotency = table_row_count(transaction, IDEMPOTENCY)?;
    let Some(terminal_outcomes) = idempotency.checked_sub(execution_failed_rows) else {
        return Ok(None);
    };
    let below_s = |count: u64| if s == 0 { 0 } else { count };
    let below_bound = |count: u64| if audit_sequence_bound == 0 { 0 } else { count };
    Ok(Some(ValidatedPrefixSequenceCounts {
        commits_count: below_s(table_row_count(transaction, COMMITS)?),
        events_count: below_s(table_row_count(transaction, EVENTS)?),
        event_routes_count: below_s(table_row_count(transaction, EVENT_ROUTES)?),
        outbox_count: below_s(table_row_count(transaction, OUTBOX)?),
        outbox_status_count: below_s(table_row_count(transaction, OUTBOX_STATUS)?),
        idempotency_count: below_s(terminal_outcomes),
        audit_count: below_bound(table_row_count(transaction, AUDIT)?),
        audit_by_request_count: below_bound(table_row_count(transaction, AUDIT_BY_REQUEST)?),
    }))
}

fn retained_snapshot(
    retained: &riffdb_storage_api::RetainedMetadataV1,
) -> ValidatedPrefixRetainedSnapshot {
    allocator_snapshot(
        retained.application_sequence(),
        retained.administration_sequence(),
    )
}

fn allocator_snapshot(
    application: ApplicationSequenceAllocator,
    administration: riffdb_storage_api::AdministrationSequenceAllocator,
) -> ValidatedPrefixRetainedSnapshot {
    match application {
        ApplicationSequenceAllocator::Next(seq) => ValidatedPrefixRetainedSnapshot {
            next_application_sequence: seq.get(),
            application_sequence_exhausted: false,
            next_administration_sequence: match administration {
                riffdb_storage_api::AdministrationSequenceAllocator::Next(s) => s.get(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted => 0,
            },
            administration_sequence_exhausted: matches!(
                administration,
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
            ),
        },
        ApplicationSequenceAllocator::Exhausted => ValidatedPrefixRetainedSnapshot {
            next_application_sequence: 0,
            application_sequence_exhausted: true,
            next_administration_sequence: match administration {
                riffdb_storage_api::AdministrationSequenceAllocator::Next(s) => s.get(),
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted => 0,
            },
            administration_sequence_exhausted: matches!(
                administration,
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted
            ),
        },
    }
}

fn load_previous_hash(
    transaction: &ReadTransaction,
) -> Result<Option<riffdb_storage_api::ValidatedPrefixCheckpointHash>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(value) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    match decode_validated_prefix_checkpoint_v2(value.value()) {
        Ok(item) => Ok(Some(item.into_parts().0.base().checkpoint_hash())),
        Err(_) => Ok(None),
    }
}

/// Loads and verifies a checkpoint. On any failure returns `Err(reason)` for ignore → full validation.
pub(crate) fn load_active_checkpoint(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
    history_incarnation: u64,
    full_counts: &[u64; 28],
) -> Result<ActiveCheckpoint, CheckpointIgnoreReason> {
    let meta = transaction
        .open_table(META)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?;
    let Some(value) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?
    else {
        return Err(CheckpointIgnoreReason::Absent);
    };
    let checkpoint_v2 = match decode_validated_prefix_checkpoint_v2(value.value()) {
        Ok(item) => item.into_parts().0,
        Err(_) => return Err(CheckpointIgnoreReason::DecodeFailed),
    };
    let checkpoint = checkpoint_v2.base();
    // from_stored_parts already rechecked self-hash; recompute for belt-and-suspenders.
    let computed = checkpoint
        .computed_hash()
        .map_err(|_| CheckpointIgnoreReason::SelfHashMismatch)?;
    if computed != checkpoint.checkpoint_hash() {
        return Err(CheckpointIgnoreReason::SelfHashMismatch);
    }
    if checkpoint.database_id() != database_id {
        return Err(CheckpointIgnoreReason::DatabaseIdMismatch);
    }
    if checkpoint.history_incarnation() != history_incarnation {
        return Err(CheckpointIgnoreReason::IncarnationMismatch);
    }
    if checkpoint.registry_digest() != current_record_registry_digest() {
        return Err(CheckpointIgnoreReason::RegistryDigestMismatch);
    }
    let live_watermark = load_retention_watermark_sequence(transaction)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?;
    if checkpoint.retention_watermark_sequence() != live_watermark {
        return Err(CheckpointIgnoreReason::WatermarkMismatch);
    }
    let head = last_commit_sequence(transaction)
        .map_err(|_| CheckpointIgnoreReason::DecodeFailed)?
        .map(CommitSequence::get)
        .unwrap_or(0);
    if checkpoint.checkpoint_commit_sequence() > head {
        return Err(CheckpointIgnoreReason::SequenceBeyondHead);
    }
    let counts = checkpoint.counts();
    // Phase indices: 10 COMMITS, 12 EVENTS, 13 EVENT_ROUTES, 14 OUTBOX, 15 OUTBOX_STATUS,
    // 8 IDEMPOTENCY, 21 AUDIT, 22 AUDIT_BY_REQUEST.
    if counts.commits_count > full_counts[10]
        || counts.events_count > full_counts[12]
        || counts.event_routes_count > full_counts[13]
        || counts.outbox_count > full_counts[14]
        || counts.outbox_status_count > full_counts[15]
        || counts.idempotency_count > full_counts[8]
        || counts.audit_count > full_counts[21]
        || counts.audit_by_request_count > full_counts[22]
    {
        return Err(CheckpointIgnoreReason::CountImpossible);
    }
    // At S=head no legitimate suffix can change live entity cardinality. This
    // O(1) check preserves the old fail-to-full-validation behavior for a
    // vanished or injected current row without comparing the at-S snapshot to
    // a legitimately newer current state when S<head.
    if checkpoint.checkpoint_commit_sequence() == head
        && checkpoint_v2.entity_counts().live_entity_count != full_counts[5]
    {
        return Err(CheckpointIgnoreReason::EntityChainMismatch);
    }
    // The V2 proof is anchored at S, not at the current head. Comparing the
    // checkpoint fingerprint directly to current ENTITY_CHAIN_HEADS would
    // invalidate every legitimate post-checkpoint write. Load the atomically
    // published at-S snapshot instead; the startup entity-chain pass advances
    // these exact heads through (S, head] and then compares them to current
    // authoritative heads and entity presence.
    let entity_heads_at_s = load_checkpoint_entity_heads(transaction, &checkpoint_v2)?;
    Ok(ActiveCheckpoint {
        checkpoint_commit_sequence: checkpoint.checkpoint_commit_sequence(),
        audit_sequence_bound: checkpoint.audit_sequence_bound(),
        retained: checkpoint.retained(),
        counts,
        entity_heads_at_s: Some(entity_heads_at_s),
        checkpoint_hash: *checkpoint.checkpoint_hash().as_bytes(),
    })
}

fn load_checkpoint_entity_heads(
    transaction: &ReadTransaction,
    checkpoint: &StoredValidatedPrefixCheckpointV2,
) -> Result<
    std::collections::BTreeMap<EntityTarget, riffdb_storage_api::EntityChainHeadV1>,
    CheckpointIgnoreReason,
> {
    let table = transaction
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .map_err(|_| CheckpointIgnoreReason::EntityChainMismatch)?;
    let mut heads = std::collections::BTreeMap::new();
    let mut live_pairs = Vec::new();
    let mut live_entity_count = 0_u64;
    let mut deleted_entity_count = 0_u64;
    let mut entity_transition_count = 0_u64;
    for row in table
        .iter()
        .map_err(|_| CheckpointIgnoreReason::EntityChainMismatch)?
    {
        let (key, value) = row.map_err(|_| CheckpointIgnoreReason::EntityChainMismatch)?;
        let decoded = riffdb_storage_api::decode_entity_chain_head_v1(value.value())
            .map_err(|_| CheckpointIgnoreReason::EntityChainMismatch)?;
        let head = decoded.into_parts().0;
        if head.target().key().as_bytes() != key.value() {
            return Err(CheckpointIgnoreReason::EntityChainMismatch);
        }
        entity_transition_count = entity_transition_count
            .checked_add(head.chain_revision())
            .ok_or(CheckpointIgnoreReason::EntityChainMismatch)?;
        match head.state() {
            riffdb_storage_api::EntityChainStateV1::Live { version, .. } => {
                live_entity_count = live_entity_count
                    .checked_add(1)
                    .ok_or(CheckpointIgnoreReason::EntityChainMismatch)?;
                live_pairs.push((head.target().clone(), version));
            }
            riffdb_storage_api::EntityChainStateV1::Deleted => {
                deleted_entity_count = deleted_entity_count
                    .checked_add(1)
                    .ok_or(CheckpointIgnoreReason::EntityChainMismatch)?;
            }
            riffdb_storage_api::EntityChainStateV1::NeverExisted => {
                return Err(CheckpointIgnoreReason::EntityChainMismatch);
            }
        }
        if heads.insert(head.target().clone(), head).is_some() {
            return Err(CheckpointIgnoreReason::EntityChainMismatch);
        }
    }
    let counts = ValidatedPrefixEntityTransitionCounts {
        live_entity_count,
        deleted_entity_count,
        entity_transition_count,
    };
    let transition_fingerprint = EntityTransitionFingerprint::from_sorted_heads(heads.values())
        .map_err(|_| CheckpointIgnoreReason::EntityChainMismatch)?;
    live_pairs.sort_by(|left, right| left.0.cmp(&right.0));
    let live_fingerprint = EntityChainFingerprint::from_sorted_pairs(
        live_pairs
            .iter()
            .map(|(target, version)| (target, *version)),
    );
    if counts != checkpoint.entity_counts()
        || transition_fingerprint != checkpoint.entity_transition_fingerprint()
        || live_fingerprint != checkpoint.base().entity_chain_fingerprint()
    {
        return Err(CheckpointIgnoreReason::EntityChainMismatch);
    }
    Ok(heads)
}

/// Derives W=8 window start sequences in [1, S] from the checkpoint self-hash.
pub(crate) fn sample_window_starts(
    checkpoint_hash: &[u8; 32],
    s: u64,
) -> [u64; SAMPLE_WINDOW_COUNT] {
    let mut starts = [0_u64; SAMPLE_WINDOW_COUNT];
    if s == 0 {
        return starts;
    }
    for (i, start) in starts.iter_mut().enumerate() {
        let off = (i * 4) % 32;
        let word = u32::from_be_bytes([
            checkpoint_hash[off],
            checkpoint_hash[(off + 1) % 32],
            checkpoint_hash[(off + 2) % 32],
            checkpoint_hash[(off + 3) % 32],
        ]);
        // Uniform in [1, S].
        *start = (u64::from(word) % s).saturating_add(1);
    }
    starts
}

fn last_commit_sequence(
    transaction: &ReadTransaction,
) -> Result<Option<CommitSequence>, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    command_authority_head(&commits, &events)
}

fn last_audit_sequence(
    transaction: &ReadTransaction,
) -> Result<Option<AdministrationSequence>, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let Some((key, _)) = table.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    Ok(Some(keys::decode_audit_key(key.value()).map_err(|_| {
        storage_error(StorageErrorKind::CorruptData)
    })?))
}

fn count_commits_le(
    transaction: &ReadTransaction,
    s: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let upper =
        CommitSequence::new(s).ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let upper_key = keys::encode_application_sequence_key(upper);
    let mut count = 0_u64;
    for entry in table
        .range::<&[u8]>(..=upper_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        let sequence = keys::decode_application_sequence_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if sequence.get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_event_keys_le(
    transaction: &ReadTransaction,
    definition: redb::TableDefinition<'_, &'static [u8], &'static [u8]>,
    s: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(definition).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        let Ok(id) = keys::decode_event_key(key.value()) else {
            continue;
        };
        if id.commit_sequence().get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_event_routes_le(
    transaction: &ReadTransaction,
    s: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        let Ok((_, id)) = keys::decode_event_route_key(key.value()) else {
            continue;
        };
        if id.commit_sequence().get() <= s {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_idempotency_le(
    transaction: &ReadTransaction,
    s: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if s == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        if classify_terminal_row(value.value()).is_prefix_outcome(s) {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_audit_le(
    transaction: &ReadTransaction,
    bound: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if bound == 0 {
        return Ok(0);
    }
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let upper = AdministrationSequence::new(bound)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let upper_key = keys::encode_audit_key(upper);
    let mut count = 0_u64;
    for entry in table
        .range::<&[u8]>(..=upper_key.as_slice())
        .map_err(precommit_storage_error)?
    {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        let seq = keys::decode_audit_key(key.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if seq.get() <= bound {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn count_audit_by_request_le(
    transaction: &ReadTransaction,
    bound: u64,
    rows_walked: &mut u64,
) -> Result<u64, StorageError> {
    if bound == 0 {
        return Ok(0);
    }
    let table = transaction
        .open_table(AUDIT_BY_REQUEST)
        .map_err(table_error)?;
    let mut count = 0_u64;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        *rows_walked = rows_walked.saturating_add(1);
        let Ok((_, seq)) = keys::decode_audit_by_request_key(key.value()) else {
            continue;
        };
        if seq.get() <= bound {
            count = count
                .checked_add(1)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
    }
    Ok(count)
}

fn entity_chain_fingerprint_from_entities(
    transaction: &ReadTransaction,
) -> Result<EntityChainFingerprint, StorageError> {
    let table = transaction.open_table(ENTITIES).map_err(table_error)?;
    let mut pairs = Vec::new();
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (_, value) = entry.map_err(precommit_storage_error)?;
        let record = codec::decode_entity_record_v1(value.value())?
            .into_parts()
            .0;
        pairs.push((record.target().clone(), record.entity_version()));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(EntityChainFingerprint::from_sorted_pairs(
        pairs.iter().map(|(t, v)| (t, *v)),
    ))
}

/// Lower-bound event key for rows with commit sequence > S.
pub(crate) fn first_event_key_after(s: u64) -> Option<[u8; 12]> {
    let next = CommitSequence::new(s.checked_add(1)?)?;
    Some(keys::encode_event_key(EventId::new(next, 0)))
}

/// Whether a sequence-keyed structural phase uses exclusive range-skip at S.
pub(crate) fn is_range_skipped_phase(phase: usize) -> bool {
    matches!(phase, 10 | 12 | 14 | 15 | 21)
}

/// Whether inspect may be skipped for a prefix row of a full-scan seq-linked table.
///
/// Phase 8 (`IDEMPOTENCY`) is NOT decided here: its prefix test and the terminal
/// census that seeds [`CheckpointCountSource::DurableLengths`] are two questions
/// about one decode, so the session classifies the row once with
/// [`classify_terminal_row`] and answers both from
/// [`TerminalRowClass::is_prefix_outcome`] — the identical predicate this arm
/// used to spell out.
pub(crate) fn skip_inspect_for_prefix_row(
    phase: usize,
    key: &[u8],
    s: u64,
    audit_bound: u64,
) -> bool {
    match phase {
        13 => {
            keys::decode_event_route_key(key).is_ok_and(|(_, id)| id.commit_sequence().get() <= s)
        }
        22 => keys::decode_audit_by_request_key(key).is_ok_and(|(_, seq)| seq.get() <= audit_bound),
        _ => false,
    }
}

/// Closed classification of one `IDEMPOTENCY` row from a single decode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalRowClass {
    /// Terminal stored outcome at this commit sequence.
    Outcome(u64),
    /// Terminal execution failure; carries no commit sequence and is never
    /// admitted by `idempotency_count`.
    Failed,
    /// Value the terminal codec rejects; the row inspector reports the finding.
    Undecodable,
}

impl TerminalRowClass {
    /// Whether this row belongs to the validated prefix of a checkpoint at S.
    pub(crate) const fn is_prefix_outcome(self, s: u64) -> bool {
        matches!(self, Self::Outcome(sequence) if sequence <= s)
    }
}

/// Classifies one `IDEMPOTENCY` row: the single decode both the checkpointed
/// prefix skip and the terminal execution-failure census read.
pub(crate) fn classify_terminal_row(value: &[u8]) -> TerminalRowClass {
    match codec::decode_idempotency_record_v1(value) {
        Ok(item) => match item.into_parts().0 {
            crate::codec::IdempotencyRecordV1::StoredOutcome(outcome) => {
                TerminalRowClass::Outcome(outcome.commit_sequence().get())
            }
            crate::codec::IdempotencyRecordV1::ExecutionFailed(_) => TerminalRowClass::Failed,
            crate::codec::IdempotencyRecordV1::CommandLocator(locator) => {
                TerminalRowClass::Outcome(locator.commit_sequence().get())
            }
        },
        Err(_) => TerminalRowClass::Undecodable,
    }
}
