//! Fixed Source-only history/control transaction seam. No semantic recipe,
//! arbitrary callback, raw transaction, or publication authority escapes here.
//! Preparation hashes/encodes outside the primary lease. Application accepts
//! only its same canonical intent and a genuine prepared Source usage guard.
use super::*;
use crate::{
    RedbOperationalPorts,
    error::{codec_error, precommit_storage_error, storage_error, table_error, transaction_error},
    gate::ExclusiveLease,
    layout::{self, AUDIT, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, META},
    locator_capability::LocatorCapability,
    locator_memory::LocatorAllocationClass,
    source_checkpoint_adapter::{NativeSourceCheckpointRequest, NativeSourceStepIntent},
    store::{RedbReadAccess, SourceReadWork},
};
use redb::{Durability, ReadableTable, WriteTransaction};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as Old, AuthoritativeNamespaceV3 as N, ChangelogAttributionV3,
    ChangelogHistoryStateV3,
    proto_codec::{
        ProjectionSourceActiveCatalogDecodeAdmissionV1, encode_changelog_history_state_v3,
        encode_changelog_transaction_allocator_v3,
    },
};
use riffdb_types::{ProjectionSourceWorkReservationV1, owned_allocation_capacity_bound_v1};
use std::mem::size_of;

const FIXED_ROOT_BYTES: usize = 512;
const SIDE_CONTROL_BYTES: usize = riffdb_storage_api::MAX_PROJECTION_SOURCE_CONTROL_BYTES;
// Closed root family: no caller-selected key or namespace can enter this seam.
// The ordinary head/frontiers, current lineage/catalog/epoch/admission and
// lifecycle bytes all precede installation of the in-flight Source mutation.
fn fixed_keys() -> Result<[&'static str; 13], StorageError> {
    let key = |namespace: Old| namespace.metadata_key().ok_or_else(integrity);
    Ok([
        layout::META_FORMAT_VERSION,
        layout::META_DATABASE_ID,
        layout::META_HISTORY_INCARNATION,
        layout::META_RECORD_REGISTRY,
        layout::META_APPLICATION_SEQUENCE,
        layout::META_ADMINISTRATION_SEQUENCE,
        layout::META_CLEAN_CLOSE_LIFECYCLE,
        key(Old::AuthoritativeStateCatalog)?,
        key(Old::LeadershipEpoch)?,
        key(Old::ReplicationFollowerState)?,
        crate::primary_admission_roots::key()?,
        key(Old::ChangelogHistoryState)?,
        key(Old::NextChangelogTransaction)?,
    ])
}

pub(crate) struct PreparedSourceHistoryControls<'work> {
    actual: &'work RedbOperationalPorts,
    epoch: SourceUsageEpoch<'work>,
    intent: NativeSourceStepIntent,
    publication: u64,
    durable_epoch: u64,
    fixed: [Option<Vec<u8>>; 13],
    pointer: Vec<u8>,
    origin_key: [u8; 9],
    origin: Vec<u8>,
    successor: ChangelogHistoryStateV3,
    encoded_history: Vec<u8>,
    encoded_allocator: Vec<u8>,
    locator: Option<LocatorCapability>,
    _read_work: SourceReadWork,
    // All owned canonical buffers/control fields die before their admission.
    _control: ProjectionSourceWorkReservationV1,
}
impl Drop for PreparedSourceHistoryControls<'_> {
    fn drop(&mut self) {}
}

fn buffer_bound(bytes: usize) -> Result<u64, StorageError> {
    owned_allocation_capacity_bound_v1(bytes, 1).map_err(|_| unavailable())
}
fn copy_fixed(bytes: &[u8], maximum: usize) -> Result<Vec<u8>, StorageError> {
    if bytes.len() > maximum {
        return Err(unavailable());
    }
    Ok(bytes.to_vec())
}
fn require_source_only(intent: &NativeSourceStepIntent) -> Result<(), StorageError> {
    let receipt = intent.receipt();
    if receipt.attribution() != ChangelogAttributionV3::ProjectionSourceMaintenance
        || receipt.binding().predecessor_frontier != receipt.binding().covered_frontier
        || receipt.mutations().is_empty()
        || receipt.mutations().len() > 128 + 6
        || receipt.mutations().iter().any(|mutation| {
            !matches!(
                mutation.namespace(),
                N::ProjectionSourceRows
                    | N::ProjectionSourceObjects
                    | N::ProjectionSourceRoots
                    | N::ProjectionSourceMaintenance
                    | N::ProjectionSourceAllocator
            )
        })
    {
        return Err(integrity());
    }
    Ok(())
}
fn check_epoch(
    epoch: &SourceUsageEpoch<'_>,
    actual: &RedbOperationalPorts,
    intent: &NativeSourceStepIntent,
) -> Result<(), StorageError> {
    use riffdb_projection::source_driver::SourceCheckpointOperationKind as O;
    match intent.operation() {
        O::Allocate => epoch.verify_for_native_allocation_observation(actual),
        O::Copy | O::Publish => epoch.verify_for_native_step_observation(actual, intent.locator()),
        // Abort/GC require their actual recovery owner, not a candidate request.
        _ => Err(unavailable()),
    }
}

impl<'work> PreparedSourceHistoryControls<'work> {
    /// Off-gate preparation only. A joined existing drainer must already have
    /// supplied a drained current root; this helper never drains or joins work.
    pub(crate) fn prepare<'request, 'candidate, 'stage, 'slot, 'session>(
        request: &NativeSourceCheckpointRequest<
            'request,
            'candidate,
            'stage,
            'slot,
            'session,
            'work,
        >,
        intent: NativeSourceStepIntent,
    ) -> Result<Self, StorageError>
    where
        'work: 'session,
        'session: 'slot,
        'slot: 'stage,
        'stage: 'candidate,
    {
        let actual = request.actual_ports();
        require_source_only(&intent)?;
        if request.operation() != intent.operation()
            || request.captured_locator() != intent.locator()
            || request.captured_history().lineage() != intent.predecessor_history().lineage()
        {
            return Err(integrity());
        }
        let shared = &actual.shared;
        let epoch = shared.database.source_usage_domain().capture_epoch()?;
        check_epoch(&epoch, actual, &intent)?;
        if !crate::store::changelog_source_control::drained_for_validation(shared)? {
            return Err(unavailable());
        }
        let budget = shared.database.source_working_budget();
        // Exact finite buffer census, with compiled allocator rounding. No
        // receipt/postimage clone is made: the intent alias retains its charge.
        let bytes = buffer_bound(FIXED_ROOT_BYTES)?
            .checked_mul(15)
            .and_then(|n| n.checked_add(buffer_bound(SIDE_CONTROL_BYTES).ok()?.checked_mul(2)?))
            .and_then(|n| n.checked_add(size_of::<Self>() as u64))
            .ok_or_else(unavailable)?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        let scratch = SourceReadWork::admit(budget, 512 * 1024)?;
        let root = shared.capture_checkpoint_root()?;
        let publication = root.identity();
        let durable_epoch = shared.durable_commit_epoch();
        let access = RedbReadAccess::Durable(root.clone());
        let observed = crate::changelog_v3_cursor::capture_history_position_v1(&access)?;
        if observed != intent.predecessor_history() {
            return Err(unavailable());
        }
        let meta = root.open_table(META).map_err(table_error)?;
        crate::primary_admission_roots::validate_write_receipt(&meta, observed, intent.receipt())?;
        let mut fixed: [Option<Vec<u8>>; 13] = std::array::from_fn(|_| None);
        for (destination, key) in fixed.iter_mut().zip(fixed_keys()?) {
            *destination = meta
                .get(key)
                .map_err(precommit_storage_error)?
                .map(|value| copy_fixed(value.value(), FIXED_ROOT_BYTES))
                .transpose()?;
        }
        // Genuine current actual-owner origin verification stays off-gate;
        // below we retain and later CAS its exact immutable anchor bytes.
        crate::native_source_fresh::validate_same_owner_cold_origin(&shared.database, &root)?;
        if Some(
            crate::native_source_fresh::checked_captured_source_origin_v1(&shared.database, &root)?,
        ) != request.captured_native_origin()
        {
            return Err(unavailable());
        }
        let origin_key = shared
            .database
            .native_origin_context()
            .anchor_key()
            .ok_or_else(unavailable)?;
        let audit = root.open_table(AUDIT).map_err(table_error)?;
        let origin_row = audit
            .get(origin_key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(integrity)?;
        let origin = copy_fixed(origin_row.value(), SIDE_CONTROL_BYTES)?;
        let active = root.open_table(CATALOG_ACTIVE).map_err(table_error)?;
        let pointer_row = active
            .get(CATALOG_ACTIVE_KEY.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(integrity)?;
        let pointer_admission =
            ProjectionSourceActiveCatalogDecodeAdmissionV1::new(budget).map_err(record_error)?;
        let decoded = pointer_admission
            .decode(pointer_row.value())
            .map_err(record_error)?
            .into_parts()
            .0;
        if &*decoded != request.captured_catalog_pointer() {
            return Err(unavailable());
        }
        let pointer = copy_fixed(pointer_row.value(), SIDE_CONTROL_BYTES)?;
        if root
            .open_table(crate::changelog_v3_activation::HISTORY)
            .map_err(table_error)?
            .get(
                intent
                    .receipt()
                    .binding()
                    .sequence
                    .get()
                    .to_be_bytes()
                    .as_slice(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(integrity());
        }
        // Inspect existing canonical bytes and derive the exact successor once,
        // off-gate, without a second receipt serialization or payload graph.
        let successor = observed
            .advance_canonical_v1(intent.canonical_receipt())
            .map_err(crate::changelog_v3_write::value_error)?;
        let encoded_history = encode_changelog_history_state_v3(successor)
            .map_err(codec_error)?
            .into_bytes();
        let encoded_allocator =
            encode_changelog_transaction_allocator_v3(successor.expected_allocator())
                .map_err(codec_error)?
                .into_bytes();
        if encoded_history.len() > FIXED_ROOT_BYTES || encoded_allocator.len() > FIXED_ROOT_BYTES {
            return Err(unavailable());
        }
        let predecessor = root.locator_capability();
        let locator = if predecessor.is_pending() {
            LocatorCapability::try_pending(LocatorAllocationClass::Maintenance)?
        } else {
            LocatorCapability::try_complete_from_source(
                predecessor.require_complete()?.clone(),
                predecessor,
                LocatorAllocationClass::Maintenance,
            )?
        };
        // Drop this actual immutable root before returning. Primary appends may
        // continue; preinstall below rechecks the exact observed root/head.
        check_epoch(&epoch, actual, &intent)?;
        if !crate::store::changelog_source_control::drained_for_validation(shared)?
            || shared.durable_commit_epoch() != durable_epoch
            || shared.capture_checkpoint_root()?.identity() != publication
        {
            return Err(unavailable());
        }
        Ok(Self {
            actual,
            epoch,
            intent,
            publication,
            durable_epoch,
            fixed,
            pointer,
            origin_key,
            origin,
            successor,
            encoded_history,
            encoded_allocator,
            locator: Some(locator),
            _read_work: scratch,
            _control: control,
        })
    }
}

fn record_error(
    error: riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1,
) -> StorageError {
    match error {
        riffdb_storage_api::proto_codec::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => unavailable(),
        _ => integrity(),
    }
}

#[path = "source_usage_history_apply.rs"]
mod apply;
pub(crate) use apply::{
    NativeSourceNoSubmission, SourceHistoryApplyFailure, SourceHistoryNoSubmitYield,
    SourceHistoryPreinstallFailure, SourceHistorySubmitted,
};
