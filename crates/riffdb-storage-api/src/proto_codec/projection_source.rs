//! ADR-0316 first-party durable source mappings, with source-owned decode work.

use super::changelog::{history_from_wire, history_to_wire};
use super::projection::{projection_identity_from_proto, projection_identity_to_proto};
use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, DurableCodecErrorKind, encode_message,
    encoded_message_charge, fixed, require, storage_result,
};
use crate::{
    AuthoritativeTransactionBindingV3, ChangelogTransactionSequence, EncodedContentCharge,
    EncodedPageItem, GenerationSourceBaseV1, MAX_PROJECTION_SOURCE_CONTROL_BYTES,
    ProjectionCheckpointOrdinalAllocatorV1, ProjectionCheckpointOrdinalPositionV1,
    ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1, ProjectionSourceLifecycleV1,
    ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1, ProjectionSourceReferenceV1,
    ProjectionSourceRootV1, ProjectionSourceRowV1,
};
use riffdb_proto::{
    envelope::{
        ProjectionSourceDecodeErrorV1, ProjectionSourceDecodeScratchV1, ProjectionSourceDecodeV1,
        ProjectionSourceMessageV1, inspect_projection_source_decode_v1,
    },
    storage::v1 as wire,
};
use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
use riffdb_types::{
    AdministrationSequence, CommitSequence, ContentDigest, DatabaseId, DualFrontier, HashDomain,
    InspectOwnedCapacityV1, OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
    ProjectionCheckpointOrdinal, ProjectionGeneration, ProjectionSourceLocatorV1,
    inspect_canonical_decode_capacity_v1,
};
use riffdb_types::{ProjectionSourceWorkAllocationV1, ProjectionSourceWorkingBudgetV1};

mod control_encode_buffers;
mod maintenance_capacity;
pub use maintenance_capacity::projection_source_maintenance_future_encoded_bound_admitted_v1;
mod caller_admission;
pub use caller_admission::*;

pub(super) mod capture_metadata;
pub use capture_metadata::*;
mod control_encode_bound;
pub use control_encode_bound::projection_source_root_future_encoded_bound_v1;
pub(super) mod metadata;
mod row;
mod row_encode_bound;
pub use metadata::*;
pub use row::*;
pub use row_encode_bound::{
    projection_source_row_encode_work_bound_v1, projection_source_row_max_encoded_bytes_v1,
};

const ROW: &str = "riffdb.storage.v1.StoredProjectionSourceRowV1";
const OBJECT: &str = "riffdb.storage.v1.StoredProjectionSourceObjectV1";
const ROOT: &str = "riffdb.storage.v1.StoredProjectionSourceRootV1";
const MAINTENANCE: &str = "riffdb.storage.v1.StoredProjectionSourceMaintenanceV1";
const BASE: &str = "riffdb.storage.v1.StoredGenerationSourceBaseV1";
const ALLOCATOR: &str = "riffdb.storage.v1.StoredProjectionCheckpointOrdinalAllocatorV1";

/// Safe distinction between transient source capacity and ordinary validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourceRecordDecodeErrorV1 {
    /// The fixed source-worker ledger cannot admit this bounded decode.
    AdmissionUnavailable,
    /// Canonical durable validation or semantic reconstruction failed.
    Codec(DurableCodecError),
}
impl From<BudgetUnavailable> for ProjectionSourceRecordDecodeErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::AdmissionUnavailable
    }
}
impl From<DurableCodecError> for ProjectionSourceRecordDecodeErrorV1 {
    fn from(error: DurableCodecError) -> Self {
        Self::Codec(error)
    }
}
impl From<ProjectionSourceDecodeErrorV1> for ProjectionSourceRecordDecodeErrorV1 {
    fn from(error: ProjectionSourceDecodeErrorV1) -> Self {
        match error {
            ProjectionSourceDecodeErrorV1::AdmissionUnavailable => Self::AdmissionUnavailable,
            ProjectionSourceDecodeErrorV1::Envelope(error) => {
                Self::Codec(DurableCodecError::from_decode_envelope(error))
            }
        }
    }
}
impl std::fmt::Display for ProjectionSourceRecordDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionUnavailable => {
                f.write_str("projection source record admission unavailable")
            }
            Self::Codec(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ProjectionSourceRecordDecodeErrorV1 {}

pub(super) fn decode_admitted<M, T>(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
    reconstruct: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<EncodedPageItem<ProjectionSourceWorkAllocationV1<T>>, ProjectionSourceRecordDecodeErrorV1>
where
    M: ProjectionSourceMessageV1 + Clone,
    T: InspectOwnedCapacityV1,
{
    decode_admitted_with_additional_work(encoded, budget, 0, reconstruct)
}

/// Extra closed semantic allocations are admitted before any generated decode
/// or reconstruction. Existing six Source7 roles retain their zero extra work.
pub(super) fn decode_admitted_with_additional_work<M, T>(
    encoded: &[u8],
    budget: &ProjectionSourceWorkingBudgetV1,
    additional_work_bytes: u64,
    reconstruct: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<EncodedPageItem<ProjectionSourceWorkAllocationV1<T>>, ProjectionSourceRecordDecodeErrorV1>
where
    M: ProjectionSourceMessageV1 + Clone,
    T: InspectOwnedCapacityV1,
{
    let input = inspect_projection_source_decode_v1::<M>(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let maximum = semantic_work_bound(&input)?
        .checked_add(additional_work_bytes)
        .ok_or(BudgetUnavailable)?;
    let work = budget.reserve(maximum)?;
    reconstruct_admitted_with_work(encoded.len(), input, budget, work, reconstruct)
}

// Both entry points construct their reservation from this exact retained budget.
// There is no external reservation parameter or caller-selected ledger.
fn reconstruct_admitted_with_work<M, T>(
    encoded_len: usize,
    input: ProjectionSourceDecodeV1<'_, M>,
    budget: &ProjectionSourceWorkingBudgetV1,
    work: riffdb_types::ProjectionSourceWorkReservationV1,
    reconstruct: impl FnOnce(M) -> Result<T, DurableCodecError>,
) -> Result<EncodedPageItem<ProjectionSourceWorkAllocationV1<T>>, ProjectionSourceRecordDecodeErrorV1>
where
    M: ProjectionSourceMessageV1 + Clone,
    T: InspectOwnedCapacityV1,
{
    let mut scratch = ProjectionSourceDecodeScratchV1::new(encoded_len, budget)?;
    let message = input.decode_with_scratch(&mut scratch)?;
    // Wire clone, canonical decoding, transient key reconstruction, metadata
    // and census scratch were admitted together before any is constructed.
    let value = reconstruct((*message).clone())?;
    drop(message);
    drop(scratch);
    let value = work.settle_shared_value(value)?;
    let charge = EncodedContentCharge::new(encoded_len).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(value, charge))
}

fn semantic_work_bound<M: ProjectionSourceMessageV1>(
    input: &ProjectionSourceDecodeV1<'_, M>,
) -> Result<u64, ProjectionSourceRecordDecodeErrorV1> {
    let mut heap = 0_u64;
    let mut groups = 0_u64;
    for bytes in input.canonical_group_values() {
        let bytes = bytes.map_err(DurableCodecError::from_decode_envelope)?;
        let bound = inspect_canonical_decode_capacity_v1(bytes)
            .map_err(|_| DurableCodecError::corrupt())?;
        heap = heap
            .checked_add(bound.owned_heap_bytes())
            .ok_or(BudgetUnavailable)?;
        groups = groups.checked_add(1).ok_or(BudgetUnavailable)?;
    }
    if let Some(bytes) = input.canonical_measures() {
        heap = heap
            .checked_add(
                inspect_canonical_decode_capacity_v1(bytes)
                    .map_err(|_| DurableCodecError::corrupt())?
                    .owned_heap_bytes(),
            )
            .ok_or(BudgetUnavailable)?;
    }
    // Closed typed canonical vectors, verification buffers, generated clone,
    // descriptor/reference ownership and the existing inspection scratch.
    let group_headers = groups
        .checked_mul(std::mem::size_of::<riffdb_types::CanonicalValue>() as u64)
        .ok_or(BudgetUnavailable)?;
    heap.checked_add(group_headers)
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| bytes.checked_add(input.transport_work_bound_v1().ok()?.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .and_then(|bytes| bytes.checked_add(128 * 1024))
        .ok_or(BudgetUnavailable)
        .map_err(Into::into)
}

fn encode_control<M: prost::Message + riffdb_proto::durable::WritableRecordMessage>(
    name: &'static str,
    message: &M,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if encoded_message_charge(name, message)?.get() > MAX_PROJECTION_SOURCE_CONTROL_BYTES {
        return Err(DurableCodecError::new(DurableCodecErrorKind::LimitExceeded));
    }
    encode_message(name, message)
}

fn locator_to_wire(value: ProjectionSourceLocatorV1) -> wire::ProjectionSourceLocatorV1 {
    wire::ProjectionSourceLocatorV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        allocation_history_incarnation: value.allocation_history_incarnation(),
        checkpoint_ordinal: value.ordinal().get(),
    }
}
fn locator_from_wire(
    value: wire::ProjectionSourceLocatorV1,
) -> Result<ProjectionSourceLocatorV1, DurableCodecError> {
    ProjectionSourceLocatorV1::new(
        DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        value.allocation_history_incarnation,
        ProjectionCheckpointOrdinal::new(value.checkpoint_ordinal)
            .ok_or_else(DurableCodecError::corrupt)?,
    )
    .ok_or_else(DurableCodecError::corrupt)
}
fn digest_to_wire(value: ContentDigest) -> wire::ProjectionSourceDigestV1 {
    wire::ProjectionSourceDigestV1 {
        scheme: u32::from(value.scheme()),
        digest: value.as_bytes().to_vec(),
    }
}
fn digest_from_wire(
    value: wire::ProjectionSourceDigestV1,
    domain: HashDomain,
) -> Result<ContentDigest, DurableCodecError> {
    ContentDigest::from_stored_parts(
        u8::try_from(value.scheme).map_err(|_| DurableCodecError::corrupt())?,
        domain,
        fixed(value.digest)?,
    )
    .ok_or_else(DurableCodecError::corrupt)
}
fn reference_to_wire(value: &ProjectionSourceReferenceV1) -> wire::ProjectionSourceReferenceV1 {
    wire::ProjectionSourceReferenceV1 {
        identity: Some(projection_identity_to_proto(value.identity())),
        locator: Some(locator_to_wire(value.locator())),
        descriptor_digest: Some(digest_to_wire(value.digest())),
        origin_commitment: Some(digest_to_wire(value.origin())),
    }
}
fn reference_from_wire(
    value: wire::ProjectionSourceReferenceV1,
) -> Result<ProjectionSourceReferenceV1, DurableCodecError> {
    storage_result(ProjectionSourceReferenceV1::new(
        projection_identity_from_proto(require(value.identity)?)?,
        locator_from_wire(require(value.locator)?)?,
        digest_from_wire(
            require(value.descriptor_digest)?,
            HashDomain::ProjectionSourceDescriptor,
        )?,
        digest_from_wire(
            require(value.origin_commitment)?,
            HashDomain::ProjectionSourceOrigin,
        )?,
    ))
}

fn descriptor_to_wire(value: &ProjectionSourceDescriptorV1) -> wire::ProjectionSourceDescriptorV1 {
    wire::ProjectionSourceDescriptorV1 {
        identity: Some(projection_identity_to_proto(value.reference().identity())),
        locator: Some(locator_to_wire(value.reference().locator())),
        applied_through: value.applied_through().get(),
        captured_source: Some(history_to_wire(value.captured_source())),
        origin_commitment: Some(digest_to_wire(value.reference().origin())),
        prior_source_digest: value.prior_source_digest().map(digest_to_wire),
        row_count: value.row_count(),
        canonical_row_bytes: value.canonical_row_bytes(),
        canonical_row_root: Some(digest_to_wire(value.row_root())),
        descriptor_digest: Some(digest_to_wire(value.reference().digest())),
    }
}
fn descriptor_from_wire(
    value: wire::ProjectionSourceDescriptorV1,
) -> Result<ProjectionSourceDescriptorV1, DurableCodecError> {
    let expected = digest_from_wire(
        require(value.descriptor_digest)?,
        HashDomain::ProjectionSourceDescriptor,
    )?;
    let descriptor = storage_result(ProjectionSourceDescriptorV1::new(
        ProjectionSourceDescriptorPartsV1 {
            identity: projection_identity_from_proto(require(value.identity)?)?,
            locator: locator_from_wire(require(value.locator)?)?,
            applied_through: CommitSequence::new(value.applied_through)
                .ok_or_else(DurableCodecError::corrupt)?,
            captured_source: history_from_wire(require(value.captured_source)?)?,
            origin: digest_from_wire(
                require(value.origin_commitment)?,
                HashDomain::ProjectionSourceOrigin,
            )?,
            prior_source_digest: value
                .prior_source_digest
                .map(|digest| digest_from_wire(digest, HashDomain::ProjectionSourceDescriptor))
                .transpose()?,
            row_count: value.row_count,
            canonical_row_bytes: value.canonical_row_bytes,
            row_root: digest_from_wire(
                require(value.canonical_row_root)?,
                HashDomain::ProjectionSourceRows,
            )?,
        },
    ))?;
    if descriptor.reference().digest() != expected {
        return Err(DurableCodecError::corrupt());
    }
    Ok(descriptor)
}
fn frontier_to_wire(value: DualFrontier) -> wire::ProjectionSourceDualFrontierV1 {
    wire::ProjectionSourceDualFrontierV1 {
        application_sequence: value.application().map(CommitSequence::get),
        administration_sequence: value.administration().map(AdministrationSequence::get),
    }
}
fn frontier_from_wire(
    value: wire::ProjectionSourceDualFrontierV1,
) -> Result<DualFrontier, DurableCodecError> {
    Ok(DualFrontier::new(
        value
            .application_sequence
            .map(|value| CommitSequence::new(value).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
        value
            .administration_sequence
            .map(|value| AdministrationSequence::new(value).ok_or_else(DurableCodecError::corrupt))
            .transpose()?,
    ))
}
pub(super) fn publication_to_wire(
    value: AuthoritativeTransactionBindingV3,
) -> wire::ProjectionSourcePublicationBindingV1 {
    wire::ProjectionSourcePublicationBindingV1 {
        database_id: value.database_id.as_bytes().to_vec(),
        history_incarnation: value.history_incarnation,
        predecessor: value.predecessor.map(ChangelogTransactionSequence::get),
        sequence: value.sequence.get(),
        predecessor_frontier: Some(frontier_to_wire(value.predecessor_frontier)),
        covered_frontier: Some(frontier_to_wire(value.covered_frontier)),
        prior_history_hash: value.prior_history_hash.to_vec(),
    }
}
pub(super) fn publication_from_wire(
    value: wire::ProjectionSourcePublicationBindingV1,
) -> Result<AuthoritativeTransactionBindingV3, DurableCodecError> {
    Ok(AuthoritativeTransactionBindingV3 {
        database_id: DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        history_incarnation: value.history_incarnation,
        predecessor: value
            .predecessor
            .map(|value| {
                ChangelogTransactionSequence::new(value).ok_or_else(DurableCodecError::corrupt)
            })
            .transpose()?,
        sequence: ChangelogTransactionSequence::new(value.sequence)
            .ok_or_else(DurableCodecError::corrupt)?,
        predecessor_frontier: frontier_from_wire(require(value.predecessor_frontier)?)?,
        covered_frontier: frontier_from_wire(require(value.covered_frontier)?)?,
        prior_history_hash: fixed(value.prior_history_hash)?,
    })
}

#[cfg(test)]
mod tests;

mod control_encode;
pub use control_encode::*;

mod control_history_encode;
pub use control_history_encode::{
    encode_changelog_history_state_source_admitted_v3,
    encode_changelog_transaction_allocator_source_admitted_v3,
};
