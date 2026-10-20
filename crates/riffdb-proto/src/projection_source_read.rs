//! Borrowed ADR-0316 record inspection and source-owned canonical scratch.
//! No provider ledger or thread-local canonical buffer participates.

use super::{EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError};
use crate::{durable::ReadableRecordMessage, storage::v1 as wire};
use prost::Message;
use riffdb_types::exact_provider_budget_v1::BudgetUnavailable;
use riffdb_types::{
    InspectOwnedCapacityV1, MAX_PROJECTION_SOURCE_CONTROL_BYTES,
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1,
};
use riffdb_types::{
    ProjectionSourceWorkAllocationV1, ProjectionSourceWorkReservationV1,
    ProjectionSourceWorkingBudgetV1,
};
use std::{marker::PhantomData, mem::size_of};

mod sealed {
    pub trait SourceMessage {
        const IS_ROW: bool;
        const MAX_CONTROL_BYTES: usize = super::MAX_PROJECTION_SOURCE_CONTROL_BYTES;
    }
    impl SourceMessage for super::wire::StoredProjectionSourceRowV1 {
        const IS_ROW: bool = true;
    }
    impl SourceMessage for super::wire::StoredProjectionSourceObjectV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::StoredProjectionSourceRootV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::StoredProjectionSourceMaintenanceV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::StoredGenerationSourceBaseV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::ActiveCatalogPointerV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
        const IS_ROW: bool = false;
    }
    impl SourceMessage for super::wire::StoredAuthoritativeCatalogTransitionV1 {
        const IS_ROW: bool = false;
        const MAX_CONTROL_BYTES: usize = 2 * 1024;
    }
    impl SourceMessage for super::wire::StoredSourceInheritanceAdministrationV1 {
        const IS_ROW: bool = false;
        const MAX_CONTROL_BYTES: usize = 16 * 1024;
    }
}
/// Closed generated source-record types, not caller-defined schemas or codecs.
#[doc(hidden)]
pub trait ProjectionSourceMessageV1:
    ReadableRecordMessage + Default + InspectOwnedCapacityV1 + sealed::SourceMessage
{
}
impl<T: ReadableRecordMessage + Default + InspectOwnedCapacityV1 + sealed::SourceMessage>
    ProjectionSourceMessageV1 for T
{
}

/// Separate admission pressure from ordinary fail-closed durable validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionSourceDecodeErrorV1 {
    /// The fixed source-worker ledger cannot admit wire or scratch ownership.
    AdmissionUnavailable,
    /// The canonical durable envelope or payload failed validation.
    Envelope(EnvelopeError),
}
impl From<BudgetUnavailable> for ProjectionSourceDecodeErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::AdmissionUnavailable
    }
}
impl From<EnvelopeError> for ProjectionSourceDecodeErrorV1 {
    fn from(value: EnvelopeError) -> Self {
        Self::Envelope(value)
    }
}
impl std::fmt::Display for ProjectionSourceDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AdmissionUnavailable => {
                f.write_str("projection source decode admission unavailable")
            }
            Self::Envelope(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ProjectionSourceDecodeErrorV1 {}

/// Borrowed, bounded sizing observation. It cannot establish source authority.
#[doc(hidden)]
pub struct ProjectionSourceDecodeV1<'bytes, M: ProjectionSourceMessageV1> {
    original: &'bytes [u8],
    payload: &'bytes [u8],
    legacy: bool,
    canonical_measures: Option<&'bytes [u8]>,
    group_values: usize,
    marker: PhantomData<M>,
}

/// Validate registry identity, checksum and generated wire preflight without
/// constructing owned wire or semantic values. Both V1/V2 framing is supported.
#[doc(hidden)]
pub fn inspect_projection_source_decode_v1<M: ProjectionSourceMessageV1>(
    encoded: &[u8],
) -> Result<ProjectionSourceDecodeV1<'_, M>, EnvelopeError> {
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES
        || (!M::IS_ROW && encoded.len() > M::MAX_CONTROL_BYTES)
    {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    let registry = crate::durable::readable_record_registry();
    let expected = M::record_schema();
    let legacy = !encoded.starts_with(&super::COMPACT_RECORD_MAGIC_V2);
    let payload = if legacy {
        let (schema, frame) = registry.borrowed_v1_payload(encoded)?;
        if schema.record_type() != expected.record_type() {
            return Err(EnvelopeError::UnknownRecordType);
        }
        if schema.schema_hash() != expected.schema_hash() {
            return Err(EnvelopeError::UnsupportedSchemaHash);
        }
        frame.payload
    } else {
        let payload = registry.borrowed_current_compact_payload(encoded, expected)?;
        if encoded[5] != expected.compact_tag()
            || u16::from_be_bytes([encoded[6], encoded[7]]) != expected.schema_revision()
        {
            return Err(EnvelopeError::UnsupportedSchemaHash);
        }
        payload
    };
    let mut canonical_measures = None;
    let mut group_values = 0_usize;
    if M::IS_ROW {
        let mut cursor = crate::wire::Cursor::new(payload);
        while let Some(field) = cursor.next().map_err(|_| EnvelopeError::Malformed)? {
            match field.number {
                3 => {
                    field
                        .require_wire(2)
                        .map_err(|_| EnvelopeError::Malformed)?;
                    group_values = group_values
                        .checked_add(1)
                        .ok_or(EnvelopeError::PayloadTooLarge)?;
                }
                4 => {
                    if canonical_measures.is_some() {
                        return Err(EnvelopeError::InvalidPayload(
                            PayloadValidationError::NonCanonical,
                        ));
                    }
                    canonical_measures = Some(
                        field
                            .require_wire(2)
                            .map_err(|_| EnvelopeError::Malformed)?
                            .bytes,
                    );
                }
                _ => {}
            }
        }
    }
    Ok(ProjectionSourceDecodeV1 {
        original: encoded,
        payload,
        legacy,
        canonical_measures,
        group_values,
        marker: PhantomData,
    })
}

fn transport_work_bound<M>(
    original_bytes: usize,
    group_values: usize,
) -> Result<u64, BudgetUnavailable> {
    let repeated = group_values
        .checked_mul(size_of::<Vec<u8>>())
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(BudgetUnavailable)?;
    u64::try_from(original_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| bytes.checked_add(repeated))
        .and_then(|bytes| bytes.checked_add(2 * size_of::<M>() as u64))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or(BudgetUnavailable)
}

impl<'bytes, M: ProjectionSourceMessageV1> ProjectionSourceDecodeV1<'bytes, M> {
    /// Borrow canonical group values once for semantic allocation preflight.
    pub fn canonical_group_values(&self) -> ProjectionSourceGroupValuesV1<'bytes> {
        ProjectionSourceGroupValuesV1 {
            cursor: crate::wire::Cursor::new(if M::IS_ROW { self.payload } else { &[] }),
            failed: false,
        }
    }
    /// Borrow the canonical measure record without decoding or claiming schema.
    pub const fn canonical_measures(&self) -> Option<&'bytes [u8]> {
        self.canonical_measures
    }
    /// Generated message, buffer and repeated-header allowance. Canonical tree
    /// allocations are separate, inspected by the semantic storage mapper.
    pub fn transport_work_bound_v1(&self) -> Result<u64, BudgetUnavailable> {
        transport_work_bound::<M>(self.original.len(), self.group_values)
    }
    /// Compiled maximum for this closed control role, for admission before an
    /// originating native gate. Rows need their separate canonical tree census.
    pub fn maximum_control_transport_work_bound_v1() -> Result<u64, BudgetUnavailable> {
        if M::IS_ROW {
            return Err(BudgetUnavailable);
        }
        transport_work_bound::<M>(M::MAX_CONTROL_BYTES, 0)
    }
    /// Uses the single existing full canonical parser with caller-owned scratch.
    /// Generated and semantic ownership must be admitted before this operation.
    pub fn decode_with_caller_scratch(
        self,
        scratch: &mut Vec<u8>,
    ) -> Result<M, super::CallerScratchDecodeErrorV1> {
        super::decode_readable_message_with_caller_scratch_v1::<M>(self.original, scratch)
    }
    /// Decode under the source ledger, with private admitted scratch and exact
    /// Prost re-encoding. Returned message charge lasts to its final reference.
    pub fn decode_with_scratch(
        self,
        scratch: &mut ProjectionSourceDecodeScratchV1,
    ) -> Result<ProjectionSourceWorkAllocationV1<M>, ProjectionSourceDecodeErrorV1> {
        if self.original.len() > scratch.maximum {
            return Err(EnvelopeError::EnvelopeTooLarge.into());
        }
        let maximum = self
            .transport_work_bound_v1()?
            .checked_add(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .ok_or(BudgetUnavailable)?;
        let work = scratch.budget.reserve(maximum)?;
        let message = self
            .decode_with_caller_scratch(&mut scratch.buffer)
            .map_err(|error| match error {
                super::CallerScratchDecodeErrorV1::Envelope(error) => {
                    ProjectionSourceDecodeErrorV1::Envelope(error)
                }
                super::CallerScratchDecodeErrorV1::CapacityUnavailable => {
                    ProjectionSourceDecodeErrorV1::AdmissionUnavailable
                }
            })?;
        work.settle_owned_value(message).map_err(Into::into)
    }
}

/// Strict scalar allocator validation for ordinary authoritative metadata
/// checks. It cannot depend on source-worker admission: worker pressure must
/// never refuse an application command. No heap buffer, TLS scratch, ownership
/// census or fresh source ledger is constructed by this path.
#[doc(hidden)]
pub fn decode_projection_checkpoint_ordinal_allocator_inline_v1(
    encoded: &[u8],
) -> Result<wire::StoredProjectionCheckpointOrdinalAllocatorV1, EnvelopeError> {
    type Allocator = wire::StoredProjectionCheckpointOrdinalAllocatorV1;
    const PAYLOAD_MAXIMUM: usize = 22;
    const CANONICAL_PAYLOAD_BYTES: usize = 128;
    const CANONICAL_ENVELOPE_BYTES: usize = 512;
    // This closed generated message contains only integers and a scalar/empty
    // oneof. The exhaustive ownership implementation below must also change
    // if Prost ever adds an owning field to its schema.
    const {
        assert!(!<Allocator as InspectOwnedCapacityV1>::HAS_NESTED_ALLOCATIONS);
    }
    let input = inspect_projection_source_decode_v1::<Allocator>(encoded)?;
    if input.payload.len() > PAYLOAD_MAXIMUM {
        return Err(EnvelopeError::PayloadTooLarge);
    }
    // Construct the generated scalar message from checked borrowed fields.
    // In particular, never call Prost::decode here: even fixed scalar messages
    // can allocate a DecodeError path on malformed input. Cursor and these
    // closed shape errors remain inline values, including every refusal path.
    use wire::stored_projection_checkpoint_ordinal_allocator_v1::Position;
    let mut cursor = crate::wire::Cursor::new(input.payload);
    let mut incarnation = None;
    let mut position = None;
    while let Some(field) = cursor.next().map_err(|_| EnvelopeError::Malformed)? {
        match field.number {
            1 if incarnation.is_none() => {
                incarnation = Some(
                    field
                        .require_varint()
                        .map_err(|_| EnvelopeError::Malformed)?,
                );
            }
            2 if position.is_none() => {
                position = Some(Position::NextOrdinal(
                    field
                        .require_varint()
                        .map_err(|_| EnvelopeError::Malformed)?,
                ));
            }
            3 if position.is_none() => {
                if !field
                    .require_wire(2)
                    .map_err(|_| EnvelopeError::Malformed)?
                    .bytes
                    .is_empty()
                {
                    return Err(EnvelopeError::InvalidPayload(
                        PayloadValidationError::NonCanonical,
                    ));
                }
                position = Some(Position::Exhausted(wire::UnitV1 {}));
            }
            _ => {
                return Err(EnvelopeError::InvalidPayload(
                    PayloadValidationError::NonCanonical,
                ));
            }
        }
    }
    let message = Allocator {
        allocation_history_incarnation: incarnation.unwrap_or_default(),
        position,
    };
    let length = message.encoded_len();
    if length != input.payload.len() || length > PAYLOAD_MAXIMUM {
        return Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::NonCanonical,
        ));
    }
    let mut canonical = [0_u8; CANONICAL_PAYLOAD_BYTES];
    message.encode_raw(&mut &mut canonical[..]);
    if &canonical[..length] != input.payload {
        return Err(EnvelopeError::InvalidPayload(
            PayloadValidationError::NonCanonical,
        ));
    }
    if input.legacy {
        // Reproduce the ordinary StoredEnvelope Prost encoding using the same
        // Prost scalar/key primitives and borrowed byte fields. The generated
        // outer message would allocate String/Vec, so it is deliberately not
        // constructed. Exact comparison retains unknown/order/default and
        // nonminimal-varint rejection for legacy framing.
        use prost::bytes::BufMut;
        use prost::encoding::{WireType, encode_key, encode_varint, encoded_len_varint};
        let schema = Allocator::record_schema();
        let record_type = schema.record_type().as_bytes();
        let schema_hash = schema.schema_hash();
        let checksum = super::payload_crc32c(input.payload);
        let byte_field_len = |value: &[u8]| {
            if value.is_empty() {
                0
            } else {
                1 + encoded_len_varint(value.len() as u64) + value.len()
            }
        };
        let expected = 2
            + byte_field_len(record_type)
            + byte_field_len(input.payload)
            + (if checksum == 0 { 0 } else { 5 })
            + byte_field_len(schema_hash.as_bytes());
        if expected > CANONICAL_ENVELOPE_BYTES || expected != input.original.len() {
            return Err(EnvelopeError::NonCanonicalEnvelope);
        }
        let mut envelope = [0_u8; CANONICAL_ENVELOPE_BYTES];
        let mut buffer = &mut envelope[..];
        prost::encoding::uint32::encode(1, &super::STORAGE_FORMAT_VERSION_V1, &mut buffer);
        for (tag, value) in [(2, record_type), (3, input.payload)] {
            if !value.is_empty() {
                encode_key(tag, WireType::LengthDelimited, &mut buffer);
                encode_varint(value.len() as u64, &mut buffer);
                buffer.put_slice(value);
            }
        }
        if checksum != 0 {
            prost::encoding::fixed32::encode(4, &checksum, &mut buffer);
        }
        encode_key(5, WireType::LengthDelimited, &mut buffer);
        encode_varint(schema_hash.as_bytes().len() as u64, &mut buffer);
        buffer.put_slice(schema_hash.as_bytes());
        let written = CANONICAL_ENVELOPE_BYTES - buffer.len();
        if written != expected || &envelope[..written] != input.original {
            return Err(EnvelopeError::NonCanonicalEnvelope);
        }
    }
    Ok(message)
}

/// Allocation-free bounded iterator over the already preflighted row fields.
pub struct ProjectionSourceGroupValuesV1<'bytes> {
    cursor: crate::wire::Cursor<'bytes>,
    failed: bool,
}
impl<'bytes> Iterator for ProjectionSourceGroupValuesV1<'bytes> {
    type Item = Result<&'bytes [u8], EnvelopeError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            match self.cursor.next() {
                Ok(Some(field)) if field.number == 3 => {
                    return Some(
                        field
                            .require_wire(2)
                            .map(|field| field.bytes)
                            .map_err(|_| EnvelopeError::Malformed),
                    );
                }
                Ok(Some(_)) => {}
                Ok(None) => return None,
                Err(_) => {
                    self.failed = true;
                    return Some(Err(EnvelopeError::Malformed));
                }
            }
        }
    }
}

/// Private per-call encoding storage. Fields drop the data before its charge.
#[doc(hidden)]
pub struct ProjectionSourceDecodeScratchV1 {
    buffer: Vec<u8>,
    maximum: usize,
    budget: ProjectionSourceWorkingBudgetV1,
    _work: ProjectionSourceWorkReservationV1,
}
impl ProjectionSourceDecodeScratchV1 {
    pub fn new(
        maximum: usize,
        budget: &ProjectionSourceWorkingBudgetV1,
    ) -> Result<Self, ProjectionSourceDecodeErrorV1> {
        if maximum > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge.into());
        }
        let bytes = u64::try_from(maximum)
            .ok()
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(BudgetUnavailable)?;
        let work = budget.reserve(bytes)?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(maximum)
            .map_err(|_| BudgetUnavailable)?;
        Ok(Self {
            buffer,
            maximum,
            budget: budget.clone(),
            _work: work,
        })
    }
}

riffdb_types::inspect_owned_capacity_struct_v1!(wire::ActiveCatalogPointerV1 {
    owned: [contract_lineage, contract_bundle_hash],
    inline: [contract_version]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionIdentityV1 {
    owned: [contract_lineage, projection_plan_hash],
    inline: [projection_id]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourceLocatorV1 {
    owned: [database_id],
    inline: [allocation_history_incarnation, checkpoint_ordinal]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourceDigestV1 {
    owned: [digest],
    inline: [scheme]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourceReferenceV1 {
    owned: [identity, locator, descriptor_digest, origin_commitment],
    inline: []
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourceDescriptorV1 {
    owned: [
        identity,
        locator,
        captured_source,
        origin_commitment,
        prior_source_digest,
        canonical_row_root,
        descriptor_digest
    ],
    inline: [applied_through, row_count, canonical_row_bytes]
});
riffdb_types::inspect_owned_capacity_struct_v1!(
    wire::stored_changelog_history_state_v3::Position {
        owned: [history_hash],
        inline: [
            transaction_sequence,
            application_sequence,
            administration_sequence
        ]
    }
);
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredChangelogHistoryStateV3 {
    owned: [database_id, catalog_digest, anchor, tail, minimum_resume],
    inline: [history_incarnation, leadership_epoch]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourceDualFrontierV1 {
    owned: [],
    inline: [application_sequence, administration_sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::ProjectionSourcePublicationBindingV1 {
    owned: [
        database_id,
        predecessor_frontier,
        covered_frontier,
        prior_history_hash
    ],
    inline: [history_incarnation, predecessor, sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredProjectionSourceRowV1 {
    owned: [
        identity,
        locator,
        canonical_group_values,
        canonical_measures
    ],
    inline: [last_changed_sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredProjectionSourceObjectV1 {
    owned: [descriptor],
    inline: [lifecycle]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredProjectionSourceRootV1 {
    owned: [source, publication_binding],
    inline: [administration_sequence]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredProjectionSourceMaintenanceV1 {
    owned: [
        descriptor,
        expected_prior_root,
        last_copied_key,
        rolling_hash
    ],
    inline: [copied_rows, copied_canonical_row_bytes]
});
riffdb_types::inspect_owned_capacity_struct_v1!(wire::StoredGenerationSourceBaseV1 {
    owned: [source],
    inline: [generation, applied_through]
});
impl InspectOwnedCapacityV1 for wire::StoredProjectionCheckpointOrdinalAllocatorV1 {
    const HAS_NESTED_ALLOCATIONS: bool = false;
    fn inspect_owned_capacity(
        &self,
        inspection: &mut riffdb_types::OwnedCapacityInspectionV1,
    ) -> Result<(), riffdb_types::OwnedCapacityInspectionErrorV1> {
        let Self {
            allocation_history_incarnation,
            position,
        } = self;
        inspection.inline(allocation_history_incarnation)?;
        match position {
            None => Ok(()),
            Some(
                wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(
                    value,
                ),
            ) => inspection.inline(value),
            Some(wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::Exhausted(
                value,
            )) => {
                let wire::UnitV1 {} = value;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
#[path = "projection_source_read_tests.rs"]
mod tests;
