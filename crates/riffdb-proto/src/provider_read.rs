//! Borrowed provider source inspection and explicitly admitted decode scratch.
// req: OQ-040
use super::{EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError, StoredEnvelope};
use crate::durable::ReadableRecordMessage;
use crate::storage::v1 as wire;
use prost::Message;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocationOwner, ProviderBudget, Reservation,
};
use std::marker::PhantomData;

mod sealed {
    pub trait ProviderSourceMessage {
        const CANONICAL_FIELD: Option<u32>;
        fn inspect_transport_shape(&self);
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredEntityRecordV1 {
        const CANONICAL_FIELD: Option<u32> = Some(5);
        fn inspect_transport_shape(&self) {
            let Self {
                target,
                entity_version,
                written_by_contract,
                schema_binding,
                canonical_fields,
            } = self;
            inline(entity_version);
            inline(written_by_contract);
            buffer(canonical_fields);
            if let Some(target) = target {
                entity_target(target);
            }
            if let Some(binding) = schema_binding {
                schema_binding_shape(binding);
            }
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredIndexEntryV1 {
        const CANONICAL_FIELD: Option<u32> = Some(3);
        fn inspect_transport_shape(&self) {
            let Self {
                index_entry_key,
                schema_binding,
                canonical_covered_values,
            } = self;
            buffer(index_entry_key);
            buffer(canonical_covered_values);
            if let Some(binding) = schema_binding {
                schema_binding_shape(binding);
            }
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredIndexEntryV2 {
        const CANONICAL_FIELD: Option<u32> = Some(3);
        fn inspect_transport_shape(&self) {
            let Self {
                index_entry_key,
                schema_binding,
                canonical_covered_values,
                partition_key,
            } = self;
            buffer(index_entry_key);
            buffer(canonical_covered_values);
            buffer(partition_key);
            if let Some(binding) = schema_binding {
                schema_binding_shape(binding);
            }
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredIndexGenerationV2 {
        const CANONICAL_FIELD: Option<u32> = None;
        fn inspect_transport_shape(&self) {
            let Self {
                partition_key,
                index_id,
                schema_binding,
                generation,
            } = self;
            buffer(partition_key);
            inline(index_id);
            inline(generation);
            if let Some(binding) = schema_binding {
                schema_binding_shape(binding);
            }
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredContractBundleV1 {
        const CANONICAL_FIELD: Option<u32> = Some(4);
        fn inspect_transport_shape(&self) {
            let Self {
                contract_lineage,
                contract_version,
                contract_bundle_hash,
                canonical_bundle,
            } = self;
            string(contract_lineage);
            inline(contract_version);
            buffer(contract_bundle_hash);
            buffer(canonical_bundle);
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::ActiveCatalogPointerV1 {
        const CANONICAL_FIELD: Option<u32> = None;
        fn inspect_transport_shape(&self) {
            let Self {
                contract_lineage,
                contract_version,
                contract_bundle_hash,
            } = self;
            string(contract_lineage);
            inline(contract_version);
            buffer(contract_bundle_hash);
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredContractWriteRetirementV1 {
        const CANONICAL_FIELD: Option<u32> = None;
        fn inspect_transport_shape(&self) {
            let Self {
                parent_bundle_hash,
                candidate_bundle_hash,
                migration_bundle_hash,
                operation_id,
                administration_sequence,
            } = self;
            buffer(parent_bundle_hash);
            buffer(candidate_bundle_hash);
            buffer(migration_bundle_hash);
            buffer(operation_id);
            inline(administration_sequence);
        }
    }
    impl ProviderSourceMessage for crate::storage::v1::StoredContractMigrationRecordV1 {
        const CANONICAL_FIELD: Option<u32> = None;
        fn inspect_transport_shape(&self) {
            let Self {
                database_id,
                operation_id,
                semantic_input_hash,
                artifacts,
                operation_artifacts,
                source_backup_name,
                source_backup_manifest_checksum,
                principal,
                approval_id,
                predecessor_application_frontier,
                successor_application_frontier,
                checked_rows,
                changed_rows,
                batch_count,
                terminal_validation_digest,
                administration_sequence,
            } = self;
            buffer(database_id);
            buffer(operation_id);
            buffer(semantic_input_hash);
            string(source_backup_name);
            buffer(source_backup_manifest_checksum);
            if let Some(value) = approval_id {
                string(value);
            }
            inline(predecessor_application_frontier);
            inline(successor_application_frontier);
            inline(checked_rows);
            inline(changed_rows);
            inline(batch_count);
            buffer(terminal_validation_digest);
            inline(administration_sequence);
            if let Some(value) = artifacts {
                let crate::storage::v1::ContractMigrationArtifactIdentityV1 {
                    parent_bundle_hash,
                    candidate_bundle_hash,
                    migration_bundle_hash,
                } = value;
                buffer(parent_bundle_hash);
                buffer(candidate_bundle_hash);
                buffer(migration_bundle_hash);
            }
            if let Some(value) = operation_artifacts {
                let crate::storage::v1::ContractMigrationOperationArtifactsV1 {
                    candidate_bundle_length,
                    candidate_bundle_sha256,
                    migration_bundle_length,
                    migration_bundle_sha256,
                } = value;
                inline(candidate_bundle_length);
                buffer(candidate_bundle_sha256);
                inline(migration_bundle_length);
                buffer(migration_bundle_sha256);
            }
            if let Some(value) = principal {
                let crate::storage::v1::AuditPrincipalV1 {
                    principal_id,
                    actor_kind,
                    capability_id,
                    capability_revision,
                } = value;
                string(principal_id);
                inline(actor_kind);
                buffer(capability_id);
                inline(capability_revision);
            }
        }
    }
    fn inline<T: Copy>(_: &T) {}
    fn buffer(_: &Vec<u8>) {}
    fn string(_: &String) {}
    fn entity_target(target: &crate::storage::v1::EntityTargetV1) {
        let crate::storage::v1::EntityTargetV1 {
            entity_type_id,
            entity_key,
        } = target;
        inline(entity_type_id);
        buffer(entity_key);
    }
    fn schema_binding_shape(binding: &crate::storage::v1::DurableKeySchemaBindingV1) {
        let crate::storage::v1::DurableKeySchemaBindingV1 {
            contract_lineage,
            contract_version,
            contract_bundle_hash,
        } = binding;
        string(contract_lineage);
        inline(contract_version);
        buffer(contract_bundle_hash);
    }
}
/// Closed first-party provider source roles; arbitrary messages cannot opt in.
#[doc(hidden)]
pub trait ProviderSourceMessageV1:
    ReadableRecordMessage + Default + sealed::ProviderSourceMessage
{
}
impl ProviderSourceMessageV1 for wire::StoredEntityRecordV1 {}
impl ProviderSourceMessageV1 for wire::StoredIndexEntryV1 {}
impl ProviderSourceMessageV1 for wire::StoredIndexEntryV2 {}
impl ProviderSourceMessageV1 for wire::StoredIndexGenerationV2 {}

impl ProviderSourceMessageV1 for wire::StoredContractBundleV1 {}
impl ProviderSourceMessageV1 for wire::ActiveCatalogPointerV1 {}
impl ProviderSourceMessageV1 for wire::StoredContractWriteRetirementV1 {}
impl ProviderSourceMessageV1 for wire::StoredContractMigrationRecordV1 {}

/// Safe distinction between decode admission pressure and durable rejection.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderSourceDecodeErrorV1 {
    /// Aggregate capacity or allocation is unavailable; no decode is published.
    CapacityUnavailable,
    /// Unchanged durable envelope failure.
    Envelope(EnvelopeError),
}
impl From<EnvelopeError> for ProviderSourceDecodeErrorV1 {
    fn from(error: EnvelopeError) -> Self {
        Self::Envelope(error)
    }
}
impl From<BudgetUnavailable> for ProviderSourceDecodeErrorV1 {
    fn from(_: BudgetUnavailable) -> Self {
        Self::CapacityUnavailable
    }
}
impl std::fmt::Display for ProviderSourceDecodeErrorV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityUnavailable => {
                f.write_str("provider source decode admission unavailable")
            }
            Self::Envelope(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for ProviderSourceDecodeErrorV1 {}

/// Borrowed sizing input, not a decoded or authenticated semantic record.
#[doc(hidden)]
pub struct ProviderSourceDecodeV1<'bytes, M: ProviderSourceMessageV1> {
    original: &'bytes [u8],
    payload: &'bytes [u8],
    canonical: &'bytes [u8],
    legacy: bool,
    marker: PhantomData<M>,
}

/// Inspect before allocating a generated message or semantic canonical fields.
/// Both framing variants share the ordinary decoder's borrowed trust preflight.
#[doc(hidden)]
pub fn inspect_provider_source_decode_v1<M: ProviderSourceMessageV1>(
    encoded: &[u8],
) -> Result<ProviderSourceDecodeV1<'_, M>, EnvelopeError> {
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
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
        // These closed source schemas have no alternate readable identity. Keep
        // the closed generated descriptor inseparable from its payload.
        if schema.schema_hash() != expected.schema_hash() {
            return Err(EnvelopeError::UnsupportedSchemaHash);
        }
        frame.payload
    } else {
        registry.borrowed_current_compact_payload(encoded, expected)?
    };
    let mut cursor = crate::wire::Cursor::new(payload);
    let mut canonical = None;
    while let Some(field) = cursor.next().map_err(|_| EnvelopeError::Malformed)? {
        if Some(field.number) == M::CANONICAL_FIELD {
            if canonical.is_some() {
                return Err(EnvelopeError::InvalidPayload(
                    PayloadValidationError::NonCanonical,
                ));
            }
            canonical = Some(
                field
                    .require_wire(2)
                    .map_err(|_| EnvelopeError::Malformed)?
                    .bytes,
            );
        }
    }
    Ok(ProviderSourceDecodeV1 {
        original: encoded,
        payload,
        canonical: canonical.unwrap_or(&[]),
        legacy,
        marker: PhantomData,
    })
}

/// Private per-call encoding buffer. Data is destroyed before its reservation;
/// this path never borrows or retains the ordinary thread-local scratch.
#[doc(hidden)]
pub struct ProviderDecodeScratchV1 {
    buffer: Vec<u8>,
    maximum: usize,
    budget: ProviderBudget,
    _work: Reservation,
}
impl ProviderDecodeScratchV1 {
    /// Admit the complete buffer before allocating it. Input ceilings remain
    /// the existing durable-envelope ceiling, with no public cache selector.
    pub fn new(
        maximum: usize,
        budget: &ProviderBudget,
    ) -> Result<Self, ProviderSourceDecodeErrorV1> {
        if maximum > MAX_STORED_ENVELOPE_BYTES {
            return Err(EnvelopeError::EnvelopeTooLarge.into());
        }
        let bytes = u64::try_from(maximum)
            .ok()
            .and_then(|n| n.checked_mul(2))
            .and_then(|n| n.checked_add(4096))
            .ok_or(ProviderSourceDecodeErrorV1::CapacityUnavailable)?;
        let work = budget.reserve(bytes)?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(maximum)
            .map_err(|_| ProviderSourceDecodeErrorV1::CapacityUnavailable)?;
        Ok(Self {
            buffer,
            maximum,
            budget: budget.clone(),
            _work: work,
        })
    }
    fn canonical<M: Message>(
        &mut self,
        message: &M,
        expected: &[u8],
    ) -> Result<bool, EnvelopeError> {
        if message.encoded_len() != expected.len() {
            return Ok(false);
        }
        if expected.len() > self.maximum {
            return Err(EnvelopeError::EnvelopeTooLarge);
        }
        self.buffer.clear();
        // The exact encoded length fits the already admitted buffer; encoding
        // cannot trigger Vec growth. The comparison is the unchanged proof.
        message
            .encode(&mut self.buffer)
            .map_err(|_| EnvelopeError::Malformed)?;
        Ok(self.buffer.as_slice() == expected)
    }
}
impl<M: ProviderSourceMessageV1> ProviderSourceDecodeV1<'_, M> {
    /// Canonical entity/index or opaque contract-IR bytes for borrowed sizing. Normal storage
    /// reconstruction remains mandatory before using a semantic entity or row.
    pub const fn canonical_payload(&self) -> &[u8] {
        self.canonical
    }
    /// Decode a closed generated source message with explicit scratch and final
    /// message ownership. Semantic reconstruction needs separate admission.
    pub fn decode_with_scratch(
        self,
        scratch: &mut ProviderDecodeScratchV1,
    ) -> Result<ProviderAllocationOwner<M>, ProviderSourceDecodeErrorV1> {
        if self.original.len() > scratch.maximum {
            return Err(EnvelopeError::EnvelopeTooLarge.into());
        }
        // These closed schemas have no repeated messages. Their buffer lengths
        // sum to at most the input size; the deepest shape has three optional
        // children and at most seventeen buffers. Envelope buffers die before payload
        // decode. Eight times input bytes covers capacities and reconstruction;
        // 32 KiB covers all closed buffer headers and inline child storage.
        let maximum = u64::try_from(self.original.len())
            .ok()
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(32 * 1024))
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<M>() as u64))
            .ok_or(ProviderSourceDecodeErrorV1::CapacityUnavailable)?;
        let work = scratch.budget.reserve(maximum)?;
        if self.legacy {
            let envelope =
                StoredEnvelope::decode(self.original).map_err(|_| EnvelopeError::Malformed)?;
            if !scratch.canonical(&envelope, self.original)? {
                return Err(EnvelopeError::NonCanonicalEnvelope.into());
            }
            // Envelope buffers die before generated payload reconstruction.
            drop(envelope);
        }
        let message = M::decode(self.payload)
            .map_err(|_| EnvelopeError::InvalidPayload(PayloadValidationError::Malformed))?;
        // Exhaustive generated shapes make added or owning fields fail
        // compilation until the closed transport allowance is revisited.
        message.inspect_transport_shape();
        if !scratch.canonical(&message, self.payload)? {
            return Err(EnvelopeError::InvalidPayload(PayloadValidationError::NonCanonical).into());
        }
        work.commit_value(message).map_err(Into::into)
    }
}

#[path = "provider_read_tests.rs"]
#[cfg(test)]
mod tests;

/// Borrowed numerical ownership preview for the closed opaque catalog bundle.
/// It carries no decoded catalog, schema validity or Source completion proof.
#[doc(hidden)]
pub struct SourceCatalogBundleTransportV1<'bytes> {
    input: ProviderSourceDecodeV1<'bytes, wire::StoredContractBundleV1>,
    payload_buffers: [usize; 3],
    envelope_buffers: [usize; 3],
}

/// Inspect the existing envelope/parser before generated ownership is created.
/// Each buffer counts ALL occurrences, including malformed duplicate fields;
/// no caller may invent a capacity or supply a generated message.
#[doc(hidden)]
pub fn inspect_source_catalog_bundle_transport_v1(
    encoded: &[u8],
) -> Result<SourceCatalogBundleTransportV1<'_>, EnvelopeError> {
    let input = inspect_provider_source_decode_v1::<wire::StoredContractBundleV1>(encoded)?;
    let mut payload_buffers = [0_usize; 3];
    let mut cursor = crate::wire::Cursor::new(input.payload);
    while let Some(field) = cursor.next().map_err(|_| EnvelopeError::Malformed)? {
        let slot = match field.number {
            1 => Some(0),
            3 => Some(1),
            4 => Some(2),
            2 => {
                field
                    .require_varint()
                    .map_err(|_| EnvelopeError::Malformed)?;
                None
            }
            _ => None,
        };
        if let Some(slot) = slot {
            let bytes = field
                .require_wire(2)
                .map_err(|_| EnvelopeError::Malformed)?
                .bytes;
            if slot == 0 && std::str::from_utf8(bytes).is_err() {
                return Err(EnvelopeError::InvalidPayload(
                    PayloadValidationError::Malformed,
                ));
            }
            payload_buffers[slot] = payload_buffers[slot]
                .checked_add(bytes.len())
                .ok_or(EnvelopeError::PayloadTooLarge)?;
        }
    }
    let mut envelope_buffers = [0_usize; 3];
    if input.legacy {
        let mut cursor = crate::wire::Cursor::new(encoded);
        while let Some(field) = cursor.next().map_err(|_| EnvelopeError::Malformed)? {
            let slot = match field.number {
                2 => Some(0),
                3 => Some(1),
                5 => Some(2),
                _ => None,
            };
            if let Some(slot) = slot {
                let bytes = field
                    .require_wire(2)
                    .map_err(|_| EnvelopeError::Malformed)?
                    .bytes;
                envelope_buffers[slot] = envelope_buffers[slot]
                    .checked_add(bytes.len())
                    .ok_or(EnvelopeError::PayloadTooLarge)?;
            }
        }
    }
    // Exhaustive field/type witnesses keep this numeric ownership model tied to
    // the actual generated structs. Neither closure constructs a wire object.
    let _: fn(&wire::StoredContractBundleV1) = |wire::StoredContractBundleV1 {
                                                    contract_lineage,
                                                    contract_version,
                                                    contract_bundle_hash,
                                                    canonical_bundle,
                                                }| {
        let _: &String = contract_lineage;
        let _: &u64 = contract_version;
        let _: &Vec<u8> = contract_bundle_hash;
        let _: &Vec<u8> = canonical_bundle;
    };
    let _: fn(&StoredEnvelope) = |StoredEnvelope {
                                      storage_format_version,
                                      record_type,
                                      payload,
                                      payload_crc32c,
                                      schema_hash,
                                  }| {
        let _: &u32 = storage_format_version;
        let _: &String = record_type;
        let _: &Vec<u8> = payload;
        let _: &u32 = payload_crc32c;
        let _: &Vec<u8> = schema_hash;
    };
    Ok(SourceCatalogBundleTransportV1 {
        input,
        payload_buffers,
        envelope_buffers,
    })
}

impl SourceCatalogBundleTransportV1<'_> {
    /// Borrowed tag4 canonical_bundle IR bytes (not the protobuf payload) for
    /// the separately admitted sole IR decoder.
    pub fn canonical_payload(&self) -> &[u8] {
        self.input.canonical_payload()
    }
    /// Conservative actual lineage byte length, including repeated occurrences.
    pub const fn lineage_byte_bound(&self) -> usize {
        self.payload_buffers[0]
    }
    /// Generated DTO buffer construction peak. Pinned Vec<u8>/String growth is
    /// bounded by max(2*total_input, 8); an old buffer of at most total_input
    /// coexists during a reallocation. The existing owning allocator formula
    /// prices each of those allocations. This is a per-field growth derivation,
    /// not a multiplier applied to opaque IR or native pages.
    pub fn transport_work_bound_v1(&self) -> Result<u64, BudgetUnavailable> {
        fn buffers(counts: [usize; 3]) -> Result<u64, BudgetUnavailable> {
            let mut result = 0_u64;
            for count in counts {
                if count == 0 {
                    continue;
                }
                let grown = count.checked_mul(2).ok_or(BudgetUnavailable)?.max(8);
                let old = count.max(8);
                for capacity in [grown, old] {
                    result = result
                        .checked_add(
                            riffdb_types::owned_allocation_capacity_bound_v1(capacity, 1)
                                .map_err(|_| BudgetUnavailable)?,
                        )
                        .ok_or(BudgetUnavailable)?;
                }
            }
            Ok(result)
        }
        // The shared canonical kernel destroys its legacy envelope before
        // constructing M. The retained input/scratch are admitted separately.
        Ok(buffers(self.payload_buffers)?.max(buffers(self.envelope_buffers)?))
    }
    /// Transient Prost error ownership for these closed nonnested DTOs. The
    /// pinned dependency's DecodeError owns Box<Inner> (description enum plus
    /// static-name path Vec). Enumerating every owning enum-field header gives
    /// a conservative payload bound; one generated outer field adds at most one
    /// path pair, whose Vec minimum allocation is four entries. No Any/custom
    /// owned-description decoder is reachable for these string/bytes/scalars.
    /// This work is sequential with final owned-census workspace, not with the
    /// partial generated buffers: caller prices max(census,error) in addition
    /// to the buffer peak. Dependency-source attestation is mandatory for the
    /// encompassing Source constructor before physical qualification.
    pub fn transient_decode_error_work_bound_v1(&self) -> Result<u64, BudgetUnavailable> {
        use std::borrow::Cow;
        use std::mem::{align_of, size_of};
        // DecodeErrorKind's largest union member is two String headers; include
        // its Cow header too, two scalar fields, and two tag/alignment words.
        // This overcounts the enum's mutually exclusive variants deliberately.
        let description = size_of::<(String, String, Cow<'static, str>, u64, u64, usize, usize)>();
        let inner = description
            .checked_add(size_of::<Vec<(&'static str, &'static str)>>())
            .and_then(|n| n.checked_add(2 * align_of::<usize>()))
            .ok_or(BudgetUnavailable)?;
        let boxed = riffdb_types::owned_allocation_capacity_bound_v1(inner, align_of::<usize>())
            .map_err(|_| BudgetUnavailable)?;
        let path = 4usize
            .checked_mul(size_of::<(&'static str, &'static str)>())
            .ok_or(BudgetUnavailable)?;
        let path = riffdb_types::owned_allocation_capacity_bound_v1(
            path,
            align_of::<(&'static str, &'static str)>(),
        )
        .map_err(|_| BudgetUnavailable)?;
        boxed
            .checked_add(path)
            .and_then(|n| n.checked_add(size_of::<prost::DecodeError>() as u64))
            .ok_or(BudgetUnavailable)
    }
    /// Calls the one existing canonical envelope/Prost kernel. Scratch and all
    /// generated/message/mapper ownership MUST already be admitted by caller.
    pub fn decode_with_caller_scratch(
        self,
        scratch: &mut Vec<u8>,
    ) -> Result<wire::StoredContractBundleV1, super::CallerScratchDecodeErrorV1> {
        let message = super::decode_readable_message_with_caller_scratch_v1::<
            wire::StoredContractBundleV1,
        >(self.input.original, scratch)?;
        sealed::ProviderSourceMessage::inspect_transport_shape(&message);
        Ok(message)
    }
}
