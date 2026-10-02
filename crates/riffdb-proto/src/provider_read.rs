//! Borrowed provider source inspection and explicitly admitted decode scratch.
// req: OQ-040
use super::{EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError, StoredEnvelope};
use crate::durable::ReadableRecordMessage;
use crate::storage::v1 as wire;
use prost::Message;
use riffdb_types::exact_provider_budget_v1::{
    BudgetUnavailable, ProviderAllocation, ProviderBudget, Reservation,
};
use std::marker::PhantomData;
use std::sync::Arc;

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
    ) -> Result<Arc<ProviderAllocation<M>>, ProviderSourceDecodeErrorV1> {
        if self.original.len() > scratch.maximum {
            return Err(EnvelopeError::EnvelopeTooLarge.into());
        }
        // These schemas contain at most five byte/string buffers and no repeated
        // messages. Legacy envelope verification adds three buffers, all bounded
        // by the original encoding. Eight copies plus per-buffer/root overhead
        // cover both verification and generated decode before allocation.
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
