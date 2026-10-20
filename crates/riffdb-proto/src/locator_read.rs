//! Borrowed native command-directory construction sizing.
//!
//! This inspects the same readable schemas without constructing any generated
//! message. Ordinary checksum, canonicality and semantic decoding remain required.
// req: TXN-042, STO-032

use super::{EnvelopeError, MAX_STORED_ENVELOPE_BYTES, PayloadValidationError};
use crate::durable_wire::DurablePreflightError;

/// Structural counts only; callers still admit all semantic ownership.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct LocatorSegmentDecodeShapeV1 {
    /// Number of generated nested message instances, including the root.
    pub messages: u64,
    /// Sum of actual generated message inline sizes; includes containing children.
    pub wire_inline_bytes: u64,
    /// Durable event elements mapped into semantic vectors.
    pub semantic_events: u64,
    /// Read dependency elements.
    pub semantic_read_dependencies: u64,
    /// Committed entity-reference elements.
    pub semantic_entity_references: u64,
    /// Historical embedded entity mutations, including their complete post-images.
    pub semantic_entity_mutations: u64,
    /// Event-reference elements.
    pub semantic_event_references: u64,
    /// Semantic event identities, including inline occurrences.
    pub semantic_event_ids: u64,
    /// Index-generation transition elements.
    pub semantic_index_transitions: u64,
    /// Entity-transition elements.
    pub semantic_entity_transitions: u64,
    /// Audit target elements before the two audit views.
    pub semantic_audit_targets: u64,
    /// Conflict hashes expanded into outcome, commit and provenance.
    pub semantic_conflict_hashes: u64,
    /// Number of generated opaque string/byte buffer instances.
    pub buffers: u64,
    /// Number of packed scalar elements.
    pub packed_items: u64,
    /// Number of original semantic command capsules.
    pub capsules: u64,
    /// Number of exact manifest entries.
    pub manifest_entries: u64,
    /// Structurally inspected semantic canonical heap, before mapper copies.
    pub canonical_heap_bytes: u64,
    /// Caller-inspected V7 prefix reconstruction and validation work.
    pub prefix_work_bytes: u64,
}

/// Inspect one exact readable envelope before generated or semantic allocation.
/// The prefix callback supplies separately bounded storage-owned reconstruction.
#[doc(hidden)]
pub fn inspect_locator_segment_decode_shape_v1(
    encoded: &[u8],
    mut prefix_work: impl FnMut(&[u8]) -> Result<u64, EnvelopeError>,
) -> Result<Option<LocatorSegmentDecodeShapeV1>, EnvelopeError> {
    inspect_command_decode_shape(encoded, &mut prefix_work, false)
}

/// Borrowed complete allocation census for command/event source reconstruction.
/// Full canonical and semantic decoding remains mandatory after admission.
#[doc(hidden)]
pub fn inspect_command_source_decode_shape_v1(
    encoded: &[u8],
    mut prefix_work: impl FnMut(&[u8]) -> Result<u64, EnvelopeError>,
) -> Result<Option<LocatorSegmentDecodeShapeV1>, EnvelopeError> {
    inspect_command_decode_shape(encoded, &mut prefix_work, true)
}

fn inspect_command_decode_shape(
    encoded: &[u8],
    prefix_work: &mut impl FnMut(&[u8]) -> Result<u64, EnvelopeError>,
    include_historical: bool,
) -> Result<Option<LocatorSegmentDecodeShapeV1>, EnvelopeError> {
    if encoded.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(EnvelopeError::EnvelopeTooLarge);
    }
    let registry = crate::durable::readable_record_registry();
    let legacy_schema;
    let (record_type, payload) = if encoded.starts_with(&super::COMPACT_RECORD_MAGIC_V2) {
        let record_type = registry
            .peek_compact_record_type(encoded)
            .ok_or(EnvelopeError::UnknownCompactIdentity)?;
        let schema = crate::durable::readable_record_schema(record_type)
            .ok_or(EnvelopeError::UnknownRecordType)?;
        (
            record_type,
            registry.borrowed_current_compact_payload(encoded, schema)?,
        )
    } else {
        let (schema, frame) = registry.borrowed_v1_payload(encoded)?;
        legacy_schema = schema;
        (legacy_schema.record_type(), frame.payload)
    };
    let mut prefix =
        |bytes: &[u8]| prefix_work(bytes).map_err(|_| DurablePreflightError::Malformed);
    let result = (if include_historical {
        crate::durable_wire::command_source_allocation_shape(record_type, payload, &mut prefix)
    } else {
        crate::durable_wire::locator_allocation_shape(record_type, payload, &mut prefix)
    })
    .map_err(|error| {
        EnvelopeError::InvalidPayload(match error {
            DurablePreflightError::Malformed => PayloadValidationError::Malformed,
            DurablePreflightError::NonCanonical => PayloadValidationError::NonCanonical,
            DurablePreflightError::LimitExceeded => PayloadValidationError::LimitExceeded,
        })
    })?;
    Ok(result.map(|shape| LocatorSegmentDecodeShapeV1 {
        messages: shape.messages,
        wire_inline_bytes: shape.wire_inline_bytes,
        semantic_events: shape.semantic_events,
        semantic_read_dependencies: shape.semantic_read_dependencies,
        semantic_entity_references: shape.semantic_entity_references,
        semantic_entity_mutations: shape.semantic_entity_mutations,
        semantic_event_references: shape.semantic_event_references,
        semantic_event_ids: shape.semantic_event_ids,
        semantic_index_transitions: shape.semantic_index_transitions,
        semantic_entity_transitions: shape.semantic_entity_transitions,
        semantic_audit_targets: shape.semantic_audit_targets,
        semantic_conflict_hashes: shape.semantic_conflict_hashes,
        buffers: shape.buffers,
        packed_items: shape.packed_items,
        capsules: shape.capsules,
        manifest_entries: shape.manifest_entries,
        canonical_heap_bytes: shape.canonical_heap_bytes,
        prefix_work_bytes: shape.prefix_work_bytes,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: TXN-042, STO-032
    #[test]
    fn locator_scratch_is_destroyed_before_its_admitted_owner_can_release() {
        super::super::CANONICAL_SCRATCH.with(|scratch| {
            *scratch.borrow_mut() = vec![1; 128 * 1024];
            let borrowed = scratch.borrow();
            assert!(
                !super::super::release_locator_decode_scratch_v1(),
                "live physical scratch cannot be declared released"
            );
            assert_eq!(borrowed.len(), 128 * 1024);
            drop(borrowed);
        });
        assert!(super::super::release_locator_decode_scratch_v1());
        super::super::CANONICAL_SCRATCH.with(|scratch| assert_eq!(scratch.borrow().capacity(), 0));
    }

    // req: TXN-042, STO-032
    #[test]
    fn borrowed_locator_inspection_preserves_frozen_v7_prefix_and_schema_checks() {
        let fixture = include_str!("../fixtures/durable-command-prefix-v7-wire-vectors.txt");
        let line = fixture
            .lines()
            .find(|line| line.starts_with("riffdb.storage.v1.StoredCommandSegmentV6\t"))
            .unwrap();
        let (_, hex) = line.split_once("\t").unwrap();
        let bytes: Vec<u8> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |c: u8| match c {
                    b'0'..=b'9' => c - b'0',
                    b'a'..=b'f' => c - b'a' + 10,
                    _ => panic!("frozen hex"),
                };
                digit(pair[0]) * 16 + digit(pair[1])
            })
            .collect();
        let decoded = crate::durable::decode_readable_message::<
            crate::storage::v1::StoredCommandSegmentV6,
        >(&bytes)
        .unwrap();
        let body = decoded.body.as_ref().unwrap();
        let heap = |canonical: &[u8]| {
            if canonical.is_empty() {
                0
            } else {
                riffdb_types::inspect_canonical_decode_capacity_v1(canonical)
                    .unwrap()
                    .owned_heap_bytes()
            }
        };
        let mut expected_heap = 0;
        for command in &body.commands {
            expected_heap += heap(&command.canonical_service_values);
            if let Some(outcome) = command
                .base
                .as_ref()
                .and_then(|base| base.declared_outcome.as_ref())
            {
                expected_heap += heap(&outcome.canonical_value);
            }
            for event in &command.events {
                use crate::storage::v1::stored_durable_event_variant_v1::Value;
                expected_heap += match event.value.as_ref().unwrap() {
                    Value::Unanchored(event) => heap(&event.canonical_payload),
                    Value::Anchored(event) => heap(&event.canonical_payload),
                };
            }
        }
        let mut prefixes = 0;
        let shape = inspect_locator_segment_decode_shape_v1(&bytes, |prefix| {
            assert!(prefix.len() >= 40);
            prefixes += 1;
            Ok(prefix.len() as u64)
        })
        .unwrap()
        .unwrap();
        assert_eq!(prefixes, 1);
        assert_eq!(
            prefixes,
            body.commands
                .iter()
                .filter(|command| !command.command_prefix_evidence.is_empty())
                .count()
        );
        assert_eq!(
            shape.capsules,
            body.commands
                .iter()
                .filter(|command| command.base.is_some())
                .count() as u64
        );
        // The frozen codec vector has synthetic independent rows and a
        // partial manifest. Its actual wire graph, rather than application
        // storage requirements, owns these borrowed structural counts.
        assert_eq!(
            shape.manifest_entries,
            body.manifest.as_ref().unwrap().entries.len() as u64
        );
        assert_eq!(shape.canonical_heap_bytes, expected_heap);
        let mut corrupt = bytes;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 1;
        assert!(inspect_locator_segment_decode_shape_v1(&corrupt, |_| Ok(0)).is_err());
    }
}

#[cfg(test)]
#[path = "command_source_census_tests.rs"]
mod source_census_tests;
