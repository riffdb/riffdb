//! Borrowed admission for ordinary native command-segment reconstruction.
//! Counts actual closed wire allocations and canonical trees before decoding.
// req: TXN-042, STO-032

use std::mem::size_of;

use riffdb_proto::envelope::{
    EnvelopeError, PayloadValidationError, inspect_locator_segment_decode_shape_v1,
    inspect_provider_source_decode_v1,
};
use riffdb_proto::storage::v1 as wire;
use riffdb_types::inspect_canonical_decode_capacity_v1;

use crate::{
    AuthoritativeNamespaceV1 as N, AuthoritativeStateCatalogV1, CommandDerivedIndexManifestEntryV1,
    DurableCodecError, DurableCodecErrorKind, StoredCommandCapsuleV2,
};

fn corrupt() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::CorruptData)
}
fn capacity() -> DurableCodecError {
    DurableCodecError::new(DurableCodecErrorKind::LimitExceeded)
}
fn add(total: &mut u64, count: u64, each: usize) -> Result<(), DurableCodecError> {
    let bytes = count
        .checked_mul(u64::try_from(each).map_err(|_| capacity())?)
        .ok_or_else(capacity)?;
    *total = total.checked_add(bytes).ok_or_else(capacity)?;
    Ok(())
}

/// A construction allowance only; the ordinary fully checked decoder remains
/// mandatory. `None` is an independently readable historical nonsegment role.
#[doc(hidden)]
pub fn locator_segment_decode_work_bound_v1(
    encoded: &[u8],
) -> Result<Option<u64>, DurableCodecError> {
    let mut prefix_error = None;
    let mut prefix_seen = false;
    let shape = inspect_locator_segment_decode_shape_v1(encoded, |prefix| {
        prefix_seen = true;
        prefix_work_bound(prefix).map_err(|error| {
            prefix_error = Some(error);
            EnvelopeError::InvalidPayload(PayloadValidationError::Malformed)
        })
    });
    if let Some(error) = prefix_error {
        return Err(error);
    }
    let Some(shape) = shape.map_err(|_| corrupt())? else {
        return Ok(None);
    };
    // Each encountered generated DTO contributes its actual inline size.
    // Optional/oneof children are conservatively counted again; repeated elements
    // contribute their real element footprint. Eightfold capacity preserves the
    // existing fourfold growth plus a second mapper/verification graph.
    let mut bytes = 512u64 * 1024;
    // General provider receipt admission includes its ownership inspector's
    // transient workspace. Prefix reconstruction is sequential; retained
    // evidence owns mutations, not an inspector per command. Reserve one
    // conservative inspection window for the whole segment, independently of
    // the variable per-prefix graphs/copies summed below.
    if prefix_seen {
        bytes = bytes
            .checked_add(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .ok_or_else(capacity)?;
    }
    add(
        &mut bytes,
        u64::try_from(encoded.len()).map_err(|_| capacity())?,
        16,
    )?;
    bytes = bytes
        .checked_add(
            shape
                .wire_inline_bytes
                .checked_mul(8)
                .ok_or_else(capacity)?,
        )
        .ok_or_else(capacity)?;
    add(&mut bytes, shape.messages, 8 * 64)?;
    // The capsule charge below already includes outcome, provenance, commit and
    // both audit records inline. Their repeated semantic buffers require their
    // own element sizes, not another whole provenance per nested wire message.
    add(
        &mut bytes,
        shape.semantic_read_dependencies,
        8 * (size_of::<crate::StoredReadDependencyV1>() + size_of::<(Vec<u8>, usize)>()),
    )?;
    add(
        &mut bytes,
        shape.semantic_entity_references,
        8 * (size_of::<crate::CommittedEntityReferenceV2>() + size_of::<crate::AffectedEntityV1>()),
    )?;
    add(
        &mut bytes,
        shape.semantic_event_references,
        8 * size_of::<crate::EventReferenceV2>(),
    )?;
    add(
        &mut bytes,
        shape.semantic_event_ids,
        8 * size_of::<riffdb_types::EventId>(),
    )?;
    add(
        &mut bytes,
        shape.semantic_events,
        8 * (size_of::<crate::StoredDurableEventV1>() + 64),
    )?;
    add(
        &mut bytes,
        shape.semantic_index_transitions,
        8 * size_of::<crate::IndexEpochAdvanceV1>(),
    )?;
    // Transition provenance owns affected entities, and validation retains a
    // temporary live-reference list. A full owned reference bounds that smaller
    // borrowed tuple without introducing another semantic representation.
    add(
        &mut bytes,
        shape.semantic_entity_transitions,
        8 * (size_of::<crate::CommittedEntityTransitionV1>()
            + size_of::<crate::AffectedEntityV1>()
            + size_of::<crate::CommittedEntityReferenceV2>()),
    )?;
    add(
        &mut bytes,
        shape.semantic_audit_targets,
        8 * 2 * size_of::<riffdb_types::ServiceAuditTargetV1>(),
    )?;
    add(
        &mut bytes,
        shape.semantic_conflict_hashes,
        8 * 3 * size_of::<riffdb_types::ConflictKeyHash>(),
    )?;
    add(&mut bytes, shape.buffers, 256)?;
    add(&mut bytes, shape.packed_items, 64)?;
    add(
        &mut bytes,
        shape.capsules,
        8 * (size_of::<StoredCommandCapsuleV2>() + 64),
    )?;
    add(
        &mut bytes,
        shape.manifest_entries,
        8 * (size_of::<CommandDerivedIndexManifestEntryV1>() + 64),
    )?;
    bytes = bytes
        .checked_add(
            shape
                .canonical_heap_bytes
                .checked_mul(8)
                .ok_or_else(capacity)?,
        )
        .and_then(|n| n.checked_add(shape.prefix_work_bytes))
        .ok_or_else(capacity)?;
    Ok(Some(bytes))
}

fn prefix_work_bound(bytes: &[u8]) -> Result<u64, DurableCodecError> {
    if !(40..=crate::MAX_STAGED_WRITE_BYTES).contains(&bytes.len()) {
        return Err(corrupt());
    }
    let mut reader = crate::changelog_v3::ReceiptReader { remaining: bytes };
    reader.take(36).map_err(|_| corrupt())?;
    let count = usize::try_from(reader.u32().map_err(|_| corrupt())?).map_err(|_| capacity())?;
    if count > crate::MAX_CHANGELOG_FRAME_ENTRIES || count > reader.remaining.len() / 45 {
        return Err(corrupt());
    }
    // Keep every prefix's vectors, fold maps, copies and verification buffers.
    // Its general provider receipt adapter also includes fixed scratch for
    // allocation inspection; that transient window is shared by this segment,
    // not retained once for every prefix's immutable mutation graph.
    let mut work = crate::provider_receipt_work_bound_v1(count, bytes.len())
        .map_err(|_| capacity())?
        .checked_sub(256 * 1024)
        .and_then(|work| work.checked_sub(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1))
        .ok_or_else(capacity)?;
    for _ in 0..count {
        let namespace = AuthoritativeStateCatalogV1
            .by_tag(reader.u16().map_err(|_| corrupt())?)
            .ok_or_else(corrupt)?;
        let operation = reader.take(1).map_err(|_| corrupt())?[0];
        let expected_present = reader.take(1).map_err(|_| corrupt())?[0];
        let key_len =
            usize::try_from(reader.u32().map_err(|_| corrupt())?).map_err(|_| capacity())?;
        let value_len = reader.u32().map_err(|_| corrupt())?;
        let hash: [u8; 32] = reader.array().map_err(|_| corrupt())?;
        if !((expected_present == 0 && hash == [0; 32]) || expected_present == 1) {
            return Err(corrupt());
        }
        let key = reader.take(key_len).map_err(|_| corrupt())?;
        let value = match (operation, value_len, expected_present) {
            (0, len, _) if len != u32::MAX => Some(
                reader
                    .take(usize::try_from(len).map_err(|_| capacity())?)
                    .map_err(|_| corrupt())?,
            ),
            (1, u32::MAX, 1) => None,
            _ => return Err(corrupt()),
        };
        if key.is_empty() {
            return Err(corrupt());
        }
        if let Some(value) = value {
            let heap = match namespace {
                N::Entities => {
                    let inspected =
                        inspect_provider_source_decode_v1::<wire::StoredEntityRecordV1>(value)
                            .map_err(|_| corrupt())?;
                    inspect_canonical_decode_capacity_v1(inspected.canonical_payload())
                        .map_err(|_| corrupt())?
                        .owned_heap_bytes()
                }
                N::SecondaryIndexes => {
                    let inspected =
                        inspect_provider_source_decode_v1::<wire::StoredIndexEntryV2>(value)
                            .map_err(|_| corrupt())?;
                    inspect_canonical_decode_capacity_v1(inspected.canonical_payload())
                        .map_err(|_| corrupt())?
                        .owned_heap_bytes()
                }
                _ => 0,
            };
            work = work
                .checked_add(heap.checked_mul(8).ok_or_else(capacity)?)
                .ok_or_else(capacity)?;
        }
    }
    if !reader.remaining.is_empty() {
        return Err(corrupt());
    }
    Ok(work)
}
