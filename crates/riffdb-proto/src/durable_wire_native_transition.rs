//! Closed owning-field shapes for ADR-0318 family85/86 revision1.
use super::*;

shape!(CATALOG_V2_TO_V3 [
    message(1, &CHANGELOG_HISTORY_STATE_V3),
    nonempty_bytes(2, 256),
    nonempty_bytes(3, 1024),
    fixed_bytes(4, 32),
]);
shape!(AUTHORITATIVE_CATALOG_TRANSITION_V1 [
    fixed_bytes(1, 16),
    message(3, &projection_source::SOURCE_PUBLICATION_BINDING),
    fixed_bytes(4, 32),
    fixed_bytes(5, 32),
    message(6, &projection_source::PROJECTION_CHECKPOINT_ORDINAL_ALLOCATOR_V1),
    message(7, &UNIT),
    message(8, &CATALOG_V2_TO_V3),
]);
shape!(INHERITANCE_LINEAGE [fixed_bytes(1, 16), fixed_bytes(4, 32)]);
shape!(INHERITANCE_ORIGIN[fixed_bytes(2, 32)]);
shape!(INHERITANCE_ALLOCATOR_SCALAR [message(1, &UNIT), nonempty_bytes(2, 160)]);
shape!(INHERITANCE_INCARNATION_SCALAR [message(1, &UNIT), nonempty_bytes(2, 128)]);
shape!(INHERITANCE_REGISTRY_SCALAR [message(1, &UNIT), nonempty_bytes(2, 256)]);
shape!(INHERITANCE_WATERMARK_SCALAR [message(1, &UNIT), nonempty_bytes(2, 256)]);
shape!(SOURCE_INHERITANCE_ADMINISTRATION_V1 [
    message(3, &CHANGELOG_HISTORY_STATE_V3),
    message(4, &INHERITANCE_LINEAGE),
    message(5, &projection_source::SOURCE_PUBLICATION_BINDING),
    message(6, &INHERITANCE_ORIGIN),
    fixed_bytes(7, 32),
    repeated_fixed_bytes(8, 128, 96),
    message(9, &INHERITANCE_ALLOCATOR_SCALAR),
    message(10, &INHERITANCE_INCARNATION_SCALAR),
    message(11, &INHERITANCE_REGISTRY_SCALAR),
    message(12, &INHERITANCE_WATERMARK_SCALAR),
]);

pub(super) const CATALOG_TRANSITION: &Shape = &AUTHORITATIVE_CATALOG_TRANSITION_V1;
pub(super) const SOURCE_INHERITANCE: &Shape = &SOURCE_INHERITANCE_ADMINISTRATION_V1;
