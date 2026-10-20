//! Additive catalog-origin/inheritance roles; predecessor entries remain frozen.
use super::*;

pub(super) const AUTHORITATIVE_CATALOG_TRANSITION_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredAuthoritativeCatalogTransitionV1",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-authoritative-catalog-transition-v1-schema-hash.bin"
        )),
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-authoritative-catalog-transition-v1-record-bound.bin"
            );
            [bounds[0], bounds[1], bounds[2], bounds[3]]
        }) as usize,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-authoritative-catalog-transition-v1-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<121>,
        validate_payload::<121, v1::StoredAuthoritativeCatalogTransitionV1>,
    )
    .with_compact_identity(85, 1);
readable_message!(
    v1::StoredAuthoritativeCatalogTransitionV1,
    AUTHORITATIVE_CATALOG_TRANSITION_V1_RECORD_SCHEMA
);
writable_message!(v1::StoredAuthoritativeCatalogTransitionV1);

pub(super) const SOURCE_INHERITANCE_ADMINISTRATION_V1_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredSourceInheritanceAdministrationV1",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-source-inheritance-administration-v1-schema-hash.bin"
        )),
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-source-inheritance-administration-v1-record-bound.bin"
            );
            [bounds[0], bounds[1], bounds[2], bounds[3]]
        }) as usize,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-source-inheritance-administration-v1-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<122>,
        validate_payload::<122, v1::StoredSourceInheritanceAdministrationV1>,
    )
    .with_compact_identity(86, 1);
readable_message!(
    v1::StoredSourceInheritanceAdministrationV1,
    SOURCE_INHERITANCE_ADMINISTRATION_V1_RECORD_SCHEMA
);
writable_message!(v1::StoredSourceInheritanceAdministrationV1);
