//! Additive catalog-bound fencing registry roles; historical entries stay frozen.
use super::*;

pub(super) const REPLICATION_PRIMARY_ADMISSION_V2_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.ReplicationPrimaryAdmissionV2",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-replication-primary-admission-v2-schema-hash.bin"
        )),
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-replication-primary-admission-v2-record-bound.bin"
            );
            [bounds[0], bounds[1], bounds[2], bounds[3]]
        }) as usize,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-replication-primary-admission-v2-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<118>,
        validate_payload::<118, v1::ReplicationPrimaryAdmissionV2>,
    )
    .with_compact_identity(76, 2);
readable_message!(
    v1::ReplicationPrimaryAdmissionV2,
    REPLICATION_PRIMARY_ADMISSION_V2_RECORD_SCHEMA
);
writable_message!(v1::ReplicationPrimaryAdmissionV2);

pub(super) const PRIMARY_FENCE_ADMINISTRATION_V2_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredPrimaryFenceAdministrationV2",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-primary-fence-administration-v2-schema-hash.bin"
        )),
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-primary-fence-administration-v2-record-bound.bin"
            );
            [bounds[0], bounds[1], bounds[2], bounds[3]]
        }) as usize,
        u32::from_be_bytes({
            let bounds = *include_bytes!(
                "../fixtures/durable-primary-fence-administration-v2-record-bound.bin"
            );
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<119>,
        validate_payload::<119, v1::StoredPrimaryFenceAdministrationV2>,
    )
    .with_compact_identity(77, 2);
readable_message!(
    v1::StoredPrimaryFenceAdministrationV2,
    PRIMARY_FENCE_ADMINISTRATION_V2_RECORD_SCHEMA
);
writable_message!(v1::StoredPrimaryFenceAdministrationV2);

pub(super) const PROMOTION_ADMINISTRATION_V2_RECORD_SCHEMA: RecordSchema<'static> =
    RecordSchema::new_current(
        "riffdb.storage.v1.StoredPromotionAdministrationV2",
        SchemaHash::from_bytes(*include_bytes!(
            "../fixtures/durable-promotion-administration-v2-schema-hash.bin"
        )),
        u32::from_be_bytes({
            let bounds =
                *include_bytes!("../fixtures/durable-promotion-administration-v2-record-bound.bin");
            [bounds[0], bounds[1], bounds[2], bounds[3]]
        }) as usize,
        u32::from_be_bytes({
            let bounds =
                *include_bytes!("../fixtures/durable-promotion-administration-v2-record-bound.bin");
            [bounds[4], bounds[5], bounds[6], bounds[7]]
        }) as usize,
        preflight_payload::<120>,
        validate_payload::<120, v1::StoredPromotionAdministrationV2>,
    )
    .with_compact_identity(78, 2);
readable_message!(
    v1::StoredPromotionAdministrationV2,
    PROMOTION_ADMINISTRATION_V2_RECORD_SCHEMA
);
writable_message!(v1::StoredPromotionAdministrationV2);
