//! Closed ADR-0318 Audit-role registration and predecessor preservation proofs.
#![forbid(unsafe_code)]
// req: REP-003, STO-012, REC-001
use riffdb_proto::durable::{readable_record_schema, writable_record_schema};

#[test]
fn native_catalog_transition_and_inheritance_roles_are_registered() {
    let missing = [
        readable_record_schema("riffdb.storage.v1.StoredAuthoritativeCatalogTransitionV1")
            .is_none(),
        readable_record_schema("riffdb.storage.v1.StoredSourceInheritanceAdministrationV1")
            .is_none(),
    ];
    assert_eq!(
        missing,
        [false, false],
        "explicit ADR0318 readable Audit role"
    );
    for (record, family, complete_cap) in [
        (
            "riffdb.storage.v1.StoredAuthoritativeCatalogTransitionV1",
            85,
            2048,
        ),
        (
            "riffdb.storage.v1.StoredSourceInheritanceAdministrationV1",
            86,
            16384,
        ),
    ] {
        let readable =
            readable_record_schema(record).expect("explicit ADR0318 readable Audit role");
        let writable =
            writable_record_schema(record).expect("explicit ADR0318 writable Audit role");
        assert_eq!(readable.compact_tag(), family);
        assert_eq!(readable.schema_revision(), 1);
        assert!(readable.max_envelope_bytes() <= complete_cap);
        assert_eq!(writable.schema_hash(), readable.schema_hash());
        assert_eq!(writable.schema_revision(), 1);
        if family == 85 {
            assert_eq!(readable.max_envelope_bytes(), 2048);
        } else {
            assert_eq!(readable.max_payload_bytes(), 15 * 1024);
        }
    }
}

#[test]
fn native_catalog_transition_preserves_every_frozen_source_and_fencing_role() {
    for (record, family, revision, hash, bounds) in [
        (
            "riffdb.storage.v1.StoredAuthoritativeStateCatalogV3",
            69,
            3,
            include_bytes!("../fixtures/durable-authoritative-state-catalog-v3-schema-hash.bin"),
            include_bytes!("../fixtures/durable-authoritative-state-catalog-v3-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredProjectionSourceRowV1",
            79,
            1,
            include_bytes!("../fixtures/durable-projection-source-row-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-projection-source-row-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredProjectionSourceObjectV1",
            80,
            1,
            include_bytes!("../fixtures/durable-projection-source-object-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-projection-source-object-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredProjectionSourceRootV1",
            81,
            1,
            include_bytes!("../fixtures/durable-projection-source-root-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-projection-source-root-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredProjectionSourceMaintenanceV1",
            82,
            1,
            include_bytes!("../fixtures/durable-projection-source-maintenance-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-projection-source-maintenance-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredGenerationSourceBaseV1",
            83,
            1,
            include_bytes!("../fixtures/durable-generation-source-base-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-generation-source-base-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredProjectionCheckpointOrdinalAllocatorV1",
            84,
            1,
            include_bytes!(
                "../fixtures/durable-projection-checkpoint-ordinal-allocator-v1-schema-hash.bin"
            ),
            include_bytes!(
                "../fixtures/durable-projection-checkpoint-ordinal-allocator-v1-record-bound.bin"
            ),
        ),
        (
            "riffdb.storage.v1.ReplicationPrimaryAdmissionV1",
            76,
            1,
            include_bytes!("../fixtures/durable-replication-primary-admission-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-replication-primary-admission-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredPrimaryFenceAdministrationV1",
            77,
            1,
            include_bytes!("../fixtures/durable-primary-fence-administration-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-primary-fence-administration-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredPromotionAdministrationV1",
            78,
            1,
            include_bytes!("../fixtures/durable-promotion-administration-v1-schema-hash.bin"),
            include_bytes!("../fixtures/durable-promotion-administration-v1-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.ReplicationPrimaryAdmissionV2",
            76,
            2,
            include_bytes!("../fixtures/durable-replication-primary-admission-v2-schema-hash.bin"),
            include_bytes!("../fixtures/durable-replication-primary-admission-v2-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredPrimaryFenceAdministrationV2",
            77,
            2,
            include_bytes!("../fixtures/durable-primary-fence-administration-v2-schema-hash.bin"),
            include_bytes!("../fixtures/durable-primary-fence-administration-v2-record-bound.bin"),
        ),
        (
            "riffdb.storage.v1.StoredPromotionAdministrationV2",
            78,
            2,
            include_bytes!("../fixtures/durable-promotion-administration-v2-schema-hash.bin"),
            include_bytes!("../fixtures/durable-promotion-administration-v2-record-bound.bin"),
        ),
    ] {
        let readable = readable_record_schema(record).unwrap();
        assert_eq!(readable.compact_tag(), family);
        assert_eq!(readable.schema_revision(), revision);
        assert_eq!(readable.schema_hash().as_bytes(), hash);
        assert_eq!(
            readable.max_payload_bytes(),
            usize::try_from(u32::from_be_bytes([
                bounds[0], bounds[1], bounds[2], bounds[3]
            ]))
            .unwrap()
        );
        assert_eq!(
            readable.max_envelope_bytes(),
            usize::try_from(u32::from_be_bytes([
                bounds[4], bounds[5], bounds[6], bounds[7]
            ]))
            .unwrap()
        );
        assert_eq!(
            writable_record_schema(record).unwrap().schema_hash(),
            readable.schema_hash()
        );
    }
}
