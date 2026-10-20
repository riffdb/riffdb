//! Closed catalog-bound fencing registry compatibility and bound proofs.
#![forbid(unsafe_code)]
// req: REP-005, STO-012
use riffdb_proto::durable::{readable_record_schema, writable_record_schema};

#[test]
fn catalog_bound_fencing_successor_roles_have_closed_versions_and_bounds() {
    for (record, family, bound) in [
        ("riffdb.storage.v1.ReplicationPrimaryAdmissionV2", 76, 1024),
        (
            "riffdb.storage.v1.StoredPrimaryFenceAdministrationV2",
            77,
            1000,
        ),
        (
            "riffdb.storage.v1.StoredPromotionAdministrationV2",
            78,
            8192,
        ),
    ] {
        let readable =
            readable_record_schema(record).expect("explicit catalog-bound readable role");
        let writable =
            writable_record_schema(record).expect("explicit catalog-bound writable role");
        assert_eq!(readable.compact_tag(), family);
        assert_eq!(readable.schema_revision(), 2);
        assert_eq!(readable.max_payload_bytes(), bound);
        assert_eq!(writable.schema_hash(), readable.schema_hash());
        assert_eq!(writable.schema_revision(), 2);
    }
}

#[test]
fn original_fencing_roles_keep_their_exact_versions_and_bounds() {
    for (record, family, bound) in [
        ("riffdb.storage.v1.ReplicationPrimaryAdmissionV1", 76, 1024),
        (
            "riffdb.storage.v1.StoredPrimaryFenceAdministrationV1",
            77,
            1000,
        ),
        (
            "riffdb.storage.v1.StoredPromotionAdministrationV1",
            78,
            8192,
        ),
    ] {
        let readable = readable_record_schema(record).unwrap();
        assert_eq!(readable.compact_tag(), family);
        assert_eq!(readable.schema_revision(), 1);
        assert_eq!(readable.max_payload_bytes(), bound);
        assert_eq!(
            writable_record_schema(record).unwrap().schema_hash(),
            readable.schema_hash()
        );
    }
}
