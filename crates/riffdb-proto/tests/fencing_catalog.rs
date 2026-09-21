#![forbid(unsafe_code)]
//! The successor catalog has its own durable binding; V1 is never relabeled.
// req: REP-005, STO-012

#[test]
fn successor_catalog_has_a_distinct_revision_and_preserves_the_old_binding() {
    use riffdb_proto::durable::writable_record_schema;
    let old =
        writable_record_schema("riffdb.storage.v1.StoredAuthoritativeStateCatalogV1").unwrap();
    let new = writable_record_schema("riffdb.storage.v1.StoredAuthoritativeStateCatalogV2")
        .expect("accepted fencing catalog requires its own durable binding");
    assert_eq!((old.compact_tag(), old.schema_revision()), (69, 1));
    assert_eq!((new.compact_tag(), new.schema_revision()), (69, 2));
    assert_eq!(new.max_payload_bytes(), 34);
    assert_ne!(new.schema_hash(), old.schema_hash());
}

#[test]
fn primary_fence_records_have_distinct_bounded_roles() {
    use riffdb_proto::durable::writable_record_schema;
    for (name, tag, maximum) in [
        ("riffdb.storage.v1.ReplicationPrimaryAdmissionV1", 76, 1024),
        (
            "riffdb.storage.v1.StoredPrimaryFenceAdministrationV1",
            77,
            1000,
        ),
    ] {
        let schema =
            writable_record_schema(name).expect("accepted fence record requires a binding");
        assert_eq!((schema.compact_tag(), schema.schema_revision()), (tag, 1));
        assert_eq!(schema.max_payload_bytes(), maximum);
    }
    let lifecycle =
        writable_record_schema("riffdb.storage.v1.StoredReplicationAdministrationV1").unwrap();
    assert_eq!(
        (lifecycle.compact_tag(), lifecycle.schema_revision()),
        (75, 1)
    );
    let export =
        writable_record_schema("riffdb.storage.v1.StoredApplicationExportPageCommitmentV1")
            .unwrap();
    assert_eq!((export.compact_tag(), export.schema_revision()), (74, 1));
}
