//! Closed durable identity for the independent physical transaction allocator.

use riffdb_proto::durable::writable_record_schema;

#[test]
// req: REP-003, STO-012
fn physical_changelog_allocator_has_its_own_registered_envelope_identity() {
    let schema = writable_record_schema("riffdb.storage.v1.StoredChangelogTransactionAllocatorV3")
        .expect("V3 physical allocation must not reuse an application allocator or bare payload");
    assert_eq!(schema.compact_tag(), 68);
    assert_eq!(schema.max_payload_bytes(), 11);
    assert_eq!(schema.schema_revision(), 1);
    for payload in [
        &[0x08, 1, 0x08, 2][..], // duplicate Next
        &[0x08, 1, 0x12, 0][..], // two states
        &[0x18, 1][..],          // unknown field
        &[0x12, 2, 0x08, 1][..], // nonempty Exhausted
        &[0x08, 0x81, 0][..],    // nonminimal varint
    ] {
        assert!(riffdb_proto::envelope::encode(schema, payload).is_err());
    }
}

#[test]
// req: REP-003, STO-012
fn v3_catalog_and_leadership_roots_have_distinct_bounded_registry_identities() {
    for (name, tag, bound) in [
        (
            "riffdb.storage.v1.StoredAuthoritativeStateCatalogV1",
            69,
            34,
        ),
        ("riffdb.storage.v1.StoredLeadershipEpochV1", 70, 11),
    ] {
        let schema =
            writable_record_schema(name).expect("activation root needs a registered envelope");
        assert_eq!(schema.compact_tag(), tag);
        assert_eq!(schema.schema_revision(), 1);
        assert_eq!(schema.max_payload_bytes(), bound);
    }
}

#[test]
// req: REP-003, STO-012, REC-001
fn v3_history_and_follower_roots_have_bounded_noninterchangeable_identities() {
    for (name, tag, bound) in [
        ("riffdb.storage.v1.StoredChangelogHistoryStateV3", 71, 281),
        (
            "riffdb.storage.v1.StoredReplicationFollowerStateV3",
            72,
            215,
        ),
    ] {
        let schema = writable_record_schema(name).expect("complete activation requires both roots");
        assert_eq!(schema.compact_tag(), tag);
        assert_eq!(schema.schema_revision(), 1);
        assert_eq!(schema.max_payload_bytes(), bound);
    }
}
