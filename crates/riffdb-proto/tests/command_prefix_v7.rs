#![forbid(unsafe_code)]
//! Immutable successor identities for exact command-prefix evidence.
// req: REP-007, AFC-007

use riffdb_proto::durable::{readable_record_schema, writable_record_schema};

#[test]
fn command_prefix_successors_have_distinct_accepted_compact_identities() {
    for (new, old, tag, frozen_prior_hash) in [
        (
            "StoredCommandCapsuleV7",
            "StoredCommandCapsuleV6",
            54,
            "0bc288e0f08ec4b83379f5abb4b872ebfc3ea7b7a64983eedcd53a9da25731bb",
        ),
        (
            "StoredCommandSegmentV6",
            "StoredCommandSegmentV5",
            55,
            "0df52ca7232d2383b820b4b8ab844cf08d35529aa78e4b9b2bdd43b28afbd7dd",
        ),
    ] {
        let new = format!("riffdb.storage.v1.{new}");
        let old = format!("riffdb.storage.v1.{old}");
        let successor = readable_record_schema(&new).expect("approved successor is readable");
        let prior = readable_record_schema(&old).expect("old bytes remain readable");
        assert_eq!(successor.compact_tag(), tag);
        assert_eq!(successor.schema_revision(), 6);
        assert_eq!(prior.compact_tag(), tag);
        assert_eq!(prior.schema_revision(), 5);
        assert_eq!(
            prior
                .schema_hash()
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            frozen_prior_hash
        );
        assert_ne!(successor.schema_hash(), prior.schema_hash());
        assert!(writable_record_schema(&new).is_some());
    }
}
