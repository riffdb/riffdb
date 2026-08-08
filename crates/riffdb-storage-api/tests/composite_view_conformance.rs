//! Engine-neutral ADR-0104 overlay publication conformance.

use std::collections::BTreeMap;

use riffdb_storage_api::{
    CompositeCheckpointV1, CompositeFrameKindV1, CompositeFrameV1, CompositeMutationV1,
    CompositeOverlayBuilder, CompositeTableV1, CompositeViewBase, OverlayLookup, StorageValueError,
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, SchemaHash};

#[derive(Default)]
struct Base(BTreeMap<(CompositeTableV1, Vec<u8>), Vec<u8>>);

impl CompositeViewBase for Base {
    fn read_base(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        Ok(self.0.get(&(table, key.to_vec())).cloned())
    }

    fn validate_entry(
        &self,
        _table: CompositeTableV1,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), StorageValueError> {
        if key.is_empty() || value.is_some_and(<[u8]>::is_empty) {
            return Err(StorageValueError::Empty);
        }
        Ok(())
    }
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [10; 10]).expect("database")
}

fn checkpoint() -> CompositeCheckpointV1 {
    CompositeCheckpointV1::new(
        database_id(),
        1,
        SchemaHash::from_bytes([11; 32]),
        None,
        None,
        [0; 32],
    )
    .expect("checkpoint")
}

#[test]
fn exact_successor_publication_preserves_old_frozen_view() {
    let base = Base::default();
    let predecessor = CompositeOverlayBuilder::new(checkpoint()).freeze();
    let mut successor = CompositeOverlayBuilder::new(checkpoint());
    let frame = CompositeFrameV1::new(
        CompositeFrameKindV1::Command,
        database_id(),
        None,
        Some(CommitSequence::first()),
        None,
        Some(AdministrationSequence::first()),
        1,
        128,
        [0; 32],
        [1; 32],
        vec![
            CompositeMutationV1::put(
                CompositeTableV1::Entities,
                b"key".as_slice(),
                b"secret-value".as_slice(),
            )
            .expect("mutation"),
        ],
    )
    .expect("frame");
    successor.apply_frame(&frame, &base).expect("apply");
    let successor = successor.freeze();

    assert_eq!(
        predecessor.lookup(CompositeTableV1::Entities, b"key"),
        OverlayLookup::Unchanged
    );
    assert_eq!(
        successor.lookup(CompositeTableV1::Entities, b"key"),
        OverlayLookup::Value(b"secret-value")
    );
    assert_eq!(
        successor.published_application(),
        Some(CommitSequence::first())
    );
    assert_eq!(
        successor.published_administration(),
        Some(AdministrationSequence::first())
    );
}

#[test]
fn writer_private_fork_withholds_successor_from_captured_reader() {
    let base = Base::default();
    let mut first = CompositeOverlayBuilder::new(checkpoint());
    first
        .apply_frame(
            &CompositeFrameV1::new(
                CompositeFrameKindV1::Command,
                database_id(),
                None,
                Some(CommitSequence::first()),
                None,
                Some(AdministrationSequence::first()),
                1,
                128,
                [0; 32],
                [1; 32],
                vec![
                    CompositeMutationV1::put(
                        CompositeTableV1::Entities,
                        b"ticket".as_slice(),
                        b"open".as_slice(),
                    )
                    .expect("first mutation"),
                ],
            )
            .expect("first frame"),
            &base,
        )
        .expect("apply first frame");
    let captured = first.freeze();

    let mut private = CompositeOverlayBuilder::from_published(&captured);
    private
        .apply_frame(
            &CompositeFrameV1::new(
                CompositeFrameKindV1::Command,
                database_id(),
                Some(CommitSequence::first()),
                CommitSequence::new(2),
                Some(AdministrationSequence::first()),
                AdministrationSequence::new(2),
                1,
                128,
                [1; 32],
                [2; 32],
                vec![
                    CompositeMutationV1::replace(
                        CompositeTableV1::Entities,
                        b"ticket".as_slice(),
                        b"open",
                        b"closed".as_slice(),
                    )
                    .expect("successor mutation"),
                ],
            )
            .expect("successor frame"),
            &base,
        )
        .expect("apply private successor");

    assert_eq!(
        captured.lookup(CompositeTableV1::Entities, b"ticket"),
        OverlayLookup::Value(b"open")
    );
    let successor = private.freeze();
    assert_eq!(
        successor.lookup(CompositeTableV1::Entities, b"ticket"),
        OverlayLookup::Value(b"closed")
    );
    assert_eq!(captured.transition_count(), 1);
    assert_eq!(successor.transition_count(), 2);
}

#[test]
fn storage_debug_never_exposes_overlay_keys_or_values() {
    let mutation = CompositeMutationV1::put(
        CompositeTableV1::Entities,
        b"business-key-canary".as_slice(),
        b"business-value-canary".as_slice(),
    )
    .expect("mutation");
    let rendered = format!("{mutation:?}");
    assert!(!rendered.contains("business-key-canary"));
    assert!(!rendered.contains("business-value-canary"));
    assert!(rendered.contains("[REDACTED]"));
}
