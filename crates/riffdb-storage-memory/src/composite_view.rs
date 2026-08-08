//! Volatile conformance adapter for ADR-0104 composite-view schedules.
//!
//! The production memory backend remains semantic-state based. This closed raw
//! adapter independently proves the engine-neutral overlay publication rules
//! without becoming a selectable application path.

#![allow(
    dead_code,
    reason = "WP-487 conformance adapter is exercised only by deterministic schedules"
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_storage_api::{
    CompositeCheckpointV1, CompositeFrameV1, CompositeOverlayBuilder, CompositeTableV1,
    CompositeViewBase, FrozenCompositeOverlay, StorageValueError,
};

type MemoryCompositeRows = BTreeMap<(CompositeTableV1, Box<[u8]>), Box<[u8]>>;

#[derive(Clone, Default)]
struct MemoryCompositeBase {
    rows: Arc<MemoryCompositeRows>,
}

impl MemoryCompositeBase {
    fn from_rows(rows: MemoryCompositeRows) -> Self {
        Self {
            rows: Arc::new(rows),
        }
    }
}

impl CompositeViewBase for MemoryCompositeBase {
    fn read_base(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        Ok(self
            .rows
            .get(&(table, key.into()))
            .map(|value| value.to_vec()))
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

struct MemoryCompositeViewBuilder {
    base: MemoryCompositeBase,
    overlay: CompositeOverlayBuilder,
}

impl MemoryCompositeViewBuilder {
    fn new(base: MemoryCompositeBase, checkpoint: CompositeCheckpointV1) -> Self {
        Self {
            base,
            overlay: CompositeOverlayBuilder::new(checkpoint),
        }
    }

    fn apply_frame(&mut self, frame: &CompositeFrameV1) -> Result<(), StorageValueError> {
        self.overlay.apply_frame(frame, &self.base)
    }

    fn freeze(self) -> MemoryCompositeReadView {
        MemoryCompositeReadView {
            base: self.base,
            overlay: self.overlay.freeze(),
        }
    }
}

struct MemoryCompositeReadView {
    base: MemoryCompositeBase,
    overlay: FrozenCompositeOverlay,
}

impl MemoryCompositeReadView {
    fn resolve_point(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        self.overlay.resolve_point(&self.base, table, key)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use riffdb_storage_api::{CompositeFrameKindV1, CompositeMutationV1, OverlayLookup};
    use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, SchemaHash};

    use super::*;

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [9; 10]).expect("database")
    }

    fn checkpoint() -> CompositeCheckpointV1 {
        CompositeCheckpointV1::new(
            database_id(),
            1,
            SchemaHash::from_bytes([4; 32]),
            Some(CommitSequence::first()),
            Some(AdministrationSequence::first()),
            [1; 32],
        )
        .expect("checkpoint")
    }

    fn compacted_checkpoint() -> CompositeCheckpointV1 {
        CompositeCheckpointV1::new(
            database_id(),
            1,
            SchemaHash::from_bytes([4; 32]),
            CommitSequence::new(2),
            AdministrationSequence::new(2),
            [2; 32],
        )
        .expect("compacted checkpoint")
    }

    fn base(value: &[u8], generation: &[u8]) -> MemoryCompositeBase {
        let mut rows = MemoryCompositeRows::new();
        rows.insert(
            (CompositeTableV1::Entities, b"entity".as_slice().into()),
            value.into(),
        );
        rows.insert(
            (
                CompositeTableV1::IndexEpochs,
                b"generation".as_slice().into(),
            ),
            generation.into(),
        );
        MemoryCompositeBase::from_rows(rows)
    }

    fn successor_frame() -> CompositeFrameV1 {
        CompositeFrameV1::new(
            CompositeFrameKindV1::Command,
            database_id(),
            Some(CommitSequence::first()),
            CommitSequence::new(2),
            Some(AdministrationSequence::first()),
            AdministrationSequence::new(2),
            1,
            256,
            [1; 32],
            [2; 32],
            vec![
                CompositeMutationV1::replace(
                    CompositeTableV1::Entities,
                    b"entity".as_slice(),
                    b"old",
                    b"new".as_slice(),
                )
                .expect("mutation"),
            ],
        )
        .expect("frame")
    }

    #[test]
    fn captured_request_never_mixes_predecessor_base_with_successor_overlay() {
        let builder = MemoryCompositeViewBuilder::new(base(b"old", b"7"), checkpoint());
        let predecessor = Arc::new(builder.freeze());

        let mut successor_builder =
            MemoryCompositeViewBuilder::new(base(b"old", b"7"), checkpoint());
        successor_builder
            .apply_frame(&successor_frame())
            .expect("apply");
        let successor = Arc::new(successor_builder.freeze());
        let published = RwLock::new(Arc::clone(&predecessor));
        let captured_before = Arc::clone(&published.read().expect("read"));
        *published.write().expect("publish") = Arc::clone(&successor);
        let captured_after = Arc::clone(&published.read().expect("read"));

        assert_eq!(
            captured_before
                .resolve_point(CompositeTableV1::Entities, b"entity")
                .expect("predecessor"),
            Some(b"old".to_vec())
        );
        assert_eq!(
            captured_after
                .resolve_point(CompositeTableV1::Entities, b"entity")
                .expect("successor"),
            Some(b"new".to_vec())
        );
        assert_eq!(
            captured_before
                .overlay
                .lookup(CompositeTableV1::Entities, b"entity"),
            OverlayLookup::Unchanged
        );
    }

    #[test]
    fn physical_checkpoint_compaction_does_not_change_logical_generation() {
        let mut builder = MemoryCompositeViewBuilder::new(base(b"old", b"7"), checkpoint());
        builder
            .apply_frame(&successor_frame())
            .expect("apply successor");
        let before_compaction = builder.freeze();

        let after_compaction =
            MemoryCompositeViewBuilder::new(base(b"new", b"7"), compacted_checkpoint()).freeze();
        assert_eq!(
            before_compaction
                .resolve_point(CompositeTableV1::IndexEpochs, b"generation")
                .expect("before"),
            Some(b"7".to_vec())
        );
        assert_eq!(
            after_compaction
                .resolve_point(CompositeTableV1::IndexEpochs, b"generation")
                .expect("after"),
            Some(b"7".to_vec())
        );
    }
}
