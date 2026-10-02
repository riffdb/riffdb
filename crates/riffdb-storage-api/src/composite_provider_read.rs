//! Borrowed inspection of an immutable overlay for admitted provider reads.
// req: OQ-040
use super::{CompositeTableV1, FrozenCompositeOverlay, OverlayNode, OverlayValue};

/// Exact overlay decision. Checkpoint fallback is distinct from a tombstone.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BorrowedOverlayPointV1<'view> {
    /// No overlay entry: consult the same captured checkpoint.
    Checkpoint,
    /// The captured overlay deletes this key.
    Tombstone,
    /// Borrowed canonical bytes owned by the captured immutable overlay.
    Value(&'view [u8]),
}

impl FrozenCompositeOverlay {
    /// Inspect before reserving a provider copy; never allocates payload data.
    #[doc(hidden)]
    pub fn borrowed_provider_point_v1(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> BorrowedOverlayPointV1<'_> {
        match self.tables[table.index()].get(key) {
            None => BorrowedOverlayPointV1::Checkpoint,
            Some(OverlayValue::Tombstone) => BorrowedOverlayPointV1::Tombstone,
            Some(OverlayValue::Value(value)) => BorrowedOverlayPointV1::Value(value),
        }
    }

    /// Next exact overlay entry in the existing forward scan order. Tombstones
    /// are included so a merge cannot revive a deleted checkpoint value. Seeking
    /// uses only borrowed AVL nodes, with no iterator stack or row collection.
    #[doc(hidden)]
    pub fn borrowed_provider_next_v1<'view>(
        &'view self,
        table: CompositeTableV1,
        start: &[u8],
        after: Option<&[u8]>,
        end: &[u8],
    ) -> Option<(&'view [u8], Option<&'view [u8]>)> {
        borrowed_next(
            self.tables[table.index()].root.as_deref(),
            start,
            after,
            end,
        )
    }
}

fn borrowed_next<'view>(
    mut current: Option<&'view OverlayNode>,
    start: &[u8],
    after: Option<&[u8]>,
    end: &[u8],
) -> Option<(&'view [u8], Option<&'view [u8]>)> {
    let mut candidate = None;
    while let Some(node) = current {
        let key = node.key.as_ref();
        if key < start || after.is_some_and(|after| key <= after) {
            current = node.right.as_deref();
        } else {
            candidate = Some(node);
            current = node.left.as_deref();
        }
    }
    let node = candidate?;
    if node.key.as_ref() >= end {
        return None;
    }
    let value = match &node.value {
        OverlayValue::Tombstone => None,
        OverlayValue::Value(value) => Some(value.as_ref()),
    };
    Some((node.key.as_ref(), value))
}

#[cfg(test)]
mod tests {
    use super::super::PersistentOverlayMap;
    use super::*;
    use std::sync::Arc;

    #[test]
    fn borrowed_seek_preserves_exact_order_bounds_and_tombstones() {
        let mut map = PersistentOverlayMap::default();
        for (key, value) in [
            (b"a".as_slice(), Some(b"A".as_slice())),
            (b"b".as_slice(), None),
            (b"c".as_slice(), Some(b"C".as_slice())),
        ] {
            map.insert(
                Arc::from(key),
                value.map_or(OverlayValue::Tombstone, |value| {
                    OverlayValue::Value(Arc::from(value))
                }),
            );
        }
        let root = map.root.as_deref();
        assert_eq!(
            borrowed_next(root, b"a", None, b"z"),
            Some((b"a".as_slice(), Some(b"A".as_slice())))
        );
        assert_eq!(
            borrowed_next(root, b"a", Some(b"a"), b"z"),
            Some((b"b".as_slice(), None))
        );
        assert_eq!(
            borrowed_next(root, b"b", Some(b"b"), b"z"),
            Some((b"c".as_slice(), Some(b"C".as_slice())))
        );
        assert_eq!(borrowed_next(root, b"a", Some(b"c"), b"z"), None);
        assert_eq!(borrowed_next(root, b"a", Some(b"b"), b"c"), None);
        assert_eq!(borrowed_next(root, b"d", None, b"z"), None);
    }
}
