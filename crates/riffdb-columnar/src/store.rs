//! In-memory delta, immutable segments, and merged read view (D3).

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_types::{
    CanonicalValue, EntityVersion, FieldId, FrontierPosition, encode_canonical_value,
};

use crate::error::ColumnarError;
use crate::segment_v2::SegmentV2PruningIndex;

/// Opaque org-partition key: canonical encoding of the org scope value.
///
/// Encoding is exactly [`encode_canonical_value`] of the org scope
/// [`CanonicalValue`]. CP2b's org/partition equivalence test must round-trip
/// both this encoding and the aggregate partition key codec over every
/// supported org type — a mismatch is a cross-tenant authorization bypass.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrgKey(pub(crate) Vec<u8>);

impl OrgKey {
    /// Builds an org key from a scope value (public CP2b equivalence hook).
    ///
    /// Equivalent to encoding the value with the ADR-0011 canonical value
    /// codec and wrapping the bytes. See crate-level note on partition
    /// equivalence.
    pub fn from_value(value: &CanonicalValue) -> Result<Self, ColumnarError> {
        let bytes = encode_canonical_value(value)
            .map_err(|_| ColumnarError::Projection("org scope value failed canonical encoding"))?;
        Ok(Self(bytes))
    }

    /// Reconstructs an org key from already-encoded canonical bytes.
    ///
    /// Does not re-validate the payload shape; intended solely for CP2b's
    /// cross-crate partition-encoding equivalence proof, which must compare
    /// byte-for-byte against partition codecs. Hidden from the documented API
    /// so [`OrgKey::from_value`] stays the only advertised constructor.
    #[doc(hidden)]
    #[must_use]
    pub fn from_encoded_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Borrows the encoded bytes (canonical org-scope encoding).
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Encodes an organization scope value into the partition key used by the
/// columnar engine. Public hook for CP2b's org/partition equivalence proof.
pub fn encode_org_scope_key(value: &CanonicalValue) -> Result<OrgKey, ColumnarError> {
    OrgKey::from_value(value)
}

/// Primary key bytes (entity key envelope bytes).
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrimaryKeyBytes(pub(crate) Vec<u8>);

impl PrimaryKeyBytes {
    /// Wraps entity key bytes.
    #[must_use]
    pub fn from_entity_key_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Borrows the key bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Durable segment file identity within a projection directory.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SegmentId(pub(crate) String);

impl SegmentId {
    pub(crate) fn from_file_name(file_name: String) -> Self {
        Self(file_name)
    }

    /// Segment file name relative to the projection directory.
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.0
    }
}

/// One live projected row held in the delta or a segment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveRow {
    /// Authoritative entity version that produced this row image.
    pub entity_version: EntityVersion,
    /// Projected cell values aligned with the registered field order.
    pub cells: Vec<CanonicalValue>,
}

/// Per-org mutable delta map (live rows only; deletes do not exist in CP1).
pub type OrgDelta = BTreeMap<PrimaryKeyBytes, LiveRow>;

/// One immutable on-disk segment for a single org partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Segment {
    /// Segment identity / file name.
    pub id: SegmentId,
    /// Organization partition key.
    pub org: OrgKey,
    /// Rows sorted by primary key (unique keys within a segment).
    pub rows: BTreeMap<PrimaryKeyBytes, LiveRow>,
    /// SHA-256 checksum of the durable file bytes.
    pub checksum: [u8; 32],
    /// Private pruning facts retained only by a fully validated V2 open.
    pub(crate) pruning: Option<SegmentV2PruningIndex>,
}

/// Published queryable snapshot (atomic publication unit).
#[derive(Clone, Debug)]
pub struct ColumnarSnapshot {
    /// Immutable segments, oldest first (newer segments override older on merge).
    pub segments: Vec<Arc<Segment>>,
    /// In-memory delta layered above all segments.
    pub delta: BTreeMap<OrgKey, OrgDelta>,
    /// Visible frontier guaranteed by this snapshot.
    pub visible_frontier: FrontierPosition,
}

impl ColumnarSnapshot {
    /// Empty snapshot before the first publication.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            segments: Vec::new(),
            delta: BTreeMap::new(),
            visible_frontier: FrontierPosition::BeforeFirst,
        }
    }

    /// Merged live rows for one org at this snapshot.
    #[must_use]
    pub fn merged_org(&self, org: &OrgKey) -> BTreeMap<PrimaryKeyBytes, MergedRow> {
        self.merged_org_bounded(org, usize::MAX)
            .expect("usize::MAX cannot be exceeded by an in-memory map")
    }

    /// Merged live rows for one org, refusing as soon as the retained key set
    /// exceeds a server-owned admission ceiling.
    ///
    /// This avoids materializing an unbounded candidate vector merely to
    /// discover that protected authoritative admission cannot proceed.
    #[must_use]
    pub fn merged_org_bounded(
        &self,
        org: &OrgKey,
        maximum: usize,
    ) -> Option<BTreeMap<PrimaryKeyBytes, MergedRow>> {
        self.merged_org_bounded_with_segment_filter(org, maximum, |_| true)
    }

    pub(crate) fn merged_org_bounded_with_segment_filter(
        &self,
        org: &OrgKey,
        maximum: usize,
        mut include_segment: impl FnMut(&Segment) -> bool,
    ) -> Option<BTreeMap<PrimaryKeyBytes, MergedRow>> {
        let mut merged: BTreeMap<PrimaryKeyBytes, MergedRow> = BTreeMap::new();

        // Segments oldest → newest so later segments overwrite.
        for segment in &self.segments {
            if &segment.org != org || !include_segment(segment) {
                continue;
            }
            for (key, row) in &segment.rows {
                merged.insert(
                    key.clone(),
                    MergedRow {
                        entity_version: row.entity_version,
                        cells: row.cells.clone(),
                    },
                );
                if merged.len() > maximum {
                    return None;
                }
            }
        }

        if let Some(delta) = self.delta.get(org) {
            for (key, row) in delta {
                let replace = match merged.get(key) {
                    None => true,
                    Some(existing) => {
                        // Supersession / version mask: newer (or equal) wins.
                        // Equal version is idempotent overwrite for replay.
                        supersession_should_replace(
                            existing.entity_version.get(),
                            row.entity_version.get(),
                        )
                    }
                };
                if replace {
                    merged.insert(
                        key.clone(),
                        MergedRow {
                            entity_version: row.entity_version,
                            cells: row.cells.clone(),
                        },
                    );
                    if merged.len() > maximum {
                        return None;
                    }
                }
            }
        }

        Some(merged)
    }

    /// Borrowing counterpart of [`Self::merged_org_bounded_with_segment_filter`]
    /// for the query read path.
    ///
    /// Produces the same key-ordered, supersession-resolved view, but each entry
    /// borrows the winning row's key and cells from the segment or delta rather
    /// than deep-cloning them. The borrows are valid for the snapshot borrow
    /// `&self`, which every query executor holds for the whole call, so no row
    /// is cloned up front — including rows a predicate later rejects and cells a
    /// query never projects. Only cells actually selected into a result are
    /// cloned, at the point they must outlive the snapshot.
    pub(crate) fn merged_org_ref_bounded_with_segment_filter<'s>(
        &'s self,
        org: &OrgKey,
        maximum: usize,
        mut include_segment: impl FnMut(&Segment) -> bool,
    ) -> Option<BTreeMap<&'s PrimaryKeyBytes, MergedRowRef<'s>>> {
        let mut merged: BTreeMap<&'s PrimaryKeyBytes, MergedRowRef<'s>> = BTreeMap::new();

        // Segments oldest → newest so later segments overwrite.
        for segment in &self.segments {
            if &segment.org != org || !include_segment(segment) {
                continue;
            }
            for (key, row) in &segment.rows {
                merged.insert(key, MergedRowRef::from_live(row));
                if merged.len() > maximum {
                    return None;
                }
            }
        }

        if let Some(delta) = self.delta.get(org) {
            for (key, row) in delta {
                let replace = match merged.get(key) {
                    None => true,
                    Some(existing) => supersession_should_replace(
                        existing.entity_version.get(),
                        row.entity_version.get(),
                    ),
                };
                if replace {
                    merged.insert(key, MergedRowRef::from_live(row));
                    if merged.len() > maximum {
                        return None;
                    }
                }
            }
        }

        Some(merged)
    }

    /// Bounded borrowing merge over every segment for one org.
    pub(crate) fn merged_org_ref_bounded<'s>(
        &'s self,
        org: &OrgKey,
        maximum: usize,
    ) -> Option<BTreeMap<&'s PrimaryKeyBytes, MergedRowRef<'s>>> {
        self.merged_org_ref_bounded_with_segment_filter(org, maximum, |_| true)
    }
}

/// One row after merge (live only).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergedRow {
    /// Entity version of the surviving image.
    pub entity_version: EntityVersion,
    /// Projected cells.
    pub cells: Vec<CanonicalValue>,
}

/// A borrowed merged row for the query read path.
///
/// Mirrors [`MergedRow`] but borrows its cells from the snapshot's segment or
/// delta instead of owning a cloned `Vec`. Field names match `MergedRow` so the
/// `row.cells[idx]` / `row.entity_version` access shape is identical at every
/// consumer. The lifetime `'s` is the snapshot borrow held for the whole query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MergedRowRef<'s> {
    /// Entity version of the surviving image.
    pub entity_version: EntityVersion,
    /// Projected cells borrowed from the winning segment or delta row.
    pub cells: &'s [CanonicalValue],
}

impl<'s> MergedRowRef<'s> {
    #[inline]
    fn from_live(row: &'s LiveRow) -> Self {
        Self {
            entity_version: row.entity_version,
            cells: &row.cells,
        }
    }
}

/// Working (unpublished) state mutated by apply; published via Arc swap.
#[derive(Clone, Debug)]
pub(crate) struct WorkingState {
    /// Segments referenced by the working set (includes published + pending merges).
    pub segments: Vec<Arc<Segment>>,
    /// Working delta including unpublished commit effects.
    pub delta: BTreeMap<OrgKey, OrgDelta>,
    /// Processed commit position (may lead published).
    pub processed: FrontierPosition,
}

impl Default for WorkingState {
    fn default() -> Self {
        Self {
            segments: Vec::new(),
            delta: BTreeMap::new(),
            processed: FrontierPosition::BeforeFirst,
        }
    }
}

impl WorkingState {
    /// Upserts a live row into the delta. Superseded older images for the same
    /// key are masked at read time by the version comparison in
    /// [`supersession_should_replace`]; no tombstone state exists (deletes do
    /// not exist in CP1 — see the crate root docs).
    pub(crate) fn upsert_live(&mut self, org: OrgKey, key: PrimaryKeyBytes, row: LiveRow) {
        self.delta.entry(org).or_default().insert(key, row);
    }

    /// Snapshot clone of segments + delta at a frontier (for publication).
    #[must_use]
    pub(crate) fn to_snapshot(&self, visible_frontier: FrontierPosition) -> ColumnarSnapshot {
        ColumnarSnapshot {
            segments: self.segments.clone(),
            delta: self.delta.clone(),
            visible_frontier,
        }
    }
}

/// Whether an incoming live row supersedes an existing merged row.
///
/// Extracted so falsifiability can neuter supersession masking in one place.
#[inline]
pub(crate) fn supersession_should_replace(existing_version: u64, incoming_version: u64) -> bool {
    incoming_version >= existing_version
}

/// Extracts projected cells + org value from a field map.
///
/// `fields` is a [`riffdb_types::CanonicalRecord`] field slice, which is guaranteed to be in
/// strictly increasing stable-`FieldId` order with no duplicates. That lets each
/// projected field be located with a binary search rather than a linear scan,
/// so projection is O(projected × log fields) instead of O(projected × fields)
/// per record on the apply, rebuild, and generation-build paths.
pub(crate) fn project_cells(
    fields: &[(FieldId, CanonicalValue)],
    projected: &[FieldId],
    org_field: FieldId,
) -> Result<(CanonicalValue, Vec<CanonicalValue>), ColumnarError> {
    // The slice is sorted by `FieldId::get()` (see `CanonicalRecord::new`), so a
    // key-wise binary search over that ordering is exact.
    let find = |field_id: FieldId| {
        fields
            .binary_search_by_key(&field_id.get(), |(id, _)| id.get())
            .ok()
            .map(|index| &fields[index].1)
    };

    let mut cells = Vec::with_capacity(projected.len());
    for field_id in projected {
        let value = find(*field_id)
            .ok_or(ColumnarError::Projection(
                "projected field missing from entity",
            ))?
            .clone();
        cells.push(value);
    }
    let org = find(org_field)
        .ok_or(ColumnarError::Projection(
            "org scope field missing from entity",
        ))?
        .clone();
    Ok((org, cells))
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::EntityVersion;

    fn pk(n: u8) -> PrimaryKeyBytes {
        PrimaryKeyBytes::from_entity_key_bytes(vec![n])
    }

    fn live(v: u64, cell: u64) -> LiveRow {
        LiveRow {
            entity_version: EntityVersion::new(v).expect("v"),
            cells: vec![CanonicalValue::U64(cell)],
        }
    }

    #[test]
    fn newer_live_masks_older_segment_row() {
        let org = OrgKey(vec![1]);
        let segment = Arc::new(Segment {
            id: SegmentId("s1".into()),
            org: org.clone(),
            rows: BTreeMap::from([(pk(1), live(1, 10))]),
            checksum: [0; 32],
            pruning: None,
        });
        let mut snapshot = ColumnarSnapshot::empty();
        snapshot.segments.push(segment);
        snapshot
            .delta
            .insert(org.clone(), BTreeMap::from([(pk(1), live(2, 20))]));
        let merged = snapshot.merged_org(&org);
        assert_eq!(merged[&pk(1)].cells, vec![CanonicalValue::U64(20)]);
        assert_eq!(merged[&pk(1)].entity_version.get(), 2);
    }

    #[test]
    fn older_delta_row_does_not_mask_newer_segment_row() {
        let org = OrgKey(vec![1]);
        let segment = Arc::new(Segment {
            id: SegmentId("s1".into()),
            org: org.clone(),
            rows: BTreeMap::from([(pk(1), live(3, 30))]),
            checksum: [0; 32],
            pruning: None,
        });
        let mut snapshot = ColumnarSnapshot::empty();
        snapshot.segments.push(segment);
        snapshot
            .delta
            .insert(org.clone(), BTreeMap::from([(pk(1), live(2, 20))]));
        let merged = snapshot.merged_org(&org);
        assert_eq!(merged[&pk(1)].cells, vec![CanonicalValue::U64(30)]);
        assert_eq!(merged[&pk(1)].entity_version.get(), 3);
    }

    // req: OQ-019
    #[test]
    fn borrowed_merge_preserves_precedence_bounds_and_snapshot_backing() {
        let org = OrgKey(vec![1]);
        let other = OrgKey(vec![2]);
        let mut snapshot = ColumnarSnapshot::empty();
        for (name, organization, rows) in [
            ("old", org.clone(), vec![(1, 8, 80), (2, 1, 10), (3, 1, 10)]),
            ("other", other.clone(), vec![(1, 99, 990)]),
            ("new", org.clone(), vec![(1, 3, 30), (2, 2, 20), (4, 4, 40)]),
        ] {
            snapshot.segments.push(Arc::new(Segment {
                id: SegmentId(name.into()),
                org: organization,
                rows: rows
                    .into_iter()
                    .map(|(key, version, cell)| (pk(key), live(version, cell)))
                    .collect(),
                checksum: [0; 32],
                pruning: None,
            }));
        }
        snapshot.delta.insert(
            org.clone(),
            BTreeMap::from([
                (pk(1), live(2, 20)),
                (pk(2), live(2, 20)),
                (pk(3), live(9, 333)),
                (pk(5), live(1, 555)),
            ]),
        );
        snapshot
            .delta
            .insert(other.clone(), BTreeMap::from([(pk(6), live(1, 666))]));

        let merged = snapshot
            .merged_org_ref_bounded(&org, 5)
            .expect("exact bound");
        assert_eq!(
            merged
                .iter()
                .map(|(key, row)| (
                    key.as_bytes()[0],
                    row.entity_version.get(),
                    row.cells[0].clone()
                ))
                .collect::<Vec<_>>(),
            vec![
                (1, 3, CanonicalValue::U64(30)),
                (2, 2, CanonicalValue::U64(20)),
                (3, 9, CanonicalValue::U64(333)),
                (4, 4, CanonicalValue::U64(40)),
                (5, 1, CanonicalValue::U64(555)),
            ]
        );
        // A later segment wins by segment order. Older delta versions do not
        // replace it; equal and newer versions borrow the delta's actual cells.
        assert!(std::ptr::eq(
            merged[&pk(1)].cells,
            snapshot.segments[2].rows[&pk(1)].cells.as_slice()
        ));
        for key in [2, 3, 5] {
            assert!(std::ptr::eq(
                merged[&pk(key)].cells,
                snapshot.delta[&org][&pk(key)].cells.as_slice()
            ));
        }

        for organization in [&org, &other, &OrgKey(vec![3])] {
            for maximum in 0..=6 {
                for omit_new in [false, true] {
                    let include = |segment: &Segment| !omit_new || segment.id.file_name() != "new";
                    let owned = snapshot.merged_org_bounded_with_segment_filter(
                        organization,
                        maximum,
                        include,
                    );
                    let borrowed = snapshot
                        .merged_org_ref_bounded_with_segment_filter(organization, maximum, include)
                        .map(|rows| {
                            rows.into_iter()
                                .map(|(key, row)| {
                                    (
                                        key.clone(),
                                        MergedRow {
                                            entity_version: row.entity_version,
                                            cells: row.cells.to_vec(),
                                        },
                                    )
                                })
                                .collect::<BTreeMap<_, _>>()
                        });
                    assert_eq!(borrowed, owned);
                }
            }
        }
    }

    // req: PRJ-004
    #[test]
    fn projection_lookup_uses_canonical_field_order_and_preserves_missing_field_errors() {
        let first = FieldId::new(1).expect("field");
        let org = FieldId::new(255).expect("field");
        let next = FieldId::new(256).expect("field");
        let last = FieldId::new(u32::MAX).expect("field");
        let missing = FieldId::new(2).expect("field");
        let record = riffdb_types::CanonicalRecord::new(vec![
            (last, CanonicalValue::U64(4)),
            (next, CanonicalValue::U64(3)),
            (first, CanonicalValue::U64(1)),
            (org, CanonicalValue::U64(2)),
        ])
        .expect("canonical record normalizes shuffled fields");
        assert_eq!(
            project_cells(record.fields(), &[last, first, next], org).expect("project"),
            (
                CanonicalValue::U64(2),
                vec![
                    CanonicalValue::U64(4),
                    CanonicalValue::U64(1),
                    CanonicalValue::U64(3)
                ]
            )
        );
        assert!(matches!(
            project_cells(record.fields(), &[missing], missing),
            Err(ColumnarError::Projection(
                "projected field missing from entity"
            ))
        ));
        assert!(matches!(
            project_cells(record.fields(), &[first], missing),
            Err(ColumnarError::Projection(
                "org scope field missing from entity"
            ))
        ));
    }
}
