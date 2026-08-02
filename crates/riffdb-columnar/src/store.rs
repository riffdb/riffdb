//! In-memory delta, immutable segments, and merged read view (D3).

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_types::{
    CanonicalValue, EntityVersion, FieldId, FrontierPosition, encode_canonical_value,
};

use crate::error::ColumnarError;

/// Opaque org-partition key: canonical encoding of the org scope value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OrgKey(pub(crate) Vec<u8>);

impl OrgKey {
    /// Builds an org key from a scope value.
    pub fn from_value(value: &CanonicalValue) -> Result<Self, ColumnarError> {
        let bytes = encode_canonical_value(value)
            .map_err(|_| ColumnarError::Projection("org scope value failed canonical encoding"))?;
        Ok(Self(bytes))
    }

    /// Borrows the encoded bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
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
        let mut merged: BTreeMap<PrimaryKeyBytes, MergedRow> = BTreeMap::new();

        // Segments oldest → newest so later segments overwrite.
        for segment in &self.segments {
            if &segment.org != org {
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
                }
            }
        }

        merged
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
pub(crate) fn project_cells(
    fields: &[(FieldId, CanonicalValue)],
    projected: &[FieldId],
    org_field: FieldId,
) -> Result<(CanonicalValue, Vec<CanonicalValue>), ColumnarError> {
    let mut org = None;
    let mut cells = Vec::with_capacity(projected.len());
    for field_id in projected {
        let value = fields
            .iter()
            .find(|(id, _)| id == field_id)
            .map(|(_, value)| value.clone())
            .ok_or(ColumnarError::Projection(
                "projected field missing from entity",
            ))?;
        cells.push(value);
    }
    for (id, value) in fields {
        if *id == org_field {
            org = Some(value.clone());
            break;
        }
    }
    let org = org.ok_or(ColumnarError::Projection(
        "org scope field missing from entity",
    ))?;
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
}
