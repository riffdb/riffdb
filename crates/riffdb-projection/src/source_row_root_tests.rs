//! Semantic row-root tests; hashes never establish evaluation or publication.

use super::*;
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::CheckedProjectionSchema;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, DatabaseId, FieldId, ProjectionCheckpointOrdinal,
};

fn schema() -> CheckedProjectionSchema {
    let bundle = compile_contract_source("contract SourceRoot version 1 { event Input { group: string<32> } projection Totals { source event Input key (group) measure total = count() frontier transactionally_ordered } }").unwrap();
    CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(riffdb_types::ProjectionId::first())
            .unwrap(),
    )
}

fn row(
    schema: &CheckedProjectionSchema,
    ordinal: u64,
    group: &str,
    total: u64,
    changed: u64,
) -> (ProjectionGroupKey, ProjectionSourceRowV1) {
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string(group).unwrap()],
        )
        .unwrap();
    let locator = ProjectionSourceLocatorV1::new(
        DatabaseId::from_unix_milliseconds_and_random(42, [1; 10]).unwrap(),
        1,
        ProjectionCheckpointOrdinal::new(ordinal).unwrap(),
    )
    .unwrap();
    let row = ProjectionSourceRowV1::new(
        schema,
        locator,
        CommitSequence::new(10).unwrap(),
        key.clone(),
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(total))]).unwrap(),
        CommitSequence::new(changed).unwrap(),
    )
    .unwrap();
    (key, row)
}

fn root(
    schema: &CheckedProjectionSchema,
    rows: &[(ProjectionGroupKey, ProjectionSourceRowV1)],
) -> ContentDigest {
    let bytes = rows
        .iter()
        .map(|(_, row)| projection_source_row_preimage_bytes_v1(row).unwrap())
        .sum();
    let locator = rows.first().map(|(_, row)| row.locator());
    let mut root = ProjectionSourceRowRootV1::new(
        schema.identity(),
        locator,
        CommitSequence::new(10).unwrap(),
        rows.len() as u32,
        bytes,
    )
    .unwrap();
    let mut scratch = Vec::with_capacity(
        rows.iter()
            .map(|(_, row)| projection_source_row_scratch_bytes_v1(row).unwrap())
            .max()
            .unwrap_or(0),
    );
    for (key, row) in rows {
        root.append(key, row, &mut scratch).unwrap();
    }
    root.finish().unwrap()
}

// req: PRJ-004, REC-003
#[test]
fn source_row_root_binds_values_last_changed_identity_and_frontier() {
    let schema = schema();
    let original = root(&schema, &[row(&schema, 1, "a", 7, 3)]);
    assert_ne!(original, root(&schema, &[row(&schema, 1, "a", 8, 3)]));
    assert_ne!(original, root(&schema, &[row(&schema, 1, "a", 7, 4)]));
    assert_ne!(original, root(&schema, &[row(&schema, 1, "b", 7, 3)]));
    let (key, row) = row(&schema, 1, "a", 7, 3);
    let mut changed = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(row.locator()),
        CommitSequence::new(11).unwrap(),
        1,
        projection_source_row_preimage_bytes_v1(&row).unwrap(),
    )
    .unwrap();
    changed
        .append(
            &key,
            &row,
            &mut Vec::with_capacity(projection_source_row_scratch_bytes_v1(&row).unwrap()),
        )
        .unwrap();
    assert_ne!(original, changed.finish().unwrap());
}

// req: PRJ-004, REC-003
#[test]
fn source_row_root_excludes_allocation_ordinal_and_page_partitioning() {
    let schema = schema();
    let first = [row(&schema, 1, "a", 7, 3), row(&schema, 1, "b", 9, 4)];
    let later = [row(&schema, 128, "a", 7, 3), row(&schema, 128, "b", 9, 4)];
    assert_eq!(root(&schema, &first), root(&schema, &later));
    let bytes = first
        .iter()
        .map(|(_, row)| projection_source_row_preimage_bytes_v1(row).unwrap())
        .sum();
    let mut partitioned = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(first[0].1.locator()),
        CommitSequence::new(10).unwrap(),
        2,
        bytes,
    )
    .unwrap();
    let mut scratch = Vec::with_capacity(1024);
    for page in first.chunks(1) {
        for (key, row) in page {
            partitioned.append(key, row, &mut scratch).unwrap();
        }
    }
    assert_eq!(root(&schema, &first), partitioned.finish().unwrap());
}

// req: PRJ-004, STO-020
#[test]
fn source_row_root_refuses_unadmitted_scratch_without_growing_it() {
    let schema = schema();
    let (key, row) = row(&schema, 1, "a", 7, 3);
    let required = projection_source_row_scratch_bytes_v1(&row).unwrap();
    let mut scratch = Vec::with_capacity(required - 1);
    let capacity = scratch.capacity();
    let mut root = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(row.locator()),
        CommitSequence::new(10).unwrap(),
        1,
        projection_source_row_preimage_bytes_v1(&row).unwrap(),
    )
    .unwrap();
    assert_eq!(
        root.append(&key, &row, &mut scratch),
        Err(StorageValueError::LimitExceeded)
    );
    assert_eq!(scratch.capacity(), capacity);
    assert!(
        root.finish().is_err(),
        "a refused hash owner cannot certify a partial row stream"
    );
}

// req: PRJ-004, REC-003
#[test]
fn source_row_root_refuses_duplicates_foreign_keys_and_incomplete_streams() {
    let schema = schema();
    let (key, row) = row(&schema, 1, "a", 7, 3);
    let bytes = projection_source_row_preimage_bytes_v1(&row).unwrap();
    let mut incomplete = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(row.locator()),
        CommitSequence::new(10).unwrap(),
        2,
        bytes * 2,
    )
    .unwrap();
    let mut scratch = Vec::with_capacity(1024);
    incomplete.append(&key, &row, &mut scratch).unwrap();
    assert!(incomplete.finish().is_err());
    let mut duplicate = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(row.locator()),
        CommitSequence::new(10).unwrap(),
        2,
        bytes * 2,
    )
    .unwrap();
    duplicate.append(&key, &row, &mut scratch).unwrap();
    assert_eq!(
        duplicate.append(&key, &row, &mut scratch),
        Err(StorageValueError::NonCanonicalOrder)
    );
    assert!(duplicate.finish().is_err());
    let other_key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("b").unwrap()],
        )
        .unwrap();
    let mut foreign = ProjectionSourceRowRootV1::new(
        schema.identity(),
        Some(row.locator()),
        CommitSequence::new(10).unwrap(),
        1,
        bytes,
    )
    .unwrap();
    assert_eq!(
        foreign.append(&other_key, &row, &mut scratch),
        Err(StorageValueError::IdentityMismatch)
    );
}

// req: PRJ-004, STO-020
#[test]
fn source_row_root_empty_input_is_exact_and_source_limits_are_checked() {
    let schema = schema();
    let frontier = CommitSequence::new(10).unwrap();
    assert_eq!(
        root(&schema, &[]).domain(),
        HashDomain::ProjectionSourceRows
    );
    assert!(ProjectionSourceRowRootV1::new(schema.identity(), None, frontier, 0, 1).is_err());
    assert!(
        ProjectionSourceRowRootV1::new(
            schema.identity(),
            None,
            frontier,
            MAX_PROJECTION_SOURCE_ROWS + 1,
            1
        )
        .is_err()
    );
    assert!(
        ProjectionSourceRowRootV1::new(
            schema.identity(),
            None,
            frontier,
            1,
            MAX_PROJECTION_SOURCE_OBJECT_BYTES + 1
        )
        .is_err()
    );
}

// req: PRJ-004, REC-003, STO-020
#[test]
fn source_row_root_matches_independently_framed_canonical_record_bytes() {
    let schema = schema();
    let (key, row) = row(&schema, 1, "a", 7, 3);
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&1_u32.to_be_bytes());
    let identity = schema.identity().to_canonical_bytes();
    preimage.extend_from_slice(&(identity.len() as u32).to_be_bytes());
    preimage.extend_from_slice(&identity);
    preimage.extend_from_slice(&10_u64.to_be_bytes());
    let mut payload = Vec::new();
    payload.extend_from_slice(&1_u32.to_be_bytes());
    let group = riffdb_types::encode_canonical_value(&row.group_values()[0]).unwrap();
    payload.extend_from_slice(&(group.len() as u64).to_be_bytes());
    payload.extend_from_slice(&group);
    let measures =
        riffdb_types::encode_canonical_value(&CanonicalValue::Record(row.measures().clone()))
            .unwrap();
    payload.extend_from_slice(&(measures.len() as u64).to_be_bytes());
    payload.extend_from_slice(&measures);
    payload.extend_from_slice(&3_u64.to_be_bytes());
    preimage.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    preimage.extend_from_slice(&payload);
    preimage.extend_from_slice(&1_u32.to_be_bytes());
    preimage.extend_from_slice(&((payload.len() + 8) as u64).to_be_bytes());
    assert_eq!(
        root(&schema, &[(key, row)]),
        riffdb_types::hash(HashDomain::ProjectionSourceRows, &preimage)
    );
    let mut scratch = vec![99; 3];
    let original_capacity = scratch.capacity();
    riffdb_types::encode_canonical_record_into(
        &mut scratch,
        &CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(7))]).unwrap(),
    )
    .unwrap();
    assert_eq!(&scratch[3..], measures.as_slice());
    assert!(scratch.capacity() >= original_capacity);
}
