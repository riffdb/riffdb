//! Physical Source key syntax/compatibility proofs; never publication authority.
use super::*;
use riffdb_types::{
    CanonicalValue, ContractLineage, ProjectionGroupKeyBuilder, ProjectionId, ProjectionPlanHash,
};

const DATABASE: [u8; 16] = [0, 0, 0, 0, 0, 42, 0x70, 1, 0x80, 1, 1, 1, 1, 1, 1, 1];
fn locator(incarnation: u64, ordinal: u64) -> ProjectionSourceLocatorV1 {
    ProjectionSourceLocatorV1::new(
        DatabaseId::from_bytes(DATABASE).unwrap(),
        incarnation,
        ProjectionCheckpointOrdinal::new(ordinal).unwrap(),
    )
    .unwrap()
}
fn identity(lineage: &str) -> ProjectionIdentity {
    ProjectionIdentity::new(
        ContractLineage::new(lineage).unwrap(),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([7; 32]),
    )
}
fn group(value: CanonicalValue) -> ProjectionGroupKey {
    let mut builder = ProjectionGroupKeyBuilder::new(identity("K"), ProjectionGeneration::first());
    builder.push_component(value).unwrap();
    builder.finish().unwrap()
}
fn encoded(locator: ProjectionSourceLocatorV1, key: &ProjectionGroupKey) -> Vec<u8> {
    let mut out = Vec::with_capacity(source_row_key_len(key).unwrap());
    encode_source_row_key_into(&mut out, locator, key).unwrap();
    out
}

// req: STO-012, PRJ-004
#[test]
fn source_keys_v1_frozen_complete_locator_and_generation_neutral_vectors() {
    let loc = locator(1, 1);
    let object: [u8; 34] = [
        0x4f, 1, 0, 0, 0, 0, 0, 42, 0x70, 1, 0x80, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0,
        0, 0, 0, 0, 0, 0, 1,
    ];
    assert_eq!(source_object_key(loc), object);
    assert_eq!(decode_source_object_key(&object).unwrap(), loc);
    let mut maintenance = object;
    maintenance[0] = 0x4d;
    assert_eq!(source_maintenance_key(loc), maintenance);
    assert_eq!(decode_source_maintenance_key(&maintenance).unwrap(), loc);
    let mut row = object.to_vec();
    row[0] = 0x53;
    row.extend_from_slice(&[0, 0, 0, 3, 1, 1, 1]);
    let key = group(CanonicalValue::Bool(true));
    assert_eq!(encoded(loc, &key), row);
    check_source_row_key(&row, loc, &key).unwrap();
    let mut root = vec![0x52, 1, 0, 0, 0, 1, b'K', 0, 0, 0, 1];
    root.extend_from_slice(&[7; 32]);
    root.extend_from_slice(&object[2..]);
    let mut actual = Vec::with_capacity(source_root_key_len(&identity("K")));
    encode_source_root_key_into(&mut actual, &identity("K"), loc).unwrap();
    assert_eq!(actual, root);
    check_source_root_key(&root, &identity("K"), loc).unwrap();
}

// req: STO-012, PRJ-004
#[test]
fn source_row_keys_preserve_existing_maximal_group_capacity_and_order() {
    let id = identity("K");
    let base = ProjectionGroupKeyBuilder::new(id, ProjectionGeneration::first())
        .as_bytes()
        .len();
    let key = group(CanonicalValue::string("x".repeat(MAX_KEY_BYTES - base - 4 - 6)).unwrap());
    assert_eq!(key.as_bytes().len(), MAX_KEY_BYTES);
    let row = encoded(locator(1, 1), &key);
    assert_eq!(row.len(), MAX_KEY_BYTES - 17);
    check_source_row_key(&row, locator(1, 1), &key).unwrap();
    let z = group(CanonicalValue::string("z").unwrap());
    let aa = group(CanonicalValue::string("aa").unwrap());
    assert!(
        z.as_bytes() < aa.as_bytes(),
        "existing canonical frames order by framed bytes"
    );
    assert!(encoded(locator(1, 1), &z) < encoded(locator(1, 1), &aa));
    assert!(encoded(locator(1, 1), &aa) < encoded(locator(1, 2), &z));
    assert!(encoded(locator(1, 2), &aa) < encoded(locator(2, 1), &z));
}

// req: STO-012, PRJ-004
#[test]
fn source_keys_reject_foreign_context_truncation_trailing_and_unsupported_versions() {
    let key = group(CanonicalValue::Bool(true));
    let loc = locator(1, 1);
    let row = encoded(loc, &key);
    for length in 0..row.len() {
        assert!(check_source_row_key(&row[..length], loc, &key).is_err());
    }
    let mut changed = row.clone();
    changed.push(0);
    assert!(check_source_row_key(&changed, loc, &key).is_err());
    changed = row.clone();
    changed[1] = 2;
    assert!(check_source_row_key(&changed, loc, &key).is_err());
    changed = row.clone();
    changed[0] = 0x47;
    assert!(check_source_row_key(&changed, loc, &key).is_err());
    assert!(check_source_row_key(&row, locator(1, 2), &key).is_err());
    assert!(check_source_row_key(&row, locator(2, 1), &key).is_err());
    let foreign = ProjectionSourceLocatorV1::new(
        DatabaseId::from_unix_milliseconds_and_random(42, [2; 10]).unwrap(),
        1,
        ProjectionCheckpointOrdinal::first(),
    )
    .unwrap();
    assert!(check_source_row_key(&row, foreign, &key).is_err());
    let different = group(CanonicalValue::Bool(false));
    assert!(check_source_row_key(&row, loc, &different).is_err());
    for start in [18, 26] {
        let mut bad = source_object_key(loc);
        bad[start..start + 8].fill(0);
        assert!(decode_source_object_key(&bad).is_err());
    }
    let mut bad = source_object_key(loc);
    bad[8] = 0;
    assert!(decode_source_object_key(&bad).is_err());
    assert!(decode_source_object_key(&row).is_err());
    assert!(decode_source_maintenance_key(&source_object_key(loc)).is_err());
}

// req: STO-012, PRJ-004
#[test]
fn source_root_keys_bind_exact_identity_and_locator_under_bounded_identity_prefix() {
    let id = identity("K");
    let loc = locator(1, 1);
    let mut prefix = Vec::with_capacity(source_root_key_len(&id));
    encode_source_root_identity_prefix_into(&mut prefix, &id).unwrap();
    let mut first = Vec::with_capacity(source_root_key_len(&id));
    let mut second = Vec::with_capacity(source_root_key_len(&id));
    encode_source_root_key_into(&mut first, &id, loc).unwrap();
    encode_source_root_key_into(&mut second, &id, locator(1, 2)).unwrap();
    assert!(first.starts_with(&prefix) && second.starts_with(&prefix));
    assert_ne!(first, second);
    check_source_root_key(&first, &id, loc).unwrap();
    assert!(check_source_root_key(&first, &identity("L"), loc).is_err());
    assert!(check_source_root_key(&first, &id, locator(1, 2)).is_err());
    assert!(check_source_root_key(&first[..first.len() - 1], &id, loc).is_err());
    first.push(0);
    assert!(check_source_root_key(&first, &id, loc).is_err());
    // Bytes/prefixes alone intentionally provide no selected-root capability.
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn source_key_encoders_refuse_insufficient_preadmitted_scratch_without_growth() {
    let key = group(CanonicalValue::Bool(true));
    let loc = locator(1, 1);
    let mut small = Vec::with_capacity(source_row_key_len(&key).unwrap() - 1);
    small.extend_from_slice(&[9, 8, 7]);
    let before = small.clone();
    let capacity = small.capacity();
    assert!(encode_source_row_key_into(&mut small, loc, &key).is_err());
    assert_eq!(small, before);
    assert_eq!(small.capacity(), capacity);
    let id = identity("K");
    let mut root = Vec::with_capacity(source_root_key_len(&id) - 1);
    root.extend_from_slice(&[6, 5, 4]);
    let before = root.clone();
    let capacity = root.capacity();
    assert!(encode_source_root_key_into(&mut root, &id, loc).is_err());
    assert_eq!(root, before);
    assert_eq!(root.capacity(), capacity);
    let mut exact = Vec::with_capacity(source_row_key_len(&key).unwrap());
    let capacity = exact.capacity();
    encode_source_row_key_into(&mut exact, loc, &key).unwrap();
    assert_eq!(exact.capacity(), capacity);
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn source_maximal_group_progress_control_refuses_without_truncating_native_key() {
    use riffdb_storage_api::{
        ChangelogHistoryPointV3, ChangelogHistoryStateV3, ChangelogLineageV3,
        ChangelogTransactionSequence, DurableCodecErrorKind, ProjectionSourceDescriptorPartsV1,
        ProjectionSourceDescriptorV1, ProjectionSourceMaintenanceV1, ProjectionSourceRowV1,
        proto_codec::{encode_projection_source_maintenance_v1, encode_projection_source_row_v1},
    };
    use riffdb_types::{
        CanonicalRecord, CommitSequence, DualFrontier, FieldId, HashDomain, LeadershipEpochV1, hash,
    };
    let base = ProjectionGroupKeyBuilder::new(identity("K"), ProjectionGeneration::first())
        .as_bytes()
        .len();
    let key = group(CanonicalValue::string("x".repeat(MAX_KEY_BYTES - base - 4 - 6)).unwrap());
    let loc = locator(1, 1);
    let native_key = encoded(loc, &key);
    assert_eq!(key.as_bytes().len(), MAX_KEY_BYTES);
    assert_eq!(native_key.len(), 4079);
    // These are structural codec fixtures, never an evaluated native candidate.
    let row = ProjectionSourceRowV1::from_stored_parts(
        identity("K"),
        loc,
        key.components().to_vec(),
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))]).unwrap(),
        CommitSequence::first(),
    )
    .unwrap();
    let value = encode_projection_source_row_v1(&row).unwrap();
    let semantic_bytes = riffdb_projection::projection_source_row_preimage_bytes_v1(&row).unwrap();
    let mut root = riffdb_projection::ProjectionSourceRowRootV1::new(
        row.identity(),
        Some(loc),
        CommitSequence::new(10).unwrap(),
        1,
        semantic_bytes,
    )
    .unwrap();
    let mut scratch = Vec::with_capacity(
        riffdb_projection::projection_source_row_scratch_bytes_v1(&row).unwrap(),
    );
    root.append(&key, &row, &mut scratch).unwrap();
    let row_root = root.finish().unwrap();
    let point = |position, application, byte| {
        ChangelogHistoryPointV3::new(
            ChangelogTransactionSequence::new(position).unwrap(),
            [byte; 32],
            DualFrontier::new(Some(CommitSequence::new(application).unwrap()), None),
        )
    };
    let captured = ChangelogHistoryStateV3::new(
        ChangelogLineageV3::new(loc.database_id(), 1, LeadershipEpochV1::new(1).unwrap()).unwrap(),
        point(1, 5, 7),
        point(3, 10, 9),
        point(2, 7, 8),
    )
    .unwrap();
    let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity: identity("K"),
        locator: loc,
        applied_through: CommitSequence::new(10).unwrap(),
        captured_source: captured,
        origin: hash(HashDomain::ProjectionSourceOrigin, b"codec fixture origin"),
        prior_source_digest: None,
        row_count: 1,
        canonical_row_bytes: (native_key.len() + value.as_bytes().len()) as u64,
        row_root,
    })
    .unwrap();
    let before = ProjectionSourceMaintenanceV1::initial(descriptor, None).unwrap();
    assert!(encode_projection_source_maintenance_v1(&before).is_ok());
    let after = before
        .after_copy_step(&before, &[(&native_key, value.as_bytes())])
        .unwrap();
    assert_eq!(after.last_copied_key(), Some(native_key.as_slice()));
    assert_eq!(after.copied_rows(), 1);
    let error = encode_projection_source_maintenance_v1(&after).unwrap_err();
    assert_eq!(error.kind(), DurableCodecErrorKind::LimitExceeded);
    assert_eq!(after.last_copied_key().unwrap().len(), 4079);
    assert_eq!(
        before.copied_rows(),
        0,
        "preflight has not mutated durable control"
    );
    // The future worker must map this eligibility capacity failure to
    // SourceUnavailable before allocation/copy, retaining selected source and
    // retention holds. This codec proof does not qualify that absent worker.
}

// req: STO-012, PRJ-004
#[test]
fn generation_source_base_key_v1_freezes_exact_identity_and_generation_bytes() {
    let id = identity("K");
    let generation = ProjectionGeneration::new(0x0102_0304_0506_0708).unwrap();
    let mut actual = Vec::with_capacity(generation_source_base_key_len(&id));
    let capacity = actual.capacity();
    encode_generation_source_base_key_into(&mut actual, &id, generation).unwrap();
    let mut expected = vec![0x42, 0x01, 0, 0, 0, 1, b'K', 0, 0, 0, 1];
    expected.extend_from_slice(&[7; 32]);
    expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 51);
    assert_eq!(actual.capacity(), capacity);
    check_generation_source_base_key(&actual, &id, generation).unwrap();
    assert_ne!(actual[..2], SOURCE_ROOT_KEY_V1);
    assert_ne!(actual[..2], SOURCE_ROW_KEY_V1);
    let mut next = Vec::with_capacity(generation_source_base_key_len(&id));
    encode_generation_source_base_key_into(&mut next, &id, generation.checked_next().unwrap())
        .unwrap();
    assert!(actual < next);
}

// req: STO-012, PRJ-004
#[test]
fn generation_source_base_key_refuses_foreign_context_and_unadmitted_growth() {
    let id = identity("K");
    let generation = ProjectionGeneration::first();
    let mut encoded = Vec::with_capacity(generation_source_base_key_len(&id));
    encode_generation_source_base_key_into(&mut encoded, &id, generation).unwrap();
    let mut insufficient = Vec::with_capacity(generation_source_base_key_len(&id) - 1);
    insufficient.extend_from_slice(b"unchanged");
    let before = insufficient.clone();
    let capacity = insufficient.capacity();
    assert_eq!(
        encode_generation_source_base_key_into(&mut insufficient, &id, generation),
        Err(PhysicalKeyError::InvalidLength)
    );
    assert_eq!(insufficient, before);
    assert_eq!(insufficient.capacity(), capacity);
    for len in 0..encoded.len() {
        assert_eq!(
            check_generation_source_base_key(&encoded[..len], &id, generation),
            Err(PhysicalKeyError::InvalidLength)
        );
    }
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        check_generation_source_base_key(&trailing, &id, generation),
        Err(PhysicalKeyError::InvalidLength)
    );
    for (offset, value) in [(0, 0x52), (1, 2)] {
        let mut changed = encoded.clone();
        changed[offset] = value;
        assert_eq!(
            check_generation_source_base_key(&changed, &id, generation),
            Err(PhysicalKeyError::InvalidLength)
        );
    }
    assert_eq!(
        check_generation_source_base_key(&encoded, &identity("J"), generation),
        Err(PhysicalKeyError::NonCanonical)
    );
    for foreign in [
        ProjectionIdentity::new(
            ContractLineage::new("K").unwrap(),
            ProjectionId::new(2).unwrap(),
            ProjectionPlanHash::from_bytes([7; 32]),
        ),
        ProjectionIdentity::new(
            ContractLineage::new("K").unwrap(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([8; 32]),
        ),
    ] {
        assert_eq!(
            check_generation_source_base_key(&encoded, &foreign, generation),
            Err(PhysicalKeyError::NonCanonical)
        );
    }
    assert_eq!(
        check_generation_source_base_key(&encoded, &id, generation.checked_next().unwrap()),
        Err(PhysicalKeyError::NonCanonical)
    );
    let mut zero = encoded.clone();
    let offset = zero.len() - 8;
    zero[offset..].fill(0);
    assert_eq!(
        check_generation_source_base_key(&zero, &id, generation),
        Err(PhysicalKeyError::NonCanonical)
    );
}
