//! Structural row/copy proofs; these values cannot grant publication authority.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{CanonicalRecord, CanonicalValue, FieldId, ProjectionGroupKeyBuilder};

use super::*;

fn schema() -> crate::CheckedProjectionSchema {
    let bundle = compile_contract_source("contract SourceProof version 1 { event Input { group: string<32> } projection Totals { source event Input key (group) measure total = count() frontier transactionally_ordered } }").unwrap();
    crate::CheckedProjectionSchema::new(
        bundle
            .bound_projection_group_schema(ProjectionId::first())
            .unwrap(),
    )
}

fn measures() -> CanonicalRecord {
    CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(7))]).unwrap()
}

fn descriptor(rows: u32, bytes: u64) -> ProjectionSourceDescriptorV1 {
    let mut p = parts();
    p.row_count = rows;
    p.canonical_row_bytes = bytes;
    ProjectionSourceDescriptorV1::new(p).unwrap()
}

// req: PRJ-004, REC-003
#[test]
fn source_row_discards_generation_but_requires_exact_plan_locator_and_frontier() {
    let schema = schema();
    let locator = parts().locator;
    let value = CanonicalValue::string("private-group").unwrap();
    let first = schema
        .group_key(ProjectionGeneration::first(), std::slice::from_ref(&value))
        .unwrap();
    let later = schema
        .group_key(
            ProjectionGeneration::new(91).unwrap(),
            std::slice::from_ref(&value),
        )
        .unwrap();
    let row = ProjectionSourceRowV1::new(
        &schema,
        locator,
        sequence(10),
        first,
        measures(),
        sequence(7),
    )
    .unwrap();
    let same = ProjectionSourceRowV1::new(
        &schema,
        locator,
        sequence(10),
        later,
        measures(),
        sequence(7),
    )
    .unwrap();
    assert_eq!(
        row, same,
        "source rows carry neither derived generation nor fabricated marker"
    );
    assert_eq!(row.identity(), schema.identity());
    assert_eq!(row.last_changed_sequence(), sequence(7));
    assert_eq!(row.measures(), &measures());
    assert!(!format!("{row:?}").contains("private-group"));
    let wrong_locator = ProjectionSourceLocatorV1::new(
        locator.database_id(),
        locator.allocation_history_incarnation(),
        ProjectionCheckpointOrdinal::new(2).unwrap(),
    )
    .unwrap();
    assert_eq!(
        row.clone()
            .into_checked(&schema, wrong_locator, sequence(10)),
        Err(StorageValueError::IdentityMismatch)
    );
    assert_eq!(
        row.clone().into_checked(&schema, locator, sequence(6)),
        Err(StorageValueError::InvalidShape)
    );
    let mut foreign = ProjectionGroupKeyBuilder::new(identity(3), ProjectionGeneration::first());
    foreign.push_component(value.clone()).unwrap();
    assert_eq!(
        ProjectionSourceRowV1::new(
            &schema,
            locator,
            sequence(10),
            foreign.finish().unwrap(),
            measures(),
            sequence(7)
        ),
        Err(StorageValueError::IdentityMismatch)
    );
    let malformed = ProjectionSourceRowV1::from_stored_parts(
        schema.identity().clone(),
        locator,
        vec![CanonicalValue::U64(1)],
        measures(),
        sequence(7),
    )
    .unwrap();
    assert_eq!(
        malformed.into_checked(&schema, locator, sequence(10)),
        Err(StorageValueError::InvalidShape)
    );
    assert!(
        ProjectionSourceRowV1::from_stored_parts(
            schema.identity().clone(),
            locator,
            vec![CanonicalValue::Null],
            measures(),
            sequence(7)
        )
        .is_err()
    );
    let owned_key = schema
        .group_key(ProjectionGeneration::new(9).unwrap(), &[value])
        .unwrap();
    let values_pointer = owned_key.components().as_ptr();
    let (owned_identity, generation, components) = owned_key.into_parts();
    assert_eq!(&owned_identity, schema.identity());
    assert_eq!(generation.get(), 9);
    assert_eq!(
        components.as_ptr(),
        values_pointer,
        "consumption transfers the allocation without cloning it"
    );
}

// req: PRJ-004, STO-020
#[test]
fn source_row_preflight_covers_validation_scratch_before_construction() {
    let schema = schema();
    let locator = parts().locator;
    let key = schema
        .group_key(
            ProjectionGeneration::first(),
            &[CanonicalValue::string("group").unwrap()],
        )
        .unwrap();
    let measures = measures();
    let bound = ProjectionSourceRowV1::construction_work_bound_v1(&key, &measures).unwrap();
    assert!(
        bound
            >= riffdb_types::canonical_record_encoded_len(&measures).unwrap() as u64
                + std::mem::size_of::<ProjectionSourceRowV1>() as u64
    );
    assert!(bound < MAX_PROJECTION_SOURCE_WORKING_BYTES);
    let row =
        ProjectionSourceRowV1::new(&schema, locator, sequence(10), key, measures, sequence(7))
            .unwrap();
    let cloned = row.clone();
    // This inspection is test evidence only: production reserves the bound
    // before cloning, including the inspector's separately admitted scratch.
    let observed = riffdb_types::OwnedCapacityInspectionV1::inspect(&cloned).unwrap();
    let observed_heap = observed
        .shared_allocations()
        .iter()
        .try_fold(observed.unshared_bytes(), |bytes, allocation| {
            bytes.checked_add(allocation.bytes())
        })
        .unwrap();
    assert!(
        row.clone_work_bound_v1().unwrap()
            >= observed_heap + std::mem::size_of::<ProjectionSourceRowV1>() as u64
    );
    let nested_measures = CanonicalRecord::new(vec![(
        FieldId::first(),
        CanonicalValue::list(vec![
            CanonicalValue::string("nested".repeat(128)).unwrap(),
            CanonicalValue::bytes(vec![7; 2048]).unwrap(),
            CanonicalValue::record(vec![(
                FieldId::first(),
                CanonicalValue::list(vec![CanonicalValue::U64(1); 17]).unwrap(),
            )])
            .unwrap(),
        ])
        .unwrap(),
    )])
    .unwrap();
    let nested = ProjectionSourceRowV1::from_stored_parts(
        schema.identity().clone(),
        locator,
        vec![CanonicalValue::string("nested-group").unwrap()],
        nested_measures,
        sequence(7),
    )
    .unwrap();
    let nested_clone = nested.clone();
    let observed = riffdb_types::OwnedCapacityInspectionV1::inspect(&nested_clone).unwrap();
    let observed_heap = observed
        .shared_allocations()
        .iter()
        .try_fold(observed.unshared_bytes(), |bytes, allocation| {
            bytes.checked_add(allocation.bytes())
        })
        .unwrap();
    assert!(
        nested.clone_work_bound_v1().unwrap()
            >= observed_heap + std::mem::size_of::<ProjectionSourceRowV1>() as u64,
        "typed nested collection and payload ownership exceeds a scalar encoded-length proxy"
    );
    let source = descriptor(3, 18);
    let observed = riffdb_types::OwnedCapacityInspectionV1::inspect(&source).unwrap();
    let observed_heap = observed
        .shared_allocations()
        .iter()
        .try_fold(observed.unshared_bytes(), |bytes, allocation| {
            bytes.checked_add(allocation.bytes())
        })
        .unwrap();
    assert!(
        source.clone_work_bound_v1().unwrap()
            >= observed_heap + std::mem::size_of::<ProjectionSourceDescriptorV1>() as u64
    );
}

// req: PRJ-004, REC-003
#[test]
fn source_copy_progress_is_per_row_partition_independent_and_binds_changed_bytes() {
    let initial = ProjectionSourceMaintenanceV1::initial(descriptor(3, 18), None).unwrap();
    let rows: [(&[u8], &[u8]); 3] = [(b"a", b"11111"), (b"b", b"22222"), (b"c", b"33333")];
    let together = initial.after_copy_step(&initial, &rows).unwrap();
    let first = initial.after_copy_step(&initial, &rows[..1]).unwrap();
    let partitioned = first.after_copy_step(&first, &rows[1..]).unwrap();
    assert_eq!(together, partitioned);
    assert!(together.copy_complete());
    assert_eq!(together.copied_rows(), 3);
    assert_eq!(together.copied_canonical_row_bytes(), 18);
    assert_eq!(together.last_copied_key(), Some(b"c".as_slice()));
    assert_eq!(
        together.rolling_hash().domain(),
        HashDomain::ProjectionSourceCopyProgress
    );
    assert_ne!(
        together.rolling_hash().domain(),
        together.descriptor().row_root().domain()
    );
    let changed = initial
        .after_copy_step(
            &initial,
            &[(b"a", b"11111"), (b"b", b"22223"), (b"c", b"33333")],
        )
        .unwrap();
    assert_ne!(together.rolling_hash(), changed.rolling_hash());
    assert_eq!(
        initial.after_copy_step(&initial, &[(b"b", b"22222"), (b"a", b"11111")]),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        first.after_copy_step(&first, &rows[..1]),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(
        first.after_copy_step(&initial, &rows[1..]),
        Err(StorageValueError::InvalidShape),
        "stale progress cannot advance even an apparently compatible retry"
    );
    assert_eq!(initial.copied_rows(), 0);
    assert_eq!(first.copied_rows(), 1);
}

// req: PRJ-004, REC-001, REC-003
#[test]
fn source_copy_progress_binds_exact_prior_root_and_immutable_candidate() {
    let predecessor_descriptor = descriptor(3, 18);
    let tail = predecessor_descriptor.captured_source().tail();
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: predecessor_descriptor.reference().locator().database_id(),
        history_incarnation: 3,
        predecessor: Some(tail.sequence()),
        sequence: tail.sequence().checked_next().unwrap(),
        predecessor_frontier: tail.frontier(),
        covered_frontier: tail.frontier(),
        prior_history_hash: tail.history_hash(),
    };
    let prior =
        ProjectionSourceRootV1::new(predecessor_descriptor.reference().clone(), binding, None)
            .unwrap();
    let mut candidate = parts();
    candidate.locator = ProjectionSourceLocatorV1::new(
        candidate.locator.database_id(),
        3,
        ProjectionCheckpointOrdinal::new(2).unwrap(),
    )
    .unwrap();
    candidate.prior_source_digest = Some(prior.reference().digest());
    let candidate = ProjectionSourceDescriptorV1::new(candidate).unwrap();
    let initial =
        ProjectionSourceMaintenanceV1::initial(candidate.clone(), Some(prior.clone())).unwrap();
    assert_eq!(initial.expected_prior_root(), Some(&prior));
    assert_eq!(initial.source_fence(), candidate.captured_source());
    assert!(ProjectionSourceMaintenanceV1::initial(candidate.clone(), None).is_err());
    assert!(
        ProjectionSourceMaintenanceV1::initial(descriptor(3, 18), Some(prior.clone())).is_err()
    );
    let changed_prior = ProjectionSourceRootV1::new(
        prior.reference().clone(),
        AuthoritativeTransactionBindingV3 {
            prior_history_hash: [88; 32],
            ..binding
        },
        None,
    )
    .unwrap();
    let changed = ProjectionSourceMaintenanceV1::initial(candidate, Some(changed_prior)).unwrap();
    assert_ne!(
        initial.rolling_hash(),
        changed.rolling_hash(),
        "the exact expected publication binding is immutable candidate custody"
    );
    let mut different_candidate = parts();
    different_candidate.row_count = 2;
    let different = ProjectionSourceMaintenanceV1::initial(
        ProjectionSourceDescriptorV1::new(different_candidate).unwrap(),
        None,
    )
    .unwrap();
    assert_ne!(initial.rolling_hash(), different.rolling_hash());
}

// req: PRJ-004, STO-020
#[test]
fn source_copy_step_limits_are_exact_and_refuse_without_mutating_progress() {
    let maximum = MAX_PROJECTION_SOURCE_STEP_BYTES as usize;
    let value = vec![0x61; maximum / 8 - 1];
    let keys = [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h"];
    let rows: Vec<(&[u8], &[u8])> = keys
        .iter()
        .map(|key| (key.as_slice(), value.as_slice()))
        .collect();
    let initial =
        ProjectionSourceMaintenanceV1::initial(descriptor(8, maximum as u64), None).unwrap();
    assert!(
        initial
            .after_copy_step(&initial, &rows)
            .unwrap()
            .copy_complete()
    );
    let over_value = vec![0x61; maximum / 8];
    let mut over_rows = rows.clone();
    over_rows[7].1 = &over_value;
    let over =
        ProjectionSourceMaintenanceV1::initial(descriptor(8, maximum as u64 + 1), None).unwrap();
    assert_eq!(
        over.after_copy_step(&over, &over_rows),
        Err(StorageValueError::LimitExceeded)
    );
    let keys: Vec<[u8; 2]> = (0..129_u16).map(u16::to_be_bytes).collect();
    let pairs: Vec<(&[u8], &[u8])> = keys
        .iter()
        .map(|key| (key.as_slice(), b"x".as_slice()))
        .collect();
    let count = ProjectionSourceMaintenanceV1::initial(descriptor(129, 387), None).unwrap();
    assert_eq!(
        count.after_copy_step(&count, &pairs),
        Err(StorageValueError::LimitExceeded)
    );
    assert!(count.after_copy_step(&count, &pairs[..128]).is_ok());
    assert_eq!(
        initial.after_copy_step(&initial, &[]),
        Err(StorageValueError::InvalidShape)
    );
    assert_eq!(initial.copied_rows(), 0);
    assert_eq!(initial.last_copied_key(), None);
    let post = count.after_copy_step(&count, &pairs[..128]).unwrap();
    let observed = riffdb_types::OwnedCapacityInspectionV1::inspect(&post).unwrap();
    let observed_heap = observed
        .shared_allocations()
        .iter()
        .try_fold(observed.unshared_bytes(), |bytes, allocation| {
            bytes.checked_add(allocation.bytes())
        })
        .unwrap();
    assert!(
        count.copy_step_work_bound_v1().unwrap()
            >= observed_heap + std::mem::size_of::<ProjectionSourceMaintenanceV1>() as u64
    );
    assert!(initial.copy_step_work_bound_v1().unwrap() < MAX_PROJECTION_SOURCE_WORKING_BYTES);
}

// req: PRJ-004, REC-003
#[test]
fn decoded_source_copy_progress_requires_consistent_counts_key_charge_and_hash_role() {
    let source_descriptor = descriptor(3, 18);
    let initial = ProjectionSourceMaintenanceV1::initial(source_descriptor.clone(), None).unwrap();
    let reconstruct = |rows, bytes, key, digest| {
        ProjectionSourceMaintenanceV1::from_stored_parts(
            source_descriptor.clone(),
            None,
            rows,
            bytes,
            key,
            digest,
        )
    };
    assert_eq!(
        reconstruct(0, 0, None, initial.rolling_hash()).unwrap(),
        initial
    );
    for (rows, bytes, key) in [
        (0, 0, Some(vec![1])),
        (1, 0, Some(vec![1])),
        (1, 6, None),
        (4, 18, Some(vec![1])),
        (3, 17, Some(vec![1])),
        (2, 18, Some(vec![1])),
    ] {
        assert!(reconstruct(rows, bytes, key, initial.rolling_hash()).is_err());
    }
    assert!(reconstruct(1, 6, Some(Vec::new()), initial.rolling_hash()).is_err());
    assert!(reconstruct(1, 6, Some(vec![1; 4097]), initial.rolling_hash()).is_err());
    assert!(
        reconstruct(
            0,
            0,
            None,
            hash(HashDomain::ProjectionSourceRows, b"wrong role")
        )
        .is_err()
    );
    assert!(
        reconstruct(
            0,
            0,
            None,
            hash(
                HashDomain::ProjectionSourceCopyProgress,
                b"wrong initial candidate"
            )
        )
        .is_err()
    );
    assert!(
        ProjectionSourceMaintenanceV1::initial(descriptor(0, 0), None)
            .unwrap()
            .copy_complete()
    );
}
