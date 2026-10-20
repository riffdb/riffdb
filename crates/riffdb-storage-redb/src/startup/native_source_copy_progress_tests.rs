//! Structural Copying-prefix fixtures, never native publication evidence.
use super::*;
use riffdb_storage_api::proto_codec::{
    decode_projection_source_object_admitted_v1, encode_projection_source_maintenance_v1,
};
use riffdb_storage_api::{ProjectionSourceLifecycleV1 as Life, ProjectionSourceMaintenanceV1};

#[derive(Clone, Copy)]
enum ProgressCorruption {
    None,
    Hash,
    Count,
    Bytes,
    LastKey,
    Value,
    Missing,
    Deleting,
}
struct CopyFixture {
    fixture: Fixture,
    rows: u32,
    bytes: u64,
    rolling: ContentDigest,
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_copy_progress_visits_initial_partial_and_complete_with_explicit_maintenance_load() {
    for copied in [0, 2, 3] {
        for granted in [1, 256] {
            let CopyFixture {
                fixture,
                rows,
                bytes,
                rolling,
            } = copy_fixture(copied, ProgressCorruption::None);
            let pool = crate::locator_memory::NativeLocatorPool::global();
            let before = pool.charged_bytes();
            let source = fixture
                .session
                .scan
                .shared
                .database
                .source_working_budget()
                .clone();
            let observation = source.observation().unwrap();
            let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - observation.fixed_owner_bytes
                - observation.owned_bytes
                - observation.reserved_bytes;
            let pressure = source.reserve(remaining).unwrap();
            let saturated = source.observation().unwrap();
            let mut visitor = fixture
                .session
                .scan
                .begin_source_copy_progress_visit_v1(fixture.locator)
                .unwrap();
            assert!(pool.charged_bytes() > before);
            let mut visits = 0;
            loop {
                let position = visitor.position();
                let physical = visitor.physical_visits();
                let result = visitor
                    .step(EvidencePageLimit::new(granted).unwrap())
                    .unwrap();
                visits += 1;
                assert_eq!(visitor.position() - position, 1);
                assert_eq!(visitor.physical_visits() - physical, 1);
                assert_eq!(source.observation().unwrap(), saturated);
                if let Some(progress) = result {
                    assert_eq!(progress.copied_rows(), rows);
                    assert_eq!(progress.copied_canonical_row_bytes(), bytes);
                    assert_eq!(progress.rolling_hash(), rolling);
                    assert_eq!(
                        progress.complete_row_root(),
                        if copied == 3 {
                            Some(fixture.expected)
                        } else {
                            None
                        }
                    );
                    break;
                }
                assert!(visits < 16);
            }
            // ObjectLoad, MaintenanceLoad, N Measure rows, MeasureEnd,
            // N Replay rows, ReplayEnd: every control/end is a real visit.
            assert_eq!(visits, 4 + 2 * copied);
            assert_eq!(visitor.position(), u64::from(visits));
            drop(visitor);
            assert_eq!(pool.charged_bytes(), before);
            drop(pressure);
        }
    }
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_copy_progress_refuses_wrong_progress_missing_control_and_unimplemented_deletion() {
    for corruption in [
        ProgressCorruption::Hash,
        ProgressCorruption::Count,
        ProgressCorruption::Bytes,
        ProgressCorruption::LastKey,
        ProgressCorruption::Value,
        ProgressCorruption::Missing,
        ProgressCorruption::Deleting,
    ] {
        let CopyFixture { fixture, .. } = copy_fixture(2, corruption);
        let pool = crate::locator_memory::NativeLocatorPool::global();
        let before = pool.charged_bytes();
        let mut visitor = fixture
            .session
            .scan
            .begin_source_copy_progress_visit_v1(fixture.locator)
            .unwrap();
        let mut refused = false;
        for _ in 0..16 {
            match visitor.step(EvidencePageLimit::new(256).unwrap()) {
                Err(error) => {
                    assert_eq!(
                        error.kind(),
                        if matches!(corruption, ProgressCorruption::Deleting) {
                            StorageErrorKind::Unavailable
                        } else {
                            StorageErrorKind::CorruptData
                        }
                    );
                    refused = true;
                    break;
                }
                Ok(None) => {}
                Ok(Some(_)) => panic!("bad progress cannot complete"),
            }
        }
        assert!(refused);
        drop(visitor);
        assert_eq!(pool.charged_bytes(), before);
    }
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_copy_progress_cancellation_releases_expected_and_computed_progress() {
    let CopyFixture { mut fixture, .. } = copy_fixture(2, ProgressCorruption::None);
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    fixture.session.scan.cancellation = Some(cancelled.clone());
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let before = pool.charged_bytes();
    let mut visitor = fixture
        .session
        .scan
        .begin_source_copy_progress_visit_v1(fixture.locator)
        .unwrap();
    // Cancel after ObjectLoad, MaintenanceLoad, measurement and first replay.
    for _ in 0..6 {
        assert!(
            visitor
                .step(EvidencePageLimit::new(1).unwrap())
                .unwrap()
                .is_none()
        );
    }
    cancelled.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        visitor
            .step(EvidencePageLimit::new(1).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    drop(visitor);
    assert_eq!(pool.charged_bytes(), before);
}

fn copy_fixture(copied: u32, corruption: ProgressCorruption) -> CopyFixture {
    copy_fixture_with_public_control(copied, corruption, false)
}
fn copy_fixture_with_public_control(
    copied: u32,
    corruption: ProgressCorruption,
    public_before_injection: bool,
) -> CopyFixture {
    let Fixture {
        session,
        locator,
        expected,
        _root,
    } = fixture_with_public_scrub_control(Corruption::None, public_before_injection);
    drop(session);
    let mut retired = false;
    let pool = crate::locator_memory::NativeLocatorPool::global();
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let store = RedbStore::open(_root.join("db.redb")).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let object_table = read
        .open_table(source_validation::definition(1).unwrap())
        .unwrap();
    let raw_object = object_table
        .get(crate::source_keys::source_object_key(locator).as_slice())
        .unwrap()
        .unwrap();
    let object = decode_projection_source_object_admitted_v1(
        raw_object.value(),
        store.shared.database.source_working_budget(),
    )
    .unwrap();
    let descriptor = object.value().descriptor().clone();
    let table = read
        .open_table(source_validation::definition(0).unwrap())
        .unwrap();
    let rows = table
        .iter()
        .unwrap()
        .map(|row| {
            let (k, v) = row.unwrap();
            (k.value().to_vec(), v.value().to_vec())
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 3);
    let initial = ProjectionSourceMaintenanceV1::initial(descriptor.clone(), None).unwrap();
    let pairs = rows[..copied as usize]
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect::<Vec<_>>();
    let canonical = if copied == 0 {
        initial
    } else {
        initial.after_copy_step(&initial, &pairs).unwrap()
    };
    let expected_rows = canonical.copied_rows();
    let expected_bytes = canonical.copied_canonical_row_bytes();
    let expected_rolling = canonical.rolling_hash();
    let progress = match corruption {
        ProgressCorruption::Hash => ProjectionSourceMaintenanceV1::from_stored_parts(
            descriptor.clone(),
            None,
            canonical.copied_rows(),
            canonical.copied_canonical_row_bytes(),
            canonical.last_copied_key().map(<[u8]>::to_vec),
            hash(
                HashDomain::ProjectionSourceCopyProgress,
                b"wrong structural progress",
            ),
        )
        .unwrap(),
        ProgressCorruption::Count => {
            let start = ProjectionSourceMaintenanceV1::initial(descriptor.clone(), None).unwrap();
            start.after_copy_step(&start, &pairs[..1]).unwrap()
        }
        ProgressCorruption::Bytes => ProjectionSourceMaintenanceV1::from_stored_parts(
            descriptor.clone(),
            None,
            canonical.copied_rows(),
            canonical.copied_canonical_row_bytes() + 1,
            canonical.last_copied_key().map(<[u8]>::to_vec),
            canonical.rolling_hash(),
        )
        .unwrap(),
        ProgressCorruption::LastKey => ProjectionSourceMaintenanceV1::from_stored_parts(
            descriptor.clone(),
            None,
            canonical.copied_rows(),
            canonical.copied_canonical_row_bytes(),
            Some(rows[0].0.clone()),
            canonical.rolling_hash(),
        )
        .unwrap(),
        _ => canonical,
    };
    let object = encode_projection_source_object_v1(&ProjectionSourceObjectV1::new(
        descriptor,
        if matches!(corruption, ProgressCorruption::Deleting) {
            Life::Aborting
        } else {
            Life::Copying
        },
    ))
    .unwrap();
    let progress = encode_projection_source_maintenance_v1(&progress).unwrap();
    drop(raw_object);
    drop(object_table);
    drop(table);
    drop(read);
    let write = store.shared.database.begin_write().unwrap();
    {
        let mut table = write
            .open_table(source_validation::definition(1).unwrap())
            .unwrap();
        table
            .insert(
                crate::source_keys::source_object_key(locator).as_slice(),
                object.as_bytes(),
            )
            .unwrap();
    }
    if !matches!(corruption, ProgressCorruption::Missing) {
        let mut table = write
            .open_table(source_validation::definition(3).unwrap())
            .unwrap();
        table
            .insert(
                crate::source_keys::source_maintenance_key(locator).as_slice(),
                progress.as_bytes(),
            )
            .unwrap();
    }
    {
        let mut table = write
            .open_table(source_validation::definition(0).unwrap())
            .unwrap();
        for (key, _) in &rows[copied as usize..] {
            table.remove(key.as_slice()).unwrap();
        }
        if matches!(corruption, ProgressCorruption::Value) {
            // Copy a different canonical row value into the same native key.
            // Semantic key/value validation must reject it before progress success.
            table
                .insert(rows[1].0.as_slice(), rows[0].1.as_slice())
                .unwrap();
        }
    }
    write.commit().unwrap();
    let session = store
        .begin_structural_evidence_for(inputs(), EvidenceOpenPurpose::OfflineIntegrityScrub)
        .unwrap();
    CopyFixture {
        fixture: Fixture {
            session,
            locator,
            expected,
            _root,
        },
        rows: expected_rows,
        bytes: expected_bytes,
        rolling: expected_rolling,
    }
}

// Test-only access to the existing canonical Copying fixture; no new producer.
pub(super) fn public_copy_fixture(copied: u32) -> Fixture {
    copy_fixture(copied, ProgressCorruption::None).fixture
}

pub(super) fn public_copy_fixture_with_real_positive(copied: u32) -> Fixture {
    copy_fixture_with_public_control(copied, ProgressCorruption::None, true).fixture
}
