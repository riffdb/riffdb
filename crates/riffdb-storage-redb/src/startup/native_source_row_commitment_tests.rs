//! Structural row-commitment visits on one actual native pin.
//! Injected Source records are not coordinator publication/evaluation fixtures.
//! This component cannot mint whole-Source ExactEnd or application authority.
use super::*;
use riffdb_storage_api::proto_codec::{
    encode_projection_source_object_v1, encode_projection_source_row_v1,
};
use riffdb_storage_api::{
    ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1, ProjectionSourceLifecycleV1,
    ProjectionSourceObjectV1, ProjectionSourceRowV1,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContentDigest, FieldId, HashDomain,
    ProjectionCheckpointOrdinal, ProjectionGeneration, ProjectionId, ProjectionSourceLocatorV1,
    hash,
};

struct Fixture {
    session: RedbStructuralEvidenceSession,
    locator: ProjectionSourceLocatorV1,
    expected: ContentDigest,
    _root: crate::test_path::ScopedDirectory,
}
#[derive(Clone, Copy)]
enum Corruption {
    None,
    Root,
    Count,
    Charge,
    Key,
    Value,
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_row_commitment_visit_is_bounded_multipage_and_matches_independent_frame() {
    let fixture = fixture(Corruption::None);
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
    // The method is intentionally absent in this compile-RED proposal.
    // GREEN supplies only bounded row commitments, never Source authority.
    let mut visitor = fixture
        .session
        .scan
        .begin_source_row_commitment_visit_v1(fixture.locator)
        .unwrap();
    assert!(
        pool.charged_bytes() > before,
        "visitor control is admitted before construction"
    );
    let limit = EvidencePageLimit::new(1).unwrap();
    let mut pages = 0;
    loop {
        let position = visitor.position();
        let visits = visitor.physical_visits();
        let result = visitor.step(limit).unwrap();
        pages += 1;
        assert!(
            visitor.position() > position,
            "every page advances the work cursor"
        );
        assert_eq!(visitor.position() - position, 1);
        assert_eq!(
            visitor.physical_visits() - visits,
            1,
            "limit one means one row/control visit"
        );
        assert_eq!(
            source.observation().unwrap(),
            saturated,
            "no worker-ledger dependency"
        );
        if let Some(root) = result {
            assert_eq!(root, fixture.expected);
            break;
        }
        assert!(pages < 16, "bounded finite fixture traversal");
    }
    // ObjectLoad, three measured rows, MeasureEnd, three replayed rows,
    // ReplayEnd. Both exhaustion checks are explicitly charged visits.
    assert_eq!(pages, 9);
    assert_eq!(visitor.position(), 9);
    assert_eq!(visitor.physical_visits(), 9);
    drop(visitor);
    assert_eq!(pool.charged_bytes(), before);
    drop(pressure);
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_row_commitment_cancellation_releases_physical_work() {
    let mut fixture = fixture(Corruption::None);
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    fixture.session.scan.cancellation = Some(cancelled.clone());
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let before = pool.charged_bytes();
    let mut visitor = fixture
        .session
        .scan
        .begin_source_row_commitment_visit_v1(fixture.locator)
        .unwrap();
    assert!(
        visitor
            .step(EvidencePageLimit::new(1).unwrap())
            .unwrap()
            .is_none()
    );
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

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_row_commitment_refuses_wrong_root_count_charge_key_and_value() {
    for corruption in [
        Corruption::Root,
        Corruption::Count,
        Corruption::Charge,
        Corruption::Key,
        Corruption::Value,
    ] {
        let fixture = fixture(corruption);
        let pool = crate::locator_memory::NativeLocatorPool::global();
        let before = pool.charged_bytes();
        let mut visitor = fixture
            .session
            .scan
            .begin_source_row_commitment_visit_v1(fixture.locator)
            .unwrap();
        let mut failed = false;
        for _ in 0..16 {
            match visitor.step(EvidencePageLimit::new(1).unwrap()) {
                Err(error) => {
                    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
                    failed = true;
                    break;
                }
                Ok(None) => {}
                Ok(Some(_)) => panic!("a malformed row commitment cannot complete"),
            }
        }
        assert!(
            failed,
            "wrong root/count/charge/key/value must refuse in bounded work"
        );
        drop(visitor);
        assert_eq!(pool.charged_bytes(), before);
    }
}

fn fixture(corruption: Corruption) -> Fixture {
    fixture_with_public_scrub_control(corruption, false)
}
fn fixture_with_public_scrub_control(
    corruption: Corruption,
    public_before_injection: bool,
) -> Fixture {
    let root = crate::test_path::ScopedDirectory::new("source-row-commitment-structural");
    let path = root.join("db.redb");
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .unwrap();
    let mut store = RedbStore::open(&path).unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let mut startup = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut startup);
    let (_, historical) = startup
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let StructuralOpenOutcome::Clean(opened) = startup.finish(structural, historical).unwrap()
    else {
        panic!("actual Fresh initialization must validate")
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    native_source_command_fixture::activate_source_projection(&mut ports).unwrap();
    native_source_command_fixture::append_source_projection_journaled(&ports, 1).unwrap();
    assert!(
        ports
            .complete_graceful_close()
            .disposition()
            .is_successful()
    );
    drop(ports);
    // Join the bounded old-owner physical retirement before taking numerical
    // baselines. No sleeps and no unrelated asynchronous release can masquerade
    // as the visitor releasing its own charge.
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(
        retired,
        "one-command fixture retirement must finish within its bound"
    );
    if public_before_injection {
        let before = RedbOfflineIntegrityScrub::from_inputs(&path, inputs())
            .run()
            .unwrap();
        assert!(before.structural_exact_end() && before.catalog_exact_end());
        assert_eq!(before.authoritative_mutations(), 0);
        assert_eq!(before.lifecycle_mutations(), 0);
        eprintln!("source_public_commitment_real_runtime_positive_before_injection_passed");
        // Joined numerical retirement only, never sleeps or opening around a
        // live alias. The result above owns no database/validation handle.
        let mut complete = false;
        for _ in 0..256 {
            if !pool.drain_retired_step() {
                complete = true;
                break;
            }
        }
        assert!(complete);
    }
    let store = RedbStore::open(&path).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)
        .unwrap()
        .unwrap();
    drop(read);
    let bundle = native_source_command_fixture::source_projection_bundle();
    let schema = riffdb_storage_api::CheckedProjectionSchema::new(
        bundle
            .bundle()
            .bound_projection_group_schema(ProjectionId::first())
            .unwrap(),
    );
    let locator = ProjectionSourceLocatorV1::new(
        database,
        history.lineage().history_incarnation(),
        ProjectionCheckpointOrdinal::new(7).unwrap(),
    )
    .unwrap();
    let frontier = history.tail().frontier().application().unwrap();
    let rows = (1..=3_u64)
        .map(|group| {
            let key = schema
                .group_key(ProjectionGeneration::first(), &[CanonicalValue::U64(group)])
                .unwrap();
            let row = ProjectionSourceRowV1::new(
                &schema,
                locator,
                frontier,
                key.clone(),
                CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(group * 10))])
                    .unwrap(),
                frontier,
            )
            .unwrap();
            (key, row)
        })
        .collect::<Vec<_>>();
    let expected = independent_root(schema.identity(), frontier, &rows);
    let mut encoded = Vec::new();
    let mut actual_bytes = 0_u64;
    for (index, (group, row)) in rows.iter().enumerate() {
        let mut key = Vec::with_capacity(crate::source_keys::source_row_key_len(group).unwrap());
        let native_group = if matches!(corruption, Corruption::Key) && index == 1 {
            schema
                .group_key(ProjectionGeneration::first(), &[CanonicalValue::U64(52)])
                .unwrap()
        } else {
            group.clone()
        };
        crate::source_keys::encode_source_row_key_into(&mut key, locator, &native_group).unwrap();
        let value = if matches!(corruption, Corruption::Value) && index == 1 {
            let altered = ProjectionSourceRowV1::new(
                &schema,
                locator,
                frontier,
                group.clone(),
                CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(21))]).unwrap(),
                frontier,
            )
            .unwrap();
            encode_projection_source_row_v1(&altered).unwrap()
        } else {
            encode_projection_source_row_v1(row).unwrap()
        };
        actual_bytes += u64::try_from(key.len() + value.as_bytes().len()).unwrap();
        encoded.push((key, value));
    }
    let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity: schema.identity().clone(),
        locator,
        applied_through: frontier,
        captured_source: history,
        origin: hash(
            HashDomain::ProjectionSourceOrigin,
            b"nonpublication structural row-commitment fixture",
        ),
        prior_source_digest: None,
        row_count: if matches!(corruption, Corruption::Count) {
            4
        } else {
            3
        },
        canonical_row_bytes: actual_bytes + u64::from(matches!(corruption, Corruption::Charge)),
        row_root: if matches!(corruption, Corruption::Root) {
            hash(HashDomain::ProjectionSourceRows, b"wrong structural root")
        } else {
            expected
        },
    })
    .unwrap();
    // Test-only structural records: no selected Root, publication receipt,
    // allocated ordinal or successful native Source publication is fabricated.
    let object = ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Published);
    let object = encode_projection_source_object_v1(&object).unwrap();
    let transaction = store.shared.database.begin_write().unwrap();
    {
        let mut table = transaction
            .open_table(TableDefinition::<&[u8], &[u8]>::new(
                riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceRows.table(),
            ))
            .unwrap();
        for (key, value) in &encoded {
            table.insert(key.as_slice(), value.as_bytes()).unwrap();
        }
    }
    {
        let mut table = transaction
            .open_table(TableDefinition::<&[u8], &[u8]>::new(
                riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceObjects.table(),
            ))
            .unwrap();
        table
            .insert(
                crate::source_keys::source_object_key(locator).as_slice(),
                object.as_bytes(),
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let session = store
        .begin_structural_evidence_for(inputs(), EvidenceOpenPurpose::OfflineIntegrityScrub)
        .unwrap();
    Fixture {
        session,
        locator,
        expected,
        _root: root,
    }
}

fn independent_root(
    identity: &riffdb_types::ProjectionIdentity,
    frontier: CommitSequence,
    rows: &[(riffdb_types::ProjectionGroupKey, ProjectionSourceRowV1)],
) -> ContentDigest {
    let mut preimage = Vec::new();
    preimage.extend_from_slice(&1_u32.to_be_bytes());
    let identity = identity.to_canonical_bytes();
    preimage.extend_from_slice(&(identity.len() as u32).to_be_bytes());
    preimage.extend_from_slice(&identity);
    preimage.extend_from_slice(&frontier.to_be_bytes());
    let mut semantic_bytes = 0_u64;
    for (_, row) in rows {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(row.group_values().len() as u32).to_be_bytes());
        for group in row.group_values() {
            let group = riffdb_types::encode_canonical_value(group).unwrap();
            payload.extend_from_slice(&(group.len() as u64).to_be_bytes());
            payload.extend_from_slice(&group);
        }
        let measures =
            riffdb_types::encode_canonical_value(&CanonicalValue::Record(row.measures().clone()))
                .unwrap();
        payload.extend_from_slice(&(measures.len() as u64).to_be_bytes());
        payload.extend_from_slice(&measures);
        payload.extend_from_slice(&row.last_changed_sequence().to_be_bytes());
        semantic_bytes += 8 + payload.len() as u64;
        preimage.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        preimage.extend_from_slice(&payload);
    }
    preimage.extend_from_slice(&(rows.len() as u32).to_be_bytes());
    preimage.extend_from_slice(&semantic_bytes.to_be_bytes());
    hash(HashDomain::ProjectionSourceRows, &preimage)
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn source_row_commitment_large_page_limit_still_performs_one_visit() {
    let fixture = fixture(Corruption::None);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let before = pool.charged_bytes();
    let mut visitor = fixture
        .session
        .scan
        .begin_source_row_commitment_visit_v1(fixture.locator)
        .unwrap();
    for index in 0..9 {
        let position = visitor.position();
        let visits = visitor.physical_visits();
        let result = visitor.step(EvidencePageLimit::new(256).unwrap()).unwrap();
        assert_eq!(visitor.position() - position, 1);
        assert_eq!(
            visitor.physical_visits() - visits,
            1,
            "large caller limit cannot hide multiple Source input visits"
        );
        if index == 8 {
            assert_eq!(result, Some(fixture.expected));
        } else {
            assert!(result.is_none());
        }
    }
    drop(visitor);
    assert_eq!(pool.charged_bytes(), before);
}

#[path = "native_source_copy_progress_tests.rs"]
mod copy_progress;

#[path = "native_source_public_commitment_tests.rs"]
mod public_commitment;
