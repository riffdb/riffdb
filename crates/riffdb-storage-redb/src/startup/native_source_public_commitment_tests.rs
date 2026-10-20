//! Public scrub must consume canonical commitment checks, not just row syntax.
//! Injected bytes are deliberate corruption; no valid publication is fabricated.
use super::*;

// req: PRJ-004, STO-020, REC-001, REC-005
#[test]
fn public_full_scrub_rejects_false_source_commitment_after_real_runtime_positive_control() {
    let Fixture { session, _root, .. } = fixture_with_public_scrub_control(Corruption::Root, true);
    // The actual same database passed the public full validator before this
    // test-only canonical Source graph with a deliberately false root was added.
    // It never claims a legitimate allocated/published checkpoint.
    drop(session);
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
        "retained scan engine must retire before public reopening"
    );
    let result = RedbOfflineIntegrityScrub::from_inputs(&_root.join("db.redb"), inputs()).run();
    // Closed safe kinds only: no native error text, paths, keys or payloads.
    eprintln!(
        "public_source_commitment_observed_error_kind={:?}",
        result.as_ref().err().map(|error| error.kind())
    );
    assert!(
        matches!(result,Err(ref error)if error.kind()==StorageErrorKind::CorruptData),
        "public_full_scrub_must_consume_source_commitment_not_blanket_unavailable"
    );
}

fn advance_to_component(session: &mut RedbStructuralEvidenceSession) -> StructuralEvidenceCursor {
    let mut cursor =
        StructuralEvidenceCursor::start(session.scan.database_id, session.scan.open_session_id);
    for _ in 0..4096 {
        assert!(cursor.position() <= session.scan.source_component_start);
        if cursor.position() == session.scan.source_component_start {
            return cursor;
        }
        match session
            .scan
            .read_structural_evidence(cursor, EvidencePageLimit::new(256).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "canonical physical phases must pass");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(_) => {
                panic!("nonempty Source requires further closure")
            }
        }
    }
    panic!("small structural fixture exceeded its explicit traversal bound")
}
fn one_component_unit(
    session: &mut RedbStructuralEvidenceSession,
    cursor: StructuralEvidenceCursor,
    limit: u32,
) -> StructuralEvidenceCursor {
    match session
        .scan
        .read_structural_evidence(cursor, EvidencePageLimit::new(limit).unwrap())
        .unwrap()
    {
        StructuralEvidencePage::Page {
            start,
            findings,
            next,
        } => {
            assert_eq!(start, cursor);
            assert!(findings.is_empty());
            assert_eq!(
                next.position() - cursor.position(),
                1,
                "one row/control/end visit even at limit256"
            );
            next
        }
        StructuralEvidencePage::ExactEnd(_) => {
            panic!("row/progress component cannot grant Source ExactEnd")
        }
    }
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn public_source_component_advances_one_unit_and_refuses_unproved_source_end() {
    for limit in [1, 256] {
        let Fixture {
            mut session, _root, ..
        } = fixture(Corruption::None);
        let mut cursor = advance_to_component(&mut session);
        assert_eq!(
            session.scan.structural_total - cursor.position(),
            17,
            "Four origin observations, AllocatorKV, RootEnd, ObjectLoad, two row passes/prefix ends, RootLoad and ObjectEnd"
        );
        let source = session.scan.shared.database.source_working_budget().clone();
        let before = source.observation().unwrap();
        let free = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.owned_bytes
            - before.reserved_bytes;
        let pressure = source.reserve(free).unwrap();
        let saturated = source.observation().unwrap();
        // The original independently correct row commitment still completes
        // before the newly checked missing selected Root is rejected.
        for _ in 0..15 {
            let previous = cursor;
            cursor = one_component_unit(&mut session, cursor, limit);
            assert_eq!(source.observation().unwrap(), saturated);
            assert!(
                matches!(session.scan.read_structural_evidence(previous,EvidencePageLimit::new(limit).unwrap()),Err(error)if error.kind()==StorageErrorKind::InvariantViolation)
            );
        }
        let next = match session
            .scan
            .read_structural_evidence(cursor, EvidencePageLimit::new(limit).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page {
                start,
                findings,
                next,
            } => {
                assert_eq!(start, cursor);
                assert_eq!(next.position() - start.position(), 1);
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].scope(), StructuralFindingScope::Authoritative);
                next
            }
            StructuralEvidencePage::ExactEnd(_) => {
                panic!("unselected Published structural object cannot complete")
            }
        };
        assert_eq!(
            source.observation().unwrap(),
            saturated,
            "RootLoad belongs to actual Maintenance, never Source64"
        );
        assert_eq!(
            session
                .scan
                .read_structural_evidence(next, EvidencePageLimit::new(limit).unwrap())
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
        drop(pressure);
    }
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn public_source_component_cancellation_releases_its_actual_maintenance_graph() {
    let Fixture {
        mut session, _root, ..
    } = fixture(Corruption::None);
    let cursor = advance_to_component(&mut session);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let before = pool.charged_bytes();
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    session.scan.set_cancellation(cancelled.clone());
    let mut next = cursor;
    // Four origin controls, AllocatorKV, RootEnd, then actual Object DTO.
    for _ in 0..7 {
        next = one_component_unit(&mut session, next, 256);
    }
    assert!(
        pool.charged_bytes() > before,
        "Box and decoded descriptor charged before construction"
    );
    cancelled.store(true, std::sync::atomic::Ordering::Release);
    assert_eq!(
        session
            .scan
            .read_structural_evidence(next, EvidencePageLimit::new(256).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable
    );
    assert!(session.scan.source_components.is_none());
    assert_eq!(
        pool.charged_bytes(),
        before,
        "all Source aliases/Box drop before their existing-family charge"
    );
    assert_eq!(
        session
            .scan
            .read_structural_evidence(next, EvidencePageLimit::new(1).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::Unavailable,
        "cancellation cannot mint a resumable component"
    );
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn public_source_component_corruption_consumes_one_visit_then_refuses_continuation() {
    let Fixture {
        mut session, _root, ..
    } = fixture(Corruption::Root);
    let mut cursor = advance_to_component(&mut session);
    for _ in 0..14 {
        cursor = one_component_unit(&mut session, cursor, 256);
    }
    let failed = cursor;
    let next = match session
        .scan
        .read_structural_evidence(cursor, EvidencePageLimit::new(256).unwrap())
        .unwrap()
    {
        StructuralEvidencePage::Page {
            start,
            findings,
            next,
        } => {
            assert_eq!(start, failed);
            assert_eq!(next.position() - start.position(), 1);
            assert_eq!(findings.len(), 1);
            assert_eq!(findings[0].scope(), StructuralFindingScope::Authoritative);
            assert_eq!(findings[0].code(), StructuralFindingCode::CrossLinkMismatch);
            next
        }
        StructuralEvidencePage::ExactEnd(_) => panic!("false Source commitment cannot complete"),
    };
    assert_eq!(
        session
            .scan
            .read_structural_evidence(next, EvidencePageLimit::new(256).unwrap())
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData,
        "refused state cannot fabricate repeated advancing findings"
    );
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn public_source_copying_progress_is_bounded_and_never_grants_complete_source_end() {
    for copied in [0, 2, 3] {
        let Fixture {
            mut session, _root, ..
        } = copy_progress::public_copy_fixture(copied);
        let mut cursor = advance_to_component(&mut session);
        let units = 12 + 2 * u64::from(copied);
        assert_eq!(
            session.scan.structural_total - cursor.position(),
            units,
            "Four origin controls, MaintenanceLoad, prefix ends and object end are explicit"
        );
        for _ in 0..units - 1 {
            cursor = one_component_unit(&mut session, cursor, 256);
        }
        let next = match session
            .scan
            .read_structural_evidence(cursor, EvidencePageLimit::new(256).unwrap())
            .unwrap()
        {
            StructuralEvidencePage::Page {
                start,
                findings,
                next,
            } => {
                assert_eq!(start, cursor);
                assert_eq!(next.position() - start.position(), 1);
                assert_eq!(findings.len(), 1);
                assert_eq!(findings[0].code(), StructuralFindingCode::CrossLinkMismatch);
                next
            }
            StructuralEvidencePage::ExactEnd(_) => {
                panic!("structural progress with wrong origin cannot complete")
            }
        };
        assert_eq!(next.position(), session.scan.structural_total);
        assert_eq!(
            session
                .scan
                .read_structural_evidence(next, EvidencePageLimit::new(256).unwrap())
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData,
            "real progress alone cannot certify this fixture's deliberately false origin"
        );
    }
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[test]
fn public_full_scrub_rejects_unselected_published_object_after_real_runtime_positive_control() {
    // The real runtime database first passes the public full validator. Its
    // test-only rows have the independent correct commitment, but the injected
    // Published object has no selected Root. No valid publication is fabricated.
    let Fixture { session, _root, .. } = fixture_with_public_scrub_control(Corruption::None, true);
    drop(session);
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
        "join actual engine retirement before public reopening"
    );
    let result = RedbOfflineIntegrityScrub::from_inputs(&_root.join("db.redb"), inputs()).run();
    eprintln!(
        "public_source_reciprocal_root_observed_error_kind={:?}",
        result.as_ref().err().map(|error| error.kind())
    );
    assert!(
        matches!(result,Err(ref error)if error.kind()==StorageErrorKind::CorruptData),
        "public_full_scrub_must_reject_unselected_published_source_object"
    );
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[test]
fn public_full_scrub_rejects_two_copying_candidates_after_real_runtime_positive_control() {
    let Fixture {
        session,
        _root,
        locator,
        ..
    } = copy_progress::public_copy_fixture_with_real_positive(0);
    drop(session);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let path = _root.join("db.redb");
    let store = RedbStore::open(&path).unwrap();
    let (object, maintenance) = {
        let read = store.shared.database.begin_read().unwrap();
        let table = read
            .open_table(source_validation::definition(1).unwrap())
            .unwrap();
        let original = table
            .get(crate::source_keys::source_object_key(locator).as_slice())
            .unwrap()
            .unwrap();
        let original =
            riffdb_storage_api::proto_codec::decode_projection_source_object_admitted_v1(
                original.value(),
                store.shared.database.source_working_budget(),
            )
            .unwrap()
            .into_parts()
            .0;
        let old = original.descriptor();
        // Deliberate structural corruption, not an allocated ordinal or real
        // coordinator copy/publication. Both initial candidates have no rows.
        let locator = ProjectionSourceLocatorV1::new(
            locator.database_id(),
            locator.allocation_history_incarnation(),
            riffdb_types::ProjectionCheckpointOrdinal::new(8).unwrap(),
        )
        .unwrap();
        let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
            identity: old.reference().identity().clone(),
            locator,
            applied_through: old.applied_through(),
            captured_source: old.captured_source(),
            origin: old.reference().origin(),
            prior_source_digest: old.prior_source_digest(),
            row_count: old.row_count(),
            canonical_row_bytes: old.canonical_row_bytes(),
            row_root: old.row_root(),
        })
        .unwrap();
        let progress =
            riffdb_storage_api::ProjectionSourceMaintenanceV1::initial(descriptor.clone(), None)
                .unwrap();
        let object =
            ProjectionSourceObjectV1::new(descriptor, ProjectionSourceLifecycleV1::Copying);
        (
            (
                crate::source_keys::source_object_key(locator),
                encode_projection_source_object_v1(&object).unwrap(),
            ),
            (
                crate::source_keys::source_maintenance_key(locator),
                riffdb_storage_api::proto_codec::encode_projection_source_maintenance_v1(&progress)
                    .unwrap(),
            ),
        )
    };
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(source_validation::definition(1).unwrap())
        .unwrap()
        .insert(object.0.as_slice(), object.1.as_bytes())
        .unwrap();
    write
        .open_table(source_validation::definition(3).unwrap())
        .unwrap()
        .insert(maintenance.0.as_slice(), maintenance.1.as_bytes())
        .unwrap();
    write.commit().unwrap();
    drop(store);
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let result = RedbOfflineIntegrityScrub::from_inputs(&path, inputs()).run();
    eprintln!(
        "public_source_two_candidates_observed_error_kind={:?}",
        result.as_ref().err().map(|error| error.kind())
    );
    assert!(
        matches!(result,Err(ref error)if error.kind()==StorageErrorKind::CorruptData),
        "public_full_scrub_must_reject_two_copying_candidates"
    );
}

// req: PRJ-004, STO-020, REC-001, REC-005
#[test]
fn public_full_scrub_rejects_wrong_source_origin_after_real_runtime_positive_control() {
    let Fixture {
        session,
        _root,
        locator,
        ..
    } = fixture_with_public_scrub_control(Corruption::None, true);
    drop(session);
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let path = _root.join("db.redb");
    let store = RedbStore::open(&path).unwrap();
    let encoded = {
        let read = store.shared.database.begin_read().unwrap();
        let table = read
            .open_table(source_validation::definition(1).unwrap())
            .unwrap();
        let original = table
            .get(crate::source_keys::source_object_key(locator).as_slice())
            .unwrap()
            .unwrap();
        let original =
            riffdb_storage_api::proto_codec::decode_projection_source_object_admitted_v1(
                original.value(),
                store.shared.database.source_working_budget(),
            )
            .unwrap()
            .into_parts()
            .0;
        // Preserve the independently framed row root and the deliberately bogus
        // origin already in this structural fixture. Superseded with no selected
        // Root passes reciprocal checks; this is not a legitimate prior publication.
        let malformed = original
            .transition(ProjectionSourceLifecycleV1::Superseded)
            .unwrap();
        encode_projection_source_object_v1(&malformed).unwrap()
    };
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(source_validation::definition(1).unwrap())
        .unwrap()
        .insert(
            crate::source_keys::source_object_key(locator).as_slice(),
            encoded.as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    drop(store);
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    let result = RedbOfflineIntegrityScrub::from_inputs(&path, inputs()).run();
    eprintln!(
        "public_source_wrong_origin_observed_error_kind={:?}",
        result.as_ref().err().map(|error| error.kind())
    );
    assert!(
        matches!(result,Err(ref error)if error.kind()==StorageErrorKind::CorruptData),
        "public_full_scrub_must_reject_wrong_source_origin"
    );
}
