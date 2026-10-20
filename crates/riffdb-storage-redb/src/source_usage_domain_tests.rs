//! Mechanical owner protocol only. These private test census/evidence values
//! are not native physical census, actual publication or cold-mint qualification.
// req: STO-020, PRJ-004, STO-012
use super::*;
use std::sync::{Arc, Barrier};

fn install_test_census(domain: &SourceUsageDomain, usage: Usage) {
    let epoch = domain.capture_epoch().unwrap();
    domain
        .install_completed_census(CompletedCensus {
            candidate: None,
            epoch,
            usage,
        })
        .unwrap();
}

#[test]
fn source_usage_version_requires_exact_owner_and_unchanged_complete_census() {
    let domain = SourceUsageDomain::new_unavailable();
    assert!(domain.complete_observation().is_err());
    let stale = domain.capture_epoch().unwrap();
    drop(domain.begin_unaccounted_publication().unwrap());
    assert!(
        domain
            .install_completed_census(CompletedCensus {
                candidate: None,
                epoch: stale,
                usage: Usage::empty()
            })
            .is_err()
    );
    let foreign = SourceUsageDomain::new_unavailable();
    let epoch = foreign.capture_epoch().unwrap();
    assert!(
        matches!(domain.install_completed_census(CompletedCensus { candidate: None, epoch, usage: Usage::empty() }),
        Err(error) if error.kind() == StorageErrorKind::InvariantViolation)
    );
    assert!(domain.complete_observation().is_err());
    install_test_census(&domain, Usage::empty());
    assert_eq!(domain.complete_observation().unwrap(), Usage::empty());
}

#[test]
fn source_usage_unknown_retains_candidate_and_prior_until_exact_physical_outcome() {
    let domain = SourceUsageDomain::new_unavailable();
    let before = Usage::new(1, 100, 200, None).unwrap();
    let after = before.after_candidate_allocation(1, 50, 10, 20).unwrap();
    install_test_census(&domain, before);
    let epoch = domain.capture_epoch().unwrap();
    let mutation = domain
        .begin_prepared_change(PreparedChange {
            epoch,
            before,
            after,
        })
        .unwrap();
    assert!(
        domain.capture_epoch().is_err(),
        "no census while a physical writer remains live"
    );
    assert!(domain.complete_observation().is_err());
    assert_eq!(
        domain.retained_uncertain_bytes().unwrap(),
        Some(after.total_bytes())
    );
    drop(mutation);
    assert!(
        domain.complete_observation().is_err(),
        "dropping an uncertain writer proves no rollback"
    );
    assert_eq!(
        domain.retained_uncertain_bytes().unwrap(),
        Some(after.total_bytes())
    );
    // A new full physical census, not a DTO observation or mere error, may
    // reconstruct retained Copying reservation and release the uncertainty pair.
    install_test_census(&domain, after);
    assert_eq!(domain.complete_observation().unwrap(), after);
    let epoch = domain.capture_epoch().unwrap();
    let next = domain
        .begin_prepared_change(PreparedChange {
            epoch,
            before: after,
            after: before,
        })
        .unwrap();
    next.resolve_exact(VerifiedPhysicalOutcome::ExactBefore)
        .unwrap();
    assert_eq!(domain.complete_observation().unwrap(), after);
    assert_eq!(domain.retained_uncertain_bytes().unwrap(), None);
}

#[test]
fn source_usage_competing_actual_owner_versions_have_one_winner_without_wrap() {
    let domain = Arc::new(SourceUsageDomain::new_unavailable());
    install_test_census(&domain, Usage::empty());
    let ready = Arc::new(Barrier::new(3));
    let release = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let domain = Arc::clone(&domain);
        let ready = Arc::clone(&ready);
        let release = Arc::clone(&release);
        workers.push(std::thread::spawn(move || {
            let epoch = domain.capture_epoch().unwrap();
            ready.wait();
            let result = domain.begin_prepared_change(PreparedChange {
                epoch,
                before: Usage::empty(),
                after: Usage::empty(),
            });
            release.wait();
            match result {
                Ok(mutation) => {
                    mutation
                        .resolve_exact(VerifiedPhysicalOutcome::ExactAfter)
                        .unwrap();
                    true
                }
                Err(_) => false,
            }
        }));
    }
    ready.wait();
    release.wait();
    assert_eq!(
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .filter(|won| *won)
            .count(),
        1
    );
    let mut state = domain.state.lock().unwrap();
    state.version = u64::MAX;
    drop(state);
    let epoch = domain.capture_epoch().unwrap();
    assert!(
        matches!(domain.begin_prepared_change(PreparedChange { epoch, before: Usage::empty(), after: Usage::empty() }),
        Err(error) if error.kind() == StorageErrorKind::LimitExceeded)
    );
    assert_eq!(domain.state.lock().unwrap().version, u64::MAX);
}

#[test]
fn source_usage_inventory_excludes_ordinary_head_and_derived_generation_writes() {
    let affected = riffdb_storage_api::AuthoritativeStateCatalogV3
        .namespaces()
        .filter(|namespace| namespace_affects_source_usage(*namespace))
        .map(|namespace| namespace.tag())
        .collect::<Vec<_>>();
    assert_eq!(affected, [44, 45, 46, 47, 114]);
    for namespace in riffdb_storage_api::AuthoritativeNamespaceV1::ALL {
        assert!(!namespace_affects_source_usage(namespace.into()));
    }
    assert!(!namespace_affects_source_usage(
        riffdb_storage_api::AuthoritativeNamespaceV3::GenerationSourceBases
    ));
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn source_usage_prepublication_window_refuses_new_version_over_old_root() {
    let domain = Arc::new(SourceUsageDomain::new_unavailable());
    let before = Usage::new(1, 100, 200, None).unwrap();
    install_test_census(&domain, before);
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let writer_domain = Arc::clone(&domain);
    let writer_entered = Arc::clone(&entered);
    let writer_release = Arc::clone(&release);
    let writer = std::thread::spawn(move || {
        let publication = writer_domain.begin_unaccounted_publication().unwrap();
        writer_entered.wait();
        writer_release.wait();
        drop(publication);
    });
    struct Release {
        barrier: Option<Arc<Barrier>>,
        writer: Option<std::thread::JoinHandle<()>>,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(barrier) = self.barrier.take() {
                barrier.wait();
            }
            if let Some(writer) = self.writer.take() {
                let _ = writer.join();
            }
        }
    }
    let mut release = Release {
        barrier: Some(release),
        writer: Some(writer),
    };
    entered.wait();
    // The actual native hook pauses after this version advance but before the
    // new root is visible. Capturing that advanced version would be unsound.
    assert!(domain.capture_epoch().is_err());
    assert!(domain.complete_observation().is_err());
    assert_eq!(
        domain.state.lock().unwrap().publication.unwrap().prior,
        Some(before)
    );
    assert_eq!(
        domain.retained_uncertain_bytes().unwrap(),
        Some(riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES)
    );
    release.barrier.take().unwrap().wait();
    release.writer.take().unwrap().join().unwrap();
    assert!(
        domain.complete_observation().is_err(),
        "Drop is not rollback or completeness"
    );
    let epoch = domain.capture_epoch().unwrap();
    domain
        .install_completed_census(CompletedCensus {
            candidate: None,
            epoch,
            usage: before,
        })
        .unwrap();
    assert_eq!(domain.complete_observation().unwrap(), before);
}

// req: STO-020, STO-012, PRJ-004
#[test]
fn source_usage_physical_commit_borrow_verifies_owner_lease_and_preserves_pending_mutation() {
    let dir = crate::test_path::ScopedDirectory::new("source-usage-physical-commit-borrow");
    let foreign_dir =
        crate::test_path::ScopedDirectory::new("source-usage-physical-commit-foreign");
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x74; 10])
            .unwrap();
    let mut store = crate::RedbStore::open(dir.join("database.redb")).unwrap();
    store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let ports = crate::RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    let mut foreign_store = crate::RedbStore::open(foreign_dir.join("database.redb")).unwrap();
    foreign_store
        .initialize_projection_source_fixture(database)
        .unwrap();
    let foreign_ports = crate::RedbOperationalPorts {
        shared: Arc::clone(&foreign_store.shared),
    };
    let domain = ports.shared.database.source_usage_domain();
    let before = Usage::new(1, 100, 200, None).unwrap();
    let after = before.after_candidate_allocation(1, 50, 10, 20).unwrap();
    install_test_census(domain, before);
    let publication = domain.begin_unaccounted_publication().unwrap();
    publication.require_for_shared(&ports.shared).unwrap();
    assert_eq!(
        publication
            .require_for_shared(&foreign_ports.shared)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    drop(publication);
    install_test_census(domain, before);
    let epoch = domain.capture_epoch().unwrap();
    let mutation = domain
        .begin_prepared_change(PreparedChange {
            epoch,
            before,
            after,
        })
        .unwrap();
    assert!(mutation.native_version() > 0);
    mutation.verify_for_native_observation(&ports).unwrap();
    assert_eq!(
        mutation
            .verify_for_native_observation(&foreign_ports)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    let ordinary_lease = foreign_ports.shared.mutation_gate.acquire().unwrap();
    assert_eq!(
        mutation
            .verify_for_native_readback(&ports, &ordinary_lease)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    assert!(
        mutation
            .physical_commit_borrow(&ports, &ordinary_lease)
            .is_err()
    );
    drop(ordinary_lease);
    let source_lease = ports.shared.acquire_source_usage_fixture_lease().unwrap();
    mutation
        .verify_for_native_readback(&ports, &source_lease)
        .unwrap();
    let borrow = mutation
        .physical_commit_borrow(&ports, &source_lease)
        .unwrap();
    borrow.require_for_shared(&ports.shared).unwrap();
    assert_eq!(
        borrow
            .require_for_shared(&foreign_ports.shared)
            .unwrap_err()
            .kind(),
        StorageErrorKind::InvariantViolation
    );
    let transaction = ports.shared.database.begin_write().unwrap();
    let capability = ports
        .begin_composite_read()
        .unwrap()
        .locator_capability()
        .clone();
    ports
        .shared
        .commit_with_prepared_source_usage_and_locator(
            transaction,
            crate::source_continuation::SourceContinuationFootprint::fresh(),
            capability,
            borrow,
        )
        .unwrap();
    assert!(
        domain.complete_observation().is_err(),
        "consuming the physical commit borrow must not resolve the prepared mutation"
    );
    mutation
        .verify_for_native_readback(&ports, &source_lease)
        .unwrap();
    mutation
        .resolve_exact(VerifiedPhysicalOutcome::ExactAfter)
        .unwrap();
    assert_eq!(domain.complete_observation().unwrap(), after);
}
