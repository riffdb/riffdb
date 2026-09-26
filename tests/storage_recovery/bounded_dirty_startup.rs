//! ADR-0182 bounded dirty startup proofs over the shared recovery fixtures.
use super::*;

// req: REC-001, REC-002, REC-004, REC-005, PERF-019
/// OBL-0182-1 proof (ADR-0182 Decision 1). A startup without a valid
/// clean-close certificate reaches readiness through journal-suffix recovery
/// plus the bounded-root validation the clean path performs: the structural
/// walk covers only the bounded catalog tables (never the population total),
/// the historical plan holds only the bounded locators, transient population
/// indexes are left cold exactly as the clean path leaves them, and repeated
/// dirty restart creates no command, event, outcome, provenance, audit,
/// projection, outbox, or allocator advance.
#[test]
fn dirty_startup_reaches_readiness_without_a_population_walk() {
    fn bounded_dirty_open(
        path: &Path,
    ) -> (
        RedbOperationalPorts,
        u64,
        riffdb_storage_redb::benchmark_support::RecoveryAuthoritySnapshotV1,
    ) {
        let store = RedbStore::open(path).expect("dirty reopen");
        let session = store
            .begin_structural_evidence(startup_inputs())
            .expect("begin bounded dirty evidence");
        assert!(
            session.bounded_dirty_startup_path(),
            "a V3-active database without a valid clean-close certificate must take the ADR-0182 bounded dirty path"
        );
        assert!(!session.clean_close_fast_path());
        // The pinned zero population-walk count: the session's structural
        // walk total is the bounded catalog total, not the population total.
        // The population tables (entities, indexes, events, audit,
        // provenance, idempotency, outbox) contribute zero rows to it.
        let walk_total = session.structural_walk_total();
        assert!(
            walk_total < 1_000,
            "the bounded dirty walk must not scale with population; walked {walk_total} rows"
        );
        let mut session = session;
        let structural_end = drive_structural_to_end(&mut session);
        // The header proof is bounded too: it never decodes the command
        // history into the startup command cache. That cache is charged
        // against the 512 MiB evidence ceiling, so at production scale a
        // header that built it refused startup with LimitExceeded.
        assert_eq!(
            session.command_history_rows_decoded(),
            0,
            "the bounded dirty header must not decode the command history before readiness"
        );
        let opened = finish_structural_open_from(session, structural_end);
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("bounded dirty startup reaches readiness");
        // The transient population indexes stay cold exactly as on the clean
        // path: no rebuild walked the commit table before readiness.
        assert_eq!(
            ports.transient_index_rebuilds(),
            0,
            "the bounded dirty path must not rebuild population indexes before readiness"
        );
        assert!(
            ports.clean_close_fast_startup(),
            "the bounded dirty path leaves population caches cold like the clean path"
        );
        drop(ports);
        let snapshot = riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(path)
            .expect("read authority snapshot");
        (dormant_ports_for_snapshot(path), walk_total, snapshot)
    }

    fn dormant_ports_for_snapshot(path: &Path) -> RedbOperationalPorts {
        // Reopen once more purely to return live ports for follow-up probes;
        // the assertions above already proved the bounded shape.
        let store = RedbStore::open(path).expect("reopen for probes");
        let session = store
            .begin_structural_evidence(startup_inputs())
            .expect("begin evidence for probes");
        let opened = complete_structural_open_from(session);
        let (_, _, _, dormant) = opened.into_parts();
        dormant
            .into_operational_after_catalog_validation()
            .expect("activate probe ports")
    }

    // Both source catalogs: V2 (every new database, ADR-0260) and V1.
    for v1 in [false, true] {
        let path = TestDatabasePath::new(if v1 {
            "dirty-bounded-readiness-v1"
        } else {
            "dirty-bounded-readiness-v2"
        });
        if v1 {
            prepare_v1_command_database(&path.0);
        } else {
            prepare_command_database(&path.0);
        }
        // Give the database a real population beyond the fixture command: entity
        // rows, index rows, events, provenance, audit, and idempotency rows that
        // the old complete path would have enumerated before readiness.
        {
            let ports = open_operational(RedbStore::open(&path.0).expect("open populated source"));
            for ordinal in 1..=24 {
                let fixture = command_fixture_at(ordinal);
                commit_command_fixture(&ports, &fixture);
            }
            drop(ports);
        }
        let authority_before =
            riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
                .expect("read pre-restart authority");
        let (ports, walk_total, authority_after) = bounded_dirty_open(&path.0);
        assert!(
            authority_before.same_authority(&authority_after),
            "a bounded dirty restart must create no command, event, outcome, provenance, audit, projection, outbox, or allocator advance"
        );
        drop(ports);

        // A second dirty restart repeats the same bounded shape and again
        // advances nothing.
        let (ports, second_walk_total, authority_final) = bounded_dirty_open(&path.0);
        drop(ports);
        assert_eq!(
            second_walk_total, walk_total,
            "the bounded dirty walk total is stable across dirty generations"
        );
        assert!(authority_before.same_authority(&authority_final));
    }
}

// req: REC-001, REC-004
/// ADR-0182 Decision 2: rows outside the bounded roots are validated on first
/// access, never classified from a cold cache. After a bounded dirty startup
/// the transient command index is cold; the first outbox claim for a
/// command-owned event must resolve the command from its durable segment and
/// apply, not misread the segment's events as missing physical rows.
#[test]
fn bounded_dirty_startup_claims_a_command_owned_outbox_event() {
    let path = TestDatabasePath::new("bounded-dirty-outbox-claim");
    prepare_command_database(&path.0);
    let mut ports = {
        let store = RedbStore::open(&path.0).expect("dirty reopen");
        let session = store
            .begin_structural_evidence(startup_inputs())
            .expect("begin bounded dirty evidence");
        assert!(session.bounded_dirty_startup_path());
        complete_structural_open_from(session)
            .into_parts()
            .3
            .into_operational_after_catalog_validation()
            .expect("bounded dirty startup reaches readiness")
    };
    let fixture = command_fixture_at(1);
    commit_command_fixture(&ports, &fixture);
    assert_eq!(ports.transient_index_rebuilds(), 0, "the index stays cold");

    let claim = OutboxClaimV1::new(
        fixture.records.events()[0].event_id(),
        OutboxStatusObservationV1::AbsentInitialPending,
        OutboxDestinationIdV1::new("bounded-dirty-claim").expect("destination"),
        Timestamp::new(1_700_000_010, 0).expect("claimed at"),
        Timestamp::new(1_700_000_020, 0).expect("lease expiry"),
    )
    .expect("claim transition");
    let result = ports
        .claim_outbox(&claim)
        .expect("a command-owned event is not corrupt when the index is cold");
    assert!(
        matches!(result, OutboxTransitionResultV1::Applied(_)),
        "the fresh claim must apply: {result:?}"
    );
}

/// Where an out-of-band corruption was refused under the ADR-0182 layering.
#[derive(Debug)]
pub(crate) enum CorruptionRefusal {
    /// The engine open or the startup evidence session refused it.
    BeforeReadiness,
    /// Startup reported these structural findings before readiness.
    StartupFindings(Vec<StructuralFinding>),
    /// Readiness was reached and the background scrub reported these findings.
    ScrubFindings(Vec<StructuralFinding>),
    /// Readiness was reached and the background scrub refused to continue.
    ScrubRefused,
}

impl CorruptionRefusal {
    /// Whether the refusal carries an authoritative structural finding.
    pub(crate) fn authoritative(&self) -> bool {
        match self {
            Self::StartupFindings(findings) | Self::ScrubFindings(findings) => findings
                .iter()
                .any(|finding| finding.scope() == StructuralFindingScope::Authoritative),
            Self::BeforeReadiness | Self::ScrubRefused => false,
        }
    }
}

/// Opens `path` for startup and, when startup reaches readiness cleanly, runs
/// the background scrub's complete walk. Returns the first layer that refused
/// the database, or `None` when no layer did, which a corruption test must
/// treat as the failure it exists to catch.
pub(crate) fn corruption_refusal(path: &Path) -> Option<CorruptionRefusal> {
    let Ok(store) = RedbStore::open(path) else {
        return Some(CorruptionRefusal::BeforeReadiness);
    };
    let Ok(mut session) = store.begin_structural_evidence(startup_inputs()) else {
        return Some(CorruptionRefusal::BeforeReadiness);
    };
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut findings = Vec::new();
    let structural_end = loop {
        match session.read_structural_evidence(cursor, limit) {
            Ok(StructuralEvidencePage::Page {
                findings: page,
                next,
                ..
            }) => {
                findings.extend(page);
                cursor = next;
            }
            Ok(StructuralEvidencePage::ExactEnd(end)) => break end,
            Err(_) => return Some(CorruptionRefusal::BeforeReadiness),
        }
    };
    if !findings.is_empty() {
        return Some(CorruptionRefusal::StartupFindings(findings));
    }
    let Ok(validation) = validate_catalog_history(&mut session) else {
        return Some(CorruptionRefusal::BeforeReadiness);
    };
    let (catalog, historical_end) = validation.into_parts();
    if !matches!(catalog, CatalogHistoryOutcome::Ready(_)) {
        return Some(CorruptionRefusal::BeforeReadiness);
    }
    let Ok(StructuralOpenOutcome::Clean(opened)) = session.finish(structural_end, historical_end)
    else {
        return Some(CorruptionRefusal::BeforeReadiness);
    };
    let Ok(ports) = opened
        .into_parts()
        .3
        .into_operational_after_catalog_validation()
    else {
        return Some(CorruptionRefusal::BeforeReadiness);
    };
    let Ok(mut scrub) = ports
        .shared_ports()
        .begin_background_scrub(startup_inputs())
    else {
        return Some(CorruptionRefusal::ScrubRefused);
    };
    if scrub.validate_retained_history().is_err() {
        return Some(CorruptionRefusal::ScrubRefused);
    }
    let structural_end = loop {
        match scrub.read_structural_page(limit) {
            Ok(riffdb_storage_redb::BackgroundScrubPage::Page { findings: page, .. }) => {
                findings.extend(page);
            }
            Ok(riffdb_storage_redb::BackgroundScrubPage::ExactEnd(end)) => break end,
            Err(_) => return Some(CorruptionRefusal::ScrubRefused),
        }
    };
    if !findings.is_empty() {
        return Some(CorruptionRefusal::ScrubFindings(findings));
    }
    let historical_end = loop {
        match scrub.read_historical_page(limit) {
            Ok(riffdb_storage_redb::BackgroundScrubHistoricalPage::Page { .. }) => {}
            Ok(riffdb_storage_redb::BackgroundScrubHistoricalPage::ExactEnd(end)) => break end,
            Err(_) => return Some(CorruptionRefusal::ScrubRefused),
        }
    };
    scrub
        .finish(structural_end, historical_end)
        .err()
        .map(|_| CorruptionRefusal::ScrubRefused)
}
