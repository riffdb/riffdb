//! Real unchanged-frontier command owners under ADR-0186 Amendment 1.
// req: REP-003, REC-001, STO-012, PERF-007
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionV3 as Receipt,
    ChangelogAttributionV3 as A, CommandServiceAuditTransitionV1,
    proto_codec::{decode_execution_failed_v1, decode_pending_admission_v1},
};

fn admission(fixture: &CommandFixture) -> AdmissionRequestV1 {
    AdmissionRequestV1::new(fixture.candidates.clone(), &fixture.context).unwrap()
}

fn failure(fixture: &CommandFixture) -> ExecutionFailureTransitionRequestV1 {
    ExecutionFailureTransitionRequestV1::new(
        fixture.pending.clone(),
        &fixture.snapshot,
        ExecutionFailureCode::ArithmeticFault,
    )
    .unwrap()
}

fn terminalize(ports: &RedbOperationalPorts, fixture: &CommandFixture, audited: bool) {
    let request = failure(fixture);
    let expected = request.terminal_record();
    let ExecutionFailureAdmissionResult::Rechecked(rechecked) =
        ports.begin_execution_failure(request).unwrap()
    else {
        panic!("exact Pending must be rechecked");
    };
    let (awaiting, current) = rechecked.read_transaction_current().unwrap();
    assert_eq!(current.bindings(), fixture.snapshot.bindings());
    let actual = if audited {
        let (started, _) = command_audit_intents(fixture);
        let terminal = ServiceAuditAppendIntentV1::new(
            fixture.pending.admission_request_id(),
            Timestamp::new(1_700_000_003, 0).unwrap(),
            ServiceOperationV1::ExecuteCommand,
            ServiceAuditPhaseV1::Failed,
            catalog_principal(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .unwrap();
        let transition =
            CommandServiceAuditTransitionV1::started_and_failure(started, terminal).unwrap();
        let (failure, audit) = awaiting
            .terminalize_with_service_audit(transition)
            .unwrap()
            .into_parts();
        assert_eq!(audit.phase(), ServiceAuditPhaseV1::Failed);
        assert_eq!(audit.link(), ServiceAuditLinkV1::None);
        failure
    } else {
        awaiting.terminalize().unwrap()
    };
    assert_eq!(actual, expected);
}

fn command_receipts(path: &Path) -> Vec<Receipt> {
    let database = Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    let table = read
        .open_table(TableDefinition::<&[u8], &[u8]>::new(
            N::ChangelogHistory.table(),
        ))
        .unwrap();
    assert!(table.len().unwrap() < 128, "bounded test history");
    table
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            let receipt = Receipt::decode(value.value()).unwrap();
            assert_eq!(key.value(), receipt.binding().sequence.get().to_be_bytes());
            receipt
        })
        .filter(|row| {
            matches!(
                row.attribution(),
                A::CommandAdmission | A::CommandExecutionFailure
            )
        })
        .collect()
}

fn assert_replay(ports: &RedbOperationalPorts, fixture: &CommandFixture) {
    let expected = failure(fixture).terminal_record();
    assert_eq!(
        ports.admit_or_resolve(admission(fixture)).unwrap(),
        AdmissionResultV1::ExecutionFailed(expected.clone())
    );
    let ExecutionFailureAdmissionResult::ExecutionFailed(actual) =
        ports.begin_execution_failure(failure(fixture)).unwrap()
    else {
        panic!("terminal failure must replay without another transaction commit");
    };
    assert_eq!(actual, expected);
    assert_eq!(ports.read_entity(&fixture.target).unwrap(), None);
    assert_eq!(
        ports.read_commit(CommitSequence::new(1).unwrap()).unwrap(),
        None
    );
    assert_eq!(
        ports
            .read_provenance(fixture.records.provenance().provenance_id())
            .unwrap(),
        None
    );
    assert_eq!(
        ports
            .read_stored_outcome(fixture.pending.identity())
            .unwrap(),
        None
    );
}

fn assert_pair(rows: &[Receipt], fixture: &CommandFixture, audited: bool) {
    assert_eq!(
        rows.len(),
        2,
        "retries must allocate neither a duplicate admission nor failure"
    );
    let admission = &rows[0];
    let failure = &rows[1];
    assert_eq!(admission.attribution(), A::CommandAdmission);
    assert_eq!(failure.attribution(), A::CommandExecutionFailure);
    assert_eq!(
        admission.binding().predecessor_frontier,
        admission.binding().covered_frontier
    );
    assert_eq!(failure.binding().predecessor_frontier.application(), None);
    assert_eq!(failure.binding().covered_frontier.application(), None);
    let before = failure
        .binding()
        .predecessor_frontier
        .administration()
        .map_or(0, |v| v.get());
    let after = failure
        .binding()
        .covered_frontier
        .administration()
        .map_or(0, |v| v.get());
    assert_eq!(after - before, if audited { 2 } else { 0 });
    assert!(!admission.mutations().iter().any(|m| matches!(
        m.namespace(),
        N::NextApplicationSequence | N::NextAdministrationSequence
    )));
    assert!(
        !failure
            .mutations()
            .iter()
            .any(|m| m.namespace() == N::NextApplicationSequence)
    );
    assert_eq!(
        failure
            .mutations()
            .iter()
            .any(|m| m.namespace() == N::NextAdministrationSequence),
        audited
    );
    assert_eq!(
        failure
            .mutations()
            .iter()
            .filter(|m| m.namespace() == N::Audit)
            .count(),
        if audited { 2 } else { 0 }
    );
    let pending = admission
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::IdempotencyPending)
        .unwrap();
    assert!(pending.matches_prior(None));
    assert_eq!(
        decode_pending_admission_v1(pending.value().unwrap())
            .unwrap()
            .value(),
        &fixture.pending
    );
    let deleted = failure
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::IdempotencyPending)
        .unwrap();
    assert_eq!(deleted.key(), pending.key());
    assert!(deleted.value().is_none());
    assert!(deleted.matches_prior(pending.value()));
    let terminal = failure
        .mutations()
        .iter()
        .find(|m| m.namespace() == N::Idempotency)
        .unwrap();
    assert_eq!(terminal.key(), pending.key());
    assert!(terminal.matches_prior(None));
    assert_eq!(
        decode_execution_failed_v1(terminal.value().unwrap())
            .unwrap()
            .value(),
        &self::failure(fixture).terminal_record()
    );
}

#[test]
fn command_attribution_preserves_exact_pending_failure_and_audit_receipts_on_replay() {
    for audited in [false, true] {
        let path = TestDatabasePath::new("v3-command-attribution");
        prepare_command_database(&path.0);
        let fixture = two_phase_command_fixture_at(1);
        let ports = open_operational(RedbStore::open(&path.0).unwrap());
        assert_eq!(
            ports.admit_or_resolve(admission(&fixture)).unwrap(),
            AdmissionResultV1::Created(fixture.pending.clone())
        );
        assert_eq!(
            ports.admit_or_resolve(admission(&fixture)).unwrap(),
            AdmissionResultV1::Resumed(fixture.pending.clone())
        );
        terminalize(&ports, &fixture, audited);
        assert_replay(&ports, &fixture);
        drop(ports);
        let exact = command_receipts(&path.0);
        assert_pair(&exact, &fixture, audited);
        for _ in 0..2 {
            let reopened = open_operational(RedbStore::open(&path.0).unwrap());
            assert_replay(&reopened, &fixture);
            assert!(reopened.write_validated_prefix_checkpoint().unwrap());
            drop(reopened);
            assert_eq!(
                command_receipts(&path.0),
                exact,
                "checkpoint and recovery preserve the original receipts"
            );
        }
    }
}

#[test]
fn command_attribution_crash_child() {
    let Ok(phase) = std::env::var("RIFFDB_COMMAND_ATTRIBUTION_PHASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("RIFFDB_COMMAND_ATTRIBUTION_PATH").unwrap());
    let operation = match phase.as_str() {
        "admission-before" | "admission-after" => RedbTestOperation::Admission,
        "failure-before" | "failure-after" => RedbTestOperation::ExecutionFailure,
        _ => panic!("unknown closed crash phase"),
    };
    let controller = if phase.ends_with("-after") {
        RedbTestController::abort_after_commit(operation)
    } else {
        RedbTestController::abort_before_commit(operation)
    };
    let ports = open_operational(RedbStore::open_with_test_controller(&path, controller).unwrap());
    let fixture = two_phase_command_fixture_at(1);
    ports.admit_or_resolve(admission(&fixture)).unwrap();
    terminalize(&ports, &fixture, true);
    panic!("armed process edge was not reached");
}

#[test]
fn command_attribution_crashes_preserve_atomic_receipts_and_retry_exactly_once() {
    for phase in [
        "admission-before",
        "admission-after",
        "failure-before",
        "failure-after",
    ] {
        let path = TestDatabasePath::new("v3-command-attribution-crash");
        prepare_command_database(&path.0);
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "v3_command_attribution::command_attribution_crash_child",
            ])
            .env("RIFFDB_COMMAND_ATTRIBUTION_PHASE", phase)
            .env("RIFFDB_COMMAND_ATTRIBUTION_PATH", &path.0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_child_aborted(status, phase);
        let fixture = two_phase_command_fixture_at(1);
        let expected_count = match phase {
            "admission-before" => 0,
            "failure-after" => 2,
            _ => 1,
        };
        let exact = command_receipts(&path.0);
        assert_eq!(exact.len(), expected_count);
        for _ in 0..2 {
            let ports = open_operational(RedbStore::open(&path.0).unwrap());
            let expected = match expected_count {
                0 => AdmissionLookupResultV1::NotFound,
                1 => AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
                    fixture.pending.clone(),
                ))),
                2 => AdmissionLookupResultV1::Found(Box::new(
                    StoredAdmissionStateV1::ExecutionFailed(failure(&fixture).terminal_record()),
                )),
                _ => unreachable!(),
            };
            assert_eq!(
                ports.lookup_admission(fixture.candidates.clone()).unwrap(),
                expected
            );
            drop(ports);
            assert_eq!(command_receipts(&path.0), exact);
        }
        let ports = open_operational(RedbStore::open(&path.0).unwrap());
        if expected_count == 0 {
            assert_eq!(
                ports.admit_or_resolve(admission(&fixture)).unwrap(),
                AdmissionResultV1::Created(fixture.pending.clone())
            );
        }
        if expected_count < 2 {
            terminalize(&ports, &fixture, true);
        }
        assert_replay(&ports, &fixture);
        drop(ports);
        let retried = command_receipts(&path.0);
        assert_pair(&retried, &fixture, true);
        assert_eq!(&retried[..exact.len()], exact);
    }
}
