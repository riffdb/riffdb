// req: TXN-001, TXN-002, CMP-013, PERF-012
use riffdb_storage_api::{CurrentRangeObservation, IndexRangeEntry, IndexRangeObservation, IndexedSelectionRequest};
use riffdb_types::{IndexEpoch, IndexEpochPosition};

fn selection_fixture(candidate: Option<u8>) -> (PendingCommandAttempts, ReadSnapshot) {
    selection_fixture_with_root(candidate, None)
}

fn selection_fixture_with_root(candidate: Option<u8>, root_enabled: Option<bool>) -> (PendingCommandAttempts, ReadSnapshot) {
    selection_fixture_options(candidate, root_enabled, false, false)
}

fn selection_fixture_options(candidate: Option<u8>, root_enabled: Option<bool>, protected: bool, reject: bool) -> (PendingCommandAttempts, ReadSnapshot) {
    let mut source = include_str!("../../../fixtures/compiler/indexed-selection/contract.riff").to_owned();
    if root_enabled.is_some() {
        source = source.replace("entity Slot {", "entity Pool { key (pool_id: uuid, root_id: uuid) field enabled: bool } entity Slot {")
            .replace("key (pool_id: uuid, slot_id: uuid)", "key (pool_id: uuid, root_id: uuid, slot_id: uuid)")
            .replace("root Slot", "root Pool child Slot invariant root_enabled: enabled");
    }
    if protected {
        source = source.replace("command Allocate", "row policy PoolAccess on Slot { allow read when available allow update when holder == principal.id } command Allocate");
    }
    if reject {
        source = source.replace("require available: slot.available", "require available: slot.available && slot.holder == holder");
    }
    let compiled = compile_contract_source(&source).expect("selector");
    let bundle = ValidatedContractBundle::from_compiler_bundle(compiled).expect("checked bundle");
    let plan = &bundle.bundle().commands()[0];
    let reference = ExecutablePlanRef::new(bundle.lineage().clone(), bundle.contract_version(), bundle.bundle_hash(), plan.command_id(), plan.plan_hash());
    let stored = bundle.to_stored().expect("stored bundle");
    let repository = LineageRepository { active: ActiveCatalogPointerV1::from_bundle(&stored), bundles: vec![stored] };
    let resolved = resolve_executable_plan(&repository, &reference).expect("resolved");
    let input = input_record(plan.input().record(), [
        ("request", CanonicalValue::Uuid([1; 16])),
        ("pool_id", CanonicalValue::Uuid([2; 16])),
        ("holder", CanonicalValue::Uuid([3; 16])),
    ]);
    let facts = derive_input_command_facts(plan, input.clone()).expect("upfront ownership");
    assert!(facts.binding_entity_keys().is_empty());
    let selection = IndexedSelectionRequest::from_plan(plan, bundle.bundle().schema(), facts.partition_key().clone(), facts.selection_prefix_values().expect("prefix")).expect("selection request");
    let request = SnapshotRequest::new(reference, vec![], vec![], vec![]).expect("input targets")
        .with_indexed_selection(selection.clone()).expect("selector");
    let entity = bundle.bundle().schema().entity(plan.bindings()[0].entity_type()).expect("selected entity");
    let (entries, observations) = if let Some(candidate) = candidate {
        let record = input_record(entity.record(), [
            ("pool_id", CanonicalValue::Uuid([2; 16])),
            ("slot_id", CanonicalValue::Uuid([candidate; 16])),
            ("root_id", CanonicalValue::Uuid([4; 16])),
            ("available", CanonicalValue::Bool(true)),
            ("holder", CanonicalValue::Uuid([0; 16])),
        ]);
        let components: Vec<_> = entity.primary_key_fields().iter().map(|id| record.fields().iter().find(|(field, _)| field == id).expect("key field").1.clone()).collect();
        let key = entity.primary_key().encode_entity(&components).expect("selected key");
        let index = &entity.indexes()[0];
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, &record).expect("physical values");
        let entry = IndexRangeEntry::new(index.id(), index.key_schema().encode_index(&values, key.clone()).expect("entry key"), CanonicalRecord::new(vec![]).expect("cover")).expect("entry");
        let stored = StoredEntityRecordV1::new(EntityTarget::new(entity.id(), key).expect("target"), EntityVersion::first(), bundle.contract_version(), riffdb_storage_api::DurableKeySchemaBindingV1::from_plan(request.plan()), record).expect("row");
        (vec![entry], vec![EntityObservation::Present(stored)])
    } else { (vec![], vec![]) };
    let concrete = request.resolve_indexed_selection(entries.first()).expect("concrete targets");
    let roots = concrete.root_validation_targets().iter().map(|target| {
        let entity = bundle.bundle().schema().entity(target.entity_type_id()).expect("root entity");
        let record = input_record(entity.record(), [("pool_id", CanonicalValue::Uuid([2;16])), ("root_id", CanonicalValue::Uuid([4;16])), ("enabled", CanonicalValue::Bool(root_enabled.expect("root enabled")))]);
        EntityObservation::Present(StoredEntityRecordV1::new(target.clone(), EntityVersion::first(), bundle.contract_version(), riffdb_storage_api::DurableKeySchemaBindingV1::from_plan(request.plan()), record).expect("root row"))
    }).collect();
    let snapshot = ReadSnapshot::new(&concrete, None, observations, roots, vec![IndexRangeObservation::new(selection.range().clone(), IndexEpochPosition::BeforeFirst, entries).expect("range")]).expect("owned view");
    (pending_attempt(resolved, input, request), snapshot)
}

fn evaluate_selection(state: PendingCommandAttempts, snapshot: ReadSnapshot, conflicts: &ShardedConflictManager, runtime: &tokio::runtime::Runtime) -> EvaluatedCommandAttempt {
    let order = Rc::new(RefCell::new(Vec::new()));
    let repository = ScriptedRepository::new(state.lookup_candidates.clone(), AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(state.commit_context.pending().clone()))), Rc::clone(&order));
    let snapshots = ScriptedSnapshotReader::new(state.snapshot_request.clone(), snapshot, order);
    let result = runtime.block_on(evaluate_next_command_attempt(state, &repository, &snapshots, conflicts)).expect("evaluate selection");
    let CommandAttemptResolution::Evaluated(attempt) = result else { panic!("declared selection outcome") };
    attempt
}

#[test]
fn empty_and_selected_outcomes_reject_changed_generations_before_any_durable_effect() {
    for candidate in [None, Some(8)] {
        let (state, snapshot) = selection_fixture(candidate);
        let keys = state.raw_conflict_keys.clone();
        let current = TransactionCurrentState::new(&snapshot.validation_request(), snapshot.bindings().to_vec(), vec![], vec![CurrentRangeObservation::new(snapshot.ranges()[0].target().clone(), IndexEpochPosition::Value(IndexEpoch::first()))]).expect("generation changed");
        let conflicts = manager();
        let runtime = runtime();
        let attempt = evaluate_selection(state, snapshot, &conflicts, &runtime);
        assert_eq!(attempt.evaluated().mutations().len(), usize::from(candidate.is_some()));
        let bound = attempt.bind_provenance(provenance_id()).expect("provenance");
        let order = Rc::new(RefCell::new(Vec::new()));
        let (port, trace) = instrumented_transaction_port(&bound, current, conflicts.clone(), keys.clone(), order);
        let crate::command_validation::CommandCandidateChainStart::Ready(candidate) = crate::command_validation::begin_bound_command_candidate(&port, bound) else { panic!("candidate") };
        let crate::command_validation::TransactionCurrentAttemptDecision::DependencyChanged(changed) = candidate.read_transaction_current() else { panic!("generation requires reevaluation") };
        let RolledBackCandidateDisposition::Retry { state, reason } = changed.reject_storage_and_rollback() else { panic!("rollback and retry") };
        assert_eq!(reason, CandidateValidationRejection::DependencyChanged);
        assert_eq!(state.completed_attempts(), 1);
        assert_eq!(trace.lease_held_at_rollback.get(), Some(true));
        drop(state);
        acquire_and_release(&runtime, &conflicts, keys);
    }
}

#[test]
fn each_retry_can_select_a_new_candidate_without_changing_input_context_or_identity() {
    let (mut state, _) = selection_fixture(None);
    let original_pending = state.commit_context.pending().clone();
    let original_input = state.normalized_input.clone();
    let conflicts = manager();
    let runtime = runtime();
    for (ordinal, candidate) in [Some(8), None, Some(9)].into_iter().enumerate() {
        let (_, snapshot) = selection_fixture(candidate);
        let expected = snapshot.bindings().first().map(|row| row.target().clone());
        let attempt = evaluate_selection(state, snapshot, &conflicts, &runtime);
        assert_eq!(attempt.commit_context().pending(), &original_pending);
        assert_eq!(attempt.normalized_input(), &original_input);
        assert_eq!(attempt.evaluated().mutations().first().map(|mutation| mutation.target()), expected.as_ref());
        assert_eq!(attempt.state.completed_attempts(), ordinal + 1);
        state = attempt.into_retry_state_for_test();
    }
    let (_, snapshot) = selection_fixture(Some(10));
    let order = Rc::new(RefCell::new(Vec::new()));
    let repository = ScriptedRepository::new(state.lookup_candidates.clone(), AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(original_pending))), Rc::clone(&order));
    let snapshots = ScriptedSnapshotReader::new(state.snapshot_request.clone(), snapshot, order);
    assert!(matches!(runtime.block_on(evaluate_next_command_attempt(state, &repository, &snapshots, &conflicts)), Err(CommandAttemptError::RetryBudgetExhausted)));
    assert_eq!((repository.calls.get(), snapshots.calls.get()), (0, 0));
}

#[test]
fn cancelled_selection_releases_pool_ownership_without_persisting_an_outcome() {
    let (state, snapshot) = selection_fixture(Some(8));
    let keys = state.raw_conflict_keys.clone();
    let cancellation = state.cancellation.clone();
    let order = Rc::new(RefCell::new(Vec::new()));
    let repository = ScriptedRepository::new(state.lookup_candidates.clone(), AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(state.commit_context.pending().clone()))), Rc::clone(&order));
    let snapshots = ScriptedSnapshotReader::new(state.snapshot_request.clone(), snapshot, order).cancelling(cancellation);
    let conflicts = manager();
    let runtime = runtime();
    assert!(matches!(runtime.block_on(evaluate_next_command_attempt(state, &repository, &snapshots, &conflicts)), Err(CommandAttemptError::Cancelled)));
    acquire_and_release(&runtime, &conflicts, keys);
}


#[test]
fn a_selected_child_keeps_its_discovered_root_invariant_at_commit() {
    for enabled in [false, true] {
        let (state, snapshot) = selection_fixture_with_root(Some(8), Some(enabled));
        assert_eq!(snapshot.root_validations().len(), 1);
        let current = TransactionCurrentState::new(&snapshot.validation_request(), snapshot.bindings().to_vec(), snapshot.root_validations().to_vec(), vec![CurrentRangeObservation::new(snapshot.ranges()[0].target().clone(), snapshot.ranges()[0].epoch())]).expect("current");
        let conflicts = manager();
        let runtime = runtime();
        let attempt = evaluate_selection(state, snapshot, &conflicts, &runtime);
        let decision = crate::command_validation::validate_transaction_current_command_parts(attempt.resolved_plan(), attempt.normalized_input(), attempt.input_facts(), attempt.commit_context().pending().logical_time(), attempt.evaluated(), &current).expect("checked invariant");
        assert_eq!(matches!(decision, crate::command_validation::CheckedCommandDecision::Rejected(CandidateValidationRejection::CommitCheckRejected)), !enabled);
    }
}


#[test]
fn selected_row_policy_is_required_for_both_mutation_and_requirement_rejection() {
    for reject in [false, true] {
        let (state, snapshot) = selection_fixture_options(Some(8), None, true, reject);
        let keys = state.raw_conflict_keys.clone();
        let current = TransactionCurrentState::new(&snapshot.validation_request(), snapshot.bindings().to_vec(), vec![], vec![CurrentRangeObservation::new(snapshot.ranges()[0].target().clone(), snapshot.ranges()[0].epoch())]).expect("current");
        let conflicts = manager();
        let runtime = runtime();
        let attempt = evaluate_selection(state, snapshot, &conflicts, &runtime);
        assert_eq!(attempt.evaluated().mutations().is_empty(), reject);
        let bound = attempt.bind_provenance(provenance_id()).expect("provenance");
        let order = Rc::new(RefCell::new(Vec::new()));
        let (port, _) = instrumented_transaction_port(&bound, current, conflicts.clone(), keys.clone(), order);
        let crate::command_validation::CommandCandidateChainStart::Ready(candidate) = crate::command_validation::begin_bound_command_candidate(&port, bound) else { panic!("candidate") };
        let crate::command_validation::TransactionCurrentAttemptDecision::Ready(current) = candidate.read_transaction_current() else { panic!("current") };
        let crate::command_validation::CheckedCandidateDecision::Validated(checked) = crate::command_validation::validate_checked_transaction_current(current).expect("semantic checks") else { panic!("checked result") };
        assert!(matches!(checked.recheck_row_policy(), crate::command_validation::CheckedRowPolicyDecision::Denied(_)), "missing row-policy authority must never release the chosen outcome");
        acquire_and_release(&runtime, &conflicts, keys);
    }
}


#[test]
fn selected_row_policy_denies_fault_disclosure_before_terminalization() {
    let (state, snapshot) = selection_fixture_options(Some(8), None, true, false);
    let keys = state.raw_conflict_keys.clone();
    let current = TransactionCurrentState::new(&snapshot.validation_request(), snapshot.bindings().to_vec(), vec![], vec![CurrentRangeObservation::new(snapshot.ranges()[0].target().clone(), snapshot.ranges()[0].epoch())]).expect("current");
    let conflicts = manager(); let runtime = runtime();
    let attempt = evaluate_selection(state, snapshot, &conflicts, &runtime);
    let fault = ExecutionFaultAttempt::Arithmetic { state: attempt.state, lease: attempt.lease, snapshot: attempt.snapshot };
    let order = Rc::new(RefCell::new(Vec::new()));
    let (port, trace) = instrumented_execution_failure_port(current, FailureTerminalizeBehavior::Commit, conflicts.clone(), keys.clone(), order.clone());
    let crate::command_execution_failure::ExecutionFailureTransitionStart::Ready(current) = crate::command_execution_failure::begin_execution_failure_transition(&port, fault) else { panic!("current transition") };
    assert!(matches!(current.read_transaction_current(), crate::command_execution_failure::ExecutionFailureCurrentDecision::PolicyDenied));
    assert!(order.borrow().contains(&"failure-abandon"));
    assert!(!order.borrow().contains(&"failure-terminalize"));
    assert_eq!(trace.lease_held_at_storage_action.get(), Some(true));
    acquire_and_release(&runtime, &conflicts, keys);
}

#[test]
fn selected_entry_cannot_substitute_a_record_from_outside_its_prefix() {
    let (state, snapshot) = selection_fixture(Some(8));
    let (_, other) = selection_fixture(Some(9));
    let selection = state.snapshot_request.indexed_selection().expect("selector");
    assert!(selection.validate_record(&snapshot.ranges()[0].entries()[0], &other.bindings()[0]).is_err());
    let missing = EntityObservation::Absent(snapshot.bindings()[0].target().clone());
    assert!(selection.validate_record(&snapshot.ranges()[0].entries()[0], &missing).is_err());
}

#[test]
fn skipped_earlier_candidate_that_later_aborts_cannot_satisfy_strict_first_order() {
    // Counterexample proving why skip-locked pool allocation requires a distinct
    // contract (`mutate any`) rather than silently changing `mutate first` (ADR-0264):
    // 1. Caller A inspects/locks candidate 1 (`slot_1`),
    // 2. Caller B skips `slot_1` and commits an allocation of candidate 2 (`slot_2`),
    // 3. Caller A aborts without mutating `slot_1`.
    // Because `slot_1` remained available throughout the committed history, a
    // strict `mutate first` evaluation against the post-abort state selects `slot_1`,
    // and any candidate commit of `slot_2` whose authoritative range observation
    // advanced or whose first eligible row is still `slot_1` is rejected by
    // `mutate first` validation.
    let (state_a, snapshot_slot_1) = selection_fixture(Some(1));
    let (state_b, snapshot_slot_2) = selection_fixture(Some(2));
    let conflicts = manager();
    let runtime = runtime();

    // Caller A starts an attempt on slot_1 and cancels before commit (aborting without mutation).
    let cancellation_a = state_a.cancellation.clone();
    let order_a = Rc::new(RefCell::new(Vec::new()));
    let repo_a = ScriptedRepository::new(
        state_a.lookup_candidates.clone(),
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
            state_a.commit_context.pending().clone(),
        ))),
        Rc::clone(&order_a),
    );
    let reader_a = ScriptedSnapshotReader::new(state_a.snapshot_request.clone(), snapshot_slot_1.clone(), order_a)
        .cancelling(cancellation_a);
    assert!(matches!(
        runtime.block_on(evaluate_next_command_attempt(state_a, &repo_a, &reader_a, &conflicts)),
        Err(CommandAttemptError::Cancelled)
    ));

    // A fresh strict `mutate first` evaluation against the authoritative first candidate
    // must select `slot_1` (`[1; 16]`), never `slot_2` (`[2; 16]`).
    let (fresh_first_state, fresh_first_snapshot) = selection_fixture(Some(1));
    let expected_slot_1 = fresh_first_snapshot.bindings()[0].target().clone();
    let expected_slot_2 = snapshot_slot_2.bindings()[0].target().clone();
    assert_ne!(expected_slot_1, expected_slot_2);

    let first_attempt = evaluate_selection(fresh_first_state, fresh_first_snapshot, &conflicts, &runtime);
    assert_eq!(
        first_attempt.evaluated().mutations()[0].target(),
        &expected_slot_1,
        "strict mutate first must select the earliest available candidate in canonical index order"
    );
    drop(first_attempt);

    // Conversely, if Caller B evaluated against `slot_2` while `slot_1` remained the
    // authoritative first entry (or the range epoch advanced), strict `mutate first`
    // rejects B's candidate at transaction-current validation before any durable effect.
    let keys_b = state_b.raw_conflict_keys.clone();
    let attempt_b = evaluate_selection(state_b, snapshot_slot_2.clone(), &conflicts, &runtime);
    let bound_b = attempt_b.bind_provenance(provenance_id()).expect("provenance");
    let current_after_epoch_advance = TransactionCurrentState::new(
        &snapshot_slot_2.validation_request(),
        snapshot_slot_2.bindings().to_vec(),
        vec![],
        vec![CurrentRangeObservation::new(
            snapshot_slot_2.ranges()[0].target().clone(),
            IndexEpochPosition::Value(IndexEpoch::first()),
        )],
    )
    .expect("current state");
    let order_b = Rc::new(RefCell::new(Vec::new()));
    let (port_b, _) = instrumented_transaction_port(
        &bound_b,
        current_after_epoch_advance,
        conflicts.clone(),
        keys_b,
        order_b,
    );
    let crate::command_validation::CommandCandidateChainStart::Ready(candidate_b) =
        crate::command_validation::begin_bound_command_candidate(&port_b, bound_b)
    else {
        panic!("candidate B")
    };
    assert!(matches!(
        candidate_b.read_transaction_current(),
        crate::command_validation::TransactionCurrentAttemptDecision::DependencyChanged(_)
    ));
}

#[test]
fn all_candidates_locked_in_flight_remains_distinct_from_proven_empty_range() {
    // Contention on in-flight candidates must never be conflated with a proven
    // empty index prefix (`NoCapacity`):
    // - While an in-flight holder owns the conflict capability, a competing caller
    //   whose acquisition deadline expires fails with `CommandAttemptError::DeadlineExceeded`
    //   (retryable contention), never an empty business outcome (`0` mutations).
    // - An empty snapshot observation (`None`) cannot commit if the authoritative
    //   `(partition, index)` range epoch changed.
    let (holder_state, _) = selection_fixture(Some(1));
    let (contended_state, contended_snapshot) = selection_fixture(Some(1));
    let conflicts = manager();
    let runtime = runtime();

    let held_lease = runtime
        .block_on(conflicts.acquire_mut(
            holder_state.raw_conflict_keys.clone(),
            future_deadline(),
            CancellationToken::new(),
        ))
        .expect("holder acquires conflict lease");

    let mut expired_state = contended_state;
    expired_state.deadline = Instant::now();
    let order = Rc::new(RefCell::new(Vec::new()));
    let repository = ScriptedRepository::new(
        expired_state.lookup_candidates.clone(),
        AdmissionLookupResultV1::Found(Box::new(StoredAdmissionStateV1::Pending(
            expired_state.commit_context.pending().clone(),
        ))),
        Rc::clone(&order),
    );
    let snapshots = ScriptedSnapshotReader::new(
        expired_state.snapshot_request.clone(),
        contended_snapshot,
        order,
    );
    assert!(
        matches!(
            runtime.block_on(evaluate_next_command_attempt(
                expired_state,
                &repository,
                &snapshots,
                &conflicts
            )),
            Err(CommandAttemptError::DeadlineExceeded)
        ),
        "all-candidates-busy contention must surface as retryable contention, never as an empty pool outcome"
    );
    assert_eq!(snapshots.calls.get(), 0, "contended caller must not read or fabricate an empty snapshot");
    drop(held_lease);
}

#[test]
fn companion_and_candidate_conflict_keys_acquire_in_canonical_order_without_deadlock() {
    // Any discovered candidate conflict key combined with companion conflict keys
    // must be acquired as a single canonical set so reversed caller key orderings
    // cannot deadlock across shards.
    let conflicts = manager();
    let runtime = runtime();
    let (state_a, _) = selection_fixture(Some(1));
    let companion_key = state_a.raw_conflict_keys[0].clone();
    let mut candidate_bytes = companion_key.as_bytes().to_vec();
    *candidate_bytes.last_mut().expect("non-empty conflict key") ^= 0xFF;
    let candidate_key = ConflictKey::new(candidate_bytes).expect("distinct candidate conflict key");
    assert_ne!(companion_key, candidate_key);

    let lease_ab = runtime
        .block_on(conflicts.acquire_mut(
            vec![companion_key.clone(), candidate_key.clone()],
            future_deadline(),
            CancellationToken::new(),
        ))
        .expect("canonical acquisition of [companion, candidate]");
    assert_eq!(lease_ab.key_count(), 2);
    drop(lease_ab);

    let lease_ba = runtime
        .block_on(conflicts.acquire_mut(
            vec![candidate_key, companion_key],
            future_deadline(),
            CancellationToken::new(),
        ))
        .expect("canonical acquisition of [candidate, companion]");
    assert_eq!(lease_ba.key_count(), 2);
    drop(lease_ba);
}

#[test]
fn strict_first_selection_participates_in_transaction_local_serial_micro_batching() {
    let (mut first_state, _) = selection_fixture(Some(1));
    let (mut second_state, _) = selection_fixture(Some(2));
    first_state.terminal_admission = true;
    second_state.terminal_admission = true;
    assert!(first_state.serial_micro_batch_eligible());
    assert!(second_state.serial_micro_batch_eligible());

    let conflicts = manager();
    let runtime = runtime();
    let acquired = runtime
        .block_on(acquire_transaction_local_serial_group(
            vec![first_state, second_state],
            &conflicts,
        ))
        .expect("same-pool mutate first attempts share one transaction-local serial group lease");
    assert_eq!(acquired.len(), 2);
}
