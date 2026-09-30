//! Atomic allocation, empty-result replay, and release against real redb.
// req: TXN-001, TXN-002, CMP-015, PERF-012
use super::*;
use riffdb_commit::{CommandExecutionResult, CommittedOutcomeDisposition};
use riffdb_storage_api::EntityTarget;

const SOURCE: &str = r"
contract SlotBudgets version 1 {
  entity Budget {
    key (organization_id: uuid, fiscal_year: i64)
    field approved_amount: decimal<28,2>
    field free: bool
    field holder: uuid
    index by_free (organization_id, free, fiscal_year)
  }
  event AllocatedSlot { year: i64 }
  aggregate Budgets {
    root Budget
    partition_by organization_id
    conflict_key (organization_id)
  }
  command CreateBudget {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input approved_amount: decimal<28,2>
    idempotency_key idempotency_key
    create Budget(organization_id, fiscal_year) as budget else Exists {}
    set budget.approved_amount = approved_amount
    set budget.free = true
    set budget.holder = organization_id
    return Created {}
  }
  command Allocate {
    input idempotency_key: string<128>
    input organization_id: uuid
    input holder: uuid
    idempotency_key idempotency_key
    mutate first Budget.by_free(organization_id, true) as budget else NoCapacity {}
    require available: budget.free else Taken {}
    set budget.free = false checked_by available
    set budget.holder = holder checked_by available
    emit AllocatedSlot { year: budget.fiscal_year }
    return Allocated { year: budget.fiscal_year }
  }
  command Release {
    input idempotency_key: string<128>
    input organization_id: uuid
    input fiscal_year: i64
    input holder: uuid
    idempotency_key idempotency_key
    mutate Budget(organization_id, fiscal_year) as budget else Missing {}
    require owner: budget.holder == holder else WrongOwner {}
    set budget.free = true checked_by owner
    return Released {}
  }
}
";

fn prepare(
    database: &BudgetDatabase,
    ports: &RedbOperationalPorts,
    name: &str,
    seed: u8,
    attempt: u8,
    year: i64,
    holder: u8,
) -> CommandExecutionPreparation {
    prepare_in_pool(database, ports, name, seed, attempt, year, holder, 0)
}

#[allow(clippy::too_many_arguments)]
fn prepare_in_pool(
    database: &BudgetDatabase,
    ports: &RedbOperationalPorts,
    name: &str,
    seed: u8,
    attempt: u8,
    year: i64,
    holder: u8,
    pool: i64,
) -> CommandExecutionPreparation {
    let plan = database
        .bundle()
        .bundle()
        .commands()
        .iter()
        .find(|plan| plan.name() == name)
        .expect("plan");
    let key = format!("selection-{seed}");
    let values = [
        (
            "idempotency_key",
            CanonicalValue::string(&key).expect("key"),
        ),
        ("organization_id", CanonicalValue::Uuid(ORGANIZATION_ID)),
        ("fiscal_year", CanonicalValue::I64(year)),
        ("pool", CanonicalValue::I64(pool)),
        ("approved_amount", decimal(10_000)),
        ("holder", CanonicalValue::Uuid([holder; 16])),
    ];
    let input = CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    values
                        .iter()
                        .find(|(name, _)| *name == field.name())
                        .expect("field")
                        .1
                        .clone(),
                )
            })
            .collect(),
    )
    .expect("input");
    database.prepare_plan(ports, plan, input, &key, seed, attempt)
}

#[test]
fn allocation_selects_first_then_persists_empty_and_replays_it_after_release() {
    let database =
        BudgetDatabase::create_from_source_with_setup("indexed-selection", SOURCE, |_, _| {});
    let ports = database.open();
    // Submit both callers with no observed slot identity. The coordinator owns
    // selection and conflict serialization for each complete command.
    let requests = [
        ("CreateBudget", 1, 1, FISCAL_YEAR + 1, 0, "Created"),
        ("CreateBudget", 2, 2, FISCAL_YEAR, 0, "Created"),
        ("Allocate", 10, 10, 0, 10, "Allocated"),
        ("Allocate", 11, 11, 0, 11, "Allocated"),
        ("Allocate", 12, 12, 0, 12, "NoCapacity"),
        ("Release", 13, 13, FISCAL_YEAR, 10, "Released"),
        ("Allocate", 12, 14, 0, 12, "NoCapacity"),
        ("Allocate", 15, 15, 0, 15, "Allocated"),
    ];
    let prepared: Vec<_> = requests
        .iter()
        .map(|(name, seed, attempt, year, holder, _)| {
            prepare(&database, &ports, name, *seed, *attempt, *year, *holder)
        })
        .collect();
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(80)),
    );
    let executor = coordinator.command_executor();
    let runtime = runtime();
    let mut outcomes = Vec::new();
    for (position, (preparation, (name, _, _, _, _, expected))) in
        prepared.into_iter().zip(requests).enumerate()
    {
        let result = runtime.block_on(async {
            executor
                .reserve_capacity()
                .await
                .expect("capacity")
                .submit(preparation)
                .expect("submit")
                .completion()
                .await
                .expect("completion")
        });
        let CommandExecutionResult::Committed(outcome) = result else {
            panic!("declared outcome required: {result:?}");
        };
        let plan = database
            .bundle()
            .bundle()
            .commands()
            .iter()
            .find(|plan| plan.name() == name)
            .expect("plan");
        let declared = plan
            .outcomes()
            .iter()
            .find(|outcome| outcome.name() == expected)
            .expect("outcome");
        assert_eq!(
            outcome.stored_outcome().declared_outcome().outcome_id(),
            declared.id()
        );
        if expected == "Allocated" {
            assert_eq!(
                outcome.stored_outcome().declared_outcome().value().fields()[0].1,
                CanonicalValue::I64(if position == 3 {
                    FISCAL_YEAR + 1
                } else {
                    FISCAL_YEAR
                })
            );
        }
        if position == 6 {
            assert_eq!(outcome.disposition(), CommittedOutcomeDisposition::Replay);
            assert_eq!(outcome.stored_outcome(), &outcomes[4]);
        }
        outcomes.push(outcome.into_stored_outcome());
    }
    drop(executor);
    coordinator.shutdown().expect("drain");
    let ports = database.open();
    for outcome in outcomes {
        assert_eq!(
            ports
                .read_stored_outcome(outcome.identity())
                .expect("outcome"),
            Some(outcome.clone())
        );
        let provenance = ports
            .read_provenance(outcome.provenance_id())
            .expect("provenance")
            .expect("present");
        assert_eq!(provenance.commit_sequence(), outcome.commit_sequence());
        let commit = ports
            .read_commit(outcome.commit_sequence())
            .expect("commit")
            .expect("present");
        assert_eq!(commit.provenance_id(), outcome.provenance_id());
    }
}

fn submit(
    executor: &riffdb_commit::CommandExecutor,
    preparation: CommandExecutionPreparation,
) -> riffdb_commit::CommittedOutcome {
    let result = runtime().block_on(async {
        executor
            .reserve_capacity()
            .await
            .expect("capacity")
            .submit(preparation)
            .expect("submit")
            .completion()
            .await
            .expect("completion")
    });
    let CommandExecutionResult::Committed(outcome) = result else {
        panic!("declared outcome: {result:?}")
    };
    outcome
}

fn outcome_name<'a>(
    database: &'a BudgetDatabase,
    outcome: &riffdb_commit::CommittedOutcome,
) -> &'a str {
    let plan = database
        .bundle()
        .bundle()
        .commands()
        .iter()
        .find(|p| p.name() == "Allocate")
        .expect("plan");
    plan.outcomes()
        .iter()
        .find(|o| o.id() == outcome.stored_outcome().declared_outcome().outcome_id())
        .expect("outcome")
        .name()
}

#[test]
fn first_candidate_requirement_failure_never_skips_to_another_row() {
    let source = SOURCE.replace(
        "require available: budget.free",
        "require available: budget.free && budget.fiscal_year > 0",
    );
    let database =
        BudgetDatabase::create_from_source_with_setup("selection-no-skip", &source, |_, _| {});
    let ports = database.open();
    let preparations = [
        prepare(&database, &ports, "CreateBudget", 1, 1, -1, 0),
        prepare(&database, &ports, "CreateBudget", 2, 2, 1, 0),
        prepare(&database, &ports, "Allocate", 3, 3, 0, 3),
    ];
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(50)),
    );
    let executor = coordinator.command_executor();
    for (position, preparation) in preparations.into_iter().enumerate() {
        let result = submit(&executor, preparation);
        if position == 2 {
            assert_eq!(outcome_name(&database, &result), "Taken");
        }
    }
    drop(executor);
    coordinator.shutdown().expect("drain");
}

#[test]
fn uncertain_selection_commit_resolves_identity_before_another_allocation() {
    let database =
        BudgetDatabase::create_from_source_with_setup("selection-unknown", SOURCE, |_, _| {});
    seed(&database, 2, false);
    let controller = RedbTestController::return_unknown_after_commit(
        riffdb_storage_redb::RedbTestOperation::CommandBatch,
    );
    let ports = database.open_with_controller(controller.clone());
    let preparation = prepare(&database, &ports, "Allocate", 101, 101, 0, 1);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(100)),
    );
    let executor = coordinator.command_executor();
    let first = submit(&executor, preparation);
    assert_eq!(outcome_name(&database, &first), "Allocated");
    assert_eq!(controller.indexed_selection_observations(), (1, 1, 1));
    assert_eq!(
        executor.lifecycle_state(),
        riffdb_commit::CoordinatorLifecycleState::Fenced
    );
    drop(executor);
    coordinator.shutdown().expect("drain");
    let ports = database.open();
    let preparation = prepare(&database, &ports, "Allocate", 101, 102, 0, 1);
    let next = prepare(&database, &ports, "Allocate", 103, 103, 0, 2);
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(110)),
    );
    let executor = coordinator.command_executor();
    let replay = submit(&executor, preparation);
    assert_eq!(replay.disposition(), CommittedOutcomeDisposition::Replay);
    assert_eq!(first.stored_outcome(), replay.stored_outcome());
    let next = submit(&executor, next);
    assert_ne!(
        first.stored_outcome().declared_outcome().value(),
        next.stored_outcome().declared_outcome().value()
    );
    drop(executor);
    coordinator.shutdown().expect("drain");
}

fn seed(database: &BudgetDatabase, population: u8, independent: bool) {
    let ports = database.open();
    let preparations: Vec<_> = (0..population)
        .map(|n| {
            prepare_in_pool(
                database,
                &ports,
                "CreateBudget",
                n,
                n,
                i64::from(n),
                0,
                if independent { i64::from(n % 32) } else { 0 },
            )
        })
        .collect();
    let coordinator = start_coordinator(
        ports,
        Arc::new(FixedAdmissionClock::new(command_timestamp())),
        Arc::new(IncrementingProvenanceSource::new(0)),
    );
    let executor = coordinator.command_executor();
    for preparation in preparations {
        submit(&executor, preparation);
    }
    drop(executor);
    coordinator.shutdown().expect("seed drain");
}

#[derive(Default)]
struct LockWaits(Mutex<Vec<Duration>>, std::sync::Condvar);
impl riffdb_conflict::ConflictObserver for LockWaits {
    fn observe(&self, event: riffdb_conflict::ConflictEvent) {
        if event.kind() == riffdb_conflict::ConflictEventKind::Acquired {
            self.0.lock().expect("waits").push(event.wait_duration());
            self.1.notify_all();
        }
    }
}

// A fresh database for each population/concurrency case; every worker starts
// through a barrier, never a timing sleep. Independent pools share a partition.
#[test]
fn concurrent_selection_has_bounded_work_in_shared_and_independent_pools() {
    let source = SOURCE
        .replace("fiscal_year: i64)", "pool: i64, fiscal_year: i64)")
        .replace(
            "index by_free (organization_id, free, fiscal_year)",
            "index by_free (organization_id, pool, free, fiscal_year)",
        )
        .replace(
            "conflict_key (organization_id)",
            "conflict_key (organization_id, pool)",
        )
        .replace(
            "input organization_id: uuid",
            "input organization_id: uuid\n    input pool: i64",
        )
        .replace(
            "Budget(organization_id, fiscal_year)",
            "Budget(organization_id, pool, fiscal_year)",
        )
        .replace(
            "by_free(organization_id, true)",
            "by_free(organization_id, pool, true)",
        );
    let mut evidence = Vec::new();
    for population in [32_u8, 128] {
        for clients in [1_usize, 8, 32] {
            for independent in [false, true] {
                let database = BudgetDatabase::create_from_source_with_setup(
                    "selection-race",
                    &source,
                    |_, _| {},
                );
                seed(&database, population, independent);
                let controller = RedbTestController::observe_index_migration();
                let ports = database.open_with_controller(controller.clone());
                let mut work: Vec<Vec<_>> = (0..clients).map(|_| Vec::new()).collect();
                for n in 0..32_u8 {
                    work[usize::from(n) % clients].push(prepare_in_pool(
                        &database,
                        &ports,
                        "Allocate",
                        160 + n,
                        160 + n,
                        0,
                        n,
                        if independent { i64::from(n) } else { 0 },
                    ));
                }
                let waits = Arc::new(LockWaits::default());
                let conflicts = Arc::new(
                    ShardedConflictManager::with_observer(
                        ConflictManagerConfig::default(),
                        waits.clone(),
                    )
                    .expect("conflicts"),
                );
                let coordinator = RunningCommandCoordinator::start(
                    CoordinatorWorkloadCapacity::new(64).expect("capacity"),
                    riffdb_commit::CoordinatorDurability::Group,
                    ports,
                    conflicts,
                    Arc::new(FixedAdmissionClock::new(command_timestamp())),
                    Arc::new(FixedServiceUuidV7Source),
                    Arc::new(FixedAdministrationClock(timestamp(1_700_000_002))),
                    Arc::new(FixedAuthorizationClock(timestamp(1_700_000_002))),
                    Arc::new(IncrementingProvenanceSource::new(160)),
                    Arc::new(DiscardApplicationCommitNotifications),
                )
                .expect("group coordinator");
                let executor = coordinator.command_executor();
                let barrier = Arc::new(std::sync::Barrier::new(clients + 1));
                let (elapsed, mut results) = std::thread::scope(|scope| {
                    let handles: Vec<_> = work
                        .into_iter()
                        .map(|requests| {
                            let executor = executor.clone();
                            let barrier = barrier.clone();
                            scope.spawn(move || {
                                barrier.wait();
                                requests
                                    .into_iter()
                                    .map(|request| {
                                        let start = Instant::now();
                                        let outcome = submit(&executor, request);
                                        (start.elapsed(), outcome)
                                    })
                                    .collect::<Vec<_>>()
                            })
                        })
                        .collect();
                    let start = Instant::now();
                    barrier.wait();
                    let results = handles
                        .into_iter()
                        .flat_map(|handle| handle.join().expect("worker"))
                        .collect::<Vec<_>>();
                    (start.elapsed(), results)
                });
                let mut selected = std::collections::BTreeSet::new();
                for (_, outcome) in &results {
                    assert_eq!(outcome_name(&database, outcome), "Allocated");
                    let CanonicalValue::I64(year) =
                        outcome.stored_outcome().declared_outcome().value().fields()[0].1
                    else {
                        panic!("year")
                    };
                    assert!(selected.insert(year), "no duplicate allocation");
                }
                // Independent in-memory oracle: each pool removes its least
                // available key. Compare in durable commit order, not thread order.
                let mut model: std::collections::BTreeMap<i64, std::collections::BTreeSet<i64>> =
                    std::collections::BTreeMap::new();
                for year in 0..i64::from(population) {
                    model
                        .entry(if independent { year % 32 } else { 0 })
                        .or_default()
                        .insert(year);
                }
                let mut ordered: Vec<_> = results.iter().map(|(_, outcome)| outcome).collect();
                ordered.sort_by_key(|outcome| outcome.stored_outcome().commit_sequence());
                for outcome in ordered {
                    let CanonicalValue::I64(year) =
                        outcome.stored_outcome().declared_outcome().value().fields()[0].1
                    else {
                        panic!("year")
                    };
                    let pool = if independent { year % 32 } else { 0 };
                    assert_eq!(
                        model.get_mut(&pool).expect("pool").pop_first(),
                        Some(year),
                        "redb selection matches the memory oracle"
                    );
                }
                drop(executor);
                coordinator.shutdown().expect("drain");
                let (reads, entries, max_entries) = controller.indexed_selection_observations();
                assert_eq!((reads, entries, max_entries), (32, 32, 1));
                results.sort_by_key(|(elapsed, _)| *elapsed);
                let lock = waits.0.lock().expect("waits");
                let (lock, timeout) = waits
                    .1
                    .wait_timeout_while(lock, Duration::from_secs(10), |events| events.len() < 32)
                    .expect("observer drained");
                assert!(!timeout.timed_out());
                assert_eq!(lock.len(), 32);
                let lock_ns: u128 = lock.iter().map(Duration::as_nanos).sum();
                evidence.push(format!("{{\"population\":{population},\"clients\":{clients},\"independent_pools\":{independent},\"commands\":32,\"elapsed_ns\":{},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"selection_retries\":{},\"lock_wait_ns\":{lock_ns},\"inspected_entries\":{entries},\"max_entries_per_evaluation\":{max_entries}}}", elapsed.as_nanos(), results[15].0.as_nanos(), results[30].0.as_nanos(), results[31].0.as_nanos(), reads - 32));
            }
        }
    }
    if let Some(path) = std::env::var_os("RIFFDB_WP817_EVIDENCE") {
        std::fs::write(
            path,
            format!(
                "{{\"schema\":1,\"profile\":\"{}\",\"durability\":\"group\",\"cases\":[{}]}}\n",
                if cfg!(debug_assertions) {
                    "debug"
                } else {
                    "release"
                },
                evidence.join(",")
            ),
        )
        .expect("benchmark evidence");
    }
}

#[test]
fn selection_process_crash_recovers_atomic_effects_and_exact_replay() {
    const CHILD_PATH: &str = "RIFFDB_WP817_CRASH_PATH";
    const CHILD_PHASE: &str = "RIFFDB_WP817_CRASH_PHASE";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let checked_bundle = ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(SOURCE).expect("contract"),
        )
        .expect("bundle");
        let entity = &checked_bundle.bundle().schema().entities()[0];
        let database = BudgetDatabase {
            path: path.into(),
            target_entity_type: entity.id(),
            approved_amount_field: entity
                .record()
                .fields()
                .iter()
                .find(|f| f.name() == "approved_amount")
                .expect("field")
                .id(),
            checked_bundle,
            _scratch: ScratchDir::new("selection-crash-child").expect("scratch"),
        };
        let controller = if std::env::var(CHILD_PHASE).expect("phase") == "before" {
            RedbTestController::abort_before_commit(
                riffdb_storage_redb::RedbTestOperation::CommandBatch,
            )
        } else {
            RedbTestController::abort_after_commit(
                riffdb_storage_redb::RedbTestOperation::CommandBatch,
            )
        };
        let ports = database.open_with_controller(controller);
        let preparation = prepare(&database, &ports, "Allocate", 101, 101, 0, 1);
        let coordinator = start_coordinator(
            ports,
            Arc::new(FixedAdmissionClock::new(command_timestamp())),
            Arc::new(IncrementingProvenanceSource::new(100)),
        );
        submit(&coordinator.command_executor(), preparation);
        panic!("armed process boundary did not abort");
    }
    for phase in ["before", "after"] {
        let database =
            BudgetDatabase::create_from_source_with_setup("selection-crash", SOURCE, |_, _| {});
        seed(&database, 2, false);
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg(std::thread::current().name().expect("named test thread"))
            .arg("--nocapture")
            .env(CHILD_PATH, database.path())
            .env(CHILD_PHASE, phase)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("child");
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(6), "closed failpoint must abort");
        }
        let ports = database.open();
        let first = prepare(&database, &ports, "Allocate", 101, 102, 0, 1);
        let next = prepare(&database, &ports, "Allocate", 103, 103, 0, 2);
        let coordinator = start_coordinator(
            ports,
            Arc::new(FixedAdmissionClock::new(command_timestamp())),
            Arc::new(IncrementingProvenanceSource::new(110)),
        );
        let executor = coordinator.command_executor();
        let first = submit(&executor, first);
        assert_eq!(
            first.disposition(),
            if phase == "after" {
                CommittedOutcomeDisposition::Replay
            } else {
                CommittedOutcomeDisposition::FirstCommit
            }
        );
        let next = submit(&executor, next);
        assert_ne!(
            first.stored_outcome().declared_outcome().value(),
            next.stored_outcome().declared_outcome().value()
        );
        drop(executor);
        coordinator.shutdown().expect("drain");
        let ports = database.open();
        for outcome in [first, next] {
            let stored = outcome.stored_outcome();
            assert_eq!(
                ports
                    .read_stored_outcome(stored.identity())
                    .expect("outcome"),
                Some(stored.clone())
            );
            let commit = ports
                .read_commit(stored.commit_sequence())
                .expect("commit")
                .expect("exists");
            assert_eq!(commit.events().len(), 1);
            assert_eq!(commit.provenance_id(), stored.provenance_id());
            assert!(
                ports
                    .read_provenance(stored.provenance_id())
                    .expect("provenance")
                    .is_some()
            );
            for event in commit.events() {
                assert_eq!(
                    ports.read_durable_event(event.event_id()).expect("event"),
                    Some(event.clone())
                );
            }
            let year = stored.declared_outcome().value().fields()[0].1.clone();
            let entity = &database.bundle().bundle().schema().entities()[0];
            let key = entity
                .primary_key()
                .encode_entity(&[CanonicalValue::Uuid(ORGANIZATION_ID), year])
                .expect("key");
            let row = ports
                .read_entity(&EntityTarget::new(entity.id(), key).expect("target"))
                .expect("row")
                .expect("allocated");
            let free = entity
                .record()
                .fields()
                .iter()
                .find(|f| f.name() == "free")
                .expect("field")
                .id();
            assert_eq!(
                row.fields()
                    .fields()
                    .iter()
                    .find(|(f, _)| *f == free)
                    .expect("free")
                    .1,
                CanonicalValue::Bool(false)
            );
        }
    }
}
