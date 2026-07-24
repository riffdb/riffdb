//! Deliberately unsafe PostgreSQL controls, isolated from benchmark adapters.

use std::error::Error;
use std::fmt;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use postgres::{Client, Config, IsolationLevel, NoTls, Transaction};
use riffdb_budget_comparison_core::{
    AllocateBudget, Amount, BudgetBackend, BudgetKey, BudgetOperation, BudgetOutcome,
    ContentionWorkload, CreateBudget, MatterId, OperationId, OrganizationId,
    WorkloadIdempotencyKey, expected_contention_observation, verify_contention,
};
use riffdb_budget_comparison_postgres::PostgresBudgetAdapter;

use crate::{
    DeclaredOutcomeName, PostgresDirectDmlObservation, PostgresDuplicateRetryObservation,
    PostgresLostUpdateObservation, PostgresSameKeyDifferentInputObservation, ProtectedPostgresUrl,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const TRANSACTION_SETTINGS: &str = "SET LOCAL synchronous_commit = on; \
    SET LOCAL lock_timeout = '5s'; \
    SET LOCAL statement_timeout = '10s'; \
    SET LOCAL idle_in_transaction_session_timeout = '10s'";

/// Fixed inputs for all four accepted safety scenarios.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafetyWorkloads {
    /// Two 80.00 allocations against one 100.00 budget.
    pub lost_update: ContentionWorkload,
    /// Seed for the direct-DML scenario.
    pub direct_dml_seed: CreateBudget,
    /// Initial valid 30.00 allocation for the direct-DML scenario.
    pub direct_dml_initial: AllocateBudget,
    /// Rejected -10.00 allocation for the direct-DML scenario.
    pub direct_dml_invalid: AllocateBudget,
    /// Seed for the discarded-response replay scenario.
    pub duplicate_retry_seed: CreateBudget,
    /// 30.00 request submitted twice with one identity.
    pub duplicate_retry_allocation: AllocateBudget,
    /// Seed for the different-input identity scenario.
    pub same_key_seed: CreateBudget,
    /// First 30.00 request under the reused identity.
    pub same_key_first: AllocateBudget,
    /// Second 40.00 request under the reused identity.
    pub same_key_mismatch: AllocateBudget,
}

/// Returns stable, pairwise-disjoint entity identities for the four scenarios.
#[must_use]
pub fn safety_workloads() -> SafetyWorkloads {
    let lost_key = key("018f22a1-7b3c-7def-8123-456789ab3001");
    let direct_key = key("018f22a1-7b3c-7def-8123-456789ab3002");
    let duplicate_key = key("018f22a1-7b3c-7def-8123-456789ab3003");
    let mismatch_key = key("018f22a1-7b3c-7def-8123-456789ab3004");
    SafetyWorkloads {
        lost_update: ContentionWorkload {
            case_id: operation_id("safety-lost-update"),
            seed: create("safety-lost-seed", "wp139-lost-seed", lost_key),
            contenders: [
                allocate(
                    "safety-lost-a",
                    "wp139-lost-a",
                    lost_key,
                    "018f22a1-7b3c-7def-8123-456789ab3101",
                    "80.00",
                ),
                allocate(
                    "safety-lost-b",
                    "wp139-lost-b",
                    lost_key,
                    "018f22a1-7b3c-7def-8123-456789ab3102",
                    "80.00",
                ),
            ],
        },
        direct_dml_seed: create("safety-direct-seed", "wp139-direct-seed", direct_key),
        direct_dml_initial: allocate(
            "safety-direct-thirty",
            "wp139-direct-thirty",
            direct_key,
            "018f22a1-7b3c-7def-8123-456789ab3201",
            "30.00",
        ),
        direct_dml_invalid: allocate(
            "safety-direct-negative",
            "wp139-direct-negative",
            direct_key,
            "018f22a1-7b3c-7def-8123-456789ab3202",
            "-10.00",
        ),
        duplicate_retry_seed: create("safety-retry-seed", "wp139-retry-seed", duplicate_key),
        duplicate_retry_allocation: allocate(
            "safety-retry-thirty",
            "wp139-retry-same-key",
            duplicate_key,
            "018f22a1-7b3c-7def-8123-456789ab3301",
            "30.00",
        ),
        same_key_seed: create("safety-mismatch-seed", "wp139-mismatch-seed", mismatch_key),
        same_key_first: allocate(
            "safety-mismatch-thirty",
            "wp139-secret-canary-same-key",
            mismatch_key,
            "018f22a1-7b3c-7def-8123-456789ab3401",
            "30.00",
        ),
        same_key_mismatch: allocate(
            "safety-mismatch-forty",
            "wp139-secret-canary-same-key",
            mismatch_key,
            "018f22a1-7b3c-7def-8123-456789ab3401",
            "40.00",
        ),
    }
}

/// All PostgreSQL observations used to assemble the final report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PostgresNegativeControlObservations {
    /// Unlocked check-then-write result.
    pub lost_update: PostgresLostUpdateObservation,
    /// Direct-DML precondition bypass result.
    pub direct_dml: PostgresDirectDmlObservation,
    /// Discarded-response duplicate retry result.
    pub duplicate_retry: PostgresDuplicateRetryObservation,
    /// Same-key/different-input result.
    pub same_key_different_input: PostgresSameKeyDifferentInputObservation,
}

/// Conspicuously unsafe PostgreSQL scenario harness.
///
/// This type intentionally does not implement `BudgetBackend` and is not a
/// benchmark adapter.
#[derive(Clone)]
pub struct PostgresSafetyNegativeControl {
    config: Config,
    canonical: PostgresBudgetAdapter,
}

impl PostgresSafetyNegativeControl {
    /// Creates the isolated negative-control harness.
    pub fn new(url: &ProtectedPostgresUrl) -> Result<Self, PostgresNegativeControlError> {
        let mut config = url
            .expose_for_connection()
            .parse::<Config>()
            .map_err(|_| PostgresNegativeControlError::InvalidConfiguration)?;
        config.connect_timeout(CONNECT_TIMEOUT);
        let canonical = PostgresBudgetAdapter::new(url.expose_for_connection())
            .map_err(|_| PostgresNegativeControlError::InvalidConfiguration)?;
        Ok(Self { config, canonical })
    }

    /// Runs the closed four-scenario PostgreSQL suite.
    ///
    /// Each scenario recreates the dedicated destructive-test table.
    pub fn run_all(
        &self,
        workloads: &SafetyWorkloads,
    ) -> Result<PostgresNegativeControlObservations, PostgresNegativeControlError> {
        self.verify_server_settings()?;
        Ok(PostgresNegativeControlObservations {
            lost_update: self.run_lost_update(&workloads.lost_update)?,
            direct_dml: self.run_direct_dml(
                &workloads.direct_dml_seed,
                &workloads.direct_dml_initial,
                &workloads.direct_dml_invalid,
            )?,
            duplicate_retry: self.run_duplicate_retry(
                &workloads.duplicate_retry_seed,
                &workloads.duplicate_retry_allocation,
            )?,
            same_key_different_input: self.run_same_key_different_input(
                &workloads.same_key_seed,
                &workloads.same_key_first,
                &workloads.same_key_mismatch,
            )?,
        })
    }

    /// Runs the deterministic unlocked lost-update control.
    pub fn run_lost_update(
        &self,
        workload: &ContentionWorkload,
    ) -> Result<PostgresLostUpdateObservation, PostgresNegativeControlError> {
        self.reset_schema()?;
        let canonical_observation = self
            .canonical
            .run_contention(workload)
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
        let expected = expected_contention_observation(workload)
            .map_err(|_| PostgresNegativeControlError::InvalidWorkload)?;
        verify_contention(&expected, &canonical_observation)
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;

        self.reset_schema()?;
        require_created(
            self.canonical
                .execute(&BudgetOperation::Create(workload.seed.clone())),
        )?;

        let (event_sender, event_receiver) = mpsc::sync_channel(4);
        let (first_sender, first_receiver) = mpsc::sync_channel(1);
        let (second_sender, second_receiver) = mpsc::sync_channel(1);
        let first_config = self.config.clone();
        let first = workload.contenders[0].clone();
        let first_events = event_sender.clone();
        let first_worker = thread::spawn(move || {
            negative_lost_update_worker(
                WorkerId::First,
                first_config,
                first,
                first_receiver,
                first_events,
            )
        });
        let second_config = self.config.clone();
        let second = workload.contenders[1].clone();
        let second_worker = thread::spawn(move || {
            negative_lost_update_worker(
                WorkerId::Second,
                second_config,
                second,
                second_receiver,
                event_sender,
            )
        });

        let scheduled = schedule_lost_update(
            &event_receiver,
            &first_sender,
            &second_sender,
            workload.contenders[0].amount,
        );
        if scheduled.is_err() {
            let _ = first_sender.send(WorkerControl::Cancel);
            let _ = second_sender.send(WorkerControl::Cancel);
        }
        let first_result = first_worker
            .join()
            .map_err(|_| PostgresNegativeControlError::WorkerPanicked)?;
        let second_result = second_worker
            .join()
            .map_err(|_| PostgresNegativeControlError::WorkerPanicked)?;
        scheduled?;
        first_result?;
        second_result?;

        let (final_amount, row_checks_hold) = self.read_amount_and_checks(workload.seed.key)?;
        let logical_accepted_amount = workload.contenders[0]
            .amount
            .checked_add(workload.contenders[1].amount)
            .map_err(|_| PostgresNegativeControlError::Arithmetic)?;
        if final_amount != amount("80.00")
            || logical_accepted_amount != amount("160.00")
            || !row_checks_hold
        {
            return Err(PostgresNegativeControlError::UnexpectedOutcome);
        }
        Ok(PostgresLostUpdateObservation {
            accepted_count: 2,
            canonical_adapter_oracle_passed: true,
            final_allocated_amount: final_amount,
            logical_accepted_amount,
            row_checks_hold,
        })
    }

    /// Runs the direct-DML command-precondition bypass.
    pub fn run_direct_dml(
        &self,
        seed: &CreateBudget,
        initial: &AllocateBudget,
        invalid: &AllocateBudget,
    ) -> Result<PostgresDirectDmlObservation, PostgresNegativeControlError> {
        self.reset_schema()?;
        require_created(
            self.canonical
                .execute(&BudgetOperation::Create(seed.clone())),
        )?;
        require_allocated(
            self.canonical
                .execute(&BudgetOperation::Allocate(initial.clone())),
        )?;
        let invalid_result = self
            .canonical
            .execute(&BudgetOperation::Allocate(invalid.clone()))
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
        if !matches!(invalid_result.outcome, BudgetOutcome::InvalidAmount { .. }) {
            return Err(PostgresNegativeControlError::UnexpectedOutcome);
        }

        let mut client = self.connect()?;
        let mut transaction = begin_transaction(&mut client)?;
        let organization = invalid.key.organization_id.to_string();
        let requested = invalid.amount.to_string();
        let updated = transaction
            .execute(
                "UPDATE riffdb_wp045_budget_v1 \
                 SET allocated_amount = allocated_amount + $3::text::numeric(28,2), \
                     updated_at = transaction_timestamp() \
                 WHERE organization_id = $1::text::uuid AND fiscal_year = $2",
                &[&organization, &invalid.key.fiscal_year, &requested],
            )
            .map_err(|_| PostgresNegativeControlError::Database)?;
        if updated != 1 {
            return Err(PostgresNegativeControlError::UnexpectedRowCount);
        }
        transaction
            .commit()
            .map_err(|_| PostgresNegativeControlError::Database)?;
        let (final_amount, row_checks_hold) = self.read_amount_and_checks(seed.key)?;
        if initial.amount != amount("30.00")
            || invalid.amount != amount("-10.00")
            || final_amount != amount("20.00")
            || !row_checks_hold
        {
            return Err(PostgresNegativeControlError::UnexpectedOutcome);
        }
        Ok(PostgresDirectDmlObservation {
            direct_dml_committed: true,
            final_allocated_amount: final_amount,
            requested_amount: invalid.amount,
            row_checks_hold,
            safe_adapter_outcome: DeclaredOutcomeName::InvalidAmount,
            starting_allocated_amount: initial.amount,
        })
    }

    /// Runs an intentionally non-idempotent retry after discarding one result.
    pub fn run_duplicate_retry(
        &self,
        seed: &CreateBudget,
        allocation: &AllocateBudget,
    ) -> Result<PostgresDuplicateRetryObservation, PostgresNegativeControlError> {
        self.reset_schema()?;
        require_created(
            self.canonical
                .execute(&BudgetOperation::Create(seed.clone())),
        )?;
        let first = self
            .canonical
            .execute(&BudgetOperation::Allocate(allocation.clone()))
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
        require_allocated(Ok(first.clone()))?;
        drop(first);
        let second = self
            .canonical
            .execute(&BudgetOperation::Allocate(allocation.clone()))
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
        require_allocated(Ok(second))?;
        let (final_amount, _) = self.read_amount_and_checks(seed.key)?;
        if allocation.amount != amount("30.00") || final_amount != amount("60.00") {
            return Err(PostgresNegativeControlError::UnexpectedOutcome);
        }
        Ok(PostgresDuplicateRetryObservation {
            committed_allocation_count: 2,
            final_allocated_amount: final_amount,
            first_outcome: DeclaredOutcomeName::Allocated,
            second_outcome: DeclaredOutcomeName::Allocated,
        })
    }

    /// Runs two different inputs under one ignored PostgreSQL identity.
    pub fn run_same_key_different_input(
        &self,
        seed: &CreateBudget,
        first: &AllocateBudget,
        second: &AllocateBudget,
    ) -> Result<PostgresSameKeyDifferentInputObservation, PostgresNegativeControlError> {
        self.reset_schema()?;
        require_created(
            self.canonical
                .execute(&BudgetOperation::Create(seed.clone())),
        )?;
        require_allocated(
            self.canonical
                .execute(&BudgetOperation::Allocate(first.clone())),
        )?;
        require_allocated(
            self.canonical
                .execute(&BudgetOperation::Allocate(second.clone())),
        )?;
        let (final_amount, _) = self.read_amount_and_checks(seed.key)?;
        if first.idempotency_key != second.idempotency_key
            || first.amount != amount("30.00")
            || second.amount != amount("40.00")
            || final_amount != amount("70.00")
        {
            return Err(PostgresNegativeControlError::UnexpectedOutcome);
        }
        Ok(PostgresSameKeyDifferentInputObservation {
            committed_allocation_count: 2,
            final_allocated_amount: final_amount,
            first_amount: first.amount,
            second_amount: second.amount,
        })
    }

    fn reset_schema(&self) -> Result<(), PostgresNegativeControlError> {
        self.canonical
            .reset_schema()
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)
    }

    fn verify_server_settings(&self) -> Result<(), PostgresNegativeControlError> {
        let settings = self
            .canonical
            .probe_transaction_settings()
            .map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
        if settings.server_version_num != "180004"
            || settings.isolation != "read committed"
            || settings.synchronous_commit != "on"
            || settings.fsync != "on"
            || settings.full_page_writes != "on"
            || settings.lock_timeout != "5s"
            || settings.statement_timeout != "10s"
            || settings.idle_in_transaction_session_timeout != "10s"
        {
            return Err(PostgresNegativeControlError::ServerSettings);
        }
        Ok(())
    }

    fn connect(&self) -> Result<Client, PostgresNegativeControlError> {
        self.config
            .connect(NoTls)
            .map_err(|_| PostgresNegativeControlError::Database)
    }

    fn read_amount_and_checks(
        &self,
        key: BudgetKey,
    ) -> Result<(Amount, bool), PostgresNegativeControlError> {
        let mut client = self.connect()?;
        let organization = key.organization_id.to_string();
        let row = client
            .query_one(
                "SELECT allocated_amount::text, \
                        allocated_amount >= 0.00 AND allocated_amount <= approved_amount \
                 FROM riffdb_wp045_budget_v1 \
                 WHERE organization_id = $1::text::uuid AND fiscal_year = $2",
                &[&organization, &key.fiscal_year],
            )
            .map_err(|_| PostgresNegativeControlError::Database)?;
        let amount_text = row
            .try_get::<_, String>(0)
            .map_err(|_| PostgresNegativeControlError::Decode)?;
        let amount =
            Amount::parse(&amount_text).map_err(|_| PostgresNegativeControlError::Decode)?;
        let checks = row
            .try_get::<_, bool>(1)
            .map_err(|_| PostgresNegativeControlError::Decode)?;
        Ok((amount, checks))
    }
}

impl fmt::Debug for PostgresSafetyNegativeControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgresSafetyNegativeControl([REDACTED])")
    }
}

fn begin_transaction(client: &mut Client) -> Result<Transaction<'_>, PostgresNegativeControlError> {
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .map_err(|_| PostgresNegativeControlError::Database)?;
    transaction
        .batch_execute(TRANSACTION_SETTINGS)
        .map_err(|_| PostgresNegativeControlError::Database)?;
    Ok(transaction)
}

fn negative_lost_update_worker(
    worker_id: WorkerId,
    config: Config,
    command: AllocateBudget,
    controls: mpsc::Receiver<WorkerControl>,
    events: mpsc::SyncSender<WorkerEvent>,
) -> Result<(), PostgresNegativeControlError> {
    let result = negative_lost_update_worker_inner(worker_id, config, command, &controls, &events);
    if result.is_err() {
        let _ = events.send(WorkerEvent::Failed(worker_id));
    }
    result
}

fn negative_lost_update_worker_inner(
    worker_id: WorkerId,
    config: Config,
    command: AllocateBudget,
    controls: &mpsc::Receiver<WorkerControl>,
    events: &mpsc::SyncSender<WorkerEvent>,
) -> Result<(), PostgresNegativeControlError> {
    let mut client = config
        .connect(NoTls)
        .map_err(|_| PostgresNegativeControlError::Database)?;
    let mut transaction = begin_transaction(&mut client)?;
    let organization = command.key.organization_id.to_string();
    let row = transaction
        .query_one(
            "SELECT approved_amount::text, allocated_amount::text \
             FROM riffdb_wp045_budget_v1 \
             WHERE organization_id = $1::text::uuid AND fiscal_year = $2",
            &[&organization, &command.key.fiscal_year],
        )
        .map_err(|_| PostgresNegativeControlError::Database)?;
    let approved = parse_row_amount(&row, 0)?;
    let allocated = parse_row_amount(&row, 1)?;
    let absolute_post_image = allocated
        .checked_add(command.amount)
        .map_err(|_| PostgresNegativeControlError::Arithmetic)?;
    if command.amount <= Amount::ZERO || absolute_post_image > approved {
        return Err(PostgresNegativeControlError::UnexpectedOutcome);
    }
    events
        .send(WorkerEvent::Read {
            worker_id,
            allocated,
            absolute_post_image,
        })
        .map_err(|_| PostgresNegativeControlError::Coordination)?;

    match controls
        .recv()
        .map_err(|_| PostgresNegativeControlError::Coordination)?
    {
        WorkerControl::Cancel => {
            transaction
                .rollback()
                .map_err(|_| PostgresNegativeControlError::Database)?;
            return Ok(());
        }
        WorkerControl::Commit => return Err(PostgresNegativeControlError::Coordination),
        WorkerControl::Update => {}
    }
    let post_image = absolute_post_image.to_string();
    let updated = transaction
        .execute(
            "UPDATE riffdb_wp045_budget_v1 \
             SET allocated_amount = $3::text::numeric(28,2), \
                 updated_at = transaction_timestamp() \
             WHERE organization_id = $1::text::uuid AND fiscal_year = $2",
            &[&organization, &command.key.fiscal_year, &post_image],
        )
        .map_err(|_| PostgresNegativeControlError::Database)?;
    if updated != 1 {
        return Err(PostgresNegativeControlError::UnexpectedRowCount);
    }
    events
        .send(WorkerEvent::Updated(worker_id))
        .map_err(|_| PostgresNegativeControlError::Coordination)?;

    match controls
        .recv()
        .map_err(|_| PostgresNegativeControlError::Coordination)?
    {
        WorkerControl::Cancel => {
            transaction
                .rollback()
                .map_err(|_| PostgresNegativeControlError::Database)?;
            return Ok(());
        }
        WorkerControl::Update => return Err(PostgresNegativeControlError::Coordination),
        WorkerControl::Commit => {}
    }
    transaction
        .commit()
        .map_err(|_| PostgresNegativeControlError::Database)?;
    events
        .send(WorkerEvent::Committed(worker_id))
        .map_err(|_| PostgresNegativeControlError::Coordination)
}

fn schedule_lost_update(
    events: &mpsc::Receiver<WorkerEvent>,
    first: &mpsc::SyncSender<WorkerControl>,
    second: &mpsc::SyncSender<WorkerControl>,
    requested: Amount,
) -> Result<(), PostgresNegativeControlError> {
    let mut read = [false; 2];
    for _ in 0..2 {
        match events
            .recv()
            .map_err(|_| PostgresNegativeControlError::Coordination)?
        {
            WorkerEvent::Read {
                worker_id,
                allocated,
                absolute_post_image,
            } if allocated == Amount::ZERO && absolute_post_image == requested => {
                let index = worker_id.index();
                if read[index] {
                    return Err(PostgresNegativeControlError::Coordination);
                }
                read[index] = true;
            }
            WorkerEvent::Read { .. }
            | WorkerEvent::Updated(_)
            | WorkerEvent::Committed(_)
            | WorkerEvent::Failed(_) => {
                return Err(PostgresNegativeControlError::Coordination);
            }
        }
    }
    if read != [true, true] {
        return Err(PostgresNegativeControlError::Coordination);
    }

    schedule_worker(events, first, WorkerId::First)?;
    schedule_worker(events, second, WorkerId::Second)
}

fn schedule_worker(
    events: &mpsc::Receiver<WorkerEvent>,
    controls: &mpsc::SyncSender<WorkerControl>,
    worker_id: WorkerId,
) -> Result<(), PostgresNegativeControlError> {
    controls
        .send(WorkerControl::Update)
        .map_err(|_| PostgresNegativeControlError::Coordination)?;
    if events
        .recv()
        .map_err(|_| PostgresNegativeControlError::Coordination)?
        != WorkerEvent::Updated(worker_id)
    {
        return Err(PostgresNegativeControlError::Coordination);
    }
    controls
        .send(WorkerControl::Commit)
        .map_err(|_| PostgresNegativeControlError::Coordination)?;
    if events
        .recv()
        .map_err(|_| PostgresNegativeControlError::Coordination)?
        != WorkerEvent::Committed(worker_id)
    {
        return Err(PostgresNegativeControlError::Coordination);
    }
    Ok(())
}

fn parse_row_amount(
    row: &postgres::Row,
    index: usize,
) -> Result<Amount, PostgresNegativeControlError> {
    let text = row
        .try_get::<_, String>(index)
        .map_err(|_| PostgresNegativeControlError::Decode)?;
    Amount::parse(&text).map_err(|_| PostgresNegativeControlError::Decode)
}

fn require_created(
    result: Result<
        riffdb_budget_comparison_core::CommandObservation,
        riffdb_budget_comparison_postgres::PostgresAdapterError,
    >,
) -> Result<(), PostgresNegativeControlError> {
    let observation = result.map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
    if matches!(observation.outcome, BudgetOutcome::BudgetCreated { .. }) {
        Ok(())
    } else {
        Err(PostgresNegativeControlError::UnexpectedOutcome)
    }
}

fn require_allocated(
    result: Result<
        riffdb_budget_comparison_core::CommandObservation,
        riffdb_budget_comparison_postgres::PostgresAdapterError,
    >,
) -> Result<(), PostgresNegativeControlError> {
    let observation = result.map_err(|_| PostgresNegativeControlError::CanonicalAdapter)?;
    if matches!(observation.outcome, BudgetOutcome::Allocated { .. }) {
        Ok(())
    } else {
        Err(PostgresNegativeControlError::UnexpectedOutcome)
    }
}

fn key(organization_id: &str) -> BudgetKey {
    BudgetKey {
        organization_id: match OrganizationId::parse(organization_id) {
            Ok(value) => value,
            Err(_) => unreachable!("built-in organization ID is valid"),
        },
        fiscal_year: 2026,
    }
}

fn create(operation: &str, idempotency_key: &str, key: BudgetKey) -> CreateBudget {
    CreateBudget {
        operation_id: operation_id(operation),
        idempotency_key: workload_key(idempotency_key),
        key,
        approved_amount: amount("100.00"),
    }
}

fn allocate(
    operation: &str,
    idempotency_key: &str,
    key: BudgetKey,
    matter: &str,
    value: &str,
) -> AllocateBudget {
    AllocateBudget {
        operation_id: operation_id(operation),
        idempotency_key: workload_key(idempotency_key),
        key,
        matter_id: match MatterId::parse(matter) {
            Ok(value) => value,
            Err(_) => unreachable!("built-in matter ID is valid"),
        },
        amount: amount(value),
    }
}

fn operation_id(value: &str) -> OperationId {
    match OperationId::new(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in operation ID is valid"),
    }
}

fn workload_key(value: &str) -> WorkloadIdempotencyKey {
    match WorkloadIdempotencyKey::new(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in idempotency key is valid"),
    }
}

fn amount(value: &str) -> Amount {
    match Amount::parse(value) {
        Ok(value) => value,
        Err(_) => unreachable!("built-in amount is valid"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerId {
    First,
    Second,
}

impl WorkerId {
    const fn index(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerControl {
    Update,
    Commit,
    Cancel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerEvent {
    Read {
        worker_id: WorkerId,
        allocated: Amount,
        absolute_post_image: Amount,
    },
    Updated(WorkerId),
    Committed(WorkerId),
    Failed(WorkerId),
}

/// A closed, redaction-safe PostgreSQL negative-control failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresNegativeControlError {
    /// The supplied PostgreSQL URL could not configure both adapters.
    InvalidConfiguration,
    /// PostgreSQL connection, query, update, or transaction work failed.
    Database,
    /// PostgreSQL returned a value outside the exact scenario schema.
    Decode,
    /// Fixed-scale arithmetic failed.
    Arithmetic,
    /// A built-in scenario was internally inconsistent.
    InvalidWorkload,
    /// The unchanged canonical adapter failed its role in the scenario.
    CanonicalAdapter,
    /// PostgreSQL version, transaction, wait, or durability settings differ.
    ServerSettings,
    /// A declared outcome differed from the fixed scenario.
    UnexpectedOutcome,
    /// One controlled SQL statement affected an unexpected number of rows.
    UnexpectedRowCount,
    /// Explicit worker coordination did not follow the fixed schedule.
    Coordination,
    /// A worker panicked.
    WorkerPanicked,
}

impl fmt::Display for PostgresNegativeControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "PostgreSQL safety configuration is invalid",
            Self::Database => "PostgreSQL safety operation failed",
            Self::Decode => "PostgreSQL safety result is invalid",
            Self::Arithmetic => "PostgreSQL safety amount arithmetic failed",
            Self::InvalidWorkload => "PostgreSQL safety workload is invalid",
            Self::CanonicalAdapter => "canonical PostgreSQL adapter check failed",
            Self::ServerSettings => "PostgreSQL safety server settings are invalid",
            Self::UnexpectedOutcome => "PostgreSQL safety outcome is unexpected",
            Self::UnexpectedRowCount => "PostgreSQL safety update count is unexpected",
            Self::Coordination => "PostgreSQL safety schedule coordination failed",
            Self::WorkerPanicked => "PostgreSQL safety worker panicked",
        })
    }
}

impl Error for PostgresNegativeControlError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safety_workloads_use_four_distinct_keys_and_only_intended_identity_reuse() {
        let workloads = safety_workloads();
        let keys = [
            workloads.lost_update.seed.key,
            workloads.direct_dml_seed.key,
            workloads.duplicate_retry_seed.key,
            workloads.same_key_seed.key,
        ];
        for (index, key) in keys.iter().enumerate() {
            assert!(!keys[index + 1..].contains(key));
        }
        assert_eq!(
            workloads.same_key_first.idempotency_key,
            workloads.same_key_mismatch.idempotency_key
        );
        assert_eq!(
            workloads.same_key_first.matter_id,
            workloads.same_key_mismatch.matter_id
        );
        assert_ne!(
            workloads.same_key_first.amount,
            workloads.same_key_mismatch.amount
        );
        assert_eq!(workloads.duplicate_retry_allocation.amount, amount("30.00"));
    }

    #[test]
    fn negative_control_debug_does_not_expose_configuration() {
        let url = ProtectedPostgresUrl::from_test_url(
            "postgresql://user:secret-canary@127.0.0.1/example",
        );
        let control = PostgresSafetyNegativeControl::new(&url).expect("configuration");
        let debug = format!("{control:?}");
        assert_eq!(debug, "PostgresSafetyNegativeControl([REDACTED])");
        assert!(!debug.contains("secret-canary"));
    }
}
