//! Synchronous, explicit-SQL PostgreSQL adapter for the budget comparison.

#![forbid(unsafe_code)]

use postgres::{Client, Config, IsolationLevel, NoTls, Row, Transaction};
use riffdb_budget_comparison_core::{
    AllocateBudget, Amount, BudgetBackend, BudgetKey, BudgetOperation, BudgetOutcome, BudgetState,
    CommandObservation, ContentionObservation, ContentionWorkload, CreateBudget,
    SequentialObservation, SequentialWorkload, observe_sequential,
};
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Barrier, mpsc};
use std::thread;
use std::time::Duration;

/// Exact CI image accepted for the WP-045 live correctness preflight.
pub const POSTGRES_IMAGE: &str = "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818";

/// Dedicated comparison table schema.
///
/// `approved_amount=0` is allowed only so `CreateBudget` can insert an
/// unobservable transaction-local candidate before evaluating its positive
/// approval precondition. Invalid candidates roll back; successful candidates
/// are updated before commit. This preserves duplicate-binding priority under
/// concurrent creation without weakening the durable allocated-value checks.
pub const SCHEMA_SQL: &str = r#"
DROP TABLE IF EXISTS riffdb_wp045_budget_v1;
CREATE TABLE riffdb_wp045_budget_v1 (
    organization_id UUID NOT NULL,
    fiscal_year BIGINT NOT NULL,
    approved_amount NUMERIC(28,2) NOT NULL,
    allocated_amount NUMERIC(28,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (organization_id, fiscal_year),
    CHECK (allocated_amount >= 0.00),
    CHECK (allocated_amount <= approved_amount)
);
"#;

const MAX_DATABASE_URL_BYTES: usize = 4_096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// PostgreSQL comparison adapter. Each operation opens an independent connection.
#[derive(Clone)]
pub struct PostgresBudgetAdapter {
    config: Config,
}

impl PostgresBudgetAdapter {
    /// Creates an adapter from a bounded PostgreSQL connection URL.
    pub fn new(database_url: impl Into<String>) -> Result<Self, PostgresAdapterError> {
        let database_url = database_url.into();
        if database_url.is_empty() || database_url.len() > MAX_DATABASE_URL_BYTES {
            return Err(PostgresAdapterError::InvalidConfiguration);
        }
        let mut config = database_url
            .parse::<Config>()
            .map_err(|_| PostgresAdapterError::InvalidConfiguration)?;
        config.connect_timeout(CONNECT_TIMEOUT);
        Ok(Self { config })
    }

    /// Recreates the dedicated table. Use only against an isolated test database.
    pub fn reset_schema(&self) -> Result<(), PostgresAdapterError> {
        let mut client = self.connect()?;
        client
            .batch_execute(SCHEMA_SQL)
            .map_err(|_| PostgresAdapterError::Database)
    }

    /// Runs the shared sequential workload against this adapter.
    pub fn run_sequential(
        &self,
        workload: &SequentialWorkload,
    ) -> Result<SequentialObservation, PostgresAdapterError> {
        observe_sequential(self, workload)
    }

    /// Runs both contenders from independently prepared READ COMMITTED transactions.
    ///
    /// Both workers report successful transaction preparation before receiving a
    /// go signal. They then rendezvous at a barrier immediately before their row
    /// lock query. A preparation failure cancels the peer instead of stranding it.
    pub fn run_contention(
        &self,
        workload: &ContentionWorkload,
    ) -> Result<ContentionObservation, PostgresAdapterError> {
        let seed = self.execute(&BudgetOperation::Create(workload.seed.clone()))?;
        if !matches!(seed.outcome, BudgetOutcome::BudgetCreated { .. }) {
            return Err(PostgresAdapterError::UnexpectedOutcome);
        }

        let barrier = Arc::new(Barrier::new(2));
        let (ready_sender, ready_receiver) = mpsc::channel::<bool>();
        let (control_a_sender, control_a_receiver) = mpsc::channel::<WorkerControl>();
        let (control_b_sender, control_b_receiver) = mpsc::channel::<WorkerControl>();

        let adapter_a = self.clone();
        let command_a = workload.contenders[0].clone();
        let ready_a = ready_sender.clone();
        let barrier_a = Arc::clone(&barrier);
        let worker_a = thread::spawn(move || {
            adapter_a.prepared_allocate(command_a, ready_a, control_a_receiver, barrier_a)
        });

        let adapter_b = self.clone();
        let command_b = workload.contenders[1].clone();
        let barrier_b = Arc::clone(&barrier);
        let worker_b = thread::spawn(move || {
            adapter_b.prepared_allocate(command_b, ready_sender, control_b_receiver, barrier_b)
        });

        let ready_a = ready_receiver.recv();
        let ready_b = ready_receiver.recv();
        let control = if matches!(ready_a, Ok(true)) && matches!(ready_b, Ok(true)) {
            WorkerControl::Go
        } else {
            WorkerControl::Cancel
        };
        let _ = control_a_sender.send(control);
        let _ = control_b_sender.send(control);

        let result_a = worker_a
            .join()
            .map_err(|_| PostgresAdapterError::WorkerPanicked)?;
        let result_b = worker_b
            .join()
            .map_err(|_| PostgresAdapterError::WorkerPanicked)?;
        let mut outcomes = vec![result_a?, result_b?];
        outcomes.sort();

        Ok(ContentionObservation {
            case_id: workload.case_id.clone(),
            outcomes,
            final_budget: self.read_budget(workload.seed.key)?,
        })
    }

    /// Reads the effective transaction settings used by the adapter.
    pub fn probe_transaction_settings(&self) -> Result<TransactionSettings, PostgresAdapterError> {
        let mut client = self.connect()?;
        let mut transaction = begin_transaction(&mut client)?;
        let isolation: String = transaction
            .query_one("SHOW transaction_isolation", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let synchronous_commit: String = transaction
            .query_one("SHOW synchronous_commit", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let server_version_num: String = transaction
            .query_one("SHOW server_version_num", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let fsync: String = transaction
            .query_one("SHOW fsync", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let full_page_writes: String = transaction
            .query_one("SHOW full_page_writes", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let lock_timeout: String = transaction
            .query_one("SHOW lock_timeout", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let statement_timeout: String = transaction
            .query_one("SHOW statement_timeout", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        let idle_in_transaction_session_timeout: String = transaction
            .query_one("SHOW idle_in_transaction_session_timeout", &[])
            .and_then(|row| row.try_get(0))
            .map_err(|_| PostgresAdapterError::Database)?;
        transaction
            .commit()
            .map_err(|_| PostgresAdapterError::Database)?;
        Ok(TransactionSettings {
            isolation,
            synchronous_commit,
            server_version_num,
            fsync,
            full_page_writes,
            lock_timeout,
            statement_timeout,
            idle_in_transaction_session_timeout,
        })
    }

    fn connect(&self) -> Result<Client, PostgresAdapterError> {
        self.config
            .connect(NoTls)
            .map_err(|_| PostgresAdapterError::Database)
    }

    fn execute_create(
        &self,
        command: &CreateBudget,
    ) -> Result<BudgetOutcome, PostgresAdapterError> {
        let mut client = self.connect()?;
        let mut transaction = begin_transaction(&mut client)?;
        let organization = command.key.organization_id.to_string();
        let inserted = transaction
            .query_opt(
                r#"
                INSERT INTO riffdb_wp045_budget_v1 (
                    organization_id, fiscal_year, approved_amount, allocated_amount, updated_at
                )
                VALUES ($1::text::uuid, $2, 0.00, 0.00, transaction_timestamp())
                ON CONFLICT (organization_id, fiscal_year) DO NOTHING
                RETURNING 1
                "#,
                &[&organization, &command.key.fiscal_year],
            )
            .map_err(|_| PostgresAdapterError::Database)?;

        if inserted.is_none() {
            transaction
                .commit()
                .map_err(|_| PostgresAdapterError::Database)?;
            return Ok(BudgetOutcome::BudgetAlreadyExists { key: command.key });
        }
        if command.approved_amount <= Amount::ZERO {
            transaction
                .rollback()
                .map_err(|_| PostgresAdapterError::Database)?;
            return Ok(BudgetOutcome::InvalidApprovedAmount {
                minimum: Amount::MINIMUM_POSITIVE,
            });
        }

        let approved = command.approved_amount.to_string();
        let row = transaction
            .query_one(
                r#"
                UPDATE riffdb_wp045_budget_v1
                SET approved_amount = $3::text::numeric(28,2),
                    updated_at = transaction_timestamp()
                WHERE organization_id = $1::text::uuid AND fiscal_year = $2
                RETURNING organization_id::text AS organization_id,
                          fiscal_year,
                          approved_amount::text AS approved_amount,
                          allocated_amount::text AS allocated_amount,
                          updated_at IS NOT NULL AS timestamp_present
                "#,
                &[&organization, &command.key.fiscal_year, &approved],
            )
            .map_err(|_| PostgresAdapterError::Database)?;
        let budget = decode_budget(&row)?;
        transaction
            .commit()
            .map_err(|_| PostgresAdapterError::Database)?;
        Ok(BudgetOutcome::BudgetCreated { budget })
    }

    fn execute_allocate(
        &self,
        command: &AllocateBudget,
    ) -> Result<BudgetOutcome, PostgresAdapterError> {
        let mut client = self.connect()?;
        let mut transaction = begin_transaction(&mut client)?;
        let outcome = allocate_in_transaction(&mut transaction, command)?;
        transaction
            .commit()
            .map_err(|_| PostgresAdapterError::Database)?;
        Ok(outcome)
    }

    fn prepared_allocate(
        &self,
        command: AllocateBudget,
        ready: mpsc::Sender<bool>,
        control: mpsc::Receiver<WorkerControl>,
        barrier: Arc<Barrier>,
    ) -> Result<BudgetOutcome, PostgresAdapterError> {
        let mut client = match self.connect() {
            Ok(client) => client,
            Err(error) => {
                let _ = ready.send(false);
                return Err(error);
            }
        };
        let mut transaction = match begin_transaction(&mut client) {
            Ok(transaction) => transaction,
            Err(error) => {
                let _ = ready.send(false);
                return Err(error);
            }
        };
        ready
            .send(true)
            .map_err(|_| PostgresAdapterError::Coordination)?;
        drop(ready);
        match control
            .recv()
            .map_err(|_| PostgresAdapterError::Coordination)?
        {
            WorkerControl::Cancel => {
                transaction
                    .rollback()
                    .map_err(|_| PostgresAdapterError::Database)?;
                Err(PostgresAdapterError::Coordination)
            }
            WorkerControl::Go => {
                barrier.wait();
                let outcome = allocate_in_transaction(&mut transaction, &command)?;
                transaction
                    .commit()
                    .map_err(|_| PostgresAdapterError::Database)?;
                Ok(outcome)
            }
        }
    }
}

impl BudgetBackend for PostgresBudgetAdapter {
    type Error = PostgresAdapterError;

    fn execute(&self, operation: &BudgetOperation) -> Result<CommandObservation, Self::Error> {
        let outcome = match operation {
            BudgetOperation::Create(command) => self.execute_create(command)?,
            BudgetOperation::Allocate(command) => self.execute_allocate(command)?,
        };
        Ok(CommandObservation {
            operation_id: operation.operation_id().clone(),
            outcome,
        })
    }

    fn read_budget(&self, key: BudgetKey) -> Result<Option<BudgetState>, Self::Error> {
        let mut client = self.connect()?;
        let organization = key.organization_id.to_string();
        client
            .query_opt(
                r#"
                SELECT organization_id::text AS organization_id,
                       fiscal_year,
                       approved_amount::text AS approved_amount,
                       allocated_amount::text AS allocated_amount,
                       updated_at IS NOT NULL AS timestamp_present
                FROM riffdb_wp045_budget_v1
                WHERE organization_id = $1::text::uuid AND fiscal_year = $2
                "#,
                &[&organization, &key.fiscal_year],
            )
            .map_err(|_| PostgresAdapterError::Database)?
            .as_ref()
            .map(decode_budget)
            .transpose()
    }
}

fn begin_transaction(client: &mut Client) -> Result<Transaction<'_>, PostgresAdapterError> {
    let mut transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .map_err(|_| PostgresAdapterError::Database)?;
    transaction
        .batch_execute(
            "SET LOCAL synchronous_commit = on; \
             SET LOCAL lock_timeout = '5s'; \
             SET LOCAL statement_timeout = '10s'; \
             SET LOCAL idle_in_transaction_session_timeout = '10s'",
        )
        .map_err(|_| PostgresAdapterError::Database)?;
    Ok(transaction)
}

fn allocate_in_transaction(
    transaction: &mut Transaction<'_>,
    command: &AllocateBudget,
) -> Result<BudgetOutcome, PostgresAdapterError> {
    let organization = command.key.organization_id.to_string();
    let current = transaction
        .query_opt(
            r#"
            SELECT organization_id::text AS organization_id,
                   fiscal_year,
                   approved_amount::text AS approved_amount,
                   allocated_amount::text AS allocated_amount,
                   updated_at IS NOT NULL AS timestamp_present
            FROM riffdb_wp045_budget_v1
            WHERE organization_id = $1::text::uuid AND fiscal_year = $2
            FOR UPDATE
            "#,
            &[&organization, &command.key.fiscal_year],
        )
        .map_err(|_| PostgresAdapterError::Database)?;
    let Some(current) = current.as_ref().map(decode_budget).transpose()? else {
        return Ok(BudgetOutcome::BudgetNotFound { key: command.key });
    };
    if command.amount <= Amount::ZERO {
        return Ok(BudgetOutcome::InvalidAmount {
            minimum: Amount::MINIMUM_POSITIVE,
        });
    }
    let allocated = current
        .allocated_amount
        .checked_add(command.amount)
        .map_err(|_| PostgresAdapterError::Arithmetic)?;
    if allocated > current.approved_amount {
        return Ok(BudgetOutcome::InsufficientBudget {
            approved: current.approved_amount,
            allocated: current.allocated_amount,
            requested: command.amount,
        });
    }
    let allocated_text = allocated.to_string();
    let row = transaction
        .query_one(
            r#"
            UPDATE riffdb_wp045_budget_v1
            SET allocated_amount = $3::text::numeric(28,2),
                updated_at = transaction_timestamp()
            WHERE organization_id = $1::text::uuid AND fiscal_year = $2
            RETURNING organization_id::text AS organization_id,
                      fiscal_year,
                      approved_amount::text AS approved_amount,
                      allocated_amount::text AS allocated_amount,
                      updated_at IS NOT NULL AS timestamp_present
            "#,
            &[&organization, &command.key.fiscal_year, &allocated_text],
        )
        .map_err(|_| PostgresAdapterError::Database)?;
    let budget = decode_budget(&row)?;
    let remaining = budget
        .approved_amount
        .checked_sub(budget.allocated_amount)
        .map_err(|_| PostgresAdapterError::Arithmetic)?;
    Ok(BudgetOutcome::Allocated { budget, remaining })
}

fn decode_budget(row: &Row) -> Result<BudgetState, PostgresAdapterError> {
    let organization: String = row
        .try_get("organization_id")
        .map_err(|_| PostgresAdapterError::Decode)?;
    let fiscal_year: i64 = row
        .try_get("fiscal_year")
        .map_err(|_| PostgresAdapterError::Decode)?;
    let approved: String = row
        .try_get("approved_amount")
        .map_err(|_| PostgresAdapterError::Decode)?;
    let allocated: String = row
        .try_get("allocated_amount")
        .map_err(|_| PostgresAdapterError::Decode)?;
    let timestamp_present: bool = row
        .try_get("timestamp_present")
        .map_err(|_| PostgresAdapterError::Decode)?;
    if !timestamp_present {
        return Err(PostgresAdapterError::Decode);
    }
    Ok(BudgetState {
        key: BudgetKey {
            organization_id: riffdb_budget_comparison_core::OrganizationId::parse(&organization)
                .map_err(|_| PostgresAdapterError::Decode)?,
            fiscal_year,
        },
        approved_amount: Amount::parse(&approved).map_err(|_| PostgresAdapterError::Decode)?,
        allocated_amount: Amount::parse(&allocated).map_err(|_| PostgresAdapterError::Decode)?,
    })
}

/// Reads optional live-test configuration and enforces fail-closed CI mode.
pub fn live_database_url() -> Result<Option<String>, LiveConfigurationError> {
    let required = match std::env::var("RIFFDB_BUDGET_POSTGRES_REQUIRED") {
        Ok(value) if value == "1" => true,
        Ok(value) if value == "0" || value.is_empty() => false,
        Ok(_) => return Err(LiveConfigurationError::InvalidRequiredFlag),
        Err(std::env::VarError::NotPresent) => false,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(LiveConfigurationError::InvalidRequiredFlag);
        }
    };
    match std::env::var("RIFFDB_BUDGET_POSTGRES_URL") {
        Ok(value) if value.is_empty() => {
            if required {
                Err(LiveConfigurationError::RequiredUrlMissing)
            } else {
                Ok(None)
            }
        }
        Ok(value) if value.len() <= MAX_DATABASE_URL_BYTES => Ok(Some(value)),
        Ok(_) => Err(LiveConfigurationError::InvalidUrl),
        Err(std::env::VarError::NotPresent) => {
            if required {
                Err(LiveConfigurationError::RequiredUrlMissing)
            } else {
                Ok(None)
            }
        }
        Err(std::env::VarError::NotUnicode(_)) => Err(LiveConfigurationError::InvalidUrl),
    }
}

/// Effective transaction settings observed inside one adapter transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionSettings {
    /// PostgreSQL isolation spelling.
    pub isolation: String,
    /// PostgreSQL synchronous commit spelling.
    pub synchronous_commit: String,
    /// Server version encoded as PostgreSQL's monotonic integer.
    pub server_version_num: String,
    /// Effective WAL/data-file synchronization setting.
    pub fsync: String,
    /// Effective full-page WAL protection setting.
    pub full_page_writes: String,
    /// Effective row/table lock wait bound.
    pub lock_timeout: String,
    /// Effective statement execution bound.
    pub statement_timeout: String,
    /// Effective idle transaction bound.
    pub idle_in_transaction_session_timeout: String,
}

impl fmt::Debug for PostgresBudgetAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresBudgetAdapter")
            .field("config", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerControl {
    Go,
    Cancel,
}

/// A safe PostgreSQL adapter failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PostgresAdapterError {
    /// Connection configuration is empty or over its bound.
    InvalidConfiguration,
    /// PostgreSQL connection, statement, transaction, or commit failed.
    Database,
    /// A returned row did not match the exact adapter schema.
    Decode,
    /// Exact fixed-scale arithmetic failed.
    Arithmetic,
    /// A contention worker or channel could not complete.
    Coordination,
    /// A worker panicked rather than returning a typed failure.
    WorkerPanicked,
    /// Workload setup returned an undeclared result for the requested phase.
    UnexpectedOutcome,
}

impl fmt::Display for PostgresAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration => {
                formatter.write_str("PostgreSQL adapter configuration is invalid")
            }
            Self::Database => formatter.write_str("PostgreSQL adapter operation failed"),
            Self::Decode => formatter.write_str("PostgreSQL returned an invalid budget row"),
            Self::Arithmetic => formatter.write_str("budget arithmetic exceeded decimal<28,2>"),
            Self::Coordination => formatter.write_str("contention worker coordination failed"),
            Self::WorkerPanicked => formatter.write_str("contention worker panicked"),
            Self::UnexpectedOutcome => {
                formatter.write_str("workload setup returned an unexpected outcome")
            }
        }
    }
}

impl Error for PostgresAdapterError {}

/// A safe live-test environment failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveConfigurationError {
    /// Required mode was not exactly `0`, `1`, or empty.
    InvalidRequiredFlag,
    /// Required mode was set but no URL was supplied.
    RequiredUrlMissing,
    /// URL is non-Unicode, empty in required mode, or over 4 KiB.
    InvalidUrl,
}

impl fmt::Display for LiveConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequiredFlag => {
                formatter.write_str("RIFFDB_BUDGET_POSTGRES_REQUIRED must be 0 or 1")
            }
            Self::RequiredUrlMissing => {
                formatter.write_str("required PostgreSQL live test URL is missing")
            }
            Self::InvalidUrl => formatter.write_str("PostgreSQL live test URL is invalid"),
        }
    }
}

impl Error for LiveConfigurationError {}
