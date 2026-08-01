//! Bounded engine-mechanics evidence for the command-growth benchmark.

use std::fmt;
use std::fs;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::{Duration, Instant};

use redb::{Database, Durability, ReadableDatabase, WriteTransaction};
use riffdb_storage_api::{
    AuditPrincipalV1, DatabaseInitializationPort, DatabaseInitializationResult,
    ServiceAuditAppendIntentV1, ServiceAuditAppendRepository, ServiceAuditAppendResult,
};
use riffdb_types::{
    ActorId, ActorKind, CapabilityId, DatabaseId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
    Timestamp,
};

use crate::layout::{
    COMMITS, ENTITIES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING, INDEX_EPOCHS, META,
    META_APPLICATION_SEQUENCE, OUTBOX, PROVENANCE, SECONDARY_INDEXES, create_all_tables,
};
use crate::store::{RedbDormantPorts, RedbOperationalPorts, RedbStore};

const MAX_WINDOW_COMMANDS: usize = 4_096;
const MAX_GROUP_COMMANDS: usize = 64;
const PENDING_VALUE_BYTES: usize = 512;
const ENTITY_VALUE_BYTES: usize = 768;
const INDEX_VALUE_BYTES: usize = 256;
const EPOCH_VALUE_BYTES: usize = 128;
const PROVENANCE_VALUE_BYTES: usize = 768;
const EVENT_VALUE_BYTES: usize = 512;
const OUTBOX_VALUE_BYTES: usize = 640;
const COMMIT_VALUE_BYTES: usize = 1_024;
const OUTCOME_VALUE_BYTES: usize = 768;

/// Experimental engine durability used only by the benchmark harness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineDurability {
    /// No durability; isolates page/table work from durable flush work.
    None,
    /// One-phase immediate durability.
    ImmediateOnePhase,
    /// Hardened RiffDB recovery-oracle mechanics: two-phase immediate durability.
    ImmediateTwoPhase,
}

impl EngineDurability {
    /// Stable report label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ImmediateOnePhase => "immediate_one_phase",
            Self::ImmediateTwoPhase => "immediate_two_phase",
        }
    }
}

/// Checked mechanics profile for one bounded window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMechanicsProfile {
    durability: EngineDurability,
    group_commands: usize,
}

impl EngineMechanicsProfile {
    /// Creates a bounded profile. Grouping is experimental and never changes production.
    pub fn new(
        durability: EngineDurability,
        group_commands: usize,
    ) -> Result<Self, EngineBenchmarkError> {
        if group_commands == 0 || group_commands > MAX_GROUP_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        Ok(Self {
            durability,
            group_commands,
        })
    }

    /// Returns the selected durability.
    #[must_use]
    pub const fn durability(self) -> EngineDurability {
        self.durability
    }

    /// Returns the maximum commands staged per experimental transaction.
    #[must_use]
    pub const fn group_commands(self) -> usize {
        self.group_commands
    }
}

/// Timing decomposition for one growing-database window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMechanicsSample {
    commands: usize,
    admission_work: Duration,
    admission_commit: Duration,
    terminal_work: Duration,
    terminal_commit: Duration,
    elapsed: Duration,
    file_bytes: u64,
}

impl EngineMechanicsSample {
    /// Commands completed in the window.
    #[must_use]
    pub const fn commands(self) -> usize {
        self.commands
    }

    /// Time spent staging pending admissions.
    #[must_use]
    pub const fn admission_work(self) -> Duration {
        self.admission_work
    }

    /// Time spent in pending-admission commit calls.
    #[must_use]
    pub const fn admission_commit(self) -> Duration {
        self.admission_commit
    }

    /// Time spent opening tables and staging terminal record graphs.
    #[must_use]
    pub const fn terminal_work(self) -> Duration {
        self.terminal_work
    }

    /// Time spent in terminal commit calls.
    #[must_use]
    pub const fn terminal_commit(self) -> Duration {
        self.terminal_commit
    }

    /// Complete measured window duration.
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.elapsed
    }

    /// Database file size after the window.
    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }
}

/// Closed failure from the benchmark-only engine mechanics driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineBenchmarkError {
    /// A count was zero or exceeded an accepted bound.
    InvalidConfiguration,
    /// The local database engine or filesystem operation failed.
    Engine,
}

impl fmt::Display for EngineBenchmarkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidConfiguration => "engine benchmark configuration is invalid",
            Self::Engine => "engine benchmark operation failed",
        })
    }
}

impl std::error::Error for EngineBenchmarkError {}

/// One measured window through the real durable service-audit repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceAuditGrowthSample {
    commands: usize,
    preparation: Duration,
    append: Duration,
    file_bytes: u64,
}

impl ServiceAuditGrowthSample {
    /// Complete request lifecycles appended in the window.
    #[must_use]
    pub const fn commands(self) -> usize {
        self.commands
    }

    /// Time spent constructing and semantically validating typed intents.
    #[must_use]
    pub const fn preparation(self) -> Duration {
        self.preparation
    }

    /// Time spent in the repository, including transaction-current reads,
    /// record encoding, table work, and two standard one-phase immediate
    /// durable commits per
    /// request lifecycle.
    #[must_use]
    pub const fn append(self) -> Duration {
        self.append
    }

    /// Database file size after the window.
    #[must_use]
    pub const fn file_bytes(self) -> u64 {
        self.file_bytes
    }
}

/// Persistent benchmark-only handle to the real service-audit repository.
///
/// Construction uses the same standard one-phase immediate operational adapter
/// as the server. It intentionally bypasses only startup evidence orchestration:
/// benchmark setup creates a new empty database whose complete state is known.
pub struct ServiceAuditGrowthHarness {
    path: std::path::PathBuf,
    ports: RedbOperationalPorts,
}

impl ServiceAuditGrowthHarness {
    /// Creates an empty initialized operational fixture.
    pub fn new(path: &Path) -> Result<Self, EngineBenchmarkError> {
        let mut store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
        let database_id = DatabaseId::from_bytes(uuid_v7_bytes(0x11, 0))
            .map_err(|_| EngineBenchmarkError::Engine)?;
        match store
            .initialize_database(database_id)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            DatabaseInitializationResult::Installed(actual) if actual == database_id => {}
            _ => return Err(EngineBenchmarkError::Engine),
        }
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        Ok(Self {
            path: path.to_path_buf(),
            ports,
        })
    }

    /// Reopens an existing fixture for continued window growth after a measurement drop.
    pub fn reopen(path: &Path) -> Result<Self, EngineBenchmarkError> {
        let store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
        let dormant = RedbDormantPorts {
            shared: store.shared,
        };
        let ports = dormant
            .into_operational_after_catalog_validation()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        Ok(Self {
            path: path.to_path_buf(),
            ports,
        })
    }

    /// Appends one started/failed lifecycle per command through the real port.
    ///
    /// Uses two independent durable transitions per command (Started, then
    /// Failed). Retained for PERF-013 windows and generation-mode equivalence.
    pub fn run_window(
        &mut self,
        first_command: u64,
        commands: usize,
    ) -> Result<ServiceAuditGrowthSample, EngineBenchmarkError> {
        if first_command == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        let preparation_started = Instant::now();
        let intents = build_started_failed_intents(first_command, commands)?;
        let preparation = preparation_started.elapsed();

        let append_started = Instant::now();
        for intent in &intents {
            match self
                .ports
                .append_service_audit(intent)
                .map_err(|_| EngineBenchmarkError::Engine)?
            {
                ServiceAuditAppendResult::Appended(_) => {}
                ServiceAuditAppendResult::PhaseConflict => {
                    return Err(EngineBenchmarkError::Engine);
                }
            }
        }
        let append = append_started.elapsed();
        let file_bytes = fs::metadata(&self.path)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .len();
        Ok(ServiceAuditGrowthSample {
            commands,
            preparation,
            append,
            file_bytes,
        })
    }

    /// Appends Started+Failed fused pairs in groups of up to
    /// [`riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS`] commands per durable
    /// transaction via the engine fused staging path (same mechanics as
    /// `append_service_audit_fused_pair`, multi-request).
    ///
    /// History shape matches sequential Started-then-Failed per command (same
    /// administration-sequence order); only the commit batching changes.
    pub fn run_window_grouped_fused(
        &mut self,
        first_command: u64,
        commands: usize,
    ) -> Result<ServiceAuditGrowthSample, EngineBenchmarkError> {
        use crate::administration::stage_service_audit_group_in_write;
        use crate::hooks::RedbTestOperation;

        if first_command == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
            return Err(EngineBenchmarkError::InvalidConfiguration);
        }
        let group_bound = riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS;
        let preparation_started = Instant::now();
        // Pre-build all intents so preparation is comparable to run_window.
        let intents = build_started_failed_intents(first_command, commands)?;
        let preparation = preparation_started.elapsed();

        let append_started = Instant::now();
        let mut offset = 0_usize;
        while offset < intents.len() {
            // Two intents per command; group by command count ≤ group_bound.
            let commands_left = (intents.len() - offset) / 2;
            let group_commands = commands_left.min(group_bound);
            let intent_end = offset
                .checked_add(
                    group_commands
                        .checked_mul(2)
                        .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
                )
                .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
            let group = &intents[offset..intent_end];
            let access = self
                .ports
                .begin_write()
                .map_err(|_| EngineBenchmarkError::Engine)?;
            stage_service_audit_group_in_write(&access, group)
                .map_err(|_| EngineBenchmarkError::Engine)?;
            access
                .commit_for(RedbTestOperation::ServiceAudit)
                .map_err(|_| EngineBenchmarkError::Engine)?;
            offset = intent_end;
        }
        let append = append_started.elapsed();
        let file_bytes = fs::metadata(&self.path)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .len();
        Ok(ServiceAuditGrowthSample {
            commands,
            preparation,
            append,
            file_bytes,
        })
    }
}

fn build_started_failed_intents(
    first_command: u64,
    commands: usize,
) -> Result<Vec<ServiceAuditAppendIntentV1>, EngineBenchmarkError> {
    let mut intents = Vec::with_capacity(
        commands
            .checked_mul(2)
            .ok_or(EngineBenchmarkError::InvalidConfiguration)?,
    );
    for offset in 0..commands {
        let command = sequence_at(first_command, offset)?;
        let request_id = RequestId::from_bytes(uuid_v7_bytes(0x33, command))
            .map_err(|_| EngineBenchmarkError::Engine)?;
        intents.push(service_audit_intent(
            request_id,
            command,
            ServiceAuditPhaseV1::Started,
        )?);
        intents.push(service_audit_intent(
            request_id,
            command,
            ServiceAuditPhaseV1::Failed,
        )?);
    }
    Ok(intents)
}

/// Creates the exact table inventory used by RiffDB and installs durable metadata.
pub fn initialize_engine_mechanics(path: &Path) -> Result<(), EngineBenchmarkError> {
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let mut transaction = database
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    configure(&mut transaction, EngineDurability::ImmediateTwoPhase)?;
    create_all_tables(&transaction).map_err(|_| EngineBenchmarkError::Engine)?;
    transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(META_APPLICATION_SEQUENCE, 1_u64.to_be_bytes().as_slice())
        .map_err(|_| EngineBenchmarkError::Engine)?;
    transaction
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)
}

/// Runs one bounded two-transition command window against a retained database.
///
/// Every command first installs a pending row and reaches the selected commit
/// boundary. Only then does a second transaction replace that row with the same
/// nine-table terminal graph shape used by the authoritative command path.
pub fn run_engine_mechanics_window(
    path: &Path,
    first_sequence: u64,
    commands: usize,
    profile: EngineMechanicsProfile,
) -> Result<EngineMechanicsSample, EngineBenchmarkError> {
    if first_sequence == 0 || commands == 0 || commands > MAX_WINDOW_COMMANDS {
        return Err(EngineBenchmarkError::InvalidConfiguration);
    }
    let command_count = u64::try_from(commands).map_err(|_| EngineBenchmarkError::Engine)?;
    first_sequence
        .checked_add(command_count)
        .ok_or(EngineBenchmarkError::InvalidConfiguration)?;
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let started = Instant::now();
    let mut admission_work = Duration::ZERO;
    let mut admission_commit = Duration::ZERO;
    let mut terminal_work = Duration::ZERO;
    let mut terminal_commit = Duration::ZERO;

    for group_start in (0..commands).step_by(profile.group_commands) {
        let group_end = commands.min(group_start + profile.group_commands);
        let work_started = Instant::now();
        let mut transaction = database
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, profile.durability)?;
        stage_admissions(&transaction, first_sequence, group_start, group_end)?;
        admission_work = admission_work.saturating_add(work_started.elapsed());
        let commit_started = Instant::now();
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        admission_commit = admission_commit.saturating_add(commit_started.elapsed());

        let work_started = Instant::now();
        let mut transaction = database
            .begin_write()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        configure(&mut transaction, profile.durability)?;
        stage_terminals(&transaction, first_sequence, group_start, group_end)?;
        terminal_work = terminal_work.saturating_add(work_started.elapsed());
        let commit_started = Instant::now();
        transaction
            .commit()
            .map_err(|_| EngineBenchmarkError::Engine)?;
        terminal_commit = terminal_commit.saturating_add(commit_started.elapsed());
    }

    if profile.durability == EngineDurability::None {
        durable_barrier(&database)?;
    }
    let elapsed = started.elapsed();
    let file_bytes = fs::metadata(path)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .len();
    Ok(EngineMechanicsSample {
        commands,
        admission_work,
        admission_commit,
        terminal_work,
        terminal_commit,
        elapsed,
        file_bytes,
    })
}

/// Lightweight engine reopen (allocator probe only). Series-compatible with
/// pre-Package-H measurements: open + read META, no full evidence drain.
pub fn measure_engine_reopen(path: &Path) -> Result<Duration, EngineBenchmarkError> {
    let started = Instant::now();
    let database = Database::create(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let transaction = database
        .begin_read()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let table = transaction
        .open_table(META)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    if table
        .get(META_APPLICATION_SEQUENCE)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .is_none()
    {
        return Err(EngineBenchmarkError::Engine);
    }
    drop(table);
    drop(transaction);
    drop(database);
    Ok(started.elapsed())
}

/// Full clean startup drain: open + structural + historical + operational handoff.
pub fn measure_clean_startup(path: &Path) -> Result<Duration, EngineBenchmarkError> {
    let started = Instant::now();
    drain_startup_evidence(path)?;
    Ok(started.elapsed())
}

/// Timing for one PERF-013 clean startup with evidence-drain half-split.
///
/// `first_half` / `second_half` sum wall time over the first and second halves of
/// combined structural + historical evidence *page reads* (by page count, each
/// ExactEnd response counted as one page). Opaque cursors do not expose a
/// retained-command sequence midpoint, so page-count half-split is the drain-API
/// linear check. Validation work is unchanged.
///
/// **Interpretation:** only the *movement* of `first_half / second_half` across
/// retained counts N is meaningful — the absolute ratio is not 1:1 (structural
/// work is front-loaded). A midpoint that crosses the structural→historical
/// junction confounds the ratio; use `structural_pages` / `historical_pages` to
/// detect that.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanStartupMeasurement {
    elapsed: Duration,
    first_half: Duration,
    second_half: Duration,
    structural_pages: u64,
    historical_pages: u64,
    evidence_pages: u64,
}

impl CleanStartupMeasurement {
    /// Full open + structural + historical + operational handoff wall time.
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.elapsed
    }

    /// Wall time spent in the first half of evidence page reads.
    #[must_use]
    pub const fn first_half(self) -> Duration {
        self.first_half
    }

    /// Wall time spent in the second half of evidence page reads.
    #[must_use]
    pub const fn second_half(self) -> Duration {
        self.second_half
    }

    /// Structural evidence page reads (including ExactEnd).
    #[must_use]
    pub const fn structural_pages(self) -> u64 {
        self.structural_pages
    }

    /// Historical evidence page reads (including ExactEnd).
    #[must_use]
    pub const fn historical_pages(self) -> u64 {
        self.historical_pages
    }

    /// Structural + historical evidence page reads (including ExactEnd responses).
    #[must_use]
    pub const fn evidence_pages(self) -> u64 {
        self.evidence_pages
    }
}

/// Full clean startup with combined structural+historical page half-split timings.
///
/// `expected_retained_commands` pre-sizes the per-page timing buffer so a 10M-command
/// drain does not reallocate (RSS pollution) mid-measurement.
pub fn measure_clean_startup_linear(
    path: &Path,
    expected_retained_commands: u64,
) -> Result<CleanStartupMeasurement, EngineBenchmarkError> {
    let started = Instant::now();
    let detail = drain_startup_evidence_linear(path, expected_retained_commands)?;
    Ok(CleanStartupMeasurement {
        elapsed: started.elapsed(),
        first_half: detail.first_half,
        second_half: detail.second_half,
        structural_pages: detail.structural_pages,
        historical_pages: detail.historical_pages,
        evidence_pages: detail.evidence_pages,
    })
}

/// Splits ordered page durations into first/second half sums (by page count).
///
/// When the page count is odd the extra page is attributed to the second half
/// (`mid = len / 2`). Empty input yields `(0, 0)`.
#[must_use]
pub fn split_half_page_durations(page_ns: &[u64]) -> (u64, u64) {
    let mid = page_ns.len() / 2;
    let first = page_ns[..mid].iter().copied().sum();
    let second = page_ns[mid..].iter().copied().sum();
    (first, second)
}

/// Soft upper bound on structural+historical page reads for a retained count.
///
/// Page limit is 64 entries; structural evidence is denser than one item per
/// command. Over-estimate slightly so the Vec never reallocates during drain.
#[must_use]
pub fn expected_evidence_page_capacity(retained_commands: u64) -> usize {
    // ~4 evidence items/command worst case + both ExactEnd pages + headroom.
    let items = retained_commands.saturating_mul(4).saturating_add(128);
    let pages = items.div_ceil(32).saturating_add(16);
    usize::try_from(pages).unwrap_or(usize::MAX).max(32)
}

struct DrainLinearDetail {
    first_half: Duration,
    second_half: Duration,
    structural_pages: u64,
    historical_pages: u64,
    evidence_pages: u64,
}

fn drain_startup_evidence(path: &Path) -> Result<(), EngineBenchmarkError> {
    let _ = drain_startup_evidence_linear(path, 0)?;
    Ok(())
}

fn drain_startup_evidence_linear(
    path: &Path,
    expected_retained_commands: u64,
) -> Result<DrainLinearDetail, EngineBenchmarkError> {
    use riffdb_storage_api::{
        EvidencePageLimit, HistoricalEvidenceCursor, HistoricalEvidencePage,
        ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
        StartupValidationInputs, StructuralEvidenceCursor, StructuralEvidenceOpen,
        StructuralEvidencePage, StructuralEvidenceSession, StructuralOpenOutcome,
    };
    use riffdb_types::{DigestKeyId, Timestamp};

    let store = RedbStore::open(path).map_err(|_| EngineBenchmarkError::Engine)?;
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).ok_or(EngineBenchmarkError::Engine)?);
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).map_err(|_| EngineBenchmarkError::Engine)?,
        ReadableCapabilityDigestInventory::new(vec![key])
            .map_err(|_| EngineBenchmarkError::Engine)?,
        ReadableIdempotencyDigestInventory::new(vec![key])
            .map_err(|_| EngineBenchmarkError::Engine)?,
    );
    let mut session = store
        .begin_structural_evidence(inputs)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).ok_or(EngineBenchmarkError::Engine)?;
    let mut page_ns =
        Vec::with_capacity(expected_evidence_page_capacity(expected_retained_commands));
    let mut structural_pages = 0_u64;
    let mut structural = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        let page_started = Instant::now();
        match session
            .read_structural_evidence(structural, limit)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            StructuralEvidencePage::Page { next, .. } => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                structural_pages = structural_pages.saturating_add(1);
                structural = next;
            }
            StructuralEvidencePage::ExactEnd(end) => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                structural_pages = structural_pages.saturating_add(1);
                break end;
            }
        }
    };
    let mut historical_pages = 0_u64;
    let mut historical = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let historical_end = loop {
        let page_started = Instant::now();
        match session
            .read_historical_evidence(historical, limit)
            .map_err(|_| EngineBenchmarkError::Engine)?
        {
            HistoricalEvidencePage::Page { next, .. } => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                historical_pages = historical_pages.saturating_add(1);
                historical = next;
            }
            HistoricalEvidencePage::ExactEnd(end) => {
                page_ns.push(u64::try_from(page_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
                historical_pages = historical_pages.saturating_add(1);
                break end;
            }
        }
    };
    let evidence_pages = u64::try_from(page_ns.len()).unwrap_or(u64::MAX);
    let (first_half_ns, second_half_ns) = split_half_page_durations(&page_ns);
    let outcome = session
        .finish(structural_end, historical_end)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    match outcome {
        StructuralOpenOutcome::Clean(opened) => {
            let (_, _, _, dormant) = opened.into_parts();
            let _ = dormant
                .into_operational_after_catalog_validation()
                .map_err(|_| EngineBenchmarkError::Engine)?;
        }
        StructuralOpenOutcome::MigrationRequired(_) => {
            return Err(EngineBenchmarkError::Engine);
        }
    }
    Ok(DrainLinearDetail {
        first_half: Duration::from_nanos(first_half_ns),
        second_half: Duration::from_nanos(second_half_ns),
        structural_pages,
        historical_pages,
        evidence_pages,
    })
}

fn configure(
    transaction: &mut WriteTransaction,
    durability: EngineDurability,
) -> Result<(), EngineBenchmarkError> {
    let (redb_durability, two_phase) = match durability {
        EngineDurability::None => (Durability::None, false),
        EngineDurability::ImmediateOnePhase => (Durability::Immediate, false),
        EngineDurability::ImmediateTwoPhase => (Durability::Immediate, true),
    };
    transaction.set_two_phase_commit(two_phase);
    transaction
        .set_durability(redb_durability)
        .map_err(|_| EngineBenchmarkError::Engine)
}

fn stage_admissions(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    let mut pending = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(|_| EngineBenchmarkError::Engine)?;
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        let key = record_key(sequence, 0x59);
        let value = record_value(sequence, PENDING_VALUE_BYTES, 0x50);
        pending
            .insert(key.as_slice(), value.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn stage_terminals(
    transaction: &WriteTransaction,
    first_sequence: u64,
    start: usize,
    end: usize,
) -> Result<(), EngineBenchmarkError> {
    for offset in start..end {
        let sequence = sequence_at(first_sequence, offset)?;
        let key = record_key(sequence, 0x59);
        let mut pending = transaction
            .open_table(IDEMPOTENCY_PENDING)
            .map_err(|_| EngineBenchmarkError::Engine)?;
        if pending
            .remove(key.as_slice())
            .map_err(|_| EngineBenchmarkError::Engine)?
            .is_none()
        {
            return Err(EngineBenchmarkError::Engine);
        }
        drop(pending);
        insert_record(transaction, ENTITIES, sequence, 0x45, ENTITY_VALUE_BYTES)?;
        insert_record(
            transaction,
            SECONDARY_INDEXES,
            sequence,
            0x49,
            INDEX_VALUE_BYTES,
        )?;
        insert_record(transaction, INDEX_EPOCHS, sequence, 0x58, EPOCH_VALUE_BYTES)?;
        insert_record(
            transaction,
            PROVENANCE,
            sequence,
            0x50,
            PROVENANCE_VALUE_BYTES,
        )?;
        insert_record(transaction, EVENTS, sequence, 0x56, EVENT_VALUE_BYTES)?;
        insert_record(transaction, OUTBOX, sequence, 0x4f, OUTBOX_VALUE_BYTES)?;
        insert_record(transaction, COMMITS, sequence, 0x43, COMMIT_VALUE_BYTES)?;
        insert_record(
            transaction,
            IDEMPOTENCY,
            sequence,
            0x59,
            OUTCOME_VALUE_BYTES,
        )?;
        transaction
            .open_table(META)
            .map_err(|_| EngineBenchmarkError::Engine)?
            .insert(
                META_APPLICATION_SEQUENCE,
                sequence
                    .checked_add(1)
                    .ok_or(EngineBenchmarkError::Engine)?
                    .to_be_bytes()
                    .as_slice(),
            )
            .map_err(|_| EngineBenchmarkError::Engine)?;
    }
    Ok(())
}

fn insert_record(
    transaction: &WriteTransaction,
    definition: redb::TableDefinition<&[u8], &[u8]>,
    sequence: u64,
    tag: u8,
    bytes: usize,
) -> Result<(), EngineBenchmarkError> {
    let key = record_key(sequence, tag);
    let value = record_value(sequence, bytes, tag);
    transaction
        .open_table(definition)
        .map_err(|_| EngineBenchmarkError::Engine)?
        .insert(key.as_slice(), value.as_slice())
        .map_err(|_| EngineBenchmarkError::Engine)?;
    Ok(())
}

fn sequence_at(first: u64, offset: usize) -> Result<u64, EngineBenchmarkError> {
    first
        .checked_add(u64::try_from(offset).map_err(|_| EngineBenchmarkError::Engine)?)
        .ok_or(EngineBenchmarkError::Engine)
}

fn record_key(sequence: u64, tag: u8) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[0] = tag;
    key[8..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn record_value(sequence: u64, bytes: usize, tag: u8) -> Vec<u8> {
    let mut value = vec![tag; bytes];
    value[..8].copy_from_slice(&sequence.to_be_bytes());
    value
}

fn durable_barrier(database: &Database) -> Result<(), EngineBenchmarkError> {
    let mut transaction = database
        .begin_write()
        .map_err(|_| EngineBenchmarkError::Engine)?;
    configure(&mut transaction, EngineDurability::ImmediateTwoPhase)?;
    transaction
        .commit()
        .map_err(|_| EngineBenchmarkError::Engine)
}

fn service_audit_intent(
    request_id: RequestId,
    command: u64,
    phase: ServiceAuditPhaseV1,
) -> Result<ServiceAuditAppendIntentV1, EngineBenchmarkError> {
    let seconds = i64::try_from(command).map_err(|_| EngineBenchmarkError::Engine)?;
    let principal = AuditPrincipalV1::new(
        ActorId::new("command-growth-agent").map_err(|_| EngineBenchmarkError::Engine)?,
        ActorKind::Service,
        CapabilityId::from_bytes(uuid_v7_bytes(0x22, 0))
            .map_err(|_| EngineBenchmarkError::Engine)?,
        NonZeroU64::MIN,
    );
    ServiceAuditAppendIntentV1::new(
        request_id,
        Timestamp::new(seconds, 0).map_err(|_| EngineBenchmarkError::Engine)?,
        ServiceOperationV1::ExecuteCommand,
        phase,
        principal,
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .map_err(|_| EngineBenchmarkError::Engine)
}

fn uuid_v7_bytes(tag: u8, sequence: u64) -> [u8; 16] {
    let mut bytes = [tag; 16];
    bytes[0..6].copy_from_slice(&sequence.to_be_bytes()[2..]);
    bytes[6] = 0x70 | (tag & 0x0f);
    bytes[8] = 0x80 | (tag & 0x3f);
    bytes[9..].copy_from_slice(&sequence.to_be_bytes()[1..]);
    bytes
}

#[cfg(test)]
mod tests {
    use super::{
        EngineDurability, EngineMechanicsProfile, ServiceAuditGrowthHarness,
        expected_evidence_page_capacity, initialize_engine_mechanics, measure_clean_startup,
        measure_clean_startup_linear, run_engine_mechanics_window, split_half_page_durations,
    };
    use std::time::Duration;

    #[test]
    fn split_half_page_durations_empty_and_balanced() {
        assert_eq!(split_half_page_durations(&[]), (0, 0));
        assert_eq!(split_half_page_durations(&[10]), (0, 10));
        assert_eq!(split_half_page_durations(&[10, 20]), (10, 20));
        assert_eq!(split_half_page_durations(&[1, 2, 3, 4]), (3, 7));
        assert_eq!(split_half_page_durations(&[1, 2, 3]), (1, 5));
    }

    #[test]
    fn expected_page_capacity_grows_with_retained() {
        assert!(
            expected_evidence_page_capacity(10_000_000) > expected_evidence_page_capacity(1_000)
        );
        assert!(expected_evidence_page_capacity(0) >= 32);
    }

    #[test]
    fn clean_startup_linear_matches_plain_measurement_shape() {
        let dir = tempfile_dir();
        let path = dir.join("linear-check.redb");
        let mut harness = ServiceAuditGrowthHarness::new(&path).expect("new harness");
        harness.run_window(1, 32).expect("seed window");
        drop(harness);

        let plain = measure_clean_startup(&path).expect("plain startup");
        let linear = measure_clean_startup_linear(&path, 32).expect("linear startup");
        assert!(linear.elapsed() > Duration::ZERO);
        assert!(plain > Duration::ZERO);
        // Page-read halves are subsets of the full open+drain+handoff wall time.
        let halves = linear.first_half().saturating_add(linear.second_half());
        assert!(halves <= linear.elapsed());
        assert!(linear.evidence_pages() >= 1);
        assert_eq!(
            linear.evidence_pages(),
            linear
                .structural_pages()
                .saturating_add(linear.historical_pages())
        );
    }

    #[test]
    fn grouped_fused_and_ungrouped_generation_yield_identical_drain_shape() {
        // Equivalence: same Started/Failed history order → same evidence page counts.
        let dir = tempfile_dir();
        let ungrouped_path = dir.join("ungrouped.redb");
        let grouped_path = dir.join("grouped.redb");
        const N: usize = 128;

        let mut ungrouped = ServiceAuditGrowthHarness::new(&ungrouped_path).expect("ungrouped new");
        ungrouped.run_window(1, N).expect("ungrouped generate");
        drop(ungrouped);

        let mut grouped = ServiceAuditGrowthHarness::new(&grouped_path).expect("grouped new");
        grouped
            .run_window_grouped_fused(1, N)
            .expect("grouped generate");
        drop(grouped);

        let u = measure_clean_startup_linear(&ungrouped_path, N as u64).expect("ungrouped drain");
        let g = measure_clean_startup_linear(&grouped_path, N as u64).expect("grouped drain");
        assert_eq!(
            u.structural_pages(),
            g.structural_pages(),
            "structural pages"
        );
        assert_eq!(
            u.historical_pages(),
            g.historical_pages(),
            "historical pages"
        );
        assert_eq!(u.evidence_pages(), g.evidence_pages(), "evidence pages");
        assert!(u.structural_pages() >= 1);
        assert!(u.historical_pages() >= 1);
    }

    #[test]
    fn engine_mechanics_profile_rejects_zero_group() {
        assert!(EngineMechanicsProfile::new(EngineDurability::None, 0).is_err());
    }

    #[test]
    fn initialize_and_window_still_compile_paths() {
        let dir = tempfile_dir();
        let path = dir.join("mechanics.redb");
        initialize_engine_mechanics(&path).expect("init");
        let profile =
            EngineMechanicsProfile::new(EngineDurability::ImmediateOnePhase, 1).expect("profile");
        let sample = run_engine_mechanics_window(&path, 1, 4, profile).expect("window");
        assert_eq!(sample.commands(), 4);
    }

    fn tempfile_dir() -> std::path::PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("riffdb-bench-support-{stamp}"));
        std::fs::create_dir_all(&path).expect("mkdir");
        path
    }
}
