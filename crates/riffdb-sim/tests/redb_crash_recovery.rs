//! Phase-0 DST campaign: the pinned redb engine under simulated crashes.
//!
//! Every campaign is a pure function of one seed: a seeded scripted workload
//! commits transactions with `Durability::Immediate` against
//! `Builder::create_with_backend(SimBackend)`, the seeded fault schedule
//! crashes the disk (including inside recovery's reopen window), and after
//! every recovery the reopened database must open without corruption and
//! equal the shadow model's state at the last acknowledged sync.
//!
//! Oracle notes (engine-level, phase-0):
//!
//! - The shadow model is a plain `BTreeMap` updated at each commit. Because a
//!   crash can strike inside `commit()` after the commit became durable but
//!   before it was acknowledged, the acceptance set while a commit is in
//!   flight is exactly {state at last acknowledged commit, in-flight state};
//!   at every other moment it is the single acknowledged state.
//! - Sync-per-durable-commit correspondence is asserted from backend
//!   observation, not assumed: see
//!   [`every_acknowledged_immediate_commit_syncs_the_backend_exactly_once`].
//! - Each transaction touches exactly one user table. redb 4.1.0 drains
//!   `pending_table_updates` from a `std::collections::HashMap` during
//!   commit, so a transaction updating two tables flushes their roots in
//!   hash-random order and the backend write order — and therefore the trace
//!   digest — would not replay across runs. Alternating single-table
//!   transactions keeps the workload deterministic while still covering both
//!   tables.

use std::collections::BTreeMap;

use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use riffdb_sim::{FaultConfig, SimBackend, SimDisk, SplitMix64, TraceHash, fnv1a64};

const DB_FILE: &str = "engine.redb";
const TABLE_ALPHA: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("alpha");
const TABLE_BETA: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("beta");
const KEY_SPACE: u64 = 48;
const MAX_VALUE_BYTES: u64 = 1024;
const TARGET_COMMITTED_TXNS: u64 = 60;
const EXPLICIT_CRASH_EVERY: u64 = 15;
const MAX_REOPEN_ATTEMPTS_BEFORE_QUIESCE: u64 = 8;
/// Hard bound turning any persistent-reopen-failure livelock (for example a
/// full disk nobody grows) into a diagnosable panic instead of a hang.
const MAX_REOPEN_ATTEMPTS_HARD: u64 = 64;
const MAX_TOTAL_SCHEDULED_FAULTS: u64 = 48;
/// Distinct stream tag so workload draws never alias fault-schedule draws.
const WORKLOAD_STREAM: u64 = 0x57F2_C0DE_D15C_0001;

/// Shadow model state: `(table index, key) -> value`.
type ShadowState = BTreeMap<(u8, u64), Vec<u8>>;

#[derive(Debug)]
struct CampaignReport {
    trace_digest: u64,
    summary_digest: u64,
    committed_txns: u64,
    verified_recoveries: u64,
    reopen_window_crashes: u64,
    min_syncs_per_commit: u64,
    max_syncs_per_commit: u64,
    crashes: u64,
    transient_errors: u64,
    torn_decisions: u64,
}

struct Campaign {
    seed: u64,
    disk: SimDisk,
    workload: SplitMix64,
    key_space: u64,
    max_value_bytes: u64,
    committed: ShadowState,
    in_flight: Option<ShadowState>,
    committed_txns: u64,
    verified_recoveries: u64,
    reopen_window_crashes: u64,
    min_syncs_per_commit: u64,
    max_syncs_per_commit: u64,
    faults_quiesced: bool,
    summary: TraceHash,
}

enum TxnOutcome {
    Committed,
    Aborted,
    EngineFault,
}

fn table_definition(index: u8) -> TableDefinition<'static, u64, &'static [u8]> {
    if index == 0 { TABLE_ALPHA } else { TABLE_BETA }
}

fn open_database(disk: &SimDisk) -> Result<Database, redb::DatabaseError> {
    Database::builder()
        .set_cache_size(1024 * 1024)
        .create_with_backend(SimBackend::new(disk, DB_FILE))
}

fn read_full_state(db: &Database) -> Result<ShadowState, redb::Error> {
    let tx = db.begin_read()?;
    let mut state = ShadowState::new();
    for index in 0..2_u8 {
        let table = match tx.open_table(table_definition(index)) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => continue,
            Err(error) => return Err(error.into()),
        };
        for row in table.iter()? {
            let (key, value) = row?;
            state.insert((index, key.value()), value.value().to_vec());
        }
    }
    Ok(state)
}

fn fold_state(summary: &mut TraceHash, state: &ShadowState) {
    summary.fold_u64(state.len() as u64);
    for ((table, key), value) in state {
        summary.fold_u64(u64::from(*table));
        summary.fold_u64(*key);
        summary.fold_u64(fnv1a64(value));
    }
}

impl Campaign {
    fn new(config: FaultConfig) -> Self {
        let seed = config.seed;
        Self {
            seed,
            disk: SimDisk::new(config),
            workload: SplitMix64::new(seed ^ WORKLOAD_STREAM),
            key_space: KEY_SPACE,
            max_value_bytes: MAX_VALUE_BYTES,
            committed: ShadowState::new(),
            in_flight: None,
            committed_txns: 0,
            verified_recoveries: 0,
            reopen_window_crashes: 0,
            min_syncs_per_commit: u64::MAX,
            max_syncs_per_commit: 0,
            faults_quiesced: false,
            summary: TraceHash::new(),
        }
    }

    /// Permanently quiesces the schedule once the fault budget is spent, so
    /// every seed terminates deterministically.
    fn enforce_fault_budget(&mut self) {
        if self.faults_quiesced {
            return;
        }
        let counters = self.disk.counters();
        if counters.crashes + counters.transient_errors >= MAX_TOTAL_SCHEDULED_FAULTS {
            self.faults_quiesced = true;
            self.disk.set_faults_enabled(false);
        }
    }

    fn quiesce_faults(&mut self) {
        if !self.faults_quiesced {
            self.faults_quiesced = true;
            self.disk.set_faults_enabled(false);
        }
    }

    /// Reopens the database from the durable image, verifying the recovered
    /// contents against the shadow acceptance set. Faults may strike inside
    /// this window (crash during recovery); the loop recovers and retries,
    /// quiescing the schedule after a bounded number of faulted attempts.
    fn reopen_verified(&mut self) -> Database {
        let mut attempts = 0;
        loop {
            attempts += 1;
            assert!(
                attempts <= MAX_REOPEN_ATTEMPTS_HARD,
                "seed {:#x}: reopen failed {MAX_REOPEN_ATTEMPTS_HARD} times \
                 with the schedule quiesced; a state-derived fault (such as \
                 an unfixed full disk) is blocking recovery",
                self.seed,
            );
            if attempts > MAX_REOPEN_ATTEMPTS_BEFORE_QUIESCE {
                self.quiesce_faults();
            }
            self.enforce_fault_budget();
            if self.disk.is_crashed() {
                self.disk.recover_after_crash();
            }
            let before = self.disk.counters();
            let db = match open_database(&self.disk) {
                Ok(db) => db,
                Err(error) => {
                    self.assert_fault_induced(&before, &format!("reopen failed: {error}"));
                    if self.disk.is_crashed() {
                        self.reopen_window_crashes += 1;
                    }
                    continue;
                }
            };
            let recovered = match read_full_state(&db) {
                Ok(recovered) => recovered,
                Err(error) => {
                    self.assert_fault_induced(
                        &before,
                        &format!("recovery verification read failed: {error}"),
                    );
                    if self.disk.is_crashed() {
                        self.reopen_window_crashes += 1;
                    }
                    drop(db);
                    continue;
                }
            };
            self.accept_recovered(recovered);
            return db;
        }
    }

    /// The reopened database must open without corruption for every seed and
    /// equal the shadow snapshot at the last acknowledged sync — or, while a
    /// commit was in flight at the crash, the in-flight state whose durable
    /// sync was acknowledged inside `commit()` before the crash struck.
    fn accept_recovered(&mut self, recovered: ShadowState) {
        let matches_committed = recovered == self.committed;
        let matches_in_flight = self.in_flight.as_ref() == Some(&recovered);
        assert!(
            matches_committed || matches_in_flight,
            "seed {:#x}: recovered state diverged from the shadow model \
             (recovered {} rows, committed shadow {} rows, in-flight {:?} rows)",
            self.seed,
            recovered.len(),
            self.committed.len(),
            self.in_flight.as_ref().map(ShadowState::len),
        );
        self.verified_recoveries += 1;
        fold_state(&mut self.summary, &recovered);
        self.committed = recovered;
        self.in_flight = None;
    }

    fn assert_fault_induced(&self, before: &riffdb_sim::FaultCounters, context: &str) {
        let now = self.disk.counters();
        assert!(
            self.disk.is_crashed()
                || now.crashes > before.crashes
                || now.transient_errors > before.transient_errors
                || now.capacity_rejections > before.capacity_rejections,
            "seed {:#x}: {context} without an injected fault: \
             torn unsynced state bricked the engine",
            self.seed,
        );
    }

    /// Draws and applies one scripted transaction. All randomness is drawn
    /// up front so a plan is a pure function of the workload stream position.
    fn apply_one_transaction(&mut self, db: &Database) -> TxnOutcome {
        let table_index = self.workload.next_below(2) as u8;
        let op_count = 1 + self.workload.next_below(6);
        let mut plan: Vec<(u64, Option<Vec<u8>>)> = Vec::new();
        for _ in 0..op_count {
            let key = self.workload.next_below(self.key_space);
            if self.workload.next_below(3) == 2 {
                plan.push((key, None));
            } else {
                let length = 1 + self.workload.next_below(self.max_value_bytes) as usize;
                let mut value = Vec::with_capacity(length);
                while value.len() < length {
                    value.extend_from_slice(&self.workload.next_u64().to_le_bytes());
                }
                value.truncate(length);
                plan.push((key, Some(value)));
            }
        }
        let abort = self.workload.chance(1, 8);

        let mut next = self.committed.clone();
        let Ok(mut tx) = db.begin_write() else {
            return TxnOutcome::EngineFault;
        };
        if tx.set_durability(Durability::Immediate).is_err() {
            return TxnOutcome::EngineFault;
        }
        {
            let Ok(mut table) = tx.open_table(table_definition(table_index)) else {
                return TxnOutcome::EngineFault;
            };
            for (key, value) in &plan {
                match value {
                    Some(value) => {
                        if table.insert(key, value.as_slice()).is_err() {
                            return TxnOutcome::EngineFault;
                        }
                        next.insert((table_index, *key), value.clone());
                    }
                    None => {
                        if table.remove(key).is_err() {
                            return TxnOutcome::EngineFault;
                        }
                        next.remove(&(table_index, *key));
                    }
                }
            }
        }
        if abort {
            // Shadow state is untouched; a failed abort is an engine fault
            // path but never a state change.
            return match tx.abort() {
                Ok(()) => TxnOutcome::Aborted,
                Err(_) => TxnOutcome::EngineFault,
            };
        }

        let syncs_before = self.disk.counters().syncs;
        self.in_flight = Some(next.clone());
        match tx.commit() {
            Ok(()) => {
                let syncs = self.disk.counters().syncs - syncs_before;
                assert!(
                    syncs >= 1,
                    "seed {:#x}: acknowledged Immediate commit performed no \
                     acknowledged backend sync",
                    self.seed,
                );
                self.min_syncs_per_commit = self.min_syncs_per_commit.min(syncs);
                self.max_syncs_per_commit = self.max_syncs_per_commit.max(syncs);
                self.committed = next;
                self.in_flight = None;
                self.committed_txns += 1;
                TxnOutcome::Committed
            }
            // The commit may or may not have become durable before the fault;
            // `in_flight` stays armed and the next recovery resolves it.
            Err(_) => TxnOutcome::EngineFault,
        }
    }

    fn into_report(mut self) -> CampaignReport {
        let counters = self.disk.counters();
        self.summary.fold_u64(self.committed_txns);
        self.summary.fold_u64(self.verified_recoveries);
        self.summary.fold_u64(self.reopen_window_crashes);
        self.summary.fold_u64(self.min_syncs_per_commit);
        self.summary.fold_u64(self.max_syncs_per_commit);
        self.summary.fold_u64(counters.writes);
        self.summary.fold_u64(counters.syncs);
        self.summary.fold_u64(counters.reads);
        self.summary.fold_u64(counters.crashes);
        self.summary.fold_u64(counters.recoveries);
        self.summary.fold_u64(counters.transient_errors);
        self.summary.fold_u64(counters.capacity_rejections);
        CampaignReport {
            trace_digest: self.disk.trace_digest(),
            summary_digest: self.summary.digest(),
            committed_txns: self.committed_txns,
            verified_recoveries: self.verified_recoveries,
            reopen_window_crashes: self.reopen_window_crashes,
            min_syncs_per_commit: self.min_syncs_per_commit,
            max_syncs_per_commit: self.max_syncs_per_commit,
            crashes: counters.crashes,
            transient_errors: counters.transient_errors,
            torn_decisions: counters.torn_kept + counters.torn_dropped + counters.torn_truncated,
        }
    }
}

fn campaign_fault_config(seed: u64) -> FaultConfig {
    FaultConfig {
        seed,
        torn_write_granularity: 512,
        crash_after_operations: Some((100, 2500)),
        transient_error_denominator: 300,
        capacity_bytes: None,
    }
}

/// The complete crash→recover→continue loop for one seed. Deterministic:
/// every draw comes from the seed, and the returned digests replay exactly.
fn run_campaign(seed: u64) -> CampaignReport {
    let mut campaign = Campaign::new(campaign_fault_config(seed));
    let mut next_explicit_crash = EXPLICIT_CRASH_EVERY;
    'lifetimes: while campaign.committed_txns < TARGET_COMMITTED_TXNS {
        let db = campaign.reopen_verified();
        loop {
            if campaign.committed_txns >= TARGET_COMMITTED_TXNS {
                break 'lifetimes;
            }
            if campaign.committed_txns >= next_explicit_crash {
                next_explicit_crash += EXPLICIT_CRASH_EVERY;
                // Crash as a first-class operation, independent of the
                // schedule, so every seed exercises several full
                // crash→recover→continue cycles.
                campaign.disk.crash();
                drop(db);
                continue 'lifetimes;
            }
            campaign.enforce_fault_budget();
            match campaign.apply_one_transaction(&db) {
                TxnOutcome::Committed | TxnOutcome::Aborted => {}
                TxnOutcome::EngineFault => {
                    drop(db);
                    continue 'lifetimes;
                }
            }
        }
    }
    // Final quiesced crash-recovery pass: with no commit in flight the
    // recovered state must equal the acknowledged shadow exactly.
    campaign.quiesce_faults();
    campaign.disk.crash();
    let db = campaign.reopen_verified();
    assert!(
        campaign.in_flight.is_none(),
        "verification must have resolved any in-flight commit"
    );
    drop(db);
    campaign.into_report()
}

#[test]
fn same_seed_replays_a_byte_identical_trace_digest() {
    // SIM-001: same seed + trace format version ⇒ identical versioned digest.
    let first = run_campaign(0xA5EE_D001);
    let second = run_campaign(0xA5EE_D001);
    assert_eq!(
        first.trace_digest, second.trace_digest,
        "identical seeds must replay identical operation and fault traces"
    );
    assert_eq!(
        first.summary_digest, second.summary_digest,
        "identical seeds must replay identical recovered-state summaries"
    );
    assert_eq!(first.committed_txns, second.committed_txns);
    assert_eq!(first.crashes, second.crashes);

    // Guard against a degenerate constant hash: a different seed must diverge.
    let other = run_campaign(0xA5EE_D002);
    assert_ne!(
        first.trace_digest, other.trace_digest,
        "different seeds must produce different traces"
    );
}

#[test]
fn seeded_sweep_recovers_every_crash_without_corruption_or_shadow_divergence() {
    // The standing regression battery: dozens of seeds through the full
    // crash→recover→continue loop. Every recovery is verified inside
    // `run_campaign`; the aggregate assertions prove no fault arm is dead
    // (SIM-002) and that the crash-during-recovery window is explored.
    let mut total_crashes = 0;
    let mut total_transients = 0;
    let mut total_torn = 0;
    let mut total_reopen_window_crashes = 0;
    for index in 0..32_u64 {
        let report = run_campaign(0x5EED_0000 + index);
        assert_eq!(
            report.committed_txns, TARGET_COMMITTED_TXNS,
            "seed {index}: campaign must reach its committed-transaction target"
        );
        assert!(
            report.crashes >= TARGET_COMMITTED_TXNS / EXPLICIT_CRASH_EVERY,
            "seed {index}: explicit crash cadence must run"
        );
        assert!(
            report.verified_recoveries > report.crashes / 2,
            "seed {index}: recoveries must be verified, not skipped"
        );
        assert!(report.min_syncs_per_commit >= 1);
        assert!(
            report.max_syncs_per_commit >= report.min_syncs_per_commit,
            "seed {index}: sync-per-commit observation window inverted"
        );
        total_crashes += report.crashes;
        total_transients += report.transient_errors;
        total_torn += report.torn_decisions;
        total_reopen_window_crashes += report.reopen_window_crashes;
    }
    // Guaranteed floor: three explicit cadence crashes plus the final
    // quiesced crash per seed; the scheduled arm adds more on top (observed
    // 131 total on the pinned generator version).
    assert!(total_crashes >= 32 * 4, "crash arm coverage collapsed");
    assert!(total_transients > 0, "transient-error arm never fired");
    assert!(total_torn > 0, "torn-write decisions never exercised");
    assert!(
        total_reopen_window_crashes > 0,
        "no seed crashed inside the recovery reopen window"
    );
}

#[test]
fn a_crash_landing_inside_the_reopen_window_is_survivable() {
    // Deterministic pin of the crash-during-recovery arm: durable state is
    // built quietly, then the schedule is re-aimed so tightly that every
    // reopen's recovery reads crash until the campaign quiesces the schedule,
    // after which the final open must still verify against the shadow.
    let mut campaign = Campaign::new(FaultConfig::quiet(0xC4A5_11D0));
    let db = campaign.reopen_verified();
    // Build real durable state so recovery has work to do.
    for _ in 0..8 {
        match campaign.apply_one_transaction(&db) {
            TxnOutcome::Committed | TxnOutcome::Aborted => {}
            TxnOutcome::EngineFault => panic!("quiet disk cannot fault"),
        }
    }
    drop(db);
    campaign.disk.set_crash_after_operations(Some((5, 6)));
    campaign.disk.crash();
    let db = campaign.reopen_verified();
    drop(db);
    let report = campaign.into_report();
    assert!(
        report.reopen_window_crashes >= 1,
        "the schedule must be able to place a crash inside the reopen window"
    );
    assert!(report.verified_recoveries >= 2);
}

#[test]
fn every_acknowledged_immediate_commit_syncs_the_backend_exactly_once() {
    // The observed sync-per-durable-commit correspondence for redb 4.1.0's
    // default one-phase commit: exactly one acknowledged `sync_data` per
    // acknowledged `Durability::Immediate` commit (data, commit slot, and god
    // byte are flushed by a single fsync). The shadow model snapshots at
    // acknowledged commits, which this correspondence ties to acknowledged
    // syncs. If a redb upgrade changes the algorithm, this pin is the canary
    // to re-derive the oracle.
    let disk = SimDisk::new(FaultConfig::quiet(0x0B5E_44ED));
    let db = open_database(&disk).expect("quiet disk opens");
    for round in 0..10_u64 {
        let before = disk.counters().syncs;
        let mut tx = db.begin_write().expect("begin");
        tx.set_durability(Durability::Immediate)
            .expect("durability accepted");
        {
            let mut table = tx.open_table(TABLE_ALPHA).expect("open table");
            table
                .insert(round, [round as u8; 64].as_slice())
                .expect("insert");
        }
        tx.commit().expect("commit");
        let syncs = disk.counters().syncs - before;
        assert_eq!(
            syncs, 1,
            "observed correspondence: one acknowledged sync per acknowledged \
             Immediate commit (round {round})"
        );
    }
    drop(db);
}

#[test]
fn capacity_exhaustion_fails_commits_and_added_space_recovers_committed_state() {
    // SIM-002 space-exhaustion arm through the full engine: commits fail once
    // growth exceeds capacity, nothing acknowledged is lost, and after the
    // simulated operator adds space the engine reopens onto the shadow state.
    //
    // redb 4.1.0's initial `create` allocates 1,056,768 bytes, so the
    // capacity sits just above that and the workload (wide key space, large
    // values) forces region growth beyond it.
    let config = FaultConfig {
        seed: 0xF00D_D15C,
        torn_write_granularity: 512,
        crash_after_operations: None,
        transient_error_denominator: 0,
        capacity_bytes: Some(1_500_000),
    };
    let mut campaign = Campaign::new(config);
    campaign.key_space = 256;
    campaign.max_value_bytes = 12 * 1024;
    let mut rejected = false;
    let db = campaign.reopen_verified();
    for _ in 0..2000 {
        match campaign.apply_one_transaction(&db) {
            TxnOutcome::Committed | TxnOutcome::Aborted => {}
            TxnOutcome::EngineFault => {
                assert!(
                    campaign.disk.counters().capacity_rejections > 0,
                    "the only configured fault arm is capacity exhaustion"
                );
                rejected = true;
                break;
            }
        }
    }
    drop(db);
    assert!(
        rejected,
        "the campaign must run into the configured capacity"
    );
    assert!(campaign.committed_txns > 0, "some commits must land first");
    // The simulated operator adds space; recovery must land on the shadow.
    campaign.disk.set_capacity_bytes(None);
    let db = campaign.reopen_verified();
    drop(db);
    let report = campaign.into_report();
    assert!(report.verified_recoveries >= 2);
}
