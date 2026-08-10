//! The simulated storage medium: durable/volatile images plus a seeded fault
//! schedule (`SIM-002`), with every operation and fault decision folded into
//! the versioned trace chain (`SIM-001`).

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};

use crate::rng::SplitMix64;
use crate::trace::{TraceHash, fnv1a64};

// Trace event tags. Stable within one `TRACE_FORMAT_VERSION`. The operation
// tags double as operation-kind discriminators inside refusal events (stale
// handle, closed handle, out of range, capacity), so refusals are
// self-sufficient trace entries.
pub(crate) const TRACE_LEN: u64 = 1;
pub(crate) const TRACE_READ: u64 = 2;
pub(crate) const TRACE_WRITE: u64 = 3;
pub(crate) const TRACE_SET_LEN: u64 = 4;
pub(crate) const TRACE_SYNC: u64 = 5;
pub(crate) const TRACE_CLOSE: u64 = 6;
const TRACE_CRASH: u64 = 7;
const TRACE_RECOVER_BEGIN: u64 = 8;
const TRACE_RECOVER_DECISION: u64 = 9;
const TRACE_RECOVER_END: u64 = 10;
const TRACE_TRANSIENT_ERROR: u64 = 11;
const TRACE_CAPACITY_EXHAUSTED: u64 = 12;
const TRACE_STALE_HANDLE: u64 = 13;
const TRACE_OUT_OF_RANGE: u64 = 14;
const TRACE_FAULTS_TOGGLED: u64 = 15;
const TRACE_CAPACITY_CHANGED: u64 = 16;
const TRACE_SCHEDULE_CHANGED: u64 = 17;
const TRACE_CLOSED_REFUSAL: u64 = 18;

// Recovery decisions for one unsynced mutation.
const DECISION_KEEP: u64 = 0;
const DECISION_DROP: u64 = 1;
const DECISION_TORN_PREFIX: u64 = 2;

/// Seeded fault schedule configuration. All arms draw from the same seed, so
/// one `u64` replays the complete schedule.
#[derive(Clone, Debug)]
pub struct FaultConfig {
    /// Seed for the fault-schedule PRNG stream.
    pub seed: u64,
    /// Torn-write granularity in bytes: on crash, a torn unsynced write keeps
    /// a prefix that is a whole number of granules. Defaults to 512.
    pub torn_write_granularity: u64,
    /// Half-open range the schedule draws `operations-until-crash` from after
    /// every recovery. `None` disables scheduled crashes; explicit
    /// [`SimDisk::crash`] calls remain available.
    pub crash_after_operations: Option<(u64, u64)>,
    /// Denominator `d` of the per-write/per-sync transient-error probability
    /// `1/d`. Zero disables transient errors.
    pub transient_error_denominator: u64,
    /// Total capacity across all simulated files. Growth beyond it fails with
    /// a storage-full error. `None` means unbounded.
    pub capacity_bytes: Option<u64>,
}

impl FaultConfig {
    /// A schedule with every fault arm disabled: a plain deterministic disk.
    #[must_use]
    pub const fn quiet(seed: u64) -> Self {
        Self {
            seed,
            torn_write_granularity: 512,
            crash_after_operations: None,
            transient_error_denominator: 0,
            capacity_bytes: None,
        }
    }
}

/// Counters proving which fault arms actually fired for a seed. A fault kind
/// no seed can reach would make `SIM-002` evidence vacuous; sweep tests assert
/// these stay live.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultCounters {
    /// Acknowledged writes.
    pub writes: u64,
    /// Acknowledged syncs (volatile image folded into the durable image).
    pub syncs: u64,
    /// Acknowledged reads.
    pub reads: u64,
    /// Crashes, scheduled or explicit.
    pub crashes: u64,
    /// Completed [`SimDisk::recover_after_crash`] calls.
    pub recoveries: u64,
    /// Injected transient write/sync errors.
    pub transient_errors: u64,
    /// Rejected growth beyond the configured capacity.
    pub capacity_rejections: u64,
    /// Unsynced mutations kept whole during crash recovery.
    pub torn_kept: u64,
    /// Unsynced mutations dropped whole during crash recovery.
    pub torn_dropped: u64,
    /// Unsynced writes kept as a granule prefix during crash recovery.
    pub torn_truncated: u64,
}

#[derive(Debug)]
enum Mutation {
    Write { offset: u64, data: Vec<u8> },
    SetLen { len: u64 },
}

/// Seed-drawn fate of one unsynced mutation at crash recovery.
enum RecoveryDecision {
    Keep,
    Drop,
    TornPrefix(u64),
}

#[derive(Debug, Default)]
struct SimFile {
    /// Contents as of the last acknowledged sync.
    durable: Vec<u8>,
    /// Contents as seen by readers: durable plus unsynced mutations.
    volatile: Vec<u8>,
    /// Ordered unsynced mutations; each faces an independent keep, drop, or
    /// prefix-truncate decision on crash.
    unsynced: Vec<Mutation>,
}

#[derive(Debug)]
struct DiskInner {
    files: BTreeMap<String, SimFile>,
    config: FaultConfig,
    rng: SplitMix64,
    trace: TraceHash,
    counters: FaultCounters,
    crashed: bool,
    /// Bumped on every recovery; handles from earlier epochs fail closed.
    epoch: u64,
    /// Remaining countable operations before the next scheduled crash.
    operations_until_crash: Option<u64>,
    faults_enabled: bool,
}

impl DiskInner {
    fn fold_name(&mut self, file: &str) {
        self.trace.fold_u64(file.len() as u64);
        self.trace.fold_bytes(file.as_bytes());
    }

    /// Folds a self-sufficient out-of-range refusal: operation kind, offset,
    /// and length, so the chain does not depend on refusals being derivable
    /// from prior events.
    fn fold_out_of_range(&mut self, file: &str, operation: u64, offset: u64, length: u64) {
        self.trace.fold_u64(TRACE_OUT_OF_RANGE);
        self.fold_name(file);
        self.trace.fold_u64(operation);
        self.trace.fold_u64(offset);
        self.trace.fold_u64(length);
    }

    fn draw_crash_countdown(&mut self) -> Option<u64> {
        let (low, high) = self.config.crash_after_operations?;
        assert!(
            low < high,
            "crash_after_operations must be a nonempty range"
        );
        Some(low + self.rng.next_below(high - low))
    }

    /// Injects the transient-error and scheduled-crash arms for one countable
    /// operation. `Err` means the operation must not be applied.
    fn fault_gate(&mut self, transient_eligible: bool) -> Result<(), io::Error> {
        if !self.faults_enabled {
            return Ok(());
        }
        if transient_eligible && self.rng.chance(1, self.config.transient_error_denominator) {
            self.counters.transient_errors += 1;
            self.trace.fold_u64(TRACE_TRANSIENT_ERROR);
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "simulated transient storage error",
            ));
        }
        if let Some(remaining) = self.operations_until_crash.as_mut() {
            if *remaining == 0 {
                self.crash();
                return Err(crashed_error());
            }
            *remaining -= 1;
        }
        Ok(())
    }

    fn crash(&mut self) {
        self.crashed = true;
        self.counters.crashes += 1;
        self.trace.fold_u64(TRACE_CRASH);
        self.trace.fold_u64(self.epoch);
    }

    fn total_volatile_bytes(&self) -> u64 {
        self.files
            .values()
            .map(|file| file.volatile.len() as u64)
            .sum()
    }
}

fn crashed_error() -> io::Error {
    io::Error::other("simulated disk crashed; outstanding handles fail closed")
}

fn stale_error() -> io::Error {
    io::Error::other("simulated disk handle predates the last crash recovery")
}

fn out_of_range_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "simulated access beyond file length",
    )
}

/// A simulated disk holding named files, each with a durable image (contents
/// as of the last acknowledged sync) and a volatile image (unsynced writes).
///
/// `write` mutates the volatile image, `sync` folds volatile into durable, and
/// `read` sees volatile-over-durable. [`SimDisk::crash`] is a first-class
/// operation: outstanding handles fail closed until
/// [`SimDisk::recover_after_crash`] resolves every unsynced mutation with a
/// seeded keep/drop/prefix-truncate decision and opens a fresh handle epoch.
#[derive(Clone, Debug)]
pub struct SimDisk {
    inner: Arc<Mutex<DiskInner>>,
}

impl SimDisk {
    /// Creates a disk with the given fault schedule.
    #[must_use]
    pub fn new(config: FaultConfig) -> Self {
        assert!(
            config.torn_write_granularity > 0,
            "torn_write_granularity must be nonzero"
        );
        let mut inner = DiskInner {
            files: BTreeMap::new(),
            rng: SplitMix64::new(config.seed),
            config,
            trace: TraceHash::new(),
            counters: FaultCounters::default(),
            crashed: false,
            epoch: 0,
            operations_until_crash: None,
            faults_enabled: true,
        };
        inner.operations_until_crash = inner.draw_crash_countdown();
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, DiskInner> {
        self.inner
            .lock()
            .expect("simulated disk lock poisoned by a panicking test")
    }

    /// Current handle epoch; recovery bumps it so stale handles fail closed.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.lock().epoch
    }

    /// Whether the disk is crashed and awaiting recovery.
    #[must_use]
    pub fn is_crashed(&self) -> bool {
        self.lock().crashed
    }

    /// Digest of the versioned trace chain over every operation and fault
    /// decision so far.
    #[must_use]
    pub fn trace_digest(&self) -> u64 {
        self.lock().trace.digest()
    }

    /// Counters of operations and fired fault arms.
    #[must_use]
    pub fn counters(&self) -> FaultCounters {
        self.lock().counters
    }

    /// Enables or disables the seeded fault arms (crash schedule, transient
    /// errors). Deterministic campaigns use this to guarantee termination;
    /// the toggle itself is traced.
    pub fn set_faults_enabled(&self, enabled: bool) {
        let mut inner = self.lock();
        inner.faults_enabled = enabled;
        inner.trace.fold_u64(TRACE_FAULTS_TOGGLED);
        inner.trace.fold_u64(u64::from(enabled));
    }

    /// Changes the configured capacity (the simulated operator adding or
    /// removing space). Capacity exhaustion is state-derived rather than
    /// schedule-drawn, so recovery from a full disk needs this seam. The
    /// change itself is traced.
    pub fn set_capacity_bytes(&self, capacity: Option<u64>) {
        let mut inner = self.lock();
        inner.config.capacity_bytes = capacity;
        inner.trace.fold_u64(TRACE_CAPACITY_CHANGED);
        inner
            .trace
            .fold_u64(capacity.map_or(u64::MAX, |bytes| bytes));
    }

    /// Replaces the crash schedule and immediately redraws the countdown, so
    /// a campaign can aim crashes at a specific window (for example the
    /// recovery reopen reads). The change itself is traced.
    pub fn set_crash_after_operations(&self, range: Option<(u64, u64)>) {
        let mut inner = self.lock();
        inner.config.crash_after_operations = range;
        inner.operations_until_crash = inner.draw_crash_countdown();
        inner.trace.fold_u64(TRACE_SCHEDULE_CHANGED);
        let (low, high) = range.unwrap_or((u64::MAX, u64::MAX));
        inner.trace.fold_u64(low);
        inner.trace.fold_u64(high);
    }

    /// Copy of a file's durable image (test oracle surface).
    #[must_use]
    pub fn durable_bytes(&self, file: &str) -> Vec<u8> {
        self.lock()
            .files
            .get(file)
            .map(|state| state.durable.clone())
            .unwrap_or_default()
    }

    /// Copy of a file's volatile image (test oracle surface).
    #[must_use]
    pub fn volatile_bytes(&self, file: &str) -> Vec<u8> {
        self.lock()
            .files
            .get(file)
            .map(|state| state.volatile.clone())
            .unwrap_or_default()
    }

    /// Crashes the disk: outstanding handles fail closed until recovery.
    pub fn crash(&self) {
        let mut inner = self.lock();
        if !inner.crashed {
            inner.crash();
        }
    }

    /// Resolves the crash: every unsynced mutation is independently kept,
    /// dropped, or prefix-truncated at the configured granularity; volatile
    /// images are rebuilt from the surviving durable images; a fresh handle
    /// epoch opens. Panics if the disk is not crashed — recovering a healthy
    /// disk is a harness bug.
    pub fn recover_after_crash(&self) {
        let mut inner = self.lock();
        assert!(inner.crashed, "recover_after_crash requires a crashed disk");
        inner.trace.fold_u64(TRACE_RECOVER_BEGIN);
        let granularity = inner.config.torn_write_granularity;
        let names: Vec<String> = inner.files.keys().cloned().collect();
        for name in names {
            let mut file = inner
                .files
                .remove(&name)
                .expect("file present while recovering");
            let mutations = std::mem::take(&mut file.unsynced);
            for (index, mutation) in mutations.into_iter().enumerate() {
                let decision = match &mutation {
                    Mutation::Write { data, .. } => match inner.rng.next_below(4) {
                        // Writes: keep 2/4, drop 1/4, prefix-truncate 1/4.
                        0 | 1 => RecoveryDecision::Keep,
                        2 => RecoveryDecision::Drop,
                        _ => {
                            let granules = (data.len() as u64).div_ceil(granularity).max(1);
                            RecoveryDecision::TornPrefix(
                                inner.rng.next_below(granules) * granularity,
                            )
                        }
                    },
                    // Length changes are metadata: they survive whole or not
                    // at all (keep 1/2, drop 1/2).
                    Mutation::SetLen { .. } => {
                        if inner.rng.next_below(2) == 0 {
                            RecoveryDecision::Keep
                        } else {
                            RecoveryDecision::Drop
                        }
                    }
                };
                let (decision_tag, kept_bytes) = match decision {
                    RecoveryDecision::Keep => {
                        inner.counters.torn_kept += 1;
                        (
                            DECISION_KEEP,
                            apply_kept_mutation(&mut file, &mutation, u64::MAX),
                        )
                    }
                    RecoveryDecision::Drop => {
                        inner.counters.torn_dropped += 1;
                        (DECISION_DROP, 0)
                    }
                    RecoveryDecision::TornPrefix(kept) => {
                        inner.counters.torn_truncated += 1;
                        (
                            DECISION_TORN_PREFIX,
                            apply_kept_mutation(&mut file, &mutation, kept),
                        )
                    }
                };
                inner.trace.fold_u64(TRACE_RECOVER_DECISION);
                inner.trace.fold_u64(index as u64);
                inner.trace.fold_u64(decision_tag);
                inner.trace.fold_u64(kept_bytes);
            }
            file.volatile = file.durable.clone();
            inner.files.insert(name, file);
        }
        inner.crashed = false;
        inner.epoch += 1;
        inner.counters.recoveries += 1;
        inner.operations_until_crash = inner.draw_crash_countdown();
        let epoch = inner.epoch;
        inner.trace.fold_u64(TRACE_RECOVER_END);
        inner.trace.fold_u64(epoch);
    }

    pub(crate) fn guarded_len(&self, epoch: u64, file: &str) -> Result<u64, io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_LEN)?;
        let length = inner
            .files
            .get(file)
            .map_or(0, |state| state.volatile.len() as u64);
        inner.trace.fold_u64(TRACE_LEN);
        inner.fold_name(file);
        inner.trace.fold_u64(length);
        Ok(length)
    }

    pub(crate) fn guarded_read(
        &self,
        epoch: u64,
        file: &str,
        offset: u64,
        out: &mut [u8],
    ) -> Result<(), io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_READ)?;
        // Reads count toward the crash schedule so crashes can land inside a
        // recovery/reopen window, not only between workload writes.
        inner.fault_gate(false)?;
        let state = inner.files.entry(file.to_owned()).or_default();
        let volatile_len = state.volatile.len() as u64;
        let out_of_range = offset
            .checked_add(out.len() as u64)
            .is_none_or(|end| end > volatile_len);
        if out_of_range {
            inner.fold_out_of_range(file, TRACE_READ, offset, out.len() as u64);
            return Err(out_of_range_error());
        }
        let offset = usize::try_from(offset).expect("bounded by volatile length");
        out.copy_from_slice(&state.volatile[offset..offset + out.len()]);
        let digest = fnv1a64(out);
        inner.counters.reads += 1;
        inner.trace.fold_u64(TRACE_READ);
        inner.fold_name(file);
        inner.trace.fold_u64(offset as u64);
        inner.trace.fold_u64(out.len() as u64);
        inner.trace.fold_u64(digest);
        Ok(())
    }

    pub(crate) fn guarded_write(
        &self,
        epoch: u64,
        file: &str,
        offset: u64,
        data: &[u8],
    ) -> Result<(), io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_WRITE)?;
        inner.fault_gate(true)?;
        let state = inner.files.entry(file.to_owned()).or_default();
        let volatile_len = state.volatile.len() as u64;
        // Beyond-length writes mirror redb's in-memory backend contract: the
        // engine always grows the file through `set_len` before writing.
        let out_of_range = offset
            .checked_add(data.len() as u64)
            .is_none_or(|end| end > volatile_len);
        if out_of_range {
            inner.fold_out_of_range(file, TRACE_WRITE, offset, data.len() as u64);
            return Err(out_of_range_error());
        }
        let start = usize::try_from(offset).expect("bounded by volatile length");
        state.volatile[start..start + data.len()].copy_from_slice(data);
        state.unsynced.push(Mutation::Write {
            offset,
            data: data.to_vec(),
        });
        let digest = fnv1a64(data);
        inner.counters.writes += 1;
        inner.trace.fold_u64(TRACE_WRITE);
        inner.fold_name(file);
        inner.trace.fold_u64(offset);
        inner.trace.fold_u64(data.len() as u64);
        inner.trace.fold_u64(digest);
        Ok(())
    }

    pub(crate) fn guarded_set_len(
        &self,
        epoch: u64,
        file: &str,
        len: u64,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_SET_LEN)?;
        let current = inner
            .files
            .get(file)
            .map_or(0, |state| state.volatile.len() as u64);
        if let Some(capacity) = inner.config.capacity_bytes
            && len > current
            && inner.total_volatile_bytes() + (len - current) > capacity
        {
            inner.counters.capacity_rejections += 1;
            inner.trace.fold_u64(TRACE_CAPACITY_EXHAUSTED);
            inner.fold_name(file);
            inner.trace.fold_u64(TRACE_SET_LEN);
            inner.trace.fold_u64(len);
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulated disk capacity exhausted",
            ));
        }
        inner.fault_gate(false)?;
        let state = inner.files.entry(file.to_owned()).or_default();
        let Ok(target) = usize::try_from(len) else {
            inner.fold_out_of_range(file, TRACE_SET_LEN, 0, len);
            return Err(out_of_range_error());
        };
        // Growth zero-initializes, per the redb StorageBackend contract.
        state.volatile.resize(target, 0);
        state.unsynced.push(Mutation::SetLen { len });
        inner.trace.fold_u64(TRACE_SET_LEN);
        inner.fold_name(file);
        inner.trace.fold_u64(len);
        Ok(())
    }

    pub(crate) fn guarded_sync(&self, epoch: u64, file: &str) -> Result<(), io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_SYNC)?;
        // A crash at the sync boundary strikes before the fold: the sync is
        // not acknowledged and its mutations stay torn-write candidates.
        inner.fault_gate(true)?;
        let state = inner.files.entry(file.to_owned()).or_default();
        state.durable = state.volatile.clone();
        state.unsynced.clear();
        inner.counters.syncs += 1;
        inner.trace.fold_u64(TRACE_SYNC);
        inner.fold_name(file);
        Ok(())
    }

    /// Folds a refusal issued by an already-closed backend handle. The
    /// refusal never reaches the guarded operations, so it carries its own
    /// discriminator and the refused operation kind; it consumes no fault
    /// randomness and is legal in any disk state.
    pub(crate) fn trace_closed_refusal(&self, file: &str, operation: u64) {
        let mut inner = self.lock();
        inner.trace.fold_u64(TRACE_CLOSED_REFUSAL);
        inner.fold_name(file);
        inner.trace.fold_u64(operation);
    }

    pub(crate) fn trace_close(&self, epoch: u64, file: &str) -> Result<(), io::Error> {
        let mut inner = self.lock();
        check_epoch(&mut inner, epoch, file, TRACE_CLOSE)?;
        inner.trace.fold_u64(TRACE_CLOSE);
        inner.fold_name(file);
        Ok(())
    }
}

/// Applies the kept portion of one recovered mutation to the durable image and
/// returns the number of bytes applied. Writes landing beyond the surviving
/// file length are clamped: their covering length change did not survive, so
/// the orphaned tail is unreachable, exactly as on a real medium.
fn apply_kept_mutation(file: &mut SimFile, mutation: &Mutation, kept_limit: u64) -> u64 {
    match mutation {
        Mutation::SetLen { len } => {
            let target = usize::try_from(*len).expect("set_len accepted within capacity");
            file.durable.resize(target, 0);
            *len
        }
        Mutation::Write { offset, data } => {
            let kept = (data.len() as u64).min(kept_limit);
            let durable_len = file.durable.len() as u64;
            if *offset >= durable_len {
                return 0;
            }
            let writable = kept.min(durable_len - *offset);
            let start = usize::try_from(*offset).expect("bounded by durable length");
            let end = start + usize::try_from(writable).expect("bounded by durable length");
            file.durable[start..end]
                .copy_from_slice(&data[..usize::try_from(writable).expect("bounded slice")]);
            writable
        }
    }
}

fn check_epoch(
    inner: &mut DiskInner,
    epoch: u64,
    file: &str,
    operation: u64,
) -> Result<(), io::Error> {
    if inner.crashed {
        inner.trace.fold_u64(TRACE_STALE_HANDLE);
        inner.fold_name(file);
        inner.trace.fold_u64(operation);
        return Err(crashed_error());
    }
    if epoch != inner.epoch {
        inner.trace.fold_u64(TRACE_STALE_HANDLE);
        inner.fold_name(file);
        inner.trace.fold_u64(operation);
        return Err(stale_error());
    }
    Ok(())
}
