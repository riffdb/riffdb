//! The changelog emitter: derives ADR-0100 frames from published snapshots.
//!
//! # The one thing to understand about this module
//!
//! It cannot see writer-private state. Not "does not"; *cannot*. Every read in
//! this file goes through [`PublishedDurableSnapshot`], a trait whose only
//! implementations are pinned snapshots handed over by storage **after** a
//! frontier swap. This module holds no `SharedRedb`, opens no transaction,
//! reads no journal byte, and touches no composite-overlay internal. ADR-0100
//! §2's gating obligation is therefore a property of the type graph, and the
//! falsifiability test in `storage_recovery_matrix` exists to prove that
//! neutering it turns exactly one test red.
//!
//! # Derivation
//!
//! For each observed advancement covering `(predecessor, covered]`:
//!
//! | class | source | attribution |
//! |---|---|---|
//! | `Commit` | `commits` | range over the covered application interval; a row is a segment (ADR-0102) or a legacy commit record |
//! | `Event` | `events` | range over `EventId = (sequence, ordinal)` in the same interval |
//! | `OutboxIntent` | `outbox` | range over the same event-id interval |
//! | `AdministrationAudit` | `audit` | range over the covered administration interval |
//! | `Provenance` | `provenance` | point read per decoded commit record's provenance identity |
//! | `Entity` | `entities` | point read per decoded `CommittedEntityReferenceV2` (the ADR-0083 supersession chain) |
//! | `EventRoute` | `event_routes` | point read per `(partition hash, event id)` of a decoded commit |
//! | `ServiceAuditRequestIndex` | `audit_by_request` | point read per decoded `Service` audit record |
//!
//! An attributed point read that finds nothing is **skipped, not an error**.
//! Under ADR-0102 a command segment *owns* its derived rows — provenance,
//! events, routes, outbox intents and audits live inside the segment row and
//! have no standalone row at all — and that segment row is itself shipped as
//! the `Commit` entry of the same frame. The differential exactness test is
//! what holds this rule honest: if the rule ever skipped a row that really
//! exists, the reconstructed map would stop matching the final snapshot.
//!
//! Entries are canonically ordered by `(class, key)` and deduplicated. Because
//! every read comes from one immutable snapshot, a key touched more than once
//! inside a frame resolves to one value: the covered-frontier value. That is
//! the correct statement for a frame, which a follower applies atomically.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use riffdb_storage_api::{
    ChangelogEmissionStateV1, ChangelogEntryClassV1, ChangelogEntryClassV2, ChangelogEntryV1,
    ChangelogEntryV2, ChangelogFrameBindingV2, ChangelogFrameConsumer, ChangelogFrameConsumerV2,
    ChangelogFrameV1, ChangelogFrameV2, ChangelogPublicationPort, ChangelogResyncReasonV1,
    ChangelogStreamValidatorV1, ChangelogStreamValidatorV2, ChangelogV2RotationReceipt,
    CompositeTableV1, EntityChainStateV1, MAX_CHANGELOG_FRAME_ENTRIES, PublishedDurableSnapshot,
    PublishedFrontierAdvancement, StorageError, StorageErrorKind,
};
use riffdb_types::{
    AdministrationSequence, CommitSequence, DatabaseId, DualFrontier, EventId, PartitionKeyHash,
};

use crate::error::storage_error;
use crate::keys::{
    encode_application_sequence_key, encode_audit_by_request_key, encode_audit_key,
    encode_event_key, encode_event_route_key, encode_provenance_key,
};
use crate::layout::{META_DATABASE_ID, META_HISTORY_INCARNATION};

/// Default bound on advancements buffered between the writer and the emitter.
///
/// One buffered advancement pins one published snapshot, so this is also the
/// bound on how many superseded read views the emitter can hold open. Past it
/// the emitter reports [`ChangelogResyncReasonV1::BufferOverflow`] and stops —
/// it never applies backpressure to the writer (ADR-0101 §4 releases the
/// frontier before any dependent effect, and a dependent effect that could
/// stall the release would invert that).
pub const DEFAULT_CHANGELOG_BUFFER_ADVANCEMENTS: usize = 64;

/// A pinned published durable snapshot handed to the changelog emitter.
///
/// Constructed only at a publication site, from the exact read view that
/// publication installed.
pub(crate) struct RedbPublishedSnapshot {
    access: crate::store::RedbReadAccess,
}

impl RedbPublishedSnapshot {
    pub(crate) const fn new(access: crate::store::RedbReadAccess) -> Self {
        Self { access }
    }
}

impl PublishedDurableSnapshot for RedbPublishedSnapshot {
    fn read_value(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError> {
        self.access.read_value(journal_table(table), key)
    }

    fn read_range(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
        self.access.read_range(
            journal_table(table),
            start_inclusive,
            end_exclusive,
            max_rows,
        )
    }

    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
        self.access.application_frontier()
    }

    fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
        self.access.administration_frontier()
    }
}

const fn journal_table(table: CompositeTableV1) -> crate::journal::JournalTable {
    match table {
        CompositeTableV1::Meta => crate::journal::JournalTable::Meta,
        CompositeTableV1::Entities => crate::journal::JournalTable::Entities,
        CompositeTableV1::SecondaryIndexes => crate::journal::JournalTable::SecondaryIndexes,
        CompositeTableV1::IndexEpochs => crate::journal::JournalTable::IndexEpochs,
        CompositeTableV1::Idempotency => crate::journal::JournalTable::Idempotency,
        CompositeTableV1::IdempotencyPending => crate::journal::JournalTable::IdempotencyPending,
        CompositeTableV1::Events => crate::journal::JournalTable::Events,
        CompositeTableV1::EventRoutes => crate::journal::JournalTable::EventRoutes,
        CompositeTableV1::Outbox => crate::journal::JournalTable::Outbox,
        CompositeTableV1::Provenance => crate::journal::JournalTable::Provenance,
        CompositeTableV1::Commits => crate::journal::JournalTable::Commits,
        CompositeTableV1::Audit => crate::journal::JournalTable::Audit,
        CompositeTableV1::AuditByRequest => crate::journal::JournalTable::AuditByRequest,
        CompositeTableV1::EntityChainHeads => crate::journal::JournalTable::EntityChainHeads,
    }
}

enum EmitterMessage {
    Advancement(PublishedFrontierAdvancement),
    Stop,
}

#[derive(Debug)]
struct EmitterProgress {
    emitted: u64,
    processed: u64,
    state: ChangelogEmissionStateV1,
    covered: DualFrontier,
    chain_hash: [u8; 32],
}

/// The in-process changelog emitter.
///
/// Implements [`ChangelogPublicationPort`]; storage holds it as the publication
/// observer, and it owns a single worker thread that derives, encodes,
/// self-validates, and hands frames to a [`ChangelogFrameConsumer`].
pub struct RedbChangelogEmitter {
    sender: SyncSender<EmitterMessage>,
    stopping: AtomicBool,
    announced: AtomicBool,
    observed: AtomicU64,
    progress: Mutex<EmitterProgress>,
    signal: Condvar,
}

impl std::fmt::Debug for RedbChangelogEmitter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbChangelogEmitter")
            .field("observed", &self.observed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ChangelogPublicationPort for RedbChangelogEmitter {
    /// Storage's publication edge calls this, after the frontier swap.
    ///
    /// It performs one relaxed counter bump and one non-blocking channel
    /// offer. It allocates nothing, performs no I/O, cannot wait on the
    /// consumer, and cannot return an error — the frontier it is being told
    /// about is already published, so there is nothing left to refuse.
    ///
    /// The only lock it can touch is the progress mutex, and only on the
    /// overflow branch, held across a single compare-and-set. That mutex is
    /// never held across derivation, encoding, or a consumer callback, so a
    /// stalled consumer can never be on the other side of it.
    fn observe_published_advancement(&self, advancement: PublishedFrontierAdvancement) {
        let _ = self.observed.fetch_add(1, Ordering::Relaxed);
        match self
            .sender
            .try_send(EmitterMessage::Advancement(advancement))
        {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                // Overflow is the emitter's problem, never the writer's. The
                // advancement is dropped and the emitter goes typed-lagging; it
                // never guesses the boundaries of what it dropped, and it never
                // waits for room.
                self.mark_overflow();
            }
        }
    }
}

impl RedbChangelogEmitter {
    /// Returns the emitter's current lifecycle state.
    #[must_use]
    pub fn state(&self) -> ChangelogEmissionStateV1 {
        self.locked_progress().state
    }

    /// Returns how many frames have been handed to the consumer.
    #[must_use]
    pub fn emitted_frames(&self) -> u64 {
        self.locked_progress().emitted
    }

    /// Returns how many advancements the worker has dequeued and handled.
    #[must_use]
    pub fn processed_advancements(&self) -> u64 {
        self.locked_progress().processed
    }

    /// Returns how many advancements the publication edge offered.
    #[must_use]
    pub fn observed_advancements(&self) -> u64 {
        self.observed.load(Ordering::Relaxed)
    }

    /// Records buffer overflow as the authoritative typed lagging state.
    ///
    /// Recorded by the publication edge itself rather than by the worker: a
    /// consumer that never returns must not be able to hide the fact that the
    /// stream has a hole in it.
    fn mark_overflow(&self) {
        let mut progress = self.locked_progress();
        if progress.state.is_streaming() {
            progress.state =
                ChangelogEmissionStateV1::Resync(ChangelogResyncReasonV1::BufferOverflow);
        }
        drop(progress);
        self.signal.notify_all();
    }

    /// Returns the covered frontier of the last emitted frame.
    #[must_use]
    pub fn emitted_frontier(&self) -> DualFrontier {
        self.locked_progress().covered
    }

    /// Returns the frame hash of the last emitted frame (zeros before the first).
    #[must_use]
    pub fn emitted_chain_hash(&self) -> [u8; 32] {
        self.locked_progress().chain_hash
    }

    /// Blocks until at least `frames` frames were emitted or the emitter stopped.
    ///
    /// This is the deterministic synchronization point tests use instead of a
    /// sleep: it is satisfied by the worker's own progress signal.
    pub fn wait_for_emitted(&self, frames: u64) -> ChangelogEmissionStateV1 {
        let mut progress = self.locked_progress();
        while progress.emitted < frames && progress.state.is_streaming() {
            progress = self
                .signal
                .wait(progress)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        progress.state
    }

    /// Blocks until the emitter has stopped, returning why.
    pub fn wait_for_resync(&self) -> ChangelogResyncReasonV1 {
        let mut progress = self.locked_progress();
        loop {
            if let ChangelogEmissionStateV1::Resync(reason) = progress.state {
                return reason;
            }
            progress = self
                .signal
                .wait(progress)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn locked_progress(&self) -> std::sync::MutexGuard<'_, EmitterProgress> {
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a worker-side stop, announcing it to the consumer first.
    ///
    /// The announcement precedes the state flip so a caller woken by
    /// [`Self::wait_for_resync`] always observes the consumer callback that
    /// belongs to it.
    fn enter_resync(&self, reason: ChangelogResyncReasonV1, consumer: &dyn ChangelogFrameConsumer) {
        if !self.state().is_streaming() {
            return;
        }
        if !self.announced.swap(true, Ordering::AcqRel) {
            consumer.note_resync_required(reason);
        }
        let mut progress = self.locked_progress();
        if progress.state.is_streaming() {
            progress.state = ChangelogEmissionStateV1::Resync(reason);
        }
        drop(progress);
        self.signal.notify_all();
    }

    /// Announces an already-recorded stop that the publication edge observed.
    fn announce_recorded_resync(&self, consumer: &dyn ChangelogFrameConsumer) {
        if let ChangelogEmissionStateV1::Resync(reason) = self.state()
            && !self.announced.swap(true, Ordering::AcqRel)
        {
            consumer.note_resync_required(reason);
        }
    }

    fn enter_resync_v2(
        &self,
        reason: ChangelogResyncReasonV1,
        consumer: &dyn ChangelogFrameConsumerV2,
    ) {
        if !self.state().is_streaming() {
            return;
        }
        if !self.announced.swap(true, Ordering::AcqRel) {
            consumer.note_resync_required(reason);
        }
        let mut progress = self.locked_progress();
        if progress.state.is_streaming() {
            progress.state = ChangelogEmissionStateV1::Resync(reason);
        }
        drop(progress);
        self.signal.notify_all();
    }

    fn announce_recorded_resync_v2(&self, consumer: &dyn ChangelogFrameConsumerV2) {
        if let ChangelogEmissionStateV1::Resync(reason) = self.state()
            && !self.announced.swap(true, Ordering::AcqRel)
        {
            consumer.note_resync_required(reason);
        }
    }
}

/// An owning handle to a started emitter and its worker thread.
///
/// Dropping the handle stops the worker and joins it, so an emitter never
/// outlives the scope that started it. A consumer that never returns from
/// [`ChangelogFrameConsumer::accept_frame`] therefore blocks the drop — which
/// is the correct trade: the writer is never blocked, and the owner of a
/// deliberately parked consumer is the one who must release it.
///
/// The store may outlive the handle. A publication arriving after the worker
/// has stopped finds a disconnected channel and is recorded as typed lag, never
/// as an error on the writer.
pub struct RedbChangelogEmitterHandle {
    emitter: Arc<RedbChangelogEmitter>,
    worker: Option<JoinHandle<()>>,
}

impl RedbChangelogEmitterHandle {
    /// Borrows the emitter for inspection.
    #[must_use]
    pub fn emitter(&self) -> &Arc<RedbChangelogEmitter> {
        &self.emitter
    }

    /// Returns the publication port to inject at store bind.
    #[must_use]
    pub fn port(&self) -> Arc<dyn ChangelogPublicationPort> {
        Arc::clone(&self.emitter) as Arc<dyn ChangelogPublicationPort>
    }
}

impl Drop for RedbChangelogEmitterHandle {
    fn drop(&mut self) {
        self.emitter.stopping.store(true, Ordering::Release);
        let _ = self.emitter.sender.try_send(EmitterMessage::Stop);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Starts an emitter bound to one in-process consumer.
///
/// The emitter is constructed before any store: it needs no storage handle,
/// because every snapshot it will ever read arrives with an advancement.
pub fn start_changelog_emitter(
    consumer: Arc<dyn ChangelogFrameConsumer>,
    buffered_advancements: usize,
) -> Result<RedbChangelogEmitterHandle, StorageError> {
    if buffered_advancements == 0 {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let (sender, receiver) = sync_channel(buffered_advancements);
    let emitter = Arc::new(RedbChangelogEmitter {
        sender,
        stopping: AtomicBool::new(false),
        announced: AtomicBool::new(false),
        observed: AtomicU64::new(0),
        progress: Mutex::new(EmitterProgress {
            emitted: 0,
            processed: 0,
            state: ChangelogEmissionStateV1::Streaming,
            covered: DualFrontier::INITIAL,
            chain_hash: [0; 32],
        }),
        signal: Condvar::new(),
    });
    let worker_emitter = Arc::clone(&emitter);
    let worker = std::thread::Builder::new()
        .name("riffdb-changelog".to_owned())
        .spawn(move || run_emitter(&worker_emitter, &receiver, consumer.as_ref()))
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    Ok(RedbChangelogEmitterHandle {
        emitter,
        worker: Some(worker),
    })
}

/// Starts the delete-aware emitter at one exact, durable V1-to-V2 rotation.
///
/// The first observed advancement must continue the receipt's predecessor
/// frontier. Starting after that point without a retained V2 cursor is a typed
/// gap and requires a new bootstrap; this function never guesses how much of
/// the V2 chain a late consumer may have missed.
pub fn start_changelog_emitter_v2(
    consumer: Arc<dyn ChangelogFrameConsumerV2>,
    receipt: ChangelogV2RotationReceipt,
    buffered_advancements: usize,
) -> Result<RedbChangelogEmitterHandle, StorageError> {
    if buffered_advancements == 0 {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let (sender, receiver) = sync_channel(buffered_advancements);
    let emitter = Arc::new(RedbChangelogEmitter {
        sender,
        stopping: AtomicBool::new(false),
        announced: AtomicBool::new(false),
        observed: AtomicU64::new(0),
        progress: Mutex::new(EmitterProgress {
            emitted: 0,
            processed: 0,
            state: ChangelogEmissionStateV1::Streaming,
            covered: receipt.predecessor(),
            chain_hash: receipt.v2_chain_anchor(),
        }),
        signal: Condvar::new(),
    });
    let worker_emitter = Arc::clone(&emitter);
    let worker = std::thread::Builder::new()
        .name("riffdb-changelog-v2".to_owned())
        .spawn(move || {
            run_emitter_v2(&worker_emitter, &receiver, consumer.as_ref(), receipt);
        })
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    Ok(RedbChangelogEmitterHandle {
        emitter,
        worker: Some(worker),
    })
}

fn run_emitter(
    emitter: &Arc<RedbChangelogEmitter>,
    receiver: &Receiver<EmitterMessage>,
    consumer: &dyn ChangelogFrameConsumer,
) {
    let mut anchor: Option<StreamAnchor> = None;
    loop {
        if emitter.stopping.load(Ordering::Acquire) {
            return;
        }
        let Ok(message) = receiver.recv() else {
            return;
        };
        let advancement = match message {
            EmitterMessage::Stop => return,
            EmitterMessage::Advancement(advancement) => advancement,
        };
        // A buffer overflow is recorded by the publication edge, which must
        // never call a consumer. Announcing it is this thread's job.
        emitter.announce_recorded_resync(consumer);
        if emitter.state().is_streaming() {
            handle_advancement(emitter, consumer, &advancement, &mut anchor);
        }
        let mut progress = emitter.locked_progress();
        progress.processed = progress.processed.saturating_add(1);
        drop(progress);
        emitter.signal.notify_all();
    }
}

fn run_emitter_v2(
    emitter: &Arc<RedbChangelogEmitter>,
    receiver: &Receiver<EmitterMessage>,
    consumer: &dyn ChangelogFrameConsumerV2,
    receipt: ChangelogV2RotationReceipt,
) {
    let mut validator = ChangelogStreamValidatorV2::from_rotation(receipt);
    loop {
        if emitter.stopping.load(Ordering::Acquire) {
            return;
        }
        let Ok(message) = receiver.recv() else {
            return;
        };
        let advancement = match message {
            EmitterMessage::Stop => return,
            EmitterMessage::Advancement(advancement) => advancement,
        };
        emitter.announce_recorded_resync_v2(consumer);
        if emitter.state().is_streaming() {
            handle_advancement_v2(emitter, consumer, &advancement, receipt, &mut validator);
        }
        let mut progress = emitter.locked_progress();
        progress.processed = progress.processed.saturating_add(1);
        drop(progress);
        emitter.signal.notify_all();
    }
}

struct StreamAnchor {
    database_id: DatabaseId,
    history_incarnation: u64,
    validator: ChangelogStreamValidatorV1,
}

fn handle_advancement(
    emitter: &Arc<RedbChangelogEmitter>,
    consumer: &dyn ChangelogFrameConsumer,
    advancement: &PublishedFrontierAdvancement,
    anchor: &mut Option<StreamAnchor>,
) {
    let snapshot = advancement.snapshot().as_ref();
    let identity = match read_identity(snapshot) {
        Ok(identity) => identity,
        Err(reason) => {
            emitter.enter_resync(reason, consumer);
            return;
        }
    };
    let anchor = anchor.get_or_insert_with(|| StreamAnchor {
        database_id: identity.0,
        history_incarnation: identity.1,
        validator: ChangelogStreamValidatorV1::anchored_at(
            identity.0,
            identity.1,
            advancement.predecessor(),
            [0; 32],
        ),
    });
    if anchor.database_id != identity.0 || anchor.history_incarnation != identity.1 {
        emitter.enter_resync(ChangelogResyncReasonV1::DerivationFailed, consumer);
        return;
    }
    // Gap-freeness, checked before a single row is read: an advancement whose
    // predecessor is not the emitted frontier means at least one publication
    // never reached this emitter. There is nothing to guess at — the chain
    // either continues exactly or the emitter stops.
    if advancement.predecessor() != anchor.validator.expected_predecessor() {
        emitter.enter_resync(ChangelogResyncReasonV1::FrontierGap, consumer);
        return;
    }
    let frame = match derive_frame(
        advancement,
        anchor.database_id,
        anchor.history_incarnation,
        anchor.validator.expected_chain_hash(),
    ) {
        Ok(frame) => frame,
        Err(reason) => {
            emitter.enter_resync(reason, consumer);
            return;
        }
    };
    let Ok(encoded) = frame.encode() else {
        emitter.enter_resync(ChangelogResyncReasonV1::FrameLimitExceeded, consumer);
        return;
    };
    // Every emitted frame passes the same validating decode a follower will
    // apply. An emitter that could produce a frame its own validator rejects is
    // a gap generator; this is the cheapest place to make that impossible.
    let Ok(validated) = anchor.validator.accept(encoded.as_bytes()) else {
        emitter.enter_resync(ChangelogResyncReasonV1::ValidationFailed, consumer);
        return;
    };
    {
        let mut progress = emitter.locked_progress();
        progress.emitted = progress.emitted.saturating_add(1);
        progress.covered = validated.header().covered();
        progress.chain_hash = encoded.frame_hash();
    }
    consumer.accept_frame(&validated, &encoded);
    emitter.signal.notify_all();
}

fn handle_advancement_v2(
    emitter: &Arc<RedbChangelogEmitter>,
    consumer: &dyn ChangelogFrameConsumerV2,
    advancement: &PublishedFrontierAdvancement,
    receipt: ChangelogV2RotationReceipt,
    validator: &mut ChangelogStreamValidatorV2,
) {
    let snapshot = advancement.snapshot().as_ref();
    let identity = match read_identity(snapshot) {
        Ok(identity) => identity,
        Err(reason) => {
            emitter.enter_resync_v2(reason, consumer);
            return;
        }
    };
    if identity != (receipt.database_id(), receipt.history_incarnation()) {
        emitter.enter_resync_v2(ChangelogResyncReasonV1::DerivationFailed, consumer);
        return;
    }
    if advancement.predecessor() != validator.expected_predecessor() {
        emitter.enter_resync_v2(ChangelogResyncReasonV1::FrontierGap, consumer);
        return;
    }
    let frame = match derive_frame_v2(
        advancement,
        identity.0,
        identity.1,
        validator.expected_chain_hash(),
    ) {
        Ok(frame) => frame,
        Err(reason) => {
            emitter.enter_resync_v2(reason, consumer);
            return;
        }
    };
    let Ok(encoded) = frame.encode() else {
        emitter.enter_resync_v2(ChangelogResyncReasonV1::FrameLimitExceeded, consumer);
        return;
    };
    let Ok(validated) = validator.accept(encoded.as_bytes()) else {
        emitter.enter_resync_v2(ChangelogResyncReasonV1::ValidationFailed, consumer);
        return;
    };
    {
        let mut progress = emitter.locked_progress();
        progress.emitted = progress.emitted.saturating_add(1);
        progress.covered = validated.header().covered();
        progress.chain_hash = encoded.frame_hash();
    }
    consumer.accept_frame(&validated, &encoded);
    emitter.signal.notify_all();
}

fn read_identity(
    snapshot: &dyn PublishedDurableSnapshot,
) -> Result<(DatabaseId, u64), ChangelogResyncReasonV1> {
    let database_row = snapshot
        .read_value(CompositeTableV1::Meta, META_DATABASE_ID.as_bytes())
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        .ok_or(ChangelogResyncReasonV1::DerivationFailed)?;
    let database_id = *crate::codec::decode_database_identity_v1(&database_row)
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        .value();
    let incarnation_row = snapshot
        .read_value(CompositeTableV1::Meta, META_HISTORY_INCARNATION.as_bytes())
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        .ok_or(ChangelogResyncReasonV1::DerivationFailed)?;
    let history_incarnation = *crate::codec::decode_history_incarnation_v1(&incarnation_row)
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        .value();
    Ok((database_id, history_incarnation))
}

/// Derives one changelog frame from a pinned published snapshot.
///
/// Visible for the falsifiability and exactness suites; the derivation is pure
/// with respect to its snapshot and performs no storage mutation.
pub(crate) fn derive_frame(
    advancement: &PublishedFrontierAdvancement,
    database_id: DatabaseId,
    history_incarnation: u64,
    chain_hash: [u8; 32],
) -> Result<ChangelogFrameV1, ChangelogResyncReasonV1> {
    let predecessor = advancement.predecessor();
    let covered = advancement.covered();
    let snapshot = advancement.snapshot().as_ref();
    assert_snapshot_is_the_published_frontier(snapshot, covered)?;

    let mut entries = Vec::new();
    let mut attribution = Attribution::default();

    // Sequence-attributed range reads over the covered application interval.
    if let Some(covered_application) = covered.application() {
        let start = sequence_key_start(predecessor.application());
        let end = sequence_key_end(covered_application);
        for (key, value) in range(snapshot, CompositeTableV1::Commits, &start, &end)? {
            attribute_commit_row(&value, &mut attribution)?;
            entries.push(ChangelogEntryV1::new(
                ChangelogEntryClassV1::Commit,
                key,
                value,
            ));
        }
        let start = event_key_start(predecessor.application());
        let end = event_key_end(covered_application);
        for (key, value) in range(snapshot, CompositeTableV1::Events, &start, &end)? {
            entries.push(ChangelogEntryV1::new(
                ChangelogEntryClassV1::Event,
                key,
                value,
            ));
        }
        for (key, value) in range(snapshot, CompositeTableV1::Outbox, &start, &end)? {
            entries.push(ChangelogEntryV1::new(
                ChangelogEntryClassV1::OutboxIntent,
                key,
                value,
            ));
        }
    }

    // Sequence-attributed range read over the covered administration interval.
    if let Some(covered_administration) = covered.administration() {
        let start = audit_key_start(predecessor.administration());
        let end = audit_key_end(covered_administration);
        for (key, value) in range(snapshot, CompositeTableV1::Audit, &start, &end)? {
            attribute_audit_row(&value, &mut attribution)?;
            entries.push(ChangelogEntryV1::new(
                ChangelogEntryClassV1::AdministrationAudit,
                key,
                value,
            ));
        }
    }

    // Identity-keyed rows named by the records above. Absence means the row is
    // segment-owned (ADR-0102) and already carried by its `Commit` entry.
    for (class, key) in attribution.into_point_reads() {
        if let Some(value) = snapshot
            .read_value(class.table(), &key)
            .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        {
            entries.push(ChangelogEntryV1::new(class, key, value.into_boxed_slice()));
        }
    }

    entries.sort_by(|left, right| {
        (left.class().tag(), left.key()).cmp(&(right.class().tag(), right.key()))
    });
    entries.dedup_by(|left, right| left.class() == right.class() && left.key() == right.key());
    if entries.len() > MAX_CHANGELOG_FRAME_ENTRIES {
        return Err(ChangelogResyncReasonV1::FrameLimitExceeded);
    }

    ChangelogFrameV1::new(
        riffdb_storage_api::ChangelogFrameBindingV1::new(
            database_id,
            history_incarnation,
            chain_hash,
            advancement.frame_hash(),
            advancement.journaled(),
        ),
        predecessor,
        covered,
        entries,
    )
    .map_err(|_| ChangelogResyncReasonV1::FrameLimitExceeded)
}

/// Derives one delete-aware V2 frame from an exact published snapshot.
///
/// Every touched entity contributes its final materialized row when live, one
/// semantic tombstone for each delete transition, and one required final chain
/// head. The transitions themselves come only from the current command-segment
/// record shipped in the same frame; an old segment that touched entities but
/// cannot carry transitions is refused rather than inferred.
pub(crate) fn derive_frame_v2(
    advancement: &PublishedFrontierAdvancement,
    database_id: DatabaseId,
    history_incarnation: u64,
    chain_hash: [u8; 32],
) -> Result<ChangelogFrameV2, ChangelogResyncReasonV1> {
    let predecessor = advancement.predecessor();
    let covered = advancement.covered();
    let snapshot = advancement.snapshot().as_ref();
    assert_snapshot_is_the_published_frontier(snapshot, covered)?;

    let mut entries = Vec::new();
    let mut attribution = Attribution::default();

    if let Some(covered_application) = covered.application() {
        let start = sequence_key_start(predecessor.application());
        let end = sequence_key_end(covered_application);
        for (key, value) in range(snapshot, CompositeTableV1::Commits, &start, &end)? {
            attribute_commit_row(&value, &mut attribution)?;
            entries.push(v2_put(ChangelogEntryClassV2::Commit, key, value)?);
        }
        let start = event_key_start(predecessor.application());
        let end = event_key_end(covered_application);
        for (key, value) in range(snapshot, CompositeTableV1::Events, &start, &end)? {
            entries.push(v2_put(ChangelogEntryClassV2::Event, key, value)?);
        }
        for (key, value) in range(snapshot, CompositeTableV1::Outbox, &start, &end)? {
            entries.push(v2_put(ChangelogEntryClassV2::OutboxIntent, key, value)?);
        }
    }

    if let Some(covered_administration) = covered.administration() {
        let start = audit_key_start(predecessor.administration());
        let end = audit_key_end(covered_administration);
        for (key, value) in range(snapshot, CompositeTableV1::Audit, &start, &end)? {
            attribute_audit_row(&value, &mut attribution)?;
            entries.push(v2_put(
                ChangelogEntryClassV2::AdministrationAudit,
                key,
                value,
            )?);
        }
    }

    if attribution.unrepresentable_entity_segment {
        return Err(ChangelogResyncReasonV1::DerivationFailed);
    }
    let covered_application = covered.application();
    let predecessor_application = predecessor.application();
    let mut prior_transition_position = None;
    for transition in &attribution.entity_transitions {
        let position = (transition.command_sequence(), transition.mutation_ordinal());
        if prior_transition_position.is_some_and(|prior| prior >= position)
            || predecessor_application.is_some_and(|prior| transition.command_sequence() <= prior)
            || covered_application.is_none_or(|last| transition.command_sequence() > last)
        {
            return Err(ChangelogResyncReasonV1::DerivationFailed);
        }
        prior_transition_position = Some(position);
        attribution
            .entities
            .push(Box::from(crate::keys::encode_entity_key(
                transition.target().key(),
            )));
        attribution
            .entity_chain_heads
            .push(Box::from(crate::keys::encode_entity_key(
                transition.target().key(),
            )));
        if transition.next_state() == EntityChainStateV1::Deleted {
            entries.push(
                ChangelogEntryV2::entity_delete_tombstone(transition.clone())
                    .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?,
            );
        }
    }

    for (class, key) in attribution.into_v2_point_reads() {
        let value = snapshot
            .read_value(
                class
                    .legacy_put_table()
                    .unwrap_or(CompositeTableV1::EntityChainHeads),
                &key,
            )
            .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
        match (class, value) {
            (ChangelogEntryClassV2::EntityChainHead, None) => {
                return Err(ChangelogResyncReasonV1::DerivationFailed);
            }
            (_, None) => {}
            (_, Some(value)) => entries.push(v2_put(class, key, value.into_boxed_slice())?),
        }
    }

    entries.sort_by(compare_v2_entries);
    entries.dedup_by(|left, right| match (&*left, &*right) {
        (
            ChangelogEntryV2::Put {
                class: left_class,
                key: left_key,
                ..
            },
            ChangelogEntryV2::Put {
                class: right_class,
                key: right_key,
                ..
            },
        ) => left_class == right_class && left_key == right_key,
        (
            ChangelogEntryV2::EntityDeleteTombstone(left),
            ChangelogEntryV2::EntityDeleteTombstone(right),
        ) => left.transition_hash() == right.transition_hash(),
        _ => false,
    });
    if entries.len() > MAX_CHANGELOG_FRAME_ENTRIES {
        return Err(ChangelogResyncReasonV1::FrameLimitExceeded);
    }

    ChangelogFrameV2::new(
        ChangelogFrameBindingV2 {
            database_id,
            history_incarnation,
            chain_hash,
            journal_frame_hash: advancement.frame_hash(),
            journaled: advancement.journaled(),
        },
        predecessor,
        covered,
        entries,
    )
    .map_err(|_| ChangelogResyncReasonV1::FrameLimitExceeded)
}

fn v2_put(
    class: ChangelogEntryClassV2,
    key: impl Into<Box<[u8]>>,
    value: impl Into<Box<[u8]>>,
) -> Result<ChangelogEntryV2, ChangelogResyncReasonV1> {
    ChangelogEntryV2::put(class, key, value)
        .map_err(|_| ChangelogResyncReasonV1::FrameLimitExceeded)
}

fn compare_v2_entries(left: &ChangelogEntryV2, right: &ChangelogEntryV2) -> std::cmp::Ordering {
    let class = left.class().cmp(&right.class());
    if class != std::cmp::Ordering::Equal {
        return class;
    }
    match (left.delete_transition(), right.delete_transition()) {
        (Some(left), Some(right)) => (
            left.command_sequence(),
            left.mutation_ordinal(),
            left.target().key().as_bytes(),
        )
            .cmp(&(
                right.command_sequence(),
                right.mutation_ordinal(),
                right.target().key().as_bytes(),
            )),
        (None, None) => left.key().cmp(right.key()),
        (Some(_), None) => std::cmp::Ordering::Greater,
        (None, Some(_)) => std::cmp::Ordering::Less,
    }
}

/// The ADR-0100 §2 gate, executable.
///
/// A pinned published snapshot must be *exactly* the frontier the advancement
/// published — no more. If the emitter were ever handed a writer-private root,
/// an unflushed subgroup, or any later snapshot, that root's own frontier would
/// exceed the covered frontier and the probe rows past the covered frontier
/// would exist. Both are checked. Neutering the gate at the publication site
/// makes exactly this function fail, and the emitter stops rather than shipping
/// state the primary's own recovery may erase.
fn assert_snapshot_is_the_published_frontier(
    snapshot: &dyn PublishedDurableSnapshot,
    covered: DualFrontier,
) -> Result<(), ChangelogResyncReasonV1> {
    let application = snapshot
        .application_frontier()
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
    let administration = snapshot
        .administration_frontier()
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
    if application != covered.application() || administration != covered.administration() {
        return Err(ChangelogResyncReasonV1::UnpublishedStateVisible);
    }
    if let Some(next) = covered
        .application()
        .map_or(Some(CommitSequence::first()), CommitSequence::checked_next)
        && snapshot
            .read_value(
                CompositeTableV1::Commits,
                encode_application_sequence_key(next).as_slice(),
            )
            .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
            .is_some()
    {
        return Err(ChangelogResyncReasonV1::UnpublishedStateVisible);
    }
    if let Some(next) = covered.administration().map_or(
        Some(AdministrationSequence::first()),
        AdministrationSequence::checked_next,
    ) && snapshot
        .read_value(CompositeTableV1::Audit, encode_audit_key(next).as_slice())
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?
        .is_some()
    {
        return Err(ChangelogResyncReasonV1::UnpublishedStateVisible);
    }
    Ok(())
}

#[derive(Default)]
struct Attribution {
    entities: Vec<Box<[u8]>>,
    entity_chain_heads: Vec<Box<[u8]>>,
    entity_transitions: Vec<riffdb_storage_api::CommittedEntityTransitionV1>,
    unrepresentable_entity_segment: bool,
    provenance: Vec<Box<[u8]>>,
    event_routes: Vec<Box<[u8]>>,
    audit_by_request: Vec<Box<[u8]>>,
}

impl Attribution {
    fn into_point_reads(self) -> Vec<(ChangelogEntryClassV1, Box<[u8]>)> {
        let mut reads = Vec::with_capacity(
            self.entities.len()
                + self.provenance.len()
                + self.event_routes.len()
                + self.audit_by_request.len(),
        );
        reads.extend(
            self.entities
                .into_iter()
                .map(|key| (ChangelogEntryClassV1::Entity, key)),
        );
        reads.extend(
            self.provenance
                .into_iter()
                .map(|key| (ChangelogEntryClassV1::Provenance, key)),
        );
        reads.extend(
            self.event_routes
                .into_iter()
                .map(|key| (ChangelogEntryClassV1::EventRoute, key)),
        );
        reads.extend(
            self.audit_by_request
                .into_iter()
                .map(|key| (ChangelogEntryClassV1::ServiceAuditRequestIndex, key)),
        );
        reads
    }

    fn into_v2_point_reads(self) -> Vec<(ChangelogEntryClassV2, Box<[u8]>)> {
        let mut reads = Vec::with_capacity(
            self.entities.len()
                + self.entity_chain_heads.len()
                + self.provenance.len()
                + self.event_routes.len()
                + self.audit_by_request.len(),
        );
        reads.extend(
            self.entities
                .into_iter()
                .map(|key| (ChangelogEntryClassV2::Entity, key)),
        );
        reads.extend(
            self.entity_chain_heads
                .into_iter()
                .map(|key| (ChangelogEntryClassV2::EntityChainHead, key)),
        );
        reads.extend(
            self.provenance
                .into_iter()
                .map(|key| (ChangelogEntryClassV2::Provenance, key)),
        );
        reads.extend(
            self.event_routes
                .into_iter()
                .map(|key| (ChangelogEntryClassV2::EventRoute, key)),
        );
        reads.extend(
            self.audit_by_request
                .into_iter()
                .map(|key| (ChangelogEntryClassV2::ServiceAuditRequestIndex, key)),
        );
        reads
    }
}

fn attribute_commit_row(
    encoded: &[u8],
    attribution: &mut Attribution,
) -> Result<(), ChangelogResyncReasonV1> {
    match riffdb_storage_api::decode_command_segment_v1(encoded) {
        Ok(segment) => {
            for capsule in segment.value().commands() {
                attribute_commit_record(capsule.base().commit(), attribution);
                if !capsule.base().commit().entity_references().is_empty()
                    && capsule.entity_transitions().is_empty()
                {
                    attribution.unrepresentable_entity_segment = true;
                }
                attribution
                    .entity_transitions
                    .extend(capsule.entity_transitions().iter().cloned());
                for audit in [
                    capsule.base().started_audit(),
                    capsule.base().terminal_audit(),
                ] {
                    attribution.audit_by_request.push(Box::from(
                        encode_audit_by_request_key(
                            audit.request_id(),
                            audit.administration_sequence(),
                        )
                        .as_slice(),
                    ));
                }
            }
            return Ok(());
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(_) => return Err(ChangelogResyncReasonV1::DerivationFailed),
    }
    // Legacy non-segment shape: the entity supersession references and the
    // event references are both derivable without an event-table handle.
    let references = crate::codec::decode_commit_entity_references(encoded)
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
    if !references.value().is_empty() {
        attribution.unrepresentable_entity_segment = true;
    }
    for reference in references.value() {
        attribution
            .entities
            .push(Box::from(crate::keys::encode_entity_key(
                reference.target().key(),
            )));
    }
    Ok(())
}

fn attribute_commit_record(
    commit: &riffdb_storage_api::StoredCommitRecordV1,
    attribution: &mut Attribution,
) {
    attribution.provenance.push(Box::from(
        encode_provenance_key(commit.provenance_id()).as_slice(),
    ));
    for reference in commit.entity_references() {
        attribution
            .entities
            .push(Box::from(crate::keys::encode_entity_key(
                reference.target().key(),
            )));
    }
    let partition: PartitionKeyHash = commit.partition_hash();
    for event in commit.events() {
        attribution.event_routes.push(Box::from(
            encode_event_route_key(partition, event.event_id()).as_slice(),
        ));
    }
}

fn attribute_audit_row(
    encoded: &[u8],
    attribution: &mut Attribution,
) -> Result<(), ChangelogResyncReasonV1> {
    let record = crate::codec::decode_administration_audit_record_v1(encoded)
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
    if let riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(service) = record.value()
    {
        attribution.audit_by_request.push(Box::from(
            encode_audit_by_request_key(service.request_id(), service.administration_sequence())
                .as_slice(),
        ));
    }
    Ok(())
}

fn range(
    snapshot: &dyn PublishedDurableSnapshot,
    table: CompositeTableV1,
    start_inclusive: &[u8],
    end_exclusive: &[u8],
) -> Result<Vec<riffdb_storage_api::CompositeRow>, ChangelogResyncReasonV1> {
    if start_inclusive >= end_exclusive {
        return Ok(Vec::new());
    }
    let rows = snapshot
        .read_range(
            table,
            start_inclusive,
            end_exclusive,
            MAX_CHANGELOG_FRAME_ENTRIES,
        )
        .map_err(|_| ChangelogResyncReasonV1::DerivationFailed)?;
    if rows.len() >= MAX_CHANGELOG_FRAME_ENTRIES {
        return Err(ChangelogResyncReasonV1::FrameLimitExceeded);
    }
    Ok(rows)
}

fn sequence_key_start(predecessor: Option<CommitSequence>) -> [u8; 8] {
    predecessor.map_or([0; 8], |sequence| {
        sequence
            .checked_next()
            .map_or([u8::MAX; 8], encode_application_sequence_key)
    })
}

fn sequence_key_end(covered: CommitSequence) -> [u8; 8] {
    covered
        .checked_next()
        .map_or([u8::MAX; 8], encode_application_sequence_key)
}

fn event_key_start(predecessor: Option<CommitSequence>) -> [u8; 12] {
    let mut key = [0_u8; 12];
    key[..8].copy_from_slice(&sequence_key_start(predecessor));
    key
}

fn event_key_end(covered: CommitSequence) -> [u8; 12] {
    covered.checked_next().map_or([u8::MAX; 12], |next| {
        encode_event_key(EventId::new(next, 0))
    })
}

fn audit_key_start(predecessor: Option<AdministrationSequence>) -> [u8; 9] {
    predecessor.map_or([0; 9], |sequence| {
        sequence
            .checked_next()
            .map_or([u8::MAX; 9], encode_audit_key)
    })
}

fn audit_key_end(covered: AdministrationSequence) -> [u8; 9] {
    covered
        .checked_next()
        .map_or([u8::MAX; 9], encode_audit_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot that reports a frontier of its own choosing and holds no rows.
    struct StubSnapshot {
        application: Option<CommitSequence>,
        administration: Option<AdministrationSequence>,
    }

    impl PublishedDurableSnapshot for StubSnapshot {
        fn read_value(
            &self,
            _table: CompositeTableV1,
            _key: &[u8],
        ) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(None)
        }

        fn read_range(
            &self,
            _table: CompositeTableV1,
            _start_inclusive: &[u8],
            _end_exclusive: &[u8],
            _max_rows: usize,
        ) -> Result<Vec<riffdb_storage_api::CompositeRow>, StorageError> {
            Ok(Vec::new())
        }

        fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError> {
            Ok(self.application)
        }

        fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError> {
            Ok(self.administration)
        }
    }

    fn covered(application: u64, administration: u64) -> DualFrontier {
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        )
    }

    #[test]
    fn the_gate_accepts_only_a_snapshot_at_the_exact_covered_frontier() {
        // Limb 1 of the ADR-0100 §2 gate, pinned directly. The stub holds no
        // rows, so the beyond-frontier probes always pass and this test can only
        // fail or succeed on the frontier comparison itself.
        let exact = StubSnapshot {
            application: CommitSequence::new(5),
            administration: AdministrationSequence::new(9),
        };
        assert_eq!(
            assert_snapshot_is_the_published_frontier(&exact, covered(5, 9)),
            Ok(())
        );

        for (label, ahead) in [
            (
                "an application frontier ahead of covered",
                StubSnapshot {
                    application: CommitSequence::new(6),
                    administration: AdministrationSequence::new(9),
                },
            ),
            (
                "an administration frontier ahead of covered",
                StubSnapshot {
                    application: CommitSequence::new(5),
                    administration: AdministrationSequence::new(10),
                },
            ),
            (
                "both components ahead of covered",
                StubSnapshot {
                    application: CommitSequence::new(6),
                    administration: AdministrationSequence::new(10),
                },
            ),
            (
                "a frontier behind covered",
                StubSnapshot {
                    application: CommitSequence::new(4),
                    administration: AdministrationSequence::new(9),
                },
            ),
        ] {
            assert_eq!(
                assert_snapshot_is_the_published_frontier(&ahead, covered(5, 9)),
                Err(ChangelogResyncReasonV1::UnpublishedStateVisible),
                "{label} must stop the emitter"
            );
        }
    }
}
