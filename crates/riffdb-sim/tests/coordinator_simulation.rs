#![forbid(unsafe_code)]

//! ADR-0113 Phase-2 composition and digest proofs.

use std::future::Future;
use std::num::NonZeroU8;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use redb::StorageBackend;
use riffdb_conflict::{
    CancellationToken, ConflictManager, ConflictManagerConfig, ConflictSchedulePoint,
    DeterministicConflictScheduler, ShardedConflictManager,
};
use riffdb_sim::{
    CoordinatorPhase, CoordinatorSchedule, FaultConfig, SimBackend, SimDisk,
    coordinator_decision_digest,
};
use riffdb_types::{AggregateTypeId, ConflictKey, ConflictKeyBuilder};

const LANES: NonZeroU8 = NonZeroU8::new(4).expect("nonzero coordinator lanes");
const FILE: &str = "coordinator-phase2.redb";

#[derive(Default)]
struct RecordingConflictScheduler {
    points: Mutex<Vec<(ConflictSchedulePoint, u64)>>,
}

impl DeterministicConflictScheduler for RecordingConflictScheduler {
    fn checkpoint(&self, point: ConflictSchedulePoint, waiter_id: u64) {
        self.points
            .lock()
            .expect("conflict schedule trace lock")
            .push((point, waiter_id));
    }
}

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<Output>(future: impl Future<Output = Output>) -> Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn conflict_key(lane: u8) -> ConflictKey {
    let mut builder =
        ConflictKeyBuilder::new(AggregateTypeId::new(1).expect("nonzero aggregate type"));
    builder
        .push_u64(u64::from(lane))
        .expect("bounded lane component");
    builder.finish().expect("canonical conflict key")
}

/// Runs the seed-only schedule through the production conflict checkpoints and
/// the Phase-1 simulated disk. The raw disk digest is deliberately not used as
/// the coordinator determinism pin because redb operation ordering is outside
/// this pure scheduler; this driver proves the intended port composition.
fn drive_composed_schedule(seed: u64) -> (CoordinatorSchedule, u64, usize) {
    let schedule = CoordinatorSchedule::generate(seed, LANES).expect("bounded schedule");
    let conflict_trace = Arc::new(RecordingConflictScheduler::default());
    let (conflicts, _deadline_driver) = ShardedConflictManager::with_test_scheduler(
        ConflictManagerConfig::default(),
        conflict_trace.clone(),
    )
    .expect("deterministic conflict manager");
    let disk = SimDisk::new(FaultConfig::quiet(seed));
    let mut backend = SimBackend::new(&disk, FILE);
    backend
        .set_len(u64::from(LANES.get()) * 512)
        .expect("bounded simulated coordinator file");
    backend.sync_data().expect("durable simulated baseline");

    for decision in schedule.decisions() {
        match decision.phase() {
            CoordinatorPhase::Admission => {
                let lease = block_on(conflicts.acquire_mut(
                    vec![conflict_key(decision.lane())],
                    Instant::now() + Duration::from_secs(60),
                    CancellationToken::new(),
                ))
                .expect("seeded admission acquires its production conflict capability");
                lease.release();
            }
            CoordinatorPhase::EpochSeal => backend
                .write(u64::from(decision.lane()) * 512, &[decision.lane(); 512])
                .expect("sealed lane marker"),
            CoordinatorPhase::DurableFence => {
                backend.sync_data().expect("seeded durable fence");
            }
            _ => {}
        }
        if decision.crash_after() {
            disk.crash();
            drop(backend);
            disk.recover_after_crash();
            backend = SimBackend::new(&disk, FILE);
        }
    }

    let points = conflict_trace
        .points
        .lock()
        .expect("conflict schedule trace lock")
        .len();
    (schedule, disk.trace_digest(), points)
}

#[test]
fn coordinator_interleaving_decisions_feed_the_digest() {
    let (first, first_disk_digest, first_conflict_points) =
        drive_composed_schedule(0x7660_0000_0000_0001);
    let (replay, replay_disk_digest, replay_conflict_points) =
        drive_composed_schedule(0x7660_0000_0000_0001);
    assert_eq!(first, replay, "one seed reproduces every lane decision");
    assert_eq!(
        first.digest(),
        replay.digest(),
        "one seed reproduces one digest"
    );
    assert_eq!(
        first_disk_digest, replay_disk_digest,
        "SimDisk composition replays"
    );
    assert_eq!(first_conflict_points, replay_conflict_points);
    assert!(
        first_conflict_points > 0,
        "production conflict hooks were not reached"
    );

    let decisions = first.decisions();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| decision.crash_after())
            .count(),
        1,
        "the seed selects exactly one bounded crash placement"
    );
    for lane in 0..LANES.get() {
        let phases = decisions
            .iter()
            .filter(|decision| decision.lane() == lane)
            .map(|decision| decision.phase())
            .collect::<Vec<_>>();
        assert_eq!(
            phases,
            CoordinatorPhase::ALL,
            "lane {lane} preserves dependencies"
        );
    }

    for index in 0..decisions.len() {
        let mut crash_changed = decisions.to_vec();
        crash_changed[index] =
            crash_changed[index].with_crash_after(!crash_changed[index].crash_after());
        assert_ne!(
            coordinator_decision_digest(first.seed(), first.lane_count(), &crash_changed),
            first.digest(),
            "crash decision {index} was absent from the digest"
        );

        let mut lane_changed = decisions.to_vec();
        lane_changed[index] =
            lane_changed[index].with_lane((lane_changed[index].lane() + 1) % LANES.get());
        assert_ne!(
            coordinator_decision_digest(first.seed(), first.lane_count(), &lane_changed),
            first.digest(),
            "lane decision {index} was absent from the digest"
        );
    }
}

#[test]
fn coordinator_schedule_is_bounded_and_seed_sensitive() {
    let one = CoordinatorSchedule::generate(1, LANES).expect("schedule one");
    let two = CoordinatorSchedule::generate(2, LANES).expect("schedule two");
    assert_ne!(one.decisions(), two.decisions());
    assert_ne!(one.digest(), two.digest());
    assert!(CoordinatorSchedule::generate(3, NonZeroU8::new(9).expect("nonzero")).is_err());
}
