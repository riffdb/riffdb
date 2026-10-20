use super::*;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Duration;

// Tests use real Tokio scheduling and real ledger reservations. Channel/gate
// allocations are preexisting cfg(test) instrumentation, not Source graph proofs.
static TEST_SERIAL: Mutex<()> = Mutex::new(());
const LIMIT: Duration = Duration::from_secs(30);
fn deadline() -> Instant {
    Instant::now().checked_add(LIMIT).unwrap()
}
fn reserved(budget: &ProjectionSourceWorkingBudgetV1) -> u64 {
    budget.observation().unwrap().reserved_bytes
}
struct Probe {
    budget: ProjectionSourceWorkingBudgetV1,
    entered: SyncSender<()>,
    release: Receiver<()>,
}
impl ClosedSourceBody for Probe {
    type Output = u8;
    fn actual_budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.budget
    }
    fn run(self) -> u8 {
        self.entered.send(()).unwrap();
        self.release.recv_timeout(LIMIT).unwrap();
        37
    }
}
fn probe(budget: &ProjectionSourceWorkingBudgetV1) -> (Probe, Receiver<()>, SyncSender<()>) {
    let (entered, observed) = sync_channel(1);
    let (release, waited) = sync_channel(1);
    (
        Probe {
            budget: budget.clone(),
            entered,
            release: waited,
        },
        observed,
        release,
    )
}
pub(super) struct WakeGate {
    first: AtomicBool,
    entered: SyncSender<()>,
    release: Mutex<Receiver<()>>,
}
impl WakeGate {
    pub(super) fn before_notify(&self) {
        if !self.first.swap(true, Ordering::AcqRel) {
            self.entered.send(()).unwrap();
            lock(&self.release).recv_timeout(LIMIT).unwrap();
        }
    }
}
fn gate() -> (Arc<WakeGate>, Receiver<()>, SyncSender<()>) {
    let (entered, observed) = sync_channel(1);
    let (release, waited) = sync_channel(1);
    (
        Arc::new(WakeGate {
            first: AtomicBool::new(false),
            entered,
            release: Mutex::new(waited),
        }),
        observed,
        release,
    )
}
fn scaffold(budget: &ProjectionSourceWorkingBudgetV1) -> SourceRuntimeScaffold {
    let scaffold = SourceRuntimeScaffold::bootstrap_on_actual_ledger(budget).unwrap();
    scaffold.freeze_after_actual_parks(deadline()).unwrap();
    scaffold
}
fn pending_state(scaffold: &SourceRuntimeScaffold) -> (bool, bool, bool, u64, Option<ThreadId>) {
    let state = lock(&scaffold.registry().unwrap().state);
    let p = state.pending.as_ref().unwrap();
    (
        p.joined.is_some(),
        p.finished_serial == Some(p.serial),
        p.acknowledged_serial == Some(p.serial),
        p.serial,
        p.worker,
    )
}

// req: PRJ-004, STO-020
#[test]
fn source_task_ready_keeps_full_charge_until_same_worker_post_poll_park() {
    let _serial = lock(&TEST_SERIAL);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let scaffold = scaffold(&budget);
    let baseline = reserved(&budget);
    let (body, body_entered, body_release) = probe(&budget);
    let (gate, wake_entered, wake_release) = gate();
    scaffold.start_inner(body, Some(gate)).unwrap();
    let admitted = reserved(&budget);
    assert!(admitted > baseline);
    scaffold.poll_once().unwrap(); // Register RootWake before allowing completion.
    body_entered.recv_timeout(LIMIT).unwrap();
    body_release.send(()).unwrap();
    wake_entered.recv_timeout(LIMIT).unwrap(); // Worker is inside wake_join, BEFORE release.
    scaffold.poll_once().unwrap();
    let state = pending_state(&scaffold);
    assert!(state.0 && state.1 && !state.2);
    assert!(matches!(
        scaffold.take_acknowledged::<u8>(),
        Err(Refusal::Busy)
    ));
    assert_eq!(reserved(&budget), admitted);
    wake_release.send(()).unwrap();
    let completed = scaffold.wait::<u8>(deadline()).unwrap();
    assert_eq!(*completed, 37);
    // Task/control backing is retired; the actual output Box still owns the allowance.
    assert_eq!(reserved(&budget), admitted);
    drop(completed);
    assert_eq!(reserved(&budget), baseline);
    scaffold.finish_joined().unwrap();
    assert_eq!(reserved(&budget), 0);
}

// req: PRJ-004, STO-020
#[test]
fn source_task_wrong_worker_and_serial_cannot_acknowledge_actual_task() {
    let _serial = lock(&TEST_SERIAL);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let scaffold = scaffold(&budget);
    let (body, entered, release) = probe(&budget);
    let (gate, wake_entered, wake_release) = gate();
    scaffold.start_inner(body, Some(gate)).unwrap();
    scaffold.poll_once().unwrap();
    entered.recv_timeout(LIMIT).unwrap();
    release.send(()).unwrap();
    wake_entered.recv_timeout(LIMIT).unwrap();
    scaffold.poll_once().unwrap();
    let (_, _, ack, serial, worker) = pending_state(&scaffold);
    assert!(!ack);
    assert_ne!(worker, Some(thread::current().id()));
    // Exercise the sole private transition against a genuine live task stamp.
    // Correct serial + wrong worker and correct worker + stale serial both fail.
    // Neither test changes/restores the genuine completion marker.
    {
        let mut state = lock(&scaffold.registry().unwrap().state);
        let p = state.pending.as_mut().unwrap();
        acknowledge_park_slot(p, thread::current().id(), Some(serial));
        assert_ne!(p.acknowledged_serial, Some(serial));
        acknowledge_park_slot(p, worker.unwrap(), Some(serial.checked_add(1).unwrap()));
        assert_ne!(p.acknowledged_serial, Some(serial));
    }
    scaffold.registry().unwrap().observe_actual_park(); // A real root-thread event still fails.
    assert!(!pending_state(&scaffold).2);
    wake_release.send(()).unwrap();
    drop(scaffold.wait::<u8>(deadline()).unwrap());
    scaffold.finish_joined().unwrap();
}

// req: PRJ-004, STO-020
#[test]
fn source_task_timeout_retains_original_slot_and_blocks_new_source_job() {
    let _serial = lock(&TEST_SERIAL);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let scaffold = scaffold(&budget);
    let baseline = reserved(&budget);
    let (body, entered, release) = probe(&budget);
    scaffold.start(body).unwrap();
    entered.recv_timeout(LIMIT).unwrap();
    let held = reserved(&budget);
    assert!(matches!(
        scaffold.wait::<u8>(Instant::now()),
        Err(Refusal::TimedOut)
    ));
    assert_eq!(reserved(&budget), held);
    let (other, _other_entered, _other_release) = probe(&budget);
    assert_eq!(scaffold.start(other), Err(Refusal::Busy));
    assert_eq!(reserved(&budget), held);
    release.send(()).unwrap();
    let completed = scaffold.wait::<u8>(deadline()).unwrap();
    assert_eq!(*completed, 37);
    drop(completed);
    assert_eq!(reserved(&budget), baseline);
    scaffold.finish_joined().unwrap();
    assert_eq!(reserved(&budget), 0);
}

// req: PRJ-004, STO-020
#[test]
fn source_task_cancel_keeps_actual_runtime_and_charge_until_explicit_native_join() {
    let _serial = lock(&TEST_SERIAL);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let scaffold = scaffold(&budget);
    let (body, entered, release) = probe(&budget);
    scaffold.start(body).unwrap();
    entered.recv_timeout(LIMIT).unwrap();
    let held = reserved(&budget);
    drop(scaffold); // Transfers the real Runtime/header/reservations to one escrow.
    assert_eq!(reserved(&budget), held);
    assert!(OWNER_LIVE.load(Ordering::Acquire));
    assert!(matches!(
        SourceRuntimeScaffold::bootstrap_on_actual_ledger(&budget),
        Err(Refusal::Busy)
    ));
    assert_eq!(reserved(&budget), held);
    release.send(()).unwrap();
    // Full Runtime Drop joins actual native threads; no timeout-return inference.
    reap_retained_after_daemon_drain().unwrap();
    assert!(!OWNER_LIVE.load(Ordering::Acquire));
    assert_eq!(reserved(&budget), 0);
}

// req: PRJ-004, STO-020
#[test]
fn source_task_foreign_ledger_refuses_before_task_control_allocation() {
    let _serial = lock(&TEST_SERIAL);
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let foreign = ProjectionSourceWorkingBudgetV1::new();
    let scaffold = scaffold(&budget);
    let baseline = reserved(&budget);
    let foreign_baseline = reserved(&foreign);
    let (body, _entered, _release) = probe(&foreign);
    assert_eq!(scaffold.start(body), Err(Refusal::ForeignLedger));
    assert_eq!(reserved(&budget), baseline);
    assert_eq!(reserved(&foreign), foreign_baseline);
    scaffold.finish_joined().unwrap();
}

// req: PRJ-004, STO-020
#[test]
fn source_execution_startup_prepares_each_configured_database_and_refunds_on_drop_or_busy() {
    let _serial = lock(&TEST_SERIAL);
    let root = tempfile::tempdir().unwrap();
    let config_path = root.path().join("config.toml");
    let alpha_db = root.path().join("alpha.redb");
    let alpha_backups = root.path().join("alpha-backups");
    let zeta_db = root.path().join("zeta.redb");
    let zeta_backups = root.path().join("zeta-backups");
    std::fs::write(
        &config_path,
        format!(
            "[server]
grpc_listen = \"127.0.0.1:7001\"
[databases.alpha]
path = \"{}\"
backup_root = \"{}\"
environment = \"development\"
[databases.zeta]
path = \"{}\"
backup_root = \"{}\"
environment = \"production\"
",
            alpha_db.display(),
            alpha_backups.display(),
            zeta_db.display(),
            zeta_backups.display(),
        ),
    )
    .unwrap();
    let config =
        crate::config::ServerConfig::parse(["--config".into(), config_path.into_os_string()])
            .unwrap();
    let seed_probe = riffdb_storage_redb::SourceExecutionBootstrapV1::try_new().unwrap();
    let seed_baseline = reserved(seed_probe.numerical_budget());
    drop(seed_probe);
    let expected_registry_bytes = arc_allocation::<Registry>().unwrap();

    let mut startup = SourceExecutionStartup::prepare(&config).unwrap();
    let alpha = startup.seeds.take(0).unwrap();
    let zeta = startup.seeds.take(1).unwrap();
    let alpha_budget = alpha.numerical_budget().clone();
    let zeta_budget = zeta.numerical_budget().clone();
    assert!(matches!(startup.seeds.take(0), Err(Refusal::Invariant)));
    assert!(matches!(startup.seeds.take(2), Err(Refusal::Invariant)));
    assert_eq!(
        reserved(&alpha_budget),
        seed_baseline + expected_registry_bytes
    );
    assert_eq!(
        reserved(&zeta_budget),
        seed_baseline + expected_registry_bytes
    );
    drop(startup);
    assert_eq!(reserved(&alpha_budget), seed_baseline);
    assert_eq!(reserved(&zeta_budget), seed_baseline);
    drop(alpha);
    drop(zeta);
    assert_eq!(reserved(&alpha_budget), 0);
    assert_eq!(reserved(&zeta_budget), 0);
    assert!(!OWNER_LIVE.load(Ordering::Acquire));

    let live_budget = ProjectionSourceWorkingBudgetV1::new();
    let live = scaffold(&live_budget);
    let mut busy_startup = SourceExecutionStartup::prepare(&config).unwrap();
    let busy_alpha = busy_startup.seeds.take(0).unwrap();
    let busy_zeta = busy_startup.seeds.take(1).unwrap();
    let busy_alpha_budget = busy_alpha.numerical_budget().clone();
    let busy_zeta_budget = busy_zeta.numerical_budget().clone();
    assert_eq!(
        reserved(&busy_alpha_budget),
        seed_baseline + expected_registry_bytes
    );
    assert_eq!(
        reserved(&busy_zeta_budget),
        seed_baseline + expected_registry_bytes
    );
    assert!(matches!(
        busy_startup.build_existing_daemon_runtime(),
        Err(Refusal::Busy)
    ));
    assert_eq!(reserved(&busy_alpha_budget), seed_baseline);
    assert_eq!(reserved(&busy_zeta_budget), seed_baseline);
    drop(busy_alpha);
    drop(busy_zeta);
    assert_eq!(reserved(&busy_alpha_budget), 0);
    assert_eq!(reserved(&busy_zeta_budget), 0);
    live.finish_joined().unwrap();
    assert!(!OWNER_LIVE.load(Ordering::Acquire));
}
