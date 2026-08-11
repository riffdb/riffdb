#![forbid(unsafe_code)]

//! Process-level crash and authority recovery for the bounded scheduler.

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::future::Future;
use std::num::{NonZeroU8, NonZeroU64};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::time::Duration;

use riffdb_scheduler::{
    BusinessResolution, CheckpointDisposition, ClaimDisposition, FencedClaim, ReleaseDisposition,
    ScheduleAttemptKind, ScheduleDefinition, ScheduleInputError, ScheduleSource, ScheduleTarget,
    ScheduleTargetComponent, ScheduledAttempt, ScheduledWork, SchedulerApplication,
    SchedulerCancellation, SchedulerFailure, SchedulerFailureClass, SchedulerPolicy,
    SchedulerRunError, SchedulerRunResult, SchedulerWakeupHint, SchedulerWorker,
};
use riffdb_types::{QueryOperationName, ReactiveModuleHash, ReactiveOperationName, Timestamp};
use serde::{Deserialize, Serialize};

const CHILD_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_CHILD";
const STATE_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_STATE";
const KILL_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_KILL";
const REVOKE_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_REVOKE";
const MODE_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_MODE";
const DOMAIN_ENV: &str = "RIFFDB_SCHEDULER_RECOVERY_DOMAIN";
const CRASH_EXIT: i32 = 73;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DurableState {
    delivery_attempt: u8,
    revision: u64,
    fence: u64,
    lease_active: bool,
    lease_expired: bool,
    business_key: Option<String>,
    business_revision: Option<u64>,
    business_commits: u64,
    emitted_events: u64,
    releases: u64,
    checkpoints: u64,
    query_selections: u64,
    negative_acks: u64,
}

impl Default for DurableState {
    fn default() -> Self {
        Self {
            delivery_attempt: 1,
            revision: 1,
            fence: 0,
            lease_active: false,
            lease_expired: false,
            business_key: None,
            business_revision: None,
            business_commits: 0,
            emitted_events: 0,
            releases: 0,
            checkpoints: 0,
            query_selections: 0,
            negative_acks: 0,
        }
    }
}

#[derive(Clone)]
struct Work {
    schedule: ScheduleDefinition,
    due: Timestamp,
    target: ScheduleTarget,
    command: QueryOperationName,
    attempt: NonZeroU8,
}

impl ScheduledWork for Work {
    fn schedule(&self) -> &ScheduleDefinition {
        &self.schedule
    }

    fn due(&self) -> Timestamp {
        self.due
    }

    fn target(&self) -> &ScheduleTarget {
        &self.target
    }

    fn business_command(&self) -> &QueryOperationName {
        &self.command
    }

    fn attempt(&self) -> NonZeroU8 {
        self.attempt
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Failure(SchedulerFailureClass);

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scheduler recovery fixture failed")
    }
}

impl Error for Failure {}

impl SchedulerFailure for Failure {
    fn class(&self) -> SchedulerFailureClass {
        self.0
    }
}

struct DurableApplication {
    path: PathBuf,
    state: DurableState,
    revoke_at: Option<String>,
    domain: String,
}

impl DurableApplication {
    fn open(path: PathBuf) -> Self {
        Self {
            state: read_state(&path),
            path,
            revoke_at: env::var(REVOKE_ENV).ok(),
            domain: env::var(DOMAIN_ENV).expect("adapter domain"),
        }
    }

    fn persist(&self) {
        write_state(&self.path, &self.state);
    }

    fn authorize(&self, stage: &str) -> Result<(), Failure> {
        if self.revoke_at.as_deref() == Some(stage) {
            Err(Failure(SchedulerFailureClass::AuthorizationRevoked))
        } else {
            Ok(())
        }
    }

    fn crash(stage: &str) {
        if env::var(KILL_ENV).ok().as_deref() == Some(stage) {
            std::process::exit(CRASH_EXIT);
        }
    }

    fn work(&self) -> Result<Work, ScheduleInputError> {
        let (schedule_name, command_name) = match self.domain.as_str() {
            "mlflow" => ("MlflowRunScheduler", "CompleteRun"),
            "woodpecker" => ("WoodpeckerPipelineScheduler", "StartPipeline"),
            _ => panic!("unknown adapter domain"),
        };
        Ok(Work {
            schedule: schedule(schedule_name),
            due: Timestamp::new(1_788_726_400, 0).expect("fixture timestamp"),
            target: ScheduleTarget::new(
                ScheduleTargetComponent::text("organization-alpha")?,
                vec![ScheduleTargetComponent::text("workflow-17")?],
            )?,
            command: QueryOperationName::new(command_name).expect("command name"),
            attempt: NonZeroU8::new(self.state.delivery_attempt).expect("nonzero attempt"),
        })
    }
}

impl SchedulerApplication for DurableApplication {
    type Work = Work;
    type Failure = Failure;

    fn next(
        &mut self,
        _maximum_wait: Duration,
    ) -> impl Future<Output = Result<Option<Self::Work>, Self::Failure>> + Send {
        Self::crash("before_query");
        let result = self.authorize("query").and_then(|()| {
            if self.state.checkpoints > 0 {
                Ok(None)
            } else {
                self.state.query_selections += 1;
                self.persist();
                self.work()
                    .map(Some)
                    .map_err(|_| Failure(SchedulerFailureClass::Permanent))
            }
        });
        Self::crash("after_query");
        async move { result }
    }

    fn resolve_business(
        &mut self,
        _work: &Self::Work,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<Option<BusinessResolution>, Self::Failure>> + Send {
        let result = self.authorize("resolve").map(|()| {
            self.state.business_key.as_deref().and_then(|key| {
                if key == attempt.idempotency_key() {
                    Some(BusinessResolution::new(
                        NonZeroU64::new(
                            self.state
                                .business_revision
                                .expect("business revision accompanies key"),
                        )
                        .expect("business revision is nonzero"),
                        true,
                    ))
                } else {
                    None
                }
            })
        });
        async move { result }
    }

    fn claim(
        &mut self,
        _work: &Self::Work,
        attempt: &ScheduledAttempt,
        _lease_seconds: NonZeroU64,
    ) -> impl Future<Output = Result<ClaimDisposition, Self::Failure>> + Send {
        Self::crash("before_claim");
        let result = self.authorize("claim").map(|()| {
            assert!(matches!(attempt.kind(), ScheduleAttemptKind::Claim { .. }));
            if self.state.lease_active && !self.state.lease_expired {
                ClaimDisposition::Unavailable
            } else {
                self.state.fence += 1;
                self.state.revision += 1;
                self.state.lease_active = true;
                self.state.lease_expired = false;
                self.persist();
                ClaimDisposition::Acquired(FencedClaim::new(
                    [0x51; 16],
                    NonZeroU64::new(self.state.fence).expect("fence"),
                    NonZeroU64::new(self.state.revision).expect("revision"),
                ))
            }
        });
        Self::crash("after_claim");
        async move { result }
    }

    fn execute(
        &mut self,
        _work: &Self::Work,
        claim: &FencedClaim,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<BusinessResolution, Self::Failure>> + Send {
        Self::crash("before_business");
        let result = self.authorize("business").and_then(|()| {
            if !self.state.lease_active
                || claim.fencing_token().get() != self.state.fence
                || claim.successor_revision().get() != self.state.revision
            {
                return Err(Failure(SchedulerFailureClass::Permanent));
            }
            if let Some(key) = &self.state.business_key {
                if key != attempt.idempotency_key() {
                    return Err(Failure(SchedulerFailureClass::Permanent));
                }
                return Ok(BusinessResolution::new(
                    NonZeroU64::new(
                        self.state
                            .business_revision
                            .expect("business revision accompanies key"),
                    )
                    .expect("revision"),
                    true,
                ));
            }
            self.state.revision += 1;
            self.state.business_revision = Some(self.state.revision);
            self.state.business_key = Some(attempt.idempotency_key().to_owned());
            self.state.business_commits += 1;
            self.state.emitted_events += 1;
            self.persist();
            Self::crash("after_business");
            Self::crash("after_event");
            Ok(BusinessResolution::new(
                NonZeroU64::new(self.state.revision).expect("revision"),
                false,
            ))
        });
        async move { result }
    }

    fn release(
        &mut self,
        _work: &Self::Work,
        claim: &FencedClaim,
        expected_revision: NonZeroU64,
        attempt: &ScheduledAttempt,
    ) -> impl Future<Output = Result<ReleaseDisposition, Self::Failure>> + Send {
        Self::crash("before_release");
        let result = self.authorize("release").and_then(|()| {
            assert!(matches!(
                attempt.kind(),
                ScheduleAttemptKind::Release { .. }
            ));
            if !self.state.lease_active
                || claim.fencing_token().get() != self.state.fence
                || expected_revision.get() != self.state.revision
            {
                return Err(Failure(SchedulerFailureClass::Permanent));
            }
            self.state.lease_active = false;
            self.state.lease_expired = false;
            self.state.revision += 1;
            self.state.releases += 1;
            self.persist();
            Self::crash("after_release");
            Ok(ReleaseDisposition::Released)
        });
        async move { result }
    }

    fn acknowledge(
        &mut self,
        _work: &Self::Work,
    ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send {
        Self::crash("before_checkpoint");
        let result = self.authorize("checkpoint").map(|()| {
            self.state.checkpoints += 1;
            self.persist();
            Self::crash("after_checkpoint");
            CheckpointDisposition::Applied
        });
        async move { result }
    }

    fn negative_acknowledge(
        &mut self,
        _work: &Self::Work,
        _retry_delay: Duration,
    ) -> impl Future<Output = Result<CheckpointDisposition, Self::Failure>> + Send {
        let result = self.authorize("checkpoint").map(|()| {
            self.state.negative_acks += 1;
            self.persist();
            CheckpointDisposition::Applied
        });
        async move { result }
    }
}

fn schedule(name: &str) -> ScheduleDefinition {
    ScheduleDefinition::new(
        QueryOperationName::new(name).expect("schedule name"),
        ScheduleSource::DurableEvents {
            module_hash: ReactiveModuleHash::from_bytes([0x39; 32]),
            operation: ReactiveOperationName::new("DueWorkflowEvents").expect("operation name"),
        },
    )
}

fn policy() -> SchedulerPolicy {
    SchedulerPolicy::new(
        NonZeroU8::new(4).expect("in flight"),
        NonZeroU8::new(10).expect("attempts"),
        NonZeroU64::new(60).expect("lease"),
        NonZeroU64::new(1).expect("retry"),
        NonZeroU64::new(60).expect("retry cap"),
        Duration::ZERO,
    )
    .expect("bounded policy")
}

fn read_state(path: &Path) -> DurableState {
    serde_json::from_slice(&fs::read(path).expect("read durable state"))
        .expect("decode durable state")
}

fn write_state(path: &Path, state: &DurableState) {
    let encoded = serde_json::to_vec(state).expect("encode durable state");
    let temporary = path.with_extension("next");
    fs::write(&temporary, encoded).expect("write durable state");
    fs::rename(temporary, path).expect("publish durable state");
}

fn state_path(case: &str) -> PathBuf {
    let root = PathBuf::from(env::var("HOME").expect("HOME")).join("tmp");
    let directory = root.join(format!("riffdb-scheduler-{}-{case}", std::process::id()));
    fs::create_dir_all(&directory).expect("create scheduler test directory");
    directory.join("state.json")
}

fn launch(
    path: &Path,
    domain: &str,
    kill: Option<&str>,
    revoke: Option<&str>,
    mode: Option<&str>,
) -> ExitStatus {
    let mut command = Command::new(env::current_exe().expect("current test executable"));
    command
        .arg("--exact")
        .arg("scheduler_child")
        .arg("--nocapture")
        .env(CHILD_ENV, "1")
        .env(STATE_ENV, path)
        .env(DOMAIN_ENV, domain);
    if let Some(kill) = kill {
        command.env(KILL_ENV, kill);
    }
    if let Some(revoke) = revoke {
        command.env(REVOKE_ENV, revoke);
    }
    if let Some(mode) = mode {
        command.env(MODE_ENV, mode);
    }
    command.status().expect("launch scheduler recovery child")
}

fn redeliver(path: &Path) {
    let mut state = read_state(path);
    if state.checkpoints == 0 {
        state.delivery_attempt += 1;
        // This models the service-observed passage of the recorded lease
        // deadline. Authoritative availability is still changed only by the
        // next compiled claim command.
        state.lease_expired = state.lease_active;
        write_state(path, &state);
    }
}

fn assert_exactly_once(path: &Path) {
    let state = read_state(path);
    assert_eq!(state.business_commits, 1);
    assert_eq!(state.emitted_events, 1);
    assert_eq!(state.checkpoints, 1);
    assert!(!state.lease_active);
}

#[test]
fn scheduler_child() {
    if env::var(CHILD_ENV).ok().as_deref() != Some("1") {
        return;
    }
    let path = PathBuf::from(env::var(STATE_ENV).expect("state path"));
    let mut application = DurableApplication::open(path);
    if env::var(MODE_ENV).ok().as_deref() == Some("stale") {
        let work = application.work().expect("work");
        let attempt = ScheduledAttempt::derive(
            work.schedule(),
            work.due(),
            work.target(),
            ScheduleAttemptKind::Business(work.business_command().clone()),
        );
        let stale = FencedClaim::new(
            [0x51; 16],
            NonZeroU64::MIN,
            NonZeroU64::new(2).expect("revision"),
        );
        let result = futures_lite(&mut application, &work, &stale, &attempt);
        assert_eq!(
            result.expect_err("stale worker must fail").class(),
            SchedulerFailureClass::Permanent
        );
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("scheduler runtime");
    let result = runtime.block_on(SchedulerWorker::new(policy()).run_once(
        &mut application,
        SchedulerWakeupHint::new(NonZeroU64::MIN),
        &SchedulerCancellation::default(),
    ));
    if env::var(REVOKE_ENV).is_ok() {
        let SchedulerRunError::Application { source, .. } =
            result.expect_err("revoked operation must fail")
        else {
            panic!("revocation must be an application failure");
        };
        assert_eq!(source.class(), SchedulerFailureClass::AuthorizationRevoked);
    } else {
        assert!(matches!(
            result.expect("scheduler run"),
            SchedulerRunResult::Completed { .. } | SchedulerRunResult::Idle
        ));
    }
}

fn futures_lite(
    application: &mut DurableApplication,
    work: &Work,
    claim: &FencedClaim,
    attempt: &ScheduledAttempt,
) -> Result<BusinessResolution, Failure> {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("scheduler runtime")
        .block_on(application.execute(work, claim, attempt))
}

#[test]
fn process_crashes_restart_without_lost_work_or_duplicate_business_state() {
    for domain in ["mlflow", "woodpecker"] {
        for kill in [
            "before_query",
            "after_query",
            "before_claim",
            "after_claim",
            "before_business",
            "after_business",
            "after_event",
            "before_release",
            "after_release",
            "before_checkpoint",
            "after_checkpoint",
        ] {
            let path = state_path(&format!("{domain}-{kill}"));
            write_state(&path, &DurableState::default());
            assert_eq!(
                launch(&path, domain, Some(kill), None, None).code(),
                Some(CRASH_EXIT)
            );
            redeliver(&path);
            assert!(launch(&path, domain, None, None, None).success());
            // A duplicate payload-free wakeup observes the checkpoint and
            // cannot duplicate business state.
            assert!(launch(&path, domain, None, None, None).success());
            assert_exactly_once(&path);
            assert!(launch(&path, domain, None, None, Some("stale")).success());
            fs::remove_dir_all(path.parent().expect("state directory"))
                .expect("remove scheduler test directory");
        }
    }
}

#[test]
fn revoked_authority_cannot_use_a_claim_and_restart_recovers() {
    for domain in ["mlflow", "woodpecker"] {
        let path = state_path(&format!("{domain}-revocation"));
        write_state(&path, &DurableState::default());
        assert!(launch(&path, domain, None, Some("business"), None).success());
        let revoked = read_state(&path);
        assert_eq!(revoked.business_commits, 0);
        assert_eq!(revoked.emitted_events, 0);
        assert_eq!(revoked.checkpoints, 0);
        assert!(revoked.lease_active);

        redeliver(&path);
        assert!(launch(&path, domain, None, None, None).success());
        assert_exactly_once(&path);
        fs::remove_dir_all(path.parent().expect("state directory"))
            .expect("remove scheduler test directory");
    }
}
