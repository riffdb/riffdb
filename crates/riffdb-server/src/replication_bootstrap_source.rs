//! Bounded async custody for source artifacts. No authorization or wire release
//! is implied here; authenticated composition must check each outbound item.
use riffdb_storage_api::{
    ChangelogCursorErrorV3 as Refusal, ChangelogHistoryPointV3,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapPageV3 as Page,
    ReplicationSourceHoldIdV1 as HoldId, StorageError, StorageErrorKind,
};
use riffdb_storage_redb::{
    RedbBootstrapRepository, RedbBootstrapSourceBuild, RedbHeldBootstrapSource,
    RedbOperationalPorts,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const MAX_JOBS: usize = 4;
const LIFETIME: Duration = Duration::from_secs(15 * 60);

/// One source's fixed capacity for live source pins and open transfer files.
/// Private scratch-directory inventory and durable hold budgets are separate
/// requirements: dropping a job retains artifacts and never removes a hold.
pub struct BootstrapSourceJobs {
    backend: SourceBackend,
    capacity: Arc<Semaphore>,
}

#[derive(Clone)]
enum SourceBackend {
    Direct(Arc<RedbOperationalPorts>),
    Repository(RedbBootstrapRepository),
}

impl BootstrapSourceJobs {
    /// One instance must be shared by the source's replication composition.
    #[must_use]
    pub fn new(ports: Arc<RedbOperationalPorts>) -> Self {
        Self {
            backend: SourceBackend::Direct(ports),
            capacity: Arc::new(Semaphore::new(MAX_JOBS)),
        }
    }

    /// Shares a fixed persistent artifact inventory owned by server composition.
    #[must_use]
    pub fn from_repository(repository: RedbBootstrapRepository) -> Self {
        Self {
            backend: SourceBackend::Repository(repository),
            capacity: Arc::new(Semaphore::new(MAX_JOBS)),
        }
    }

    /// Opens an ID in the configured private area; no peer path is accepted.
    pub async fn begin_managed(&self, id: HoldId) -> Result<BootstrapSourceJob, Refusal> {
        self.open(None, id, false).await
    }

    /// Revalidates an existing ID in the configured private area.
    pub async fn resume_managed(&self, id: HoldId) -> Result<BootstrapSourceJob, Refusal> {
        self.open(None, id, true).await
    }

    /// Creates one private artifact. Existing paths are refused, never replaced.
    pub async fn begin(&self, path: PathBuf, id: HoldId) -> Result<BootstrapSourceJob, Refusal> {
        self.open(Some(path), id, false).await
    }

    /// Reopens a completed artifact with bounded initial progress validation.
    pub async fn resume(&self, path: PathBuf, id: HoldId) -> Result<BootstrapSourceJob, Refusal> {
        self.open(Some(path), id, true).await
    }

    /// Records a currently authorized receiver's durable publication/attachment
    /// acknowledgement. Caller owns that evidence and policy checks. This
    /// bounded operation also handles retries after the prior reply was lost;
    /// it does not reopen or recreate the completed bootstrap artifact.
    pub async fn attach(
        &self,
        manifest: Manifest,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let owner = OwnedJob {
            value: self.backend.clone(),
            permit,
        };
        run_step(
            owner,
            Instant::now() + LIFETIME,
            move |backend| match backend {
                SourceBackend::Direct(ports) => {
                    ports.attach_replication_bootstrap_v3(manifest, acknowledged)
                }
                SourceBackend::Repository(repository) => repository.attach(manifest, acknowledged),
            },
        )
        .await?;
        Ok(())
    }

    /// Advances only a registered follower hold after the receiver persisted its
    /// acknowledgement. A cancelled waiter retains capacity through real work.
    pub async fn acknowledge_follower(
        &self,
        id: HoldId,
        lineage: riffdb_storage_api::ChangelogLineageV3,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Result<(), Refusal> {
        let owner = OwnedJob {
            value: self.backend.clone(),
            permit: Arc::clone(&self.capacity)
                .try_acquire_owned()
                .map_err(|_| unavailable())?,
        };
        run_step(
            owner,
            Instant::now() + LIFETIME,
            move |backend| match backend {
                SourceBackend::Direct(ports) => {
                    ports.acknowledge_replication_follower_v3(id, lineage, acknowledged)
                }
                SourceBackend::Repository(repository) => {
                    repository.acknowledge_follower(id, lineage, acknowledged)
                }
            },
        )
        .await?;
        Ok(())
    }

    async fn open(
        &self,
        path: Option<PathBuf>,
        id: HoldId,
        resume: bool,
    ) -> Result<BootstrapSourceJob, Refusal> {
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let deadline = Instant::now() + LIFETIME;
        let owner = OwnedJob {
            value: self.backend.clone(),
            permit,
        };
        let owner = run_step(owner, deadline, move |backend| match (backend, path) {
            (SourceBackend::Direct(ports), Some(path)) => {
                if resume {
                    ports.begin_replication_bootstrap_resume_v3(&path, id)
                } else {
                    ports.begin_replication_bootstrap_v3(&path, id)
                }
            }
            (SourceBackend::Repository(repository), None) => {
                if resume {
                    repository.resume(id)
                } else {
                    repository.begin(id)
                }
            }
            _ => Err(unavailable()),
        })
        .await?;
        Ok(BootstrapSourceJob {
            owner: Some(owner),
            deadline,
            complete: false,
        })
    }
}

// Resource precedes permit so cancellation releases file locks and snapshot
// pins before making capacity available to another job.
struct OwnedJob<T> {
    value: T,
    permit: OwnedSemaphorePermit,
}

impl<T> OwnedJob<T> {
    fn map<U>(self, step: impl FnOnce(T) -> Result<U, Refusal>) -> Result<OwnedJob<U>, Refusal> {
        let value = step(self.value)?;
        Ok(OwnedJob {
            value,
            permit: self.permit,
        })
    }
}

/// Move-only, deadline-bound construction. Cancellation of an advance fuses the
/// handle while its bounded storage step retains ownership through completion.
pub struct BootstrapSourceJob {
    owner: Option<OwnedJob<RedbBootstrapSourceBuild>>,
    deadline: Instant,
    complete: bool,
}

impl BootstrapSourceJob {
    /// Performs at most one page copy or verification outside the async runtime.
    pub async fn advance(&mut self) -> Result<bool, Refusal> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let returned = run_step(owner, self.deadline, |mut build| {
            let complete = build.advance()?;
            Ok((build, complete))
        })
        .await?;
        let (build, complete) = returned.value;
        self.owner = Some(OwnedJob {
            value: build,
            permit: returned.permit,
        });
        self.complete = complete;
        Ok(complete)
    }

    /// Registers the hold in one source-control operation. A cancellation or
    /// deadline during registration never removes a possibly durable hold.
    pub async fn finish(mut self) -> Result<HeldBootstrapSourceJob, Refusal> {
        if !self.complete {
            return Err(unavailable());
        }
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let owner = run_step(owner, self.deadline, RedbBootstrapSourceBuild::finish).await?;
        Ok(HeldBootstrapSourceJob {
            owner: Some(owner),
            deadline: self.deadline,
        })
    }
}

/// Exact held artifact retaining the same job capacity and original deadline.
/// Drop releases local resources; durable attachment or audited abort is still
/// required to remove the source hold. Current authorization is caller-owned.
pub struct HeldBootstrapSourceJob {
    owner: Option<OwnedJob<RedbHeldBootstrapSource>>,
    deadline: Instant,
}

impl HeldBootstrapSourceJob {
    /// Reads the fixed manifest while this job's deadline remains valid.
    pub async fn manifest(&mut self) -> Result<Manifest, Refusal> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let returned = run_step(owner, self.deadline, |source| {
            source.verify_private_identity()?;
            let manifest = source.manifest();
            Ok((source, manifest))
        })
        .await?;
        let (source, manifest) = returned.value;
        self.owner = Some(OwnedJob {
            value: source,
            permit: returned.permit,
        });
        Ok(manifest)
    }

    /// Reads one bounded page; a failed or cancelled read fuses this handle.
    pub async fn read_page(&mut self, ordinal: u32) -> Result<Page, Refusal> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let returned = run_step(owner, self.deadline, move |source| {
            let page = source.read_page(ordinal)?;
            Ok((source, page))
        })
        .await?;
        let (source, page) = returned.value;
        self.owner = Some(OwnedJob {
            value: source,
            permit: returned.permit,
        });
        Ok(page)
    }
}

async fn run_step<T: Send + 'static, U: Send + 'static>(
    owner: OwnedJob<T>,
    deadline: Instant,
    step: impl FnOnce(T) -> Result<U, Refusal> + Send + 'static,
) -> Result<OwnedJob<U>, Refusal> {
    if Instant::now() >= deadline {
        return Err(unavailable());
    }
    let task = tokio::task::spawn_blocking(move || {
        // Ownership, including capacity, follows the actual storage operation.
        // Dropping its async waiter cannot admit overlapping replacement work.
        if Instant::now() >= deadline {
            return Err(unavailable());
        }
        owner.map(step)
    });
    let result = tokio::time::timeout_at(deadline, task)
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())??;
    if Instant::now() >= deadline {
        return Err(unavailable());
    }
    Ok(result)
}

fn unavailable() -> Refusal {
    StorageError::new(StorageErrorKind::Unavailable, None).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct ObservedResource(Arc<AtomicBool>);
    impl Drop for ObservedResource {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    // req: REP-003, PERF-007
    async fn cancelled_bootstrap_step_retains_capacity_until_its_resource_is_dropped() {
        let capacity = Arc::new(Semaphore::new(1));
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = OwnedJob {
            value: ObservedResource(Arc::clone(&dropped)),
            permit: Arc::clone(&capacity).try_acquire_owned().unwrap(),
        };
        let (started, observing) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let waiter = tokio::spawn(run_step(owner, Instant::now() + LIFETIME, move |value| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            Ok(value)
        }));
        observing.await.unwrap();
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(Arc::clone(&capacity).try_acquire_owned().is_err());
        release.send(()).unwrap();
        let _permit = capacity.acquire_owned().await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    // req: REP-003
    async fn expired_bootstrap_step_releases_resources_without_starting_storage_work() {
        let capacity = Arc::new(Semaphore::new(1));
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = OwnedJob {
            value: ObservedResource(Arc::clone(&dropped)),
            permit: Arc::clone(&capacity).try_acquire_owned().unwrap(),
        };
        assert!(
            run_step(owner, Instant::now(), |_| -> Result<(), Refusal> {
                panic!("expired work must never start")
            })
            .await
            .is_err()
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test(start_paused = true)]
    // req: REP-003, PERF-007
    async fn bootstrap_deadline_retains_capacity_through_the_inflight_storage_step() {
        let capacity = Arc::new(Semaphore::new(1));
        let dropped = Arc::new(AtomicBool::new(false));
        let owner = OwnedJob {
            value: ObservedResource(Arc::clone(&dropped)),
            permit: Arc::clone(&capacity).try_acquire_owned().unwrap(),
        };
        let (started, observing) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);
        let waiter = tokio::spawn(run_step(owner, Instant::now() + LIFETIME, move |value| {
            started.send(()).unwrap();
            blocked.recv().unwrap();
            Ok(value)
        }));
        observing.await.unwrap();
        tokio::time::advance(LIFETIME).await;
        assert!(waiter.await.unwrap().is_err());
        assert!(!dropped.load(Ordering::SeqCst));
        assert!(Arc::clone(&capacity).try_acquire_owned().is_err());
        release.send(()).unwrap();
        let _permit = capacity.acquire_owned().await.unwrap();
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    // req: REP-003, REC-001, PERF-007
    async fn managed_bootstrap_jobs_reuse_held_ids_and_cleanup_only_after_attachment() {
        use riffdb_storage_api::{
            ReadableCapabilityDigestInventory, ReadableDigestKey,
            ReadableIdempotencyDigestInventory, StartupValidationInputs,
        };
        use riffdb_types::{DigestKeyId, Timestamp};
        let (_scope, path) =
            crate::real_storage_support::temporary_database_scope("managed-bootstrap-jobs");
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        let startup = crate::startup::open_redb_startup(
            &path,
            StartupValidationInputs::new(
                Timestamp::new(1000, 0).unwrap(),
                ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
                ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
            ),
            &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
        )
        .unwrap();
        let (_, _, _, _, _, ports) = startup.into_parts();
        let root = path.parent().unwrap().join("artifacts");
        let jobs = BootstrapSourceJobs::from_repository(ports.bootstrap_repository(&root).unwrap());
        let id = HoldId::new([0x59; 16]).unwrap();
        let mut build = jobs.begin_managed(id).await.unwrap();
        while !build.advance().await.unwrap() {}
        let mut held = build.finish().await.unwrap();
        let manifest = held.manifest().await.unwrap();
        assert!(
            jobs.attach(manifest, manifest.fence().history().tail())
                .await
                .is_err()
        );
        drop(held);
        // A lost initial response cannot move this ID to a later source fence.
        for resume in [false, true] {
            let mut build = if resume {
                jobs.resume_managed(id).await
            } else {
                jobs.begin_managed(id).await
            }
            .unwrap();
            while !build.advance().await.unwrap() {}
            let mut held = build.finish().await.unwrap();
            assert_eq!(held.manifest().await.unwrap(), manifest);
            assert!(held.read_page(1).await.is_ok());
        }
        let peer_path = path.parent().unwrap().join("peer-chosen");
        assert!(jobs.begin(peer_path.clone(), id).await.is_err());
        assert!(!peer_path.exists());
        for _ in 0..2 {
            jobs.attach(manifest, manifest.fence().history().tail())
                .await
                .unwrap();
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        }
        assert!(jobs.begin_managed(id).await.is_err());
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS);
    }

    #[tokio::test]
    // req: REP-003, REC-001
    async fn bootstrap_jobs_bound_real_source_files_and_reopen_the_exact_held_artifact() {
        use riffdb_storage_api::{
            ReadableCapabilityDigestInventory, ReadableDigestKey,
            ReadableIdempotencyDigestInventory, ReplicationBootstrapTranscriptV3,
            StartupValidationInputs,
        };
        use riffdb_types::{DigestKeyId, Timestamp};
        let (_scope, path) =
            crate::real_storage_support::temporary_database_scope("bootstrap-source-jobs");
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        let inputs = StartupValidationInputs::new(
            Timestamp::new(1000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        );
        let startup = crate::startup::open_redb_startup(
            &path,
            inputs,
            &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
        )
        .unwrap();
        let (_, _, _, _, _, ports) = startup.into_parts();
        let jobs = BootstrapSourceJobs::new(Arc::new(ports));
        let directory = path.parent().unwrap();
        let id = HoldId::new([0x58; 16]).unwrap();
        let artifact = directory.join("source");
        let mut build = jobs.begin(artifact.clone(), id).await.unwrap();
        let mut others = Vec::new();
        for index in 1..MAX_JOBS {
            others.push(
                jobs.begin(directory.join(format!("other-{index}")), id)
                    .await
                    .unwrap(),
            );
        }
        let refused = directory.join("over-capacity");
        assert!(jobs.begin(refused.clone(), id).await.is_err());
        assert!(!refused.exists(), "admission refuses before creating files");
        drop(others);
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS - 1);
        let mut steps = 0;
        while !build.advance().await.unwrap() {
            steps += 1;
            assert!(steps < 200);
        }
        let mut held = build.finish().await.unwrap();
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS - 1);
        let manifest = held.manifest().await.unwrap();
        let mut transcript = ReplicationBootstrapTranscriptV3::new(manifest.fence());
        for ordinal in 1..=manifest.page_count() {
            transcript
                .observe(&held.read_page(ordinal).await.unwrap())
                .unwrap();
        }
        transcript.verify_manifest(manifest).unwrap();
        drop(held);
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS);
        let mut resumed = jobs.resume(artifact.clone(), id).await.unwrap();
        for ordinal in 1..=manifest.page_count() {
            assert_eq!(
                resumed.advance().await.unwrap(),
                ordinal == manifest.page_count()
            );
        }
        assert_eq!(
            resumed.finish().await.unwrap().manifest().await.unwrap(),
            manifest
        );
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS);
        // Source-side protocol custody only: actual receiver publication and
        // current authorization remain requirements of full RPC integration.
        let acknowledged = manifest.fence().history().tail();
        jobs.attach(manifest, acknowledged).await.unwrap();
        jobs.attach(manifest, acknowledged).await.unwrap();
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS);
        let mut obsolete = jobs.resume(artifact, id).await.unwrap();
        while !obsolete.advance().await.unwrap() {}
        assert!(obsolete.finish().await.is_err());
        assert_eq!(jobs.capacity.available_permits(), MAX_JOBS);
    }
}
