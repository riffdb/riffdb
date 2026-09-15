//! Async custody for one receiver construction. Capacity follows real storage
//! work through cancellation, including publication and final startup.
#[path = "replication_receiver_connection.rs"]
mod connection;
#[path = "replication_follower_receiver.rs"]
mod follower;
use super::{BootstrapProjectionRebuild, BootstrapRebuiltCandidate, check_cancel};
pub use connection::BootstrapReceiverConnection;
pub use follower::{
    FollowerReadSnapshots, FollowerReadView, FollowerReceiver, RunningFollowerReceiver,
};
use riffdb_storage_api::{
    MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES, ReplicationBootstrapManifestV1 as Manifest,
    ReplicationBootstrapProgressV1 as Progress, StartupValidationInputs, StorageError,
    StorageErrorKind,
};
use riffdb_storage_redb::{
    RedbBootstrapMaterializer, RedbBootstrapReceiverRepository, RedbBootstrapStage,
    RedbFollowerApplier,
};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

const LIFETIME: Duration = Duration::from_secs(15 * 60);

/// One shared receiver's construction capacity. Persistent scratch inventory is
/// separate: dropping a job releases locks but retains resumable private files.
pub struct BootstrapReceiverJobs {
    capacity: Arc<Semaphore>,
    repository: Option<RedbBootstrapReceiverRepository>,
}
impl Default for BootstrapReceiverJobs {
    fn default() -> Self {
        Self::new()
    }
}
impl BootstrapReceiverJobs {
    /// Waits until all cancelled construction/storage work has released custody.
    /// Call only after dropping the active job; this does not cancel its owner.
    pub async fn drain(&self) -> Result<(), StorageError> {
        let permit = self.capacity.acquire().await.map_err(|_| unavailable())?;
        drop(permit);
        Ok(())
    }

    /// Share this factory across all connections for one configured receiver.
    #[must_use]
    pub fn new() -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(1)),
            repository: None,
        }
    }

    /// Binds one receiver to a fixed persistent inventory. Jobs retain the root
    /// lock through actual blocking work, publication, startup and tail custody.
    #[must_use]
    pub fn from_repository(repository: RedbBootstrapReceiverRepository) -> Self {
        Self {
            capacity: Arc::new(Semaphore::new(1)),
            repository: Some(repository),
        }
    }

    /// Creates a new private transfer after admission; existing paths refuse.
    pub async fn begin(
        &self,
        path: PathBuf,
        manifest: Manifest,
    ) -> Result<BootstrapReceiverTransferJob, StorageError> {
        self.open(path, manifest, false).await
    }
    /// Reopens only the exact manifest's durable page boundary.
    pub async fn resume(
        &self,
        path: PathBuf,
        manifest: Manifest,
    ) -> Result<BootstrapReceiverTransferJob, StorageError> {
        self.open(path, manifest, true).await
    }
    async fn open(
        &self,
        path: PathBuf,
        manifest: Manifest,
        resume: bool,
    ) -> Result<BootstrapReceiverTransferJob, StorageError> {
        if self.repository.is_some() {
            return Err(unavailable());
        }
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| unavailable())?;
        let owner = Custody {
            value: (),
            repository: self.repository.clone(),
            permit,
            cancellation: Arc::new(AtomicBool::new(false)),
            deadline: Instant::now() + LIFETIME,
        };
        let owner = run_step(owner, move |(), _| {
            if resume {
                RedbBootstrapStage::open(&path, manifest)
            } else {
                RedbBootstrapStage::create(&path, manifest)
            }
        })
        .await?;
        Ok(BootstrapReceiverTransferJob { owner: Some(owner) })
    }
}

// Storage resources drop BEFORE the admission permit. A detached blocking task
// owns this whole value; losing its waiter cannot release construction capacity.
struct Custody<T> {
    value: T,
    repository: Option<RedbBootstrapReceiverRepository>,
    permit: OwnedSemaphorePermit,
    cancellation: Arc<AtomicBool>,
    deadline: Instant,
}
impl<T> Custody<T> {
    fn check(&self) -> Result<(), StorageError> {
        check_cancel(&self.cancellation)?;
        if Instant::now() >= self.deadline {
            return Err(unavailable());
        }
        Ok(())
    }
    fn map<U>(
        self,
        step: impl FnOnce(T, &Arc<AtomicBool>) -> Result<U, StorageError>,
    ) -> Result<Custody<U>, StorageError> {
        self.check()?;
        let value = step(self.value, &self.cancellation)?;
        let next = Custody {
            value,
            repository: self.repository,
            permit: self.permit,
            cancellation: self.cancellation,
            deadline: self.deadline,
        };
        next.check()?;
        Ok(next)
    }
}
impl<T, U> Custody<(T, U)> {
    fn separate(self) -> (Custody<T>, U) {
        let (value, output) = self.value;
        (
            Custody {
                value,
                repository: self.repository,
                permit: self.permit,
                cancellation: self.cancellation,
                deadline: self.deadline,
            },
            output,
        )
    }
}

/// Move-only transfer owner. Every append commits one bounded page plus progress;
/// a cancelled or failed operation fuses the handle and requires durable resume.
pub struct BootstrapReceiverTransferJob {
    owner: Option<Custody<RedbBootstrapStage>>,
}
impl BootstrapReceiverTransferJob {
    /// Last durable page boundary, never a source acknowledgement.
    pub fn progress(&self) -> Result<Progress, StorageError> {
        let owner = self.owner.as_ref().ok_or_else(unavailable)?;
        owner.check()?;
        Ok(owner.value.progress().clone())
    }
    /// Persists an exact next page or read-only retry of the last durable page.
    pub async fn append(&mut self, bytes: Vec<u8>) -> Result<Progress, StorageError> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        if bytes.is_empty() || bytes.len() > MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES {
            return Err(StorageError::new(StorageErrorKind::CorruptData, None));
        }
        let returned = run_step(owner, move |mut stage, _| {
            let progress = stage.append(&bytes)?;
            Ok((stage, progress))
        })
        .await?;
        let (owner, progress) = returned.separate();
        self.owner = Some(owner);
        Ok(progress)
    }
    /// Creates or resumes the fixed managed candidate after complete source EOF.
    /// Initial creation recovery never discards a published progress boundary.
    pub async fn materialize_managed(
        mut self,
        inputs: StartupValidationInputs,
    ) -> Result<BootstrapReceiverBuildJob, StorageError> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let repository = owner.repository.clone().ok_or_else(unavailable)?;
        let owner = run_step(owner, move |stage, cancellation| {
            repository
                .materialize_transfer(stage, cancellation)
                .map(|materializer| Build::Copy(Box::new(materializer)))
        })
        .await?;
        Ok(BootstrapReceiverBuildJob {
            owner: Some(owner),
            inputs,
        })
    }
    /// Transfers complete durable pages into a private physical candidate. Resume
    /// is explicit; an absent or conflicting construction is never overwritten.
    pub async fn materialize(
        mut self,
        path: PathBuf,
        resume: bool,
        inputs: StartupValidationInputs,
    ) -> Result<BootstrapReceiverBuildJob, StorageError> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        if owner.repository.is_some() {
            return Err(unavailable());
        }
        let owner = run_step(owner, move |stage, cancellation| {
            let input = stage.into_materialization_input()?;
            let materializer = if resume {
                RedbBootstrapMaterializer::open_cancellable(&path, input, cancellation)?
            } else {
                RedbBootstrapMaterializer::create(&path, input)?
            };
            Ok(Build::Copy(Box::new(materializer)))
        })
        .await?;
        Ok(BootstrapReceiverBuildJob {
            owner: Some(owner),
            inputs,
        })
    }
}

enum Build {
    Copy(Box<RedbBootstrapMaterializer>),
    Rebuild(Box<BootstrapProjectionRebuild>),
    Ready(Box<BootstrapRebuiltCandidate>),
}

/// Offline construction through complete semantic replay and full scrub. Neither
/// intermediate progress nor a completed scrub grants serving or acknowledgement.
pub struct BootstrapReceiverBuildJob {
    owner: Option<Custody<Build>>,
    inputs: StartupValidationInputs,
}
impl BootstrapReceiverBuildJob {
    /// Copies one page, advances one replay step, or performs a complete seal or
    /// validation pass. All filesystem/engine work runs outside the async runtime.
    /// Catalog/replay checks observe cancellation between bounded evidence reads.
    pub async fn advance(&mut self) -> Result<bool, StorageError> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let inputs = self.inputs.clone();
        let returned = run_step(owner, move |state, cancellation| {
            let state = match state {
                Build::Copy(mut materializer) => {
                    if materializer.copy_next_page()?.is_some() {
                        Build::Copy(materializer)
                    } else {
                        let candidate = materializer.finish_cancellable(cancellation)?;
                        Build::Rebuild(Box::new(BootstrapProjectionRebuild::new(
                            candidate,
                            inputs,
                            Arc::clone(cancellation),
                        )?))
                    }
                }
                Build::Rebuild(mut worker) => {
                    if worker.advance()? {
                        Build::Ready(Box::new(worker.finish()?))
                    } else {
                        Build::Rebuild(worker)
                    }
                }
                Build::Ready(candidate) => Build::Ready(candidate),
            };
            let complete = matches!(state, Build::Ready(_));
            Ok((state, complete))
        })
        .await?;
        let (owner, complete) = returned.separate();
        self.owner = Some(owner);
        Ok(complete)
    }
    /// Publishes only a fully rebuilt candidate, then runs production follower
    /// startup against that exact manifest. Cancellation may leave a durable
    /// replacement, but never returns an applier or sends an acknowledgement.
    /// The caller must persist its local acknowledgement before source attachment.
    pub async fn publish_and_activate(
        self,
        path: PathBuf,
    ) -> Result<RedbFollowerApplier, StorageError> {
        Ok(self.publish_owner(path).await?.value.0.applier)
    }

    async fn publish_owner(
        mut self,
        path: PathBuf,
    ) -> Result<Custody<(crate::startup::CheckedRedbFollowerStartup, Manifest)>, StorageError> {
        let owner = self.owner.take().ok_or_else(unavailable)?;
        let inputs = self.inputs;
        let returned = run_step(owner, move |state, cancellation| {
            let Build::Ready(candidate) = state else {
                return Err(StorageError::new(StorageErrorKind::CorruptData, None));
            };
            check_cancel(cancellation)?;
            let manifest = candidate.manifest();
            let published = candidate.publish(&path)?;
            check_cancel(cancellation)?;
            let applier = published.activate_cancellable(inputs, Arc::clone(cancellation))?;
            Ok((applier, manifest))
        })
        .await?;
        Ok(returned)
    }
}

struct CancelWaiter(Option<Arc<AtomicBool>>);
impl Drop for CancelWaiter {
    fn drop(&mut self) {
        if let Some(flag) = self.0.take() {
            flag.store(true, Ordering::Release);
        }
    }
}
async fn run_step<T: Send + 'static, U: Send + 'static>(
    owner: Custody<T>,
    step: impl FnOnce(T, &Arc<AtomicBool>) -> Result<U, StorageError> + Send + 'static,
) -> Result<Custody<U>, StorageError> {
    owner.check()?;
    let deadline = owner.deadline;
    let mut cancellation = CancelWaiter(Some(Arc::clone(&owner.cancellation)));
    let task = tokio::task::spawn_blocking(move || owner.map(step));
    let returned = tokio::time::timeout_at(deadline, task)
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())??;
    returned.check()?;
    cancellation.0 = None;
    Ok(returned)
}
fn unavailable() -> StorageError {
    StorageError::new(StorageErrorKind::Unavailable, None)
}

#[cfg(test)]
#[path = "replication_bootstrap_receiver_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "replication_receiver_connection_tests.rs"]
mod connection_tests;

#[cfg(test)]
#[path = "replication_receiver_repository_tests.rs"]
mod repository_tests;
