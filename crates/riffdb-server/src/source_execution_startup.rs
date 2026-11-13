//! Concrete startup funding for the existing ordinary daemon runtime.
//! No Source body or cross-crate execution-origin token is exposed here.
use super::*;
use riffdb_storage_redb::SourceExecutionBootstrapV1;

const DATABASE_SLOTS: usize = 32;
static STARTUP_ESCROW: Mutex<Option<StartupRetirement>> = Mutex::new(None);

// Inline fixed storage; no Vec, map, path clone or second numerical pool.
// This owner is consumed by the actual configured-storage startup loop.
pub(crate) struct ConfiguredSourceSeeds {
    seeds: [Option<SourceExecutionBootstrapV1>; DATABASE_SLOTS],
    count: usize,
}
impl ConfiguredSourceSeeds {
    pub(crate) fn take(&mut self, index: usize) -> Result<SourceExecutionBootstrapV1, Refusal> {
        if index >= self.count {
            return Err(Refusal::Invariant);
        }
        self.seeds[index].take().ok_or(Refusal::Invariant)
    }
}

// EACH configured database pays the complete shared physical allocation before
// it exists. A smaller database never borrows another database's Source ledger.
// The actual callback/registry may outlive every configured Engine/storage.
struct SharedStartupAdmission {
    #[expect(
        dead_code,
        reason = "RAII custody retains source work reservations until Drop"
    )]
    registry: [Option<ProjectionSourceWorkReservationV1>; DATABASE_SLOTS],
    callback: [Option<ProjectionSourceWorkReservationV1>; DATABASE_SLOTS],
}
pub(crate) struct SourceExecutionStartup {
    pub(super) seeds: ConfiguredSourceSeeds,
    registry: Option<Arc<Registry>>,
    admissions: Option<SharedStartupAdmission>,
}
struct StartupRetirement {
    // Retain the actual registry (and any future task slot), not a detached ID.
    #[expect(
        dead_code,
        reason = "RAII custody retains startup registry in escrow until retirement"
    )]
    registry: Option<Arc<Registry>>,
    #[expect(
        dead_code,
        reason = "RAII custody retains startup work reservations in escrow until retirement"
    )]
    admissions: Option<SharedStartupAdmission>,
}
impl SourceExecutionStartup {
    pub(crate) fn prepare(config: &crate::config::ServerConfig) -> Result<Self, Refusal> {
        require_task_layout()?;
        let count = config.databases().len();
        if count == 0 || count > DATABASE_SLOTS {
            return Err(Refusal::UnsupportedWorkers);
        }
        let mut seeds: [Option<SourceExecutionBootstrapV1>; DATABASE_SLOTS] =
            std::array::from_fn(|_| None);
        let mut registry_work = std::array::from_fn(|_| None);
        // Construct all fixed-policy ledgers, then admit the whole registry on
        // every one BEFORE allocating the shared registry Arc.
        let registry_bytes = arc_allocation::<Registry>()?;
        for index in 0..count {
            let seed = SourceExecutionBootstrapV1::try_new().map_err(|_| Refusal::Capacity)?;
            registry_work[index] = Some(
                seed.numerical_budget()
                    .reserve(registry_bytes)
                    .map_err(|_| Refusal::Capacity)?,
            );
            seeds[index] = Some(seed);
        }
        // The root's existing ordinary Rust Thread owner was created before
        // this bootstrap. It is not evidence of a Source-created pthread.
        let root = thread::current();
        let registry = Arc::new(Registry {
            root_id: root.id(),
            root,
            state: Mutex::new(RegistryState {
                workers: [None; ORDINARY_WORKERS],
                worker_count: 0,
                frozen: false,
                next_serial: 1,
                pending: None,
            }),
        });
        Ok(Self {
            seeds: ConfiguredSourceSeeds { seeds, count },
            registry: Some(registry),
            admissions: Some(SharedStartupAdmission {
                registry: registry_work,
                callback: std::array::from_fn(|_| None),
            }),
        })
    }
    pub(crate) fn build_existing_daemon_runtime(
        mut self,
    ) -> Result<(ConfiguredDaemonRuntime, ConfiguredSourceSeeds), Refusal> {
        // This replaces the daemon's ONE ordinary runtime construction; it is
        // never a per-job runtime or Source-only worker/thread bootstrap.
        if OWNER_LIVE
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(Refusal::Busy);
        }
        let registry = self.registry.as_ref().ok_or(Refusal::Invariant)?;
        let callback_registry = Arc::clone(registry);
        let callback = move || callback_registry.observe_actual_park();
        let callback_bytes = match callback_allocation(&callback) {
            Ok(bytes) => bytes,
            Err(error) => {
                OWNER_LIVE.store(false, Ordering::Release);
                return Err(error);
            }
        };
        // The closure itself is still inline. Builder's callback Arc has not
        // been allocated. Every fixed configured ledger pays it in full now.
        for index in 0..self.seeds.count {
            let budget = self.seeds.seeds[index]
                .as_ref()
                .ok_or(Refusal::Invariant)?
                .numerical_budget();
            match budget.reserve(callback_bytes) {
                Ok(work) => {
                    self.admissions.as_mut().ok_or(Refusal::Invariant)?.callback[index] = Some(work)
                }
                Err(_) => {
                    OWNER_LIVE.store(false, Ordering::Release);
                    return Err(Refusal::Capacity);
                }
            }
        }
        let mut builder = Builder::new_multi_thread();
        builder
            .worker_threads(ORDINARY_WORKERS)
            .enable_all()
            .on_thread_park(callback);
        let runtime = builder.build();
        drop(builder); // destroy the builder's own actual callback Arc alias
        let retirement = StartupRetirement {
            registry: self.registry.take(),
            admissions: self.admissions.take(),
        };
        match runtime {
            Ok(runtime) => {
                let seeds = ConfiguredSourceSeeds {
                    seeds: std::mem::replace(&mut self.seeds.seeds, std::array::from_fn(|_| None)),
                    count: self.seeds.count,
                };
                Ok((
                    ConfiguredDaemonRuntime {
                        runtime: Some(runtime),
                        retirement: Some(retirement),
                    },
                    seeds,
                ))
            }
            Err(_) => {
                // Builder error may have partially launched native threads.
                // Missing Runtime/Arc uniqueness does not prove their retirement.
                retain_startup(retirement);
                Err(Refusal::Capacity)
            }
        }
    }
}
impl Drop for SourceExecutionStartup {
    fn drop(&mut self) {
        // No worker existed yet unless build moved this exact owner out.
        // Destroy the physical shared shell before reservation arrays refund.
        drop(self.registry.take());
        drop(self.admissions.take());
    }
}

pub(crate) struct ConfiguredDaemonRuntime {
    runtime: Option<Runtime>,
    retirement: Option<StartupRetirement>,
}
#[expect(
    dead_code,
    reason = "Inactive daemon runtime scaffold pending ClosedSourceBody composition wiring"
)]
impl ConfiguredDaemonRuntime {
    // Ordinary daemon future only. This does not publish a Source callback or
    // claim an authenticated Source worker/stack capability.
    pub(crate) fn block_on<F: Future>(&self, future: F) -> Result<F::Output, Refusal> {
        self.runtime
            .as_ref()
            .ok_or(Refusal::Invariant)
            .map(|runtime| runtime.block_on(future))
    }
    pub(crate) fn shutdown_timeout(mut self, duration: std::time::Duration) {
        // Transfer charges before any runtime shutdown call, including its
        // possible panic: no unwinding field-drop can refund an opaque alias.
        if let Some(retirement) = self.retirement.take() {
            retain_startup(retirement);
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(duration);
        }
        // Existing bounded daemon shutdown does not prove full native join.
    }
}
impl Drop for ConfiguredDaemonRuntime {
    fn drop(&mut self) {
        // An arbitrary/panicking drop cannot refund before a native join witness.
        // Zero-timeout preserves the daemon's bounded failure shutdown property.
        if let Some(retirement) = self.retirement.take() {
            retain_startup(retirement);
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(std::time::Duration::ZERO);
        }
    }
}
fn retain_startup(retirement: StartupRetirement) {
    let mut escrow = lock(&STARTUP_ESCROW);
    if escrow.is_none() {
        *escrow = Some(retirement);
    } else {
        // OWNER_LIVE makes a second owner unreachable. Retain, never refund,
        // even if a future internal bug violates that sole-owner condition.
        std::mem::forget(retirement);
    }
    // OWNER_LIVE stays true. No replacement runtime/new startup ledger bypass.
}
