//! Exact-engine scoped history for the readonly predecessor maintenance phase.
use super::*;
use crate::backup::OwnedBackupReadOnlyV1;
use crate::engine_owner::EngineOwner;
use crate::engine_owner::MaintenanceWeakBackingControlV1;
use riffdb_types::{
    OwnedCapacitySharedV1, OwnedCapacityWeakV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

type ReadEngine = EngineOwner<redb::ReadOnlyDatabase>;
struct ImmutableScopeAffinityV1 {
    engine: OwnedCapacityWeakV1<ReadEngine, MaintenanceWeakBackingControlV1>,
    resolver: OwnedCapacityWeakV1<ImmutableResolverV1, MaintenanceWeakBackingControlV1>,
}
// One owning shell around the SAME existing HistoryResolver semantic value.
// Its admission covers newly constructed path/media/interval/control storage.
// It contains no engine back-reference and never registers ambient resolution.
pub(in crate::history_stub) struct ImmutableResolverV1 {
    value: HistoryResolver,
    _control: crate::locator_memory::LocatorCharge,
}
impl std::ops::Deref for ImmutableResolverV1 {
    type Target = HistoryResolver;
    fn deref(&self) -> &HistoryResolver {
        &self.value
    }
}
#[repr(C)]
struct ResolverAllocationLayoutV1 {
    counts: [usize; 2],
    value: ImmutableResolverV1,
}
fn immutable_scope_bytes(owner: &OwnedBackupReadOnlyV1) -> Result<usize, StorageError> {
    let resolver = owned_allocation_capacity_bound_v1(
        size_of::<ResolverAllocationLayoutV1>(),
        align_of::<ResolverAllocationLayoutV1>(),
    )
    .map_err(|_| unavailable())?;
    let path = owned_allocation_capacity_bound_v1(
        owner.history_directory().as_os_str().len(),
        align_of::<u8>(),
    )
    .map_err(|_| unavailable())?;
    let interval = OwnedBackupReadOnlyV1::history_interval_allocation_bytes()?;
    // Price actual Option/enumeration slots too; a generic weak-header count
    // cannot assume their discriminants occupy a spare pointer niche.
    let headers = size_of::<ImmutableBackupReadsV1<'static>>()
        .checked_add(size_of::<ScopedPrivateReads>())
        .and_then(|bytes| {
            bytes.checked_add(size_of::<OwnedCapacitySharedV1<ImmutableResolverV1>>())
        })
        .and_then(|bytes| bytes.checked_add(size_of::<Option<PrivateResolverOwnerV1>>()))
        .and_then(|bytes| bytes.checked_add(size_of::<ImmutableScopeAffinityV1>()))
        .and_then(|bytes| bytes.checked_add(size_of::<Option<ImmutableScopeAffinityV1>>()))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(unavailable)?;
    resolver
        .checked_add(path)
        .and_then(|bytes| bytes.checked_add(interval))
        .and_then(|bytes| bytes.checked_add(headers))
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(unavailable)
}
thread_local! {
    static IMMUTABLE_BACKUP_ENGINE: RefCell<Option<ImmutableScopeAffinityV1>> = const { RefCell::new(None) };
}

/// Never leaves its actual validation thread. Nested reads retain the same
/// actual engine; a durable database identity cannot substitute for affinity.
pub(crate) struct ImmutableBackupReadsV1<'owner> {
    scope: Option<ScopedPrivateReads>,
    resolver: OwnedCapacitySharedV1<ImmutableResolverV1>,
    owner: &'owner OwnedBackupReadOnlyV1,
    finished: bool,
}

pub(crate) fn begin_immutable_backup_reads(
    owner: &OwnedBackupReadOnlyV1,
) -> Result<ImmutableBackupReadsV1<'_>, StorageError> {
    owner.verify()?;
    if let Some(resolver) = private_resolver() {
        if !same_engine(owner) {
            return Err(unavailable());
        }
        return Ok(ImmutableBackupReadsV1 {
            scope: None,
            resolver: resolver.immutable().ok_or_else(unavailable)?.clone(),
            owner,
            finished: false,
        });
    }
    if IMMUTABLE_BACKUP_ENGINE.with(|slot| slot.borrow().is_some()) {
        return Err(unavailable());
    }
    // Measure borrowed immutable inputs and admit BEFORE Box, path, media Arc
    // and resolver Arc construction. This is actual-engine Maintenance work.
    let graph_control = owner
        .engine()
        .admit_maintenance_decode(immutable_scope_bytes(owner)?)?;
    let database_id = owner.history_database_id()?;
    let interval = owner.begin_history_interval()?;
    let resolver = OwnedCapacitySharedV1::new(ImmutableResolverV1 {
        value: HistoryResolver {
            database_id,
            location: ExtentLocation::Backup(owner.history_directory().to_path_buf()),
            media: interval.media(),
            pending: Mutex::new(PendingFrames::default()),
        },
        _control: graph_control,
    });
    // Construct the cleanup owner BEFORE any subsequent fallible admission.
    // On error its Box/media interval drops while resolver's graph charge lives.
    let scope = ScopedPrivateReads {
        resolver: PrivateResolverOwnerV1::Immutable(resolver.clone()),
        owner: Some(interval),
        finished: false,
        _thread: PhantomData,
    };
    // Both genuine weak relationships are admitted before either TLS slot is
    // published. No fallible construction follows PRIVATE_RESOLVER publication.
    let affinity = ImmutableScopeAffinityV1 {
        engine: owner
            .engine()
            .admit_maintenance_weak_backing(owner.engine())?,
        resolver: owner.engine().admit_maintenance_weak_backing(&resolver)?,
    };
    PRIVATE_RESOLVER.with(|slot| {
        let mut current = slot.borrow_mut();
        if current.is_some() {
            return Err(unavailable());
        }
        *current = Some(PrivateResolverOwnerV1::Immutable(resolver.clone()));
        Ok(())
    })?;
    IMMUTABLE_BACKUP_ENGINE.with(|slot| *slot.borrow_mut() = Some(affinity));
    Ok(ImmutableBackupReadsV1 {
        scope: Some(scope),
        resolver,
        owner,
        finished: false,
    })
}

fn same_engine(owner: &OwnedBackupReadOnlyV1) -> bool {
    IMMUTABLE_BACKUP_ENGINE.with(|slot| {
        slot.borrow().as_ref().is_some_and(|affinity| {
            affinity.engine.same_allocation(owner.engine())
                && PRIVATE_RESOLVER.with(|slot| {
                    slot.borrow()
                        .as_ref()
                        .and_then(PrivateResolverOwnerV1::immutable)
                        .is_some_and(|current| affinity.resolver.same_allocation(current))
                })
        })
    })
}
fn clear_owned_affinity(
    owner: &OwnedBackupReadOnlyV1,
    resolver: &OwnedCapacitySharedV1<ImmutableResolverV1>,
) {
    IMMUTABLE_BACKUP_ENGINE.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.as_ref().is_some_and(|affinity| {
            affinity.engine.same_allocation(owner.engine())
                && affinity.resolver.same_allocation(resolver)
        }) {
            *slot = None;
        }
    });
}

impl ImmutableBackupReadsV1<'_> {
    pub(crate) fn finish(&mut self) -> Result<(), StorageError> {
        if self.finished
            || !same_engine(self.owner)
            || !PRIVATE_RESOLVER.with(|slot| {
                slot.borrow().as_ref().is_some_and(|current| {
                    current
                        .immutable()
                        .is_some_and(|current| current.same_allocation(&self.resolver))
                })
            })
        {
            return Err(unavailable());
        }
        self.owner.verify()?;
        if let Some(scope) = &mut self.scope {
            scope.finish()?;
            clear_owned_affinity(self.owner, &self.resolver);
        }
        self.finished = true;
        Ok(())
    }
}
impl Drop for ImmutableBackupReadsV1<'_> {
    fn drop(&mut self) {
        if self.scope.is_some() {
            clear_owned_affinity(self.owner, &self.resolver);
            // The existing scope destructor clears only its exact resolver.
        } else if !self.finished {
            self.owner.invalidate();
        }
    }
}
