//! The scoped resolver is never registered for ambient same-database lookup.
//! Constructors below require actual native preparation/validation custody.
use super::*;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;

thread_local! {
    static PRIVATE_RESOLVER: RefCell<Option<Arc<HistoryResolver>>> = const { RefCell::new(None) };
}

pub(super) fn private_resolver() -> Option<Arc<HistoryResolver>> {
    PRIVATE_RESOLVER.with(|slot| slot.borrow().clone())
}

struct ScopedPrivateReads {
    resolver: Arc<HistoryResolver>,
    owner: Box<dyn crate::media::OwnedRestoreReads>,
    finished: bool,
    _thread: PhantomData<Rc<()>>,
}

impl ScopedPrivateReads {
    fn begin(resolver: &HistoryResolver) -> Result<Option<Self>, StorageError> {
        let Some(owner) = resolver
            .media
            .begin_owned_restore_reads()
            .map_err(|_| unavailable())?
        else {
            return Ok(None);
        };
        let resolver = Arc::new(HistoryResolver {
            database_id: resolver.database_id,
            location: resolver.location.clone(),
            media: owner.media(),
            pending: Mutex::new(PendingFrames::default()),
        });
        PRIVATE_RESOLVER.with(|slot| {
            let mut current = slot.borrow_mut();
            if current.is_some() {
                return Err(unavailable());
            }
            *current = Some(Arc::clone(&resolver));
            Ok(())
        })?;
        Ok(Some(Self {
            resolver,
            owner,
            finished: false,
            _thread: PhantomData,
        }))
    }

    fn finish(&mut self) -> Result<(), StorageError> {
        if self.finished
            || !PRIVATE_RESOLVER.with(|slot| {
                slot.borrow()
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &self.resolver))
            })
        {
            return Err(unavailable());
        }
        self.owner.finish().map_err(|_| unavailable())?;
        PRIVATE_RESOLVER.with(|slot| *slot.borrow_mut() = None);
        self.finished = true;
        Ok(())
    }

    fn verify_finished(&self) -> Result<(), StorageError> {
        if !self.finished {
            return Err(unavailable());
        }
        self.owner.verify_finished().map_err(|_| unavailable())
    }
}

impl Drop for ScopedPrivateReads {
    fn drop(&mut self) {
        if !self.finished {
            PRIVATE_RESOLVER.with(|slot| {
                let mut current = slot.borrow_mut();
                if current
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &self.resolver))
                {
                    *current = None;
                }
            });
        }
        // Dropping the unfinished actual read owner permanently invalidates it.
    }
}

fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}

pub(super) fn validate_owned_history(
    read: &redb::ReadTransaction,
    binding: &crate::maintenance::RestoreIntegrityBinding,
    resolver: &HistoryResolver,
) -> Result<Option<riffdb_storage_api::ChangelogHistoryStateV3>, StorageError> {
    let generation = binding.validated_generation()?;
    if crate::store::read_identity_from_read_transaction(read)? != resolver.database_id {
        return Err(storage_error(StorageErrorKind::CorruptData));
    }
    let mut scope = ScopedPrivateReads::begin(resolver)?;
    let history = crate::changelog_v3_roots::validate_retained_history(read)?;
    generation.verify(binding)?;
    if let Some(scope) = &mut scope {
        scope.finish()?;
    }
    generation.verify(binding)?;
    // Only now can the complete validator return its trusted running roots.
    Ok(history)
}

pub(super) struct HydrationRead<'a> {
    resolver: &'a HistoryResolver,
    custody: &'a crate::maintenance::RestoreCustody,
}

pub(super) struct HydrationBatch {
    native: crate::maintenance::NativeRestorePreparationGeneration,
    scope: Option<ScopedPrivateReads>,
}

impl<'a> HydrationRead<'a> {
    pub(super) fn new(
        resolver: &'a HistoryResolver,
        custody: &'a crate::maintenance::RestoreCustody,
    ) -> Self {
        Self { resolver, custody }
    }
    pub(super) fn begin_batch(&self) -> Result<HydrationBatch, StorageError> {
        let native = self.custody.preparation_read_generation()?;
        let scope = ScopedPrivateReads::begin(self.resolver)?;
        native.verify_read()?;
        Ok(HydrationBatch { native, scope })
    }
}

impl HydrationBatch {
    pub(super) fn finish_reading(&mut self) -> Result<(), StorageError> {
        self.native.verify_read()?;
        if let Some(scope) = &mut self.scope {
            scope.finish()?;
        }
        self.native.verify_read()
    }
    pub(super) fn verify_before_commit(&self) -> Result<(), StorageError> {
        #[cfg(test)]
        BEFORE_HYDRATION_COMMIT.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
        self.native.verify_preparation_owner()?;
        if let Some(scope) = &self.scope {
            scope.verify_finished()?;
        }
        self.native.verify_preparation_owner()
    }
}

#[cfg(test)]
thread_local! {
    static BEFORE_HYDRATION_COMMIT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}
#[cfg(test)]
pub(super) fn set_hydration_commit_hook(hook: Box<dyn FnOnce()>) {
    BEFORE_HYDRATION_COMMIT.with(|slot| *slot.borrow_mut() = Some(hook));
}
#[cfg(test)]
pub(super) fn private_read_probe<T>(
    resolver: &HistoryResolver,
    read: impl FnOnce() -> Result<T, StorageError>,
) -> Result<T, StorageError> {
    let mut scope = ScopedPrivateReads::begin(resolver)?.ok_or_else(unavailable)?;
    let candidate = read()?;
    scope.finish()?;
    Ok(candidate)
}
