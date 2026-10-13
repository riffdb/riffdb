//! ADR-0299 D9: move the actual verified publication owner into startup.
//! This bounded slot grants no engine, semantic proof or application admission.
use super::codec::publication_intent::{PublicationIntent, PublicationPhase};
use super::path_guard::PinnedDirectory;
use super::staged::StagedDirectoryCleanup;
use super::store::RedbMaintenanceStorage;
use super::store::{NativePublicationStartup, NativeStartupOwner};
use crate::backup::publication_artifacts::OwnedPublicationArtifacts;
use crate::error::storage_error;
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::OfflineMaintenanceOperationId;
use std::ffi::OsString;
use std::sync::{Arc, Mutex};

pub(super) struct RetainedPublication {
    pub(super) intent: PublicationIntent,
    artifacts: OwnedPublicationArtifacts,
    stage: StagedDirectoryCleanup,
    backup: PinnedDirectory,
    target: Arc<PinnedDirectory>,
    names: [OsString; 3],
}

impl RetainedPublication {
    pub(super) fn new(
        intent: PublicationIntent,
        artifacts: OwnedPublicationArtifacts,
        stage: StagedDirectoryCleanup,
        backup: PinnedDirectory,
        target: Arc<PinnedDirectory>,
        names: [OsString; 3],
    ) -> Result<Self, StorageError> {
        if intent.phase != PublicationPhase::Published {
            return Err(corrupt());
        }
        let mut owner = Self {
            intent,
            artifacts,
            stage,
            backup,
            target,
            names,
        };
        owner.verify_continuity()?;
        Ok(owner)
    }

    pub(super) fn verify_continuity(&mut self) -> Result<(), StorageError> {
        self.stage.verify_empty()?;
        self.backup.verify()?;
        self.target.verify()?;
        for name in &self.names {
            let temporary = super::staged::target_temporary_name(name, self.intent.operation_id);
            if self.target.regular_file_length(&temporary)?.is_some() {
                return Err(corrupt());
            }
        }
        self.artifacts
            .verify_published_owner(&self.intent, &self.target, &self.names)?;
        self.stage.verify_empty()
    }
}

#[derive(Default)]
enum SlotState {
    #[default]
    Empty,
    Available(Box<RetainedPublication>),
    Consuming,
    Failed,
}

/// Exactly one operation-local original owner. The mutex protects only a
/// consuming state transition; no file I/O or engine operation holds its guard.
#[derive(Default)]
pub(super) struct RetainedPublicationSlot(Mutex<SlotState>);

impl RetainedPublicationSlot {
    pub(super) fn invalidate_retained(&self) {
        if let Ok(mut slot) = self.0.lock()
            && matches!(*slot, SlotState::Available(_) | SlotState::Consuming)
        {
            *slot = SlotState::Failed;
        }
    }

    pub(super) fn install(&self, mut owner: RetainedPublication) -> Result<(), StorageError> {
        owner.verify_continuity()?;
        let mut slot = self.0.lock().map_err(|_| corrupt())?;
        if !matches!(*slot, SlotState::Empty) {
            return Err(corrupt());
        }
        *slot = SlotState::Available(Box::new(owner));
        Ok(())
    }

    pub(super) fn take(
        &self,
        operation: OfflineMaintenanceOperationId,
    ) -> Result<Option<RetainedStartup<'_>>, StorageError> {
        let mut slot = self.0.lock().map_err(|_| corrupt())?;
        match std::mem::replace(&mut *slot, SlotState::Consuming) {
            SlotState::Empty => {
                *slot = SlotState::Empty;
                Ok(None)
            }
            SlotState::Available(owner) if owner.intent.operation_id == operation => {
                Ok(Some(RetainedStartup {
                    owner: Some(owner),
                    slot: self,
                    completed: false,
                }))
            }
            SlotState::Consuming => {
                *slot = SlotState::Consuming;
                Err(storage_error(StorageErrorKind::Unavailable))
            }
            SlotState::Available(_) | SlotState::Failed => {
                *slot = SlotState::Failed;
                Err(corrupt())
            }
        }
    }
}

pub(super) struct RetainedStartup<'a> {
    owner: Option<Box<RetainedPublication>>,
    slot: &'a RetainedPublicationSlot,
    completed: bool,
}

impl RetainedStartup<'_> {
    pub(super) fn consume(
        mut self,
        storage: &RedbMaintenanceStorage,
    ) -> Result<NativePublicationStartup, StorageError> {
        let owner = self.owner.take().ok_or_else(corrupt)?;
        let startup = NativeStartupOwner::Original(owner).into_startup_consumed(storage)?;
        {
            let mut slot = self.slot.0.lock().map_err(|_| corrupt())?;
            if !matches!(*slot, SlotState::Consuming) {
                return Err(corrupt());
            }
            *slot = SlotState::Empty;
        }
        self.completed = true;
        Ok(startup)
    }
}

impl Drop for RetainedStartup<'_> {
    fn drop(&mut self) {
        if !self.completed
            && let Ok(mut slot) = self.slot.0.lock()
        {
            *slot = SlotState::Failed;
        }
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
