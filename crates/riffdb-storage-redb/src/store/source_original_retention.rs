//! Crate-private Original-prefix retention custody only. This is not a verified
//! projection plan, checkpoint source, read cursor or publication capability.
//! It keeps no redb read transaction while unrelated commands advance.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkReservationV1;
#[path = "source_original_retention/source_original_metadata.rs"]
mod source_original_metadata;
use riffdb_storage_api::proto_codec::ProjectionSourceActiveCatalogDecodeAdmissionV1;
pub(super) use source_original_metadata::OriginalCapturedRead;
#[path = "source_original_retention/source_logical_read.rs"]
mod source_logical_read;
pub(crate) use source_logical_read::OriginalLogicalRead;

pub(crate) struct OriginalPrefixRetentionLease {
    // Preserve the actual originating engine and its one source-work ledger.
    _engine: riffdb_types::OwnedCapacitySharedV1<EngineOwner<Database>>,
    pin: riffdb_types::OwnedCapacitySharedV1<crate::source_prefix_pin::SourcePrefixPin>,
    captured_history: riffdb_storage_api::ChangelogHistoryStateV3,
    active_catalog: Option<
        riffdb_types::ProjectionSourceWorkAllocationV1<riffdb_storage_api::ActiveCatalogPointerV1>,
    >,
    native_origin: Option<riffdb_storage_api::NativeCatalogOriginReferenceV1>,
    tentative_source_locator: Option<riffdb_types::ProjectionSourceLocatorV1>,
    _worker: riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1,
    // Header allowance outlives every graph, pin, engine and worker destructor.
    _control: ProjectionSourceWorkReservationV1,
}
impl OriginalPrefixRetentionLease {
    #[cfg(test)]
    pub(crate) fn retained_pin_control_bytes_for_test(&self) -> u64 {
        self.pin.control_charge_bytes_for_test()
    }
    pub(crate) fn originating_budget(&self) -> &riffdb_types::ProjectionSourceWorkingBudgetV1 {
        self._engine.source_working_budget()
    }
    pub(crate) fn captured_history(&self) -> riffdb_storage_api::ChangelogHistoryStateV3 {
        self.captured_history
    }
    // A checked stored Next expectation only, never allocation authority.
    pub(crate) fn tentative_source_locator(
        &self,
    ) -> Option<riffdb_types::ProjectionSourceLocatorV1> {
        self.tentative_source_locator
    }
    pub(crate) fn captured_active_catalog_pointer(
        &self,
    ) -> Option<&riffdb_storage_api::ActiveCatalogPointerV1> {
        self.active_catalog.as_deref()
    }
    pub(crate) fn captured_native_origin(
        &self,
    ) -> Option<riffdb_storage_api::NativeCatalogOriginReferenceV1> {
        self.native_origin
    }
    pub(crate) fn belongs_to_ports(&self, ports: &RedbOperationalPorts) -> bool {
        self._engine.same_allocation(&ports.shared.database)
    }

    pub(crate) fn captured_through(&self) -> CommitSequence {
        self.pin.captured_through()
    }
    // Mint only from this genuine closed capture. The fixed native request
    // validates session/stage/engine affinity before requesting this owner.
    // Shared-owner cloning retains the existing registration; it never
    // registers a second worker/pin or recaptures a later root.
    pub(crate) fn retain_original_source_inputs(
        &self,
    ) -> Result<RetainedOriginalSourceInputs, StorageError> {
        let locator = self
            .tentative_source_locator
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let control = self
            .originating_budget()
            .reserve(std::mem::size_of::<RetainedOriginalSourceInputs>() as u64)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Ok(RetainedOriginalSourceInputs {
            pin: self.pin.clone(),
            captured_history: self.captured_history,
            captured_locator: locator,
            _control: control,
        })
    }
}

// Compact input obligation only. No plan/schema/descriptor graph or EngineOwner
// back reference can be retained by the engine-owned usage slot through it.
// All original application/receipt/extent floors stay registered until its
// final physical pin reference dies; a numerical census cannot release it.
pub(crate) struct RetainedOriginalSourceInputs {
    pin: riffdb_types::OwnedCapacitySharedV1<crate::source_prefix_pin::SourcePrefixPin>,
    captured_history: riffdb_storage_api::ChangelogHistoryStateV3,
    captured_locator: riffdb_types::ProjectionSourceLocatorV1,
    // Last: actual pin/control ownership dies before this Source64 admission.
    _control: ProjectionSourceWorkReservationV1,
}
impl Drop for RetainedOriginalSourceInputs {
    fn drop(&mut self) {}
}
impl RetainedOriginalSourceInputs {
    pub(crate) fn captured_history(&self) -> riffdb_storage_api::ChangelogHistoryStateV3 {
        self.captured_history
    }
    pub(crate) fn captured_locator(&self) -> riffdb_types::ProjectionSourceLocatorV1 {
        self.captured_locator
    }
    pub(crate) fn matches_owner(
        &self,
        owner: &crate::engine_owner::EngineOwner<redb::Database>,
    ) -> bool {
        self.pin.belongs_to_owner(owner.source_prefix_pins())
    }
}

// No externally callable raw-worker constructor exists. The ordinary path
// acquires its own claim; only the sealed reconstruction can transfer a claim.
enum OriginalCaptureClaim<'ports> {
    Direct(riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1),
    Reconstructed(crate::source_usage_domain::SourceUsageReconstruction<'ports>),
}

impl RedbOperationalPorts {
    pub(crate) fn capture_original_prefix_retention(
        &self,
    ) -> Result<OriginalPrefixRetentionLease, StorageError> {
        if !self.shared.is_source_mode() || self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let worker = self
            .shared
            .database
            .source_working_budget()
            .try_acquire_worker()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        self.capture_original_prefix_with_claim(OriginalCaptureClaim::Direct(worker))
    }

    // The census handoff is a numerical reconstruction, never source closure.
    // Resolve its existing claim only under the same actual Source lease/read
    // admission that observes the pinned original history below.
    pub(crate) fn capture_original_prefix_from_usage(
        &self,
        reconstruction: crate::source_usage_domain::SourceUsageReconstruction<'_>,
    ) -> Result<OriginalPrefixRetentionLease, StorageError> {
        self.capture_original_prefix_with_claim(OriginalCaptureClaim::Reconstructed(reconstruction))
    }

    fn capture_original_prefix_with_claim(
        &self,
        claim: OriginalCaptureClaim<'_>,
    ) -> Result<OriginalPrefixRetentionLease, StorageError> {
        if !self.shared.is_source_mode() || self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        // The returned lease outlives temporary capture work. Reserve its own
        // exact inline header before constructing captured graphs on this ledger.
        let control_bytes = u64::try_from(std::mem::size_of::<OriginalPrefixRetentionLease>())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let control = self
            .shared
            .database
            .source_working_budget()
            .reserve(control_bytes)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        // Release old TLS capacity and admit fixed metadata work off-gate.
        let context = OriginalSourceReadContext::admit(self)?;
        // A bounded off-gate hint chooses only numerical admission. It never
        // chooses the eventual source catalog or supplies captured authority.
        let hint = context.begin_native_base()?;
        let mut hint_read = hint.logical()?;
        let hint_catalog = hint_read.history_position()?.lineage().catalog_digest();
        drop(hint_read);
        drop(hint);
        let mut pointer_admission =
            if hint_catalog == riffdb_storage_api::AuthoritativeStateCatalogV3.digest() {
                Some(
                    ProjectionSourceActiveCatalogDecodeAdmissionV1::new(context.budget())
                        .map_err(source_original_metadata::source_record_error)?,
                )
            } else {
                None
            };

        // Use the existing bounded FIFO source lane. Retirement cancels queued
        // capture admission; no ordinary-writer gate or independent drain owner.
        let _lease = self
            .shared
            .mutation_gate
            .acquire_source()?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        if !self.shared.is_source_mode() || self.shared.write_fenced.load(Ordering::Acquire) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let worker = match claim {
            OriginalCaptureClaim::Direct(worker) => worker,
            OriginalCaptureClaim::Reconstructed(reconstruction) => {
                reconstruction.into_worker_for_original_capture(self, &_lease)?
            }
        };
        // Admit fixed scalar/root work before copied metadata or decoding.
        // Variable frame/fold work has its separate checked Source64 allowance.
        // No full receipt hash, frame I/O or mutation fold runs under this gate.
        let captured = context.begin_native_base()?;
        let mut read = captured.logical()?;
        if read.watermark()? != 0 {
            return Err(storage_error(StorageErrorKind::HistoryPruned));
        }
        let history = read.history_position()?;

        let catalog = history.lineage().catalog_digest();
        let v3 = catalog == riffdb_storage_api::AuthoritativeStateCatalogV3.digest();
        let tentative_source_locator = if v3 {
            // A hint racing a catalog change cannot authorize unadmitted work.
            if pointer_admission.is_none() {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
            Some(read.tentative_locator(history)?)
        } else if catalog == riffdb_storage_api::AuthoritativeStateCatalogV1.digest()
            || catalog == riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
        {
            None
        } else {
            return Err(storage_error(StorageErrorKind::Unavailable));
        };
        let pin = self
            .shared
            .database
            .source_prefix_pins()
            .try_register_captured(history, 0)?;
        // The conservative logical floors are installed before releasing the
        // gate. Full verification then uses this identical immutable read.
        drop(_lease);
        let verified = read.verified_history()?;
        if verified != history {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let (native_origin, active_catalog) = if v3 {
            let origin = read.checked_origin()?;
            let admission = pointer_admission
                .take()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
            let pointer = read.pointer(admission)?;
            if !pointer.belongs_to(context.budget()) {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            (Some(origin), Some(pointer))
        } else {
            (None, None)
        };
        drop(read);
        drop(captured);
        Ok(OriginalPrefixRetentionLease {
            _engine: self.shared.database.clone(),
            pin,
            captured_history: history,
            tentative_source_locator,
            active_catalog,
            native_origin,
            _worker: worker,
            _control: control,
        })
    }
}

// Match the existing native caller-admitted scratch destruction rule. A live
// decoder borrow is an internal ownership violation: retain its finite charge
// rather than release physical capacity. There is no asynchronous/reentrant
// callback inside this closed synchronous capture.
pub(crate) struct SourceReadWork(Option<ProjectionSourceWorkReservationV1>);
impl SourceReadWork {
    pub(crate) fn admit(
        budget: &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1,
        bytes: u64,
    ) -> Result<Self, StorageError> {
        budget
            .reserve(bytes)
            .map(|work| Self(Some(work)))
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))
    }
}
impl Drop for SourceReadWork {
    fn drop(&mut self) {
        if !riffdb_proto::envelope::release_locator_decode_scratch_v1()
            && let Some(work) = self.0.take()
        {
            std::mem::forget(work);
        }
    }
}

/// Private synchronous read work, tied to the exact originating engine. The
/// fixed scalar/root allowance cannot stand in for variable frame/fold work.
pub(crate) struct OriginalSourceReadContext<'ports> {
    ports: &'ports RedbOperationalPorts,
    _scratch: SourceReadWork,
}
impl<'ports> OriginalSourceReadContext<'ports> {
    pub(in crate::store) fn ports(&self) -> &RedbOperationalPorts {
        self.ports
    }
    pub(crate) fn admit(ports: &'ports RedbOperationalPorts) -> Result<Self, StorageError> {
        // Prior ordinary reads may retain a larger TLS workspace. Do not adopt
        // that unmeasured capacity under the fixed scalar/root allowance.
        if !riffdb_proto::envelope::release_locator_decode_scratch_v1() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let scratch =
            SourceReadWork::admit(ports.shared.database.source_working_budget(), 512 * 1024)?;
        Ok(Self {
            ports,
            _scratch: scratch,
        })
    }
    pub(crate) fn budget(&self) -> &riffdb_types::ProjectionSourceWorkingBudgetV1 {
        self.ports.shared.database.source_working_budget()
    }
    pub(crate) fn admit_work(&self, bytes: u64) -> Result<SourceReadWork, StorageError> {
        SourceReadWork::admit(self.budget(), bytes)
    }
    pub(crate) fn validate_terminal(
        &self,
        bytes: &[u8],
        history: riffdb_storage_api::ChangelogHistoryStateV3,
    ) -> Result<(), StorageError> {
        crate::history_stub::validate_original_source_terminal_v1(
            bytes,
            history,
            self.budget(),
            self.ports
                .shared
                .history_resolver
                .get()
                .map(|resolver| &**resolver),
        )
    }
}

#[cfg(test)]
type OriginalFoldPause = (
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
#[cfg(test)]
std::thread_local! {
    static ORIGINAL_FOLD_PAUSE:std::cell::RefCell<Option<OriginalFoldPause>>=
        const {std::cell::RefCell::new(None)};
}
#[cfg(test)]
pub(crate) fn install_original_fold_pause_for_test(
    entered: std::sync::mpsc::SyncSender<()>,
    release: std::sync::mpsc::Receiver<()>,
) -> bool {
    ORIGINAL_FOLD_PAUSE.with(|slot| slot.replace(Some((entered, release))).is_none())
}
#[cfg(test)]
pub(crate) fn pause_original_fold_for_test() -> Result<(), StorageError> {
    let pause = ORIGINAL_FOLD_PAUSE.with(|slot| slot.take());
    if let Some((entered, release)) = pause {
        entered
            .send(())
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        release
            .recv()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    }
    Ok(())
}

#[cfg(test)]
impl RedbOperationalPorts {
    pub(crate) fn original_proof_writer_gate_available_for_test(
        &self,
    ) -> Result<bool, StorageError> {
        let writer = self.shared.mutation_gate.try_acquire()?;
        let available = writer.is_some();
        drop(writer);
        Ok(available)
    }
}

impl OriginalSourceReadContext<'_> {
    pub(super) fn decode_command_at(
        &self,
        bytes: &[u8],
        physical: CommitSequence,
    ) -> Result<riffdb_storage_api::ProjectionSourceCommandReadV1, StorageError> {
        crate::history_stub::decode_original_source_command_v1(
            bytes,
            physical,
            self.budget(),
            self.ports
                .shared
                .history_resolver
                .get()
                .map(|resolver| &**resolver),
        )
    }
}
#[cfg(test)]
impl RedbOperationalPorts {
    pub(crate) fn checkpoint_original_cursor_fixture_for_test(&self) -> Result<(), StorageError> {
        let gate = self.shared.mutation_gate.acquire()?;
        self.shared.publish_pending_journal_prefix()?;
        // Direct materialization only advances the active anchor. Exercise
        // the real asynchronous rotation/installation/sealing protocol instead.
        self.shared.start_async_checkpoint_for_direct_write()?;
        let completion = self
            .shared
            .journal_checkpoint
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .as_ref()
            .and_then(|checkpoint| checkpoint.completion.clone())
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        drop(gate);
        // Retain only its existing completion capability while the actual
        // materializer runs; neither writer gate nor state mutex is held.
        completion
            .poll(true)
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))??;
        let gate = self.shared.mutation_gate.acquire()?;
        self.shared.poll_async_checkpoint_locked(false)?;
        drop(gate);
        self.shared.join_journal_sealer();
        Ok(())
    }
}
