//! Owning-only numerical transition from a joined maintenance engine.
//! Draft for the exhaustive1990+closed1993 EngineOwner field composition.
//! No application/Original/catalog/cut authority is produced here.
#![allow(dead_code)]
use super::*;
use redb::{ReadOnlyDatabase, ReadOnlyReopenHandoff, ReadOnlyReopenResourcePlan};
use riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1;
use riffdb_types::{OwnedCapacitySharedV1, ProjectionSourceWorkReservationV1};

// Native objects FIRST; both pre-existing and transition controls LAST. In
// particular, a `?` never drops standalone control locals before the native DB.
struct RetiringReadOnlyEngineV1 {
    database: Option<ReadOnlyDatabase>,
    handoff: Option<ReadOnlyReopenHandoff>,
    native_origin_context: crate::native_source_fresh::OriginContextCellV1,
    provider: SharedPhysicalOwnershipCell,
    locator_build_domain: crate::locator_build_domain::LocatorBuildDomain,
    source_working_domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    worker: Option<ProjectionSourceWorkerOwnershipV1>,
    engine_control: LocatorCharge,
    transition_control: Option<LocatorCharge>,
    source_control: Option<ProjectionSourceWorkReservationV1>,
}

/// File-only native handoff and the MOVED original domain, not an engine/read.
/// Construction proves numerical retirement only. The cut phase must separately
/// retain its actual primary/artifact owner and original validated observations.
/// No raw engine/handoff/domain getter or Clone exists.
struct PreparedNativeSourceReopenV1 {
    handoff: Option<ReadOnlyReopenHandoff>,
    domain: crate::source_working_domain::SourceWorkingDomain,
    worker: ProjectionSourceWorkerOwnershipV1,
    // The handoff is still a maintenance result until native+descriptor peak
    // Source preadmission and the fixed whole-owner facade construction.
    maintenance_control: LocatorCharge,
    source_control: ProjectionSourceWorkReservationV1,
}

impl EngineOwner<ReadOnlyDatabase> {
    fn prepare_source_reopen(
        mut owner: OwnedCapacitySharedV1<Self>,
    ) -> Result<PreparedNativeSourceReopenV1, StorageError> {
        // Admit actual fixed firstparty headers before constructing the worker,
        // retirement container or prepared result. No new Source budget exists.
        let source_bytes = u64::try_from(std::mem::size_of::<PreparedNativeSourceReopenV1>())
            .map_err(|_| unavailable())?;
        let source_control = owner
            .source_working_budget()
            .reserve(source_bytes)
            .map_err(|_| unavailable())?;
        let worker = owner
            .source_working_budget()
            .try_acquire_worker()
            .map_err(|_| unavailable())?;
        let controls = std::mem::size_of::<RetiringReadOnlyEngineV1>()
            .checked_add(std::mem::size_of::<PreparedNativeSourceReopenV1>())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<ReadOnlyReopenResourcePlan>()))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PendingOpenedSourceReadV1>()))
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<OpenedPredecessorSourceReadV1>())
            })
            .ok_or_else(unavailable)?;
        let transition_control = owner.admit_maintenance_decode(controls)?;
        // Atomic checked extraction proves sole strong ownership of the value.
        // Closed weak backing may remain and must retain its independent charge;
        // the closed caller must first clear/join its scoped TLS weak owners.
        // Alias refusal destroys only this handle; a surviving actual owner
        // continues retaining its original native/cache/control graph.
        // The actual joined-maintenance caller has destroyed its resolver
        // scope/TLS. Independently reject any surviving strong OR weak alias
        // using the existing checked mutable-access operation before extraction.
        // No other strong handle can manufacture a new weak after this succeeds.
        if owner.get_mut().is_none() {
            return Err(unavailable());
        }
        let owner = owner.try_into_value().map_err(|_| unavailable())?;
        let EngineOwner {
            database,
            cache_bytes: _,
            native_origin_context,
            _control,
            provider,
            locator_build_domain,
            source_working_domain,
        } = owner;
        let mut retiring = RetiringReadOnlyEngineV1 {
            database: Some(database),
            handoff: None,
            native_origin_context,
            provider,
            locator_build_domain,
            source_working_domain: Some(source_working_domain),
            worker: Some(worker),
            engine_control: _control,
            transition_control: Some(transition_control),
            source_control: Some(source_control),
        };
        // This native preparation is still in the existing maintenance phase.
        // Its native private capture/profile bound is audited independently;
        // the public handle sizeof above prices ONLY firstparty inline work.
        let plan = retiring
            .database
            .as_ref()
            .ok_or_else(unavailable)?
            .plan_read_only_reopen()
            .map_err(crate::store::native_read_error)?;
        let database = retiring.database.take().ok_or_else(unavailable)?;
        retiring.handoff = Some(
            database
                .into_read_only_reopen_handoff(plan)
                .map_err(crate::store::native_read_error)?,
        );
        // Check every transition invariant in ONE pattern before moving any
        // native handoff/control out. The success arm is entirely infallible;
        // the failure arm drops the intact native-FIRST retiring container.
        match retiring {
            RetiringReadOnlyEngineV1 {
                database: None,
                handoff: Some(handoff),
                native_origin_context,
                provider,
                locator_build_domain,
                source_working_domain: Some(domain),
                worker: Some(worker),
                engine_control,
                transition_control: Some(maintenance_control),
                source_control: Some(source_control),
            } => {
                // The original domain INCLUDES its existing source_prefix_pins.
                // No domain/pins/worker factory or second ledger is invoked.
                let prepared = PreparedNativeSourceReopenV1 {
                    handoff: Some(handoff),
                    domain,
                    worker,
                    maintenance_control,
                    source_control,
                };
                // The native old cache retired inside its consuming handoff.
                // These old owner fields retire before returning the Source
                // transition, whose native-FIRST/controls-LAST order is fixed.
                drop(native_origin_context);
                drop(provider);
                drop(locator_build_domain);
                drop(engine_control);
                Ok(prepared)
            }
            retiring => {
                drop(retiring);
                Err(unavailable())
            }
        }
    }
}

/// The only outer constructor consumes a completely validated, fields-private
/// predecessor. Its permit is minted here and moved directly into that owner's
/// consuming method; no externally chosen engine/backup pair is accepted.
pub(crate) struct PredecessorSourceConstructionV1 {
    _private: (),
}
impl PredecessorSourceConstructionV1 {
    pub(crate) fn bind(
        self,
        engine: OwnedCapacitySharedV1<EngineOwner<ReadOnlyDatabase>>,
        retained: crate::backup::RetainedPredecessorSourceDescriptorsV1,
    ) -> Result<OpenedPredecessorSourceReadV1, StorageError> {
        // Only the original OwnedBackupReadOnly owning method can receive this
        // unique permit. It extracts both values from the same sealed session.
        struct PendingEngineBindingV1 {
            engine: Option<OwnedCapacitySharedV1<EngineOwner<ReadOnlyDatabase>>>,
            retained: crate::backup::RetainedPredecessorSourceDescriptorsV1,
        }
        let mut pending = PendingEngineBindingV1 {
            engine: Some(engine),
            retained,
        };
        pending.retained.check_cancellation()?;
        let engine = pending.engine.take().ok_or_else(unavailable)?;
        let prepared = EngineOwner::prepare_source_reopen(engine)?;
        open_source_phase(prepared, pending.retained)
    }
}

pub(crate) struct OfflineSourceReadDataV1 {
    // Numerical pair custody only. It does not prove an auxiliary file is the
    // configured derived database or mint a GenerationSourceBase/Source grant.
    // Every actual primary/auxiliary child shares this same data owner.
    auxiliary: Option<RetainedAuxiliaryReadV1>,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    configured_derived: Option<crate::maintenance::ConfiguredDerivedBindingV1>,
    // Original physical exclusion/artifacts before original domain and worker.
    retained: crate::backup::RetainedPredecessorSourceDescriptorsV1,
    domain: crate::source_working_domain::SourceWorkingDomain,
    worker: ProjectionSourceWorkerOwnershipV1,
}
// The native file-core alias physically retires before its same-ledger charge.
// No EngineOwner, callback, raw descriptor or new worker lives in this retainer.
struct RetainedAuxiliaryReadV1 {
    _custody: redb::ReadOnlyPostHandoffCustody,
    _work: ProjectionSourceWorkReservationV1,
}
struct PendingAuxiliaryReadV1 {
    handoff: Option<ReadOnlyReopenHandoff>,
    database: Option<redb::PlannedReadOnlyDatabase>,
    custody: Option<redb::ReadOnlyPostHandoffCustody>,
    work: ProjectionSourceWorkReservationV1,
}

pub(crate) fn auxiliary_read_capture_control_bytes() -> Result<u64, StorageError> {
    let bytes = std::mem::size_of::<PendingAuxiliaryReadV1>()
        .checked_add(std::mem::size_of::<RetainedAuxiliaryReadV1>())
        .and_then(|n| n.checked_add(std::mem::size_of::<redb::PlannedReadOnlyDatabase>()))
        .and_then(|n| n.checked_add(std::mem::size_of::<redb::ReadOnlyPostHandoffCustody>()))
        .and_then(|n| u64::try_from(n).ok())
        .ok_or_else(unavailable)?;
    Ok(bytes)
}
pub(crate) fn auxiliary_source_read_bytes(
    handoff: &ReadOnlyReopenHandoff,
) -> Result<u64, StorageError> {
    handoff
        .peak_bytes()
        .checked_add(auxiliary_read_capture_control_bytes()?)
        .ok_or_else(unavailable)
}

impl OfflineSourceReadDataV1 {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(crate) fn require_configured_binding(
        &self,
        binding: &crate::maintenance::ConfiguredDerivedBindingV1,
    ) -> Result<(), StorageError> {
        self.check_cancellation()?;
        if self.configured_derived.is_some() {
            return Err(unavailable());
        }
        binding.require_domain(&self.domain)
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(crate) fn install_configured_binding(
        &mut self,
        binding: crate::maintenance::ConfiguredDerivedBindingV1,
    ) {
        self.configured_derived = Some(binding);
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(crate) fn configured_derived_is_absent(&self) -> Result<bool, StorageError> {
        self.check_cancellation()?;
        self.configured_derived
            .as_ref()
            .ok_or_else(unavailable)?
            .require_absent()
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub(crate) fn drop_failed_configured_auxiliary(&mut self) {
        drop(self.auxiliary.take());
    }

    // This consumes only an already parsed maintenance engine's genuine D4
    // handoff. Initial cold maintenance construction is NOT supplied here.
    pub(crate) fn open_auxiliary_handoff(
        &mut self,
        handoff: ReadOnlyReopenHandoff,
    ) -> Result<redb::PlannedReadOnlyDatabase, StorageError> {
        self.check_cancellation()?;
        if self.auxiliary.is_some() {
            return Err(unavailable());
        }
        let required = auxiliary_source_read_bytes(&handoff)?;
        let work = self
            .domain
            .budget()
            .reserve(required)
            .map_err(|_| unavailable())?;
        let mut pending = PendingAuxiliaryReadV1 {
            handoff: Some(handoff),
            database: None,
            custody: None,
            work,
        };
        let handoff = pending.handoff.take().ok_or_else(unavailable)?;
        pending.database = Some(
            redb::Database::builder()
                .open_read_only_planned_handoff(handoff)
                .map_err(crate::store::native_read_error)?,
        );
        pending.custody = Some(
            pending
                .database
                .as_ref()
                .ok_or_else(unavailable)?
                .retain_post_handoff_custody()
                .map_err(crate::store::native_read_error)?,
        );
        self.check_cancellation()?;
        match pending {
            PendingAuxiliaryReadV1 {
                handoff: None,
                database: Some(database),
                custody: Some(custody),
                work,
            } => {
                self.auxiliary = Some(RetainedAuxiliaryReadV1 {
                    _custody: custody,
                    _work: work,
                });
                Ok(database)
            }
            pending => {
                drop(pending);
                Err(unavailable())
            }
        }
    }
    pub(crate) fn source_original_history_allocation_bytes(&self) -> Result<u64, StorageError> {
        self.retained.source_original_history_allocation_bytes()
    }
    pub(crate) fn source_original_history(
        &self,
        construction: crate::store::SourceOriginalHistoryConstructionV1,
    ) -> Result<crate::history_stub::SourceOriginalHistoryV1, StorageError> {
        self.retained.source_original_history(construction)
    }
    pub(crate) fn originating_budget(&self) -> &riffdb_types::ProjectionSourceWorkingBudgetV1 {
        self.domain.budget()
    }

    pub(crate) fn check_cancellation(&self) -> Result<(), StorageError> {
        self.retained.check_cancellation()
    }
    #[cfg(test)]
    pub(crate) fn originating_budget_for_test(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        self.domain.budget()
    }
}

/// Opened Native plus ALL its fixed data/numerical custody. This cannot be
/// constructed from a read transaction, IDs or an independently selected token.
pub(crate) struct OpenedPredecessorSourceReadV1 {
    database: redb::PlannedReadOnlyDatabase,
    data: OfflineSourceReadDataV1,
    source_control: ProjectionSourceWorkReservationV1,
}
impl OpenedPredecessorSourceReadV1 {
    pub(crate) fn from_validated_predecessor(
        predecessor: crate::startup::MaintenanceValidatedPredecessorV1,
    ) -> Result<Self, StorageError> {
        predecessor.into_source_read_phase(PredecessorSourceConstructionV1 { _private: () })
    }
    // No raw getter/unpack tuple. Only the Store-minted, consumed permit can
    // assemble the existing canonical kernel's finished fixed facade.
    pub(crate) fn into_kernel(
        self,
        construction: crate::store::OfflineKernelConstructionV1,
    ) -> crate::store::SourceOwnedOfflineReadV1 {
        let Self {
            database,
            data,
            source_control,
        } = self;
        construction.bind(database, data, source_control)
    }
}

struct PendingOpenedSourceReadV1 {
    handoff: Option<ReadOnlyReopenHandoff>,
    database: Option<redb::PlannedReadOnlyDatabase>,
    retained: Option<crate::backup::RetainedPredecessorSourceDescriptorsV1>,
    domain: Option<crate::source_working_domain::SourceWorkingDomain>,
    worker: Option<ProjectionSourceWorkerOwnershipV1>,
    maintenance_control: LocatorCharge,
    source_control: ProjectionSourceWorkReservationV1,
}

fn open_source_phase(
    prepared: PreparedNativeSourceReopenV1,
    retained: crate::backup::RetainedPredecessorSourceDescriptorsV1,
) -> Result<OpenedPredecessorSourceReadV1, StorageError> {
    // Before ANY fallible quote/census operation, join all moved fields into
    // the native-FIRST transition container. Its inline construction was
    // already admitted by prepare_source_reopen's actual Maintenance control.
    let PreparedNativeSourceReopenV1 {
        handoff,
        domain,
        worker,
        maintenance_control,
        source_control,
    } = prepared;
    let mut pending = PendingOpenedSourceReadV1 {
        handoff,
        database: None,
        retained: Some(retained),
        domain: Some(domain),
        worker: Some(worker),
        maintenance_control,
        source_control,
    };
    // Quote itself is pure Native build/layout work, with no actual lock/hash
    // traversal. Native profile also pins the audited Rust std::File layout.
    let native_capture = pending
        .retained
        .as_ref()
        .ok_or_else(unavailable)?
        .native_quote_capture_bytes()?;
    let controls = std::mem::size_of::<PendingOpenedSourceReadV1>()
        .checked_add(std::mem::size_of::<OpenedPredecessorSourceReadV1>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PredecessorSourceConstructionV1>()))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(unavailable)?;
    // Same MOVED domain, no budget factory. Prepay all inspection work before
    // preview allocates its metadata/cursor buffer or visits any descriptor.
    let scratch_bytes = riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
        .checked_add(native_capture)
        .and_then(|bytes| bytes.checked_add(controls))
        .ok_or_else(unavailable)?;
    let mut scratch = pending
        .domain
        .as_ref()
        .ok_or_else(unavailable)?
        .budget()
        .reserve(scratch_bytes)
        .map_err(|_| unavailable())?;
    pending
        .retained
        .as_ref()
        .ok_or_else(unavailable)?
        .check_cancellation()?;
    // Native quote requires the exact qualified build profile. The encompassing
    // source inventory also authenticates std1.98.1/cap-std4.0.2 source and lock;
    // this closed field visitor never treats arbitrary custom backends as fd-only.
    let native_descriptor = pending
        .retained
        .as_ref()
        .ok_or_else(unavailable)?
        .native_retained_resource_bytes()?;
    let preview = riffdb_types::OwnedCapacityInspectionV1::preview_with(
        scratch.inspection_scope().map_err(|_| unavailable())?,
        |inspection| {
            pending
                .retained
                .as_ref()
                .ok_or(riffdb_types::OwnedCapacityInspectionErrorV1)?
                .inspect_source_descriptor_capacity(inspection)
        },
    )
    .map_err(|_| unavailable())?;
    let mut descriptors = preview.unshared_bytes();
    for shared in preview.shared_allocations() {
        descriptors = descriptors
            .checked_add(shared.bytes())
            .ok_or_else(unavailable)?;
    }
    let peak = pending
        .handoff
        .as_ref()
        .ok_or_else(unavailable)?
        .peak_bytes();
    let kernel = crate::store::offline_source_control_bytes()?;
    let required = descriptors
        .checked_add(native_descriptor)
        .and_then(|bytes| bytes.checked_add(peak))
        .and_then(|bytes| bytes.checked_add(kernel))
        .and_then(|bytes| bytes.checked_add(controls))
        .ok_or_else(unavailable)?;
    let extra = pending
        .domain
        .as_ref()
        .ok_or_else(unavailable)?
        .budget()
        .reserve(required)
        .map_err(|_| unavailable())?;
    pending
        .source_control
        .absorb(extra)
        .map_err(|_| unavailable())?;
    drop(preview);
    // Metadata and every inspection allocation physically retire before scratch
    // refund. Native+descriptor peak remains on source_control through all aliases.
    drop(scratch);
    let handoff = pending.handoff.take().ok_or_else(unavailable)?;
    pending.database = Some(
        redb::Database::builder()
            .open_read_only_planned_handoff(handoff)
            .map_err(crate::store::native_read_error)?,
    );
    pending
        .retained
        .as_ref()
        .ok_or_else(unavailable)?
        .check_cancellation()?;
    // No fallible loose-local extraction. The complete native-FIRST owner stays
    // intact on every error; success moves all fields in one infallible arm.
    match pending {
        PendingOpenedSourceReadV1 {
            handoff: None,
            database: Some(database),
            retained: Some(retained),
            domain: Some(domain),
            worker: Some(worker),
            maintenance_control,
            source_control,
        } => {
            let opened = OpenedPredecessorSourceReadV1 {
                database,
                data: OfflineSourceReadDataV1 {
                    auxiliary: None,
                    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
                    configured_derived: None,
                    retained,
                    domain,
                    worker,
                },
                source_control,
            };
            drop(maintenance_control);
            Ok(opened)
        }
        pending => {
            drop(pending);
            Err(unavailable())
        }
    }
}
