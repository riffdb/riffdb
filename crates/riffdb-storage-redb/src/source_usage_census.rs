//! Closed physical Source usage reconstruction, never source/evaluation authority.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use crate::source_keys::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, ChangelogLineageV3, ProjectionCheckpointOrdinalAllocatorV1,
    ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1, ProjectionSourceRecordDecodeErrorV1,
    proto_codec::*,
};
use riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkerOwnershipV1;
use riffdb_types::{
    MAX_KEY_BYTES, ProjectionSourceLocatorV1, ProjectionSourceWorkAllocationV1,
    ProjectionSourceWorkReservationV1,
};
use std::{marker::PhantomData, rc::Rc};
#[path = "source_usage_census_check.rs"]
mod check;
#[path = "source_usage_census_object.rs"]
mod object;
use super::read::{CensusRead, EncodedRow, PageBudget, ReadRow};
use check::{decode_error, root_for_identity, validate_row};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    Allocator,
    Roots,
    Objects,
    Rows,
    Maintenance,
    Done,
}
#[derive(Default)]
struct Totals {
    roots: u16,
    selected: u64,
    total: u64,
    object_rows: u64,
    object_row_bytes: u64,
    global_rows: u64,
    global_row_bytes: u64,
    candidate: Option<Candidate>,
    allocator_bytes: u64,
}
struct Candidate {
    locator: ProjectionSourceLocatorV1,
    full: u64,
    actual: u64,
    maintenance_seen: bool,
}
struct CurrentObject {
    object: ProjectionSourceWorkAllocationV1<ProjectionSourceObjectV1>,
    maintenance: Option<ProjectionSourceWorkAllocationV1<ProjectionSourceMaintenanceV1>>,
    object_bytes: u64,
    maintenance_bytes: u64,
    rows: u32,
    bytes: u64,
}
/// One actual-engine worker phase. No redb snapshot is retained in these fields.
/// The read-only numeric result cannot acquire a Source or authorize mutation.
pub(crate) struct SourceUsageCensus<'ports> {
    current: Option<CurrentObject>,
    ports: &'ports crate::RedbOperationalPorts,
    epoch: Option<SourceUsageEpoch<'ports>>,
    lineage: Option<ChangelogLineageV3>,
    allocator: Option<ProjectionCheckpointOrdinalAllocatorV1>,
    phase: Phase,
    after: Vec<u8>,
    row_after: Vec<u8>,
    totals: Totals,
    failed: bool,
    worker: Option<ProjectionSourceWorkerOwnershipV1>,
    control: Option<ProjectionSourceWorkReservationV1>,
    // The existing bounded TLS scratch primitive is created/dropped on this
    // synchronous worker frame. This owner cannot enter another thread/queue.
    _thread: PhantomData<Rc<()>>,
}
impl Drop for SourceUsageCensus<'_> {
    fn drop(&mut self) {}
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SourceUsageCensusStep {
    pub(crate) rows: u16,
    pub(crate) bytes: u64,
    pub(crate) done: bool,
}

impl<'ports> SourceUsageCensus<'ports> {
    pub(crate) fn start(ports: &'ports crate::RedbOperationalPorts) -> Result<Self, StorageError> {
        if !ports.shared.is_source_mode()
            || ports
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let budget = ports.shared.database.source_working_budget();
        let worker = budget.try_acquire_worker().map_err(|_| unavailable())?;
        let bytes = riffdb_types::owned_allocation_capacity_bound_v1(MAX_KEY_BYTES, 1)
            .map_err(|_| limit())?
            .checked_mul(2)
            .and_then(|n| n.checked_add(std::mem::size_of::<Self>() as u64))
            .ok_or_else(limit)?;
        let control = budget.reserve(bytes).map_err(|_| unavailable())?;
        let gate = ports
            .shared
            .mutation_gate
            .acquire_source()?
            .ok_or_else(unavailable)?;
        let epoch = ports
            .shared
            .database
            .source_usage_domain()
            .capture_epoch()?;
        drop(gate);
        // Fixed metadata eligibility is read off-gate, never inferred from Fresh.
        let (read, lineage) = CensusRead::capture(ports, &epoch, None)?;
        drop(read);
        Ok(Self {
            current: None,
            ports,
            epoch: Some(epoch),
            lineage: Some(lineage),
            allocator: None,
            phase: Phase::Allocator,
            after: Vec::with_capacity(MAX_KEY_BYTES),
            row_after: Vec::with_capacity(MAX_KEY_BYTES),
            totals: Totals::default(),
            failed: false,
            worker: Some(worker),
            control: Some(control),
            _thread: PhantomData,
        })
    }
    pub(crate) fn step(&mut self) -> Result<SourceUsageCensusStep, StorageError> {
        if self.failed {
            return Err(unavailable());
        }
        let result = self.step_inner();
        if let Err(error) = &result {
            self.failed = true;
            self.current = None;
            if error.kind() != StorageErrorKind::Unavailable
                && let Some(epoch) = self.epoch.as_ref()
            {
                epoch.domain.observe_unreadable(epoch);
            }
        }
        result
    }
    fn step_inner(&mut self) -> Result<SourceUsageCensusStep, StorageError> {
        let (read, _) = CensusRead::capture(
            self.ports,
            self.epoch.as_ref().ok_or_else(integrity)?,
            self.lineage,
        )?;
        let mut page = PageBudget::default();
        match self.phase {
            Phase::Allocator => {
                let (allocator, bytes) = read.allocator(&mut page)?;
                self.allocator = Some(allocator);
                self.totals.allocator_bytes = bytes;
                self.totals.total = bytes;
                self.phase = Phase::Roots;
            }
            Phase::Roots => {
                match read.next(N::ProjectionSourceRoots, &self.after, None, &mut page)? {
                    ReadRow::PageFull => {}
                    ReadRow::End => {
                        self.after.clear();
                        self.phase = Phase::Objects;
                    }
                    ReadRow::Row(row) => {
                        let root =
                            decode_projection_source_root_admitted_v1(row.value(), read.budget())
                                .map_err(decode_error)?
                                .into_parts()
                                .0;
                        check_source_root_key(
                            row.key(),
                            root.reference().identity(),
                            root.reference().locator(),
                        )
                        .map_err(|_| integrity())?;
                        let selected =
                            root_for_identity(&read, root.reference().identity(), &mut page)?
                                .ok_or_else(integrity)?;
                        if *selected != *root {
                            return Err(integrity());
                        }
                        let object_key = source_object_key(root.reference().locator());
                        let object_row =
                            match read.point(N::ProjectionSourceObjects, &object_key, &mut page)? {
                                ReadRow::Row(row) => row,
                                ReadRow::End => return Err(integrity()),
                                ReadRow::PageFull => return Err(integrity()),
                            };
                        let object = decode_projection_source_object_admitted_v1(
                            object_row.value(),
                            read.budget(),
                        )
                        .map_err(decode_error)?
                        .into_parts()
                        .0;
                        if !root.matches_published_object(&object) {
                            return Err(integrity());
                        }
                        let roots = self.totals.roots.checked_add(1).ok_or_else(limit)?;
                        if roots > riffdb_storage_api::MAX_PROJECTION_SOURCE_ROOTS {
                            return Err(limit());
                        }
                        let selected = add(
                            add(row.bytes(), object_row.bytes())?,
                            object.descriptor().canonical_row_bytes(),
                        )?;
                        self.totals.selected = add(self.totals.selected, selected)?;
                        self.totals.roots = roots;
                        self.totals.total = add(self.totals.total, row.bytes())?;
                        save_key(&mut self.after, row.key());
                    }
                }
            }
            Phase::Objects => {
                object::step_objects(self, &read, &mut page)?;
            }
            Phase::Rows => {
                while !page.full() {
                    match read.next(N::ProjectionSourceRows, &self.after, None, &mut page)? {
                        ReadRow::PageFull => break,
                        ReadRow::End => {
                            if self.totals.global_rows != self.totals.object_rows
                                || self.totals.global_row_bytes != self.totals.object_row_bytes
                            {
                                return Err(integrity());
                            }
                            self.totals.total =
                                add(self.totals.total, self.totals.global_row_bytes)?;
                            self.after.clear();
                            self.phase = Phase::Maintenance;
                            break;
                        }
                        ReadRow::Row(row) => {
                            let locator =
                                source_row_locator_prefix(row.key()).map_err(|_| integrity())?;
                            let object_row = match read.point(
                                N::ProjectionSourceObjects,
                                &source_object_key(locator),
                                &mut page,
                            )? {
                                ReadRow::Row(row) => row,
                                ReadRow::End => return Err(integrity()),
                                ReadRow::PageFull => break,
                            };
                            let object = decode_projection_source_object_admitted_v1(
                                object_row.value(),
                                read.budget(),
                            )
                            .map_err(decode_error)?
                            .into_parts()
                            .0;
                            validate_row(&read, &row, &object)?;
                            self.totals.global_rows = add(self.totals.global_rows, 1)?;
                            self.totals.global_row_bytes =
                                add(self.totals.global_row_bytes, row.bytes())?;
                            save_key(&mut self.after, row.key());
                        }
                    }
                }
            }
            Phase::Maintenance => {
                while !page.full() {
                    match read.next(N::ProjectionSourceMaintenance, &self.after, None, &mut page)? {
                        ReadRow::PageFull => break,
                        ReadRow::End => {
                            if self
                                .totals
                                .candidate
                                .as_ref()
                                .is_some_and(|candidate| !candidate.maintenance_seen)
                            {
                                return Err(integrity());
                            }
                            self.phase = Phase::Done;
                            break;
                        }
                        ReadRow::Row(row) => {
                            let maintenance = decode_projection_source_maintenance_admitted_v1(
                                row.value(),
                                read.budget(),
                            )
                            .map_err(decode_error)?
                            .into_parts()
                            .0;
                            let locator = decode_source_maintenance_key(row.key())
                                .map_err(|_| integrity())?;
                            if locator != maintenance.descriptor().reference().locator() {
                                return Err(integrity());
                            }
                            let object_row = match read.point(
                                N::ProjectionSourceObjects,
                                &source_object_key(locator),
                                &mut page,
                            )? {
                                ReadRow::Row(row) => row,
                                ReadRow::End => return Err(integrity()),
                                ReadRow::PageFull => break,
                            };
                            let object = decode_projection_source_object_admitted_v1(
                                object_row.value(),
                                read.budget(),
                            )
                            .map_err(decode_error)?
                            .into_parts()
                            .0;
                            if object.descriptor() != maintenance.descriptor() {
                                return Err(integrity());
                            }
                            if maintenance.last_copied_key().is_some_and(|key| {
                                source_row_locator_prefix(key).ok() != Some(locator)
                            }) {
                                return Err(integrity());
                            }
                            if let Some(candidate) = self.totals.candidate.as_mut()
                                && candidate.locator == locator
                            {
                                if candidate.maintenance_seen {
                                    return Err(integrity());
                                }
                                candidate.maintenance_seen = true;
                            }
                            self.totals.total = add(self.totals.total, row.bytes())?;
                            save_key(&mut self.after, row.key());
                        }
                    }
                }
            }
            Phase::Done => {}
        }
        if self.totals.total > riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES
            || self.totals.object_row_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES
            || self.totals.global_row_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES
            || self.totals.selected > riffdb_storage_api::MAX_PROJECTION_SELECTED_SOURCE_BYTES
        {
            return Err(limit());
        }
        drop(read);
        let epoch = self.epoch.as_ref().ok_or_else(integrity)?;
        epoch.domain.require_epoch(epoch)?;
        Ok(SourceUsageCensusStep {
            rows: page.rows,
            bytes: page.bytes,
            done: self.phase == Phase::Done,
        })
    }
    pub(crate) fn finish(mut self) -> Result<SourceUsageReconstruction<'ports>, StorageError> {
        if self.failed || self.phase != Phase::Done {
            return Err(unavailable());
        }
        let mut total = self.totals.total;
        let candidate = self
            .totals
            .candidate
            .as_ref()
            .map(|candidate| candidate.full);
        if let Some(candidate) = &self.totals.candidate {
            total = add(
                total,
                candidate
                    .full
                    .checked_sub(candidate.actual)
                    .ok_or_else(integrity)?,
            )?;
        }
        let usage = Usage::new(self.totals.roots, self.totals.selected, total, candidate)
            .map_err(|_| limit())?;
        // No snapshot/row/control graph is kept while waiting for the primary gate.
        self.current = None;
        let gate = self
            .ports
            .shared
            .mutation_gate
            .acquire_source()?
            .ok_or_else(unavailable)?;
        if !self.ports.shared.is_source_mode()
            || self
                .ports
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        let epoch = self.epoch.take().ok_or_else(integrity)?;
        // Final CAS uses the exact actual engine/domain and complete Source
        // publication fence; no population read or hashing occurs under this gate.
        let version = epoch.version;
        let domain = epoch.domain;
        if self.control.as_ref().is_none_or(|work| {
            work.bytes() < std::mem::size_of::<SourceUsageReconstruction<'_>>() as u64
        }) {
            return Err(limit());
        }
        let candidate = self
            .totals
            .candidate
            .as_ref()
            .map(|candidate| CandidateUsage {
                locator: candidate.locator,
                full: candidate.full,
                actual: candidate.actual,
            });
        domain.install_completed_census(CompletedCensus {
            epoch,
            usage,
            candidate,
        })?;
        drop(gate);
        Ok(SourceUsageReconstruction {
            ports: self.ports,
            domain,
            version,
            usage,
            worker: self.worker.take(),
            _control: self.control.take().ok_or_else(integrity)?,
        })
    }
}

/// Numeric reconstruction plus the SAME actual-engine worker phase. No Original
/// closure, Source candidate or publication authority follows from this owner.
/// The consuming native Original factory separately validates its exact source.
pub(crate) struct SourceUsageReconstruction<'ports> {
    ports: &'ports crate::RedbOperationalPorts,
    domain: &'ports SourceUsageDomain,
    version: u64,
    usage: Usage,
    worker: Option<ProjectionSourceWorkerOwnershipV1>,
    _control: ProjectionSourceWorkReservationV1,
}
impl Drop for SourceUsageReconstruction<'_> {
    fn drop(&mut self) {}
}
impl SourceUsageReconstruction<'_> {
    pub(crate) fn usage(&self) -> Usage {
        self.usage
    }
    /// Only the closed native Original capture factory consumes this method,
    /// under its SAME actual source gate/read admission. This checks affinity
    /// and retains the existing claim; it does not attest prefix completeness.
    pub(crate) fn into_worker_for_original_capture(
        mut self,
        ports: &crate::RedbOperationalPorts,
        lease: &crate::gate::ExclusiveLease,
    ) -> Result<ProjectionSourceWorkerOwnershipV1, StorageError> {
        if !self
            .ports
            .shared
            .database
            .same_allocation(&ports.shared.database)
            || !std::ptr::eq(self.domain, ports.shared.database.source_usage_domain())
            || !lease.is_source_grant_for(&ports.shared.mutation_gate)
        {
            return Err(integrity());
        }
        if !ports.shared.is_source_mode()
            || ports
                .shared
                .write_fenced
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(unavailable());
        }
        self.domain.require_epoch(&SourceUsageEpoch {
            domain: self.domain,
            version: self.version,
        })?;
        if self
            .domain
            .state
            .lock()
            .map_err(|_| unavailable())?
            .input_obligation
            .is_some()
        {
            return Err(unavailable());
        }
        if self.domain.complete_observation()? != self.usage {
            return Err(unavailable());
        }
        self.worker.take().ok_or_else(integrity)
    }
}

fn add(left: u64, right: u64) -> Result<u64, StorageError> {
    left.checked_add(right).ok_or_else(limit)
}
fn save_key(output: &mut Vec<u8>, key: &[u8]) {
    output.clear();
    output.extend_from_slice(key);
}
