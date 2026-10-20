//! Bounded row-commitment-only inspection over the existing actual scan pin.
//! This produces no publication capability or complete-Source evidence end.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use crate::locator_memory::LocatorCharge;
use riffdb_storage_api::proto_codec::{
    decode_projection_source_maintenance_with_caller_scratch_v1,
    decode_projection_source_object_with_caller_scratch_v1,
    decode_projection_source_row_with_caller_scratch_v1,
    inspect_projection_source_maintenance_caller_decode_v1,
    inspect_projection_source_object_caller_decode_v1,
    inspect_projection_source_row_caller_decode_v1,
};
use riffdb_storage_api::{
    ProjectionSourceLifecycleV1, ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1,
    ProjectionSourceRowRootV1, ProjectionSourceRowV1, projection_source_row_preimage_bytes_v1,
    projection_source_row_scratch_bytes_v1,
};
use riffdb_types::{
    ContentDigest, ProjectionGeneration, ProjectionGroupKey, ProjectionGroupKeyBuilder,
    ProjectionSourceLocatorV1, owned_allocation_capacity_bound_v1,
};
use std::mem::{align_of, size_of};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Phase {
    ObjectLoad,
    MaintenanceLoad,
    Measure,
    Replay,
    Done,
    Refused,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    PublishedRows,
    CopyingProgress,
}
#[derive(Debug)]
enum Completion {
    RowRoot(ContentDigest),
    Copying(SourceCopyProgressV1),
}
/// Fixed verified prefix observation, not Source publication or closure authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SourceCopyProgressV1 {
    copied_rows: u32,
    copied_canonical_row_bytes: u64,
    rolling_hash: ContentDigest,
    complete_row_root: Option<ContentDigest>,
}
impl SourceCopyProgressV1 {
    pub(super) const fn copied_rows(&self) -> u32 {
        self.copied_rows
    }
    pub(super) const fn copied_canonical_row_bytes(&self) -> u64 {
        self.copied_canonical_row_bytes
    }
    pub(super) const fn rolling_hash(&self) -> ContentDigest {
        self.rolling_hash
    }
    pub(super) const fn complete_row_root(&self) -> Option<ContentDigest> {
        self.complete_row_root
    }
}
/// The Copying entry cannot accidentally return a partial final-row root.
pub(super) struct SourceCopyProgressVisitorV1<'scan, O: EvidenceScanReadOwnerV1> {
    kernel: SourceRowCommitmentVisitorV1<'scan, O>,
}
impl<O: EvidenceScanReadOwnerV1> SourceCopyProgressVisitorV1<'_, O> {
    pub(super) const fn position(&self) -> u64 {
        self.kernel.position()
    }
    pub(super) const fn physical_visits(&self) -> u64 {
        self.kernel.physical_visits()
    }
    pub(super) fn step(
        &mut self,
        limit: EvidencePageLimit,
    ) -> Result<Option<SourceCopyProgressV1>, StorageError> {
        match self.kernel.step_completion(limit)? {
            None => Ok(None),
            Some(Completion::Copying(progress)) => Ok(Some(progress)),
            Some(Completion::RowRoot(_)) => Err(invariant()),
        }
    }
}
impl<O: EvidenceScanReadOwnerV1> Drop for SourceCopyProgressVisitorV1<'_, O> {
    fn drop(&mut self) {}
}
/// The only input owner is the real complete scan. No caller-selected read,
/// engine, resolver, ledger or current-root substitution can be supplied.
pub(super) struct SourceRowCommitmentVisitorV1<'scan, O: EvidenceScanReadOwnerV1> {
    kernel: SourceCommitmentKernelV1,
    _control: LocatorCharge,
    scan: &'scan EvidenceScanCoreV1<O>,
}
struct SourceCommitmentKernelV1 {
    // The owned hash retains immutable identity aliases from the admitted
    // descriptor; destroy those aliases before its physical charge can drop.
    row_root: Option<ProjectionSourceRowRootV1<'static>>,
    scratch: Vec<u8>,
    computed_progress: Option<AdmittedProgressV1>,
    expected_progress: Option<
        source_validation_admission::MaintenanceDecodedSourceV1<ProjectionSourceMaintenanceV1>,
    >,
    mode: Mode,
    retain_metadata: bool,
    range: Option<Range<'static, &'static [u8], &'static [u8]>>,
    object:
        Option<source_validation_admission::MaintenanceDecodedSourceV1<ProjectionSourceObjectV1>>,
    locator: ProjectionSourceLocatorV1,
    phase: Phase,
    position: u64,
    physical_visits: u64,
    measured_rows: u32,
    measured_native_bytes: u64,
    object_native_bytes: u64,
    maintenance_native_bytes: u64,
    measured_semantic_bytes: u64,
    maximum_scratch: usize,
    replayed_rows: u32,
    replayed_native_bytes: u64,
    replayed_semantic_bytes: u64,
    // Data/ranges physically drop before their numerical charges.
    _replay_work: Option<LocatorCharge>,
}
impl<O: EvidenceScanReadOwnerV1> EvidenceScanCoreV1<O> {
    pub(super) fn begin_source_row_commitment_visit_v1(
        &self,
        locator: ProjectionSourceLocatorV1,
    ) -> Result<SourceRowCommitmentVisitorV1<'_, O>, StorageError> {
        self.begin_source_visit_v1(locator, Mode::PublishedRows)
    }
    pub(super) fn begin_source_copy_progress_visit_v1(
        &self,
        locator: ProjectionSourceLocatorV1,
    ) -> Result<SourceCopyProgressVisitorV1<'_, O>, StorageError> {
        Ok(SourceCopyProgressVisitorV1 {
            kernel: self.begin_source_visit_v1(locator, Mode::CopyingProgress)?,
        })
    }
    fn begin_source_visit_v1(
        &self,
        locator: ProjectionSourceLocatorV1,
        mode: Mode,
    ) -> Result<SourceRowCommitmentVisitorV1<'_, O>, StorageError> {
        self.check_cancellation()?;
        if self.validation_read.is_none() || locator.database_id() != self.database_id {
            return Err(invariant());
        }
        let control = self
            .shared
            .admit_source_maintenance_decode(size_of::<SourceRowCommitmentVisitorV1<'_, O>>())?;
        Ok(SourceRowCommitmentVisitorV1 {
            kernel: SourceCommitmentKernelV1::new(locator, mode),
            _control: control,
            scan: self,
        })
    }
}
impl SourceCommitmentKernelV1 {
    fn new(locator: ProjectionSourceLocatorV1, mode: Mode) -> Self {
        Self::new_with_metadata_custody(locator, mode, false)
    }
    // Only the closed public scan retains these existing admitted metadata
    // graphs until its next separately counted reciprocal Root visit.
    fn new_public(locator: ProjectionSourceLocatorV1, mode: Mode) -> Self {
        Self::new_with_metadata_custody(locator, mode, true)
    }
    fn new_with_metadata_custody(
        locator: ProjectionSourceLocatorV1,
        mode: Mode,
        retain_metadata: bool,
    ) -> Self {
        Self {
            object: None,
            computed_progress: None,
            expected_progress: None,
            mode,
            retain_metadata,
            row_root: None,
            scratch: Vec::new(),
            range: None,
            locator,
            phase: Phase::ObjectLoad,
            position: 0,
            physical_visits: 0,
            measured_rows: 0,
            measured_native_bytes: 0,
            object_native_bytes: 0,
            maintenance_native_bytes: 0,
            measured_semantic_bytes: 0,
            maximum_scratch: 0,
            replayed_rows: 0,
            replayed_native_bytes: 0,
            replayed_semantic_bytes: 0,
            _replay_work: None,
        }
    }
    pub(super) const fn position(&self) -> u64 {
        self.position
    }
    pub(super) const fn physical_visits(&self) -> u64 {
        self.physical_visits
    }
    fn step_completion<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
        limit: EvidencePageLimit,
    ) -> Result<Option<Completion>, StorageError> {
        if matches!(self.phase, Phase::Done | Phase::Refused) {
            return Err(invariant());
        }
        // Source visits retain the existing4-MiB page input bound even
        // when the caller grants a larger record count. Never multiply a
        // maximum-sized row by a larger EvidencePageLimit.
        for _ in 0..limit.get().min(1) {
            if let Err(error) = scan.check_cancellation() {
                self.refuse();
                return Err(error);
            }
            self.physical_visits = self
                .physical_visits
                .checked_add(1)
                .ok_or_else(limit_exceeded)?;
            let result = match self.phase {
                Phase::ObjectLoad => self.load_object(scan).map(|()| None),
                Phase::MaintenanceLoad => self.load_maintenance(scan).map(|()| None),
                Phase::Measure => self.measure_one(scan).map(|()| None),
                Phase::Replay => self.replay_one(scan),
                Phase::Done | Phase::Refused => Err(invariant()),
            };
            match result {
                Err(error) => {
                    self.refuse();
                    return Err(error);
                }
                Ok(commitment) => {
                    self.position = self.position.checked_add(1).ok_or_else(limit_exceeded)?;
                    if commitment.is_some() {
                        return Ok(commitment);
                    }
                }
            }
        }
        Ok(None)
    }
    fn read<O: EvidenceScanReadOwnerV1>(
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<&ReadTransaction, StorageError> {
        scan.validation_read.as_ref().ok_or_else(invariant)
    }
    fn descriptor(
        &self,
    ) -> Result<&riffdb_storage_api::ProjectionSourceDescriptorV1, StorageError> {
        Ok(self
            .object
            .as_ref()
            .ok_or_else(invariant)?
            .value()
            .descriptor())
    }
    fn load_object<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<(), StorageError> {
        let table = Self::read(scan)?
            .open_table(source_validation::definition(1)?)
            .map_err(table_error)?;
        let key = crate::source_keys::source_object_key(self.locator);
        let value = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        check_input(key.as_slice(), value.value())?;
        let object = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_object_caller_decode_v1,
            decode_projection_source_object_with_caller_scratch_v1,
        )?;
        self.accept_object(scan, object, add_native(0, key.as_slice(), value.value())?)
    }
    fn accept_object<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
        object: source_validation_admission::MaintenanceDecodedSourceV1<ProjectionSourceObjectV1>,
        actual_native_bytes: u64,
    ) -> Result<(), StorageError> {
        if object.value().descriptor().reference().locator() != self.locator {
            return Err(corrupt());
        }
        let eligible = match self.mode {
            Mode::PublishedRows => matches!(
                object.value().lifecycle(),
                ProjectionSourceLifecycleV1::Published | ProjectionSourceLifecycleV1::Superseded
            ),
            Mode::CopyingProgress => {
                object.value().lifecycle() == ProjectionSourceLifecycleV1::Copying
            }
        };
        // Actual bounded Abort/GC checkpoint semantics are not yet implemented.
        if !eligible {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.object_native_bytes = actual_native_bytes;
        self.object = Some(object);
        if self.mode == Mode::CopyingProgress {
            self.phase = Phase::MaintenanceLoad;
        } else {
            self.range = self.open_prefix(scan)?;
            self.phase = Phase::Measure;
        }
        Ok(())
    }
    fn load_maintenance<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<(), StorageError> {
        let table = Self::read(scan)?
            .open_table(source_validation::definition(3)?)
            .map_err(table_error)?;
        let key = crate::source_keys::source_maintenance_key(self.locator);
        let value = table
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(corrupt)?;
        check_input(key.as_slice(), value.value())?;
        let expected = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_maintenance_caller_decode_v1,
            decode_projection_source_maintenance_with_caller_scratch_v1,
        )?;
        if expected.value().descriptor() != self.descriptor()? {
            return Err(corrupt());
        }
        let bound =
            ProjectionSourceMaintenanceV1::initial_work_bound_v1().map_err(|_| corrupt())?;
        let work = scan.shared.admit_source_maintenance_decode(
            usize::try_from(bound).map_err(|_| limit_exceeded())?,
        )?;
        let computed = ProjectionSourceMaintenanceV1::initial(
            self.descriptor()?.clone(),
            expected.value().expected_prior_root().cloned(),
        )
        .map_err(|_| corrupt())?;
        self.computed_progress = Some(AdmittedProgressV1 {
            value: computed,
            _work: work,
        });
        self.maintenance_native_bytes = add_native(0, key.as_slice(), value.value())?;
        self.expected_progress = Some(expected);
        self.range = self.open_prefix(scan)?;
        self.phase = Phase::Measure;
        Ok(())
    }
    #[allow(clippy::type_complexity)]
    fn open_prefix<O: EvidenceScanReadOwnerV1>(
        &self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<Option<Range<'static, &'static [u8], &'static [u8]>>, StorageError> {
        let table = match Self::read(scan)?.open_table(source_validation::definition(0)?) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(table_error(error)),
        };
        let prefix = crate::source_keys::source_row_prefix(self.locator);
        let mut upper = prefix;
        let mut advanced = false;
        for byte in upper.iter_mut().rev() {
            if *byte != u8::MAX {
                *byte += 1;
                advanced = true;
                break;
            }
            *byte = 0;
        }
        if !advanced {
            return Err(invariant());
        }
        table
            .range::<&[u8]>((Included(prefix.as_slice()), Excluded(upper.as_slice())))
            .map(Some)
            .map_err(precommit_storage_error)
    }
    fn measure_one<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<(), StorageError> {
        let next = self.range.as_mut().and_then(Iterator::next);
        let Some(next) = next else {
            return self.finish_measurement(scan);
        };
        let (key, value) = next.map_err(precommit_storage_error)?;
        check_input(key.value(), value.value())?;
        let row = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_row_caller_decode_v1,
            decode_projection_source_row_with_caller_scratch_v1,
        )?;
        let _key = self.checked_key(scan, key.value(), row.value())?;
        self.measured_rows = self.measured_rows.checked_add(1).ok_or_else(corrupt)?;
        self.measured_native_bytes =
            add_native(self.measured_native_bytes, key.value(), value.value())?;
        self.measured_semantic_bytes = self
            .measured_semantic_bytes
            .checked_add(
                projection_source_row_preimage_bytes_v1(row.value()).map_err(|_| corrupt())?,
            )
            .ok_or_else(corrupt)?;
        self.maximum_scratch = self
            .maximum_scratch
            .max(projection_source_row_scratch_bytes_v1(row.value()).map_err(|_| corrupt())?);
        let descriptor = self.descriptor()?;
        if self.measured_rows > descriptor.row_count()
            || self.measured_native_bytes > descriptor.canonical_row_bytes()
            || self.measured_semantic_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_OBJECT_BYTES
        {
            return Err(corrupt());
        }
        Ok(())
    }
    fn finish_measurement<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<(), StorageError> {
        let descriptor = self.descriptor()?;
        let (rows, bytes) = match self.mode {
            Mode::PublishedRows => (descriptor.row_count(), descriptor.canonical_row_bytes()),
            Mode::CopyingProgress => {
                let progress = self
                    .expected_progress
                    .as_ref()
                    .ok_or_else(invariant)?
                    .value();
                (
                    progress.copied_rows(),
                    progress.copied_canonical_row_bytes(),
                )
            }
        };
        if self.measured_rows != rows || self.measured_native_bytes != bytes {
            return Err(corrupt());
        }
        let complete = self.mode == Mode::PublishedRows
            || self
                .expected_progress
                .as_ref()
                .is_some_and(|p| p.value().copy_complete());
        if !complete {
            self.range = self.open_prefix(scan)?;
            self.phase = Phase::Replay;
            return Ok(());
        }
        // Fixed hash/control fields are admitted with Self. Identity::clone
        // retains only immutable ContractLineage Arc aliases. The descriptor
        // owner remains alive (and charged) until those aliases are destroyed.
        // Admit canonical scratch before any Vec allocation.
        let scratch_bytes =
            owned_allocation_capacity_bound_v1(self.maximum_scratch, align_of::<u8>())
                .map_err(|_| limit_exceeded())?;
        let maximum = usize::try_from(scratch_bytes).map_err(|_| limit_exceeded())?;
        let work = scan.shared.admit_source_maintenance_decode(maximum)?;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(self.maximum_scratch)
            .map_err(|_| limit_exceeded())?;
        if owned_allocation_capacity_bound_v1(scratch.capacity(), align_of::<u8>())
            .map_err(|_| limit_exceeded())?
            > scratch_bytes
        {
            return Err(limit_exceeded());
        }
        let row_root = ProjectionSourceRowRootV1::new_owned(
            descriptor.reference().identity().clone(),
            if self.measured_rows == 0 {
                None
            } else {
                Some(self.locator)
            },
            descriptor.applied_through(),
            self.measured_rows,
            self.measured_semantic_bytes,
        )
        .map_err(|_| corrupt())?;
        self.scratch = scratch;
        self.row_root = Some(row_root);
        self._replay_work = Some(work);
        self.range = self.open_prefix(scan)?;
        self.phase = Phase::Replay;
        Ok(())
    }
    fn replay_one<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<Option<Completion>, StorageError> {
        let next = self.range.as_mut().and_then(Iterator::next);
        let Some(next) = next else {
            return self.finish_replay().map(Some);
        };
        let (key, value) = next.map_err(precommit_storage_error)?;
        check_input(key.value(), value.value())?;
        let row = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_row_caller_decode_v1,
            decode_projection_source_row_with_caller_scratch_v1,
        )?;
        let order = self.checked_key(scan, key.value(), row.value())?;
        self.replayed_rows = self.replayed_rows.checked_add(1).ok_or_else(corrupt)?;
        self.replayed_native_bytes =
            add_native(self.replayed_native_bytes, key.value(), value.value())?;
        self.replayed_semantic_bytes = self
            .replayed_semantic_bytes
            .checked_add(
                projection_source_row_preimage_bytes_v1(row.value()).map_err(|_| corrupt())?,
            )
            .ok_or_else(corrupt)?;
        if let Some(root) = self.row_root.as_mut() {
            root.append(&order.value, row.value(), &mut self.scratch)
                .map_err(|_| corrupt())?;
        }
        if self.mode == Mode::CopyingProgress {
            let before = self.computed_progress.as_ref().ok_or_else(invariant)?;
            let bound = before
                .value
                .copy_step_work_bound_v1()
                .map_err(|_| corrupt())?;
            let work = scan.shared.admit_source_maintenance_decode(
                usize::try_from(bound).map_err(|_| limit_exceeded())?,
            )?;
            // One complete borrowed key/value pair, with the old owner retained
            // until the sole semantic copy-step owner has produced its successor.
            let value = before
                .value
                .after_copy_step(&before.value, &[(key.value(), value.value())])
                .map_err(|_| corrupt())?;
            self.computed_progress = Some(AdmittedProgressV1 { value, _work: work });
        }
        Ok(None)
    }
    fn finish_replay(&mut self) -> Result<Completion, StorageError> {
        if self.replayed_rows != self.measured_rows
            || self.replayed_native_bytes != self.measured_native_bytes
            || self.replayed_semantic_bytes != self.measured_semantic_bytes
        {
            return Err(corrupt());
        }
        let root = self
            .row_root
            .take()
            .map(|hash| hash.finish().map_err(|_| corrupt()))
            .transpose()?;
        let expected_root = self.descriptor()?.row_root();
        if root.is_some_and(|value| value != expected_root) {
            return Err(corrupt());
        }
        let completion = match self.mode {
            Mode::PublishedRows => Completion::RowRoot(root.ok_or_else(invariant)?),
            Mode::CopyingProgress => {
                let computed = &self.computed_progress.as_ref().ok_or_else(invariant)?.value;
                let expected = self
                    .expected_progress
                    .as_ref()
                    .ok_or_else(invariant)?
                    .value();
                if computed != expected || expected.copy_complete() != root.is_some() {
                    return Err(corrupt());
                }
                Completion::Copying(SourceCopyProgressV1 {
                    copied_rows: computed.copied_rows(),
                    copied_canonical_row_bytes: computed.copied_canonical_row_bytes(),
                    rolling_hash: computed.rolling_hash(),
                    complete_row_root: root,
                })
            }
        };
        self.phase = Phase::Done;
        if self.retain_metadata {
            self.release_replay_graphs();
        } else {
            self.release_graphs();
        }
        Ok(completion)
    }
    fn checked_key<O: EvidenceScanReadOwnerV1>(
        &self,
        scan: &EvidenceScanCoreV1<O>,
        key: &[u8],
        row: &ProjectionSourceRowV1,
    ) -> Result<AdmittedOrderKeyV1, StorageError> {
        let descriptor = self.descriptor()?;
        if row.locator() != self.locator
            || row.identity() != descriptor.reference().identity()
            || row.last_changed_sequence() > descriptor.applied_through()
        {
            return Err(corrupt());
        }
        let maximum =
            ProjectionSourceRowV1::stored_parts_work_bound_v1(row.group_values(), row.measures())
                .map_err(|_| corrupt())?;
        let work = scan.shared.admit_source_maintenance_decode(
            usize::try_from(maximum).map_err(|_| limit_exceeded())?,
        )?;
        let mut builder =
            ProjectionGroupKeyBuilder::new(row.identity().clone(), ProjectionGeneration::first());
        for value in row.group_values() {
            builder
                .push_component(value.clone())
                .map_err(|_| corrupt())?;
        }
        let value = builder.finish().map_err(|_| corrupt())?;
        crate::source_keys::check_source_row_key(key, self.locator, &value)
            .map_err(|_| corrupt())?;
        Ok(AdmittedOrderKeyV1 { value, _work: work })
    }
    fn release_replay_graphs(&mut self) {
        self.range = None;
        self.row_root = None;
        self.scratch = Vec::new();
        self.computed_progress = None;
        self._replay_work = None;
    }
    fn release_graphs(&mut self) {
        self.release_replay_graphs();
        self.expected_progress = None;
        self.object = None;
    }
    fn refuse(&mut self) {
        self.phase = Phase::Refused;
        self.release_graphs();
    }
}
impl<O: EvidenceScanReadOwnerV1> SourceRowCommitmentVisitorV1<'_, O> {
    pub(super) const fn position(&self) -> u64 {
        self.kernel.position()
    }
    pub(super) const fn physical_visits(&self) -> u64 {
        self.kernel.physical_visits()
    }
    pub(super) fn step(
        &mut self,
        limit: EvidencePageLimit,
    ) -> Result<Option<ContentDigest>, StorageError> {
        match self.step_completion(limit)? {
            None => Ok(None),
            Some(Completion::RowRoot(root)) => Ok(Some(root)),
            Some(Completion::Copying(_)) => Err(invariant()),
        }
    }
    fn step_completion(
        &mut self,
        limit: EvidencePageLimit,
    ) -> Result<Option<Completion>, StorageError> {
        self.kernel.step_completion(self.scan, limit)
    }
}

#[path = "source_public_commitments.rs"]
mod public;
pub(super) use public::{AdmittedSourceValidationV1, planned_work};

// Unused visitors retain the actual scan/read/lease borrow and all numerical
// custody until physical destruction, including compiler-shortened borrows.
impl<O: EvidenceScanReadOwnerV1> Drop for SourceRowCommitmentVisitorV1<'_, O> {
    fn drop(&mut self) {}
}
struct AdmittedProgressV1 {
    value: ProjectionSourceMaintenanceV1,
    _work: LocatorCharge,
}
impl Drop for AdmittedProgressV1 {
    fn drop(&mut self) {}
}
struct AdmittedOrderKeyV1 {
    value: ProjectionGroupKey,
    _work: LocatorCharge,
}
impl Drop for AdmittedOrderKeyV1 {
    fn drop(&mut self) {}
}
fn add_native(before: u64, key: &[u8], value: &[u8]) -> Result<u64, StorageError> {
    key.len()
        .checked_add(value.len())
        .and_then(|n| u64::try_from(n).ok())
        .and_then(|n| before.checked_add(n))
        .ok_or_else(corrupt)
}

fn check_input(key: &[u8], value: &[u8]) -> Result<(), StorageError> {
    // Borrowed limits precede DTO/scratch/key-builder work. Native temporary
    // page/cache bounds remain the separately tracked resource-plan obligation.
    if key.len() > riffdb_types::MAX_KEY_BYTES
        || key
            .len()
            .checked_add(value.len())
            .is_none_or(|n| n > riffdb_storage_api::MAX_SCAN_PAGE_BYTES)
    {
        return Err(corrupt());
    }
    Ok(())
}
