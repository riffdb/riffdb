//! Explicit bounded public-walk consumption of the sole Source row/progress kernel.
//! This component cannot grant publication or complete Source-origin/base closure.
use super::*;
#[path = "source_public_quota.rs"]
mod quota;
use quota::SourceQuotaValidationV1;

/// Data Box is destroyed and deallocated before its actual maintenance charge.
/// Kept before the Core's shared owner; no new Arc/engine/self-reference exists.
pub(in crate::startup) struct AdmittedSourceValidationV1 {
    data: Box<SourceValidationStateV1>,
    _work: LocatorCharge,
}
struct SourceValidationStateV1 {
    kernel: Option<SourceCommitmentKernelV1>,
    completed: Option<Completion>,
    quota: SourceQuotaValidationV1,
    origin: crate::native_source_fresh::SourceOriginObservationV1,
    wrong_origin: bool,
    objects: Option<Range<'static, &'static [u8], &'static [u8]>>,
    position: u64,
    completed_objects: u64,
    measured_rows: u64,
    maintenance_loads: u64,
    expected: [u64; source_validation::SOURCE_PHASES],
    done: bool,
    refused: bool,
}
impl Drop for AdmittedSourceValidationV1 {
    fn drop(&mut self) {}
}

/// Physical table counts stay separate from the extra verification-work cursor.
/// One ObjectLoad, two row passes, two explicit prefix ends, one Copying control,
/// and one final object-range end. Canonical table phases are untouched.
pub(in crate::startup) fn planned_work(counts: &[u64]) -> Result<u64, StorageError> {
    if counts.len() != source_validation::SOURCE_PHASES {
        return Err(invariant());
    }
    if counts.iter().all(|count| *count == 0) {
        return Ok(0);
    }
    // One AllocatorKV, every Root plus RootEnd, each Object with its
    // separately counted RootLoad, both row passes/prefix ends, MaintenanceLoad,
    // and ObjectEnd. Four fixed Origin control observations precede them.
    // No quota/origin point read is hidden in a population work unit.
    counts[1]
        .checked_mul(4)
        .and_then(|objects| {
            counts[0]
                .checked_mul(2)
                .and_then(|rows| objects.checked_add(rows))
        })
        .and_then(|work| work.checked_add(counts[2]))
        .and_then(|work| work.checked_add(counts[3]))
        .and_then(|work| work.checked_add(7))
        .ok_or_else(limit_exceeded)
}
impl<O: EvidenceScanReadOwnerV1> EvidenceScanCoreV1<O> {
    pub(in crate::startup) fn inspect_source_commitment_work(
        &mut self,
        position: u64,
    ) -> Result<Option<StructuralFinding>, StorageError> {
        if self.clean_close_fast
            || self.bounded_dirty_startup
            || self.source_component_start == self.structural_total
        {
            return Err(invariant());
        }
        let relative = position
            .checked_sub(self.source_component_start)
            .ok_or_else(invariant)?;
        if relative == 0 {
            // All canonical rows have already advanced their strict phase/index
            // cursor. Release its final native range before Source-work ranges.
            self.structural_cursors = None;
        }
        let mut owner = match self.source_components.take() {
            Some(owner) => owner,
            None => {
                if relative != 0 {
                    return Err(invariant());
                }
                AdmittedSourceValidationV1::new(self)?
            }
        };
        // A finding consumed its one attempted visit. A refused continuation
        // cannot manufacture another advancing finding without physical work.
        if owner.data.refused {
            return Err(corrupt());
        }
        let result = owner.data.advance(self, relative);
        match result {
            Ok(()) => {
                self.source_components = Some(owner);
                Ok(None)
            }
            Err(error) if error.kind() == StorageErrorKind::CorruptData => {
                owner.data.refuse();
                self.source_components = Some(owner);
                Ok(Some(authoritative(
                    StructuralFindingCode::CrossLinkMismatch,
                )))
            }
            Err(error) => Err(error),
        }
    }
    pub(in crate::startup) fn verify_source_commitment_work_end(&self) -> Result<(), StorageError> {
        if self.source_component_start == self.structural_total {
            return Ok(());
        }
        let owner = self.source_components.as_ref().ok_or_else(invariant)?;
        if owner.data.refused {
            return Err(corrupt());
        }
        if !owner.data.done
            || owner.data.position
                != self
                    .structural_total
                    .checked_sub(self.source_component_start)
                    .ok_or_else(invariant)?
        {
            return Err(invariant());
        }
        Ok(())
    }
}
impl AdmittedSourceValidationV1 {
    // Only the Core's closed actual walk calls this. A caller cannot choose a
    // read, owner, ledger, detached kernel or a completed Source token.
    fn new<O: EvidenceScanReadOwnerV1>(scan: &EvidenceScanCoreV1<O>) -> Result<Self, StorageError> {
        let counts = &scan.additive_structural_counts[EXISTING_ADDITIVE_STRUCTURAL_TABLE_COUNT..];
        if planned_work(counts)?
            != scan
                .structural_total
                .checked_sub(scan.source_component_start)
                .ok_or_else(invariant)?
        {
            return Err(invariant());
        }
        let maximum = owned_allocation_capacity_bound_v1(
            size_of::<SourceValidationStateV1>(),
            align_of::<SourceValidationStateV1>(),
        )
        .map_err(|_| limit_exceeded())?
        .checked_add(size_of::<Self>() as u64)
        .ok_or_else(limit_exceeded)?;
        let work = scan.shared.admit_source_maintenance_decode(
            usize::try_from(maximum).map_err(|_| limit_exceeded())?,
        )?;
        let data = Box::new(SourceValidationStateV1 {
            kernel: None,
            completed: None,
            quota: SourceQuotaValidationV1::new(),
            origin: crate::native_source_fresh::SourceOriginObservationV1::new(),
            wrong_origin: false,
            objects: None,
            position: 0,
            completed_objects: 0,
            measured_rows: 0,
            maintenance_loads: 0,
            expected: counts.try_into().map_err(|_| invariant())?,
            done: false,
            refused: false,
        });
        Ok(Self { data, _work: work })
    }
}
impl SourceValidationStateV1 {
    fn advance<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
        position: u64,
    ) -> Result<(), StorageError> {
        scan.check_cancellation()?;
        if self.refused {
            return Err(corrupt());
        }
        if self.done || position != self.position {
            return Err(invariant());
        }
        // The attempted physical control/row/end visit owns this work unit even
        // if it yields a corruption finding. A refused state never completes.
        self.position = self.position.checked_add(1).ok_or_else(limit_exceeded)?;
        if !self.origin.complete() {
            scan.shared.observe_source_origin_control(
                SourceCommitmentKernelV1::read(scan)?,
                &mut self.origin,
            )?;
            return Ok(());
        }
        if !self.quota.preamble_complete() {
            self.quota.step_preamble(scan)?;
            return Ok(());
        }
        if let Some(completed) = self.completed.as_ref() {
            // Check reciprocity only AFTER the unchanged row/progress owner
            // completed. A false row root remains the nearest original defect.
            let kernel = self.kernel.as_ref().ok_or_else(invariant)?;
            self.quota.check_completed(scan, kernel)?;
            // Record the local invariant but finish existing reciprocal/quota
            // checks first, preserving their already qualified corruption timing.
            let descriptor = kernel
                .object
                .as_ref()
                .ok_or_else(invariant)?
                .value()
                .descriptor();
            self.wrong_origin |= !self.origin.checked()?.matches_descriptor(descriptor)?;

            self.measured_rows = self
                .measured_rows
                .checked_add(u64::from(kernel.measured_rows))
                .ok_or_else(corrupt)?;
            self.completed_objects = self.completed_objects.checked_add(1).ok_or_else(corrupt)?;
            if matches!(completed, Completion::Copying(_)) {
                self.maintenance_loads =
                    self.maintenance_loads.checked_add(1).ok_or_else(corrupt)?;
            }
            self.completed = None;
            self.kernel = None;
            return Ok(());
        }
        if let Some(kernel) = self.kernel.as_mut() {
            if let Some(completed) =
                kernel.step_completion(scan, EvidencePageLimit::new(1).ok_or_else(invariant)?)?
            {
                self.completed = Some(completed);
            }
            return Ok(());
        }
        if self.objects.is_none() {
            let table = SourceCommitmentKernelV1::read(scan)?
                .open_table(source_validation::definition(1)?)
                .map_err(table_error)?;
            self.objects = Some(table.range::<&[u8]>(..).map_err(precommit_storage_error)?);
        }
        let next = self.objects.as_mut().ok_or_else(invariant)?.next();
        let Some(next) = next else {
            if self.completed_objects != self.expected[1]
                || self.measured_rows != self.expected[0]
                || self.maintenance_loads != self.expected[3]
            {
                return Err(corrupt());
            }
            self.quota.finish(self.expected[2])?;
            if self.wrong_origin {
                return Err(corrupt());
            }
            self.objects = None;
            self.done = true;
            return Ok(());
        };
        let (key, value) = next.map_err(precommit_storage_error)?;
        check_input(key.value(), value.value())?;
        let locator =
            crate::source_keys::decode_source_object_key(key.value()).map_err(|_| corrupt())?;
        if locator.database_id() != scan.database_id {
            return Err(corrupt());
        }
        let object = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_object_caller_decode_v1,
            decode_projection_source_object_with_caller_scratch_v1,
        )?;
        let mode = match object.value().lifecycle() {
            ProjectionSourceLifecycleV1::Published | ProjectionSourceLifecycleV1::Superseded => {
                Mode::PublishedRows
            }
            ProjectionSourceLifecycleV1::Copying => Mode::CopyingProgress,
            // No inferred deleted subset or synthetic abort/recovery completion.
            _ => return Err(storage_error(StorageErrorKind::Unavailable)),
        };
        let object_bytes = add_native(0, key.value(), value.value())?;
        self.quota.observe_object(object.value(), object_bytes)?;
        let mut kernel = SourceCommitmentKernelV1::new_public(locator, mode);
        kernel.accept_object(scan, object, object_bytes)?;
        // The range-loaded object is this step's sole ObjectLoad. Do not reopen
        // it through a second point lookup or hide a prefix/population scan.
        kernel.position = 1;
        kernel.physical_visits = 1;
        self.kernel = Some(kernel);
        Ok(())
    }
    fn refuse(&mut self) {
        self.refused = true;
        self.kernel = None;
        self.completed = None;
        self.quota = SourceQuotaValidationV1::new();
        self.objects = None;
    }
}
