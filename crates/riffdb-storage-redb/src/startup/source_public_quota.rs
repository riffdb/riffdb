//! Bounded reciprocal Root/lifecycle and exact existing native quota checks.
//! This observation never installs a usage-domain fact or grants Source closure.
use super::*;
use riffdb_storage_api::proto_codec::{
    decode_projection_checkpoint_ordinal_allocator_inline_v1,
    decode_projection_source_root_with_caller_scratch_v1,
    inspect_projection_source_root_caller_decode_v1,
};
use riffdb_storage_api::{ProjectionSourceRootV1, ProjectionSourceTableUsageV1};
type RootOwner = source_validation_admission::MaintenanceDecodedSourceV1<ProjectionSourceRootV1>;
#[derive(Clone, Copy, Eq, PartialEq)]
enum QuotaPhase {
    Allocator,
    Roots,
    Objects,
}
pub(super) struct SourceQuotaValidationV1 {
    // Charged previous DTO provides constant-space adjacent canonical identity
    // uniqueness. It and its native range die before object-prefix work begins.
    previous_root: Option<RootOwner>,
    roots: Option<Range<'static, &'static [u8], &'static [u8]>>,
    phase: QuotaPhase,
    roots_seen: u16,
    selected_roots: u16,
    selected_bytes: u64,
    total_bytes: u64,
    allocator_bytes: u64,
    candidate: Option<(u64, u64)>,
}
impl SourceQuotaValidationV1 {
    pub(super) fn new() -> Self {
        Self {
            previous_root: None,
            roots: None,
            phase: QuotaPhase::Allocator,
            roots_seen: 0,
            selected_roots: 0,
            selected_bytes: 0,
            total_bytes: 0,
            allocator_bytes: 0,
            candidate: None,
        }
    }
    pub(super) fn preamble_complete(&self) -> bool {
        self.phase == QuotaPhase::Objects
    }
    pub(super) fn step_preamble<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
    ) -> Result<(), StorageError> {
        match self.phase {
            QuotaPhase::Allocator => {
                let read = SourceCommitmentKernelV1::read(scan)?;
                let table = read.open_table(META).map_err(table_error)?;
                let key = riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
                    .metadata_key()
                    .ok_or_else(invariant)?;
                let value = table
                    .get(key)
                    .map_err(precommit_storage_error)?
                    .ok_or_else(corrupt)?;
                // Existing exact stack-only canonical decoder; no Source ledger.
                // The scalar's presence is physical charge, not original/fork
                // validity for any Object. That remains the origin closure task.
                decode_projection_checkpoint_ordinal_allocator_inline_v1(value.value())
                    .map_err(|_| corrupt())?;
                self.allocator_bytes = add_native(0, key.as_bytes(), value.value())?;
                self.total_bytes = self.allocator_bytes;
                self.phase = QuotaPhase::Roots;
            }
            QuotaPhase::Roots => {
                if self.roots.is_none() {
                    match SourceCommitmentKernelV1::read(scan)?
                        .open_table(source_validation::definition(2)?)
                    {
                        Ok(table) => {
                            self.roots =
                                Some(table.range::<&[u8]>(..).map_err(precommit_storage_error)?)
                        }
                        Err(redb::TableError::TableDoesNotExist(_)) => {}
                        Err(error) => return Err(table_error(error)),
                    }
                }
                let next = self.roots.as_mut().and_then(Iterator::next);
                let Some(next) = next else {
                    self.previous_root = None;
                    self.roots = None;
                    self.phase = QuotaPhase::Objects;
                    return Ok(());
                };
                let (key, value) = next.map_err(precommit_storage_error)?;
                check_input(key.value(), value.value())?;
                let root = source_validation_admission::decode(
                    &scan.shared,
                    value.value(),
                    inspect_projection_source_root_caller_decode_v1,
                    decode_projection_source_root_with_caller_scratch_v1,
                )?;
                crate::source_keys::check_source_root_key(
                    key.value(),
                    root.value().reference().identity(),
                    root.value().reference().locator(),
                )
                .map_err(|_| corrupt())?;
                if root.value().reference().locator().database_id() != scan.database_id
                    || self.previous_root.as_ref().is_some_and(|previous| {
                        previous.value().reference().identity()
                            == root.value().reference().identity()
                    })
                {
                    return Err(corrupt());
                }
                self.roots_seen = self.roots_seen.checked_add(1).ok_or_else(corrupt)?;
                if self.roots_seen > riffdb_storage_api::MAX_PROJECTION_SOURCE_ROOTS {
                    return Err(corrupt());
                }
                self.total_bytes = add_native(self.total_bytes, key.value(), value.value())?;
                self.previous_root = Some(root);
            }
            QuotaPhase::Objects => return Err(invariant()),
        }
        self.check_running_caps()
    }
    pub(super) fn observe_object(
        &mut self,
        object: &ProjectionSourceObjectV1,
        bytes: u64,
    ) -> Result<(), StorageError> {
        crate::source_charge::object_charge(object.descriptor().canonical_row_bytes(), bytes)
            .map_err(|_| corrupt())?;
        self.total_bytes = self.total_bytes.checked_add(bytes).ok_or_else(corrupt)?;
        self.check_running_caps()
    }
    pub(super) fn check_completed<O: EvidenceScanReadOwnerV1>(
        &mut self,
        scan: &EvidenceScanCoreV1<O>,
        object: &SourceCommitmentKernelV1,
    ) -> Result<(), StorageError> {
        if object.phase != Phase::Done {
            return Err(invariant());
        }
        let actual = object.object.as_ref().ok_or_else(invariant)?.value();
        let descriptor = actual.descriptor();
        // Global canonical Root sweep already proved one key per identity. This
        // separately counted single prefix visit selects that same pinned Root.
        let (root, root_bytes) = self.root_for_identity(scan, descriptor.reference().identity())?;
        match actual.lifecycle() {
            ProjectionSourceLifecycleV1::Published => {
                let root = root.as_ref().ok_or_else(corrupt)?;
                if !root.value().matches_published_object(actual) {
                    return Err(corrupt());
                }
                self.selected_roots = self.selected_roots.checked_add(1).ok_or_else(corrupt)?;
                self.selected_bytes = self
                    .selected_bytes
                    .checked_add(
                        crate::source_charge::selected_charge(
                            descriptor.canonical_row_bytes(),
                            object.object_native_bytes,
                            root_bytes,
                        )
                        .map_err(|_| corrupt())?,
                    )
                    .ok_or_else(corrupt)?;
            }
            ProjectionSourceLifecycleV1::Superseded => {
                if root.as_ref().is_some_and(|root| {
                    root.value().reference().locator() == descriptor.reference().locator()
                }) {
                    return Err(corrupt());
                }
            }
            ProjectionSourceLifecycleV1::Copying => {
                let progress = object.expected_progress.as_ref().ok_or_else(corrupt)?;
                if progress.value().expected_prior_root() != root.as_ref().map(|root| root.value())
                    || self.candidate.is_some()
                {
                    return Err(corrupt());
                }
                self.candidate = Some(
                    crate::source_charge::candidate_charge(
                        descriptor.reference(),
                        descriptor.canonical_row_bytes(),
                        object.object_native_bytes,
                        object.measured_native_bytes,
                        object.maintenance_native_bytes,
                        self.allocator_bytes,
                    )
                    .map_err(charge_error)?,
                );
            }
            _ => return Err(storage_error(StorageErrorKind::Unavailable)),
        }
        self.total_bytes = self
            .total_bytes
            .checked_add(object.measured_native_bytes)
            .and_then(|total| total.checked_add(object.maintenance_native_bytes))
            .ok_or_else(corrupt)?;
        self.check_running_caps()
    }
    fn root_for_identity<O: EvidenceScanReadOwnerV1>(
        &self,
        scan: &EvidenceScanCoreV1<O>,
        identity: &riffdb_types::ProjectionIdentity,
    ) -> Result<(Option<RootOwner>, u64), StorageError> {
        let capacity = crate::source_keys::source_root_key_len(identity);
        if capacity > riffdb_types::MAX_KEY_BYTES {
            return Err(corrupt());
        }
        let bytes = owned_allocation_capacity_bound_v1(capacity, align_of::<u8>())
            .map_err(|_| limit_exceeded())?
            .checked_add(size_of::<Vec<u8>>() as u64)
            .ok_or_else(limit_exceeded)?;
        let _work = scan.shared.admit_source_maintenance_decode(
            usize::try_from(bytes).map_err(|_| limit_exceeded())?,
        )?;
        let mut prefix = Vec::new();
        prefix
            .try_reserve_exact(capacity)
            .map_err(|_| limit_exceeded())?;
        if owned_allocation_capacity_bound_v1(prefix.capacity(), align_of::<u8>())
            .map_err(|_| limit_exceeded())?
            > bytes - size_of::<Vec<u8>>() as u64
        {
            return Err(limit_exceeded());
        }
        crate::source_keys::encode_source_root_identity_prefix_into(&mut prefix, identity)
            .map_err(|_| corrupt())?;
        let table = match SourceCommitmentKernelV1::read(scan)?
            .open_table(source_validation::definition(2)?)
        {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok((None, 0)),
            Err(error) => return Err(table_error(error)),
        };
        let mut range = table
            .range::<&[u8]>((Included(prefix.as_slice()), std::ops::Bound::Unbounded))
            .map_err(precommit_storage_error)?;
        let Some(next) = range.next() else {
            return Ok((None, 0));
        };
        let (key, value) = next.map_err(precommit_storage_error)?;
        if !key.value().starts_with(&prefix) {
            return Ok((None, 0));
        }
        check_input(key.value(), value.value())?;
        let root = source_validation_admission::decode(
            &scan.shared,
            value.value(),
            inspect_projection_source_root_caller_decode_v1,
            decode_projection_source_root_with_caller_scratch_v1,
        )?;
        crate::source_keys::check_source_root_key(
            key.value(),
            identity,
            root.value().reference().locator(),
        )
        .map_err(|_| corrupt())?;
        Ok((Some(root), add_native(0, key.value(), value.value())?))
    }
    pub(super) fn finish(&self, expected_roots: u64) -> Result<(), StorageError> {
        if self.phase != QuotaPhase::Objects
            || u64::from(self.roots_seen) != expected_roots
            || self.selected_roots != self.roots_seen
        {
            return Err(corrupt());
        }
        let mut total = self.total_bytes;
        if let Some((full, actual)) = self.candidate {
            total = total
                .checked_add(full.checked_sub(actual).ok_or_else(corrupt)?)
                .ok_or_else(corrupt)?;
        }
        ProjectionSourceTableUsageV1::new(
            self.selected_roots,
            self.selected_bytes,
            total,
            self.candidate.map(|(full, _)| full),
        )
        .map_err(|_| corrupt())?;
        Ok(())
    }
    fn check_running_caps(&self) -> Result<(), StorageError> {
        if self.total_bytes > riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES
            || self.selected_bytes > riffdb_storage_api::MAX_PROJECTION_SELECTED_SOURCE_BYTES
        {
            return Err(corrupt());
        }
        Ok(())
    }
}
fn charge_error(error: crate::source_charge::SourceChargeErrorV1) -> StorageError {
    match error {
        crate::source_charge::SourceChargeErrorV1::Capacity => corrupt(),
        // Preserve the original1808 producing-bound failure classification.
        // This helper's valid closed references are proven below1KiB in1779;
        // it is distinct from the pending Maintenance last-key capacity issue.
        crate::source_charge::SourceChargeErrorV1::Codec(error) => crate::error::codec_error(error),
    }
}
