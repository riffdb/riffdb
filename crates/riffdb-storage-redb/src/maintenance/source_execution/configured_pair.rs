//! Actual configured child custody only; no Source acquisition/seed authority.
use super::*;
use crate::engine_owner::source_reopen::OfflineSourceReadDataV1;
use crate::maintenance::path_guard::{
    PinnedDirectory, SourceChildIdentityV1, SourceParentVerificationWorkV1,
};
use crate::source_execution_domain::{
    ConfiguredSourceExecutionDomainV1, SourceOperationPhysicalCustodyV1,
};
use riffdb_types::ProjectionSourceWorkReservationV1;
use std::ffi::CString;
use std::fs::File;

#[derive(Clone, Copy)]
enum ChildState {
    Present(SourceChildIdentityV1),
    Absent,
}
// Concrete directory/name and original operation precede the last reservation.
// ClaimData contains no engine/base backedge; this cannot form a read cycle.
pub(crate) struct ConfiguredDerivedBindingV1 {
    parent: PinnedDirectory,
    name: CString,
    state: ChildState,
    domain: ConfiguredSourceExecutionDomainV1,
    operation: SourceOperationPhysicalCustodyV1,
    _work: ProjectionSourceWorkReservationV1,
}
impl ConfiguredDerivedBindingV1 {
    fn require_current_with(
        &self,
        verification: &mut SourceParentVerificationWorkV1,
    ) -> Result<(), StorageError> {
        self.operation.require_current()?;
        self.parent.verify_source_with(verification)?;
        match self.state {
            ChildState::Present(identity) => {
                if !self
                    .parent
                    .source_child_still_matches(&self.name, identity)?
                {
                    return Err(unavailable());
                }
            }
            ChildState::Absent => {
                if self
                    .parent
                    .source_regular_file_length(&self.name)?
                    .is_some()
                {
                    return Err(unavailable());
                }
            }
        }
        self.operation.require_current()
    }
    fn require_current(&self) -> Result<(), StorageError> {
        self.operation.require_current()?;
        let mut verification =
            SourceParentVerificationWorkV1::reserve_for(&self.parent, self.domain.budget())?;
        self.require_current_with(&mut verification)
    }
    pub(crate) fn require_domain(
        &self,
        domain: &crate::source_working_domain::SourceWorkingDomain,
    ) -> Result<(), StorageError> {
        domain.require_same_operation(&self.operation)?;
        self.require_current()
    }
    pub(crate) fn require_absent(&self) -> Result<bool, StorageError> {
        self.require_current()?;
        Ok(matches!(self.state, ChildState::Absent))
    }
}
// A native File is held BEFORE the binding's resource charge on every error.
struct ConfiguredDerivedAcquisitionV1 {
    file: Option<File>,
    binding: Option<ConfiguredDerivedBindingV1>,
}
// No exposed enum fields, raw handoff getter, parsed-engine parameter or Clone.
pub(crate) struct ConfiguredDerivedContinuationV1 {
    handoff: Option<redb::ReadOnlyReopenHandoff>,
    binding: Option<ConfiguredDerivedBindingV1>,
}
impl SourceOwnedOperationV1 {
    fn configured_derived_retained_capture_bytes(&self) -> Result<u64, StorageError> {
        self.claim.require_current()?;
        let name = self
            .owner
            .database_file
            .file_name()
            .ok_or_else(unavailable)?;
        let len = name
            .len()
            .checked_add(".derived".len())
            .and_then(|len| len.checked_add(1))
            .ok_or_else(unavailable)?;
        if len > 4096 {
            return Err(unavailable());
        }
        self.owner
            .database_parent_guard
            .source_retained_clone_bytes()?
            .checked_add(
                riffdb_types::owned_allocation_capacity_bound_v1(len, 1)
                    .map_err(|_| unavailable())?,
            )
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<ConfiguredDerivedBindingV1>() as u64)
            })
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<ConfiguredDerivedAcquisitionV1>() as u64)
            })
            .and_then(|bytes| {
                bytes.checked_add(std::mem::size_of::<ConfiguredDerivedContinuationV1>() as u64)
            })
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Vec<u8>>() as u64))
            .and_then(|bytes| {
                bytes.checked_add((3 * std::mem::size_of::<rustix::fs::Stat>()) as u64)
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    crate::store::SourceOwnedOfflineReadV1::configured_pair_capture_bytes().ok()?,
                )
            })
            .ok_or_else(unavailable)
    }
    fn configured_derived_acquisition_bytes(&self) -> Result<(u64, u64, u64), StorageError> {
        let retained = self.configured_derived_retained_capture_bytes()?;
        let verification = self
            .owner
            .database_parent_guard
            .source_parent_verification_bytes()?;
        let total = retained.checked_add(verification).ok_or_else(unavailable)?;
        Ok((total, retained, verification))
    }
    fn configured_derived_acquisition(
        &self,
    ) -> Result<ConfiguredDerivedAcquisitionV1, StorageError> {
        self.claim.require_current()?;
        if self.cancellation.load(std::sync::atomic::Ordering::Acquire) {
            return Err(unavailable());
        }
        // Preadmit BOTH retained capture and complete parent-verification work
        // on the SAME original Source ledger before any buffer or clone is allocated.
        let (total_bytes, _retained_bytes, verification_bytes) =
            self.configured_derived_acquisition_bytes()?;
        let mut work = self
            .claim
            .domain()
            .budget()
            .reserve(total_bytes)
            .map_err(|_| unavailable())?;
        let verification_work = work.split(verification_bytes).map_err(|_| unavailable())?;
        let mut verification = SourceParentVerificationWorkV1::from_reservation(
            &self.owner.database_parent_guard,
            verification_work,
        )?;
        let mut acquired = ConfiguredDerivedAcquisitionV1 {
            file: None,
            binding: None,
        };
        let parent = self
            .owner
            .database_parent_guard
            .source_retained_clone_with(&mut verification)?;
        let source_name = self
            .owner
            .database_file
            .file_name()
            .ok_or_else(unavailable)?;
        let len = source_name
            .len()
            .checked_add(".derived".len())
            .and_then(|len| len.checked_add(1))
            .ok_or_else(unavailable)?;
        let mut name_bytes = Vec::new();
        name_bytes
            .try_reserve_exact(len)
            .map_err(|_| unavailable())?;
        name_bytes.extend_from_slice(source_name.as_encoded_bytes());
        name_bytes.extend_from_slice(b".derived");
        name_bytes.push(0);
        let name = CString::from_vec_with_nul(name_bytes).map_err(|_| unavailable())?;
        // This is the exact spelling of store::derived_store_path. The caller
        // supplies neither path nor parsed engine; all lookup uses this parent.
        let state = if parent.source_regular_file_length(&name)?.is_some() {
            let file = parent.source_open_file(&name)?;
            let identity = parent.capture_source_child_identity(&name, &file)?;
            acquired.file = Some(file);
            ChildState::Present(identity)
        } else {
            ChildState::Absent
        };
        acquired.binding = Some(ConfiguredDerivedBindingV1 {
            parent,
            name,
            state,
            domain: self.claim.domain().clone(),
            operation: self.claim.retain_physical_custody()?,
            _work: work,
        });
        acquired
            .binding
            .as_ref()
            .ok_or_else(unavailable)?
            .require_current_with(&mut verification)?;
        drop(verification);
        if self.cancellation.load(std::sync::atomic::Ordering::Acquire) {
            return Err(unavailable());
        }
        Ok(acquired)
    }
    // Real absence is physically distinct from Present. Present remains gated on
    // Native's prepaid INITIAL maintenance constructor; neither is wired into
    // production Source execution until all owning producers qualify.
    fn configured_absent_derived(&self) -> Result<ConfiguredDerivedContinuationV1, StorageError> {
        let mut acquired = self.configured_derived_acquisition()?;
        let binding = acquired.binding.as_ref().ok_or_else(unavailable)?;
        if acquired.file.is_some() || !binding.require_absent()? {
            return Err(unavailable());
        }
        Ok(ConfiguredDerivedContinuationV1 {
            handoff: None,
            binding: acquired.binding.take(),
        })
    }
}
impl ConfiguredDerivedContinuationV1 {
    pub(crate) fn attach_to(
        &mut self,
        owner: &mut OfflineSourceReadDataV1,
    ) -> Result<Option<redb::PlannedReadOnlyDatabase>, StorageError> {
        let binding = self.binding.as_ref().ok_or_else(unavailable)?;
        owner.require_configured_binding(binding)?;
        let database = match binding.state {
            ChildState::Present(_) => {
                let handoff = self.handoff.take().ok_or_else(unavailable)?;
                Some(owner.open_auxiliary_handoff(handoff)?)
            }
            ChildState::Absent => {
                if self.handoff.is_some() {
                    return Err(unavailable());
                }
                None
            }
        };
        if let Err(error) = binding.require_domain_from_owner(owner) {
            // Native database FIRST, then its adopted native-core retainer. The
            // continuation still retains parent/claim/work on this refusal.
            drop(database);
            owner.drop_failed_configured_auxiliary();
            return Err(error);
        }
        owner.install_configured_binding(self.binding.take().ok_or_else(unavailable)?);
        Ok(database)
    }
}
impl ConfiguredDerivedBindingV1 {
    fn require_domain_from_owner(
        &self,
        owner: &OfflineSourceReadDataV1,
    ) -> Result<(), StorageError> {
        owner.require_configured_binding(self)
    }
}

#[cfg(test)]
impl SourceOwnedOperationV1 {
    pub(crate) fn cancel_configured_pair_for_test(&self) {
        self.cancellation
            .store(true, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn configured_derived_bytes_for_test(&self) -> Result<u64, StorageError> {
        self.configured_derived_acquisition_bytes()
            .map(|(total, _, _)| total)
    }
    pub(crate) fn configured_derived_retained_capture_bytes_for_test(
        &self,
    ) -> Result<u64, StorageError> {
        self.configured_derived_retained_capture_bytes()
    }
    pub(crate) fn configured_derived_verification_bytes_for_test(
        &self,
    ) -> Result<u64, StorageError> {
        self.owner
            .database_parent_guard
            .source_parent_verification_bytes()
    }
    pub(crate) fn configured_absent_derived_for_test(
        &self,
    ) -> Result<ConfiguredDerivedContinuationV1, StorageError> {
        self.configured_absent_derived()
    }
    pub(crate) fn configured_derived_check_file_for_test(
        &self,
        file: &File,
    ) -> Result<(), StorageError> {
        let acquired = self.configured_derived_acquisition()?;
        let binding = acquired.binding.as_ref().ok_or_else(unavailable)?;
        // Genuine different-sibling descriptors fail the SAME production binding
        // check before any native parser/plan/planned-open is entered.
        binding
            .parent
            .capture_source_child_identity(&binding.name, file)
            .map(|_| ())
    }
    // Ordinary diagnostic maintenance parsing ONLY. The SAME file opened by
    // the fixed producer is consumed by native, never a caller-selected handoff.
    // This test producer is NOT a Source-funded initial cold construction path.
    pub(crate) fn configured_present_derived_for_test(
        &self,
    ) -> Result<ConfiguredDerivedContinuationV1, StorageError> {
        let mut acquired = self.configured_derived_acquisition()?;
        let binding = acquired.binding.as_ref().ok_or_else(unavailable)?;
        binding.require_current()?;
        let file = acquired.file.take().ok_or_else(unavailable)?;
        let mut builder = redb::Database::builder();
        builder.set_cache_size(0);
        let database = builder
            .open_read_only_maintenance_file(file)
            .map_err(crate::store::native_read_error)?;
        let plan = database
            .plan_read_only_reopen()
            .map_err(crate::store::native_read_error)?;
        let handoff = database
            .into_read_only_reopen_handoff(plan)
            .map_err(crate::store::native_read_error)?;
        acquired
            .binding
            .as_ref()
            .ok_or_else(unavailable)?
            .require_current()?;
        Ok(ConfiguredDerivedContinuationV1 {
            handoff: Some(handoff),
            binding: acquired.binding.take(),
        })
    }
}
