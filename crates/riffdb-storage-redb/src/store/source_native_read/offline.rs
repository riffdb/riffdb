//! Fixed consuming offline resource facade over the ONE canonical read kernel.
//! It is numerical/artifact custody, never Source/D4/cut publication authority.
#![cfg_attr(not(test), allow(dead_code))]
use super::*;
use crate::engine_owner::source_reopen::{OfflineSourceReadDataV1, OpenedPredecessorSourceReadV1};

pub(crate) struct SourceOwnedOfflineReadV1 {
    // Native facade/cache/lock dies before actual immutable descriptors/domain.
    database: PlannedReadOnlyDatabase,
    // Same-base physical pair only; no configured-path/metadata authority.
    auxiliary: Option<PlannedReadOnlyDatabase>,
    // This is the sole numerical root token; every child retains this same data.
    base: ActualBaseReadCustody,
}

/// Fields-private, single-use construction permit. Only from_opened below can
/// mint it; no Clone/Default/constructor/raw tuple-return operation exists.
pub(crate) struct OfflineKernelConstructionV1 {
    _private: (),
}
impl OfflineKernelConstructionV1 {
    // A caller needs the actual consumed permit. The sole mint passes it directly
    // into the sealed opened owner's consuming method, which returns this fixed
    // facade; no raw native parts or independently selected custody can escape.
    pub(crate) fn bind(
        self,
        database: PlannedReadOnlyDatabase,
        data: OfflineSourceReadDataV1,
        reservation: ProjectionSourceWorkReservationV1,
    ) -> SourceOwnedOfflineReadV1 {
        let numeric = NumericOwner::new(reservation);
        let base = ActualBaseReadCustody(Some(Arc::new(ActualBaseReadData {
            _access: ActualBaseAccess::Offline { _owner: data },
            _numeric: numeric,
        })));
        SourceOwnedOfflineReadV1 {
            database,
            auxiliary: None,
            base,
        }
    }
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) struct ConfiguredPairAttachFailureV1 {
    primary: SourceOwnedOfflineReadV1,
    continuation: crate::maintenance::ConfiguredDerivedContinuationV1,
    error: StorageError,
}
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
impl SourceOwnedOfflineReadV1 {
    pub(crate) fn configured_pair_capture_bytes() -> Result<u64, StorageError> {
        u64::try_from(std::mem::size_of::<ConfiguredPairAttachFailureV1>())
            .ok()
            .and_then(|bytes| {
                bytes.checked_add(
                    std::mem::size_of::<Result<Self, ConfiguredPairAttachFailureV1>>() as u64,
                )
            })
            .ok_or_else(unavailable)
    }
    #[allow(clippy::result_large_err)]
    pub(crate) fn attach_configured_derived(
        mut self,
        mut continuation: crate::maintenance::ConfiguredDerivedContinuationV1,
    ) -> Result<Self, ConfiguredPairAttachFailureV1> {
        let result = (|| {
            if self.auxiliary.is_some() {
                return Err(unavailable());
            }
            let base = self.base.0.as_mut().ok_or_else(invariant)?;
            let root = Arc::get_mut(base).ok_or_else(unavailable)?;
            let ActualBaseAccess::Offline { _owner } = &mut root._access else {
                return Err(invariant());
            };
            self.auxiliary = continuation.attach_to(_owner)?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(self),
            Err(error) => Err(ConfiguredPairAttachFailureV1 {
                primary: self,
                continuation,
                error,
            }),
        }
    }
    pub(crate) fn configured_derived_is_absent(&self) -> Result<bool, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.configured_derived_is_absent()
    }
}
#[cfg(all(target_os = "linux", target_arch = "x86_64", test))]
impl ConfiguredPairAttachFailureV1 {
    pub(crate) fn into_test_parts(self) -> (SourceOwnedOfflineReadV1, StorageError) {
        let Self {
            primary,
            continuation,
            error,
        } = self;
        drop(continuation);
        (primary, error)
    }
}

impl SourceOwnedOfflineReadV1 {
    pub(in crate::store) fn original_history_admission(
        &self,
    ) -> Result<(riffdb_types::ProjectionSourceWorkingBudgetV1, u64), StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.check_cancellation()?;
        require_aliases(&root._numeric, 1)?;
        Ok((
            _owner.originating_budget().clone(),
            _owner.source_original_history_allocation_bytes()?,
        ))
    }
    pub(in crate::store) fn construct_original_history(
        &self,
        construction: crate::store::SourceOriginalHistoryConstructionV1,
    ) -> Result<crate::history_stub::SourceOriginalHistoryV1, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.check_cancellation()?;
        if !construction
            .budget()
            .shares_owner_with(_owner.originating_budget())
        {
            return Err(invariant());
        }
        _owner.source_original_history(construction)
    }

    pub(crate) fn from_opened(owner: OpenedPredecessorSourceReadV1) -> Self {
        owner.into_kernel(OfflineKernelConstructionV1 { _private: () })
    }

    #[cfg(test)]
    pub(crate) fn originating_budget_for_test(
        &self,
    ) -> &riffdb_types::exact_provider_budget_v1::ProjectionSourceWorkingBudgetV1 {
        let root = self.base.0.as_ref().expect("fixed offline owner");
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            panic!("fixed offline owner");
        };
        _owner.originating_budget_for_test()
    }

    // Only physical custody is joined. A future actual operation producer must
    // additionally prove the configured sidecar and prepay INITIAL maintenance
    // construction; no such cold constructor or metadata-reader grant exists.
    pub(crate) fn attach_prepared_auxiliary_read(
        &mut self,
        handoff: ReadOnlyReopenHandoff,
    ) -> Result<(), StorageError> {
        if self.auxiliary.is_some() {
            return Err(unavailable());
        }
        let base = self.base.0.as_mut().ok_or_else(invariant)?;
        let root = Arc::get_mut(base).ok_or_else(unavailable)?;
        let ActualBaseAccess::Offline { _owner } = &mut root._access else {
            return Err(invariant());
        };
        let database = _owner.open_auxiliary_handoff(handoff)?;
        self.auxiliary = Some(database);
        Ok(())
    }
    pub(crate) fn begin_auxiliary_read(
        &mut self,
    ) -> Result<SourceNativeReadTransaction<'_>, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.check_cancellation()?;
        require_aliases(&root._numeric, 1)?;
        let numeric = root._numeric.share()?;
        let base = self.base.share()?;
        self.auxiliary
            .as_mut()
            .ok_or_else(unavailable)?
            .begin_read()
            .map(|inner| SourceNativeReadTransaction {
                inner,
                _source: None,
                _base: Some(base),
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }

    pub(crate) fn begin_read(&mut self) -> Result<SourceNativeReadTransaction<'_>, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.check_cancellation()?;
        require_aliases(&root._numeric, 1)?;
        let numeric = root._numeric.share()?;
        let base = self.base.share()?;
        self.database
            .begin_read()
            .map(|inner| SourceNativeReadTransaction {
                inner,
                _source: None,
                _base: Some(base),
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }
}
// Keep the original owner borrow live through physical Native destruction.
impl Drop for SourceOwnedOfflineReadV1 {
    fn drop(&mut self) {}
}

pub(crate) fn offline_source_control_bytes() -> Result<u64, StorageError> {
    // The existing kernel quote includes all read/table/range/value/row controls.
    // Its ActualBase block now contains the actual fixed Offline payload, not a
    // pointer-only marker. All native/descriptor dynamic bytes are additional.
    source_control_bytes()?
        .checked_add(crate::engine_owner::source_reopen::auxiliary_read_capture_control_bytes()?)
        .ok_or_else(unavailable)?
        .checked_add(
            u64::try_from(std::mem::size_of::<SourceOwnedOfflineReadV1>())
                .map_err(|_| unavailable())?,
        )
        .and_then(|bytes| bytes.checked_add(shared_owner_bound::<ActualBaseReadData>().ok()?))
        .and_then(|bytes| bytes.checked_add(shared_owner_bound::<NumericAdmission>().ok()?))
        .ok_or_else(unavailable)
}

/// Opaque actual offline descriptor/domain custody, minted only from this
/// facade. Sharing its already-owned backing performs no allocation and does
/// not add a native numeric fanout alias. No raw access, Clone or ID adoption.
#[cfg(feature = "projection-source-driver")]
pub(in crate::store) struct OfflineSourceSessionCustodyV1 {
    base: ActualBaseReadCustody,
}
#[cfg(feature = "projection-source-driver")]
impl OfflineSourceSessionCustodyV1 {
    pub(in crate::store) fn share_for_catalog(&self) -> Result<Self, StorageError> {
        Ok(Self {
            base: self.base.share()?,
        })
    }
    pub(in crate::store) fn originating_budget(
        &self,
    ) -> Result<&riffdb_types::ProjectionSourceWorkingBudgetV1, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        Ok(_owner.originating_budget())
    }
    pub(in crate::store) fn matches_session(&self, other: &Self) -> bool {
        match (self.base.0.as_ref(), other.base.0.as_ref()) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    pub(in crate::store) fn matches_facade(&self, facade: &SourceOwnedOfflineReadV1) -> bool {
        match (self.base.0.as_ref(), facade.base.0.as_ref()) {
            (Some(actual), Some(captured)) => Arc::ptr_eq(actual, captured),
            _ => false,
        }
    }
    pub(in crate::store) fn matches_read(&self, read: &SourceNativeReadTransaction<'_>) -> bool {
        match (
            self.base.0.as_ref(),
            read._base.as_ref().and_then(|base| base.0.as_ref()),
        ) {
            (Some(actual), Some(captured)) => Arc::ptr_eq(actual, captured),
            _ => false,
        }
    }
}
#[cfg(feature = "projection-source-driver")]
impl SourceOwnedOfflineReadV1 {
    pub(in crate::store) fn original_session_custody(
        &self,
    ) -> Result<OfflineSourceSessionCustodyV1, StorageError> {
        let root = self.base.0.as_ref().ok_or_else(invariant)?;
        let ActualBaseAccess::Offline { _owner } = &root._access else {
            return Err(invariant());
        };
        _owner.check_cancellation()?;
        require_aliases(&root._numeric, 1)?;
        Ok(OfflineSourceSessionCustodyV1 {
            base: self.base.share()?,
        })
    }
}
