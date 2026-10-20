//! Closed native read resources charged to the actual originating Source ledger.
//! Numerical custody grants no Original, catalog, Source or publication evidence.
#![cfg_attr(not(test), allow(dead_code))]
use super::RedbReadAccess;
use super::source_original_retention::{OriginalCapturedRead, OriginalPrefixRetentionLease};
use crate::error::{
    database_error, precommit_storage_error, storage_error, table_error, transaction_error,
};
use redb::{
    PlannedByteRange, PlannedBytesTable, PlannedKeyValue, PlannedReadOnlyDatabase,
    PlannedReadTransaction, PlannedStrTable, PlannedValue, ReadOnlyReopenError,
    ReadOnlyReopenHandoff,
};
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::ProjectionSourceWorkReservationV1;
use std::sync::Arc;

// Data-only ownership: no EngineOwner, Source lease, callback or authority.
struct NumericAdmission {
    _reservation: ProjectionSourceWorkReservationV1,
}

// Match the existing qualified native SharedPageAllocation protocol. Every
// strong owner uses this private wrapper, with no Weak or raw Arc escape.
struct NumericOwner(Option<Arc<NumericAdmission>>);
impl NumericOwner {
    fn new(reservation: ProjectionSourceWorkReservationV1) -> Self {
        Self(Some(Arc::new(NumericAdmission {
            _reservation: reservation,
        })))
    }
    fn share(&self) -> Result<Self, StorageError> {
        let owner = self.0.as_ref().ok_or_else(invariant)?;
        Ok(Self(Some(Arc::clone(owner))))
    }
    fn aliases(&self) -> Result<usize, StorageError> {
        self.0.as_ref().map(Arc::strong_count).ok_or_else(invariant)
    }
}
impl Drop for NumericOwner {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            // The sole final into_inner deallocates the Arc control before
            // returning the payload; only then can its reservation refund.
            drop(Arc::into_inner(owner));
        }
    }
}

// Physical data owner is deliberately separate from reservation-only NumericOwner.
#[allow(clippy::large_enum_variant)]
enum ActualBaseAccess {
    Existing {
        _access: RedbReadAccess,
    },
    Current {
        _engine:
            riffdb_types::OwnedCapacitySharedV1<crate::engine_owner::EngineOwner<redb::Database>>,
    },
    // One actual closed predecessor session, moved original ledger/worker and
    // retained artifact graph. No independent read/identity token can enter.
    #[cfg(unix)]
    Offline {
        _owner: crate::engine_owner::source_reopen::OfflineSourceReadDataV1,
    },
}
struct ActualBaseReadData {
    _access: ActualBaseAccess,

    // Acts as the root token, preserving the same read/table/range fanout counts.
    _numeric: NumericOwner,
}
struct ActualBaseReadCustody(Option<Arc<ActualBaseReadData>>);
impl ActualBaseReadCustody {
    fn new(access: RedbReadAccess, numeric: NumericOwner) -> Self {
        Self(Some(Arc::new(ActualBaseReadData {
            _access: ActualBaseAccess::Existing { _access: access },
            _numeric: numeric,
        })))
    }
    fn share(&self) -> Result<Self, StorageError> {
        Ok(Self(Some(Arc::clone(
            self.0.as_ref().ok_or_else(invariant)?,
        ))))
    }
}
impl Drop for ActualBaseReadCustody {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            drop(Arc::into_inner(owner));
        }
    }
}
fn share_base(
    base: &Option<ActualBaseReadCustody>,
) -> Result<Option<ActualBaseReadCustody>, StorageError> {
    base.as_ref().map(ActualBaseReadCustody::share).transpose()
}

pub(crate) struct SourceOwnedNativeRead<'source> {
    // Every native table/value/range exclusively borrows this facade. Native
    // data, cache, final-page counter and file custody disappear before refund.
    database: PlannedReadOnlyDatabase,
    // Keeps the actual one-worker claim, original engine and Source ledger live.
    _source: &'source OriginalPrefixRetentionLease,
    _numeric: NumericOwner,
}

struct PendingSourceRead<'source> {
    // Refused admission/open destroys the remaining handoff before its allowance.
    handoff: Option<ReadOnlyReopenHandoff>,
    source: &'source OriginalPrefixRetentionLease,
    numeric: Option<NumericOwner>,
}

impl<'source> SourceOwnedNativeRead<'source> {
    pub(crate) fn open(
        source: &'source OriginalPrefixRetentionLease,
        handoff: ReadOnlyReopenHandoff,
    ) -> Result<Self, StorageError> {
        let mut pending = PendingSourceRead {
            handoff: Some(handoff),
            source,
            numeric: None,
        };
        let peak = pending.handoff.as_ref().ok_or_else(invariant)?.peak_bytes();
        // Actual inline controls coexist during construction. The native quote
        // covers its concrete file-only custody and all native allocations.
        // One of every closed first-party inline wrapper is a conservative
        // envelope even though exclusive borrows prohibit several overlaps.
        // Native quote already owns its own inline/native graphs; charging these
        // whole wrappers again preserves a finite bound without subtracting
        // representations or relying on coincidental layout equivalence.
        let control = source_control_bytes()?;
        let numeric = shared_owner_bound::<NumericAdmission>()?;
        let required = peak
            .checked_add(control)
            .and_then(|bytes| bytes.checked_add(numeric))
            .ok_or_else(unavailable)?;
        let reservation = pending
            .source
            .originating_budget()
            .reserve(required)
            .map_err(|_| unavailable())?;
        pending.numeric = Some(NumericOwner::new(reservation));
        let handoff = pending.handoff.take().ok_or_else(invariant)?;
        // The handoff has already joined the maintenance engine/cache/read pins.
        // It retains the same actual file exclusion, never a pathname-only reopen.
        let database = redb::Database::builder()
            .open_read_only_planned_handoff(handoff)
            .map_err(native_read_error)?;
        let numeric = pending.numeric.take().ok_or_else(invariant)?;
        Ok(Self {
            database,
            _source: source,
            _numeric: numeric,
        })
    }

    pub(crate) fn begin_read(&mut self) -> Result<SourceNativeReadTransaction<'_>, StorageError> {
        require_aliases(&self._numeric, 1)?;
        let numeric = self._numeric.share()?;
        self.database
            .begin_read()
            .map(|inner| SourceNativeReadTransaction {
                inner,
                _source: None,
                _base: None,
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }
}

// Preserve the originating ledger/worker borrow through actual native destructor
// execution. Numeric Arc aliases have no engine backreference or callable API.
// Every native child stores its inner data first and the same numeric owner last,
// so forgotten children leak their actual numerical charge with their native data.
impl Drop for SourceOwnedNativeRead<'_> {
    fn drop(&mut self) {}
}

/// Native errors are destroyed inside every first-party call while the actual
/// Source owner/allowance is borrowed. Only source-free StorageError escapes.
pub(crate) struct SourceNativeReadTransaction<'owner> {
    inner: PlannedReadTransaction<'owner>,
    _source: Option<&'owner OriginalPrefixRetentionLease>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
pub(crate) struct SourceNativeBytesTable<'owner> {
    inner: PlannedBytesTable<'owner>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
pub(crate) struct SourceNativeStrTable<'owner> {
    inner: PlannedStrTable<'owner>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
pub(crate) struct SourceNativeByteRange<'owner> {
    inner: PlannedByteRange<'owner>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
pub(crate) struct SourceNativeKeyValue<'owner> {
    inner: PlannedKeyValue<'owner>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
pub(crate) struct SourceNativeValue<'owner> {
    inner: PlannedValue<'owner>,
    _base: Option<ActualBaseReadCustody>,
    _numeric: NumericOwner,
}
impl SourceNativeReadTransaction<'_> {
    pub(crate) fn optional_bytes_table(
        &mut self,
        name: &str,
    ) -> Result<Option<SourceNativeBytesTable<'_>>, StorageError> {
        require_aliases(&self._numeric, 2)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        match self.inner.open_bytes_table(name) {
            Ok(inner) => Ok(Some(SourceNativeBytesTable {
                inner,
                _base: base,
                _numeric: numeric,
            })),
            Err(ReadOnlyReopenError::Table(redb::TableError::TableDoesNotExist(_))) => Ok(None),
            Err(error) => Err(native_read_error(error)),
        }
    }

    pub(crate) fn open_bytes_table(
        &mut self,
        name: &str,
    ) -> Result<SourceNativeBytesTable<'_>, StorageError> {
        require_aliases(&self._numeric, 2)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .open_bytes_table(name)
            .map(|inner| SourceNativeBytesTable {
                inner,
                _base: base,
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }
    pub(crate) fn open_str_table(
        &mut self,
        name: &str,
    ) -> Result<SourceNativeStrTable<'_>, StorageError> {
        require_aliases(&self._numeric, 2)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .open_str_table(name)
            .map(|inner| SourceNativeStrTable {
                inner,
                _base: base,
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }
}
impl SourceNativeBytesTable<'_> {
    pub(crate) fn get(
        &mut self,
        key: &[u8],
    ) -> Result<Option<SourceNativeValue<'_>>, StorageError> {
        require_aliases(&self._numeric, 3)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .get(key)
            .map(|value| {
                value.map(|inner| SourceNativeValue {
                    inner,
                    _base: base,
                    _numeric: numeric,
                })
            })
            .map_err(native_read_error)
    }
    pub(crate) fn range_from(
        &mut self,
        start: Option<&[u8]>,
    ) -> Result<SourceNativeByteRange<'_>, StorageError> {
        require_aliases(&self._numeric, 3)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .range_from(start)
            .map(|inner| SourceNativeByteRange {
                inner,
                _base: base,
                _numeric: numeric,
            })
            .map_err(native_read_error)
    }
}
impl SourceNativeStrTable<'_> {
    pub(crate) fn get(&mut self, key: &str) -> Result<Option<SourceNativeValue<'_>>, StorageError> {
        require_aliases(&self._numeric, 3)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .get(key)
            .map(|value| {
                value.map(|inner| SourceNativeValue {
                    inner,
                    _base: base,
                    _numeric: numeric,
                })
            })
            .map_err(native_read_error)
    }
}
impl SourceNativeByteRange<'_> {
    pub(crate) fn next(&mut self) -> Result<Option<SourceNativeKeyValue<'_>>, StorageError> {
        require_aliases(&self._numeric, 4)?;
        let numeric = self._numeric.share()?;
        let base = share_base(&self._base)?;
        self.inner
            .next()
            .map(|row| {
                row.map(|inner| SourceNativeKeyValue {
                    inner,
                    _base: base,
                    _numeric: numeric,
                })
            })
            .map_err(native_read_error)
    }
}
impl SourceNativeKeyValue<'_> {
    pub(crate) fn key(&self) -> &[u8] {
        self.inner.key()
    }
    pub(crate) fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
impl SourceNativeValue<'_> {
    pub(crate) fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
// Explicit destructors retain each exclusive parent borrow through physical
// native destruction even when the caller never otherwise uses the wrapper.
impl Drop for SourceNativeReadTransaction<'_> {
    fn drop(&mut self) {}
}
impl Drop for SourceNativeBytesTable<'_> {
    fn drop(&mut self) {}
}
impl Drop for SourceNativeStrTable<'_> {
    fn drop(&mut self) {}
}
impl Drop for SourceNativeByteRange<'_> {
    fn drop(&mut self) {}
}
impl Drop for SourceNativeKeyValue<'_> {
    fn drop(&mut self) {}
}
impl Drop for SourceNativeValue<'_> {
    fn drop(&mut self) {}
}
impl Drop for PendingSourceRead<'_> {
    fn drop(&mut self) {}
}

pub(crate) fn native_read_error(error: ReadOnlyReopenError) -> StorageError {
    match error {
        ReadOnlyReopenError::Native(error) => database_error(error),
        ReadOnlyReopenError::Table(error) => table_error(error),
        ReadOnlyReopenError::Transaction(error) => transaction_error(error),
        ReadOnlyReopenError::Storage(error) => precommit_storage_error(error),
        // Capacity/profile/staleness are typed refusal, never successful absence.
        ReadOnlyReopenError::Capacity
        | ReadOnlyReopenError::Stale
        | ReadOnlyReopenError::Unsupported
        | ReadOnlyReopenError::Overflow => unavailable(),
    }
}
// Exclusive Rust borrows bound normal live children. Safe forget can terminate
// that borrow without destruction; an extra numeric alias therefore refuses
// further native construction rather than growing an unbounded leaked graph.
fn shared_owner_bound<T>() -> Result<u64, StorageError> {
    let bytes = std::mem::size_of::<T>()
        .checked_add(2 * std::mem::size_of::<usize>())
        .ok_or_else(unavailable)?;
    riffdb_types::owned_allocation_capacity_bound_v1(bytes, std::mem::align_of::<T>())
        .map_err(|_| unavailable())
}
fn source_control_bytes() -> Result<u64, StorageError> {
    let mut control = 0_u64;
    for bytes in [
        std::mem::size_of::<SourceOwnedNativeRead<'static>>(),
        std::mem::size_of::<PendingSourceRead<'static>>(),
        std::mem::size_of::<SourceNativeReadTransaction<'static>>(),
        std::mem::size_of::<SourceNativeBytesTable<'static>>(),
        std::mem::size_of::<SourceNativeStrTable<'static>>(),
        std::mem::size_of::<SourceNativeByteRange<'static>>(),
        std::mem::size_of::<SourceNativeKeyValue<'static>>(),
        std::mem::size_of::<SourceNativeValue<'static>>(),
        // Additional same-ledger allowance temporarily coexists during absorb.
        // NumericAdmission's Arc bound already prices the retained allowance.
        std::mem::size_of::<ProjectionSourceWorkReservationV1>(),
    ] {
        control = control
            .checked_add(u64::try_from(bytes).map_err(|_| unavailable())?)
            .ok_or_else(unavailable)?;
    }
    Ok(control)
}

impl OriginalCapturedRead<'_, '_> {
    /// Numerical checkpoint-only reads; Composite overlay semantics remain with
    /// the existing logical Source reader. This grants no Source completeness.
    pub(crate) fn open_native_checkpoint_read<'read>(
        &'read self,
        prefix: &'read OriginalPrefixRetentionLease,
    ) -> Result<SourceNativeReadTransaction<'read>, StorageError> {
        let access = self.checked_existing_native_access(prefix)?;
        let checkpoint = match access {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => root.transaction(),
            RedbReadAccess::Composite(view) => view.checkpoint_root().transaction(),
        };
        // Native-owned pure layout calculation: no root capture, I/O, Arc or
        // transaction registration exists before this actual Source admission.
        let capture =
            redb::ReadTransaction::read_context_plan_capture_bytes().map_err(native_read_error)?;
        let fixed = source_control_bytes()?
            .checked_add(shared_owner_bound::<NumericAdmission>()?)
            .and_then(|bytes| bytes.checked_add(shared_owner_bound::<ActualBaseReadData>().ok()?))
            .ok_or_else(unavailable)?;
        let initial = fixed.checked_add(capture).ok_or_else(unavailable)?;
        let mut reservation = prefix
            .originating_budget()
            .reserve(initial)
            .map_err(|_| unavailable())?;
        // A failed plan's native error is mapped and destroyed while its capture
        // allowance lives. Never use the unrelated metadata scratch as credit.
        let plan = checkpoint.plan_read_context().map_err(native_read_error)?;
        let additional = plan
            .peak_bytes()
            .max(capture)
            .checked_sub(capture)
            .ok_or_else(invariant)?;
        if additional != 0 {
            let extra = prefix
                .originating_budget()
                .reserve(additional)
                .map_err(|_| unavailable())?;
            reservation.absorb(extra).map_err(|_| unavailable())?;
        }
        let numeric = NumericOwner::new(reservation);
        // Derived Clone for RedbReadAccess clones only its actual Arc variant.
        // The SAME actual composite/base/root/engine survives forgotten children.
        let base = ActualBaseReadCustody::new(access.clone(), numeric.share()?);
        let inner = checkpoint
            .open_planned_read_context(plan)
            .map_err(native_read_error)?;
        Ok(SourceNativeReadTransaction {
            inner,
            _source: Some(prefix),
            _base: Some(base),
            _numeric: numeric,
        })
    }
}

fn require_aliases(owner: &NumericOwner, expected: usize) -> Result<(), StorageError> {
    if owner.aliases()? == expected {
        Ok(())
    } else {
        Err(unavailable())
    }
}
fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

#[path = "source_native_read/first_entry.rs"]
mod first_entry;
pub(in crate::store) use first_entry::OriginalNativeBase;

#[cfg(test)]
#[path = "source_native_read/error_tests.rs"]
mod tests;

#[cfg(unix)]
#[path = "source_native_read/offline.rs"]
pub(in crate::store) mod offline;
#[cfg(unix)]
pub(crate) use offline::{
    OfflineKernelConstructionV1, SourceOwnedOfflineReadV1, offline_source_control_bytes,
};
