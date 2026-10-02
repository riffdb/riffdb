//! Admitted bounded byte rows from one captured checkpoint and overlay.
// req: OQ-040
use super::RedbReadAccess;
use crate::error::{precommit_storage_error, storage_error};
use crate::journal::JournalTable;
use riffdb_storage_api::{
    CompositeRow, CompositeTableV1, FrozenCompositeOverlay, MAX_COMPOSITE_COMPONENT_BYTES,
    MAX_COMPOSITE_OVERLAY_TRANSITIONS, StorageError, StorageErrorKind,
};
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use std::cmp::Ordering;
use std::sync::Arc;

pub(crate) type ProviderSourceRowBytesV1 = Arc<ProviderAllocation<CompositeRow>>;
pub(crate) type ProviderSourcePageBytesV1 = Arc<ProviderAllocation<Vec<ProviderSourceRowBytesV1>>>;
const MAX_PROVIDER_SOURCE_ROWS: usize = 502;
const PAGE_METADATA_BYTES: u64 = 32 * 1024;
const _: () = assert!(
    2 * MAX_PROVIDER_SOURCE_ROWS * std::mem::size_of::<ProviderSourceRowBytesV1>() + 8192
        < PAGE_METADATA_BYTES as usize
);

fn copy_row(
    key: &[u8],
    value: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderSourceRowBytesV1, StorageError> {
    if key.len() > MAX_COMPOSITE_COMPONENT_BYTES || value.len() > MAX_COMPOSITE_COMPONENT_BYTES {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let allowance = key
        .len()
        .checked_add(value.len())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let work = budget
        .reserve(allowance)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let mut owned_key = Vec::new();
    owned_key
        .try_reserve_exact(key.len())
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    owned_key.extend_from_slice(key);
    let mut owned_value = Vec::new();
    owned_value
        .try_reserve_exact(value.len())
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    owned_value.extend_from_slice(value);
    work.commit_value((owned_key.into_boxed_slice(), owned_value.into_boxed_slice()))
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
}

pub(crate) fn copy_provider_range_v1(
    table: &redb::ReadOnlyTable<&'static [u8], &'static [u8]>,
    overlay: Option<(&FrozenCompositeOverlay, CompositeTableV1)>,
    range: (&[u8], &[u8]),
    max_rows: usize,
    budget: &ProviderBudget,
) -> Result<ProviderSourcePageBytesV1, StorageError> {
    if max_rows == 0 || max_rows > MAX_PROVIDER_SOURCE_ROWS || range.0 >= range.1 {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    // Admit collection and bounded iterator/selection metadata before opening
    // the range or allocating its array. Row payloads are admitted individually.
    let work = budget
        .reserve(PAGE_METADATA_BYTES)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(max_rows)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let mut base = table
        .range::<&[u8]>((
            std::ops::Bound::Included(range.0),
            std::ops::Bound::Excluded(range.1),
        ))
        .map_err(precommit_storage_error)?
        .peekable();
    let mut after = None;
    let mut inspected = 0;
    let maximum_inspected = max_rows
        .checked_add(MAX_COMPOSITE_OVERLAY_TRANSITIONS)
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    while rows.len() < max_rows {
        let overlay_next = overlay.and_then(|(overlay, id)| {
            overlay.borrowed_provider_next_v1(id, range.0, after, range.1)
        });
        let base_key = match base.peek() {
            Some(Ok((key, _))) => Some(key.value()),
            Some(Err(_)) => {
                return match base.next() {
                    Some(Err(error)) => Err(precommit_storage_error(error)),
                    _ => Err(storage_error(StorageErrorKind::InvariantViolation)),
                };
            }
            None => None,
        };
        let choice = match (base_key, overlay_next) {
            (Some(key), Some((overlay_key, _))) => key.cmp(overlay_key),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => break,
        };
        if inspected >= maximum_inspected {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        inspected += 1;
        if choice == Ordering::Less {
            let (key, value) = base
                .next()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .map_err(precommit_storage_error)?;
            rows.push(copy_row(key.value(), value.value(), budget)?);
        } else {
            if choice == Ordering::Equal {
                base.next()
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                    .map_err(precommit_storage_error)?;
            }
            let (key, value) =
                overlay_next.ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            after = Some(key);
            if let Some(value) = value {
                rows.push(copy_row(key, value, budget)?);
            }
        }
    }
    // The iterator's scratch dies before transferring the array allowance,
    // including a refused ledger transition inside commit_value.
    drop(base);
    work.commit_value(rows)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
}

impl RedbReadAccess {
    pub(crate) fn read_provider_range_v1(
        &self,
        table: JournalTable,
        range: (&[u8], &[u8]),
        max_rows: usize,
        budget: &ProviderBudget,
    ) -> Result<ProviderSourcePageBytesV1, StorageError> {
        if let Self::Composite(view) = self {
            return view.resolve_provider_range_v1(table.composite(), range, max_rows, budget);
        }
        copy_provider_range_v1(
            self.cached_byte_table(table)?,
            None,
            range,
            max_rows,
            budget,
        )
    }
}
