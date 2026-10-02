//! Pre-allocation admission for one immutable provider source copy.
// req: OQ-040
use super::RedbReadAccess;
use crate::error::{precommit_storage_error, storage_error};
use crate::journal::JournalTable;
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{ProviderAllocation, ProviderBudget};
use std::sync::Arc;

pub(crate) type ProviderSourceBytesV1 = Arc<ProviderAllocation<Vec<u8>>>;

pub(crate) fn copy_provider_bytes_v1(
    source: &[u8],
    budget: &ProviderBudget,
) -> Result<ProviderSourceBytesV1, StorageError> {
    if source.len() > riffdb_storage_api::MAX_COMPOSITE_COMPONENT_BYTES {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let allowance = u64::try_from(source.len())
        .ok()
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(2048))
        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
    let work = budget
        .reserve(allowance)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(source.len())
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
    bytes.extend_from_slice(source);
    work.commit_value(bytes)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
}

impl RedbReadAccess {
    pub(crate) fn read_provider_value_v1(
        &self,
        table: JournalTable,
        key: &[u8],
        budget: &ProviderBudget,
    ) -> Result<Option<ProviderSourceBytesV1>, StorageError> {
        if let Self::Composite(view) = self {
            return view.resolve_provider_value_v1(table.composite(), key, budget);
        }
        let encoded = self
            .cached_byte_table(table)?
            .get(key)
            .map_err(precommit_storage_error)?;
        encoded
            .map(|value| copy_provider_bytes_v1(value.value(), budget))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;

    #[test]
    fn copied_source_bytes_retain_ownership_through_their_final_capture() {
        let budget = ProviderBudget::new();
        let source = b"bounded-source";
        let bytes = copy_provider_bytes_v1(source, &budget).unwrap();
        assert_eq!(bytes.as_slice(), source);
        let capture = bytes.clone();
        drop(bytes);
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            2 * source.len() as u64 + 2048
        );
        drop(capture);
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        let used = budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget.reserve(EXACT_PROVIDER_BUDGET_BYTES - used).unwrap();
        assert!(
            matches!(copy_provider_bytes_v1(source, &budget), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    #[test]
    fn oversized_source_cannot_publish_a_copy_or_charge() {
        let budget = ProviderBudget::new();
        let source = vec![0; riffdb_storage_api::MAX_COMPOSITE_COMPONENT_BYTES + 1];
        assert!(
            matches!(copy_provider_bytes_v1(&source, &budget), Err(error) if error.kind() == StorageErrorKind::LimitExceeded)
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
