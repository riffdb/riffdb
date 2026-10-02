//! Actual encoded index data retains its charge through semantic record clones.
// req: OQ-040
use super::StoredIndexEntryV2;
use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, ProviderAllocation, Reservation};
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct ProviderIndexEncodedCaptureV1 {
    // Contains actual shared bytes, rather than a detached accounting sidecar.
    data: Option<Arc<ProviderAllocation<Arc<[u8]>>>>,
}
impl PartialEq for ProviderIndexEncodedCaptureV1 {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ProviderIndexEncodedCaptureV1 {}

impl StoredIndexEntryV2 {
    /// Consumes only a fresh ordinary decode. Moving the original byte Arc into
    /// its charge before creating aliases also preserves failure destruction.
    pub(crate) fn from_admitted_provider_encoded_v1(
        record: Self,
        bytes: Reservation,
    ) -> Result<Self, BudgetUnavailable> {
        if record.provider_encoded.data.is_some() {
            return Err(BudgetUnavailable);
        }
        let Self {
            key,
            schema_binding,
            covered_values,
            covered_values_encoded,
            partition_key,
            provider_encoded: _,
        } = record;
        let original = bytes.commit_value(covered_values_encoded)?;
        Ok(Self {
            key,
            schema_binding,
            covered_values,
            covered_values_encoded: Arc::clone(&**original),
            partition_key,
            provider_encoded: ProviderIndexEncodedCaptureV1 {
                data: Some(original),
            },
        })
    }
}
