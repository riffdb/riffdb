//! Original entity data retains its admission through semantic record clones.
// req: OQ-040
use super::StoredEntityRecordV1;
use riffdb_types::exact_provider_budget_v1::ProviderAllocation;
use std::sync::Arc;

#[derive(Clone, Default)]
pub(super) struct ProviderEntityFieldCaptureV1 {
    // The original decoded record has an empty capture, so this is acyclic.
    // Its immutable allocation destroys actual field data before quota release.
    _data: Option<Arc<ProviderAllocation<StoredEntityRecordV1>>>,
}
impl PartialEq for ProviderEntityFieldCaptureV1 {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for ProviderEntityFieldCaptureV1 {}

impl StoredEntityRecordV1 {
    /// Only a freshly reconstructed, admitted provider original may enter here.
    /// Its metadata clone requires the caller's separate construction allowance.
    #[doc(hidden)]
    pub fn from_admitted_provider_original_v1(original: Arc<ProviderAllocation<Self>>) -> Self {
        let mut record = (*original).clone();
        record.provider_fields = ProviderEntityFieldCaptureV1 {
            _data: Some(original),
        };
        record
    }
}
