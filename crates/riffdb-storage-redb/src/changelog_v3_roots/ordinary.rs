//! Ordinary transport preserves all existing source/provider/legacy dispatch.
use super::*;
pub(super) struct OrdinaryRootRead<'a, M, H, S, A> {
    pub(super) meta: &'a M,
    pub(super) history_table: Option<&'a H>,
    pub(super) source_holds: Option<&'a S>,
    pub(super) audit: Option<&'a A>,
    pub(super) budget: Option<&'a riffdb_types::exact_provider_budget_v1::ProviderBudget>,
    pub(super) source: Option<&'a crate::store::OriginalSourceReadContext<'a>>,
}
impl<M, H, S, A> RootEvidenceRead for OrdinaryRootRead<'_, M, H, S, A>
where
    M: ReadableTable<&'static str, &'static [u8]>,
    H: ReadableTable<&'static [u8], &'static [u8]>,
    S: ReadableTable<&'static [u8], &'static [u8]>,
    A: ReadableTable<&'static [u8], &'static [u8]>,
{
    fn meta_value(&mut self, key: &str) -> Result<Option<RootValue>, StorageError> {
        self.meta
            .get(key)
            .map_err(precommit_storage_error)?
            .map(|v| RootValue::copy(v.value()))
            .transpose()
    }
    fn meta_present(&mut self, key: &str) -> Result<bool, StorageError> {
        Ok(self
            .meta
            .get(key)
            .map_err(precommit_storage_error)?
            .is_some())
    }
    fn history_present(&self) -> bool {
        self.history_table.is_some()
    }
    fn holds_present(&self) -> bool {
        self.source_holds.is_some()
    }
    fn history_empty(&mut self) -> Result<bool, StorageError> {
        self.history_table
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .is_empty()
            .map_err(precommit_storage_error)
    }
    fn holds_empty(&mut self) -> Result<bool, StorageError> {
        self.source_holds
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .is_empty()
            .map_err(precommit_storage_error)
    }
    fn validate_source_allocator(
        &mut self,
        catalog: [u8; 32],
        incarnation: u64,
    ) -> Result<(), StorageError> {
        let key = riffdb_storage_api::AuthoritativeNamespaceV3::ProjectionSourceAllocator
            .metadata_key()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let allocator = self.meta.get(key).map_err(precommit_storage_error)?;
        validate_source_allocator_value(catalog, incarnation, allocator.as_ref().map(|v| v.value()))
    }
    fn validate_admission(
        &mut self,
        history: ChangelogHistoryStateV3,
        attached: bool,
    ) -> Result<(), StorageError> {
        if self.source.is_some()
            && crate::primary_admission_roots::read(self.meta)?.is_some_and(|a| a.fence().is_some())
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        crate::primary_admission_roots::validate_with_admission(
            self.meta,
            history,
            attached,
            self.audit,
            self.history_table
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?,
            self.budget,
        )
    }
    fn validate_terminal(&mut self, history: ChangelogHistoryStateV3) -> Result<(), StorageError> {
        let receipt = self
            .history_table
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?
            .get(history.tail().sequence().get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if let Some(source) = self.source {
            return source.validate_terminal(receipt.value(), history);
        }
        if let Some(budget) = self.budget {
            let candidate =
                crate::history_stub::provider_receipt_from_value_v1(receipt.value(), budget)?;
            return crate::changelog_v3::ValidatedReceiptSummaryV3::from_receipt(
                &candidate.receipt,
            )
            .and_then(|s| s.validate_terminal_receipt(history))
            .map_err(|_| storage_error(StorageErrorKind::CorruptData));
        }
        crate::history_stub::resolve_history_summary(receipt.value())?
            .validate_terminal_receipt(history)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }
}
