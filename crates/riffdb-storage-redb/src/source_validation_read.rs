//! Least-authority point readers shared by checkpoint and composite validation.

use crate::error::{precommit_storage_error, storage_error};
use redb::ReadableTable;
use riffdb_storage_api::{
    AuthoritativeTransactionV3, ChangelogTransactionSequence, StorageError, StorageErrorKind,
};
use riffdb_types::exact_provider_budget_v1::ProviderBudget;

pub(crate) struct SourceReceipt {
    receipt: AuthoritativeTransactionV3,
    // Last: retain admitted decoding ownership until the receipt is destroyed.
    _work: Option<riffdb_types::exact_provider_budget_v1::Reservation>,
}
impl std::ops::Deref for SourceReceipt {
    type Target = AuthoritativeTransactionV3;
    fn deref(&self) -> &Self::Target {
        &self.receipt
    }
}

pub(crate) trait ReceiptRead {
    fn receipt(
        &self,
        sequence: ChangelogTransactionSequence,
        budget: Option<&ProviderBudget>,
    ) -> Result<Option<SourceReceipt>, StorageError>;
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> ReceiptRead for T {
    fn receipt(
        &self,
        sequence: ChangelogTransactionSequence,
        budget: Option<&ProviderBudget>,
    ) -> Result<Option<SourceReceipt>, StorageError> {
        let Some(row) = self
            .get(sequence.get().to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
        else {
            return Ok(None);
        };
        if let Some(budget) = budget {
            return crate::history_stub::provider_receipt_from_value_v1(row.value(), budget).map(
                |candidate| {
                    Some(SourceReceipt {
                        receipt: candidate.receipt,
                        _work: Some(candidate.work),
                    })
                },
            );
        }
        AuthoritativeTransactionV3::decode(&crate::history_stub::resolve_history_value(
            row.value(),
        )?)
        .map(|receipt| {
            Some(SourceReceipt {
                receipt,
                _work: None,
            })
        })
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }
}

pub(crate) trait PointRead {
    fn point(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;
}

impl<T: ReadableTable<&'static [u8], &'static [u8]>> PointRead for T {
    fn point(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(row) = self.get(key).map_err(precommit_storage_error)? else {
            return Ok(None);
        };
        if row.value().len() > riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        Ok(Some(row.value().to_vec()))
    }
}

pub(crate) struct CompositeReceipts<'a>(
    pub(crate) &'a crate::composite_view::RedbCompositeReadView,
);
impl ReceiptRead for CompositeReceipts<'_> {
    fn receipt(
        &self,
        sequence: ChangelogTransactionSequence,
        budget: Option<&ProviderBudget>,
    ) -> Result<Option<SourceReceipt>, StorageError> {
        // This internal validator owns existing bounded decode work. Provider
        // reads keep their original caller-admitted cursor path.
        if budget.is_some() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.0
            .changelog_suffix()
            .read_receipt(self.0.checkpoint_root(), sequence)
            .map(|receipt| {
                receipt.map(|receipt| SourceReceipt {
                    receipt,
                    _work: None,
                })
            })
    }
}

pub(crate) struct CompositePoints<'a> {
    pub(crate) view: &'a crate::composite_view::RedbCompositeReadView,
    pub(crate) table: crate::journal::JournalTable,
}
impl PointRead for CompositePoints<'_> {
    fn point(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.view.resolve_point(self.table.composite(), key)
    }
}
