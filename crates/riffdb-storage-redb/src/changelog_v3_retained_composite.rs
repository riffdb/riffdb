//! The same complete source validation, paged over an acknowledged composite pin.

use super::*;

impl RetainedHistoryValidation {
    pub(super) fn composite_receipts(&mut self, limit: usize) -> Result<bool, StorageError> {
        let source = self.root.as_ref().ok_or_else(corrupt)?.clone();
        let mut bytes = 0_usize;
        for _ in 0..limit {
            let sequence = match self.after_receipt {
                Some(key) => {
                    let previous = u64::from_be_bytes(key);
                    if previous == self.history.tail().sequence().get() {
                        break;
                    }
                    previous.checked_add(1).ok_or_else(corrupt)?
                }
                None => self.history.minimum_resume().sequence().get(),
            };
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(sequence)
                .ok_or_else(corrupt)?;
            let receipt = source.receipt(sequence, None)?.ok_or_else(corrupt)?;
            let encoded = receipt.encode().map_err(|_| corrupt())?;
            let next = bytes.checked_add(encoded.len()).ok_or_else(corrupt)?;
            if next > MAX_CHANGELOG_FRAME_BYTES {
                return Ok(false);
            }
            if receipt.binding().sequence != sequence || sequence > self.history.tail().sequence() {
                return Err(corrupt());
            }
            if self.first {
                self.covered
                    .validate_terminal_receipt(&receipt)
                    .map_err(|_| corrupt())?;
                self.first = false;
            } else {
                self.covered = self.covered.advance(&receipt).map_err(|_| corrupt())?;
            }
            if !matches!(
                receipt.attribution(),
                ChangelogAttributionV3::HistoryReclamation
                    | ChangelogAttributionV3::ValidatedPrefixCheckpoint
            ) {
                self.independent_sequence = sequence.get();
            }
            self.step_rows[usize::from(self.initial_pass_complete)] =
                self.step_rows[usize::from(self.initial_pass_complete)].saturating_add(1);
            #[cfg(test)]
            {
                self.rows_read[0] += 1;
            }
            self.after_receipt = Some(sequence.get().to_be_bytes());
            bytes = next;
        }
        if self.after_receipt != Some(self.history.tail().sequence().get().to_be_bytes()) {
            return Ok(false);
        }
        if self.first || self.covered != self.history {
            return Err(corrupt());
        }
        let holds = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
        crate::changelog_v3_roots::validate_source_holds(&holds, self.history, &source)?;
        self.phase = Phase::Audit;
        Ok(false)
    }

    pub(super) fn composite_audit(&mut self, limit: usize) -> Result<bool, StorageError> {
        let source = self.root.as_ref().ok_or_else(corrupt)?.clone();
        let ValidationSource::Composite(view) = &source else {
            return Err(corrupt());
        };
        let holds = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let audit = CompositePoints {
            view,
            table: crate::journal::JournalTable::Audit,
        };
        let indexes = CompositePoints {
            view,
            table: crate::journal::JournalTable::AuditByRequest,
        };
        let mut bytes = 0_usize;
        for _ in 0..limit {
            let Some((key, value)) = source.next_audit(self.after_audit.as_deref())? else {
                self.registrations.finish()?;
                if let Some(fence) = &self.fence {
                    fence.finish()?;
                }
                self.promotion.finish(self.history, &source)?;
                self.phase = Phase::Done;
                self.initial_pass_complete = true;
                return Ok(true);
            };
            if key.len() > riffdb_types::MAX_KEY_BYTES || value.len() > AUDIT_PAGE_BYTES {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let next = bytes.checked_add(value.len()).ok_or_else(corrupt)?;
            if next > AUDIT_PAGE_BYTES {
                return Ok(false);
            }
            self.registrations.observe(&key, &value, &holds, &source)?;
            if let Some(fence) = &mut self.fence {
                fence.observe(&key, &value, &source)?;
            }
            self.promotion
                .observe((&key, &value), self.history, &audit, &indexes, &source)?;
            self.step_rows[usize::from(self.initial_pass_complete)] =
                self.step_rows[usize::from(self.initial_pass_complete)].saturating_add(1);
            #[cfg(test)]
            {
                self.rows_read[1] += 1;
            }
            self.after_audit = Some(key.into_vec());
            bytes = next;
        }
        Ok(false)
    }
}
