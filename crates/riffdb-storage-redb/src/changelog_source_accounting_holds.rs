//! Read-only, same-pin retention-floor evidence; never a source deletion proof.
//! Audit pages use the existing exact registration and release validator.
use super::*;
use crate::replication_registration_links::RegistrationLinksValidation;
use crate::source_validation_pin::ValidationSource;
use redb::ReadableTable;
use std::ops::Bound;

const ROWS_PER_PAGE: usize = 256;
const PAGE_BYTES: usize = riffdb_proto::envelope::MAX_STORED_ENVELOPE_BYTES;

pub(super) struct AccountingHolds {
    source: Option<ValidationSource>,
    history: History,
    qualification: u64,
    registrations: RegistrationLinksValidation,
    after_audit: Option<Vec<u8>>,
    complete: bool,
}

/// Borrowed only from an exact-ended hold walk. The type contains no mutation
/// license, source certificate, admission token or writer capability.
pub(super) struct ValidatedAccountingHolds<'a> {
    source: &'a ValidationSource,
    history: History,
}

impl AccountingHolds {
    pub(super) fn source(&self) -> Option<&ValidationSource> {
        self.source.as_ref()
    }

    pub(super) fn capture(
        source: ValidationSource,
        qualification: u64,
    ) -> Result<Self, StorageError> {
        if qualification == u64::MAX {
            return Err(unavailable());
        }
        let history = source.history()?.ok_or_else(corrupt)?;
        let registrations = {
            let holds = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
            crate::changelog_v3_roots::validate_source_holds(&holds, history, &source)?;
            RegistrationLinksValidation::new(&holds, history)?
        };
        Ok(Self {
            source: Some(source),
            history,
            qualification,
            registrations,
            after_audit: None,
            complete: false,
        })
    }

    pub(super) fn advance(&mut self) -> Result<bool, StorageError> {
        if self.complete {
            return Ok(true);
        }
        let source = self.source.as_ref().ok_or_else(unavailable)?.clone();
        let holds = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let mut bytes = 0_usize;
        if matches!(&source, ValidationSource::Composite(_)) {
            for _ in 0..ROWS_PER_PAGE {
                let Some((key, value)) = source.next_audit(self.after_audit.as_deref())? else {
                    self.registrations.finish()?;
                    self.complete = true;
                    return Ok(true);
                };
                if !self.observe(&key, &value, &holds, &source, &mut bytes)? {
                    return Ok(false);
                }
            }
            return Ok(false);
        }
        let audit = source
            .open_table(crate::layout::AUDIT)
            .map_err(table_error)?;
        let low = self
            .after_audit
            .as_ref()
            .map_or(Bound::Unbounded, |key| Bound::Excluded(key.as_slice()));
        for (rows, entry) in audit
            .range::<&[u8]>((low, Bound::Unbounded))
            .map_err(precommit_storage_error)?
            .enumerate()
        {
            if rows == ROWS_PER_PAGE {
                return Ok(false);
            }
            let (key, value) = entry.map_err(precommit_storage_error)?;
            if !self.observe(key.value(), value.value(), &holds, &source, &mut bytes)? {
                return Ok(false);
            }
        }
        self.registrations.finish()?;
        self.complete = true;
        Ok(true)
    }

    fn observe(
        &mut self,
        key: &[u8],
        value: &[u8],
        holds: &impl ReadableTable<&'static [u8], &'static [u8]>,
        source: &ValidationSource,
        bytes: &mut usize,
    ) -> Result<bool, StorageError> {
        if key.len() > riffdb_types::MAX_KEY_BYTES || value.len() > PAGE_BYTES {
            return Err(storage_error(StorageErrorKind::LimitExceeded));
        }
        let next = bytes.checked_add(value.len()).ok_or_else(corrupt)?;
        if next > PAGE_BYTES {
            return Ok(false);
        }
        self.registrations.observe(key, value, holds, source)?;
        self.after_audit = Some(key.to_vec());
        *bytes = next;
        Ok(true)
    }

    pub(super) fn validated(
        &self,
        qualification: u64,
    ) -> Result<ValidatedAccountingHolds<'_>, StorageError> {
        if !self.complete || qualification == u64::MAX || self.qualification != qualification {
            return Err(unavailable());
        }
        Ok(ValidatedAccountingHolds {
            source: self.source.as_ref().ok_or_else(unavailable)?,
            history: self.history,
        })
    }

    /// A completed hold census retains only bounded evidence between captures.
    /// The caller must destroy the old reader before capturing a new source pin.
    pub(super) fn release_completed_pin(&mut self) {
        if self.complete {
            self.source = None;
        }
    }

    /// Only the actual native unchanged-source qualification admits reuse.
    /// Check every current hold point again and then validate the entire newly
    /// appended audit suffix before another floor can be borrowed.
    pub(super) fn continue_on(
        &mut self,
        source: ValidationSource,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        if !self.complete || qualification == u64::MAX || self.qualification != qualification {
            return Ok(false);
        }
        let history = source.history()?.ok_or_else(corrupt)?;
        if history.lineage() != self.history.lineage()
            || history.anchor() != self.history.anchor()
            || history.minimum_resume() != self.history.minimum_resume()
            || history.tail().sequence() < self.history.tail().sequence()
        {
            return Ok(false);
        }
        let holds = source.open_table(SOURCE_HOLDS).map_err(table_error)?;
        crate::changelog_v3_roots::validate_source_holds(&holds, history, &source)?;
        self.registrations.rebind_unchanged(history)?;
        self.source = Some(source);
        self.history = history;
        self.complete = false;
        Ok(true)
    }

    pub(super) fn rebase_own_cut(
        &mut self,
        proof: &crate::journal::SourceReclamationProof,
        validated: &crate::changelog_v3_retained_validation::ValidatedRetainedHistory,
        predecessor: u64,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        let before = proof.predecessor();
        let after = validated.history();
        if after.minimum_resume() != proof.retained().minimum_resume()
            || before.tail().sequence().get().checked_add(1) != Some(after.tail().sequence().get())
        {
            return Ok(false);
        }
        self.rebind_owned(before, after, predecessor, qualification)
    }

    pub(super) fn rebase_own_acknowledgement(
        &mut self,
        publication: &AcknowledgementPublication,
    ) -> Result<bool, StorageError> {
        let Some((before, after, _, qualification)) = publication.successor() else {
            return Ok(true);
        };
        let Some(predecessor) = qualification.checked_sub(1) else {
            return Ok(false);
        };
        if before.minimum_resume() != after.minimum_resume() {
            return Ok(false);
        }
        self.rebind_owned(before, after, predecessor, qualification)
    }

    // The public-to-parent entry points above require the actual sealed native
    // transition. This helper cannot be called with a caller-authored point.
    // Recheck current hold points at the next real capture, outside writer/rest
    // charging. A checked cut/ack changes no registration or release audit row.
    fn rebind_owned(
        &mut self,
        before: History,
        after: History,
        predecessor: u64,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        if !self.complete
            || self.source.is_some()
            || self.qualification != predecessor
            || predecessor.checked_add(1) != Some(qualification)
            || qualification == u64::MAX
            || self.history.lineage() != before.lineage()
            || self.history.anchor() != before.anchor()
            || self.history.minimum_resume() != before.minimum_resume()
            || self.history.tail().sequence() > before.tail().sequence()
            || before.lineage() != after.lineage()
            || before.anchor() != after.anchor()
        {
            return Ok(false);
        }
        self.registrations.rebind_unchanged(after)?;
        self.history = after;
        self.qualification = qualification;
        Ok(true)
    }
}

impl ValidatedAccountingHolds<'_> {
    pub(super) fn floor(
        &self,
        checkpoint: riffdb_storage_api::ChangelogHistoryPointV3,
        derived_floor: Option<u64>,
    ) -> Result<[u64; 8], StorageError> {
        retention::floor_on_source(self.source, self.history, checkpoint, derived_floor)
            .map(|(_, observation)| observation)
    }
}

// Includes both source/audit validators' bounded flags and key cursors.
// Native decoded rows retain their unchanged separate per-page ceilings.
const _: () = assert!(
    std::mem::size_of::<ReplicationSourceControl>()
        + std::mem::size_of::<AccountingHolds>()
        + 2 * RegistrationLinksValidation::bookkeeping_bound()
        + 2 * riffdb_types::MAX_KEY_BYTES
        <= 64 * 1024
);

fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

#[cfg(test)]
#[path = "changelog_source_accounting_holds_tests.rs"]
mod tests;
