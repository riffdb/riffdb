//! Compact export-head identity and closed ledger transition validation.
//!
//! The service owns the exact canonical body and immutable binding. Storage
//! additionally proves the binding's genesis, contiguous prefix, complete CAS,
//! and actual encoded byte charges before admitting an authoritative write.

use std::fmt;

use riffdb_types::{ApplicationExportOperationId, ContractLineage};

use crate::{
    ApplicationExportLedgerPrefixV1, ApplicationExportOperationWriteResultV1,
    ApplicationExportPageCommitmentV1, MAX_APPLICATION_EXPORT_STATE_BYTES, StorageError,
    StorageValueError, StoredApplicationExportOperationV1,
};

/// Version-aware retained operation; legacy bodies retain their original
/// encoder, cursor identity and page-hash representation until terminal.
#[derive(Clone, Eq, PartialEq)]
pub enum StoredApplicationExportOperation {
    /// Original opaque V1 envelope, including service state versions V1/V2.
    Legacy(StoredApplicationExportOperationV1),
    /// Compact V2 envelope and separately retained page commitments.
    Compact(StoredApplicationExportOperationV2),
}

impl StoredApplicationExportOperation {
    /// Exact caller-stable operation identity across both encodings.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        match self {
            Self::Legacy(value) => value.operation_id(),
            Self::Compact(value) => value.operation_id(),
        }
    }

    /// Protected lineage of the retained operation.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        match self {
            Self::Legacy(value) => value.lineage(),
            Self::Compact(value) => value.lineage(),
        }
    }

    /// Exact canonical service body. The compact cursor must additionally bind
    /// the complete prefix and immutable binding, not this body alone.
    #[must_use]
    pub fn canonical_state(&self) -> &[u8] {
        match self {
            Self::Legacy(value) => value.canonical_state(),
            Self::Compact(value) => value.canonical_state(),
        }
    }
}

impl fmt::Debug for StoredApplicationExportOperation {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("StoredApplicationExportOperation([REDACTED])")
    }
}

/// Closed version-aware repository for atomic head/page transitions. This port
/// neither accepts application writes nor exposes a transaction callback.
pub trait ApplicationExportLedgerRepository {
    /// Reads a complete canonical head without reading prior ledger entries.
    fn read_application_export_head(
        &self,
        operation: ApplicationExportOperationId,
    ) -> Result<Option<StoredApplicationExportOperation>, StorageError>;

    /// Compares the complete expected head, admits exactly the next member, and
    /// installs both atomically. `None` permits creation or a prefix-preserving
    /// terminal update, never a head-only advance. Actual key/value charges are
    /// calculated internally; the service supplies terminal-document headroom.
    fn compare_and_swap_application_export_head(
        &mut self,
        expected: Option<&StoredApplicationExportOperation>,
        replacement: &StoredApplicationExportOperation,
        append: Option<&ApplicationExportPageCommitmentV1>,
        terminal_reserve: usize,
    ) -> Result<ApplicationExportOperationWriteResultV1, StorageError>;

    /// Reads and verifies the complete bounded ledger under one immutable
    /// snapshot, refusing if its head differs from the exact expected head.
    /// Called once for terminalization or recovery, not once per page.
    fn verify_application_export_ledger(
        &self,
        expected: &StoredApplicationExportOperationV2,
    ) -> Result<Vec<riffdb_types::ApplicationExportPageHash>, StorageError>;

    /// Bounded startup inventory, preserving the original encoding of each row.
    fn list_application_export_heads(
        &self,
        maximum: usize,
    ) -> Result<Vec<StoredApplicationExportOperation>, StorageError>;
}

/// Bounded compact successor to the frozen opaque V1 operation envelope.
/// Construction checks identity, shape and raw bounds, but does not prove that
/// the claimed ledger is present in storage. Recovery must verify its prefix.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredApplicationExportOperationV2 {
    operation_id: ApplicationExportOperationId,
    lineage: ContractLineage,
    immutable_binding: Vec<u8>,
    canonical_state: Vec<u8>,
    prefix: ApplicationExportLedgerPrefixV1,
}

impl StoredApplicationExportOperationV2 {
    /// Checks the immutable operation binding and the lower bound on retained
    /// bytes. The repository additionally charges the complete encoded head,
    /// physical key, ledger records and terminal-document reservation.
    pub fn new(
        operation_id: ApplicationExportOperationId,
        lineage: ContractLineage,
        immutable_binding: Vec<u8>,
        canonical_state: Vec<u8>,
        prefix: ApplicationExportLedgerPrefixV1,
    ) -> Result<Self, StorageValueError> {
        if immutable_binding.is_empty() || canonical_state.is_empty() {
            return Err(StorageValueError::Empty);
        }
        let raw_bytes = immutable_binding
            .len()
            .checked_add(canonical_state.len())
            .ok_or(StorageValueError::SizeOverflow)?;
        if raw_bytes > MAX_APPLICATION_EXPORT_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let genesis = ApplicationExportLedgerPrefixV1::genesis(operation_id, &immutable_binding)?;
        if prefix.operation() != operation_id || prefix.genesis_hash() != genesis.genesis_hash() {
            return Err(StorageValueError::IdentityMismatch);
        }
        if prefix.pages() == 0 && prefix != genesis {
            return Err(StorageValueError::IdentityMismatch);
        }
        prefix.check_budget(raw_bytes, 0)?;
        Ok(Self {
            operation_id,
            lineage,
            immutable_binding,
            canonical_state,
            prefix,
        })
    }

    /// Caller-stable operation identity; no page uses a synthetic operation ID.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Protected lineage used by current observation authorization.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact canonical immutable snapshot, authority, selection and lease frame.
    #[must_use]
    pub fn immutable_binding(&self) -> &[u8] {
        &self.immutable_binding
    }

    /// Exact service-owned canonical compact progress or terminal body.
    #[must_use]
    pub fn canonical_state(&self) -> &[u8] {
        &self.canonical_state
    }

    /// Fixed-size evidence of all retained page commitments.
    #[must_use]
    pub const fn prefix(&self) -> &ApplicationExportLedgerPrefixV1 {
        &self.prefix
    }

    /// Independently reconstructs genesis for complete retained-prefix checks.
    pub fn genesis(&self) -> Result<ApplicationExportLedgerPrefixV1, StorageValueError> {
        ApplicationExportLedgerPrefixV1::genesis(self.operation_id, &self.immutable_binding)
    }

    /// Proves a creation, unchanged-prefix update, or exactly one append.
    ///
    /// This is an admission check, not a write or retry reconciliation. The
    /// repository must compare the complete stored head with `expected`, and
    /// must atomically install the exact entry and replacement. Encoded charges
    /// come from the repository's actual key/value codecs, never a public caller.
    pub fn validate_transition(
        &self,
        expected: Option<&Self>,
        append: Option<(&ApplicationExportPageCommitmentV1, usize)>,
        encoded_head_charge: usize,
        terminal_reserve: usize,
    ) -> Result<(), StorageValueError> {
        let previous = if let Some(expected) = expected {
            if self.operation_id != expected.operation_id
                || self.lineage != expected.lineage
                || self.immutable_binding != expected.immutable_binding
            {
                return Err(StorageValueError::IdentityMismatch);
            }
            expected.prefix
        } else {
            if append.is_some() || self.prefix.pages() != 0 {
                return Err(StorageValueError::IdentityMismatch);
            }
            self.genesis()?
        };
        let required = match append {
            Some((entry, charge)) => previous.advance(entry, charge)?,
            None => previous,
        };
        if self.prefix != required {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.prefix
            .check_budget(encoded_head_charge, terminal_reserve)
    }
}

impl fmt::Debug for StoredApplicationExportOperationV2 {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("StoredApplicationExportOperationV2([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApplicationExportPageOrdinalV1;
    use riffdb_types::{ApplicationExportClassV1, ApplicationExportPageHash};

    fn head() -> StoredApplicationExportOperationV2 {
        let operation =
            ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x61; 10]).unwrap();
        let binding = b"complete-service-owned-immutable-binding".to_vec();
        StoredApplicationExportOperationV2::new(
            operation,
            ContractLineage::new("ExportLedger").unwrap(),
            binding.clone(),
            b"accepted-state".to_vec(),
            ApplicationExportLedgerPrefixV1::genesis(operation, &binding).unwrap(),
        )
        .unwrap()
    }

    // req: EXP-006, EXP-007, EXP-009
    #[test]
    fn export_compact_head_requires_exact_atomic_prefix_transition() {
        let initial = head();
        let entry = ApplicationExportPageCommitmentV1::new(
            initial.operation_id(),
            ApplicationExportPageOrdinalV1::new(1).unwrap(),
            ApplicationExportClassV1::Entity,
            ApplicationExportPageHash::from_bytes([0x45; 32]),
            2,
            40,
        )
        .unwrap();
        // Synthetic charges exercise model validation, not a durable codec.
        let mut next = initial.clone();
        next.prefix = initial.prefix.advance(&entry, 100).unwrap();
        next.canonical_state = b"exporting-state".to_vec();
        assert!(initial.validate_transition(None, None, 512, 1024).is_ok());
        assert!(
            next.validate_transition(Some(&initial), Some((&entry, 100)), 512, 1024)
                .is_ok()
        );
        assert!(
            next.validate_transition(None, Some((&entry, 100)), 512, 1024)
                .is_err()
        );
        assert!(
            next.validate_transition(Some(&initial), None, 512, 1024)
                .is_err()
        );
        assert!(
            initial
                .validate_transition(Some(&initial), Some((&entry, 100)), 512, 1024)
                .is_err()
        );
        assert!(
            next.validate_transition(Some(&initial), Some((&entry, 99)), 512, 1024)
                .is_err()
        );
        assert!(
            next.validate_transition(Some(&next), Some((&entry, 100)), 512, 1024)
                .is_err()
        );
        let mut terminal = next.clone();
        terminal.canonical_state = b"terminal-documents".to_vec();
        assert!(
            terminal
                .validate_transition(Some(&next), None, 512, 0)
                .is_ok()
        );
        terminal.immutable_binding.push(0);
        assert!(
            terminal
                .validate_transition(Some(&next), None, 512, 0)
                .is_err()
        );
    }

    // req: EXP-002, EXP-006, EXP-008, EXP-010
    #[test]
    fn export_compact_head_refuses_substituted_genesis_and_aggregate_overflow() {
        let initial = head();
        assert!(
            StoredApplicationExportOperationV2::new(
                initial.operation_id,
                initial.lineage.clone(),
                b"different-authority".to_vec(),
                initial.canonical_state.clone(),
                initial.prefix,
            )
            .is_err()
        );
        assert!(
            StoredApplicationExportOperationV2::new(
                initial.operation_id,
                initial.lineage.clone(),
                initial.immutable_binding.clone(),
                vec![0; MAX_APPLICATION_EXPORT_STATE_BYTES],
                initial.prefix,
            )
            .is_err()
        );
        assert!(
            initial
                .validate_transition(None, None, 512, MAX_APPLICATION_EXPORT_STATE_BYTES - 512)
                .is_ok()
        );
        assert!(
            initial
                .validate_transition(None, None, 512, MAX_APPLICATION_EXPORT_STATE_BYTES - 511)
                .is_err()
        );
        assert!(
            initial
                .validate_transition(None, None, usize::MAX, 1)
                .is_err()
        );
        let debug = format!("{initial:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("complete-service-owned"));
    }
}
