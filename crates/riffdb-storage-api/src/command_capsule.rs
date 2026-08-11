//! Canonical successful-command capsule and checked auxiliary locators.

use std::fmt;

use riffdb_types::{CommitSequence, ServiceAuditLinkV1, ServiceAuditPhaseV1};

use crate::{
    StorageValueError, StoredCommitRecordV1, StoredDurableEventV1, StoredOutcomeV1,
    StoredProvenanceRecordV1, StoredServiceAuditRecordV1,
};

/// Canonical semantic owner for the five durable views of one successful command.
///
/// The in-memory value deliberately retains the established semantic DTOs. Its
/// durable codec normalizes their shared fields into one payload and reconstructs
/// these exact views after a same-snapshot locator join.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredCommandCapsuleV1 {
    outcome: StoredOutcomeV1,
    provenance: StoredProvenanceRecordV1,
    commit: StoredCommitRecordV1,
    started_audit: StoredServiceAuditRecordV1,
    terminal_audit: StoredServiceAuditRecordV1,
}

impl StoredCommandCapsuleV1 {
    /// Proves complete commit/outcome/provenance/audit reciprocity.
    pub fn new(
        outcome: StoredOutcomeV1,
        provenance: StoredProvenanceRecordV1,
        commit: StoredCommitRecordV1,
        started_audit: StoredServiceAuditRecordV1,
        terminal_audit: StoredServiceAuditRecordV1,
    ) -> Result<Self, StorageValueError> {
        let sequence = commit.commit_sequence();
        let provenance_id = commit.provenance_id();
        // A live post-image reference exists only for a put. Provenance also
        // retains deletes, so the live references must be an exact ordered
        // subsequence of the affected targets. The V2 constructor below joins
        // every affected target to its exact entity transition and therefore
        // proves the otherwise post-image-free delete members.
        let mut affected_entities = provenance.affected_entities().iter();
        let affected_entities_match = commit.entity_references().iter().all(|reference| {
            affected_entities.any(|affected| {
                affected.target() == reference.target()
                    && affected.entity_version() == reference.entity_version()
            })
        });
        let event_ids_match = provenance.event_ids().len() == commit.events().len()
            && provenance
                .event_ids()
                .iter()
                .copied()
                .eq(commit.events().iter().map(StoredDurableEventV1::event_id));

        let common_audit = started_audit.request_id() == terminal_audit.request_id()
            && started_audit.operation() == terminal_audit.operation()
            && started_audit.principal() == terminal_audit.principal()
            && started_audit.ingress() == terminal_audit.ingress()
            && started_audit.targets() == terminal_audit.targets()
            && started_audit.approval_id() == terminal_audit.approval_id();

        if outcome.commit_sequence() != sequence
            || outcome.admission_request_id() != commit.admission_request_id()
            || outcome.plan() != commit.plan()
            || outcome.canonical_input_hash() != commit.canonical_input_hash()
            || outcome.actor() != commit.actor()
            || outcome.logical_time() != commit.logical_time()
            || outcome.partition_hash() != commit.partition_hash()
            || outcome.conflict_hashes() != commit.conflict_hashes()
            || outcome.declared_outcome() != commit.declared_outcome()
            || outcome.provenance_id() != provenance_id
            || outcome.durability_mode() != commit.durability_mode()
            || provenance.commit_sequence() != sequence
            || provenance.provenance_id() != provenance_id
            || provenance.identity() != outcome.identity()
            || provenance.admission_request_id() != commit.admission_request_id()
            || provenance.plan() != commit.plan()
            || provenance.canonical_input_hash() != commit.canonical_input_hash()
            || provenance.actor() != commit.actor()
            || provenance.logical_time() != commit.logical_time()
            || provenance.partition_hash() != commit.partition_hash()
            || provenance.conflict_hashes() != commit.conflict_hashes()
            || provenance.outcome_id() != commit.declared_outcome().outcome_id()
            || !affected_entities_match
            || !event_ids_match
            || provenance.admitted_claims() != outcome.admitted_claims()
            || provenance.causation() != outcome.causation()
            || !common_audit
            || started_audit.administration_sequence() >= terminal_audit.administration_sequence()
            || started_audit.phase() != ServiceAuditPhaseV1::Started
            || started_audit.link() != ServiceAuditLinkV1::None
            || terminal_audit.phase() != ServiceAuditPhaseV1::Succeeded
            || terminal_audit.link()
                != (ServiceAuditLinkV1::Command {
                    commit_sequence: sequence,
                    provenance_id,
                })
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        Ok(Self {
            outcome,
            provenance,
            commit,
            started_audit,
            terminal_audit,
        })
    }

    /// Returns the canonical application commit sequence.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.commit.commit_sequence()
    }

    /// Borrows the exactly reconstructed terminal outcome view.
    #[must_use]
    pub const fn outcome(&self) -> &StoredOutcomeV1 {
        &self.outcome
    }

    /// Borrows the exactly reconstructed provenance view.
    #[must_use]
    pub const fn provenance(&self) -> &StoredProvenanceRecordV1 {
        &self.provenance
    }

    /// Borrows the exactly reconstructed commit view.
    #[must_use]
    pub const fn commit(&self) -> &StoredCommitRecordV1 {
        &self.commit
    }

    /// Borrows the exactly reconstructed Started audit view.
    #[must_use]
    pub const fn started_audit(&self) -> &StoredServiceAuditRecordV1 {
        &self.started_audit
    }

    /// Borrows the exactly reconstructed terminal audit view.
    #[must_use]
    pub const fn terminal_audit(&self) -> &StoredServiceAuditRecordV1 {
        &self.terminal_audit
    }

    /// Borrows one checked audit member selected by a locator.
    #[must_use]
    pub const fn audit(&self, member: StoredCommandAuditMemberV1) -> &StoredServiceAuditRecordV1 {
        match member {
            StoredCommandAuditMemberV1::Started => &self.started_audit,
            StoredCommandAuditMemberV1::Terminal => &self.terminal_audit,
        }
    }

    /// Consumes the capsule into the five established semantic views.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        StoredOutcomeV1,
        StoredProvenanceRecordV1,
        StoredCommitRecordV1,
        StoredServiceAuditRecordV1,
        StoredServiceAuditRecordV1,
    ) {
        (
            self.outcome,
            self.provenance,
            self.commit,
            self.started_audit,
            self.terminal_audit,
        )
    }
}

impl fmt::Debug for StoredCommandCapsuleV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredCommandCapsuleV1([REDACTED])")
    }
}

/// Payload-free locator used by successful idempotency and provenance rows.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StoredCommandLocatorV1 {
    commit_sequence: CommitSequence,
}

impl StoredCommandLocatorV1 {
    /// Constructs a locator for one nonzero command sequence.
    #[must_use]
    pub const fn new(commit_sequence: CommitSequence) -> Self {
        Self { commit_sequence }
    }

    /// Returns the located command sequence.
    #[must_use]
    pub const fn commit_sequence(self) -> CommitSequence {
        self.commit_sequence
    }
}

impl fmt::Debug for StoredCommandLocatorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredCommandLocatorV1([REDACTED])")
    }
}

/// Closed audit member selected by one command-audit locator.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StoredCommandAuditMemberV1 {
    /// The invocation's Started record.
    Started,
    /// The invocation's successful terminal record.
    Terminal,
}

/// Payload-free locator for one of the two command-owned audit rows.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StoredCommandAuditLocatorV1 {
    command: StoredCommandLocatorV1,
    member: StoredCommandAuditMemberV1,
}

impl StoredCommandAuditLocatorV1 {
    /// Constructs an exact command/audit-member locator.
    #[must_use]
    pub const fn new(commit_sequence: CommitSequence, member: StoredCommandAuditMemberV1) -> Self {
        Self {
            command: StoredCommandLocatorV1::new(commit_sequence),
            member,
        }
    }

    /// Returns the located command sequence.
    #[must_use]
    pub const fn commit_sequence(self) -> CommitSequence {
        self.command.commit_sequence()
    }

    /// Returns the selected closed audit member.
    #[must_use]
    pub const fn member(self) -> StoredCommandAuditMemberV1 {
        self.member
    }
}

impl fmt::Debug for StoredCommandAuditLocatorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredCommandAuditLocatorV1([REDACTED])")
    }
}
