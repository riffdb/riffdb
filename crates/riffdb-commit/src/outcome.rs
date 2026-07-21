//! Current-invocation wrapper for an immutable durable command outcome.

use std::{error::Error, fmt};

use riffdb_storage_api::{DurabilityMode, StoredOutcomeV1};

use crate::CoordinatorDurability;

/// Whether this invocation first committed or replayed an existing outcome.
///
/// This value is current-invocation metadata. It is never persisted in or used
/// to rewrite [`StoredOutcomeV1`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CommittedOutcomeDisposition {
    /// This invocation produced the authoritative application commit.
    FirstCommit,
    /// This invocation returned the exact already persisted terminal outcome.
    Replay,
}

/// A committed outcome retained a mode forbidden at a production boundary.
///
/// `Memory` is valid for storage models and coordinator tests, but the
/// production application service cannot report it as durable success.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommittedOutcomeDurabilityError;

impl fmt::Display for CommittedOutcomeDurabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("committed outcome has non-production durability")
    }
}

impl Error for CommittedOutcomeDurabilityError {}

/// Transport-neutral current-call view of one exact stored command outcome.
///
/// The wrapper retains the full storage-owned result without copying or
/// reinterpreting any durable field. Its disposition is response metadata only.
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedOutcome {
    stored_outcome: Box<StoredOutcomeV1>,
    disposition: CommittedOutcomeDisposition,
}

impl CommittedOutcome {
    /// Wraps the exact outcome produced by this invocation's first commit.
    #[must_use]
    pub fn first_commit(stored_outcome: StoredOutcomeV1) -> Self {
        Self {
            stored_outcome: Box::new(stored_outcome),
            disposition: CommittedOutcomeDisposition::FirstCommit,
        }
    }

    /// Wraps the exact durable outcome found by equal-input recovery.
    #[must_use]
    pub fn replay(stored_outcome: StoredOutcomeV1) -> Self {
        Self {
            stored_outcome: Box::new(stored_outcome),
            disposition: CommittedOutcomeDisposition::Replay,
        }
    }

    /// Borrows the complete immutable durable outcome without translation.
    #[must_use]
    pub fn stored_outcome(&self) -> &StoredOutcomeV1 {
        self.stored_outcome.as_ref()
    }

    /// Returns this invocation's non-durable completion disposition.
    #[must_use]
    pub const fn disposition(&self) -> CommittedOutcomeDisposition {
        self.disposition
    }

    /// Returns the original production durability without exposing storage DTOs.
    ///
    /// A test-only memory outcome fails closed instead of being presented as a
    /// production durability guarantee.
    pub fn durability(&self) -> Result<CoordinatorDurability, CommittedOutcomeDurabilityError> {
        match self.stored_outcome.durability_mode() {
            DurabilityMode::Sync => Ok(CoordinatorDurability::Sync),
            DurabilityMode::Group => Ok(CoordinatorDurability::Group),
            DurabilityMode::Memory => Err(CommittedOutcomeDurabilityError),
        }
    }

    /// Recovers the complete immutable durable outcome without translation.
    #[must_use]
    pub fn into_stored_outcome(self) -> StoredOutcomeV1 {
        *self.stored_outcome
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        DeclaredOutcome, DurabilityMode, ExecutablePlanRef, IdempotencyIdentity,
        IdempotencyKeyDigest, StoredAdmittedProvenanceClaimsV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CommandId, CommitSequence, ContractBundleHash, ContractLineage,
        ContractVersion, DatabaseId, DigestKeyId, Environment, FieldId, LogicalTime, OutcomeId,
        PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, TenantId, TenantScope, Timestamp,
        hash_partition_key,
    };

    use super::*;

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn stored_outcome() -> StoredOutcomeV1 {
        stored_outcome_with_durability(DurabilityMode::Sync)
    }

    fn stored_outcome_with_durability(durability: DurabilityMode) -> StoredOutcomeV1 {
        let lineage = ContractLineage::new("budget").expect("lineage");
        let command_id = CommandId::new(1).expect("command ID");
        let plan = ExecutablePlanRef::new(
            lineage.clone(),
            ContractVersion::new(3).expect("contract version"),
            ContractBundleHash::from_bytes([0x41; 32]),
            command_id,
            PlanHash::from_bytes([0x42; 32]),
        );
        let tenant_scope = TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant"));
        let principal = ActorId::new("principal-a").expect("principal");
        let actor = AdmittedActorContext::new(
            principal.clone(),
            ActorKind::Human,
            tenant_scope.clone(),
            None,
        );
        let identity = IdempotencyIdentity::new(
            DatabaseId::from_bytes(uuid_bytes(0x11)).expect("database UUIDv7"),
            Environment::new("test").expect("environment"),
            tenant_scope,
            principal,
            lineage,
            command_id,
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x43; 32],
            ),
        );
        let request_id = RequestId::from_bytes(uuid_bytes(0x12)).expect("request UUIDv7");
        let provenance_id = ProvenanceId::from_bytes(uuid_bytes(0x13)).expect("provenance UUIDv7");
        let logical_time = LogicalTime::new(Timestamp::new(-7, 23).expect("timestamp"));
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(9).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let declared_outcome = DeclaredOutcome::new(
            OutcomeId::new(1).expect("outcome ID"),
            CanonicalRecord::new(vec![(
                FieldId::new(1).expect("field ID"),
                riffdb_types::CanonicalValue::Bool(true),
            )])
            .expect("canonical outcome"),
        )
        .expect("declared outcome");

        StoredOutcomeV1::new(
            identity,
            CommitSequence::first(),
            request_id,
            plan,
            CanonicalInputHash::from_bytes([0x44; 32]),
            actor,
            logical_time,
            partition.clone(),
            hash_partition_key(partition.as_bytes()),
            Vec::new(),
            declared_outcome,
            StoredAdmittedProvenanceClaimsV1::default(),
            provenance_id,
            durability,
        )
        .expect("stored outcome")
    }

    #[test]
    fn first_commit_preserves_the_exact_stored_outcome() {
        let expected = stored_outcome();
        let wrapper = CommittedOutcome::first_commit(expected.clone());

        assert_eq!(
            wrapper.disposition(),
            CommittedOutcomeDisposition::FirstCommit
        );
        assert!(wrapper.stored_outcome() == &expected);
        assert!(wrapper.into_stored_outcome() == expected);
    }

    #[test]
    fn replay_changes_only_current_invocation_disposition() {
        let expected = stored_outcome();
        let first = CommittedOutcome::first_commit(expected.clone());
        let replay = CommittedOutcome::replay(expected.clone());

        assert!(first.stored_outcome() == replay.stored_outcome());
        assert_eq!(replay.disposition(), CommittedOutcomeDisposition::Replay);
        assert!(replay.into_stored_outcome() == expected);
    }

    #[test]
    fn durability_accessor_is_storage_neutral_and_rejects_memory() {
        assert_eq!(
            CommittedOutcome::first_commit(stored_outcome_with_durability(DurabilityMode::Sync))
                .durability(),
            Ok(CoordinatorDurability::Sync)
        );
        assert_eq!(
            CommittedOutcome::replay(stored_outcome_with_durability(DurabilityMode::Group))
                .durability(),
            Ok(CoordinatorDurability::Group)
        );
        assert_eq!(
            CommittedOutcome::first_commit(stored_outcome_with_durability(DurabilityMode::Memory))
                .durability(),
            Err(CommittedOutcomeDurabilityError)
        );
    }
}
