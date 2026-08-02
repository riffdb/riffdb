//! Retention watermark, operator holds, and history tombstones (ADR-0085 Amendment 2).

use riffdb_types::{AdministrationSequence, HashDomain, ProjectionId, SchemaHash, Timestamp, hash};

use crate::StorageValueError;

/// Domain label framed into the watermark self-hash preimage.
const WATERMARK_HASH_LABEL: &[u8] = b"riffdb.retention-watermark/v1\0";
/// Domain label framed into the tombstone chain hash preimage.
const TOMBSTONE_HASH_LABEL: &[u8] = b"riffdb.history-tombstone/v1\0";
/// Domain label framed into the chain-root binding preimage.
const TOMBSTONE_CHAIN_ROOT_LABEL: &[u8] = b"riffdb.history-tombstone-chain-root/v1\0";
/// Maximum number of concurrent operator retention holds.
pub const MAX_RETENTION_HOLDS: usize = 64;
/// Maximum hold-id length in UTF-8 bytes.
pub const MAX_RETENTION_HOLD_ID_BYTES: usize = 64;
/// Maximum hold-reason length in UTF-8 bytes.
pub const MAX_RETENTION_HOLD_REASON_BYTES: usize = 256;

/// Self-hash of one bound retention watermark.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RetentionWatermarkHash([u8; 32]);

impl RetentionWatermarkHash {
    /// Constructs a hash from exact 32 digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Chained self-hash of one history tombstone.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HistoryTombstoneHash([u8; 32]);

impl HistoryTombstoneHash {
    /// Constructs a hash from exact 32 digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Content digest over pruned rows in one tombstone range.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct HistoryTombstoneContentDigest([u8; 32]);

impl HistoryTombstoneContentDigest {
    /// Constructs a digest from exact 32 bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Bound retention watermark (ADR-0085 Amendment 2).
///
/// Sequence 0 means no history has been pruned. Advances only during offline
/// prune; re-validated (never advanced) at startup. When any history has been
/// pruned the record durably binds the registry digest CURRENT AT CHAIN
/// ROOTING; tombstone-chain verification uses that recorded value, never the
/// process's current digest, so registry migrations cannot invalidate an
/// existing chain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRetentionWatermarkV1 {
    watermark_sequence: u64,
    history_incarnation: u64,
    watermark_hash: RetentionWatermarkHash,
    chain_root_registry_digest: Option<SchemaHash>,
}

impl StoredRetentionWatermarkV1 {
    /// Constructs a watermark and computes its self-hash.
    ///
    /// `chain_root_registry_digest` must be present exactly when
    /// `watermark_sequence > 0` (a tombstone chain exists and is rooted).
    pub fn new(
        watermark_sequence: u64,
        history_incarnation: u64,
        chain_root_registry_digest: Option<SchemaHash>,
    ) -> Result<Self, StorageValueError> {
        let mut value = Self {
            watermark_sequence,
            history_incarnation,
            watermark_hash: RetentionWatermarkHash::from_bytes([0; 32]),
            chain_root_registry_digest,
        };
        value.watermark_hash = value.computed_hash()?;
        Self::from_stored_parts(
            value.watermark_sequence,
            value.history_incarnation,
            value.watermark_hash,
            value.chain_root_registry_digest,
        )
    }

    /// Reconstructs a semantically checked durable watermark.
    pub fn from_stored_parts(
        watermark_sequence: u64,
        history_incarnation: u64,
        watermark_hash: RetentionWatermarkHash,
        chain_root_registry_digest: Option<SchemaHash>,
    ) -> Result<Self, StorageValueError> {
        if history_incarnation < crate::HISTORY_INCARNATION_INITIAL {
            return Err(StorageValueError::InvalidShape);
        }
        if (watermark_sequence > 0) != chain_root_registry_digest.is_some() {
            return Err(StorageValueError::InvalidShape);
        }
        let value = Self {
            watermark_sequence,
            history_incarnation,
            watermark_hash,
            chain_root_registry_digest,
        };
        if value.computed_hash()? != watermark_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Highest commit sequence that has been pruned (inclusive). 0 = unpruned.
    #[must_use]
    pub const fn watermark_sequence(&self) -> u64 {
        self.watermark_sequence
    }

    /// History incarnation at the time the watermark was written.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Self-hash over the bound fields.
    #[must_use]
    pub const fn watermark_hash(&self) -> RetentionWatermarkHash {
        self.watermark_hash
    }

    /// Registry digest recorded when the tombstone chain was rooted, present
    /// exactly when any history has been pruned.
    #[must_use]
    pub const fn chain_root_registry_digest(&self) -> Option<SchemaHash> {
        self.chain_root_registry_digest
    }

    /// Recomputes the domain-separated self-hash.
    pub fn computed_hash(&self) -> Result<RetentionWatermarkHash, StorageValueError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WATERMARK_HASH_LABEL);
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&self.watermark_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.history_incarnation.to_be_bytes());
        match self.chain_root_registry_digest {
            None => bytes.push(0),
            Some(digest) => {
                bytes.push(1);
                bytes.extend_from_slice(digest.as_bytes());
            }
        }
        let digest = hash(HashDomain::Schema, &bytes);
        Ok(RetentionWatermarkHash::from_bytes(*digest.as_bytes()))
    }
}

/// Typed kind of one retention hold row (never inferred from hold_id shape).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionHoldKind {
    /// Operator sequence floor: binds the fencing minimum.
    Operator,
    /// Projection detached from the fencing minimum. The hold_id is the
    /// decimal projection ID; the sequence carries no meaning and is 0.
    /// Removed only by the audited projection-reattach administration action.
    ProjectionDetach,
}

/// One named retention hold row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionHoldV1 {
    hold_id: String,
    sequence: u64,
    reason: String,
    kind: RetentionHoldKind,
}

impl RetentionHoldV1 {
    /// Constructs an operator hold with validated bounds.
    pub fn new(
        hold_id: impl Into<String>,
        sequence: u64,
        reason: impl Into<String>,
    ) -> Result<Self, StorageValueError> {
        Self::from_stored_parts(hold_id, sequence, reason, RetentionHoldKind::Operator)
    }

    /// Constructs a projection-detach hold for one projection identity.
    pub fn new_projection_detach(
        projection_id: ProjectionId,
        reason: impl Into<String>,
    ) -> Result<Self, StorageValueError> {
        Self::from_stored_parts(
            projection_id.get().to_string(),
            0,
            reason,
            RetentionHoldKind::ProjectionDetach,
        )
    }

    /// Reconstructs a semantically checked stored hold.
    pub fn from_stored_parts(
        hold_id: impl Into<String>,
        sequence: u64,
        reason: impl Into<String>,
        kind: RetentionHoldKind,
    ) -> Result<Self, StorageValueError> {
        let hold_id = hold_id.into();
        let reason = reason.into();
        if hold_id.is_empty()
            || hold_id.len() > MAX_RETENTION_HOLD_ID_BYTES
            || reason.len() > MAX_RETENTION_HOLD_REASON_BYTES
            || !hold_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
        {
            return Err(StorageValueError::InvalidShape);
        }
        if kind == RetentionHoldKind::ProjectionDetach {
            // Canonical detach identity: the exact decimal of a nonzero
            // projection ID, and no fencing sequence.
            let parsed = hold_id
                .parse::<u32>()
                .ok()
                .and_then(ProjectionId::new)
                .is_some_and(|id| id.get().to_string() == hold_id);
            if !parsed || sequence != 0 {
                return Err(StorageValueError::InvalidShape);
            }
        }
        Ok(Self {
            hold_id,
            sequence,
            reason,
            kind,
        })
    }

    /// Hold identifier (unique within the holds list across kinds).
    #[must_use]
    pub fn hold_id(&self) -> &str {
        &self.hold_id
    }

    /// Sequence floor this hold enforces (watermark must not exceed).
    /// Always 0 for projection-detach holds, which never join the minimum.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Human-readable reason for the hold.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Typed kind of this hold row.
    #[must_use]
    pub const fn kind(&self) -> RetentionHoldKind {
        self.kind
    }
}

/// Bounded list of operator retention holds (canonical order by hold_id).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRetentionHoldsV1 {
    holds: Vec<RetentionHoldV1>,
}

impl StoredRetentionHoldsV1 {
    /// Constructs holds with canonical uniqueness and order checks.
    pub fn new(holds: Vec<RetentionHoldV1>) -> Result<Self, StorageValueError> {
        if holds.len() > MAX_RETENTION_HOLDS {
            return Err(StorageValueError::LimitExceeded);
        }
        if holds
            .windows(2)
            .any(|pair| pair[0].hold_id >= pair[1].hold_id)
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        // Empty is valid; uniqueness is implied by strict ascending hold_id.
        Ok(Self { holds })
    }

    /// Empty holds list.
    #[must_use]
    pub const fn empty() -> Self {
        Self { holds: Vec::new() }
    }

    /// Borrows holds in canonical hold_id order.
    #[must_use]
    pub fn holds(&self) -> &[RetentionHoldV1] {
        &self.holds
    }

    /// Minimum sequence across OPERATOR holds, if any is present.
    /// Projection-detach holds never join the fencing minimum.
    #[must_use]
    pub fn min_hold_sequence(&self) -> Option<u64> {
        self.holds
            .iter()
            .filter(|hold| hold.kind() == RetentionHoldKind::Operator)
            .map(RetentionHoldV1::sequence)
            .min()
    }
}

/// Audited offline retention administration action (ADR-0085 Amendment 2):
/// one record in the shared administration audit sequence space for every
/// projection detach/reattach performed by the offline maintenance verbs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRetentionAdministrationV1 {
    administration_sequence: AdministrationSequence,
    action: RetentionAdministrationAction,
    projection_id: ProjectionId,
    reason: String,
    timestamp: Timestamp,
}

/// Which retention administration action a record describes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionAdministrationAction {
    /// A projection was detached from the retention fencing minimum.
    ProjectionDetach,
    /// A previously detached projection was reattached to the minimum.
    ProjectionReattach,
}

impl StoredRetentionAdministrationV1 {
    /// Constructs a bounded retention administration record.
    pub fn new(
        administration_sequence: AdministrationSequence,
        action: RetentionAdministrationAction,
        projection_id: ProjectionId,
        reason: impl Into<String>,
        timestamp: Timestamp,
    ) -> Result<Self, StorageValueError> {
        let reason = reason.into();
        if reason.len() > MAX_RETENTION_HOLD_REASON_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            administration_sequence,
            action,
            projection_id,
            reason,
            timestamp,
        })
    }

    /// Shared nonzero administration ordering sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }

    /// Which action this record describes.
    #[must_use]
    pub const fn action(&self) -> RetentionAdministrationAction {
        self.action
    }

    /// Stable nonzero projection identity the action targeted.
    #[must_use]
    pub const fn projection_id(&self) -> ProjectionId {
        self.projection_id
    }

    /// Operator-supplied reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }

    /// Operator-supplied wall-clock timestamp (the offline storage layer
    /// holds no clock; the maintenance verb provides it).
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
}

/// Per-range history tombstone (ADR-0085 Amendment 2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredHistoryTombstoneV1 {
    first_sequence: u64,
    last_sequence: u64,
    commits_count: u64,
    events_count: u64,
    outbox_count: u64,
    outbox_status_count: u64,
    content_digest: HistoryTombstoneContentDigest,
    previous_tombstone_hash: Option<HistoryTombstoneHash>,
    tombstone_hash: HistoryTombstoneHash,
    history_incarnation: u64,
}

impl StoredHistoryTombstoneV1 {
    /// Constructs a tombstone and computes its chained hash.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        first_sequence: u64,
        last_sequence: u64,
        commits_count: u64,
        events_count: u64,
        outbox_count: u64,
        outbox_status_count: u64,
        content_digest: HistoryTombstoneContentDigest,
        previous_tombstone_hash: Option<HistoryTombstoneHash>,
        history_incarnation: u64,
    ) -> Result<Self, StorageValueError> {
        let mut value = Self {
            first_sequence,
            last_sequence,
            commits_count,
            events_count,
            outbox_count,
            outbox_status_count,
            content_digest,
            previous_tombstone_hash,
            tombstone_hash: HistoryTombstoneHash::from_bytes([0; 32]),
            history_incarnation,
        };
        value.tombstone_hash = value.computed_hash()?;
        Self::from_stored_parts(
            value.first_sequence,
            value.last_sequence,
            value.commits_count,
            value.events_count,
            value.outbox_count,
            value.outbox_status_count,
            value.content_digest,
            value.previous_tombstone_hash,
            value.tombstone_hash,
            value.history_incarnation,
        )
    }

    /// Reconstructs a semantically checked durable tombstone.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        first_sequence: u64,
        last_sequence: u64,
        commits_count: u64,
        events_count: u64,
        outbox_count: u64,
        outbox_status_count: u64,
        content_digest: HistoryTombstoneContentDigest,
        previous_tombstone_hash: Option<HistoryTombstoneHash>,
        tombstone_hash: HistoryTombstoneHash,
        history_incarnation: u64,
    ) -> Result<Self, StorageValueError> {
        if first_sequence == 0
            || last_sequence < first_sequence
            || history_incarnation < crate::HISTORY_INCARNATION_INITIAL
        {
            return Err(StorageValueError::InvalidShape);
        }
        let value = Self {
            first_sequence,
            last_sequence,
            commits_count,
            events_count,
            outbox_count,
            outbox_status_count,
            content_digest,
            previous_tombstone_hash,
            tombstone_hash,
            history_incarnation,
        };
        if value.computed_hash()? != tombstone_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// First commit sequence covered (inclusive).
    #[must_use]
    pub const fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    /// Last commit sequence covered (inclusive).
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Pruned commit-row count for this range.
    #[must_use]
    pub const fn commits_count(&self) -> u64 {
        self.commits_count
    }

    /// Pruned event-row count for this range.
    #[must_use]
    pub const fn events_count(&self) -> u64 {
        self.events_count
    }

    /// Pruned outbox-row count for this range.
    #[must_use]
    pub const fn outbox_count(&self) -> u64 {
        self.outbox_count
    }

    /// Pruned outbox-status-row count for this range.
    #[must_use]
    pub const fn outbox_status_count(&self) -> u64 {
        self.outbox_status_count
    }

    /// Content digest over pruned rows.
    #[must_use]
    pub const fn content_digest(&self) -> HistoryTombstoneContentDigest {
        self.content_digest
    }

    /// Previous tombstone hash, if any.
    #[must_use]
    pub const fn previous_tombstone_hash(&self) -> Option<HistoryTombstoneHash> {
        self.previous_tombstone_hash
    }

    /// This tombstone's chained hash.
    #[must_use]
    pub const fn tombstone_hash(&self) -> HistoryTombstoneHash {
        self.tombstone_hash
    }

    /// History incarnation at prune time.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Recomputes the domain-separated chained hash.
    pub fn computed_hash(&self) -> Result<HistoryTombstoneHash, StorageValueError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(TOMBSTONE_HASH_LABEL);
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&self.first_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.last_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.commits_count.to_be_bytes());
        bytes.extend_from_slice(&self.events_count.to_be_bytes());
        bytes.extend_from_slice(&self.outbox_count.to_be_bytes());
        bytes.extend_from_slice(&self.outbox_status_count.to_be_bytes());
        bytes.extend_from_slice(self.content_digest.as_bytes());
        match self.previous_tombstone_hash {
            None => bytes.push(0),
            Some(hash) => {
                bytes.push(1);
                bytes.extend_from_slice(hash.as_bytes());
            }
        }
        bytes.extend_from_slice(&self.history_incarnation.to_be_bytes());
        let digest = hash(HashDomain::Schema, &bytes);
        Ok(HistoryTombstoneHash::from_bytes(*digest.as_bytes()))
    }
}

/// Registry-bound chain root for the first history tombstone.
#[must_use]
pub fn history_tombstone_chain_root(registry_digest: SchemaHash) -> HistoryTombstoneHash {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(TOMBSTONE_CHAIN_ROOT_LABEL);
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(registry_digest.as_bytes());
    let digest = hash(HashDomain::Schema, &bytes);
    HistoryTombstoneHash::from_bytes(*digest.as_bytes())
}

/// Which fencing input produced the binding minimum (for diagnostics / tests).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetentionFenceBinding {
    /// Durable application head (last committed sequence): the watermark can
    /// never pass the end of durable history.
    DurableApplicationHead,
    /// Projection durable frontier (non-detached).
    ProjectionDurableFrontier,
    /// Operator hold sequence.
    OperatorHold,
    /// Undelivered outbox low-water mark.
    UndeliveredOutboxLowWater,
    /// Staged migration frozen application frontier.
    StagedMigrationFrozenFrontier,
    /// No fencing inputs constrained the watermark (unbounded / open).
    Unbounded,
}

/// Explicit fencing inputs used to compute the max permissible watermark.
///
/// Every optional input that is "present but unreadable" must refuse prune at
/// the call site; this pure function only considers successfully decoded values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionFencingInputs {
    /// Durable application head: the last committed application sequence
    /// (0 when nothing has ever committed). The watermark must stay at or
    /// below this bound — sequences committed later must never be born below
    /// the watermark. Collectors always supply it; `None` only exists so the
    /// pure function can be property-tested input-by-input.
    pub durable_application_head: Option<u64>,
    /// Minimum durable frontier across non-detached projections, when any exist.
    pub min_projection_durable_frontier: Option<u64>,
    /// Minimum operator hold sequence, when any holds exist.
    pub min_operator_hold_sequence: Option<u64>,
    /// Lowest undelivered outbox event commit sequence, when any undelivered exist.
    pub undelivered_outbox_low_water: Option<u64>,
    /// Staged migration frozen application frontier, when a stage is open.
    pub staged_migration_frozen_frontier: Option<u64>,
}

/// Pure fencing minimum: max permissible inclusive watermark sequence.
///
/// Returns `(max_sequence, which_input_bound)`. When no fencing inputs are
/// present, returns `None` for the sequence (caller decides whether prune is
/// allowed at all — typically refuse, fail toward retention).
#[must_use]
pub fn compute_max_permissible_watermark(
    inputs: &RetentionFencingInputs,
) -> (Option<u64>, RetentionFenceBinding) {
    let mut min_value: Option<u64> = None;
    let mut binding = RetentionFenceBinding::Unbounded;

    let consider = |current: &mut Option<u64>,
                    current_binding: &mut RetentionFenceBinding,
                    candidate: Option<u64>,
                    candidate_binding: RetentionFenceBinding| {
        if let Some(value) = candidate {
            match *current {
                None => {
                    *current = Some(value);
                    *current_binding = candidate_binding;
                }
                Some(existing) if value < existing => {
                    *current = Some(value);
                    *current_binding = candidate_binding;
                }
                _ => {}
            }
        }
    };

    consider(
        &mut min_value,
        &mut binding,
        inputs.durable_application_head,
        RetentionFenceBinding::DurableApplicationHead,
    );
    consider(
        &mut min_value,
        &mut binding,
        inputs.min_projection_durable_frontier,
        RetentionFenceBinding::ProjectionDurableFrontier,
    );
    consider(
        &mut min_value,
        &mut binding,
        inputs.min_operator_hold_sequence,
        RetentionFenceBinding::OperatorHold,
    );
    consider(
        &mut min_value,
        &mut binding,
        inputs.undelivered_outbox_low_water,
        RetentionFenceBinding::UndeliveredOutboxLowWater,
    );
    consider(
        &mut min_value,
        &mut binding,
        inputs.staged_migration_frozen_frontier,
        RetentionFenceBinding::StagedMigrationFrozenFrontier,
    );

    (min_value, binding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watermark_self_hash_roundtrip() {
        let wm = StoredRetentionWatermarkV1::new(10, 1, Some(SchemaHash::from_bytes([7; 32])))
            .expect("construct");
        assert_eq!(wm.watermark_sequence(), 10);
        assert_eq!(wm.computed_hash().expect("hash"), wm.watermark_hash());
    }

    #[test]
    fn watermark_chain_root_presence_matches_sequence() {
        // Pruned watermark without a recorded rooting digest is malformed.
        assert!(StoredRetentionWatermarkV1::new(10, 1, None).is_err());
        // Unpruned watermark must not carry a rooting digest.
        assert!(
            StoredRetentionWatermarkV1::new(0, 1, Some(SchemaHash::from_bytes([7; 32]))).is_err()
        );
        assert!(StoredRetentionWatermarkV1::new(0, 1, None).is_ok());
    }

    #[test]
    fn watermark_hash_binds_chain_root_digest() {
        let a = StoredRetentionWatermarkV1::new(10, 1, Some(SchemaHash::from_bytes([7; 32])))
            .expect("a");
        let b = StoredRetentionWatermarkV1::new(10, 1, Some(SchemaHash::from_bytes([8; 32])))
            .expect("b");
        assert_ne!(a.watermark_hash(), b.watermark_hash());
    }

    #[test]
    fn detach_holds_require_canonical_projection_identity() {
        let id = ProjectionId::new(7).expect("projection id");
        let detach = RetentionHoldV1::new_projection_detach(id, "reason").expect("detach");
        assert_eq!(detach.kind(), RetentionHoldKind::ProjectionDetach);
        assert_eq!(detach.hold_id(), "7");
        assert_eq!(detach.sequence(), 0);
        // Non-decimal, zero, non-canonical, or sequenced detach rows refuse.
        assert!(
            RetentionHoldV1::from_stored_parts("x", 0, "r", RetentionHoldKind::ProjectionDetach)
                .is_err()
        );
        assert!(
            RetentionHoldV1::from_stored_parts("0", 0, "r", RetentionHoldKind::ProjectionDetach)
                .is_err()
        );
        assert!(
            RetentionHoldV1::from_stored_parts("07", 0, "r", RetentionHoldKind::ProjectionDetach)
                .is_err()
        );
        assert!(
            RetentionHoldV1::from_stored_parts("7", 3, "r", RetentionHoldKind::ProjectionDetach)
                .is_err()
        );
    }

    #[test]
    fn holds_require_canonical_order() {
        let a = RetentionHoldV1::new("a", 5, "reason").expect("a");
        let b = RetentionHoldV1::new("b", 3, "reason").expect("b");
        assert!(StoredRetentionHoldsV1::new(vec![b.clone(), a.clone()]).is_err());
        let holds = StoredRetentionHoldsV1::new(vec![a, b]).expect("ordered");
        assert_eq!(holds.min_hold_sequence(), Some(3));
    }

    const fn no_inputs() -> RetentionFencingInputs {
        RetentionFencingInputs {
            durable_application_head: None,
            min_projection_durable_frontier: None,
            min_operator_hold_sequence: None,
            undelivered_outbox_low_water: None,
            staged_migration_frozen_frontier: None,
        }
    }

    #[test]
    fn fencing_each_input_binds_alone() {
        let only_head = RetentionFencingInputs {
            durable_application_head: Some(3),
            ..no_inputs()
        };
        let (v, b) = compute_max_permissible_watermark(&only_head);
        assert_eq!(v, Some(3));
        assert_eq!(b, RetentionFenceBinding::DurableApplicationHead);

        let only_proj = RetentionFencingInputs {
            min_projection_durable_frontier: Some(10),
            ..no_inputs()
        };
        let (v, b) = compute_max_permissible_watermark(&only_proj);
        assert_eq!(v, Some(10));
        assert_eq!(b, RetentionFenceBinding::ProjectionDurableFrontier);

        let only_hold = RetentionFencingInputs {
            min_operator_hold_sequence: Some(7),
            ..no_inputs()
        };
        let (v, b) = compute_max_permissible_watermark(&only_hold);
        assert_eq!(v, Some(7));
        assert_eq!(b, RetentionFenceBinding::OperatorHold);

        let only_outbox = RetentionFencingInputs {
            undelivered_outbox_low_water: Some(4),
            ..no_inputs()
        };
        let (v, b) = compute_max_permissible_watermark(&only_outbox);
        assert_eq!(v, Some(4));
        assert_eq!(b, RetentionFenceBinding::UndeliveredOutboxLowWater);

        let only_mig = RetentionFencingInputs {
            staged_migration_frozen_frontier: Some(9),
            ..no_inputs()
        };
        let (v, b) = compute_max_permissible_watermark(&only_mig);
        assert_eq!(v, Some(9));
        assert_eq!(b, RetentionFenceBinding::StagedMigrationFrozenFrontier);
    }

    #[test]
    fn fencing_min_across_inputs() {
        let inputs = RetentionFencingInputs {
            durable_application_head: Some(80),
            min_projection_durable_frontier: Some(100),
            min_operator_hold_sequence: Some(50),
            undelivered_outbox_low_water: Some(75),
            staged_migration_frozen_frontier: Some(60),
        };
        let (v, b) = compute_max_permissible_watermark(&inputs);
        assert_eq!(v, Some(50));
        assert_eq!(b, RetentionFenceBinding::OperatorHold);
    }

    #[test]
    fn fencing_head_binds_below_every_other_input() {
        let inputs = RetentionFencingInputs {
            durable_application_head: Some(1),
            min_projection_durable_frontier: Some(100),
            min_operator_hold_sequence: Some(100),
            undelivered_outbox_low_water: Some(100),
            staged_migration_frozen_frontier: Some(100),
        };
        let (v, b) = compute_max_permissible_watermark(&inputs);
        assert_eq!(v, Some(1));
        assert_eq!(b, RetentionFenceBinding::DurableApplicationHead);
    }

    #[test]
    fn tombstone_chain_hash_binds_previous() {
        let first = StoredHistoryTombstoneV1::new(
            1,
            10,
            10,
            5,
            5,
            5,
            HistoryTombstoneContentDigest::from_bytes([1; 32]),
            None,
            1,
        )
        .expect("first");
        let second = StoredHistoryTombstoneV1::new(
            11,
            20,
            10,
            5,
            5,
            5,
            HistoryTombstoneContentDigest::from_bytes([2; 32]),
            Some(first.tombstone_hash()),
            1,
        )
        .expect("second");
        assert_ne!(first.tombstone_hash(), second.tombstone_hash());
    }
}
