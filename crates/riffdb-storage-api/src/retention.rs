//! Retention watermark, operator holds, and history tombstones (ADR-0085 Amendment 2).

use riffdb_types::{HashDomain, SchemaHash, hash};

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
/// prune; re-validated (never advanced) at startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRetentionWatermarkV1 {
    watermark_sequence: u64,
    history_incarnation: u64,
    watermark_hash: RetentionWatermarkHash,
}

impl StoredRetentionWatermarkV1 {
    /// Constructs a watermark and computes its self-hash.
    pub fn new(
        watermark_sequence: u64,
        history_incarnation: u64,
    ) -> Result<Self, StorageValueError> {
        let mut value = Self {
            watermark_sequence,
            history_incarnation,
            watermark_hash: RetentionWatermarkHash::from_bytes([0; 32]),
        };
        value.watermark_hash = value.computed_hash()?;
        Self::from_stored_parts(
            value.watermark_sequence,
            value.history_incarnation,
            value.watermark_hash,
        )
    }

    /// Reconstructs a semantically checked durable watermark.
    pub fn from_stored_parts(
        watermark_sequence: u64,
        history_incarnation: u64,
        watermark_hash: RetentionWatermarkHash,
    ) -> Result<Self, StorageValueError> {
        if history_incarnation < crate::HISTORY_INCARNATION_INITIAL {
            return Err(StorageValueError::InvalidShape);
        }
        let value = Self {
            watermark_sequence,
            history_incarnation,
            watermark_hash,
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

    /// Recomputes the domain-separated self-hash.
    pub fn computed_hash(&self) -> Result<RetentionWatermarkHash, StorageValueError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(WATERMARK_HASH_LABEL);
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(&self.watermark_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.history_incarnation.to_be_bytes());
        let digest = hash(HashDomain::Schema, &bytes);
        Ok(RetentionWatermarkHash::from_bytes(*digest.as_bytes()))
    }
}

/// One named operator retention hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionHoldV1 {
    hold_id: String,
    sequence: u64,
    reason: String,
}

impl RetentionHoldV1 {
    /// Constructs a hold with validated bounds.
    pub fn new(
        hold_id: impl Into<String>,
        sequence: u64,
        reason: impl Into<String>,
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
        Ok(Self {
            hold_id,
            sequence,
            reason,
        })
    }

    /// Operator-chosen identifier (unique within the holds list).
    #[must_use]
    pub fn hold_id(&self) -> &str {
        &self.hold_id
    }

    /// Sequence floor this hold enforces (watermark must not exceed).
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Human-readable reason for the hold.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
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

    /// Minimum sequence across holds, if any hold is present.
    #[must_use]
    pub fn min_hold_sequence(&self) -> Option<u64> {
        self.holds.iter().map(RetentionHoldV1::sequence).min()
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
        let wm = StoredRetentionWatermarkV1::new(10, 1).expect("construct");
        assert_eq!(wm.watermark_sequence(), 10);
        assert_eq!(wm.computed_hash().expect("hash"), wm.watermark_hash());
    }

    #[test]
    fn holds_require_canonical_order() {
        let a = RetentionHoldV1::new("a", 5, "reason").expect("a");
        let b = RetentionHoldV1::new("b", 3, "reason").expect("b");
        assert!(StoredRetentionHoldsV1::new(vec![b.clone(), a.clone()]).is_err());
        let holds = StoredRetentionHoldsV1::new(vec![a, b]).expect("ordered");
        assert_eq!(holds.min_hold_sequence(), Some(3));
    }

    #[test]
    fn fencing_each_input_binds_alone() {
        let only_proj = RetentionFencingInputs {
            min_projection_durable_frontier: Some(10),
            min_operator_hold_sequence: None,
            undelivered_outbox_low_water: None,
            staged_migration_frozen_frontier: None,
        };
        let (v, b) = compute_max_permissible_watermark(&only_proj);
        assert_eq!(v, Some(10));
        assert_eq!(b, RetentionFenceBinding::ProjectionDurableFrontier);

        let only_hold = RetentionFencingInputs {
            min_projection_durable_frontier: None,
            min_operator_hold_sequence: Some(7),
            undelivered_outbox_low_water: None,
            staged_migration_frozen_frontier: None,
        };
        let (v, b) = compute_max_permissible_watermark(&only_hold);
        assert_eq!(v, Some(7));
        assert_eq!(b, RetentionFenceBinding::OperatorHold);

        let only_outbox = RetentionFencingInputs {
            min_projection_durable_frontier: None,
            min_operator_hold_sequence: None,
            undelivered_outbox_low_water: Some(4),
            staged_migration_frozen_frontier: None,
        };
        let (v, b) = compute_max_permissible_watermark(&only_outbox);
        assert_eq!(v, Some(4));
        assert_eq!(b, RetentionFenceBinding::UndeliveredOutboxLowWater);

        let only_mig = RetentionFencingInputs {
            min_projection_durable_frontier: None,
            min_operator_hold_sequence: None,
            undelivered_outbox_low_water: None,
            staged_migration_frozen_frontier: Some(9),
        };
        let (v, b) = compute_max_permissible_watermark(&only_mig);
        assert_eq!(v, Some(9));
        assert_eq!(b, RetentionFenceBinding::StagedMigrationFrozenFrontier);
    }

    #[test]
    fn fencing_min_across_inputs() {
        let inputs = RetentionFencingInputs {
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
