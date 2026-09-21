//! Bounded cross-attempt consistency for the exclusive promotion ledger owner.

use super::{
    ReplicationPromotionFailureV1 as Failure, ReplicationPromotionPhaseV1 as Phase,
    ReplicationPromotionReceiptV1, ReplicationPromotionRequestV1, ReplicationPromotionSelectionV1,
    ReplicationPromotionStepV1 as Step,
};
use crate::StorageValueError;
use riffdb_types::ReplicationPromotionOperationId;
use std::collections::BTreeMap;

/// Startup bound for the distinct external promotion-attempt inventory.
pub const MAX_REPLICATION_PROMOTION_RECEIPTS_V1: usize = 65_535;

/// Canonically ordered authenticated attempts. This proves value consistency,
/// not filesystem custody, complete inventory, audit durability or cutover safety.
/// The persistence owner must validate the complete proposed inventory before
/// publishing a changed receipt, under its exclusive maintenance ownership.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationPromotionReceiptInventoryV1 {
    receipts: Vec<ReplicationPromotionReceiptV1>,
}

impl ReplicationPromotionReceiptInventoryV1 {
    /// Refuses duplicate invocations, reordered inventory and any changed
    /// request or frozen selection across retries of an admitted operation.
    /// An initial authorization denial creates no operation authority; a
    /// conflicting-request denial records the rejected request without replacing
    /// the existing operation. Neither exception may carry a selection.
    pub fn new(receipts: Vec<ReplicationPromotionReceiptV1>) -> Result<Self, StorageValueError> {
        if receipts.len() > MAX_REPLICATION_PROMOTION_RECEIPTS_V1 {
            return Err(StorageValueError::LimitExceeded);
        }
        for pair in receipts.windows(2) {
            match pair[0].request_id().cmp(&pair[1].request_id()) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Err(StorageValueError::Duplicate),
                std::cmp::Ordering::Greater => return Err(StorageValueError::NonCanonicalOrder),
            }
        }
        let mut operations = BTreeMap::new();
        for receipt in receipts.iter().filter(|r| binds_operation(r)) {
            let (request, selection) = operations
                .entry(receipt.request().operation_id())
                .or_insert((receipt.request(), None));
            if *request != receipt.request() {
                return Err(StorageValueError::IdentityMismatch);
            }
            if let Some(observed) = receipt.selection() {
                match selection {
                    Some(original) if *original != observed => {
                        return Err(StorageValueError::IdentityMismatch);
                    }
                    None => *selection = Some(observed),
                    _ => {}
                }
            }
        }
        for receipt in &receipts {
            if is_conflict(receipt) && !operations.contains_key(&receipt.request().operation_id()) {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        Ok(Self { receipts })
    }

    /// Complete bounded inventory in strict invocation-ID order.
    #[must_use]
    pub fn receipts(&self) -> &[ReplicationPromotionReceiptV1] {
        &self.receipts
    }

    /// Existing exact operation request. An initial authorization denial never
    /// establishes or replaces it, and request-ID ordering implies no chronology.
    #[must_use]
    pub fn request_for(
        &self,
        operation: ReplicationPromotionOperationId,
    ) -> Option<ReplicationPromotionRequestV1> {
        self.receipts
            .iter()
            .find(|r| r.request().operation_id() == operation && binds_operation(r))
            .map(ReplicationPromotionReceiptV1::request)
    }

    /// The one immutable selection, including one retained by a failed attempt.
    /// Later attempts must reuse it after fresh authorization and source checks.
    #[must_use]
    pub fn selection_for(
        &self,
        operation: ReplicationPromotionOperationId,
    ) -> Option<&ReplicationPromotionSelectionV1> {
        self.receipts
            .iter()
            .filter(|r| r.request().operation_id() == operation)
            .find_map(ReplicationPromotionReceiptV1::selection)
    }
}

fn is_conflict(receipt: &ReplicationPromotionReceiptV1) -> bool {
    receipt.steps().last() == Some(&Step::Denied(Failure::SelectionConflict))
}

fn binds_operation(receipt: &ReplicationPromotionReceiptV1) -> bool {
    !is_conflict(receipt)
        && !(receipt.phase() == Phase::Attempted
            && receipt.steps().last() == Some(&Step::Denied(Failure::AuthorizationDenied)))
}

impl std::fmt::Debug for ReplicationPromotionReceiptInventoryV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationPromotionReceiptInventoryV1([redacted])")
    }
}
