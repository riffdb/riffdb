//! Checked, payload-free claim groups below the application boundary (ADR-0284).

use crate::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1, MAX_CHANGELOG_FRAME_BYTES,
    MAX_CHANGELOG_FRAME_ENTRIES, OutboxClaimV1, OutboxStatusObservationV1,
    OutboxTransitionResultV1, StorageValueError, proto_codec::encode_outbox_status_v1,
};

/// ADR-0284 hard bound; this is not an operator or application selector.
pub const MAX_OUTBOX_CLAIM_GROUP_ENTRIES: usize = 16;

/// Canonically ordered, checked pending-to-delivering claims.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxClaimGroupV1 {
    claims: Vec<OutboxClaimV1>,
}

impl OutboxClaimGroupV1 {
    /// Checks count, identity order, pending preimages, canonical encodings
    /// and receipt capacity before the storage owner acquires its writer turn.
    pub fn new(claims: Vec<OutboxClaimV1>) -> Result<Self, StorageValueError> {
        if claims.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if claims.len() > MAX_OUTBOX_CLAIM_GROUP_ENTRIES
            || claims.len() > MAX_CHANGELOG_FRAME_ENTRIES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        for pair in claims.windows(2) {
            if pair[0].event_id() == pair[1].event_id() {
                return Err(StorageValueError::Duplicate);
            }
            if pair[0].event_id() > pair[1].event_id() {
                return Err(StorageValueError::NonCanonicalOrder);
            }
        }
        // The same conservative fixed allowance as completion groups covers
        // the receipt and single-frame headers; the captured receipt is
        // checked again by its existing owner.
        let mut encoded_bytes = 4096usize;
        for claim in &claims {
            if !claim.expected().is_pending() {
                return Err(StorageValueError::InvalidShape);
            }
            let after = encode_outbox_status_v1(claim.updated())
                .map_err(|_| StorageValueError::InvalidShape)?;
            let key = claim.event_id().to_be_bytes();
            let mutation = match claim.expected() {
                OutboxStatusObservationV1::AbsentInitialPending => AuthoritativeMutationV3::put(
                    AuthoritativeNamespaceV1::OutboxStatus,
                    &key,
                    None,
                    after.as_bytes(),
                ),
                OutboxStatusObservationV1::Present(expected) => {
                    let before = encode_outbox_status_v1(expected)
                        .map_err(|_| StorageValueError::InvalidShape)?;
                    AuthoritativeMutationV3::replace(
                        AuthoritativeNamespaceV1::OutboxStatus,
                        &key,
                        before.as_bytes(),
                        after.as_bytes(),
                    )
                }
            }
            .map_err(|_| StorageValueError::LimitExceeded)?;
            encoded_bytes = encoded_bytes
                .checked_add(mutation.encoded_len())
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        if encoded_bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self { claims })
    }

    /// Borrows at most sixteen exact claims.
    #[must_use]
    pub fn claims(&self) -> &[OutboxClaimV1] {
        &self.claims
    }
}

/// Bounded positional results, available only after all matched rows commit
/// or after an unchanged group is explicitly aborted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxClaimGroupCompletionV1 {
    results: Vec<OutboxTransitionResultV1>,
}

impl OutboxClaimGroupCompletionV1 {
    /// Validates every result against the exact requested identity/post-image.
    pub fn new(
        group: &OutboxClaimGroupV1,
        results: Vec<OutboxTransitionResultV1>,
    ) -> Result<Self, StorageValueError> {
        if results.len() != group.claims.len() {
            return Err(StorageValueError::InvalidShape);
        }
        for (claim, result) in group.claims.iter().zip(&results) {
            match result {
                OutboxTransitionResultV1::Applied(status) if status == claim.updated() => {}
                OutboxTransitionResultV1::StateChanged(
                    OutboxStatusObservationV1::AbsentInitialPending,
                ) => {}
                OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                    status,
                )) if status.event_id() == claim.event_id() => {}
                _ => return Err(StorageValueError::IdentityMismatch),
            }
        }
        Ok(Self { results })
    }

    /// Borrows one exact result for each requested claim.
    #[must_use]
    pub fn results(&self) -> &[OutboxTransitionResultV1] {
        &self.results
    }
}

/// Complete-group outcome. Missing authority aborts every proposed change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxClaimGroupResultV1 {
    /// Matching rows were claimed together, or no row matched and no receipt was written.
    Complete(OutboxClaimGroupCompletionV1),
    /// At least one reciprocal authoritative intent is absent; no row committed.
    AuthoritativeIntentMissing,
}
