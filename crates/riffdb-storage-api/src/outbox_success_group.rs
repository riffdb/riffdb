//! Checked, payload-free success groups below the application boundary.

use crate::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1, MAX_CHANGELOG_FRAME_BYTES,
    MAX_CHANGELOG_FRAME_ENTRIES, OutboxStatusObservationV1, OutboxSucceedV1,
    OutboxTransitionResultV1, StorageValueError, proto_codec::encode_outbox_status_v1,
};

/// ADR-0273 hard bound; this is not an operator or application selector.
pub const MAX_OUTBOX_SUCCESS_GROUP_ENTRIES: usize = 16;

/// Canonically ordered, checked delivering-to-delivered transitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxSucceedGroupV1 {
    transitions: Vec<OutboxSucceedV1>,
}

impl OutboxSucceedGroupV1 {
    /// Checks count, identity order, canonical encodings and receipt capacity
    /// before the storage owner acquires its writer turn.
    pub fn new(transitions: Vec<OutboxSucceedV1>) -> Result<Self, StorageValueError> {
        if transitions.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if transitions.len() > MAX_OUTBOX_SUCCESS_GROUP_ENTRIES
            || transitions.len() > MAX_CHANGELOG_FRAME_ENTRIES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        for pair in transitions.windows(2) {
            if pair[0].event_id() == pair[1].event_id() {
                return Err(StorageValueError::Duplicate);
            }
            if pair[0].event_id() > pair[1].event_id() {
                return Err(StorageValueError::NonCanonicalOrder);
            }
        }
        // A conservative fixed allowance covers the existing receipt and
        // single-frame headers. The real captured receipt is checked again
        // by its existing owner, including any materialized journal suffix.
        let mut encoded_bytes = 4096usize;
        for transition in &transitions {
            let before = encode_outbox_status_v1(transition.expected())
                .map_err(|_| StorageValueError::InvalidShape)?;
            let after = encode_outbox_status_v1(transition.updated())
                .map_err(|_| StorageValueError::InvalidShape)?;
            let mutation = AuthoritativeMutationV3::replace(
                AuthoritativeNamespaceV1::OutboxStatus,
                &transition.event_id().to_be_bytes(),
                before.as_bytes(),
                after.as_bytes(),
            )
            .map_err(|_| StorageValueError::LimitExceeded)?;
            encoded_bytes = encoded_bytes
                .checked_add(mutation.encoded_len())
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        if encoded_bytes > MAX_CHANGELOG_FRAME_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self { transitions })
    }

    /// Borrows at most sixteen exact success transitions.
    #[must_use]
    pub fn transitions(&self) -> &[OutboxSucceedV1] {
        &self.transitions
    }
}

/// Bounded positional results, available only after all matched rows commit
/// or after an unchanged group is explicitly aborted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxSucceedGroupCompletionV1 {
    results: Vec<OutboxTransitionResultV1>,
}

impl OutboxSucceedGroupCompletionV1 {
    /// Validates every result against the exact requested identity/post-image.
    pub fn new(
        group: &OutboxSucceedGroupV1,
        results: Vec<OutboxTransitionResultV1>,
    ) -> Result<Self, StorageValueError> {
        if results.len() != group.transitions.len() {
            return Err(StorageValueError::InvalidShape);
        }
        for (transition, result) in group.transitions.iter().zip(&results) {
            match result {
                OutboxTransitionResultV1::Applied(status) if status == transition.updated() => {}
                OutboxTransitionResultV1::StateChanged(
                    OutboxStatusObservationV1::AbsentInitialPending,
                ) => {}
                OutboxTransitionResultV1::StateChanged(OutboxStatusObservationV1::Present(
                    status,
                )) if status.event_id() == transition.event_id() => {}
                _ => return Err(StorageValueError::IdentityMismatch),
            }
        }
        Ok(Self { results })
    }

    /// Borrows one exact result for each requested transition.
    #[must_use]
    pub fn results(&self) -> &[OutboxTransitionResultV1] {
        &self.results
    }
}

/// Complete-group outcome. Missing authority aborts every proposed change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxSucceedGroupResultV1 {
    /// Matching rows committed together, or no row matched and no receipt was written.
    Complete(OutboxSucceedGroupCompletionV1),
    /// At least one reciprocal authoritative intent is absent; no row committed.
    AuthoritativeIntentMissing,
}
