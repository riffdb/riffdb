//! Complete canonical V3 inventory model, separate from backend apply/replay.

use riffdb_storage_api::{
    AuthoritativeNamespaceV1, AuthoritativeStateCursorV3, AuthoritativeStateStepV3,
    AuthoritativeTransactionV3, ChangelogHistoryPointV3, ChangelogLineageV3,
    ReplicationAuthorityClassV1,
};
use std::collections::BTreeMap;

const MAX_ROWS: usize = 131_072;
const MAX_BYTES: usize = 64 * 1024 * 1024;
type Rows = BTreeMap<Box<[u8]>, Box<[u8]>>;

/// Closed, value-free disagreement from the complete byte model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NamespaceModelError {
    /// The pinned input refused a read.
    Input,
    /// A namespace was omitted, repeated, substituted, or left incomplete.
    Inventory,
    /// Keys within one namespace were repeated or out of order.
    Order,
    /// The fixed model population or byte budget was exceeded.
    Limit,
    /// Lineage, exact position, or predecessor identity disagreed.
    Position,
    /// A mutation's absent/exact predecessor disagreed with the model.
    PriorState,
    /// The complete namespace maps differed at the compared position.
    Bytes,
}
impl std::fmt::Display for NamespaceModelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "authoritative namespace model disagreement: {self:?}")
    }
}
impl std::error::Error for NamespaceModelError {}

/// Independent in-memory net-transition oracle for every replicated namespace.
///
/// Only the shared closed catalog, receipt codec, and predecessor hash codec are
/// reused. No backend transaction, follower applier, projection replay, or
/// storage normalization participates. Empty namespaces remain explicit maps.
/// The existing semantic command model remains a separate workload oracle.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeNamespaceModel {
    lineage: ChangelogLineageV3,
    position: ChangelogHistoryPointV3,
    namespaces: BTreeMap<AuthoritativeNamespaceV1, Rows>,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
}
impl std::fmt::Debug for AuthoritativeNamespaceModel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthoritativeNamespaceModel([redacted])")
    }
}

impl AuthoritativeNamespaceModel {
    /// Captures one exact complete baseline within the fixed test-model budget.
    pub fn capture(
        input: &mut dyn AuthoritativeStateCursorV3,
    ) -> Result<Self, NamespaceModelError> {
        Self::capture_bounded(input, MAX_ROWS, MAX_BYTES)
    }

    fn capture_bounded(
        input: &mut dyn AuthoritativeStateCursorV3,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<Self, NamespaceModelError> {
        let history = input.history();
        let mut model = Self {
            lineage: history.lineage(),
            position: history.tail(),
            namespaces: BTreeMap::new(),
            rows: 0,
            bytes: 0,
            max_rows,
            max_bytes,
        };
        for namespace in AuthoritativeNamespaceV1::ALL
            .into_iter()
            .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
        {
            let mut rows = Rows::new();
            loop {
                let step = input.next_item().map_err(|_| NamespaceModelError::Input)?;
                if input.history() != history {
                    return Err(NamespaceModelError::Position);
                }
                match step {
                    Some(AuthoritativeStateStepV3::Row(row)) if row.namespace() == namespace => {
                        if rows
                            .last_key_value()
                            .is_some_and(|(last, _)| last.as_ref() >= row.key())
                        {
                            return Err(NamespaceModelError::Order);
                        }
                        let size = row
                            .key()
                            .len()
                            .checked_add(row.value().len())
                            .ok_or(NamespaceModelError::Limit)?;
                        model.rows = model
                            .rows
                            .checked_add(1)
                            .ok_or(NamespaceModelError::Limit)?;
                        model.bytes = model
                            .bytes
                            .checked_add(size)
                            .ok_or(NamespaceModelError::Limit)?;
                        if model.rows > max_rows || model.bytes > max_bytes {
                            return Err(NamespaceModelError::Limit);
                        }
                        let (_, key, value) = row.into_parts();
                        rows.insert(key, value);
                    }
                    Some(AuthoritativeStateStepV3::EndNamespace(end)) if end == namespace => break,
                    _ => return Err(NamespaceModelError::Inventory),
                }
            }
            model.namespaces.insert(namespace, rows);
        }
        if input
            .next_item()
            .map_err(|_| NamespaceModelError::Input)?
            .is_some()
        {
            return Err(NamespaceModelError::Inventory);
        }
        if input.history() != history {
            return Err(NamespaceModelError::Position);
        }
        Ok(model)
    }

    /// Exact lineage of the captured baseline and every subsequent receipt.
    #[must_use]
    pub const fn lineage(&self) -> ChangelogLineageV3 {
        self.lineage
    }

    /// Last completely modeled receipt; never a partly applied transaction.
    #[must_use]
    pub const fn position(&self) -> ChangelogHistoryPointV3 {
        self.position
    }

    /// Checks every predecessor and the whole budget before changing any map.
    /// A duplicate or gap is an error, so a test cannot hide duplicate application
    /// by replaying a receipt into an idempotent assignment-only oracle.
    pub fn apply(
        &mut self,
        receipt: &AuthoritativeTransactionV3,
    ) -> Result<(), NamespaceModelError> {
        let binding = receipt.binding();
        if binding.database_id != self.lineage.database_id()
            || binding.history_incarnation != self.lineage.history_incarnation()
            || binding.predecessor != Some(self.position.sequence())
            || Some(binding.sequence) != self.position.sequence().checked_next()
            || binding.predecessor_frontier != self.position.frontier()
            || binding.prior_history_hash != self.position.history_hash()
        {
            return Err(NamespaceModelError::Position);
        }
        let next = ChangelogHistoryPointV3::from_receipt(receipt)
            .map_err(|_| NamespaceModelError::Position)?;
        let (mut rows, mut bytes) = (self.rows, self.bytes);
        for mutation in receipt.mutations() {
            let namespace = self
                .namespaces
                .get(&mutation.namespace())
                .ok_or(NamespaceModelError::Inventory)?;
            let prior = namespace.get(mutation.key());
            if !mutation.matches_prior(prior.map(AsRef::as_ref)) {
                return Err(NamespaceModelError::PriorState);
            }
            if let Some(prior) = prior {
                rows -= 1;
                bytes -= mutation.key().len() + prior.len();
            }
            if let Some(value) = mutation.value() {
                rows = rows.checked_add(1).ok_or(NamespaceModelError::Limit)?;
                bytes = bytes
                    .checked_add(mutation.key().len())
                    .and_then(|n| n.checked_add(value.len()))
                    .ok_or(NamespaceModelError::Limit)?;
            }
        }
        if rows > self.max_rows || bytes > self.max_bytes {
            return Err(NamespaceModelError::Limit);
        }
        // All fallible checks above precede the first state change. Constructor-
        // checked receipts contain only one net transition for each key.
        for mutation in receipt.mutations() {
            if let Some(namespace) = self.namespaces.get_mut(&mutation.namespace()) {
                match mutation.value() {
                    Some(value) => {
                        namespace.insert(mutation.key().into(), value.into());
                    }
                    None => {
                        namespace.remove(mutation.key());
                    }
                }
            }
        }
        self.rows = rows;
        self.bytes = bytes;
        self.position = next;
        Ok(())
    }

    /// Compares both directions across the complete inventory at the exact
    /// lineage/hash/frontier position, including explicitly empty namespaces.
    pub fn verify(
        &self,
        input: &mut dyn AuthoritativeStateCursorV3,
    ) -> Result<(), NamespaceModelError> {
        let observed = Self::capture(input)?;
        if observed.lineage != self.lineage || observed.position != self.position {
            return Err(NamespaceModelError::Position);
        }
        if observed.namespaces != self.namespaces {
            return Err(NamespaceModelError::Bytes);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "namespaces_tests.rs"]
mod tests;
