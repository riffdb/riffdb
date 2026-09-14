//! Exact authoritative state at a published V3 fence. These read-only values
//! are not a bootstrap installation permit, receipt, or durable format.

use super::{
    AuthoritativeMutationV3, ChangelogCursorErrorV3, ChangelogHistoryStateV3, ChangelogV3Error,
};
use crate::AuthoritativeNamespaceV1;

/// One complete stored row in a replicated-authoritative namespace.
/// Construction applies the same namespace, key and byte bounds as V3 mutations.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthoritativeStateRowV3 {
    namespace: AuthoritativeNamespaceV1,
    key: Box<[u8]>,
    value: Box<[u8]>,
}

impl AuthoritativeStateRowV3 {
    /// Checks the fixed class and bounds before copying any bytes. This proves
    /// only row shape; its producer must own the published snapshot fence.
    pub fn new(
        namespace: AuthoritativeNamespaceV1,
        key: &[u8],
        value: &[u8],
    ) -> Result<Self, ChangelogV3Error> {
        AuthoritativeMutationV3::validate(namespace, key, value.len())?;
        Ok(Self {
            namespace,
            key: key.into(),
            value: value.into(),
        })
    }

    /// Closed catalog namespace.
    #[must_use]
    pub const fn namespace(&self) -> AuthoritativeNamespaceV1 {
        self.namespace
    }

    /// Exact canonical physical key; never diagnostic data.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Complete stored value; never diagnostic data.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Transfers checked row custody without copying the payload.
    #[must_use]
    pub fn into_parts(self) -> (AuthoritativeNamespaceV1, Box<[u8]>, Box<[u8]>) {
        (self.namespace, self.key, self.value)
    }
}

impl std::fmt::Debug for AuthoritativeStateRowV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AuthoritativeStateRowV3([redacted])")
    }
}

/// A complete row or the exact end of a namespace, including an empty one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthoritativeStateStepV3 {
    /// Next row, in strictly increasing namespace-tag/key order.
    Row(AuthoritativeStateRowV3),
    /// Exactly once for each replicated-authoritative namespace, in tag order.
    EndNamespace(AuthoritativeNamespaceV1),
}

/// Bounded state access for bootstrap and exact prefix comparison. The sole
/// catalog fixes the inventory; callers cannot omit or reclassify a namespace.
/// Each call returns at most one V3-bounded row. No population-sized collection,
/// write gate, durability decision or installation authority is exposed.
/// After an error the cursor remains failed; after every namespace end it
/// returns `None` forever. Dropping releases only immutable read capabilities.
pub trait AuthoritativeStateCursorV3: Send {
    /// Exact immutable history binding of every row and namespace end.
    fn history(&self) -> ChangelogHistoryStateV3;

    /// Next complete row or exact namespace end from this one pinned snapshot.
    fn next_item(&mut self) -> Result<Option<AuthoritativeStateStepV3>, ChangelogCursorErrorV3>;
}
