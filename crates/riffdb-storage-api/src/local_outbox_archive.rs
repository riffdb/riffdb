//! Exact immutable destination custody, independent of authoritative state.

use std::fmt;
use std::num::NonZeroU64;

use crate::StoredDurableEventV1;
use riffdb_types::DatabaseId;

/// Accepted archive identity and the complete checked immutable event.
#[derive(Clone, Eq, PartialEq)]
pub struct LocalOutboxArchiveRecordV1 {
    database_id: DatabaseId,
    history_incarnation: NonZeroU64,
    event: StoredDurableEventV1,
}

impl LocalOutboxArchiveRecordV1 {
    /// Binds a checked event to one nonzero database-history incarnation.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        history_incarnation: NonZeroU64,
        event: StoredDurableEventV1,
    ) -> Self {
        Self {
            database_id,
            history_incarnation,
            event,
        }
    }

    /// Permanent database binding.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// History incarnation prevents identity reuse after restore or promotion.
    #[must_use]
    pub const fn history_incarnation(&self) -> NonZeroU64 {
        self.history_incarnation
    }

    /// Complete checked event, including its canonical hash and policy anchor.
    #[must_use]
    pub const fn event(&self) -> &StoredDurableEventV1 {
        &self.event
    }

    /// Canonical big-endian database/incarnation/event identity.
    #[must_use]
    pub fn key(&self) -> [u8; 36] {
        let mut key = [0; 36];
        key[..16].copy_from_slice(self.database_id.as_bytes());
        key[16..24].copy_from_slice(&self.history_incarnation.get().to_be_bytes());
        key[24..].copy_from_slice(&self.event.event_id().to_be_bytes());
        key
    }
}

impl fmt::Debug for LocalOutboxArchiveRecordV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LocalOutboxArchiveRecordV1([REDACTED])")
    }
}
