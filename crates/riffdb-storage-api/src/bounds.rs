//! Hard process bounds enforced by the semantic storage boundary.

use std::fmt;

/// Maximum source-binding observations in one command.
pub const MAX_BINDING_OBSERVATIONS: usize = 4_096;
/// Maximum internal aggregate-root observations in one command.
pub const MAX_ROOT_VALIDATION_OBSERVATIONS: usize = 4_096;
/// Maximum combined binding, root, and range targets in one command.
pub const MAX_COMMAND_READ_TARGETS: usize = 4_096;
/// Maximum canonical read dependencies in one command.
pub const MAX_READ_DEPENDENCIES: usize = 4_096;
/// Maximum validation targets in one command.
pub const MAX_VALIDATION_TARGETS: usize = 4_096;
/// Maximum entity mutations in one command.
pub const MAX_ENTITY_MUTATIONS: usize = 4_096;
/// Maximum index deltas in one command.
pub const MAX_INDEX_DELTAS: usize = 4_096;
/// Maximum event or outbox intents in one command.
pub const MAX_EVENT_INTENTS: usize = 4_096;
/// Maximum bytes in one owned command snapshot.
pub const MAX_READ_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
/// Maximum bytes in one scan page.
pub const MAX_SCAN_PAGE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum bytes in one internal ordered commit-log scan page.
pub const MAX_COMMIT_SCAN_PAGE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum rows in one scan page.
pub const MAX_SCAN_PAGE_ENTRIES: usize = 500;
/// Maximum commands staged in one authoritative transaction.
pub const MAX_STAGED_COMMANDS: usize = 64;
/// Maximum bytes staged in one authoritative transaction.
pub const MAX_STAGED_WRITE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum findings materialized in one integrity report.
pub const MAX_INTEGRITY_FINDINGS: usize = 256;
/// Maximum bytes in one immutable contract bundle.
pub const MAX_CATALOG_BUNDLE_BYTES: usize = 15 * 1024 * 1024;
/// Maximum semantic bytes in one service-audit record.
pub const MAX_SERVICE_AUDIT_BYTES: usize = 64 * 1024;
/// Maximum readable digest keys in either operational key inventory.
pub const MAX_READABLE_DIGEST_KEYS: usize = 8;
/// Maximum bytes in one page of catalog startup evidence.
pub const MAX_HISTORICAL_EVIDENCE_PAGE_BYTES: usize = 16 * 1024 * 1024;

/// Maximum encoded content charge accepted for one complete durable `StoredEnvelope`.
pub const MAX_DURABLE_ENCODED_CONTENT_BYTES: usize = 16 * 1024 * 1024;

/// Exact encoded size of one complete durable `StoredEnvelope`.
///
/// This is the complete canonical ADR-0006 envelope byte length, not the inner
/// Protobuf payload length or a semantic DTO charge. The WP-065 codec and maximum
/// fixtures prove production charges. An in-memory backend supplies an explicit
/// synthetic test-envelope charge. It must never infer this value from semantic
/// byte accounting.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EncodedContentCharge(usize);

impl EncodedContentCharge {
    /// Checks one nonzero encoded record charge against the absolute envelope bound.
    #[must_use]
    pub const fn new(bytes: usize) -> Option<Self> {
        if bytes == 0 || bytes > MAX_DURABLE_ENCODED_CONTENT_BYTES {
            None
        } else {
            Some(Self(bytes))
        }
    }

    /// Returns the exact canonical encoded content byte charge.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

/// One semantic value paired with its complete canonical `StoredEnvelope` charge.
#[derive(Clone, Eq, PartialEq)]
pub struct EncodedPageItem<T> {
    value: T,
    encoded_content_charge: EncodedContentCharge,
}

impl<T> EncodedPageItem<T> {
    /// Binds a semantic value to its codec- or test-backend-supplied envelope charge.
    #[must_use]
    pub const fn new(value: T, encoded_content_charge: EncodedContentCharge) -> Self {
        Self {
            value,
            encoded_content_charge,
        }
    }

    /// Borrows the complete semantic value.
    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    /// Returns the complete canonical `StoredEnvelope` byte charge.
    #[must_use]
    pub const fn encoded_content_charge(&self) -> EncodedContentCharge {
        self.encoded_content_charge
    }

    /// Separates the complete semantic value from its checked charge.
    #[must_use]
    pub fn into_parts(self) -> (T, EncodedContentCharge) {
        (self.value, self.encoded_content_charge)
    }
}

impl<T> fmt::Debug for EncodedPageItem<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedPageItem")
            .field("value", &"[REDACTED]")
            .field("encoded_content_charge", &self.encoded_content_charge)
            .finish()
    }
}

pub(crate) fn checked_encoded_page_content<T>(
    items: &[EncodedPageItem<T>],
    maximum: usize,
) -> Result<usize, crate::StorageValueError> {
    let total = items.iter().try_fold(0usize, |total, item| {
        total
            .checked_add(item.encoded_content_charge().get())
            .ok_or(crate::StorageValueError::SizeOverflow)
    })?;
    if total > maximum {
        return Err(crate::StorageValueError::LimitExceeded);
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_content_charge_accepts_the_absolute_boundary_only() {
        assert_eq!(
            EncodedContentCharge::new(MAX_DURABLE_ENCODED_CONTENT_BYTES)
                .expect("absolute boundary")
                .get(),
            MAX_DURABLE_ENCODED_CONTENT_BYTES
        );
        assert!(EncodedContentCharge::new(0).is_none());
        assert!(EncodedContentCharge::new(MAX_DURABLE_ENCODED_CONTENT_BYTES + 1).is_none());
    }

    #[test]
    fn two_record_page_accepts_exact_limit_and_rejects_one_over() {
        let almost = EncodedPageItem::new(
            (),
            EncodedContentCharge::new(MAX_SCAN_PAGE_BYTES - 1).expect("charge"),
        );
        let exact = EncodedPageItem::new((), EncodedContentCharge::new(1).expect("charge"));
        assert_eq!(
            checked_encoded_page_content(&[almost.clone(), exact], MAX_SCAN_PAGE_BYTES),
            Ok(MAX_SCAN_PAGE_BYTES)
        );
        let over = EncodedPageItem::new((), EncodedContentCharge::new(2).expect("charge"));
        assert_eq!(
            checked_encoded_page_content(&[almost, over], MAX_SCAN_PAGE_BYTES),
            Err(crate::StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn commit_page_accepts_one_maximum_record_and_rejects_two_record_overflow() {
        let maximum = EncodedPageItem::new(
            (),
            EncodedContentCharge::new(MAX_COMMIT_SCAN_PAGE_BYTES).expect("maximum record"),
        );
        assert_eq!(
            checked_encoded_page_content(&[maximum], MAX_COMMIT_SCAN_PAGE_BYTES),
            Ok(MAX_COMMIT_SCAN_PAGE_BYTES)
        );
        let almost = EncodedPageItem::new(
            (),
            EncodedContentCharge::new(MAX_COMMIT_SCAN_PAGE_BYTES - 1).expect("charge"),
        );
        let two = EncodedPageItem::new((), EncodedContentCharge::new(2).expect("charge"));
        assert_eq!(
            checked_encoded_page_content(&[almost, two], MAX_COMMIT_SCAN_PAGE_BYTES),
            Err(crate::StorageValueError::LimitExceeded)
        );
    }
}
