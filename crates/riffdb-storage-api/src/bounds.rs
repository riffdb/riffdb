//! Hard process bounds enforced by the semantic storage boundary.

use std::fmt;

/// Maximum source-binding observations in one command.
pub const MAX_BINDING_OBSERVATIONS: usize = 4_096;
/// Maximum internal aggregate-root observations in one command.
pub const MAX_ROOT_VALIDATION_OBSERVATIONS: usize = 4_096;
/// Maximum combined binding, root, and range targets in one command.
pub const MAX_COMMAND_READ_TARGETS: usize = riffdb_types::MAX_COMMAND_VALIDATION_TARGETS_V1;
/// Maximum canonical read dependencies in one command.
pub const MAX_READ_DEPENDENCIES: usize = 4_096;
/// Maximum validation targets in one command.
pub const MAX_VALIDATION_TARGETS: usize = riffdb_types::MAX_COMMAND_VALIDATION_TARGETS_V1;
/// Maximum entity mutations in one command.
pub const MAX_ENTITY_MUTATIONS: usize = 4_096;
/// Maximum index deltas in one command.
pub const MAX_INDEX_DELTAS: usize = riffdb_types::MAX_COMMAND_INDEX_DELTAS_V1;
/// Maximum mutation-affected index-prefix epoch targets in one command.
pub const MAX_AFFECTED_INDEX_EPOCH_TARGETS: usize =
    riffdb_types::MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1;
/// Maximum event or outbox intents in one command.
pub const MAX_EVENT_INTENTS: usize = 4_096;
/// Maximum bytes in one owned command snapshot.
pub const MAX_READ_SNAPSHOT_BYTES: usize = riffdb_types::MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1;
/// Maximum bytes in one scan page.
pub const MAX_SCAN_PAGE_BYTES: usize = 4 * 1024 * 1024;
/// Maximum physical index candidates inspected by one partition-filtered scan call.
pub const MAX_INDEX_SCAN_INSPECTED_ENTRIES: usize = 500;
/// Maximum complete encoded candidate bytes inspected by one partition-filtered scan call.
pub const MAX_INDEX_SCAN_INSPECTED_BYTES: usize = 4 * 1024 * 1024;
/// Maximum exact partition keys in one lower index filter.
pub const MAX_INDEX_PARTITION_FILTER_KEYS: usize = 1_024;
/// Maximum length-framed partition-key content in one lower index filter.
pub const MAX_INDEX_PARTITION_FILTER_BYTES: usize = 1024 * 1024;
/// Maximum bytes in one internal ordered commit-log scan page.
pub const MAX_COMMIT_SCAN_PAGE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum rows in one scan page.
pub const MAX_SCAN_PAGE_ENTRIES: usize = 500;
/// Maximum commands staged in one authoritative transaction.
pub const MAX_STAGED_COMMANDS: usize = 64;
/// Maximum independently acknowledged transitions selected for one production group.
pub const MAX_GROUPED_WRITE_TRANSITIONS: usize = MAX_STAGED_COMMANDS;
/// Maximum time the production writer may wait to form a compatible group.
pub const MAX_GROUP_WAIT_MICROSECONDS: u64 = 200;
/// Maximum bytes staged in one authoritative transaction.
pub const MAX_STAGED_WRITE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum findings materialized in one integrity report.
pub const MAX_INTEGRITY_FINDINGS: usize = 256;
/// Maximum bytes in one immutable contract bundle.
pub const MAX_CATALOG_BUNDLE_BYTES: usize = 15 * 1024 * 1024;
/// Maximum bytes in one immutable query module.
pub const MAX_QUERY_MODULE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum immutable query modules retained by the POC database.
pub const MAX_RETAINED_QUERY_MODULES: usize = 4_096;
/// Maximum semantic bytes in one service-audit record.
pub const MAX_SERVICE_AUDIT_BYTES: usize = 64 * 1024;
/// Maximum readable digest keys in either operational key inventory.
pub const MAX_READABLE_DIGEST_KEYS: usize = 8;
/// Maximum bytes in one page of catalog startup evidence.
pub const MAX_HISTORICAL_EVIDENCE_PAGE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum physical index rows in one startup migration evidence or instruction page.
pub const MAX_INDEX_MIGRATION_PAGE_ENTRIES: usize = 500;
/// Maximum bytes in each independent startup migration page ledger.
pub const MAX_INDEX_MIGRATION_PAGE_BYTES: usize = 4 * 1024 * 1024;

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
    fn command_runtime_bounds_alias_the_shared_checked_plan_limits() {
        assert_eq!(MAX_INDEX_DELTAS, riffdb_types::MAX_COMMAND_INDEX_DELTAS_V1);
        assert_eq!(
            MAX_AFFECTED_INDEX_EPOCH_TARGETS,
            riffdb_types::MAX_COMMAND_AFFECTED_INDEX_PREFIXES_V1
        );
        assert_eq!(
            MAX_VALIDATION_TARGETS,
            riffdb_types::MAX_COMMAND_VALIDATION_TARGETS_V1
        );
        assert_eq!(
            MAX_COMMAND_READ_TARGETS,
            riffdb_types::MAX_COMMAND_VALIDATION_TARGETS_V1
        );
        assert_eq!(
            MAX_READ_SNAPSHOT_BYTES,
            riffdb_types::MAX_COMMAND_READ_STATE_SEMANTIC_BYTES_V1
        );
    }

    #[test]
    fn physical_write_group_uses_the_existing_authoritative_command_ceiling() {
        assert_eq!(MAX_GROUPED_WRITE_TRANSITIONS, 64);
        assert_eq!(MAX_GROUPED_WRITE_TRANSITIONS, MAX_STAGED_COMMANDS);
        assert_eq!(MAX_STAGED_WRITE_BYTES, 16 * 1024 * 1024);
    }

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
