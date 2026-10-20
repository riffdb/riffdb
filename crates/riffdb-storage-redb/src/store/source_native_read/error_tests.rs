//! Native diagnostics must disappear at the first-party resource boundary.
use riffdb_storage_api::StorageErrorKind;

// req: STO-020, REC-001
#[test]
fn source_native_read_errors_are_closed_classes_without_retained_native_payloads() {
    use redb::ReadOnlyReopenError as E;
    let cases = [
        (E::Unsupported, StorageErrorKind::Unavailable),
        (E::Stale, StorageErrorKind::Unavailable),
        (E::Overflow, StorageErrorKind::Unavailable),
        (E::Capacity, StorageErrorKind::Unavailable),
        (
            E::Native(redb::DatabaseError::UpgradeRequired(0)),
            StorageErrorKind::IncompatibleFormat,
        ),
        (
            E::Table(redb::TableError::TableDoesNotExist(
                "synthetic-private-resource-detail".to_owned(),
            )),
            StorageErrorKind::CorruptData,
        ),
        (
            E::Transaction(redb::TransactionError::Storage(
                redb::StorageError::Corrupted("synthetic-private-resource-detail".to_owned()),
            )),
            StorageErrorKind::CorruptData,
        ),
        (
            E::Storage(redb::StorageError::Io(std::io::Error::other(
                "synthetic-private-resource-detail",
            ))),
            StorageErrorKind::Unavailable,
        ),
    ];
    for (native, expected) in cases {
        let safe = super::native_read_error(native);
        assert_eq!(safe.kind(), expected);
        assert!(std::error::Error::source(&safe).is_none());
        assert!(!format!("{safe:?} {safe}").contains("synthetic-private-resource-detail"));
    }
}
