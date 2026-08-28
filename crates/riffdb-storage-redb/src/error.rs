//! Redacted conversion from redb and durable-codec failures.

use riffdb_storage_api::{
    DurableCodecError, DurableCodecErrorKind, StorageError, StorageErrorKind,
};

/// Constructs one source-free storage error at the adapter boundary.
#[must_use]
#[track_caller]
pub(crate) fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

/// Classifies a checked durable-codec failure without retaining payload details.
#[must_use]
pub(crate) fn codec_error(error: DurableCodecError) -> StorageError {
    let kind = match error.kind() {
        DurableCodecErrorKind::IncompatibleFormat => StorageErrorKind::IncompatibleFormat,
        DurableCodecErrorKind::CorruptData | DurableCodecErrorKind::UnexpectedRecordType => {
            StorageErrorKind::CorruptData
        }
        DurableCodecErrorKind::LimitExceeded | DurableCodecErrorKind::ReservationExceeded => {
            StorageErrorKind::LimitExceeded
        }
        DurableCodecErrorKind::InvariantViolation => StorageErrorKind::InvariantViolation,
    };
    storage_error(kind)
}

/// Redacts and classifies a database-open failure.
#[must_use]
pub(crate) fn database_error(error: redb::DatabaseError) -> StorageError {
    let kind = match error {
        redb::DatabaseError::UpgradeRequired(_) => StorageErrorKind::IncompatibleFormat,
        redb::DatabaseError::Storage(error) => return precommit_storage_error(error),
        redb::DatabaseError::DatabaseAlreadyOpen
        | redb::DatabaseError::RepairAborted
        | redb::DatabaseError::TransactionInProgress => StorageErrorKind::Unavailable,
        _ => StorageErrorKind::Unavailable,
    };
    storage_error(kind)
}

/// Redacts and classifies a transaction failure that occurred before commit.
#[must_use]
pub(crate) fn transaction_error(error: redb::TransactionError) -> StorageError {
    match error {
        redb::TransactionError::Storage(error) => precommit_storage_error(error),
        redb::TransactionError::ReadTransactionStillInUse(_) => {
            storage_error(StorageErrorKind::InvariantViolation)
        }
        _ => storage_error(StorageErrorKind::Unavailable),
    }
}

/// Redacts and classifies a table-open failure that occurred before commit.
#[must_use]
pub(crate) fn table_error(error: redb::TableError) -> StorageError {
    let kind = match error {
        redb::TableError::TableTypeMismatch { .. }
        | redb::TableError::TableIsMultimap(_)
        | redb::TableError::TableIsNotMultimap(_)
        | redb::TableError::TypeDefinitionChanged { .. } => StorageErrorKind::IncompatibleFormat,
        redb::TableError::TableDoesNotExist(_) => StorageErrorKind::CorruptData,
        redb::TableError::TableExists(_) | redb::TableError::TableAlreadyOpen(_, _) => {
            StorageErrorKind::InvariantViolation
        }
        redb::TableError::Storage(error) => return precommit_storage_error(error),
        _ => StorageErrorKind::Unavailable,
    };
    storage_error(kind)
}

/// Redacts and classifies a redb storage failure proven to precede commit.
#[must_use]
pub(crate) fn precommit_storage_error(error: redb::StorageError) -> StorageError {
    let kind = match error {
        redb::StorageError::Corrupted(_) => StorageErrorKind::CorruptData,
        redb::StorageError::ValueTooLarge(_) => StorageErrorKind::LimitExceeded,
        redb::StorageError::LockPoisoned(_) => StorageErrorKind::InvariantViolation,
        redb::StorageError::Io(_)
        | redb::StorageError::PreviousIo
        | redb::StorageError::DatabaseClosed => StorageErrorKind::Unavailable,
        _ => StorageErrorKind::Unavailable,
    };
    storage_error(kind)
}

/// Redacts a returned commit failure without inferring rollback or durability.
#[must_use]
pub(crate) fn commit_error(_error: redb::CommitError) -> StorageError {
    storage_error(StorageErrorKind::CommitStatusUnknown)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn durable_codec_classes_map_to_the_closed_storage_taxonomy() {
        let cases = [
            (
                DurableCodecErrorKind::IncompatibleFormat,
                StorageErrorKind::IncompatibleFormat,
            ),
            (
                DurableCodecErrorKind::CorruptData,
                StorageErrorKind::CorruptData,
            ),
            (
                DurableCodecErrorKind::UnexpectedRecordType,
                StorageErrorKind::CorruptData,
            ),
            (
                DurableCodecErrorKind::LimitExceeded,
                StorageErrorKind::LimitExceeded,
            ),
            (
                DurableCodecErrorKind::ReservationExceeded,
                StorageErrorKind::LimitExceeded,
            ),
            (
                DurableCodecErrorKind::InvariantViolation,
                StorageErrorKind::InvariantViolation,
            ),
        ];

        for (codec_kind, storage_kind) in cases {
            assert_eq!(
                codec_error(DurableCodecError::new(codec_kind)).kind(),
                storage_kind
            );
        }
    }

    #[test]
    fn open_and_precommit_failures_retain_only_safe_classes() {
        assert_eq!(
            database_error(redb::DatabaseError::UpgradeRequired(0)).kind(),
            StorageErrorKind::IncompatibleFormat
        );
        assert_eq!(
            database_error(redb::DatabaseError::DatabaseAlreadyOpen).kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            table_error(redb::TableError::TableDoesNotExist(
                "secret-table".to_owned()
            ))
            .kind(),
            StorageErrorKind::CorruptData
        );
        assert_eq!(
            transaction_error(redb::TransactionError::Storage(
                redb::StorageError::DatabaseClosed
            ))
            .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            precommit_storage_error(redb::StorageError::ValueTooLarge(99)).kind(),
            StorageErrorKind::LimitExceeded
        );

        let redacted = precommit_storage_error(redb::StorageError::Corrupted(
            "secret engine detail".to_owned(),
        ));
        assert_eq!(redacted.kind(), StorageErrorKind::CorruptData);
        assert!(!format!("{redacted:?} {redacted}").contains("secret"));
    }

    #[test]
    fn every_returned_commit_error_is_uncertain_and_redacted() {
        let errors = [
            redb::CommitError::Storage(redb::StorageError::Corrupted(
                "secret commit detail".to_owned(),
            )),
            redb::CommitError::Storage(redb::StorageError::Io(io::Error::other(
                "secret I/O detail",
            ))),
        ];

        for error in errors {
            let mapped = commit_error(error);
            assert_eq!(mapped.kind(), StorageErrorKind::CommitStatusUnknown);
            assert!(!format!("{mapped:?} {mapped}").contains("secret"));
        }
    }
}
