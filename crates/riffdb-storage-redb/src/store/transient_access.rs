//! Snapshot-bound access to disposable decoded command payloads.
use super::*;
use crate::transient::{CommandDerivedLocator, TransientIndexes};
use riffdb_storage_api::{
    CommandDerivedIndexKindV1, StoredCommandCapsuleV2, StoredCommandSegmentV1,
    StoredServiceAuditRecordV1,
};

impl RedbOperationalPorts {
    fn with_command_indexes<T>(
        &self,
        required: bool,
        present: impl FnOnce(&TransientIndexes) -> Result<bool, StorageError>,
        read: impl Fn(&TransientIndexes, &RedbReadAccess) -> Result<Option<T>, StorageError>,
    ) -> Result<Option<T>, StorageError> {
        // Complete exact metadata proves a negative without loading a payload
        // or opening a new storage view. This is index absence, never cache
        // absence. Linearize the negative under the same publication guard as
        // the original complete-index path.
        {
            let state = self
                .shared
                .transient_indexes
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            match &*state {
                TransientIndexState::Ready(indexes) => {
                    if indexes.command_segment_coverage().is_none() {
                        return Err(storage_error(StorageErrorKind::Unavailable));
                    }
                    if !present(indexes)? {
                        return Ok(None);
                    }
                }
                TransientIndexState::Dormant if !required => return Ok(None),
                TransientIndexState::Dormant | TransientIndexState::Invalid => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
        }
        // Capture outside the index guard: root publication may need that guard.
        // If publication won between capture and lock, retry a bounded number
        // of times rather than resolving a newer index through an older pin.
        for _ in 0..8 {
            #[cfg(test)]
            PAYLOAD_PIN_CAPTURES.with(|count| count.set(count.get() + 1));
            let access = self.begin_composite_read()?;
            let frontier = access.application_frontier()?;
            let state = self
                .shared
                .transient_indexes
                .read()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            match &*state {
                TransientIndexState::Ready(indexes) => {
                    let coverage = indexes
                        .command_segment_coverage()
                        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
                    if coverage > frontier {
                        continue;
                    }
                    return read(indexes, &access);
                }
                TransientIndexState::Dormant if !required => return Ok(None),
                TransientIndexState::Dormant | TransientIndexState::Invalid => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            }
        }
        Err(storage_error(StorageErrorKind::Unavailable))
    }

    pub(crate) fn command_derived_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        key: &[u8],
    ) -> Result<Option<(Arc<StoredCommandSegmentV1>, CommandDerivedLocator)>, StorageError> {
        self.with_command_indexes(
            false,
            |indexes| indexes.has_command_member(kind, key),
            |indexes, access| {
                indexes.command_derived_member(kind, key, |first| {
                    access.read_value(
                        JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })
            },
        )
    }

    pub(crate) fn indexed_command_at(
        &self,
        sequence: CommitSequence,
    ) -> Result<Option<StoredCommandCapsuleV2>, StorageError> {
        self.with_command_indexes(
            false,
            |indexes| indexes.has_command_at(sequence),
            |indexes, access| {
                indexes.command_at(sequence, |first| {
                    access.read_value(
                        JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })
            },
        )
    }

    pub(crate) fn indexed_command_audit(
        &self,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError> {
        self.ensure_command_audit_index()?;
        self.with_command_indexes(
            true,
            |indexes| {
                indexes.has_command_member(
                    CommandDerivedIndexKindV1::AuditSequence,
                    &crate::keys::encode_audit_key(sequence),
                )
            },
            |indexes, access| {
                indexes.command_audit_record(sequence, |first| {
                    access.read_value(
                        JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })
            },
        )
    }

    pub(crate) fn indexed_command_audit_at_access(
        &self,
        access: &RedbReadAccess,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError> {
        let frontier = access.application_frontier()?;
        self.ensure_command_audit_index()?;
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*state {
            TransientIndexState::Ready(indexes) => {
                indexes.command_audit_record_at_or_before(sequence, frontier, |first| {
                    access.read_value(
                        JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })
            }
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }
}

impl RedbWriteAccess {
    pub(crate) fn command_derived_member(
        &self,
        kind: riffdb_storage_api::CommandDerivedIndexKindV1,
        exact_key: &[u8],
    ) -> Result<
        Option<(
            Arc<riffdb_storage_api::StoredCommandSegmentV1>,
            crate::transient::CommandDerivedLocator,
        )>,
        StorageError,
    > {
        let transient = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let published = match &*transient {
            TransientIndexState::Ready(indexes) => {
                indexes.command_derived_member(kind, exact_key, |first| {
                    self.read_command_value(
                        crate::journal::JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })?
            }
            TransientIndexState::Dormant => None,
            TransientIndexState::Invalid => {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        };
        let unpublished = self
            .shared
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .command_derived_member(kind, exact_key);
        drop(transient);
        if published.is_some() && unpublished.is_some() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        if let Some(found) = published.or(unpublished) {
            return Ok(Some(found));
        }
        let mut found = None;
        if let Some(RedbWriteOwnership::Epoch(epoch)) = self.ownership.as_ref() {
            for delta in &epoch.transient_deltas {
                if let Some(member) = delta.command_derived_member(kind, exact_key)
                    && found.replace(member).is_some()
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
        }
        Ok(found)
    }

    pub(crate) fn command_audit_record(
        &self,
        sequence: riffdb_types::AdministrationSequence,
    ) -> Result<Option<riffdb_storage_api::StoredServiceAuditRecordV1>, StorageError> {
        // Publication moves a segment out of the unpublished index and into the
        // published one while holding both locks. Both probes must run under one
        // continuous transient guard, exactly as the sibling lookups do: a guard
        // released between them lets a concurrent publication land in the gap and
        // hide a record that never left the store.
        let transient = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut found = match &*transient {
            TransientIndexState::Ready(indexes) => {
                indexes.command_audit_record(sequence, |first| {
                    self.read_command_value(
                        crate::journal::JournalTable::Commits,
                        &crate::keys::encode_application_sequence_key(first),
                    )
                })?
            }
            TransientIndexState::Dormant => None,
            TransientIndexState::Invalid => {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        };
        let unpublished = self
            .shared
            .unpublished_command_indexes
            .lock()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .command_audit_record(sequence);
        drop(transient);
        if found.is_some() && unpublished.is_some() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        found = found.or(unpublished);
        if let Some(RedbWriteOwnership::Epoch(epoch)) = self.ownership.as_ref() {
            let mut deltas = epoch.transient_deltas.iter().rev();
            if let Some(record) = deltas
                .next()
                .and_then(TransientIndexDelta::command_audit_tail)
                .filter(|record| record.administration_sequence() == sequence)
                .cloned()
            {
                if found.is_some() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                return Ok(Some(record));
            }
            for delta in deltas {
                if let Some(record) = delta.command_audit_record(sequence)
                    && found.replace(record).is_some()
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
            }
        }
        Ok(found)
    }
}

#[cfg(test)]
thread_local! { static PAYLOAD_PIN_CAPTURES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }

#[cfg(test)]
mod tests {
    use super::*;

    // req: REP-002, OUT-001
    #[test]
    fn exact_negative_lookup_does_not_capture_a_payload_read_view() {
        let directory = crate::test_path::ScopedDirectory::new("review-negative");
        let path = directory.join("db.redb");
        let store = RedbStore::open(&path).unwrap();
        let ports = RedbOperationalPorts {
            shared: store.shared,
        };
        {
            let mut state = ports.shared.transient_indexes.write().unwrap();
            *state = TransientIndexState::Ready(TransientIndexes::default());
        }
        PAYLOAD_PIN_CAPTURES.with(|count| count.set(0));
        let result = ports.with_command_indexes::<()>(
            false,
            |indexes| indexes.has_command_at(CommitSequence::first()),
            |_, _| panic!("negative lookup must not resolve a payload"),
        );
        assert_eq!(result.unwrap(), None);
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 0);
        // A positive forces a capture, which refuses this deliberately
        // uninitialized fixture rather than treating it as payload absence.
        assert!(
            ports
                .with_command_indexes(false, |_| Ok(true), |_, _| Ok(Some(())))
                .is_err()
        );
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 1);
        *ports.shared.transient_indexes.write().unwrap() = TransientIndexState::Invalid;
        assert!(ports.indexed_command_at(CommitSequence::first()).is_err());
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 1);
        drop(ports);
    }
}
