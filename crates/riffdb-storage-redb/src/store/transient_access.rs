//! Snapshot-bound access to disposable decoded command payloads.
use super::*;
use crate::transient::{CommandDerivedLocator, TransientIndexes};
use riffdb_storage_api::{
    CommandDerivedIndexKindV1, StoredCommandCapsuleV2, StoredCommandSegmentV1,
    StoredServiceAuditRecordV1,
};

impl RedbOperationalPorts {
    pub(crate) fn pending_outbox_delivery_count(&self) -> Result<u64, StorageError> {
        let _lease = self.acquire_indexed_read_lease()?;
        let indexes = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        match &*indexes {
            TransientIndexState::Ready(indexes) => indexes
                .undelivered_outbox_count()
                .ok_or_else(|| storage_error(StorageErrorKind::Unavailable)),
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                Err(storage_error(StorageErrorKind::Unavailable))
            }
        }
    }

    fn with_command_indexes<T, S>(
        &self,
        required: bool,
        present: impl FnOnce(&TransientIndexes) -> Result<bool, StorageError>,
        capture: impl Fn(
            &TransientIndexes,
        )
            -> Result<Option<(crate::transient::CapturedCommandPayload, S)>, StorageError>,
        read: impl FnOnce(&Arc<StoredCommandSegmentV1>, S) -> Result<T, StorageError>,
    ) -> Result<Option<T>, StorageError> {
        // Complete exact metadata still linearizes a negative under publication.
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
        // Capture the authoritative view outside the publication guard. Under
        // that guard select only one immutable metadata entry and cache candidate.
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
            let selected = match &*state {
                TransientIndexState::Ready(indexes) => {
                    let coverage = indexes
                        .command_segment_coverage()
                        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
                    if coverage > frontier {
                        continue;
                    }
                    capture(indexes)?
                }
                TransientIndexState::Dormant if !required => return Ok(None),
                TransientIndexState::Dormant | TransientIndexState::Invalid => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
            };
            drop(state);
            return selected
                .map(|selected| self.resolve_captured_payload(selected, &access, read))
                .transpose();
        }
        Err(storage_error(StorageErrorKind::Unavailable))
    }

    fn resolve_captured_payload<T, S>(
        &self,
        (payload, selected): (crate::transient::CapturedCommandPayload, S),
        access: &RedbReadAccess,
        read: impl FnOnce(&Arc<StoredCommandSegmentV1>, S) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut resolved = payload.resolve(|first| {
            access.read_value(
                JournalTable::Commits,
                &crate::keys::encode_application_sequence_key(first),
            )
        })?;
        // Resolution and conservative ownership charging have finished. Only
        // reinsert if exactly this metadata entry still belongs to the index.
        let state = self
            .shared
            .transient_indexes
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let TransientIndexState::Ready(indexes) = &*state {
            indexes.retain_resolved_payload(&mut resolved)?;
        }
        drop(state);
        read(&resolved.segment, selected)
    }

    pub(crate) fn command_derived_member(
        &self,
        kind: CommandDerivedIndexKindV1,
        key: &[u8],
    ) -> Result<Option<(Arc<StoredCommandSegmentV1>, CommandDerivedLocator)>, StorageError> {
        self.with_command_indexes(
            false,
            |indexes| indexes.has_command_member(kind, key),
            |indexes| indexes.capture_command_member(kind, key),
            |segment, locator| {
                let command = segment
                    .commands()
                    .get(usize::from(locator.command_ordinal))
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
                if crate::transient::expected_manifest_key(
                    command,
                    kind,
                    locator.member,
                    locator.member_ordinal,
                )?
                .as_slice()
                    != key
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                Ok((Arc::clone(segment), locator))
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
            |indexes| {
                indexes
                    .capture_command_at(sequence)
                    .map(|payload| payload.map(|payload| (payload, ())))
            },
            |segment, ()| {
                let ordinal =
                    usize::try_from(sequence.get() - segment.first_commit_sequence().get())
                        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                segment
                    .commands()
                    .get(ordinal)
                    .cloned()
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
            },
        )
    }

    pub(crate) fn indexed_command_audit(
        &self,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError> {
        self.ensure_command_audit_index()?;
        let key = crate::keys::encode_audit_key(sequence);
        self.with_command_indexes(
            true,
            |indexes| indexes.has_command_member(CommandDerivedIndexKindV1::AuditSequence, &key),
            |indexes| {
                indexes.capture_command_member(CommandDerivedIndexKindV1::AuditSequence, &key)
            },
            |segment, locator| {
                crate::transient::command_audit_from_locator(segment, locator, sequence)
                    .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
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
        let captured = match &*state {
            TransientIndexState::Ready(indexes) => indexes.capture_command_member(
                CommandDerivedIndexKindV1::AuditSequence,
                &crate::keys::encode_audit_key(sequence),
            )?,
            TransientIndexState::Dormant | TransientIndexState::Invalid => {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        };
        drop(state);
        let Some((payload, locator)) = captured else {
            return Ok(None);
        };
        if frontier.is_none_or(|frontier| payload.last_sequence() > frontier) {
            return Ok(None);
        }
        self.resolve_captured_payload((payload, locator), access, |segment, locator| {
            crate::transient::command_audit_from_locator(segment, locator, sequence)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))
        })
        .map(Some)
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
            if let Some(record) = epoch
                .transient_deltas
                .last()
                .and_then(TransientIndexDelta::command_audit_tail)
                .filter(|record| record.administration_sequence() == sequence)
                .cloned()
            {
                if found.is_some() {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                return Ok(Some(record));
            }
            // A non-tail member may still belong to the newest segment.
            // The fast tail probe must not consume that segment before the
            // exact-member search, which also preserves duplicate detection.
            for delta in epoch.transient_deltas.iter().rev() {
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
impl RedbOperationalPorts {
    fn install_payload_fixture(&self, segment: &Arc<StoredCommandSegmentV1>, encoded: &[u8]) {
        let write = self.shared.database.begin_write().unwrap();
        let key = crate::keys::encode_application_sequence_key(segment.first_commit_sequence());
        write
            .open_table(crate::layout::COMMITS)
            .unwrap()
            .insert(key.as_slice(), encoded)
            .unwrap();
        let allocator = crate::codec::encode_application_sequence_allocator_v1(
            riffdb_storage_api::ApplicationSequenceAllocator::Next(
                segment.last_commit_sequence().checked_next().unwrap(),
            ),
        )
        .unwrap();
        write
            .open_table(crate::layout::META)
            .unwrap()
            .insert(
                crate::layout::META_APPLICATION_SEQUENCE,
                allocator.as_bytes(),
            )
            .unwrap();
        self.shared.commit_durable(write).unwrap();
        let mut indexes = TransientIndexes::default();
        indexes.apply(
            crate::transient::TransientIndexDelta::CommandSegmentPublished(
                Arc::clone(segment),
                None,
            ),
        );
        assert!(
            indexes.command_segment_coverage().is_some(),
            "fixture metadata must be complete"
        );
        *self.shared.transient_indexes.write().unwrap() =
            crate::transient::TransientIndexState::Ready(indexes);
    }

    fn check_payload_publication_released<T>(&self, read: impl FnOnce() -> T) -> T {
        let available = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&available);
        let shared = Arc::clone(&self.shared);
        crate::transient::PAYLOAD_READ_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                observed.store(
                    shared.transient_indexes.try_write().is_ok(),
                    std::sync::atomic::Ordering::Release,
                );
            }));
        });
        let result = read();
        assert!(
            available.load(std::sync::atomic::Ordering::Acquire),
            "payload storage I/O must not retain the index publication guard"
        );
        result
    }

    pub(crate) fn assert_private_epoch_resolves_every_command_audit(
        &self,
        published: Arc<StoredCommandSegmentV1>,
        encoded: &[u8],
        staged: Arc<StoredCommandSegmentV1>,
    ) {
        self.install_payload_fixture(&published, encoded);
        let mut epoch = self.begin_deferred_epoch().unwrap();
        epoch
            .transient_deltas
            .push(TransientIndexDelta::CommandSegmentPublished(
                Arc::clone(&staged),
                None,
            ));
        let mut access = epoch.begin_write().unwrap();
        // Query the tail first, then every earlier audit in the same newest
        // private segment. No durable publication supplies these records.
        let expected = staged
            .commands()
            .iter()
            .rev()
            .flat_map(|command| {
                [
                    command.base().terminal_audit(),
                    command.base().started_audit(),
                ]
            })
            .cloned()
            .collect::<Vec<_>>();
        let actual = expected
            .iter()
            .map(|audit| access.command_audit_record(audit.administration_sequence()))
            .collect::<Vec<_>>();
        let absent = access
            .command_audit_record(riffdb_types::AdministrationSequence::new(u64::MAX).unwrap());
        let predecessor = published.commands()[0].base().started_audit();
        let prior = access.command_audit_record(predecessor.administration_sequence());
        let Some(RedbWriteOwnership::Epoch(epoch)) = access.ownership.as_mut() else {
            panic!("private epoch ownership");
        };
        epoch
            .transient_deltas
            .push(TransientIndexDelta::CommandSegmentPublished(
                Arc::clone(&staged),
                None,
            ));
        let duplicate = access.command_audit_record(
            staged.commands()[0]
                .base()
                .started_audit()
                .administration_sequence(),
        );
        // This isolated lookup fixture stages no authoritative mutation. Clear
        // its synthetic index delta before cancellation, keeping the production
        // rule that dropping real unpublished state fences the store.
        let Some(RedbWriteOwnership::Epoch(epoch)) = access.ownership.as_mut() else {
            panic!("private epoch ownership");
        };
        epoch.transient_deltas.clear();
        access.abort().unwrap();
        assert_eq!(absent.unwrap(), None, "an absent audit stays absent");
        assert_eq!(
            prior.unwrap(),
            Some(predecessor.clone()),
            "published predecessor authority is preserved"
        );
        assert_eq!(
            duplicate.unwrap_err().kind(),
            StorageErrorKind::CorruptData,
            "duplicate private authority must fail closed"
        );
        for (found, expected) in actual.into_iter().zip(expected) {
            assert_eq!(
                found.unwrap(),
                Some(expected),
                "every private command-owned audit remains exactly resolvable"
            );
        }
    }

    pub(crate) fn assert_payload_io_releases_publication(
        &self,
        segment: Arc<StoredCommandSegmentV1>,
        encoded: &[u8],
    ) {
        self.install_payload_fixture(&segment, encoded);
        for _ in 0..2 {
            assert_eq!(
                self.check_payload_publication_released(|| self
                    .indexed_command_at(segment.first_commit_sequence())
                    .unwrap()),
                Some(segment.commands()[0].clone())
            );
        }
        let entry = segment
            .manifest()
            .entries()
            .iter()
            .find(|entry| entry.kind() == CommandDerivedIndexKindV1::Provenance)
            .unwrap();
        let (found, _) = self.check_payload_publication_released(|| {
            self.command_derived_member(entry.kind(), entry.exact_key())
                .unwrap()
                .unwrap()
        });
        assert_eq!(found.segment_digest(), segment.segment_digest());
        let audit = segment.commands()[0].base().started_audit();
        assert_eq!(
            self.check_payload_publication_released(|| self
                .indexed_command_audit(audit.administration_sequence())
                .unwrap()),
            Some(audit.clone())
        );
        let pin = self.begin_composite_read().unwrap();
        assert_eq!(
            self.check_payload_publication_released(|| self
                .indexed_command_audit_at_access(&pin, audit.administration_sequence())
                .unwrap()),
            Some(audit.clone())
        );
    }

    pub(crate) fn assert_payload_keeps_captured_authority(
        &self,
        segment: Arc<StoredCommandSegmentV1>,
        encoded: &[u8],
        replace_index: bool,
    ) {
        self.install_payload_fixture(&segment, encoded);
        let shared = Arc::clone(&self.shared);
        let captured = Arc::clone(&segment);
        crate::transient::PAYLOAD_READ_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                if replace_index {
                    let mut replacement = TransientIndexes::default();
                    replacement.apply(
                        crate::transient::TransientIndexDelta::CommandSegmentPublished(
                            Arc::clone(&captured),
                            None,
                        ),
                    );
                    // Identical complete metadata, with new custody and no cache.
                    *shared
                        .transient_indexes
                        .try_write()
                        .expect("publication guard is available") =
                        TransientIndexState::Ready(replacement);
                } else {
                    // The already captured root keeps its valid row. A later
                    // root deliberately contains corruption, which a cache hit
                    // must never hide from the next independently pinned read.
                    let write = shared.database.begin_write().unwrap();
                    let key = crate::keys::encode_application_sequence_key(
                        captured.first_commit_sequence(),
                    );
                    write
                        .open_table(crate::layout::COMMITS)
                        .unwrap()
                        .insert(key.as_slice(), b"corrupt successor".as_slice())
                        .unwrap();
                    shared.commit_durable(write).unwrap();
                }
            }));
        });
        assert_eq!(
            self.indexed_command_at(segment.first_commit_sequence())
                .unwrap(),
            Some(segment.commands()[0].clone())
        );
        if replace_index {
            let state = self.shared.transient_indexes.read().unwrap();
            let TransientIndexState::Ready(indexes) = &*state else {
                panic!("complete replacement");
            };
            assert_eq!(
                indexes.payload_stats(),
                (0, 0),
                "old read must not repopulate a replaced index"
            );
            drop(state);
            assert_eq!(
                self.indexed_command_at(segment.first_commit_sequence())
                    .unwrap(),
                Some(segment.commands()[0].clone())
            );
        } else {
            assert_eq!(
                self.indexed_command_at(segment.first_commit_sequence())
                    .unwrap_err()
                    .kind(),
                StorageErrorKind::CorruptData,
                "cached old payload must not hide successor corruption"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: END-007, END-008
    #[test]
    fn delivery_cardinality_never_rebuilds_or_turns_an_unknown_index_into_zero() {
        let directory = crate::test_path::ScopedDirectory::new("outbox-count-only");
        let store = RedbStore::open(directory.join("db.redb")).unwrap();
        let ports = RedbOperationalPorts {
            shared: store.shared,
        };
        assert_eq!(
            ports
                .shared_ports()
                .pending_outbox_delivery_count()
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert!(matches!(
            *ports.shared.transient_indexes.read().unwrap(),
            TransientIndexState::Dormant
        ));
        *ports.shared.transient_indexes.write().unwrap() =
            TransientIndexState::Ready(TransientIndexes::default());
        assert_eq!(
            ports
                .shared_ports()
                .pending_outbox_delivery_count()
                .unwrap(),
            0
        );
        *ports.shared.transient_indexes.write().unwrap() = TransientIndexState::Invalid;
        assert_eq!(
            ports
                .shared_ports()
                .pending_outbox_delivery_count()
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert!(matches!(
            *ports.shared.transient_indexes.read().unwrap(),
            TransientIndexState::Invalid
        ));
    }

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
        let result = ports.with_command_indexes::<(), ()>(
            false,
            |indexes| indexes.has_command_at(CommitSequence::first()),
            |indexes| {
                indexes
                    .capture_command_at(CommitSequence::first())
                    .map(|payload| payload.map(|payload| (payload, ())))
            },
            |_, _| panic!("negative lookup must not resolve a payload"),
        );
        assert_eq!(result.unwrap(), None);
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 0);
        // A positive forces a capture, which refuses this deliberately
        // uninitialized fixture rather than treating it as payload absence.
        assert!(
            ports
                .with_command_indexes(
                    false,
                    |_| Ok(true),
                    |indexes| indexes
                        .capture_command_at(CommitSequence::first())
                        .map(|payload| payload.map(|payload| (payload, ()))),
                    |_, _| Ok(())
                )
                .is_err()
        );
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 1);
        *ports.shared.transient_indexes.write().unwrap() = TransientIndexState::Invalid;
        assert!(ports.indexed_command_at(CommitSequence::first()).is_err());
        assert_eq!(PAYLOAD_PIN_CAPTURES.with(std::cell::Cell::get), 1);
        drop(ports);
    }
}
