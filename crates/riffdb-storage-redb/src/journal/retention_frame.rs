//! Closed logical V4 prune-frame codec (ADR-0263 Amendment 5).

use super::*;

pub(super) fn retention_prune_mutation_is_closed(mutation: &JournalMutation) -> bool {
    match mutation.table() {
        JournalTable::Meta => {
            (mutation.key() == crate::layout::META_RETENTION_WATERMARK.as_bytes()
                && mutation.value().is_some())
                || (mutation.key() == crate::layout::META_VALIDATED_PREFIX_CHECKPOINT.as_bytes()
                    && mutation.value().is_none())
                || crate::changelog_v3::journal_allocator_assignment(mutation).is_ok()
        }
        JournalTable::HistoryTombstones
        | JournalTable::Idempotency
        | JournalTable::Provenance
        | JournalTable::Audit
        | JournalTable::AuditByRequest
        | JournalTable::EventRoutes => mutation.value().is_some(),
        JournalTable::Commits
        | JournalTable::Events
        | JournalTable::Outbox
        | JournalTable::OutboxStatus
        | JournalTable::IdempotencyLocators
        | JournalTable::ProvenanceLocators
        | JournalTable::AuditByRequestLocators => mutation.value().is_none(),
        _ => false,
    }
}

/// Facts accumulated once while encoding; recovery independently derives them.
#[derive(Debug, Default)]
pub(super) struct RetentionPruneShape {
    watermark: Option<riffdb_storage_api::StoredRetentionWatermarkV1>,
    tombstone: Option<riffdb_storage_api::StoredHistoryTombstoneV1>,
    command_deletes: std::collections::BTreeSet<u64>,
    event_bounds: Option<(u64, u64)>,
    invalid: bool,
}

impl RetentionPruneShape {
    pub(super) fn observe(&mut self, mutation: &JournalMutation) {
        if self.invalid {
            return;
        }
        if !retention_prune_mutation_is_closed(mutation) {
            self.invalid = true;
            return;
        }
        match mutation.table() {
            JournalTable::HistoryTombstones => {
                let decoded = mutation
                    .value()
                    .and_then(|value| riffdb_storage_api::decode_history_tombstone_v1(value).ok());
                match decoded {
                    Some(decoded) if self.tombstone.is_none() => {
                        let row = decoded.into_parts().0;
                        let key = decode_application_sequence_key(mutation.key()).ok();
                        self.invalid |= key.map(CommitSequence::get) != Some(row.first_sequence())
                            || mutation.expected_hash().is_some();
                        self.tombstone = Some(row);
                    }
                    _ => self.invalid = true,
                }
            }
            JournalTable::Meta
                if mutation.key() == crate::layout::META_RETENTION_WATERMARK.as_bytes() =>
            {
                let decoded = mutation.value().and_then(|value| {
                    riffdb_storage_api::decode_retention_watermark_v1(value).ok()
                });
                match decoded {
                    Some(decoded) if self.watermark.is_none() => {
                        self.watermark = Some(decoded.into_parts().0)
                    }
                    _ => self.invalid = true,
                }
            }
            JournalTable::Commits => match decode_application_sequence_key(mutation.key()) {
                Ok(sequence) if self.command_deletes.len() < 256 => {
                    self.invalid |= !self.command_deletes.insert(sequence.get());
                }
                _ => self.invalid = true,
            },
            JournalTable::Events | JournalTable::Outbox | JournalTable::OutboxStatus => {
                match crate::keys::decode_event_key(mutation.key()) {
                    Ok(event) => {
                        let sequence = event.commit_sequence().get();
                        self.event_bounds = Some(
                            self.event_bounds
                                .map_or((sequence, sequence), |(low, high)| {
                                    (low.min(sequence), high.max(sequence))
                                }),
                        );
                    }
                    Err(_) => self.invalid = true,
                }
            }
            _ => {}
        }
    }

    pub(super) fn valid(&self, covered: Option<CommitSequence>) -> bool {
        let (Some(watermark), Some(tombstone)) = (&self.watermark, &self.tombstone) else {
            return false;
        };
        let count = tombstone
            .last_sequence()
            .checked_sub(tombstone.first_sequence())
            .and_then(|span| span.checked_add(1));
        !self.invalid
            && covered.is_some_and(|head| tombstone.last_sequence() <= head.get())
            && count.is_some_and(|count| (1..=256).contains(&count))
            && count == Some(tombstone.commits_count())
            && self.command_deletes.first() == Some(&tombstone.first_sequence())
            && self
                .command_deletes
                .last()
                .is_some_and(|last| *last <= tombstone.last_sequence())
            && self.event_bounds.is_none_or(|(first, last)| {
                first >= tombstone.first_sequence() && last <= tombstone.last_sequence()
            })
            && watermark.watermark_sequence() == tombstone.last_sequence()
            && watermark.history_incarnation() == tombstone.history_incarnation()
            && tombstone.previous_tombstone_hash().is_some()
    }
}

pub(super) fn retention_prune_shape_is_valid(
    mutations: &[JournalMutation],
    covered: Option<CommitSequence>,
) -> bool {
    let mut shape = RetentionPruneShape::default();
    for mutation in mutations {
        shape.observe(mutation);
    }
    shape.valid(covered)
}

impl JournalFrame {
    /// The prune transition allocates a physical receipt, never a command or audit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn encode_buffered_retention_prune(
        database_id: DatabaseId,
        application: Option<CommitSequence>,
        administration: Option<AdministrationSequence>,
        previous_hash: [u8; HASH_BYTES],
        mutations: JournalMutationBuffer,
        changelog: Option<FrameChangelogBinding>,
    ) -> Result<EncodedJournalFrame, JournalCodecError> {
        mutations.finish(
            database_id,
            JournalFrameKind::RetentionPrune,
            application,
            application,
            administration,
            administration,
            1,
            0,
            0,
            previous_hash,
            changelog,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(mutations: Vec<JournalMutation>) -> Result<JournalFrame, JournalCodecError> {
        JournalFrame::checked(
            DatabaseId::from_bytes([7, 7, 7, 7, 7, 7, 0x77, 7, 0x87, 7, 7, 7, 7, 7, 7, 7])
                .expect("identity"),
            JournalFrameKind::RetentionPrune,
            CommitSequence::new(12),
            CommitSequence::new(12),
            AdministrationSequence::new(24),
            AdministrationSequence::new(24),
            1,
            0,
            0,
            [5; HASH_BYTES],
            mutations,
        )
    }

    // req: STO-030, STO-032, REC-005
    #[test]
    fn prune_frame_preserves_frontiers_and_refuses_prior_versions_and_open_tables() {
        let tombstone = riffdb_storage_api::StoredHistoryTombstoneV1::new(
            1,
            1,
            1,
            0,
            0,
            0,
            riffdb_storage_api::HistoryTombstoneContentDigest::from_bytes([3; 32]),
            Some(riffdb_storage_api::HistoryTombstoneHash::from_bytes(
                [4; 32],
            )),
            1,
        )
        .expect("tombstone");
        let watermark = riffdb_storage_api::StoredRetentionWatermarkV1::new(
            1,
            1,
            Some(riffdb_types::SchemaHash::from_bytes([5; 32])),
        )
        .expect("watermark");
        let mutations = vec![
            JournalMutation::delete_matching(
                JournalTable::Commits,
                1_u64.to_be_bytes().as_slice(),
                &[9],
            )
            .expect("checked delete"),
            JournalMutation::put(
                JournalTable::HistoryTombstones,
                1_u64.to_be_bytes().as_slice(),
                riffdb_storage_api::encode_history_tombstone_v1(&tombstone)
                    .expect("encode")
                    .as_bytes()
                    .to_vec(),
            )
            .expect("tombstone put"),
            JournalMutation::put(
                JournalTable::Meta,
                crate::layout::META_RETENTION_WATERMARK.as_bytes(),
                riffdb_storage_api::encode_retention_watermark_v1(&watermark)
                    .expect("encode")
                    .as_bytes()
                    .to_vec(),
            )
            .expect("watermark put"),
        ];
        for removed in 0..mutations.len() {
            let mut incomplete = mutations.clone();
            incomplete.remove(removed);
            assert!(
                self::frame(incomplete).is_err(),
                "command delete, tombstone, and watermark are all mandatory"
            );
        }
        let mut duplicate = mutations.clone();
        duplicate.push(mutations[1].clone());
        assert!(self::frame(duplicate).is_err(), "one tombstone per step");
        let mut outside = mutations.clone();
        outside[0] = JournalMutation::delete_matching(
            JournalTable::Commits,
            2_u64.to_be_bytes().as_slice(),
            &[9],
        )
        .expect("delete");
        assert!(
            self::frame(outside).is_err(),
            "deletions must fit the tombstone range"
        );
        let mut buffer = JournalMutationBuffer::default();
        buffer.extend(mutations.clone()).expect("buffer");
        let frame = frame(mutations).expect("prune frame");
        let encoded = frame.encode().expect("V4");
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/compatibility/journal-frame-v4.hex");
        let encoded_hex: String = encoded
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        if std::env::var_os("RIFFDB_REGENERATE_PRUNE_FRAME_FIXTURE").is_some() {
            std::fs::write(&fixture_path, encoded_hex + "\n").expect("write V4 fixture");
            return;
        }
        assert_eq!(
            std::fs::read_to_string(&fixture_path)
                .expect("frozen V4 fixture")
                .trim(),
            encoded_hex
        );
        let buffered = JournalFrame::encode_buffered_retention_prune(
            frame.database_id,
            frame.predecessor_sequence,
            frame.predecessor_administration_sequence,
            frame.previous_hash,
            buffer,
            None,
        )
        .expect("buffered prune");
        assert_eq!(buffered.as_bytes(), encoded.as_bytes());
        let (decoded, hash) = JournalFrame::decode(encoded.as_bytes()).expect("decode");
        assert_eq!(frame, decoded);
        assert_eq!(decoded.predecessor_sequence(), decoded.covered_sequence());
        assert_eq!(
            decoded.predecessor_administration_sequence(),
            decoded.covered_administration_sequence()
        );
        assert_eq!(
            JournalFrame::decode_proven(encoded.as_bytes(), &hash).expect("proven"),
            decoded
        );
        assert!(frame.encode_as(FRAME_VERSION_V2).is_err());
        assert!(frame.encode_as(FRAME_VERSION_V3).is_err());
        for table in [
            JournalTable::Entities,
            JournalTable::SecondaryIndexes,
            JournalTable::Commits,
        ] {
            assert!(
                self::frame(vec![
                    JournalMutation::put(table, vec![1], vec![2]).expect("put")
                ])
                .is_err()
            );
        }
        let mut downgraded = encoded.as_bytes().to_vec();
        downgraded[8..10].copy_from_slice(&FRAME_VERSION_V3.to_be_bytes());
        let footer = downgraded.len() - FRAME_FOOTER_BYTES;
        let hash = digest(&downgraded[..footer]);
        downgraded[footer + 16..].copy_from_slice(&hash);
        assert_eq!(
            JournalFrame::decode(&downgraded),
            Err(JournalCodecError::UnknownVersion)
        );
    }
}
