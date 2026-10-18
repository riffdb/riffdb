//! Closed administration and retention submission through the existing journal lane.

use super::*;

impl RedbWriteAccess {
    pub(crate) fn stage_retention_mutation(
        &self,
        mutation: &crate::journal::JournalMutation,
    ) -> Result<(), StorageError> {
        if self.transaction.is_some()
            || !matches!(
                self.ownership,
                Some(RedbWriteOwnership::ServiceAudit { .. })
            )
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        self.composite_stage
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .apply_prune(mutation)?;
        self.journal_mutations
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .try_borrow_mut()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            .extend([mutation.clone()])
            .map_err(journal_storage_error)
    }

    pub(crate) fn submit_service_audit(
        self,
        results: Vec<riffdb_storage_api::ServiceAuditAppendResult>,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        self.submit_maintenance(results, None, None)
    }

    pub(crate) fn submit_retention_prune(
        self,
        through: CommitSequence,
        segments: Vec<Arc<riffdb_storage_api::StoredCommandSegmentV1>>,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        self.submit_maintenance(
            Vec::new(),
            Some(RetentionPrunePublication { through, segments }),
            None,
        )
    }

    pub(crate) fn submit_source_reclamation(
        self,
        proof: crate::journal::SourceReclamationProof,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        self.submit_maintenance(Vec::new(), None, Some(proof))
    }

    fn submit_maintenance(
        mut self,
        results: Vec<riffdb_storage_api::ServiceAuditAppendResult>,
        prune: Option<RetentionPrunePublication>,
        source_cut: Option<crate::journal::SourceReclamationProof>,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        let ownership = self
            .ownership
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if !matches!(ownership, RedbWriteOwnership::ServiceAudit { .. }) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let audit_count = results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    riffdb_storage_api::ServiceAuditAppendResult::Appended(_)
                )
            })
            .count();
        let transition_count = if prune.is_some() || source_cut.is_some() {
            1
        } else {
            audit_count
        };
        let operation = if prune.is_some() {
            RedbTestOperation::OnlineRetentionStep
        } else {
            RedbTestOperation::ServiceAudit
        };
        let attribution = if source_cut.is_some() {
            riffdb_storage_api::ChangelogAttributionV3::HistoryReclamation
        } else if prune.is_some() {
            riffdb_storage_api::ChangelogAttributionV3::RetentionPrune
        } else {
            riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit
        };
        let kind = if source_cut.is_some() {
            riffdb_storage_api::CompositeFrameKindV1::SourceReclamation
        } else if prune.is_some() {
            riffdb_storage_api::CompositeFrameKindV1::RetentionPrune
        } else {
            riffdb_storage_api::CompositeFrameKindV1::ServiceAudit
        };
        if transition_count == 0
            || transition_count > riffdb_storage_api::MAX_GROUPED_WRITE_TRANSITIONS
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let changelog_predecessor = self.shared.stage_changelog_allocator(
            &mut self
                .journal_mutations
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .borrow_mut(),
            &mut self
                .composite_stage
                .as_ref()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
                .borrow_mut(),
        )?;
        let mut prospective_encoded_bytes = self
            .journal_mutations
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .borrow()
            .encoded_frame_len()
            .map_err(journal_storage_error)?;
        if let Some(proof) = &source_cut {
            prospective_encoded_bytes = prospective_encoded_bytes
                .checked_add(4)
                .and_then(|n| n.checked_add(proof.encode().ok()?.len()))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        }
        let prospective_physical_bytes =
            crate::journal::extent_frame_bytes(prospective_encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let checkpoint_rebased = if source_cut.is_some() {
            if !self.shared.has_journal_frame_headroom(
                transition_count,
                prospective_encoded_bytes,
                prospective_physical_bytes,
            )? {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            false
        } else {
            self.shared.ensure_journal_frame_headroom(
                transition_count,
                prospective_encoded_bytes,
                prospective_physical_bytes,
            )?
        };
        if checkpoint_rebased {
            self.composite_predecessor = Some(self.shared.capture_or_initialize_composite_view()?);
        }
        let mutations = self
            .journal_mutations
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        let mut composite_stage = self
            .composite_stage
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .into_inner();
        let composite_predecessor = self
            .composite_predecessor
            .take()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if mutations.mutation_count() == self.journal_mutation_start {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if usize::try_from(mutations.mutation_count()).ok()
            != Some(composite_stage.mutation_count())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        if let Some(controller) = &self.shared.test_controller {
            controller.before_commit(operation)?;
        }
        if self.transaction.take().is_some() {
            self.shared.fence_writes();
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let covered_sequence = composite_predecessor.overlay().published_application();
        let covered_administration_sequence = if prune.is_some() || source_cut.is_some() {
            composite_predecessor.overlay().published_administration()
        } else {
            results
                .iter()
                .filter_map(|result| match result {
                    riffdb_storage_api::ServiceAuditAppendResult::Appended(record) => {
                        Some(record.administration_sequence())
                    }
                    riffdb_storage_api::ServiceAuditAppendResult::PhaseConflict => None,
                })
                .next_back()
        };
        let (
            receipt,
            encoded_bytes,
            predecessor_sequence,
            predecessor_administration_sequence,
            composite_successor,
        ) = {
            let (checkpoint_transitions, checkpoint_bytes) =
                self.shared.async_checkpoint_charge()?;
            let mut runtime_guard = self.shared.journal_runtime()?;
            let runtime = runtime_guard
                .as_mut()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
            if covered_sequence != runtime.last_sequence {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            let audit_count = u16::try_from(audit_count)
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
            let predecessor_sequence = runtime.last_sequence;
            let predecessor_administration_sequence = runtime.last_administration_sequence;
            // Derive the receipt binding before encoding so the frame records it
            // (ADR-0263 Amendment 2 B4). The stage holds exactly the mutations
            // its seal below validates and returns.
            let changelog_successor = runtime.prepare_changelog_successor(
                changelog_predecessor,
                attribution,
                DualFrontier::new(covered_sequence, covered_administration_sequence),
                composite_stage.staged_mutations(),
            )?;
            let frame = if let Some(proof) = &source_cut {
                if Some(proof.predecessor()) != changelog_predecessor {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                let frame = crate::journal::JournalFrame::source_reclamation(
                    proof.clone(),
                    runtime.last_hash,
                )
                .map_err(journal_storage_error)?;
                let exact = frame
                    .mutations()
                    .iter()
                    .map(|mutation| mutation.composite())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
                if exact.as_slice() != composite_stage.staged_mutations()
                    || mutations.mutation_count() != 1
                {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                frame.encode().map_err(journal_storage_error)?
            } else if prune.is_some() {
                crate::journal::JournalFrame::encode_buffered_retention_prune(
                    runtime.database_id,
                    predecessor_sequence,
                    predecessor_administration_sequence,
                    runtime.last_hash,
                    mutations,
                    frame_changelog_binding(changelog_successor.as_ref()),
                )
                .map_err(journal_storage_error)?
            } else {
                crate::journal::JournalFrame::encode_buffered_service_audit(
                    runtime.database_id,
                    predecessor_sequence,
                    predecessor_administration_sequence,
                    covered_administration_sequence,
                    audit_count,
                    runtime.last_hash,
                    mutations,
                    frame_changelog_binding(changelog_successor.as_ref()),
                )
                .map_err(journal_storage_error)?
            };
            let encoded_bytes = frame.as_bytes().len();
            if encoded_bytes != prospective_encoded_bytes {
                self.shared.fence_writes();
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
            if checkpoint_rebased {
                composite_stage = self
                    .shared
                    .rebuild_composite_stage_for_frame(&composite_predecessor, &frame)?;
            }
            let physical_bytes = crate::journal::extent_frame_bytes(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_transitions = runtime
                .suffix_transitions
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_audits = runtime
                .suffix_audits
                .checked_add(usize::from(audit_count))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_bytes = runtime
                .suffix_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_physical_bytes = runtime
                .suffix_physical_bytes
                .checked_add(physical_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_transitions = runtime
                .unpublished_transitions
                .checked_add(transition_count)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_audits = runtime
                .unpublished_audits
                .checked_add(usize::from(audit_count))
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            let next_unpublished_bytes = runtime
                .unpublished_bytes
                .checked_add(encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            if checkpoint_transitions.saturating_add(next_transitions)
                > crate::journal::MAX_JOURNAL_SUFFIX_TRANSITIONS
                || checkpoint_bytes.saturating_add(next_bytes)
                    > crate::journal::MAX_JOURNAL_SUFFIX_BYTES
                || next_physical_bytes > self.shared.extent_profile.extent_data_bytes()
                || next_unpublished_transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
                || next_unpublished_bytes > self.shared.extent_profile.max_frame_bytes()
            {
                return Err(storage_error(StorageErrorKind::LimitExceeded));
            }
            let frame_hash = frame.frame_hash();
            let previous_hash = runtime.last_hash;
            let (mut composite_successor, checkpoint_mutations) = composite_stage
                .seal_encoded_frame_with_mutations(
                    kind,
                    runtime.database_id,
                    predecessor_sequence,
                    predecessor_sequence,
                    predecessor_administration_sequence,
                    covered_administration_sequence,
                    u16::try_from(transition_count)
                        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                    encoded_bytes,
                    previous_hash,
                    frame_hash,
                )?;
            let checkpoint_mutations: Arc<[riffdb_storage_api::CompositeMutationV1]> =
                Arc::from(checkpoint_mutations);
            composite_successor.append_changelog_source(
                changelog_successor,
                attribution,
                Arc::clone(&checkpoint_mutations),
                encoded_bytes,
            )?;
            if let Some(proof) = &source_cut {
                composite_successor.apply_source_cut(proof)?;
            }
            let composite_successor = Arc::new(composite_successor);
            let retained_frame = frame.clone();
            let predecessor = RedbReadAccess::Composite(Arc::clone(&composite_predecessor));
            let footprint =
                crate::source_continuation::SourceContinuationFootprint::composite_checked(
                    predecessor_administration_sequence,
                    &checkpoint_mutations,
                    |mutation| {
                        predecessor.read_value(crate::journal::JournalTable::Audit, mutation.key())
                    },
                    |sequence| {
                        let end = crate::keys::encode_application_sequence_key(
                            sequence.checked_next().ok_or_else(|| {
                                storage_error(StorageErrorKind::SequenceExhausted)
                            })?,
                        );
                        Ok(predecessor
                            .read_range_reverse(
                                crate::journal::JournalTable::Commits,
                                &[],
                                &end,
                                1,
                            )?
                            .pop()
                            .map(|(key, value)| (key.into_vec(), value.into_vec())))
                    },
                )?;
            if source_cut.is_some() || changelog_successor.is_none() || !footprint.is_append_only()
            {
                self.shared.invalidate_source_continuation();
            }
            let receipt = runtime.lane.submit(frame).map_err(journal_io_error)?;
            runtime.changelog_history = if source_cut.is_some() {
                crate::changelog_v3_cursor::history(&RedbReadAccess::Composite(Arc::clone(
                    &composite_successor,
                )))
                .map(Some)
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?
            } else {
                changelog_successor.map(|(_, history)| history)
            };
            runtime.last_administration_sequence = covered_administration_sequence;
            runtime.last_hash = frame_hash;
            runtime.suffix_transitions = next_transitions;
            runtime.suffix_audits = next_audits;
            runtime.suffix_bytes = next_bytes;
            runtime.suffix_physical_bytes = next_physical_bytes;
            let placement = crate::journal::frame_placement_after(
                next_physical_bytes - physical_bytes,
                &retained_frame,
            )
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
            runtime.suffix_frames.push(ValidatedCheckpointFrame {
                source_reclamation: source_cut.as_ref().map(|proof| Arc::new(proof.clone())),
                segment_summaries: Arc::from(Vec::new()),
                extent_generation: runtime.lane.generation(),
                placement,
                database_id: runtime.database_id,
                predecessor_sequence,
                covered_sequence: predecessor_sequence,
                predecessor_administration_sequence,
                covered_administration_sequence,
                previous_hash,
                frame_hash,
                transition_count: u16::try_from(transition_count)
                    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                command_count: 0,
                audit_count,
                encoded: retained_frame,
                mutations: checkpoint_mutations,
                changelog_binding: changelog_successor.map(|(binding, _)| binding),
            });
            runtime.unpublished_transitions = next_unpublished_transitions;
            runtime.unpublished_audits = next_unpublished_audits;
            runtime.unpublished_bytes = next_unpublished_bytes;
            (
                receipt,
                encoded_bytes,
                predecessor_sequence,
                predecessor_administration_sequence,
                composite_successor,
            )
        };
        if let Err(error) = self
            .shared
            .install_private_composite_successor(&composite_predecessor, &composite_successor)
        {
            self.shared.fence_writes();
            return Err(error);
        }
        let successor = composite_predecessor.checkpoint_root_shared();
        let source_successor = source_cut.as_ref().map(|_| {
            (
                Arc::clone(&composite_successor),
                self.shared
                    .source_continuation_epoch
                    .load(Ordering::Acquire),
            )
        });
        let publication_payload =
            PendingPublicationPayload::ServiceAudit(ServiceAuditPublication {
                successor: Arc::clone(&successor),
                transition_count,
                audit_count,
                prune,
                source_reclamation: source_cut.is_some(),
                encoded_bytes,
                predecessor_sequence,
                covered_sequence,
                predecessor_administration_sequence,
                covered_administration_sequence,
                composite_predecessor: Arc::clone(&composite_predecessor),
                composite_successor,
            });
        let publication_ticket = match self
            .shared
            .register_publication(receipt.clone(), publication_payload)
        {
            Ok(ticket) => ticket,
            Err(error) => {
                self.shared.fence_writes();
                return Err(error);
            }
        };
        let fence = RedbSubmittedServiceAuditFence {
            shared: Arc::clone(&self.shared),
            receipt: Some(receipt),
            results: Some(results),
            covered_sequence,
            covered_administration_sequence,
            publication_ticket,
            completed: false,
            source_successor,
        };
        self.shared.finish_source_requested_publication()?;
        drop(ownership);
        Ok(fence)
    }
}

impl SharedRedb {
    pub(super) fn capture_source_validation_pin(
        &self,
    ) -> Result<crate::source_validation_pin::ValidationSource, StorageError> {
        let published = self
            .composite_publication
            .read()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if let Some(published) = published.as_ref() {
            return published
                .capture()
                .map(crate::source_validation_pin::ValidationSource::Composite);
        }
        drop(published);
        // A validation capture must not populate the reusable application cache.
        self.capture_checkpoint_root()
            .map(crate::source_validation_pin::ValidationSource::Checkpoint)
    }

    pub(super) fn source_composite_for_write(
        self: &Arc<Self>,
        source: crate::source_validation_pin::ValidationSource,
    ) -> Result<Arc<crate::composite_view::RedbCompositeReadView>, StorageError> {
        if let crate::source_validation_pin::ValidationSource::Composite(view) = source {
            return Ok(view);
        }
        // A legal source cut can precede the first application journal write.
        // Initialize the same closed lane under the granted sole-writer lease;
        // this performs no suffix drain and its full residence is charged.
        let mut frontier = self
            .durable_read_frontier
            .write()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if frontier.is_none() {
            *frontier = Some(Arc::clone(source.checkpoint_pin()));
        }
        self.retire_current_read_root();
        drop(frontier);
        drop(self.journal_runtime()?);
        self.capture_or_initialize_composite_view()
    }

    pub(super) fn has_journal_frame_headroom(
        &self,
        transitions: usize,
        bytes: usize,
        physical_bytes: usize,
    ) -> Result<bool, StorageError> {
        let (checkpoint_transitions, checkpoint_bytes) = self.async_checkpoint_charge()?;
        let runtime = self.journal_runtime()?;
        let runtime = runtime
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        Ok(JournalCapacityCharge {
            checkpoint_transitions,
            checkpoint_bytes,
            suffix_transitions: runtime.suffix_transitions,
            suffix_bytes: runtime.suffix_bytes,
            suffix_physical_bytes: runtime.suffix_physical_bytes,
            unpublished_transitions: runtime.unpublished_transitions,
            unpublished_bytes: runtime.unpublished_bytes,
        }
        .admits_for_profile(self.extent_profile, transitions, bytes, physical_bytes))
    }

    pub(super) fn begin_source_journal_write(
        self: &Arc<Self>,
        lease: ExclusiveLease,
        predecessor: Arc<crate::composite_view::RedbCompositeReadView>,
    ) -> Result<RedbWriteAccess, StorageError> {
        if self.write_fenced.load(Ordering::Acquire)
            || self.is_follower_mode()
            || self.application_commit_profile != RedbCommitProfile::Standard
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        // The source owner supplied its ordinarily granted FIFO lease and exact
        // acknowledged pin. No checkpoint polling, headroom drain or lane creation.
        let stage = crate::composite_view::RedbCompositeMutationStage::from_published(&predecessor);
        Ok(RedbWriteAccess {
            shared: Arc::clone(self),
            transaction: None,
            ownership: Some(RedbWriteOwnership::ServiceAudit { _lease: lease }),
            direct_changelog_attribution: None,
            journal_mutations: Some(RefCell::new(
                crate::journal::JournalMutationBuffer::default(),
            )),
            journal_mutation_start: 0,
            journal_checkpoint: None,
            composite_predecessor: Some(predecessor),
            composite_stage: Some(RefCell::new(stage)),
            command_prefix_capture: RefCell::default(),
        })
    }

    pub(super) fn finish_source_requested_publication(&self) -> Result<(), StorageError> {
        if self.mutation_gate.source_publication_requested()? {
            self.publish_pending_journal_prefix()?;
        }
        Ok(())
    }

    pub(crate) fn uses_journaled_retention(&self) -> bool {
        self.application_commit_profile == RedbCommitProfile::Standard
    }
}
