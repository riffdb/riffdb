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
        self.submit_maintenance(results, None)
    }

    pub(crate) fn submit_retention_prune(
        self,
        through: CommitSequence,
        segments: Vec<Arc<riffdb_storage_api::StoredCommandSegmentV1>>,
    ) -> Result<RedbSubmittedServiceAuditFence, StorageError> {
        self.submit_maintenance(
            Vec::new(),
            Some(RetentionPrunePublication { through, segments }),
        )
    }

    fn submit_maintenance(
        mut self,
        results: Vec<riffdb_storage_api::ServiceAuditAppendResult>,
        prune: Option<RetentionPrunePublication>,
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
        let transition_count = if prune.is_some() { 1 } else { audit_count };
        let operation = if prune.is_some() {
            RedbTestOperation::OnlineRetentionStep
        } else {
            RedbTestOperation::ServiceAudit
        };
        let attribution = if prune.is_some() {
            riffdb_storage_api::ChangelogAttributionV3::RetentionPrune
        } else {
            riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit
        };
        let kind = if prune.is_some() {
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
        let prospective_encoded_bytes = self
            .journal_mutations
            .as_ref()
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
            .borrow()
            .encoded_frame_len()
            .map_err(journal_storage_error)?;
        let prospective_physical_bytes =
            crate::journal::extent_frame_bytes(prospective_encoded_bytes)
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        let checkpoint_rebased = self.shared.ensure_journal_frame_headroom(
            transition_count,
            prospective_encoded_bytes,
            prospective_physical_bytes,
        )?;
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
        let covered_administration_sequence = if prune.is_some() {
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
            let frame = if prune.is_some() {
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
                || next_physical_bytes > crate::journal::EXTENT_DATA_BYTES
                || next_unpublished_transitions > crate::journal::MAX_JOURNAL_TRANSITIONS
                || next_unpublished_bytes > crate::journal::MAX_JOURNAL_FRAME_BYTES
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
            let composite_successor = Arc::new(composite_successor);
            let retained_frame = frame.clone();
            let receipt = runtime.lane.submit(frame).map_err(journal_io_error)?;
            runtime.changelog_history = changelog_successor.map(|(_, history)| history);
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
        let publication_payload =
            PendingPublicationPayload::ServiceAudit(ServiceAuditPublication {
                successor: Arc::clone(&successor),
                transition_count,
                audit_count,
                prune,
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
        };
        drop(ownership);
        Ok(fence)
    }
}

impl SharedRedb {
    pub(crate) fn uses_journaled_retention(&self) -> bool {
        self.application_commit_profile == RedbCommitProfile::Standard
    }
}
