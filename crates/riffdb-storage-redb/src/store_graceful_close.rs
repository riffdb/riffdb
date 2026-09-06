use super::*;

impl SharedRedb {
    pub(super) fn complete_graceful_close(
        self: &Arc<Self>,
    ) -> crate::GracefulCheckpointCloseReceiptV1 {
        let mut elapsed_us = [0_u64; 3];
        let started = Instant::now();
        if self.complete_graceful_close_barrier().is_err() {
            elapsed_us[0] = saturating_elapsed_microseconds(started);
            return crate::GracefulCheckpointCloseReceiptV1::barrier_failed(elapsed_us);
        }
        elapsed_us[0] = saturating_elapsed_microseconds(started);

        let started = Instant::now();
        if self
            .before_test_commit(RedbTestOperation::GracefulCheckpointClassification)
            .is_err()
        {
            elapsed_us[1] = saturating_elapsed_microseconds(started);
            return crate::GracefulCheckpointCloseReceiptV1::classification_failed(elapsed_us);
        }
        let read = match self.database.begin_read().map_err(transaction_error) {
            Ok(read) => read,
            Err(_) => {
                elapsed_us[1] = saturating_elapsed_microseconds(started);
                return crate::GracefulCheckpointCloseReceiptV1::classification_failed(elapsed_us);
            }
        };
        let disposition = match crate::validated_prefix::classify_graceful_checkpoint(
            &read,
            self.terminal_execution_failure_rows(),
            self.startup_validation_clean(),
        ) {
            Ok(disposition) => disposition,
            Err(_) => {
                elapsed_us[1] = saturating_elapsed_microseconds(started);
                return crate::GracefulCheckpointCloseReceiptV1::classification_failed(elapsed_us);
            }
        };
        if self
            .after_test_commit(RedbTestOperation::GracefulCheckpointClassification)
            .is_err()
        {
            elapsed_us[1] = saturating_elapsed_microseconds(started);
            return crate::GracefulCheckpointCloseReceiptV1::classification_failed(elapsed_us);
        }
        drop(read);
        elapsed_us[1] = saturating_elapsed_microseconds(started);

        let started = Instant::now();
        let mut write = match self.database.begin_write().map_err(transaction_error) {
            Ok(write) => write,
            Err(_) => {
                elapsed_us[2] = saturating_elapsed_microseconds(started);
                return crate::GracefulCheckpointCloseReceiptV1::completed(
                    disposition,
                    crate::validated_prefix::AttemptedGracefulLifecycleOutcome::Failed,
                    elapsed_us,
                );
            }
        };
        if write.set_durability(Durability::Immediate).is_err() {
            let _ = write.abort();
            elapsed_us[2] = saturating_elapsed_microseconds(started);
            return crate::GracefulCheckpointCloseReceiptV1::completed(
                disposition,
                crate::validated_prefix::AttemptedGracefulLifecycleOutcome::Failed,
                elapsed_us,
            );
        }
        let lifecycle = match self.write_final_clean_close_lifecycle_after_barrier(write) {
            Ok(()) => crate::validated_prefix::AttemptedGracefulLifecycleOutcome::Committed,
            Err(error) if error.kind() == StorageErrorKind::CommitStatusUnknown => {
                crate::validated_prefix::AttemptedGracefulLifecycleOutcome::Unknown
            }
            Err(_) => crate::validated_prefix::AttemptedGracefulLifecycleOutcome::Failed,
        };
        elapsed_us[2] = saturating_elapsed_microseconds(started);
        crate::GracefulCheckpointCloseReceiptV1::completed(disposition, lifecycle, elapsed_us)
    }

    fn complete_graceful_close_barrier(self: &Arc<Self>) -> Result<(), StorageError> {
        self.before_test_commit(RedbTestOperation::GracefulCloseBarrier)?;
        // Materialize every acknowledged published suffix before inspecting
        // checkpoint bytes. Failure leaves lifecycle DIRTY.
        self.checkpoint_published_journal_suffix_for_barrier()?;
        self.before_test_commit(RedbTestOperation::GracefulCloseBarrierSuffix)?;
        // Even a never-written database needs the selected recyclable extent
        // header named by ADR-0157. Initializing the runtime creates that
        // canonical empty extent; the spare is scratch and is removed before
        // certification because clean evidence permits no scratch name.
        {
            let mut frontier = self
                .durable_read_frontier
                .write()
                .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            if frontier.is_none() {
                *frontier = Some(self.capture_checkpoint_root()?);
                self.retire_current_read_root();
            }
        }
        drop(self.journal_runtime()?);
        let spare = crate::journal::spare_journal_path(&self.path);
        match self.journal_media.remove_file(&spare) {
            Ok(()) => crate::journal::sync_parent_directory_with_media(
                self.journal_media.as_ref(),
                &spare,
            )
            .map_err(journal_io_error)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
        }
        // Verify the selected active extent is the exact empty frontier before
        // checkpoint classification. The final CLEAN path rereads these roots.
        let transaction = self.database.begin_read().map_err(transaction_error)?;
        let database_id = read_identity_from_read_transaction(&transaction)?;
        let application_frontier = read_commit_tail(&transaction)?;
        let administration_frontier = read_administration_tail(&transaction)?;
        crate::journal::verify_clean_close_header_digest_with_media(
            self.journal_media.as_ref(),
            &self.path,
            database_id,
            application_frontier,
            administration_frontier,
        )
        .map_err(recovery_journal_error)?;
        self.after_test_commit(RedbTestOperation::GracefulCloseBarrier)
    }

    fn write_final_clean_close_lifecycle_after_barrier(
        self: &Arc<Self>,
        write: WriteTransaction,
    ) -> Result<(), StorageError> {
        let database_id = read_identity_from_write_transaction(&write)?;
        let meta = write.open_table(META).map_err(table_error)?;
        let history = meta
            .get(META_HISTORY_INCARNATION)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let history_incarnation = *decode_history_incarnation_v1(history.value())
            .map_err(crate::error::codec_error)?
            .value();
        let lifecycle_bytes = meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(lifecycle_bytes.value())
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        if lifecycle.database_id() != database_id
            || lifecycle.history_incarnation() != history_incarnation
            || lifecycle.state() != crate::clean_close::CleanCloseState::Dirty
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(lifecycle_bytes);
        drop(history);
        drop(meta);
        let application_frontier = read_commit_tail_from_write(&write)?;
        let administration_frontier = read_administration_tail_from_write(&write)?;
        let journal_header_digest = crate::journal::verify_clean_close_header_digest_with_media(
            self.journal_media.as_ref(),
            &self.path,
            database_id,
            application_frontier,
            administration_frontier,
        )
        .map_err(recovery_journal_error)?;
        let binding = crate::clean_close::bounded_state_binding_hash_for_write(
            &write,
            journal_header_digest,
        )?;

        let clean = lifecycle
            .successor_clean(binding)
            .map_err(|error| match error {
                crate::clean_close::CleanCloseCodecError::GenerationExhausted => {
                    storage_error(StorageErrorKind::SequenceExhausted)
                }
                crate::clean_close::CleanCloseCodecError::Invalid => {
                    storage_error(StorageErrorKind::CorruptData)
                }
            })?;
        let encoded = clean
            .encode()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        let mut meta = write.open_table(META).map_err(table_error)?;
        let current = meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if crate::clean_close::CleanCloseLifecycle::decode(current.value()).ok() != Some(lifecycle)
        {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(current);
        meta.insert(META_CLEAN_CLOSE_LIFECYCLE, encoded.as_slice())
            .map_err(precommit_storage_error)?;
        drop(meta);
        self.before_test_commit(RedbTestOperation::CleanCloseLifecycle)?;
        self.commit_durable(write)?;
        self.after_test_commit(RedbTestOperation::CleanCloseLifecycle)
    }
}

fn read_commit_tail_from_write(
    transaction: &WriteTransaction,
) -> Result<Option<CommitSequence>, StorageError> {
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let events = transaction.open_table(EVENTS).map_err(table_error)?;
    let retained = crate::command_authority::command_authority_head(&commits, &events)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let watermark = meta
        .get(META_RETENTION_WATERMARK)
        .map_err(precommit_storage_error)?
        .map(|encoded| {
            riffdb_storage_api::proto_codec::decode_retention_watermark_v1(encoded.value())
                .map(|item| item.value().watermark_sequence())
                .map_err(crate::error::codec_error)
        })
        .transpose()?
        .unwrap_or(0);
    if watermark == 0 {
        return Ok(retained);
    }
    let pruned = CommitSequence::new(watermark)
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    match retained {
        Some(retained) if retained <= pruned => Err(storage_error(StorageErrorKind::CorruptData)),
        Some(retained) => Ok(Some(retained)),
        None => Ok(Some(pruned)),
    }
}

fn read_administration_tail_from_write(
    transaction: &WriteTransaction,
) -> Result<Option<AdministrationSequence>, StorageError> {
    let physical = transaction
        .open_table(AUDIT)
        .map_err(table_error)?
        .last()
        .map_err(precommit_storage_error)?
        .map(|(key, _)| crate::keys::decode_audit_key(key.value()))
        .transpose()
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    let command = match commits.last().map_err(precommit_storage_error)? {
        None => None,
        Some((_, encoded)) => {
            match riffdb_storage_api::decode_command_segment_v1(encoded.value()) {
                Ok(segment) => Some(segment.value().last_administration_sequence()),
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    None
                }
                Err(error) => return Err(crate::error::codec_error(error)),
            }
        }
    };
    match (physical, command) {
        (Some(physical), Some(command)) if physical == command => {
            Err(storage_error(StorageErrorKind::CorruptData))
        }
        (Some(physical), Some(command)) => Ok(Some(physical.max(command))),
        (Some(sequence), None) | (None, Some(sequence)) => Ok(Some(sequence)),
        (None, None) => Ok(None),
    }
}
