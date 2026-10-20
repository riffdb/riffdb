//! Exact-origin Source64 reads; never searches global same-database resolvers.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use super::*;
struct SourceFrame {
    frame: JournalFrame,
    _work: crate::store::SourceReadWork,
}
#[derive(Clone, Copy)]
enum FramePurpose {
    Receipt,
    Command,
}
impl HistoryResolver {
    fn source_frame(
        &self,
        stub: &HistoryStub,
        budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
        purpose: FramePurpose,
    ) -> Result<SourceFrame, StorageError> {
        if stub.database_id != self.database_id {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let path = match &self.location {
            ExtentLocation::Database(path) | ExtentLocation::Backup(path) => path,
        };
        let bytes = path
            .as_os_str()
            .len()
            .checked_mul(16)
            .and_then(|n| n.checked_add(32 * 1024))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let _path_work = budget
            .reserve(bytes)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let paths: Vec<PathBuf> = match &self.location {
            ExtentLocation::Database(path) => vec![
                crate::journal::sealed_journal_path(path, stub.generation),
                crate::journal::journal_path(path),
                crate::journal::checkpoint_journal_path(path),
                crate::journal::sealing_journal_path(path),
            ],
            ExtentLocation::Backup(directory) => {
                crate::backup::extent_artifacts(directory, stub.generation).into()
            }
        };
        // A pinned generation moves monotonically through at most these four
        // existing filenames. A missed full pass implies a forward rename;
        // another pass per possible location covers rotation without a gate.
        for _ in 0..paths.len() {
            let read = match purpose {
                FramePurpose::Receipt => crate::journal::read_source_retained_frame_v1,
                FramePurpose::Command => crate::journal::read_source_command_retained_frame_v1,
            };
            match read(
                self.media.as_ref(),
                &paths,
                stub.database_id,
                stub.placement,
                stub.generation,
                budget,
            ) {
                Ok((frame, work)) => return Ok(SourceFrame { frame, _work: work }),
                Err(crate::journal::JournalIoError::Capacity) => {
                    return Err(storage_error(StorageErrorKind::Unavailable));
                }
                Err(crate::journal::JournalIoError::Corrupt) => {}
                Err(_) => return Err(storage_error(StorageErrorKind::Unavailable)),
            }
        }
        Err(storage_error(StorageErrorKind::CorruptData))
    }
    pub(crate) fn validate_source_terminal_v1(
        &self,
        value: &[u8],
        history: riffdb_storage_api::ChangelogHistoryStateV3,
        budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
    ) -> Result<(), StorageError> {
        let stub = HistoryStub::decode(value)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if stub.kind != StubKind::Receipt || stub.database_id != history.lineage().database_id() {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let reserved = self.source_frame(&stub, budget, FramePurpose::Receipt)?;
        let recorded = reserved
            .frame
            .changelog()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let summary = crate::changelog_v3::summary_from_journal_for_catalog(
            &reserved.frame,
            recorded.binding(&reserved.frame),
            recorded.catalog_digest(),
        )
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
        summary
            .validate_terminal_receipt(history)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))
    }
    pub(crate) fn read_source_command_v1(
        &self,
        stub: &HistoryStub,
        physical: CommitSequence,
        budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
    ) -> Result<riffdb_storage_api::ProjectionSourceCommandReadV1, StorageError> {
        if stub.kind != StubKind::Segment || stub.summary.first_commit_sequence != Some(physical) {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        let reserved = self.source_frame(stub, budget, FramePurpose::Command)?;
        let key = crate::keys::encode_application_sequence_key(physical);
        let bytes = reserved
            .frame
            .mutations()
            .iter()
            .find(|row| row.table() == JournalTable::Commits && row.key() == key.as_slice())
            .and_then(|row| row.value())
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let graph = riffdb_storage_api::decode_projection_source_command_admitted_v1(bytes, budget)
            .map_err(|error| match error {
                riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
                    storage_error(StorageErrorKind::Unavailable)
                }
                _ => storage_error(StorageErrorKind::CorruptData),
            })?;
        let segment = graph
            .validated_segment_v1()
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if SegmentSummary::of(segment)? != stub.summary {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        Ok(graph)
    }
}

/// Fixed original backup resolver: never ambient registration or live pending data.
/// Its only construction consumes the Store-minted permit through the SAME
/// retained backup owner. The enclosing cursor owns its prepaid work last.
pub(crate) struct SourceOriginalHistoryV1 {
    resolver: HistoryResolver,
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    construction: crate::store::SourceOriginalHistoryConstructionV1,
}
impl SourceOriginalHistoryV1 {
    pub(crate) fn from_retained_backup(
        construction: crate::store::SourceOriginalHistoryConstructionV1,
        history: riffdb_storage_api::ChangelogHistoryStateV3,
        directory: &Path,
        media: Arc<dyn crate::media::JournalMedia>,
    ) -> Self {
        Self {
            resolver: HistoryResolver {
                database_id: history.lineage().database_id(),
                location: ExtentLocation::Backup(directory.to_path_buf()),
                media,
                pending: Mutex::new(PendingFrames::default()),
            },
            history,
            construction,
        }
    }
    pub(crate) fn captured_history(&self) -> riffdb_storage_api::ChangelogHistoryStateV3 {
        self.history
    }
    pub(crate) fn command(
        &self,
        bytes: &[u8],
        physical: CommitSequence,
    ) -> Result<riffdb_storage_api::ProjectionSourceCommandReadV1, StorageError> {
        decode_original_source_command_v1(
            bytes,
            physical,
            self.construction.budget(),
            Some(&self.resolver),
        )
    }
    pub(crate) fn validate_terminal(&self, bytes: &[u8]) -> Result<(), StorageError> {
        validate_original_source_terminal_v1(
            bytes,
            self.history,
            self.construction.budget(),
            Some(&self.resolver),
        )
    }
}

pub(crate) fn decode_original_source_command_v1(
    bytes: &[u8],
    physical: CommitSequence,
    budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
    resolver: Option<&HistoryResolver>,
) -> Result<riffdb_storage_api::ProjectionSourceCommandReadV1, StorageError> {
    let Some(stub) = HistoryStub::decode(bytes)? else {
        return riffdb_storage_api::proto_codec::decode_projection_source_command_admitted_v1(
            bytes, budget,
        )
        .map_err(|error| match error {
            riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
                storage_error(StorageErrorKind::Unavailable)
            }
            _ => storage_error(StorageErrorKind::CorruptData),
        });
    };
    resolver
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?
        .read_source_command_v1(&stub, physical, budget)
}
pub(crate) fn validate_original_source_terminal_v1(
    bytes: &[u8],
    history: riffdb_storage_api::ChangelogHistoryStateV3,
    budget: &riffdb_types::ProjectionSourceWorkingBudgetV1,
    resolver: Option<&HistoryResolver>,
) -> Result<(), StorageError> {
    if HistoryStub::decode(bytes)?.is_none() {
        return history
            .validate_terminal_canonical_v1(bytes)
            .map_err(|_| storage_error(StorageErrorKind::CorruptData));
    }
    resolver
        .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?
        .validate_source_terminal_v1(bytes, history, budget)
}
