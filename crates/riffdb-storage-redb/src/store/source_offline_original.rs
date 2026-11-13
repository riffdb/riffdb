//! Actual consumed offline Original reader; no primary ports or repairing opener.
use riffdb_types::ProjectionSourceWorkingBudgetV1;

/// Fields private to Store. Only an actual facade can mint this one-use permit.
pub(crate) struct SourceOriginalHistoryConstructionV1 {
    budget: ProjectionSourceWorkingBudgetV1,
}
impl SourceOriginalHistoryConstructionV1 {
    pub(crate) fn budget(&self) -> &ProjectionSourceWorkingBudgetV1 {
        &self.budget
    }
}

#[cfg(test)]
use super::source_command_prefix::{SourceCommandPrefixV1, SourceCommandReadV1};
#[cfg(test)]
use super::source_native_read::{SourceNativeReadTransaction, SourceOwnedOfflineReadV1};
#[cfg(test)]
use super::*;
#[cfg(test)]
use redb::TableHandle;
#[cfg(test)]
use riffdb_storage_api::{
    ChangelogHistoryStateV3, ProjectionSourceCommandReadV1, StoredCommitRecordV1,
};

#[cfg(test)]
pub(crate) struct OfflineOriginalSourceCursorV1<'read> {
    prefix: SourceCommandPrefixV1,
    read: OfflineSourceCommandReadV1<'read>,
    _work: SourceReadWork,
}
#[cfg(test)]
impl Drop for OfflineOriginalSourceCursorV1<'_> {
    fn drop(&mut self) {}
}
#[cfg(test)]
impl OfflineOriginalSourceCursorV1<'_> {
    pub(crate) fn captured_history(&self) -> ChangelogHistoryStateV3 {
        self.read.history.captured_history()
    }
    #[cfg(feature = "projection-source-driver")]
    pub(in crate::store) fn belongs_to_session(
        &self,
        actual: &super::source_native_read::offline::OfflineSourceSessionCustodyV1,
    ) -> bool {
        actual.matches_read(&self.read.native)
    }
    #[cfg(feature = "projection-source-driver")]
    pub(in crate::store) fn rewind_original_prefix(&mut self) {
        // The immutable native pin/resolver stays fixed. Drop every old command
        // graph before the next pass starts; no new Current/head is captured.
        self.prefix =
            SourceCommandPrefixV1::new(self.captured_history().tail().frontier().application());
    }
    pub(crate) fn next_commit(&mut self) -> Result<Option<&StoredCommitRecordV1>, StorageError> {
        self.prefix
            .next_commit(&mut SourceCommandReadV1::Offline(&mut self.read))
    }
}

#[cfg(test)]
pub(super) struct OfflineSourceCommandReadV1<'read> {
    native: SourceNativeReadTransaction<'read>,
    history: crate::history_stub::SourceOriginalHistoryV1,
}
#[cfg(test)]
impl OfflineSourceCommandReadV1<'_> {
    fn validate(&mut self) -> Result<(), StorageError> {
        let history = self.history.captured_history();
        {
            let mut meta = self.native.open_str_table(META.name())?;
            let key = riffdb_storage_api::AuthoritativeNamespaceV1::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(corrupt)?;
            let row = meta.get(key)?.ok_or_else(corrupt)?;
            let actual =
                *riffdb_storage_api::proto_codec::decode_changelog_history_state_v3(row.value())
                    .map_err(crate::error::codec_error)?
                    .value();
            if actual != history {
                return Err(corrupt());
            }
        }
        {
            let mut meta = self.native.open_str_table(META.name())?;
            let row = meta.get(crate::layout::META_RETENTION_WATERMARK)?;
            let watermark = row
                .as_ref()
                .map(|value| {
                    riffdb_storage_api::proto_codec::decode_retention_watermark_v1(value.value())
                        .map(|value| value.value().watermark_sequence())
                        .map_err(crate::error::codec_error)
                })
                .transpose()?
                .unwrap_or(0);
            if watermark != 0 {
                return Err(storage_error(StorageErrorKind::HistoryPruned));
            }
        }
        let mut table = self
            .native
            .open_bytes_table(crate::changelog_v3_activation::HISTORY.name())?;
        let row = table
            .get(&history.tail().sequence().get().to_be_bytes())?
            .ok_or_else(corrupt)?;
        self.history.validate_terminal(row.value())
    }
    pub(super) fn command(
        &mut self,
        physical: CommitSequence,
    ) -> Result<ProjectionSourceCommandReadV1, StorageError> {
        let key = crate::keys::encode_application_sequence_key(physical);
        let mut table = self.native.open_bytes_table(COMMITS.name())?;
        let mut range = table.range_from(Some(&key))?;
        let row = range.next()?.ok_or_else(corrupt)?;
        if row.key() != key {
            return Err(corrupt());
        }
        self.history.command(row.value(), physical)
    }
    pub(super) fn join_event(
        &mut self,
        graph: &mut ProjectionSourceCommandReadV1,
        index: usize,
    ) -> Result<(), StorageError> {
        let key = crate::keys::encode_event_key(graph.required_events()[index].event_id());
        let mut table = self.native.open_bytes_table(EVENTS.name())?;
        let row = table.get(&key)?.ok_or_else(corrupt)?;
        graph
            .accept_event_canonical(row.value())
            .map_err(|error| match error {
                riffdb_storage_api::ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
                    storage_error(StorageErrorKind::Unavailable)
                }
                _ => corrupt(),
            })
    }
}

#[cfg(test)]
struct PendingOfflineOriginalV1<'read> {
    native: Option<SourceNativeReadTransaction<'read>>,
    history: Option<crate::history_stub::SourceOriginalHistoryV1>,
    work: Option<SourceReadWork>,
}
#[cfg(test)]
impl SourceOwnedOfflineReadV1 {
    pub(crate) fn original_cursor(
        &mut self,
    ) -> Result<OfflineOriginalSourceCursorV1<'_>, StorageError> {
        let (budget, allocation) = self.original_history_admission()?;
        let controls = std::mem::size_of::<OfflineOriginalSourceCursorV1<'_>>()
            .checked_add(std::mem::size_of::<PendingOfflineOriginalV1<'_>>())
            .and_then(|n| n.checked_add(std::mem::size_of::<SourceOriginalHistoryConstructionV1>()))
            .and_then(|n| u64::try_from(n).ok())
            .ok_or_else(unavailable)?;
        let bytes = allocation
            .checked_add(controls)
            .and_then(|n| n.checked_add(512 * 1024))
            .ok_or_else(unavailable)?;
        let work = SourceReadWork::admit(&budget, bytes)?;
        let mut pending = PendingOfflineOriginalV1 {
            native: None,
            history: None,
            work: Some(work),
        };
        pending.history =
            Some(self.construct_original_history(SourceOriginalHistoryConstructionV1 { budget })?);
        pending.native = Some(self.begin_read()?);
        match pending {
            PendingOfflineOriginalV1 {
                native: Some(native),
                history: Some(history),
                work: Some(work),
            } => {
                let frontier = history.captured_history().tail().frontier().application();
                let mut cursor = OfflineOriginalSourceCursorV1 {
                    prefix: SourceCommandPrefixV1::new(frontier),
                    read: OfflineSourceCommandReadV1 { native, history },
                    _work: work,
                };
                cursor.read.validate()?;
                Ok(cursor)
            }
            pending => {
                drop(pending);
                Err(unavailable())
            }
        }
    }
}
#[cfg(test)]
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
#[cfg(test)]
fn unavailable() -> StorageError {
    storage_error(StorageErrorKind::Unavailable)
}
