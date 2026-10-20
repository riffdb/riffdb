//! Shared Original segment/event/sequence traversal; only closed native transports.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use super::source_offline_original::OfflineSourceCommandReadV1;
use super::source_original_retention::OriginalLogicalRead;
use super::*;
use riffdb_storage_api::{
    ProjectionSourceCommandReadV1, ProjectionSourceRecordDecodeErrorV1, StoredCommitRecordV1,
};

pub(super) enum SourceCommandReadV1<'a, 'read, 'context, 'ports> {
    Primary(&'a mut OriginalLogicalRead<'read, 'context, 'ports>),
    Offline(&'a mut OfflineSourceCommandReadV1<'read>),
}
impl SourceCommandReadV1<'_, '_, '_, '_> {
    fn command(
        &mut self,
        physical: CommitSequence,
    ) -> Result<ProjectionSourceCommandReadV1, StorageError> {
        match self {
            Self::Primary(read) => read.command(physical),
            Self::Offline(read) => read.command(physical),
        }
    }
    fn join_event(
        &mut self,
        graph: &mut ProjectionSourceCommandReadV1,
        index: usize,
    ) -> Result<(), StorageError> {
        match self {
            Self::Primary(read) => read.join_event(graph, index),
            Self::Offline(read) => read.join_event(graph, index),
        }
    }
}

pub(super) struct SourceCommandPrefixV1 {
    graph: Option<ProjectionSourceCommandReadV1>,
    frontier: Option<CommitSequence>,
    next: Option<CommitSequence>,
    next_physical: Option<CommitSequence>,
    index: usize,
}
impl SourceCommandPrefixV1 {
    pub(super) fn new(frontier: Option<CommitSequence>) -> Self {
        Self {
            graph: None,
            frontier,
            next: Some(CommitSequence::first()),
            next_physical: Some(CommitSequence::first()),
            index: 0,
        }
    }
    pub(super) fn prepare_next(&mut self) -> Option<(CommitSequence, bool)> {
        let Some(expected) = self
            .next
            .filter(|n| self.frontier.is_some_and(|frontier| *n <= frontier))
        else {
            self.graph = None;
            return None;
        };
        let exhausted = self
            .graph
            .as_ref()
            .is_none_or(|graph| self.index == graph.commit_count());
        if exhausted {
            self.graph = None;
            self.index = 0;
        }
        Some((expected, exhausted))
    }
    pub(super) fn next_commit(
        &mut self,
        read: &mut SourceCommandReadV1<'_, '_, '_, '_>,
    ) -> Result<Option<&StoredCommitRecordV1>, StorageError> {
        let Some((expected, exhausted)) = self.prepare_next() else {
            return Ok(None);
        };
        if exhausted {
            let physical = self.next_physical.ok_or_else(cursor_corrupt)?;
            let mut graph = read.command(physical)?;
            for index in 0..graph.required_events().len() {
                read.join_event(&mut graph, index)?;
            }
            graph.finish_event_join().map_err(source_codec_error)?;
            if graph.commit_count() == 0 {
                return Err(cursor_corrupt());
            }
            let mut sequence = Some(physical);
            for index in 0..graph.commit_count() {
                let commit = graph.commit(index).ok_or_else(cursor_corrupt)?;
                if Some(commit.commit_sequence()) != sequence {
                    return Err(cursor_corrupt());
                }
                sequence = commit.commit_sequence().checked_next();
            }
            if physical != expected {
                return Err(cursor_corrupt());
            }
            // Install only after all fields, joins and sequence checks succeed.
            self.next_physical = sequence;
            self.graph = Some(graph);
        }
        let graph = self.graph.as_ref().ok_or_else(cursor_corrupt)?;
        let commit = graph.commit(self.index).ok_or_else(cursor_corrupt)?;
        if commit.commit_sequence() != expected {
            return Err(cursor_corrupt());
        }
        self.index = self.index.checked_add(1).ok_or_else(cursor_corrupt)?;
        self.next = expected.checked_next();
        Ok(Some(commit))
    }
}
fn cursor_corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
fn source_codec_error(error: ProjectionSourceRecordDecodeErrorV1) -> StorageError {
    match error {
        ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable => {
            storage_error(StorageErrorKind::Unavailable)
        }
        _ => cursor_corrupt(),
    }
}
