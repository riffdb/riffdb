//! Closed lookup over legacy commits, command capsules, and bounded segments.

use std::ops::Bound::{Included, Unbounded};

use redb::ReadableTable;
use riffdb_storage_api::{
    EncodedPageItem, StorageError, StorageErrorKind, StoredCommandCapsuleV1,
    StoredCommandCapsuleV2, StoredCommandSegmentV1, StoredCommitRecordV1,
};
use riffdb_types::CommitSequence;

use crate::codec::{decode_command_capsule_with_event_table, decode_commit_with_event_table};
use crate::error::precommit_storage_error;
use crate::journal::JournalTable;
use crate::keys::{decode_application_sequence_key, encode_application_sequence_key};
use crate::store::RedbReadAccess;

/// Maximum physical command-authority bytes inspected by one export source call.
pub(crate) const MAX_APPLICATION_EXPORT_COMMAND_AUTHORITY_BYTES: usize = 16 * 1024 * 1024;

/// One proven command member reconstructed from its authoritative physical row.
pub(crate) enum CommandAuthorityMember {
    CapsuleV1(StoredCommandCapsuleV1),
    CapsuleV2(StoredCommandCapsuleV2),
}

impl CommandAuthorityMember {
    pub(crate) const fn base(&self) -> &StoredCommandCapsuleV1 {
        match self {
            Self::CapsuleV1(value) => value,
            Self::CapsuleV2(value) => value.base(),
        }
    }

    pub(crate) fn into_base(self) -> StoredCommandCapsuleV1 {
        match self {
            Self::CapsuleV1(value) => value,
            Self::CapsuleV2(value) => value.base().clone(),
        }
    }
}

/// One fully validated logical command view used by snapshot-bound export.
///
/// Historical commit-only rows own events but not provenance. Capsules and
/// segments own both classes directly.
pub(crate) enum ApplicationExportCommandEvidenceV1 {
    Command(Box<CommandAuthorityMember>),
    HistoricalCommit(Box<StoredCommitRecordV1>),
}

impl ApplicationExportCommandEvidenceV1 {
    pub(crate) const fn commit(&self) -> &StoredCommitRecordV1 {
        match self {
            Self::Command(command) => command.base().commit(),
            Self::HistoricalCommit(commit) => commit,
        }
    }

    pub(crate) fn provenance(&self) -> Option<&riffdb_storage_api::StoredProvenanceRecordV1> {
        match self {
            Self::Command(command) => Some(command.base().provenance()),
            Self::HistoricalCommit(_) => None,
        }
    }
}

/// Expands one captured physical command row into complete logical evidence.
///
/// The caller reads exactly one physical row per source call. This boundary
/// validates its byte ceiling, physical first sequence, and contiguous logical
/// members before any event or provenance can be exported.
pub(crate) fn application_export_evidence_in_physical_row_access(
    access: &RedbReadAccess,
    encoded: &[u8],
    physical_sequence: CommitSequence,
) -> Result<Vec<ApplicationExportCommandEvidenceV1>, StorageError> {
    if encoded.len() > MAX_APPLICATION_EXPORT_COMMAND_AUTHORITY_BYTES {
        return Err(StorageError::new(StorageErrorKind::LimitExceeded, None));
    }
    let database = access
        .read_value(
            JournalTable::Meta,
            crate::layout::META_DATABASE_ID.as_bytes(),
        )?
        .ok_or_else(corrupt)
        .and_then(|encoded| {
            crate::codec::decode_database_identity_v1(&encoded).map(|value| *value.value())
        })?;
    let evidence = match riffdb_storage_api::decode_command_segment_v1(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            let history = access
                .read_value(
                    JournalTable::Meta,
                    crate::layout::META_HISTORY_INCARNATION.as_bytes(),
                )?
                .ok_or_else(corrupt)
                .and_then(|encoded| {
                    crate::codec::decode_history_incarnation_v1(&encoded)
                        .map(|value| *value.value())
                })?;
            if segment.first_commit_sequence() != physical_sequence
                || segment.database_id() != database
                || segment.history_incarnation() != history
            {
                return Err(corrupt());
            }
            segment
                .commands()
                .iter()
                .cloned()
                .map(CommandAuthorityMember::CapsuleV2)
                .map(Box::new)
                .map(ApplicationExportCommandEvidenceV1::Command)
                .collect::<Vec<_>>()
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            match riffdb_storage_api::decode_command_capsule_v2(encoded) {
                Ok(capsule) => vec![ApplicationExportCommandEvidenceV1::Command(Box::new(
                    CommandAuthorityMember::CapsuleV2(capsule.into_parts().0),
                ))],
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let events = access
                        .open_table(crate::layout::EVENTS)
                        .map_err(crate::error::table_error)?;
                    match decode_command_capsule_with_event_table(encoded, &events) {
                        Ok(capsule) => vec![ApplicationExportCommandEvidenceV1::Command(Box::new(
                            CommandAuthorityMember::CapsuleV1(capsule.into_parts().0),
                        ))],
                        Err(capsule_error) => {
                            match decode_commit_with_event_table(encoded, &events) {
                                Ok(commit) => {
                                    vec![ApplicationExportCommandEvidenceV1::HistoricalCommit(
                                        Box::new(commit.into_parts().0),
                                    )]
                                }
                                Err(_) => return Err(capsule_error),
                            }
                        }
                    }
                }
                Err(error) => return Err(crate::error::codec_error(error)),
            }
        }
        Err(error) => return Err(crate::error::codec_error(error)),
    };
    if evidence.is_empty() || evidence.len() > riffdb_storage_api::MAX_STAGED_COMMANDS {
        return Err(corrupt());
    }
    for (ordinal, member) in evidence.iter().enumerate() {
        let ordinal = u64::try_from(ordinal).map_err(|_| corrupt())?;
        let expected = physical_sequence
            .get()
            .checked_add(ordinal)
            .and_then(CommitSequence::new)
            .ok_or_else(corrupt)?;
        if member.commit().commit_sequence() != expected {
            return Err(corrupt());
        }
        if member
            .provenance()
            .is_some_and(|provenance| provenance.identity().database_id() != database)
        {
            return Err(corrupt());
        }
    }
    Ok(evidence)
}

/// Expands one physical authority row into its contiguous logical commit views.
///
/// The physical key is the first logical sequence for a segment and the sole
/// sequence for historical rows. Known-but-corrupt revisions never downgrade
/// into an older decoder.
pub(crate) fn commits_in_physical_row<E>(
    encoded: &[u8],
    events: &E,
    physical_sequence: CommitSequence,
) -> Result<Vec<EncodedPageItem<StoredCommitRecordV1>>, StorageError>
where
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    commits_in_physical_row_checked(encoded, events, physical_sequence, None, |_| Ok(()))
}

/// Startup supplies catalog validation while the decoded command slice is live,
/// and the walk's segment memo when it has one (ADR-0286).
pub(crate) fn commits_in_physical_row_checked<E>(
    encoded: &[u8],
    events: &E,
    physical_sequence: CommitSequence,
    segments: Option<&RetainedSegmentMemo>,
    check: impl FnOnce(&[StoredCommandCapsuleV2]) -> Result<(), StorageError>,
) -> Result<Vec<EncodedPageItem<StoredCommitRecordV1>>, StorageError>
where
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    match decode_retained_segment(encoded, segments) {
        Ok(segment) => {
            if segment.first_commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            check(segment.commands())?;
            return segment
                .commands()
                .iter()
                .map(|command| {
                    // The page budget needs the charge, not the bytes. Deriving it
                    // from the encoded length skips the payload allocation, the
                    // preflight, the envelope copy and the CRC that a full encode
                    // pays here once per scanned command.
                    let charge = riffdb_storage_api::command_capsule_v2_encoded_charge(command)
                        .map_err(crate::error::codec_error)?;
                    Ok(EncodedPageItem::new(
                        command.base().commit().clone(),
                        charge,
                    ))
                })
                .collect();
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }

    match crate::command_prefix::decode_capsule(encoded) {
        Ok(capsule) => {
            let (capsule, charge) = capsule.into_parts();
            if capsule.commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            check(std::slice::from_ref(&capsule))?;
            return Ok(vec![EncodedPageItem::new(
                capsule.base().commit().clone(),
                charge,
            )]);
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }

    match decode_command_capsule_with_event_table(encoded, events) {
        Ok(capsule) => {
            let (capsule, charge) = capsule.into_parts();
            if capsule.commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            Ok(vec![EncodedPageItem::new(capsule.commit().clone(), charge)])
        }
        Err(capsule_error) => match decode_commit_with_event_table(encoded, events) {
            Ok(commit) => {
                let (commit, charge) = commit.into_parts();
                if commit.commit_sequence() != physical_sequence {
                    return Err(corrupt());
                }
                Ok(vec![EncodedPageItem::new(commit, charge)])
            }
            Err(_) => Err(capsule_error),
        },
    }
}

/// Expands one row captured through the checkpoint-plus-suffix view. Modern
/// rows are self-contained; historical rows delegate to the frozen checkpoint
/// event table because the journal writer never emits those legacy revisions.
pub(crate) fn commits_in_physical_row_access(
    access: &RedbReadAccess,
    encoded: &[u8],
    physical_sequence: CommitSequence,
) -> Result<Vec<EncodedPageItem<StoredCommitRecordV1>>, StorageError> {
    if let Some(result) = modern_commits_in_physical_row(encoded, physical_sequence)? {
        return Ok(result);
    }
    let events = access
        .open_table(crate::layout::EVENTS)
        .map_err(crate::error::table_error)?;
    commits_in_physical_row(encoded, &events, physical_sequence)
}

fn modern_commits_in_physical_row(
    encoded: &[u8],
    physical_sequence: CommitSequence,
) -> Result<Option<Vec<EncodedPageItem<StoredCommitRecordV1>>>, StorageError> {
    match crate::history_stub::decode_segment_value(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            if segment.first_commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            return segment
                .commands()
                .iter()
                .map(|command| {
                    // The page budget needs the charge, not the bytes. Deriving it
                    // from the encoded length skips the payload allocation, the
                    // preflight, the envelope copy and the CRC that a full encode
                    // pays here once per scanned command.
                    let charge = riffdb_storage_api::command_capsule_v2_encoded_charge(command)
                        .map_err(crate::error::codec_error)?;
                    Ok(EncodedPageItem::new(
                        command.base().commit().clone(),
                        charge,
                    ))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some);
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
        Err(error) => return Err(crate::error::codec_error(error)),
    }
    match riffdb_storage_api::decode_command_capsule_v2(encoded) {
        Ok(capsule) => {
            let (capsule, charge) = capsule.into_parts();
            if capsule.commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            Ok(Some(vec![EncodedPageItem::new(
                capsule.base().commit().clone(),
                charge,
            )]))
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            Ok(None)
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

/// Returns the physical predecessor key from which a logical scan must begin.
/// The predecessor may be a segment containing `first_expected`.
pub(crate) fn physical_scan_start<C>(
    commits: &C,
    first_expected: CommitSequence,
) -> Result<Option<Vec<u8>>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
{
    let upper = encode_application_sequence_key(first_expected);
    let mut range = commits
        .range::<&[u8]>((Unbounded, Included(upper.as_slice())))
        .map_err(precommit_storage_error)?;
    let Some(entry) = range.next_back() else {
        return Ok(None);
    };
    let (key, _) = entry.map_err(precommit_storage_error)?;
    Ok(Some(key.value().to_vec()))
}

/// Resolves the last logical sequence even when the last physical row is a segment.
pub(crate) fn command_authority_head<C, E>(
    commits: &C,
    events: &E,
) -> Result<Option<CommitSequence>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    command_authority_head_profiled(commits, events).map(|profile| profile.head)
}

/// Diagnostic-only physical shape of the authority row used to resolve the
/// logical application head. The values are redaction-safe cardinalities; the
/// authority bytes themselves never cross the storage boundary.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CommandAuthorityHeadProfileV1 {
    pub(crate) head: Option<CommitSequence>,
    pub(crate) physical_bytes: u64,
    pub(crate) logical_commands: u64,
}

pub(crate) fn command_authority_head_profiled<C, E>(
    commits: &C,
    events: &E,
) -> Result<CommandAuthorityHeadProfileV1, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let Some((key, encoded)) = commits.last().map_err(precommit_storage_error)? else {
        return Ok(CommandAuthorityHeadProfileV1::default());
    };
    let physical_sequence = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
    let physical_bytes = u64::try_from(encoded.value().len()).unwrap_or(u64::MAX);
    // A history stub states its segment's tail itself (ADR-0263 Amendment 3),
    // so a head probe reads no extent and works on an offline database file.
    if let Some(summary) = crate::history_stub::segment_summary(encoded.value())? {
        if summary.first_commit_sequence != Some(physical_sequence) {
            return Err(corrupt());
        }
        return Ok(CommandAuthorityHeadProfileV1 {
            head: Some(summary.last_commit_sequence.ok_or_else(corrupt)?),
            physical_bytes,
            logical_commands: u64::from(summary.command_count),
        });
    }
    let logical = commits_in_physical_row(encoded.value(), events, physical_sequence)?;
    let head = logical
        .last()
        .ok_or_else(corrupt)?
        .value()
        .commit_sequence();
    Ok(CommandAuthorityHeadProfileV1 {
        head: Some(head),
        physical_bytes,
        logical_commands: u64::try_from(logical.len()).unwrap_or(u64::MAX),
    })
}

/// A decoded segment and the exact row bytes it was decoded from.
type RememberedSegment = (Box<[u8]>, std::sync::Arc<StoredCommandSegmentV1>);

/// The last command segment decoded by a lookup, remembered by the exact
/// `COMMITS` row bytes it was decoded from.
///
/// A successful decode of a segment row is determined by those bytes: an
/// inline segment is decoded from them, and a history stub names its frame by
/// hash, so resolving it can only yield that frame's segment (ADR-0283). A
/// lookup whose row bytes equal the remembered row therefore reuses the
/// decoded segment instead of reading and decoding it again; any other row is
/// decoded and replaces it. Only successful decodes are remembered, so a
/// refusal is re-derived and reported on every lookup exactly as without the
/// memo.
///
/// The validation walks resolve several commands of one segment, and each
/// command several times, in a row; without this each lookup decoded the
/// whole segment (resolving its stub from the journal) again. It holds one
/// segment, so its memory is bounded by the largest segment.
#[derive(Default)]
pub(crate) struct SegmentMemo {
    last: std::sync::Mutex<Option<RememberedSegment>>,
    #[cfg(test)]
    decodes: std::sync::atomic::AtomicUsize,
}

impl SegmentMemo {
    /// The segment in `row`, decoded by
    /// [`crate::history_stub::decode_segment_value`] unless it was the last
    /// one decoded.
    pub(crate) fn decode(
        &self,
        row: &[u8],
    ) -> Result<std::sync::Arc<StoredCommandSegmentV1>, riffdb_storage_api::DurableCodecError> {
        // One session owns one entry even when its bounded offline inspectors
        // run concurrently. Keep admission, decode and publication under the
        // same lock so workers cannot retain competing replacement entries.
        let mut last = self.last.lock().map_err(|_| {
            riffdb_storage_api::DurableCodecError::new(
                riffdb_storage_api::DurableCodecErrorKind::InvariantViolation,
            )
        })?;
        if let Some((bytes, segment)) = last.as_ref()
            && **bytes == *row
        {
            return Ok(std::sync::Arc::clone(segment));
        }
        #[cfg(test)]
        self.decodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let segment = std::sync::Arc::new(
            crate::history_stub::decode_segment_value(row)?
                .into_parts()
                .0,
        );
        *last = Some((row.into(), std::sync::Arc::clone(&segment)));
        Ok(segment)
    }

    /// Segment decodes this memo has performed.
    #[cfg(test)]
    pub(crate) fn decodes(&self) -> usize {
        self.decodes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The last command segment decoded by
/// [`crate::history_stub::decode_retained_segment`], remembered by the exact
/// `COMMITS` row bytes it was decoded from (ADR-0286).
///
/// It is [`SegmentMemo`] for the row-validating decoder the commit phase and
/// the walk's command history share: the commit phase decodes a row, and the
/// command history then resolves the same row's commands. Only a successful
/// decode is remembered and a remembered segment is never served for different
/// bytes, so every member and refusal is the one a decode without it yields.
#[derive(Default)]
pub(crate) struct RetainedSegmentMemo {
    last: std::cell::RefCell<Option<RememberedSegment>>,
}

impl RetainedSegmentMemo {
    /// The segment in `row`, decoded by
    /// [`crate::history_stub::decode_retained_segment`] unless it was the
    /// last one decoded.
    pub(crate) fn decode(
        &self,
        row: &[u8],
    ) -> Result<std::sync::Arc<StoredCommandSegmentV1>, riffdb_storage_api::DurableCodecError> {
        if let Some((bytes, segment)) = self.last.borrow().as_ref()
            && **bytes == *row
        {
            return Ok(std::sync::Arc::clone(segment));
        }
        let segment = std::sync::Arc::new(
            crate::history_stub::decode_retained_segment(row)?
                .into_parts()
                .0,
        );
        *self.last.borrow_mut() = Some((row.into(), std::sync::Arc::clone(&segment)));
        Ok(segment)
    }
}

/// Decodes `row` with [`crate::history_stub::decode_retained_segment`],
/// through `segments` when given.
pub(crate) fn decode_retained_segment(
    row: &[u8],
    segments: Option<&RetainedSegmentMemo>,
) -> Result<std::sync::Arc<StoredCommandSegmentV1>, riffdb_storage_api::DurableCodecError> {
    match segments {
        Some(segments) => segments.decode(row),
        None => crate::history_stub::decode_retained_segment(row)
            .map(|segment| std::sync::Arc::new(segment.into_parts().0)),
    }
}

/// Decodes the segment in `row`, through `segments` when given, and applies
/// `inspect` to it. Without a memo this is exactly
/// [`crate::history_stub::decode_segment_value`].
pub(crate) fn with_segment<R>(
    row: &[u8],
    segments: Option<&SegmentMemo>,
    inspect: impl FnOnce(&StoredCommandSegmentV1) -> R,
) -> Result<R, riffdb_storage_api::DurableCodecError> {
    match segments {
        Some(segments) => segments.decode(row).map(|segment| inspect(&segment)),
        None => {
            crate::history_stub::decode_segment_value(row).map(|segment| inspect(segment.value()))
        }
    }
}

/// Resolves a command by logical sequence. Segments use one predecessor range
/// lookup; historical per-command rows still resolve through the same path.
pub(crate) fn command_member_at<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
) -> Result<Option<CommandAuthorityMember>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    command_member_at_inner(commits, events, sequence, None)
}

/// [`command_member_at`], reusing a segment `segments` already decoded from
/// the same row bytes. The result, including every refusal, is identical.
pub(crate) fn command_member_at_remembering<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
    segments: &SegmentMemo,
) -> Result<Option<CommandAuthorityMember>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    command_member_at_inner(commits, events, sequence, Some(segments))
}

fn command_member_at_inner<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
    segments: Option<&SegmentMemo>,
) -> Result<Option<CommandAuthorityMember>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let upper = encode_application_sequence_key(sequence);
    let mut range = commits
        .range::<&[u8]>((Unbounded, Included(upper.as_slice())))
        .map_err(precommit_storage_error)?;
    let Some(entry) = range.next_back() else {
        return Ok(None);
    };
    let (key, row) = entry.map_err(precommit_storage_error)?;
    let physical_sequence = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;

    let decoded = with_segment(row.value(), segments, |segment| {
        segment_member(segment, physical_sequence, sequence)
    });
    match decoded {
        Ok(member) => member,
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            if physical_sequence != sequence {
                return Ok(None);
            }
            match riffdb_storage_api::decode_command_capsule_v2(row.value()) {
                Ok(capsule) => {
                    let capsule = capsule.into_parts().0;
                    if capsule.commit_sequence() != sequence {
                        return Err(corrupt());
                    }
                    return Ok(Some(CommandAuthorityMember::CapsuleV2(capsule)));
                }
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType => {}
                Err(error) => return Err(crate::error::codec_error(error)),
            }
            match decode_command_capsule_with_event_table(row.value(), events) {
                Ok(capsule) => {
                    let capsule = capsule.into_parts().0;
                    if capsule.commit_sequence() != sequence {
                        return Err(corrupt());
                    }
                    Ok(Some(CommandAuthorityMember::CapsuleV1(capsule)))
                }
                Err(error) if decode_commit_with_event_table(row.value(), events).is_ok() => {
                    let _ = error;
                    Ok(None)
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

/// Resolves one logical command through the captured composite authority.
pub(crate) fn command_member_at_access(
    access: &RedbReadAccess,
    sequence: CommitSequence,
) -> Result<Option<CommandAuthorityMember>, StorageError> {
    let start = encode_application_sequence_key(CommitSequence::first());
    let mut end = encode_application_sequence_key(sequence).to_vec();
    end.push(0);
    let Some((physical_key, encoded)) = access
        .read_range_reverse(JournalTable::Commits, &start, &end, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let physical_sequence =
        decode_application_sequence_key(&physical_key).map_err(|_| corrupt())?;
    match crate::history_stub::decode_segment_value(&encoded) {
        Ok(segment) => segment_member(&segment.into_parts().0, physical_sequence, sequence),
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            if physical_sequence != sequence {
                return Ok(None);
            }
            match riffdb_storage_api::decode_command_capsule_v2(&encoded) {
                Ok(capsule) => {
                    let capsule = capsule.into_parts().0;
                    if capsule.commit_sequence() != sequence {
                        return Err(corrupt());
                    }
                    Ok(Some(CommandAuthorityMember::CapsuleV2(capsule)))
                }
                Err(error)
                    if error.kind()
                        == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
                {
                    let commits = access
                        .open_table(crate::layout::COMMITS)
                        .map_err(crate::error::table_error)?;
                    let events = access
                        .open_table(crate::layout::EVENTS)
                        .map_err(crate::error::table_error)?;
                    command_member_at(&commits, &events, sequence)
                }
                Err(error) => Err(crate::error::codec_error(error)),
            }
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

/// Resolves the complete commit view irrespective of its physical authority revision.
pub(crate) fn commit_at<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
) -> Result<Option<StoredCommitRecordV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    commit_after_member(
        commits,
        events,
        sequence,
        command_member_at(commits, events, sequence)?,
    )
}

/// [`commit_at`] reusing a segment `segments` already decoded from the same
/// row bytes. The result, including every refusal, is identical.
pub(crate) fn commit_at_remembering<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
    segments: &SegmentMemo,
) -> Result<Option<StoredCommitRecordV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    let member = command_member_at_remembering(commits, events, sequence, segments)?;
    commit_after_member(commits, events, sequence, member)
}

fn commit_after_member<C, E>(
    commits: &C,
    events: &E,
    sequence: CommitSequence,
    member: Option<CommandAuthorityMember>,
) -> Result<Option<StoredCommitRecordV1>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    if let Some(member) = member {
        return Ok(Some(member.base().commit().clone()));
    }
    let key = encode_application_sequence_key(sequence);
    let Some(row) = commits
        .get(key.as_slice())
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let commit = decode_commit_with_event_table(row.value(), events)?
        .into_parts()
        .0;
    if commit.commit_sequence() != sequence {
        return Err(corrupt());
    }
    Ok(Some(commit))
}

/// Resolves a complete commit through one captured composite authority.
pub(crate) fn commit_at_access(
    access: &RedbReadAccess,
    sequence: CommitSequence,
) -> Result<Option<StoredCommitRecordV1>, StorageError> {
    if let Some(member) = command_member_at_access(access, sequence)? {
        return Ok(Some(member.base().commit().clone()));
    }
    let key = encode_application_sequence_key(sequence);
    let Some(encoded) = access.read_value(JournalTable::Commits, &key)? else {
        return Ok(None);
    };
    // A standalone modern capsule was handled above. A remaining exact row is
    // historical and therefore belongs to the immutable checkpoint.
    let commits = access
        .open_table(crate::layout::COMMITS)
        .map_err(crate::error::table_error)?;
    let events = access
        .open_table(crate::layout::EVENTS)
        .map_err(crate::error::table_error)?;
    commit_at(&commits, &events, sequence).and_then(|record| {
        if record.is_none() && !encoded.is_empty() {
            Err(corrupt())
        } else {
            Ok(record)
        }
    })
}

/// Loads the command member at `sequence` through a write access.
///
/// The read-side peer walks backwards to the greatest physical key at or below
/// `sequence`; a write access has no reverse range primitive, so this uses the
/// fact that a segment's physical key is its FIRST commit sequence and a segment
/// holds at most `MAX_STAGED_COMMANDS` commands. The owning row therefore lies
/// in a bounded window ending at `sequence`, making this a bounded forward scan
/// rather than a history walk.
///
/// Returns `None` only when no row in that window owns the sequence. A caller
/// resolving an ADR-0165 locator MUST treat that as corruption, never absence:
/// the locator asserted the row exists.
pub(crate) fn command_member_at_write_access(
    access: &crate::store::RedbWriteAccess,
    sequence: CommitSequence,
) -> Result<Option<CommandAuthorityMember>, StorageError> {
    let window = u64::try_from(riffdb_storage_api::MAX_STAGED_COMMANDS)
        .map_err(|_| corrupt())?
        .saturating_sub(1);
    let first =
        CommitSequence::new(sequence.get().saturating_sub(window).max(1)).ok_or_else(corrupt)?;
    let start = encode_application_sequence_key(first);
    let mut end = encode_application_sequence_key(sequence).to_vec();
    end.push(0);
    let rows = access.read_command_range(
        JournalTable::Commits,
        &start,
        &end,
        riffdb_storage_api::MAX_STAGED_COMMANDS.saturating_add(1),
    )?;
    // Latest owning row wins, matching the read side's reverse selection.
    for (physical_key, encoded) in rows.into_iter().rev() {
        let physical_sequence =
            decode_application_sequence_key(&physical_key).map_err(|_| corrupt())?;
        // Locators are only written for segment-owned commands: a historical row
        // carries its own physical outcome and needs none. So a non-segment row
        // does not own the sequence, and the caller fails closed if none does.
        let member = match crate::history_stub::decode_segment_value(&encoded) {
            Ok(segment) => segment_member(&segment.into_parts().0, physical_sequence, sequence)?,
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                continue;
            }
            Err(error) => return Err(crate::error::codec_error(error)),
        };
        if member.is_some() {
            return Ok(member);
        }
    }
    Ok(None)
}

fn segment_member(
    segment: &StoredCommandSegmentV1,
    physical_sequence: CommitSequence,
    requested: CommitSequence,
) -> Result<Option<CommandAuthorityMember>, StorageError> {
    if segment.first_commit_sequence() != physical_sequence
        || requested < segment.first_commit_sequence()
    {
        return Err(corrupt());
    }
    if requested > segment.last_commit_sequence() {
        return Ok(None);
    }
    let ordinal = requested
        .get()
        .checked_sub(segment.first_commit_sequence().get())
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(corrupt)?;
    let command = segment.commands().get(ordinal).ok_or_else(corrupt)?;
    if command.commit_sequence() != requested {
        return Err(corrupt());
    }
    Ok(Some(CommandAuthorityMember::CapsuleV2(command.clone())))
}

const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}

#[cfg(test)]
mod application_export_bounds_tests {
    use super::MAX_APPLICATION_EXPORT_COMMAND_AUTHORITY_BYTES;

    // req: AFC-006, EXP-002, EXP-003, EXP-004, EXP-007, EXP-010
    #[test]
    fn application_export_sparse_evidence_pages_charge_bounded_progress() {
        assert_eq!(
            MAX_APPLICATION_EXPORT_COMMAND_AUTHORITY_BYTES,
            16 * 1024 * 1024
        );
        assert_eq!(riffdb_storage_api::MAX_STAGED_COMMANDS, 256);
        assert_eq!(riffdb_storage_api::MAX_EVENT_INTENTS, 4_096);
    }
}
