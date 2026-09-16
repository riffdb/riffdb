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
    commits_in_physical_row_checked(encoded, events, physical_sequence, |_| Ok(()))
}

/// Startup supplies catalog validation while the decoded command slice is live.
pub(crate) fn commits_in_physical_row_checked<E>(
    encoded: &[u8],
    events: &E,
    physical_sequence: CommitSequence,
    check: impl FnOnce(&[StoredCommandCapsuleV2]) -> Result<(), StorageError>,
) -> Result<Vec<EncodedPageItem<StoredCommitRecordV1>>, StorageError>
where
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    match crate::command_prefix::decode_segment(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            if segment.first_commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            check(segment.commands())?;
            return segment
                .commands()
                .iter()
                .map(|command| {
                    let encoded = riffdb_storage_api::encode_command_capsule_v2(command)
                        .map_err(crate::error::codec_error)?;
                    Ok(EncodedPageItem::new(
                        command.base().commit().clone(),
                        encoded.encoded_content_charge(),
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
    match riffdb_storage_api::decode_command_segment_v1(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            if segment.first_commit_sequence() != physical_sequence {
                return Err(corrupt());
            }
            return segment
                .commands()
                .iter()
                .map(|command| {
                    let encoded = riffdb_storage_api::encode_command_capsule_v2(command)
                        .map_err(crate::error::codec_error)?;
                    Ok(EncodedPageItem::new(
                        command.base().commit().clone(),
                        encoded.encoded_content_charge(),
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
    let upper = encode_application_sequence_key(sequence);
    let mut range = commits
        .range::<&[u8]>((Unbounded, Included(upper.as_slice())))
        .map_err(precommit_storage_error)?;
    let Some(entry) = range.next_back() else {
        return Ok(None);
    };
    let (key, row) = entry.map_err(precommit_storage_error)?;
    let physical_sequence = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;

    match riffdb_storage_api::decode_command_segment_v1(row.value()) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            segment_member(segment, physical_sequence, sequence)
        }
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
    match riffdb_storage_api::decode_command_segment_v1(&encoded) {
        Ok(segment) => segment_member(segment.into_parts().0, physical_sequence, sequence),
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
    if let Some(member) = command_member_at(commits, events, sequence)? {
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
        let member = match riffdb_storage_api::decode_command_segment_v1(&encoded) {
            Ok(segment) => segment_member(segment.into_parts().0, physical_sequence, sequence)?,
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

/// Resolves the durable segment row whose physical key is exactly `first`,
/// decoding it once. The fresh-locator witnesses use this to check every
/// member of a just-published segment against its durable bytes without
/// re-reading and re-decoding the whole row once per command.
///
/// Returns `Ok(None)` when no row is keyed at `first` or the row is not a V1
/// segment; a segment row keyed at `first` that does not start at `first` is
/// corruption.
pub(crate) fn command_segment_at_access(
    access: &RedbReadAccess,
    first: CommitSequence,
) -> Result<Option<StoredCommandSegmentV1>, StorageError> {
    let start = encode_application_sequence_key(first);
    let mut end = start.to_vec();
    end.push(0);
    let Some((physical_key, encoded)) = access
        .read_range_reverse(JournalTable::Commits, &start, &end, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    durable_segment_at(&physical_key, &encoded, first)
}

/// Write-transaction twin of [`command_segment_at_access`].
pub(crate) fn command_segment_at_write_access(
    access: &crate::store::RedbWriteAccess,
    first: CommitSequence,
) -> Result<Option<StoredCommandSegmentV1>, StorageError> {
    let start = encode_application_sequence_key(first);
    let mut end = start.to_vec();
    end.push(0);
    let Some((physical_key, encoded)) = access
        .read_command_range(JournalTable::Commits, &start, &end, 1)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    durable_segment_at(&physical_key, &encoded, first)
}

fn durable_segment_at(
    physical_key: &[u8],
    encoded: &[u8],
    first: CommitSequence,
) -> Result<Option<StoredCommandSegmentV1>, StorageError> {
    let physical_sequence = decode_application_sequence_key(physical_key).map_err(|_| corrupt())?;
    if physical_sequence != first {
        return Err(corrupt());
    }
    match riffdb_storage_api::decode_command_segment_v1(encoded) {
        Ok(segment) => {
            let segment = segment.into_parts().0;
            if segment.first_commit_sequence() != first {
                return Err(corrupt());
            }
            Ok(Some(segment))
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            Ok(None)
        }
        Err(error) => Err(crate::error::codec_error(error)),
    }
}

fn segment_member(
    segment: StoredCommandSegmentV1,
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
