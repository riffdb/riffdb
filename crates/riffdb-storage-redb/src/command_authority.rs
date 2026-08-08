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
                .collect();
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
    let Some((key, encoded)) = commits.last().map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let physical_sequence = decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
    let logical = commits_in_physical_row(encoded.value(), events, physical_sequence)?;
    Ok(Some(
        logical
            .last()
            .ok_or_else(corrupt)?
            .value()
            .commit_sequence(),
    ))
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
            segment_member(segment, physical_sequence, sequence).map(Some)
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
        Ok(segment) => {
            segment_member(segment.into_parts().0, physical_sequence, sequence).map(Some)
        }
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

fn segment_member(
    segment: StoredCommandSegmentV1,
    physical_sequence: CommitSequence,
    requested: CommitSequence,
) -> Result<CommandAuthorityMember, StorageError> {
    if segment.first_commit_sequence() != physical_sequence
        || requested < segment.first_commit_sequence()
        || requested > segment.last_commit_sequence()
    {
        return Err(corrupt());
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
    Ok(CommandAuthorityMember::CapsuleV2(command.clone()))
}

const fn corrupt() -> StorageError {
    StorageError::new(StorageErrorKind::CorruptData, None)
}
