//! Bounded command history for the complete structural walk (ADR-0182,
//! ADR-0203 Decision 8).
//!
//! The walk cross-checks commit, provenance, event, route, outbox, and audit
//! rows against the command that owns them. It used to decode the whole
//! retained command history into memory first, which at the production
//! population exceeded the evidence ceiling and made both scrubs refuse a
//! healthy database. This history keeps only a compact administration-audit
//! index (one fixed-size entry per derived audit member) and resolves each
//! command on demand from its physical `COMMITS` row through a small cache of
//! recently decoded rows. The domain is unchanged: only commands after the
//! validated-prefix checkpoint resolve, exactly as the former cache held.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use redb::{ReadTransaction, ReadableTable};
use riffdb_storage_api::{
    StorageError, StorageErrorKind, StoredCommandAuditMemberV1, StoredCommandCapsuleV1,
    StoredServiceAuditRecordV1,
};
use riffdb_types::{AdministrationSequence, CommitSequence};

use crate::command_authority::{CommandAuthorityMember, RetainedSegmentMemo, command_member_at};
use crate::error::{precommit_storage_error, storage_error, table_error};
use crate::keys;
use crate::layout::{COMMITS, EVENTS};

use super::CachedCommandAudit;

/// Decoded commands retained for on-demand lookups. The structural phases
/// read in commit order, so a small window keeps each row decoded once.
const RECENT_COMMANDS: usize = 256;

/// One command resolved from its physical row.
#[derive(Clone)]
pub(super) struct LoadedCommand {
    pub(super) capsule: Arc<StoredCommandCapsuleV1>,
    /// The command's complete authority graph lives in its own physical row
    /// (a segment member or a V2 capsule).
    pub(super) embedded: bool,
}

/// Resolves the command at a commit sequence for the row inspectors.
pub(super) trait CommandLookup {
    fn command(
        &self,
        transaction: &ReadTransaction,
        sequence: CommitSequence,
    ) -> Result<Option<LoadedCommand>, StorageError>;

    /// The walk's memo of the last segment decoded from a `COMMITS` row, when
    /// this lookup resolves rows the commit phase also decodes (ADR-0286).
    fn retained_segments(&self) -> Option<&RetainedSegmentMemo> {
        None
    }
}

/// The derived command-audit members the administration continuity proof
/// merges with the physical `AUDIT` rows.
pub(super) trait DerivedAuditSource {
    /// Every derived administration sequence, ascending and unique.
    fn derived_sequences(&self) -> Box<dyn Iterator<Item = AdministrationSequence> + '_>;

    /// The derived audit record at `sequence`, when one exists.
    fn derived_record(
        &self,
        transaction: &ReadTransaction,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError>;
}

#[derive(Clone, Copy)]
struct CompactAudit {
    administration: AdministrationSequence,
    commit: CommitSequence,
    member: StoredCommandAuditMemberV1,
}

pub(super) struct CommandHistory {
    /// Commit sequences at or below this are covered by the checkpoint and
    /// outside the domain, as the former whole-history cache excluded them.
    floor: u64,
    audits: Vec<CompactAudit>,
    recent: RefCell<VecDeque<(CommitSequence, LoadedCommand)>>,
    /// The last segment decoded by an on-demand lookup or by the commit
    /// phase, shared so a row both resolve is decoded once.
    segments: RetainedSegmentMemo,
    rows_decoded: u64,
    retained_bytes: usize,
}

impl CommandHistory {
    /// An empty history for sessions that never build one.
    pub(super) fn empty() -> Self {
        Self {
            floor: 0,
            audits: Vec::new(),
            recent: RefCell::new(VecDeque::new()),
            segments: RetainedSegmentMemo::default(),
            rows_decoded: 0,
            retained_bytes: 0,
        }
    }

    /// Streams the `COMMITS` rows after `floor` once, retaining only the
    /// compact audit index. Overlapping rows, a command claimed twice, or an
    /// administration sequence derived twice fail closed as corrupt data; an
    /// index larger than `ceiling` refuses as over-limit evidence.
    pub(super) fn build(
        transaction: &ReadTransaction,
        floor: u64,
        ceiling: usize,
    ) -> Result<Self, StorageError> {
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let mut builder = CommandHistoryBuilder::new(floor, ceiling)?;
        let rows = match builder.lower_bound() {
            None => commits.iter().map_err(precommit_storage_error)?,
            Some(floor_key) => commits
                .range::<&[u8]>((Excluded(floor_key), Unbounded))
                .map_err(precommit_storage_error)?,
        };
        for entry in rows {
            let (key, value) = entry.map_err(precommit_storage_error)?;
            builder.row(&commits, &events, key.value(), value.value(), None)?;
        }
        builder.finish()
    }

    pub(super) const fn rows_decoded(&self) -> u64 {
        self.rows_decoded
    }

    pub(super) fn audit_entries(&self) -> usize {
        self.audits.len()
    }

    pub(super) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    /// The derived audit member at `sequence`, with its record and peer.
    pub(super) fn audit(
        &self,
        transaction: &ReadTransaction,
        sequence: AdministrationSequence,
    ) -> Result<Option<CachedCommandAudit>, StorageError> {
        let Ok(position) = self
            .audits
            .binary_search_by_key(&sequence, |audit| audit.administration)
        else {
            return Ok(None);
        };
        let compact = self.audits[position];
        // The build decoded this command; a later miss is a changed snapshot.
        let command = self
            .command(transaction, compact.commit)?
            .ok_or_else(corrupt)?;
        let started = command.capsule.started_audit();
        let terminal = command.capsule.terminal_audit();
        let (record, peer_sequence) = match compact.member {
            StoredCommandAuditMemberV1::Started => {
                (started.clone(), terminal.administration_sequence())
            }
            StoredCommandAuditMemberV1::Terminal => {
                (terminal.clone(), started.administration_sequence())
            }
        };
        if record.administration_sequence() != sequence {
            return Err(corrupt());
        }
        Ok(Some(CachedCommandAudit {
            commit_sequence: compact.commit,
            member: compact.member,
            record,
            peer_sequence,
        }))
    }
}

/// [`CommandHistory::build`] one `COMMITS` row at a time, so a pass that also
/// serves other consumers can decode each row once (ADR-0287).
pub(super) struct CommandHistoryBuilder {
    floor: u64,
    floor_key: Option<[u8; 8]>,
    ceiling: usize,
    audits: Vec<CompactAudit>,
    rows_decoded: u64,
    last_claimed: Option<CommitSequence>,
}

impl CommandHistoryBuilder {
    pub(super) fn new(floor: u64, ceiling: usize) -> Result<Self, StorageError> {
        let floor_key = if floor == 0 {
            None
        } else {
            Some(keys::encode_application_sequence_key(
                CommitSequence::new(floor).ok_or_else(invariant)?,
            ))
        };
        Ok(Self {
            floor,
            floor_key,
            ceiling,
            audits: Vec::new(),
            rows_decoded: 0,
            last_claimed: None,
        })
    }

    /// The exclusive lower `COMMITS` key the history starts after, if any.
    pub(super) fn lower_bound(&self) -> Option<&[u8]> {
        self.floor_key.as_ref().map(<[u8; 8]>::as_slice)
    }

    /// Indexes one `COMMITS` row above the floor, decoding its segment
    /// through `segments` when given.
    pub(super) fn row<C, E>(
        &mut self,
        commits: &C,
        events: &E,
        key: &[u8],
        value: &[u8],
        segments: Option<&RetainedSegmentMemo>,
    ) -> Result<(), StorageError>
    where
        C: ReadableTable<&'static [u8], &'static [u8]>,
        E: ReadableTable<&'static [u8], &'static [u8]>,
    {
        let entry_bytes = std::mem::size_of::<CompactAudit>();
        self.rows_decoded = self.rows_decoded.saturating_add(1);
        let physical = keys::decode_application_sequence_key(key).map_err(|_| corrupt())?;
        // The physical commit phase owns malformed-row reporting; an
        // undecodable row contributes no derived members here.
        let Some((commands, _)) = decode_row(commits, events, physical, value, segments)? else {
            return Ok(());
        };
        for command in commands {
            let sequence = command.commit_sequence();
            if self.last_claimed.is_some_and(|last| sequence <= last) {
                return Err(corrupt());
            }
            self.last_claimed = Some(sequence);
            for (member, record) in [
                (StoredCommandAuditMemberV1::Started, command.started_audit()),
                (
                    StoredCommandAuditMemberV1::Terminal,
                    command.terminal_audit(),
                ),
            ] {
                self.audits.push(CompactAudit {
                    administration: record.administration_sequence(),
                    commit: sequence,
                    member,
                });
                if self.audits.len().saturating_mul(entry_bytes) > self.ceiling {
                    return Err(storage_error(StorageErrorKind::LimitExceeded));
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish(self) -> Result<CommandHistory, StorageError> {
        let entry_bytes = std::mem::size_of::<CompactAudit>();
        let mut audits = self.audits;
        audits.sort_by_key(|audit| audit.administration);
        if audits
            .windows(2)
            .any(|pair| pair[0].administration == pair[1].administration)
        {
            return Err(corrupt());
        }
        audits.shrink_to_fit();
        let retained_bytes = audits.len().saturating_mul(entry_bytes);
        Ok(CommandHistory {
            floor: self.floor,
            audits,
            recent: RefCell::new(VecDeque::new()),
            segments: RetainedSegmentMemo::default(),
            rows_decoded: self.rows_decoded,
            retained_bytes,
        })
    }
}

impl CommandLookup for CommandHistory {
    fn command(
        &self,
        transaction: &ReadTransaction,
        sequence: CommitSequence,
    ) -> Result<Option<LoadedCommand>, StorageError> {
        if sequence.get() <= self.floor {
            return Ok(None);
        }
        if let Some(found) = self
            .recent
            .borrow()
            .iter()
            .find(|(cached, _)| *cached == sequence)
            .map(|(_, command)| command.clone())
        {
            return Ok(Some(found));
        }
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let upper = keys::encode_application_sequence_key(sequence);
        let Some(entry) = commits
            .range::<&[u8]>((Unbounded, Included(upper.as_slice())))
            .map_err(precommit_storage_error)?
            .next_back()
        else {
            return Ok(None);
        };
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
        if physical.get() <= self.floor {
            return Ok(None);
        }
        let Some((commands, embedded)) = decode_row(
            &commits,
            &events,
            physical,
            value.value(),
            Some(&self.segments),
        )?
        else {
            return Ok(None);
        };
        let mut found = None;
        let mut recent = self.recent.borrow_mut();
        for command in commands {
            let loaded = LoadedCommand {
                capsule: Arc::new(command),
                embedded,
            };
            let commit = loaded.capsule.commit_sequence();
            if commit == sequence {
                found = Some(loaded.clone());
            }
            if recent.len() == RECENT_COMMANDS {
                recent.pop_front();
            }
            recent.push_back((commit, loaded));
        }
        Ok(found)
    }

    fn retained_segments(&self) -> Option<&RetainedSegmentMemo> {
        Some(&self.segments)
    }
}

impl DerivedAuditSource for CommandHistory {
    fn derived_sequences(&self) -> Box<dyn Iterator<Item = AdministrationSequence> + '_> {
        Box::new(self.audits.iter().map(|audit| audit.administration))
    }

    fn derived_record(
        &self,
        transaction: &ReadTransaction,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError> {
        Ok(self.audit(transaction, sequence)?.map(|audit| audit.record))
    }
}

impl DerivedAuditSource for std::collections::BTreeMap<AdministrationSequence, CachedCommandAudit> {
    fn derived_sequences(&self) -> Box<dyn Iterator<Item = AdministrationSequence> + '_> {
        Box::new(self.keys().copied())
    }

    fn derived_record(
        &self,
        _transaction: &ReadTransaction,
        sequence: AdministrationSequence,
    ) -> Result<Option<StoredServiceAuditRecordV1>, StorageError> {
        Ok(self.get(&sequence).map(|audit| audit.record.clone()))
    }
}

/// A caller-owned command map, for bounded sampled inspection.
pub(super) struct MapCommandLookup<'a> {
    pub(super) capsules: &'a std::collections::BTreeMap<CommitSequence, StoredCommandCapsuleV1>,
    pub(super) embedded: &'a std::collections::BTreeSet<CommitSequence>,
}

impl CommandLookup for MapCommandLookup<'_> {
    fn command(
        &self,
        _transaction: &ReadTransaction,
        sequence: CommitSequence,
    ) -> Result<Option<LoadedCommand>, StorageError> {
        Ok(self.capsules.get(&sequence).map(|capsule| LoadedCommand {
            capsule: Arc::new(capsule.clone()),
            embedded: self.embedded.contains(&sequence),
        }))
    }
}

/// Decodes one physical `COMMITS` row into its commands, exactly as the
/// former whole-history cache did: a segment embeds every member; a
/// historical row resolves its single command, embedded only as a V2
/// capsule. A malformed row yields `None` (the commit phase reports it).
fn decode_row<C, E>(
    commits: &C,
    events: &E,
    physical: CommitSequence,
    row: &[u8],
    segments: Option<&RetainedSegmentMemo>,
) -> Result<Option<(Vec<StoredCommandCapsuleV1>, bool)>, StorageError>
where
    C: ReadableTable<&'static [u8], &'static [u8]>,
    E: ReadableTable<&'static [u8], &'static [u8]>,
{
    match crate::command_authority::decode_retained_segment(row, segments) {
        Ok(segment) => {
            if segment.first_commit_sequence() != physical {
                return Err(corrupt());
            }
            Ok(Some((
                segment
                    .commands()
                    .iter()
                    .map(|command| command.base().clone())
                    .collect(),
                true,
            )))
        }
        Err(error)
            if error.kind() == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
        {
            match command_member_at(commits, events, physical) {
                Ok(Some(member)) => {
                    let embedded = matches!(&member, CommandAuthorityMember::CapsuleV2(_));
                    Ok(Some((vec![member.into_base()], embedded)))
                }
                Ok(None) => Ok(None),
                Err(error) if error.kind() == StorageErrorKind::CorruptData => Ok(None),
                Err(error) => Err(error),
            }
        }
        Err(_) => Ok(None),
    }
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}
