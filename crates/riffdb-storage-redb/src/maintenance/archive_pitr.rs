//! Deterministic wall-clock PITR timestamp-to-sequence resolution and confirmed
//! archive frame summaries (ADR-0320).

use super::{RedbArchiveRepository, RedbVerifiedArchiveBackup};
use riffdb_storage_api::{
    ArchiveConsumerErrorV1, ArchiveManifestV1, AuthoritativeNamespaceV1, ChangelogFrameV3,
    DurableCodecErrorKind,
};
use riffdb_types::{ArchiveRestoreStopV1, CommitSequence, Timestamp};
use std::sync::atomic::{AtomicBool, Ordering};

/// Validated summary of one confirmed immutable `(frame-<seq>.v3, manifest-<seq>.v1)`
/// pair in an external changelog archive.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchiveFrameSummaryV1 {
    manifest: ArchiveManifestV1,
    frame_file_name: String,
    manifest_file_name: String,
    first_application_sequence: Option<CommitSequence>,
    last_application_sequence: Option<CommitSequence>,
    first_commit_timestamp: Option<Timestamp>,
    last_commit_timestamp: Option<Timestamp>,
    command_count: u64,
}

impl ArchiveFrameSummaryV1 {
    pub(in crate::maintenance) fn from_manifest_and_frame_bytes(
        manifest: ArchiveManifestV1,
        bytes: &[u8],
        cancellation: &AtomicBool,
    ) -> Result<Self, ArchiveConsumerErrorV1> {
        check_cancelled(cancellation)?;
        manifest.verify_frame(bytes)?;
        let (frame_file_name, manifest_file_name) =
            super::archive_repository::names(manifest.before())?;
        let frame =
            ChangelogFrameV3::decode(bytes).map_err(|_| ArchiveConsumerErrorV1::InvalidFrame)?;
        let mut first_application_sequence = None;
        let mut last_application_sequence: Option<CommitSequence> = None;
        let mut first_commit_timestamp: Option<Timestamp> = None;
        let mut last_commit_timestamp: Option<Timestamp> = None;
        let mut command_count: u64 = 0;
        for receipt in frame.receipts() {
            check_cancelled(cancellation)?;
            for mutation in receipt.mutations() {
                if mutation.namespace() != AuthoritativeNamespaceV1::Commits {
                    continue;
                }
                let value = mutation
                    .value()
                    .ok_or(ArchiveConsumerErrorV1::InvalidFrame)?;
                for (sequence, timestamp) in decode_commit_mutation_commands(value)? {
                    if last_application_sequence.is_some_and(|previous| sequence <= previous) {
                        return Err(ArchiveConsumerErrorV1::InvalidFrame);
                    }
                    if first_application_sequence.is_none() {
                        first_application_sequence = Some(sequence);
                    }
                    last_application_sequence = Some(sequence);
                    first_commit_timestamp =
                        Some(first_commit_timestamp.map_or(timestamp, |prev| prev.min(timestamp)));
                    last_commit_timestamp =
                        Some(last_commit_timestamp.map_or(timestamp, |prev| prev.max(timestamp)));
                    command_count = command_count
                        .checked_add(1)
                        .ok_or(ArchiveConsumerErrorV1::InvalidFrame)?;
                }
            }
        }
        Ok(Self {
            manifest,
            frame_file_name,
            manifest_file_name,
            first_application_sequence,
            last_application_sequence,
            first_commit_timestamp,
            last_commit_timestamp,
            command_count,
        })
    }

    /// Returns the validated `ArchiveManifestV1` descriptor for this frame.
    #[must_use]
    pub const fn manifest(&self) -> &ArchiveManifestV1 {
        &self.manifest
    }

    /// Returns the immutable frame file name (`frame-<16-hex-seq>.v3`).
    #[must_use]
    pub fn frame_file_name(&self) -> &str {
        &self.frame_file_name
    }

    /// Returns the immutable manifest file name (`manifest-<16-hex-seq>.v1`).
    #[must_use]
    pub fn manifest_file_name(&self) -> &str {
        &self.manifest_file_name
    }

    /// Returns the first application `CommitSequence` in this frame, if any.
    #[must_use]
    pub const fn first_application_sequence(&self) -> Option<CommitSequence> {
        self.first_application_sequence
    }

    /// Returns the last application `CommitSequence` in this frame, if any.
    #[must_use]
    pub const fn last_application_sequence(&self) -> Option<CommitSequence> {
        self.last_application_sequence
    }

    /// Returns the minimum coordinator commit `Timestamp` among commands in this frame.
    #[must_use]
    pub const fn first_commit_timestamp(&self) -> Option<Timestamp> {
        self.first_commit_timestamp
    }

    /// Returns the maximum coordinator commit `Timestamp` among commands in this frame.
    #[must_use]
    pub const fn last_commit_timestamp(&self) -> Option<Timestamp> {
        self.last_commit_timestamp
    }

    /// Returns the number of application commands carried in this frame.
    #[must_use]
    pub const fn command_count(&self) -> u64 {
        self.command_count
    }
}

impl RedbArchiveRepository {
    /// Validates the selected archive prefix and returns ordered summaries of
    /// every confirmed `(frame-<seq>.v3, manifest-<seq>.v1)` pair through `CURRENT`.
    pub fn frame_summaries(
        &self,
        cancellation: &AtomicBool,
    ) -> Result<Vec<ArchiveFrameSummaryV1>, ArchiveConsumerErrorV1> {
        check_cancelled(cancellation)?;
        let mut summaries = Vec::new();
        for item in self.frames() {
            check_cancelled(cancellation)?;
            let (manifest, bytes) = item?;
            summaries.push(ArchiveFrameSummaryV1::from_manifest_and_frame_bytes(
                manifest,
                &bytes,
                cancellation,
            )?);
        }
        Ok(summaries)
    }
}

/// Exact sequence stop resolved from a wall-clock PITR target timestamp over a
/// verified physical backup and its bound changelog archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchivePitrResolutionV1 {
    stop: ArchiveRestoreStopV1,
    resolved_sequence: CommitSequence,
    resolved_commit_timestamp: Timestamp,
    next_excluded_sequence: Option<CommitSequence>,
    next_excluded_commit_timestamp: Option<Timestamp>,
}

impl ArchivePitrResolutionV1 {
    /// Returns the exact `ArchiveRestoreStopV1::AtApplicationSequence` stop to
    /// pass to `RedbArchiveRestoreStage::prepare_restore`.
    #[must_use]
    pub const fn stop(&self) -> ArchiveRestoreStopV1 {
        self.stop
    }

    /// Returns the resolved inclusive application `CommitSequence`.
    #[must_use]
    pub const fn resolved_sequence(&self) -> CommitSequence {
        self.resolved_sequence
    }

    /// Returns the coordinator commit `Timestamp` of `resolved_sequence`.
    #[must_use]
    pub const fn resolved_commit_timestamp(&self) -> Timestamp {
        self.resolved_commit_timestamp
    }

    /// Returns the first excluded application `CommitSequence` after the cut, if any.
    #[must_use]
    pub const fn next_excluded_sequence(&self) -> Option<CommitSequence> {
        self.next_excluded_sequence
    }

    /// Returns the coordinator commit `Timestamp` of `next_excluded_sequence`, if any.
    #[must_use]
    pub const fn next_excluded_commit_timestamp(&self) -> Option<Timestamp> {
        self.next_excluded_commit_timestamp
    }
}

/// Typed fail-closed refusal when resolving a wall-clock PITR target timestamp.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchivePitrResolutionError {
    /// The target timestamp precedes the verified backup fence commit timestamp
    /// (or the backup fence commit was pruned and no archived commit is `<= target`).
    TargetBeforeBackupFence {
        /// Requested target timestamp.
        target_timestamp: Timestamp,
        /// Backup fence commit sequence.
        backup_fence_sequence: CommitSequence,
        /// Backup fence commit timestamp, when retained in the backup commit log.
        backup_fence_timestamp: Option<Timestamp>,
    },
    /// Neither the backup nor the archive contains any application commit at or
    /// before the target timestamp.
    TargetBeforeFirstCommit {
        /// Requested target timestamp.
        target_timestamp: Timestamp,
        /// First application commit sequence in the archive, if any.
        first_commit_sequence: Option<CommitSequence>,
        /// Commit timestamp of `first_commit_sequence`, if any.
        first_commit_timestamp: Option<Timestamp>,
    },
    /// A later commit after the first excluded commit (`> target_timestamp`) carries
    /// a commit timestamp `<= target_timestamp`, making a prefix cut ambiguous.
    NonMonotonicTimestampAcrossTarget {
        /// Requested target timestamp.
        target_timestamp: Timestamp,
        /// First application commit sequence whose timestamp exceeded `target_timestamp`.
        first_excluded_sequence: CommitSequence,
        /// Commit timestamp of `first_excluded_sequence`.
        first_excluded_timestamp: Timestamp,
        /// Later application commit sequence whose timestamp regressed to `<= target_timestamp`.
        regressed_sequence: CommitSequence,
        /// Commit timestamp of `regressed_sequence`.
        regressed_timestamp: Timestamp,
    },
    /// The backup or archive failed verification, binding, or frame decoding, or
    /// the operation was cancelled.
    Archive(ArchiveConsumerErrorV1),
}

impl std::fmt::Display for ArchivePitrResolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TargetBeforeBackupFence {
                target_timestamp,
                backup_fence_sequence,
                ..
            } => write!(
                f,
                "target timestamp {target_timestamp:?} precedes backup fence sequence {}",
                backup_fence_sequence.get()
            ),
            Self::TargetBeforeFirstCommit {
                target_timestamp, ..
            } => write!(
                f,
                "target timestamp {target_timestamp:?} precedes the first application commit"
            ),
            Self::NonMonotonicTimestampAcrossTarget {
                target_timestamp,
                first_excluded_sequence,
                regressed_sequence,
                ..
            } => write!(
                f,
                "non-monotonic commit timestamp across target {target_timestamp:?} between sequences {} and {}",
                first_excluded_sequence.get(),
                regressed_sequence.get()
            ),
            Self::Archive(error) => write!(f, "archive PITR resolution failed: {error}"),
        }
    }
}

impl std::error::Error for ArchivePitrResolutionError {}

impl RedbVerifiedArchiveBackup {
    /// Resolves a wall-clock target timestamp to the highest application
    /// `CommitSequence` whose coordinator commit timestamp is `<= target_timestamp`.
    ///
    /// Fails closed if the backup or archive fails verification, if the target
    /// precedes the verified backup fence or first commit, or if a non-monotonic
    /// clock regression straddles `target_timestamp`.
    pub fn resolve_archive_stop_at_timestamp(
        &self,
        archive: &RedbArchiveRepository,
        target_timestamp: Timestamp,
        cancellation: &AtomicBool,
    ) -> Result<ArchivePitrResolutionV1, ArchivePitrResolutionError> {
        check_cancelled(cancellation).map_err(ArchivePitrResolutionError::Archive)?;
        self.verify_for_archive()
            .map_err(ArchivePitrResolutionError::Archive)?;
        if !archive.bound_to(
            self.history().lineage(),
            self.history().tail(),
            self.manifest_digest(),
        ) {
            return Err(ArchivePitrResolutionError::Archive(
                ArchiveConsumerErrorV1::ForeignLineage,
            ));
        }

        let fence_app_sequence = self.history().tail().frontier().application();
        let mut last_eligible: Option<(CommitSequence, Timestamp)> = None;
        let mut first_observed_commit: Option<(CommitSequence, Timestamp)> = None;

        if let Some(fence_sequence) = fence_app_sequence
            && let Some(fence_timestamp) = self.fence_commit_timestamp()
        {
            first_observed_commit = Some((fence_sequence, fence_timestamp));
            if fence_timestamp > target_timestamp {
                return Err(ArchivePitrResolutionError::TargetBeforeBackupFence {
                    target_timestamp,
                    backup_fence_sequence: fence_sequence,
                    backup_fence_timestamp: Some(fence_timestamp),
                });
            }
            last_eligible = Some((fence_sequence, fence_timestamp));
        }

        let mut first_exceeding: Option<(CommitSequence, Timestamp)> = None;
        let mut previous_sequence = fence_app_sequence;

        for item in archive.frames() {
            check_cancelled(cancellation).map_err(ArchivePitrResolutionError::Archive)?;
            let (_manifest, bytes) = item.map_err(ArchivePitrResolutionError::Archive)?;
            let frame = ChangelogFrameV3::decode(&bytes).map_err(|_| {
                ArchivePitrResolutionError::Archive(ArchiveConsumerErrorV1::InvalidFrame)
            })?;
            for receipt in frame.receipts() {
                check_cancelled(cancellation).map_err(ArchivePitrResolutionError::Archive)?;
                for mutation in receipt.mutations() {
                    if mutation.namespace() != AuthoritativeNamespaceV1::Commits {
                        continue;
                    }
                    let value = mutation.value().ok_or(ArchivePitrResolutionError::Archive(
                        ArchiveConsumerErrorV1::InvalidFrame,
                    ))?;
                    for (sequence, timestamp) in decode_commit_mutation_commands(value)
                        .map_err(ArchivePitrResolutionError::Archive)?
                    {
                        if previous_sequence.is_some_and(|prev| sequence <= prev) {
                            return Err(ArchivePitrResolutionError::Archive(
                                ArchiveConsumerErrorV1::InvalidFrame,
                            ));
                        }
                        previous_sequence = Some(sequence);
                        if first_observed_commit.is_none() {
                            first_observed_commit = Some((sequence, timestamp));
                        }
                        if timestamp <= target_timestamp {
                            if let Some((first_excluded_sequence, first_excluded_timestamp)) =
                                first_exceeding
                            {
                                return Err(
                                    ArchivePitrResolutionError::NonMonotonicTimestampAcrossTarget {
                                        target_timestamp,
                                        first_excluded_sequence,
                                        first_excluded_timestamp,
                                        regressed_sequence: sequence,
                                        regressed_timestamp: timestamp,
                                    },
                                );
                            }
                            last_eligible = Some((sequence, timestamp));
                        } else if first_exceeding.is_none() {
                            first_exceeding = Some((sequence, timestamp));
                        }
                    }
                }
            }
        }

        match last_eligible {
            Some((resolved_sequence, resolved_commit_timestamp)) => Ok(ArchivePitrResolutionV1 {
                stop: ArchiveRestoreStopV1::AtApplicationSequence(resolved_sequence),
                resolved_sequence,
                resolved_commit_timestamp,
                next_excluded_sequence: first_exceeding.map(|(seq, _)| seq),
                next_excluded_commit_timestamp: first_exceeding.map(|(_, ts)| ts),
            }),
            None => match fence_app_sequence {
                Some(backup_fence_sequence) => {
                    Err(ArchivePitrResolutionError::TargetBeforeBackupFence {
                        target_timestamp,
                        backup_fence_sequence,
                        backup_fence_timestamp: self.fence_commit_timestamp(),
                    })
                }
                None => Err(ArchivePitrResolutionError::TargetBeforeFirstCommit {
                    target_timestamp,
                    first_commit_sequence: first_observed_commit.map(|(seq, _)| seq),
                    first_commit_timestamp: first_observed_commit.map(|(_, ts)| ts),
                }),
            },
        }
    }
}

fn decode_commit_mutation_commands(
    bytes: &[u8],
) -> Result<Vec<(CommitSequence, Timestamp)>, ArchiveConsumerErrorV1> {
    match crate::command_prefix::decode_segment(bytes) {
        Ok(decoded) => Ok(decoded
            .value()
            .commands()
            .iter()
            .map(|command| {
                (
                    command.commit_sequence(),
                    command.base().commit().logical_time().timestamp(),
                )
            })
            .collect()),
        Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
            match crate::command_prefix::decode_capsule(bytes) {
                Ok(decoded) => {
                    let command = decoded.value();
                    Ok(vec![(
                        command.commit_sequence(),
                        command.base().commit().logical_time().timestamp(),
                    )])
                }
                Err(error) if error.kind() == DurableCodecErrorKind::UnexpectedRecordType => {
                    let decoded = riffdb_storage_api::decode_command_capsule_v1(bytes, Vec::new())
                        .map_err(|_| ArchiveConsumerErrorV1::InvalidFrame)?;
                    let command = decoded.value();
                    Ok(vec![(
                        command.commit_sequence(),
                        command.commit().logical_time().timestamp(),
                    )])
                }
                Err(_) => Err(ArchiveConsumerErrorV1::InvalidFrame),
            }
        }
        Err(_) => Err(ArchiveConsumerErrorV1::InvalidFrame),
    }
}

fn check_cancelled(cancellation: &AtomicBool) -> Result<(), ArchiveConsumerErrorV1> {
    if cancellation.load(Ordering::Acquire) {
        Err(ArchiveConsumerErrorV1::ResyncRequired)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // req: REC-001, REC-002, REC-003, REC-006, REP-005, REP-007, AFC-007
    use super::*;
    use riffdb_storage_api::{ArchiveConsumerV1, ArchiveEncryptionPostureV1};
    use std::sync::{Arc, Mutex};

    #[test]
    fn archive_pitr_refuses_empty_pruned_foreign_and_cancelled_inputs_and_notifies_confirmed_frames()
     {
        let scope = crate::test_path::ScopedDirectory::new("archive-pitr-unit");
        let (backup_path, history) =
            crate::maintenance::archive_backup_tests::backup_current_source(&scope);
        let binding = RedbVerifiedArchiveBackup::open(&backup_path).unwrap();
        assert_eq!(binding.fence_commit_timestamp(), None);

        let observed = Arc::new(Mutex::new(Vec::<ArchiveFrameSummaryV1>::new()));
        let observed_clone = Arc::clone(&observed);
        let mut repository = binding
            .open_archive(
                &scope.join("archive"),
                ArchiveEncryptionPostureV1::Unencrypted,
            )
            .unwrap();
        repository.set_confirmed_frame_observer(Arc::new(move |summary| {
            observed_clone.lock().unwrap().push(summary.clone());
        }));

        let target = Timestamp::new(1_700_000_000, 0).unwrap();
        assert_eq!(
            binding.resolve_archive_stop_at_timestamp(&repository, target, &AtomicBool::new(false)),
            Err(ArchivePitrResolutionError::TargetBeforeFirstCommit {
                target_timestamp: target,
                first_commit_sequence: None,
                first_commit_timestamp: None,
            })
        );
        assert_eq!(
            binding.resolve_archive_stop_at_timestamp(&repository, target, &AtomicBool::new(true)),
            Err(ArchivePitrResolutionError::Archive(
                ArchiveConsumerErrorV1::ResyncRequired
            ))
        );

        let foreign = RedbArchiveRepository::open(
            &scope.join("foreign"),
            history.lineage(),
            history.tail(),
            [0x99; 32],
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
        assert_eq!(
            binding.resolve_archive_stop_at_timestamp(&foreign, target, &AtomicBool::new(false)),
            Err(ArchivePitrResolutionError::Archive(
                ArchiveConsumerErrorV1::ForeignLineage
            ))
        );

        let mut consumer = ArchiveConsumerV1::new(repository, history.lineage(), history.tail());
        let first_bytes = crate::maintenance::archive_repository_tests::fixture::frame(
            history.lineage(),
            history.tail(),
            1,
        );
        consumer.begin_stream().unwrap();
        let after_first = consumer.append(first_bytes).unwrap();
        let repository = consumer.into_sink();

        let summaries = repository.frame_summaries(&AtomicBool::new(false)).unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].manifest().covered(), after_first);
        assert_eq!(
            summaries[0].frame_file_name(),
            format!("frame-{:016x}.v3", history.tail().sequence().get() + 1)
        );
        assert_eq!(
            summaries[0].manifest_file_name(),
            format!("manifest-{:016x}.v1", history.tail().sequence().get() + 1)
        );
        assert_eq!(summaries[0].command_count(), 0);
        assert_eq!(summaries[0].first_application_sequence(), None);
        assert_eq!(summaries[0].last_application_sequence(), None);
        assert_eq!(summaries[0].first_commit_timestamp(), None);
        assert_eq!(summaries[0].last_commit_timestamp(), None);
        assert_eq!(*observed.lock().unwrap(), summaries);
    }
}
