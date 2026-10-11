//! Finite coverage supplied by two actual independently durable observations.
//! This scalar certificate supplies a ceiling, never source-cut authority.
use super::*;
use crate::source_validation_read::ReceiptRead;
use riffdb_storage_api::ChangelogHistoryPointV3 as Point;

#[derive(Clone, Copy)]
pub(super) struct CheckpointLicense {
    checkpoint: Observation,
    ceiling: Point,
    resume: Point,
    qualification: u64,
}

impl CheckpointLicense {
    pub(super) fn new(
        previous: Observation,
        checkpoint: Observation,
        history: History,
        qualification: u64,
    ) -> Option<Self> {
        (qualification != u64::MAX
            && previous.publication != checkpoint.publication
            && previous.durable_epoch < checkpoint.durable_epoch
            && previous.history.lineage() == history.lineage()
            && previous.history.anchor() == history.anchor()
            && checkpoint.history.lineage() == history.lineage()
            && checkpoint.history.anchor() == history.anchor()
            && checkpoint.history.tail().sequence() >= previous.history.tail().sequence()
            && previous.history.tail().sequence() > history.minimum_resume().sequence())
        .then_some(Self {
            checkpoint,
            ceiling: previous.history.tail(),
            resume: history.minimum_resume(),
            qualification,
        })
    }

    pub(super) fn ceiling(self) -> Point {
        self.ceiling
    }

    /// Classified checkpoint materialization can replace the original installed
    /// checkpoint. It must still cover the exact licensed point, with the same
    /// lineage and anchor and no lost independent checkpoint coverage.
    pub(super) fn covers(
        self,
        source: &crate::source_validation_pin::ValidationSource,
        history: History,
        checkpoint: Observation,
        qualification: u64,
    ) -> Result<bool, StorageError> {
        if qualification == u64::MAX
            || qualification != self.qualification
            || history.lineage() != self.checkpoint.history.lineage()
            || history.anchor() != self.checkpoint.history.anchor()
            || history.minimum_resume() != self.resume
            || self.resume.sequence() >= self.ceiling.sequence()
            || checkpoint.history.lineage() != history.lineage()
            || checkpoint.history.anchor() != history.anchor()
            || checkpoint.history.tail().sequence() < self.checkpoint.history.tail().sequence()
            || checkpoint.history.minimum_resume().sequence() > self.resume.sequence()
            || checkpoint.durable_epoch < self.checkpoint.durable_epoch
        {
            return Ok(false);
        }
        let root = crate::source_validation_pin::ValidationSource::Checkpoint(Arc::clone(
            source.checkpoint_pin(),
        ));
        let receipt = root
            .receipt(self.ceiling.sequence(), None)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        Ok(Point::from_receipt(&receipt).map_err(value_error)? == self.ceiling)
    }

    /// A checked acknowledgement can requalify this exact finite ceiling;
    /// it changes neither independent checkpoint evidence nor remaining span.
    pub(super) fn rebase_own_acknowledgement(
        &mut self,
        publication: &AcknowledgementPublication,
    ) -> Result<bool, StorageError> {
        let Some((before, after, root, epoch)) = publication.successor() else {
            return Ok(true);
        };
        if self.qualification.checked_add(1) != Some(epoch)
            || epoch == u64::MAX
            || before.lineage() != self.checkpoint.history.lineage()
            || before.anchor() != self.checkpoint.history.anchor()
            || before.minimum_resume() != self.resume
            || after.minimum_resume() != self.resume
            || self.ceiling.sequence() > before.tail().sequence()
        {
            return Ok(false);
        }
        let source = crate::source_validation_pin::ValidationSource::Checkpoint(Arc::clone(root));
        let receipt = source
            .receipt(self.ceiling.sequence(), None)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        if Point::from_receipt(&receipt).map_err(value_error)? != self.ceiling {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        self.qualification = epoch;
        Ok(true)
    }

    /// Only this owner's acknowledged, predecessor-checked cut can move resume.
    pub(super) fn consume(&mut self, resume: Point, qualification: u64) -> bool {
        if qualification == u64::MAX
            || resume.sequence() <= self.resume.sequence()
            || resume.sequence() > self.ceiling.sequence()
        {
            return false;
        }
        self.resume = resume;
        self.qualification = qualification;
        self.resume.sequence() < self.ceiling.sequence()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(ports: &RedbOperationalPorts) {
        let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
            &ports.shared.database,
            crate::RedbCommitProfile::Hardened,
            Source::OutboxTransition,
        )
        .unwrap();
        write
            .open_table(crate::layout::OUTBOX_STATUS)
            .unwrap()
            .insert(b"license-fixture".as_slice(), b"independent".as_slice())
            .unwrap();
        write.finish().unwrap().commit(&ports.shared).unwrap();
    }

    // req: STO-030, STO-031, STO-032, REC-001, END-008
    #[test]
    fn checkpoint_license_refuses_changed_lineage_holds_and_unclassified_publication() {
        let (_scope, ports, states) = crate::changelog_v3_control_tests::fixture();
        for _ in 0..300 {
            append(&ports);
        }
        let mut control = ports.replication_source_control();
        let hold = Hold::new(
            riffdb_storage_api::ReplicationSourceHoldIdV1::new([0x63; 16]).unwrap(),
            Kind::ArchiveAcknowledgement,
            states[0].lineage(),
            states[0].tail(),
        );
        control.register(hold).unwrap();
        for _ in 0..16 {
            assert_eq!(control.reclaim_history_online().unwrap().0, 0);
            for _ in 0..8 {
                control.advance_validation_off_gate().unwrap();
            }
            if control.reclamation_yields()[0] == 1 {
                break;
            }
        }
        assert_eq!(control.reclamation_yields()[0], 1);
        append(&ports);
        for _ in 0..16 {
            assert_eq!(control.reclaim_history_online().unwrap().0, 0);
            for _ in 0..8 {
                control.advance_validation_off_gate().unwrap();
            }
            if control.online_license.is_some() {
                break;
            }
        }
        let license = control
            .online_license
            .expect("a current hold preserves the finite license without granting deletion");
        let source = ports.shared.capture_source_validation_pin().unwrap();
        let history = source.history().unwrap().unwrap();
        let checkpoint = Observation {
            publication: source.checkpoint_pin().identity(),
            durable_epoch: ports.shared.durable_commit_epoch(),
            history,
        };
        let epoch = ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        assert!(license.covers(&source, history, checkpoint, epoch).unwrap());
        assert!(
            !license
                .covers(&source, history, checkpoint, epoch + 1)
                .unwrap()
        );
        let foreign = riffdb_storage_api::ChangelogLineageV3::new(
            history.lineage().database_id(),
            history.lineage().history_incarnation() + 1,
            history.lineage().leadership_epoch(),
        )
        .unwrap();
        let changed = History::new(
            foreign,
            history.anchor(),
            history.tail(),
            history.minimum_resume(),
        )
        .unwrap();
        assert!(!license.covers(&source, changed, checkpoint, epoch).unwrap());
        let missing_coverage = Observation {
            history: states[0],
            ..checkpoint
        };
        assert!(
            !license
                .covers(&source, history, missing_coverage, epoch)
                .unwrap()
        );
        drop(source);
        control
            .advance_acknowledgement(Hold::new(
                hold.id(),
                hold.kind(),
                hold.lineage(),
                states[2].tail(),
            ))
            .unwrap();
        assert_eq!(control.advance_validation_off_gate().unwrap()[2], 1);
        assert!(
            control.online_license.is_none(),
            "changed holds require complete reconstruction"
        );
        ports.shared.invalidate_source_continuation();
        for _ in 0..16 {
            control.reclaim_history_online().unwrap();
            for _ in 0..8 {
                control.advance_validation_off_gate().unwrap();
            }
        }
        let current = ports
            .shared
            .capture_source_validation_pin()
            .unwrap()
            .history()
            .unwrap()
            .unwrap();
        assert!(
            current.minimum_resume().sequence() <= states[2].tail().sequence(),
            "no relicense can bypass the current hold"
        );
        assert!(
            std::mem::size_of::<CheckpointLicense>()
                + std::mem::size_of::<eligible_bytes::Observer>()
                <= 64 * 1024
        );
    }
}
