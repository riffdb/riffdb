//! A closed source policy continuation, one bounded transition per coordinator turn.
use super::*;
use riffdb_storage_api::{
    ReplicationAdministrationActionV1 as Action, ReplicationAdministrationOriginV1 as Origin,
    ReplicationRegistrationMaintenancePort, ReplicationRegistrationMaintenanceResultV1 as ResultV1,
    ReplicationSourceHoldV2 as Policy,
};
use riffdb_types::{ReplicationFollowerAuditTargetV1, Timestamp};

impl ReplicationRegistrationMaintenancePort for RedbOperationalPorts {
    fn maintain_replication_registration(
        &self,
        timestamp: Timestamp,
    ) -> Result<ResultV1, StorageError> {
        if self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        let history = barrier.observation().history;
        let selected = select(&barrier.root, history)?;
        let Some((prior, expires)) = selected else {
            return Ok(ResultV1::Idle);
        };
        let write = barrier.begin()?;
        if !expires {
            let after = Policy::new(
                prior.hold(),
                prior.registered_at(),
                prior.budget(),
                prior.expires_at(),
                prior.phase(),
                Some(history.tail()),
            )
            .map_err(value_error)?;
            let encoded =
                encode_replication_source_hold_v2(after).map_err(crate::error::codec_error)?;
            let (receipt, _) = prepare_control_receipt(
                write.transaction()?,
                history,
                Source::ReplicationSourceHold,
            )?;
            write
                .transaction()?
                .open_table(SOURCE_HOLDS)
                .map_err(table_error)?
                .insert(after.hold().storage_key().as_slice(), encoded.as_bytes())
                .map_err(precommit_storage_error)?;
            crash_edge("registration-health-staged");
            receipt.stage(write.transaction()?)?;
            write.validate()?;
            crash_edge("registration-health-receipted");
            write.commit_retention()?;
            crash_edge("registration-health-committed");
            return Ok(ResultV1::HealthRecorded(Box::new(after)));
        }
        // Selection requires a previously durable degradation point. This step
        // revalidated source links under a fresh barrier after its publication;
        // no caller-selected policy or inferred authentication can enter expiry.
        let hold = prior.hold();
        let lineage = hold.lineage();
        let target = ReplicationFollowerAuditTargetV1::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch(),
            hold.id(),
        )
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        let after = Policy::new(
            hold,
            prior.registered_at(),
            prior.budget(),
            prior.expires_at(),
            Phase::Retired,
            prior.degraded_at(),
        )
        .map_err(value_error)?;
        let registration = prior
            .registered_at()
            .frontier()
            .administration()
            .map_or(0, |s| s.get())
            .checked_add(1)
            .and_then(AdministrationSequence::new)
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        administration::commit_transition(
            write,
            timestamp,
            Action::ExpireFollower,
            target,
            Some(State::Registered(prior)),
            after,
            Origin::ConfiguredExpiry { registration },
        )
        .map(ResultV1::Expired)
    }
}

fn select(
    root: &crate::checkpoint_root::CheckpointRoot,
    history: History,
) -> Result<Option<(Policy, bool)>, StorageError> {
    let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
    if holds.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    for row in holds.iter().map_err(precommit_storage_error)? {
        let (_, bytes) = row.map_err(precommit_storage_error)?;
        let State::Registered(policy) = *decode_replication_source_hold(bytes.value())
            .map_err(crate::error::codec_error)?
            .value()
        else {
            continue;
        };
        if policy.phase() == Phase::Retired {
            continue;
        }
        let exhausted = policy
            .budget()
            .observe(policy.hold(), history)
            .map_err(value_error)?
            .is_exhausted();
        let expired = policy
            .expires_at()
            .is_some_and(|expiry| Some(expiry) <= history.tail().frontier().application());
        if (exhausted || expired) && policy.degraded_at().is_none() {
            return Ok(Some((policy, false)));
        }
        if expired && policy.degraded_at().is_some() {
            return Ok(Some((policy, true)));
        }
    }
    Ok(None)
}
