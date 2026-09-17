//! Closed audited registration/retirement lane at the drained source barrier.
use super::*;
use crate::layout::{AUDIT, CAPABILITIES, CAPABILITY_TOKENS};
use riffdb_storage_api::{
    AuthoritativeMutationV3 as Mutation, CapabilityLifecycleV1,
    ReplicationAdministrationActionV1 as Action, ReplicationAdministrationAwaitingDecision,
    ReplicationAdministrationCandidateTransaction,
    ReplicationAdministrationCandidateV1 as Candidate, ReplicationAdministrationIntentV1 as Intent,
    ReplicationAdministrationOriginV1 as Origin, ReplicationAdministrationRefusalV1 as Refused,
    ReplicationAdministrationResultV1 as ResultV1, ReplicationAdministrationTransactionPort,
    ReplicationSourceHoldV2 as Policy, StoredReplicationAdministrationV1 as Record,
    TransactionCurrentCapabilityObservationV1 as Current,
    proto_codec::{decode_replication_administration_v1, encode_replication_administration_v1},
};
use transaction::ControlWrite;

/// Exclusive candidate before transaction-current policy observation.
pub struct RedbReplicationAdministrationCandidate {
    write: ControlWrite,
    candidate: Candidate,
}
/// Frozen candidate and authority held across the coordinator's pure decision.
pub struct RedbReplicationAdministrationAwaiting {
    write: ControlWrite,
    candidate: Candidate,
    current: Option<Current>,
}

impl ReplicationAdministrationTransactionPort for RedbOperationalPorts {
    type Candidate = RedbReplicationAdministrationCandidate;
    fn begin_replication_administration(
        &self,
        candidate: Candidate,
    ) -> Result<Self::Candidate, StorageError> {
        if self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let write = Barrier::acquire(Arc::clone(&self.shared))?.begin()?;
        Ok(RedbReplicationAdministrationCandidate { write, candidate })
    }
}
impl ReplicationAdministrationCandidateTransaction for RedbReplicationAdministrationCandidate {
    type Awaiting = RedbReplicationAdministrationAwaiting;
    fn read_transaction_current(self) -> Result<(Self::Awaiting, Option<Current>), StorageError> {
        let current = current(&self.write, &self.candidate)?;
        Ok((
            RedbReplicationAdministrationAwaiting {
                write: self.write,
                candidate: self.candidate,
                current: current.clone(),
            },
            current,
        ))
    }
    fn abandon(self) {}
}
impl ReplicationAdministrationAwaitingDecision for RedbReplicationAdministrationAwaiting {
    fn commit(self, intent: Intent) -> Result<ResultV1, StorageError> {
        if intent.candidate() != &self.candidate
            || current(&self.write, &self.candidate)? != self.current
            || !matches_principal(&intent, self.current.as_ref())
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        apply(self.write, intent)
    }
    fn abandon(self) {}
}

fn current(write: &ControlWrite, candidate: &Candidate) -> Result<Option<Current>, StorageError> {
    let transaction = write.transaction()?;
    let capabilities = transaction.open_table(CAPABILITIES).map_err(table_error)?;
    let tokens = transaction
        .open_table(CAPABILITY_TOKENS)
        .map_err(table_error)?;
    Ok(crate::administration::capability_from_tables(
        &capabilities,
        &tokens,
        write.history().lineage().database_id(),
        candidate.principal().capability_id(),
    )?
    .as_ref()
    .map(Current::from_record))
}
fn matches_principal(intent: &Intent, current: Option<&Current>) -> bool {
    let principal = intent.candidate().principal();
    current.is_some_and(|current| {
        current.capability_id() == principal.capability_id()
            && current.revision() == principal.capability_revision()
            && current.principal_id() == principal.principal_id()
            && current.actor_kind() == principal.actor_kind()
            && current.lifecycle() == &CapabilityLifecycleV1::Active
            && current.issued_at() <= intent.timestamp()
            && intent.timestamp() < current.expires_at()
    })
}

fn apply(write: ControlWrite, intent: Intent) -> Result<ResultV1, StorageError> {
    let request = intent.candidate().request();
    let target = request.target();
    let history = write.history();
    let lineage = history.lineage();
    if target.database_id() != lineage.database_id()
        || target.history_incarnation() != lineage.history_incarnation()
        || target.leadership_epoch() != lineage.leadership_epoch()
    {
        return Ok(ResultV1::Refused(Refused::LineageMismatch));
    }
    let key = Hold::storage_key_for(target.hold_id(), Kind::FollowerAcknowledgement);
    let (before, count) = {
        let holds = write
            .transaction()?
            .open_table(SOURCE_HOLDS)
            .map_err(table_error)?;
        let before = holds
            .get(key.as_slice())
            .map_err(precommit_storage_error)?
            .map(|row| {
                decode_replication_source_hold(row.value())
                    .map(|v| *v.value())
                    .map_err(crate::error::codec_error)
            })
            .transpose()?;
        (before, holds.len().map_err(precommit_storage_error)?)
    };
    let after = match (request.registration_policy(), before) {
        (Some((budget, expires_at)), Some(State::Registered(policy))) => {
            if budget != policy.budget() || expires_at != policy.expires_at() {
                return Ok(ResultV1::Refused(Refused::RegistrationConflict));
            }
            // Immutable operation replay survives attachment, acknowledgement and
            // retirement. Return evidence; never rewrite or resurrect the hold.
            return replay(&write, policy, Action::RegisterFollower);
        }
        (Some((budget, expires_at)), prior) => {
            if expires_at
                .is_some_and(|expiry| Some(expiry) <= history.tail().frontier().application())
            {
                return Ok(ResultV1::Refused(Refused::ExpiryReached));
            }
            let (hold, phase) = match prior {
                None => {
                    if count >= MAX_REPLICATION_SOURCE_HOLDS_V1 {
                        return Ok(ResultV1::Refused(Refused::CapacityExhausted));
                    }
                    (
                        Hold::new(
                            target.hold_id(),
                            Kind::FollowerAcknowledgement,
                            lineage,
                            history.tail(),
                        ),
                        Phase::AwaitingBootstrap,
                    )
                }
                Some(State::Legacy(hold)) => (hold, Phase::Attached),
                Some(State::Registered(_)) => return Err(invariant()),
            };
            Policy::new(hold, history.tail(), budget, expires_at, phase, None)
                .map_err(value_error)?
        }
        (None, Some(State::Registered(policy))) => {
            if request.retirement_generation() != Some(policy.generation()) {
                return Ok(ResultV1::Refused(Refused::RegistrationMissingOrStale));
            }
            if policy.phase() == Phase::Retired {
                return replay(&write, policy, Action::RetireFollower);
            }
            Policy::new(
                policy.hold(),
                policy.registered_at(),
                policy.budget(),
                policy.expires_at(),
                Phase::Retired,
                policy.degraded_at(),
            )
            .map_err(value_error)?
        }
        (None, _) => return Ok(ResultV1::Refused(Refused::RegistrationMissingOrStale)),
    };
    let transaction = write.transaction()?;
    let allocator_bytes = transaction
        .open_table(META)
        .map_err(table_error)?
        .get(META_ADMINISTRATION_SEQUENCE)
        .map_err(precommit_storage_error)?
        .ok_or_else(corrupt)?
        .value()
        .to_vec();
    let allocator = *decode_administration_sequence_allocator_v1(&allocator_bytes)
        .map_err(crate::error::codec_error)?
        .value();
    let allocation = allocator
        .allocate_consecutive(1)
        .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
    let sequence = allocation.assigned()[0];
    let record = Record::new(
        sequence,
        intent.timestamp(),
        request.action(),
        target,
        before,
        after,
        history.tail(),
        Origin::Explicit {
            request_id: request.request_id(),
            principal: intent.candidate().principal().clone(),
            approval_id: None,
        },
    )
    .map_err(|_| invariant())?;
    let audit = encode_replication_administration_v1(&record).map_err(crate::error::codec_error)?;
    let allocator = encode_administration_sequence_allocator_v1(allocation.next())
        .map_err(crate::error::codec_error)?;
    let hold = encode_replication_source_hold_v2(after).map_err(crate::error::codec_error)?;
    let audit_key = crate::keys::encode_audit_key(sequence);
    let mut mutations = vec![
        Mutation::put(N::Audit, &audit_key, None, audit.as_bytes()).map_err(value_error)?,
        Mutation::replace(
            N::NextAdministrationSequence,
            META_ADMINISTRATION_SEQUENCE.as_bytes(),
            &allocator_bytes,
            allocator.as_bytes(),
        )
        .map_err(value_error)?,
    ];
    mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: lineage.database_id(),
            history_incarnation: lineage.history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history
                .tail()
                .sequence()
                .checked_next()
                .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: DualFrontier::new(
                history.tail().frontier().application(),
                Some(sequence),
            ),
            prior_history_hash: history.tail().history_hash(),
        },
        Source::RetentionHold,
        mutations,
    )
    .map_err(value_error)?;
    let advance = PreparedHistoryAdvance::prepare(transaction, &receipt)?;
    if transaction
        .open_table(AUDIT)
        .map_err(table_error)?
        .insert(audit_key.as_slice(), audit.as_bytes())
        .map_err(precommit_storage_error)?
        .is_some()
    {
        return Err(corrupt());
    }
    crash_edge("administration-audit");
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
        .map_err(precommit_storage_error)?;
    {
        let mut holds = transaction.open_table(SOURCE_HOLDS).map_err(table_error)?;
        holds
            .insert(key.as_slice(), hold.as_bytes())
            .map_err(precommit_storage_error)?;
        if after.phase() == Phase::Retired {
            // Only this identity's bootstrap job loses custody. Archive holds
            // remain independent even when they happen to have the same ID.
            let bootstrap = Hold::storage_key_for(target.hold_id(), Kind::Bootstrap);
            holds
                .remove(bootstrap.as_slice())
                .map_err(precommit_storage_error)?;
        }
    }
    crash_edge("administration-hold");
    advance.stage(transaction)?;
    write.validate()?;
    crash_edge("administration-receipted");
    write.commit()?;
    crash_edge("administration-committed");
    Ok(ResultV1::Applied(Box::new(record)))
}

fn replay(write: &ControlWrite, policy: Policy, action: Action) -> Result<ResultV1, StorageError> {
    // The same-pin complete validation above reaches exact end and proves unique
    // registration/release linkage. Locate that receipt without collecting rows.
    let audit = write
        .transaction()?
        .open_table(AUDIT)
        .map_err(table_error)?;
    for row in audit.iter().map_err(precommit_storage_error)? {
        let (_, bytes) = row.map_err(precommit_storage_error)?;
        let decoded = match decode_replication_administration_v1(bytes.value()) {
            Ok(decoded) => decoded,
            Err(error)
                if error.kind()
                    == riffdb_storage_api::DurableCodecErrorKind::UnexpectedRecordType =>
            {
                continue;
            }
            Err(error) => return Err(crate::error::codec_error(error)),
        };
        let record = decoded.value();
        if record.after().hold().lineage() == policy.hold().lineage()
            && record.after().hold().id() == policy.hold().id()
            && record.generation() == policy.generation()
            && record.action() == action
        {
            return Ok(ResultV1::Replayed(Box::new(record.clone())));
        }
    }
    // An expiry is a different authoritative release action, never the result
    // of an explicit retirement. The already validated tombstone stays intact.
    if action == Action::RetireFollower {
        Ok(ResultV1::Refused(Refused::RegistrationConflict))
    } else {
        Err(corrupt())
    }
}
fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}
fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}
