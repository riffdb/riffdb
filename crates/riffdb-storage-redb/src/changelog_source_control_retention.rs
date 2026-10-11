//! Bounded deletion under the parent's drained lease and checkpoint evidence.
//! No surviving receipt or authoritative row is rewritten.

use super::*;
use riffdb_storage_api::{
    ChangelogHistoryPointV3 as Point,
    proto_codec::{decode_replication_source_hold, encode_changelog_history_state_v3},
};

const MAX_RECLAIMED_RECEIPTS: u64 = 256;

pub(super) fn floor(
    root: &crate::checkpoint_root::CheckpointRoot,
    history: History,
    prior_checkpoint: Point,
    derived_floor: Option<u64>,
) -> Result<(Option<Point>, [u64; 8]), StorageError> {
    floor_with(root, history, prior_checkpoint, derived_floor, |maximum| {
        let table = root.open_table(HISTORY).map_err(table_error)?;
        let row = table
            .get(maximum.to_be_bytes().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
        let receipt = AuthoritativeTransactionV3::decode(
            &crate::history_stub::resolve_history_value(row.value())?,
        )
        .map_err(value_error)?;
        Point::from_receipt(&receipt).map_err(value_error)
    })
}

/// Journaled planning keeps receipt reads on the same fully validated source.
/// The checkpoint component still supplies the existing bounded durable holds;
/// it cannot stand in for the source's acknowledged receipt suffix.
pub(super) fn floor_on_source(
    source: &crate::source_validation_pin::ValidationSource,
    history: History,
    prior_checkpoint: Point,
    derived_floor: Option<u64>,
) -> Result<(Option<Point>, [u64; 8]), StorageError> {
    use crate::source_validation_read::ReceiptRead;
    floor_with(
        source.checkpoint_pin(),
        history,
        prior_checkpoint,
        derived_floor,
        |maximum| {
            let sequence = riffdb_storage_api::ChangelogTransactionSequence::new(maximum)
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let receipt = source
                .receipt(sequence, None)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            Point::from_receipt(&receipt).map_err(value_error)
        },
    )
}

fn floor_with(
    root: &crate::checkpoint_root::CheckpointRoot,
    history: History,
    prior_checkpoint: Point,
    derived_floor: Option<u64>,
    point: impl FnOnce(u64) -> Result<Point, StorageError>,
) -> Result<(Option<Point>, [u64; 8]), StorageError> {
    // The barrier completely validated the bounded holds and retained chain.
    // A captured validation candidate may predate the independently installed
    // checkpoint. That later evidence cannot extend the candidate's own tail.
    let mut maximum = prior_checkpoint
        .sequence()
        .get()
        .min(history.tail().sequence().get());
    if let Some(derived_floor) = derived_floor {
        maximum = maximum.min(derived_floor);
    }
    let holds = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
    let mut hold_floor: Option<u64> = None;
    let mut hold_count = 0_u64;
    for row in holds.iter().map_err(precommit_storage_error)? {
        let (_, value) = row.map_err(precommit_storage_error)?;
        let state = *decode_replication_source_hold(value.value())
            .map_err(crate::error::codec_error)?
            .value();
        let hold = match state {
            riffdb_storage_api::ReplicationSourceHoldStateV1::Legacy(hold) => hold,
            riffdb_storage_api::ReplicationSourceHoldStateV1::Registered(policy) => {
                if policy.phase() == riffdb_storage_api::FollowerRegistrationPhaseV1::Retired {
                    continue;
                }
                policy.hold()
            }
        };
        hold_count = hold_count.saturating_add(1);
        hold_floor = Some(hold_floor.map_or(hold.fence().sequence().get(), |prior| {
            prior.min(hold.fence().sequence().get())
        }));
        maximum = maximum.min(hold.fence().sequence().get());
    }
    let first = history.minimum_resume().sequence().get();
    // Planning-only numbers from this exact barrier. Optional fences have
    // explicit presence bits; zero never stands for a missing protected fence.
    let observation = [
        first,
        history.tail().sequence().get(),
        prior_checkpoint.sequence().get(),
        derived_floor.unwrap_or(0),
        hold_floor.unwrap_or(0),
        maximum,
        hold_count,
        u64::from(derived_floor.is_some()) | (u64::from(hold_floor.is_some()) << 1),
    ];
    maximum = maximum.min(first.saturating_add(MAX_RECLAIMED_RECEIPTS));
    if maximum <= first {
        return Ok((None, observation));
    }
    point(maximum).map(|point| (Some(point), observation))
}

pub(super) fn stage(
    transaction: &WriteTransaction,
    validated: &crate::checkpoint_root::CheckpointRoot,
    predecessor: History,
    successor: History,
    floor: Point,
    receipt: PreparedHistoryAdvance,
) -> Result<(), StorageError> {
    let first = predecessor.minimum_resume().sequence().get();
    let end = floor.sequence().get();
    if end <= first || end - first > MAX_RECLAIMED_RECEIPTS {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let retained = History::new(
        successor.lineage(),
        successor.anchor(),
        successor.tail(),
        floor,
    )
    .map_err(value_error)?;
    let encoded = encode_changelog_history_state_v3(retained).map_err(crate::error::codec_error)?;
    {
        let prior = validated.open_table(HISTORY).map_err(table_error)?;
        let mut table = transaction.open_table(HISTORY).map_err(table_error)?;
        for sequence in first..end {
            let key = sequence.to_be_bytes();
            let expected = prior
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            let removed = table
                .remove(key.as_slice())
                .map_err(precommit_storage_error)?
                .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
            if removed.value() != expected.value() {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
        }
    }
    crash_edge("reclamation-deleted");
    receipt.stage(transaction)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(
            N::ChangelogHistoryState
                .metadata_key()
                .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?,
            encoded.as_bytes(),
        )
        .map_err(precommit_storage_error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: STO-030, STO-031, STO-032, REC-001, END-008
    #[test]
    fn source_floor_never_reads_beyond_its_older_captured_history() {
        let (_scope, ports, _) = crate::changelog_v3_control_tests::fixture();
        let source = ports.shared.capture_source_validation_pin().unwrap();
        let history = source.history().unwrap().unwrap();
        let checkpoint_before = source.checkpoint_pin().identity();
        let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
            &ports.shared.database,
            crate::RedbCommitProfile::Hardened,
            riffdb_storage_api::ChangelogAttributionV3::OutboxTransition,
        )
        .unwrap();
        write
            .open_table(crate::layout::OUTBOX_STATUS)
            .unwrap()
            .insert(b"older-floor-fixture".as_slice(), b"later".as_slice())
            .unwrap();
        write.finish().unwrap().commit(&ports.shared).unwrap();
        let later_source = ports.shared.capture_source_validation_pin().unwrap();
        let later = later_source.history().unwrap().unwrap();
        assert!(later_source.checkpoint_pin().identity() > checkpoint_before);
        assert!(later.tail().sequence() > history.tail().sequence());
        let result = floor_on_source(&source, history, later.tail(), None);
        assert!(
            matches!(result, Ok((Some(point), _)) if point == history.tail()),
            "planning cannot look beyond its captured source tail; refusal_kind={:?}",
            result.as_ref().err().map(StorageError::kind)
        );
    }
    fn source_audit_intent(ordinal: u64) -> riffdb_storage_api::ServiceAuditAppendIntentV1 {
        use riffdb_storage_api::{AuditPrincipalV1, ServiceAuditAppendIntentV1};
        use riffdb_types::{
            ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
            ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
        };
        ServiceAuditAppendIntentV1::new(
            RequestId::from_unix_milliseconds_and_random(19 + ordinal, [0x51; 10]).unwrap(),
            Timestamp::new(1700000000, 0).unwrap(),
            ServiceOperationV1::GetHealth,
            ServiceAuditPhaseV1::Denied,
            AuditPrincipalV1::new(
                ActorId::new("checkpoint-fixture").unwrap(),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(19, [0x52; 10]).unwrap(),
                std::num::NonZeroU64::MIN,
            ),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .unwrap()
    }
    fn checkpoint(ports: &RedbOperationalPorts) {
        let writer = ports.shared.mutation_gate.acquire().unwrap();
        ports
            .shared
            .checkpoint_published_journal_suffix_for_barrier()
            .unwrap();
        drop(writer);
        ports.shared.join_journal_sealer();
    }
    fn append_for_profile(
        ports: &mut RedbOperationalPorts,
        ordinal: u64,
        profile: crate::RedbCommitProfile,
    ) {
        use riffdb_storage_api::{ServiceAuditAppendRepository, ServiceAuditGroupAppend};
        assert_eq!(profile, crate::RedbCommitProfile::Standard);
        let ServiceAuditGroupAppend::Submitted(fence) = ports
            .submit_service_audit_group(&[source_audit_intent(ordinal)])
            .unwrap()
        else {
            panic!("ordinary journal acknowledgement required");
        };
        assert_eq!(fence.wait().unwrap().len(), 1);
    }
    // req: STO-030, STO-031, STO-032, REC-001, END-008
    #[test]
    fn source_floor_resolves_a_licensed_receipt_from_its_validated_composite_pin() {
        use crate::source_validation_pin::ValidationSource;
        use crate::source_validation_read::ReceiptRead;
        let (_scope, mut ports, _) = crate::changelog_v3_control_tests::fixture();
        Arc::get_mut(&mut ports.shared)
            .unwrap()
            .application_commit_profile = crate::RedbCommitProfile::Standard;
        append_for_profile(&mut ports, 1, crate::RedbCommitProfile::Standard);
        let source = ports.shared.capture_source_validation_pin().unwrap();
        assert!(matches!(&source, ValidationSource::Composite(_)));
        let history = source.history().unwrap().unwrap();
        let captured_checkpoint =
            crate::changelog_v3_roots::read_checkpoint_roots(source.checkpoint_pin())
                .unwrap()
                .unwrap();
        assert!(
            captured_checkpoint.tail().sequence() < history.tail().sequence(),
            "the real acknowledged suffix must remain outside this captured checkpoint"
        );
        checkpoint(&ports);
        let (previous, previous_observation) = {
            let pin = ports.shared.capture_source_validation_pin().unwrap();
            let history = pin.history().unwrap().unwrap();
            (
                history,
                Observation {
                    publication: pin.checkpoint_pin().identity(),
                    durable_epoch: ports.shared.durable_commit_epoch(),
                    history,
                },
            )
        };
        append_for_profile(&mut ports, 2, crate::RedbCommitProfile::Standard);
        checkpoint(&ports);
        let (current, current_observation) = {
            let pin = ports.shared.capture_source_validation_pin().unwrap();
            let history = pin.history().unwrap().unwrap();
            (
                history,
                Observation {
                    publication: pin.checkpoint_pin().identity(),
                    durable_epoch: ports.shared.durable_commit_epoch(),
                    history,
                },
            )
        };
        assert_eq!(previous.tail(), history.tail());
        assert!(
            current.tail().sequence() > previous.tail().sequence(),
            "the licensed point must have real later independent checkpoint coverage"
        );
        let receipt = source
            .receipt(previous.tail().sequence(), None)
            .unwrap()
            .unwrap();
        assert_eq!(
            riffdb_storage_api::ChangelogHistoryPointV3::from_receipt(&receipt).unwrap(),
            previous.tail()
        );
        assert!(
            ValidationSource::Checkpoint(Arc::clone(source.checkpoint_pin()))
                .receipt(previous.tail().sequence(), None)
                .unwrap()
                .is_none()
        );
        let result = floor_on_source(&source, history, previous.tail(), None);
        assert!(
            matches!(result, Ok((Some(point), _)) if point == previous.tail()),
            "eligibility planning must resolve the receipt from the validated source, including its acknowledged suffix; refusal_kind={:?}",
            result.as_ref().err().map(StorageError::kind)
        );
        let epoch = ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire);
        let license = super::super::checkpoint_license::CheckpointLicense::new(
            previous_observation,
            current_observation,
            history,
            epoch,
        )
        .unwrap();
        assert!(
            license
                .covers(&source, history, current_observation, epoch)
                .unwrap(),
            "real independent checkpoint coverage must validate its exact point on the same acknowledged composite pin"
        );
    }
}
