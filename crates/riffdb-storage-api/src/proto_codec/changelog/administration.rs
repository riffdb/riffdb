//! Exact lifecycle evidence, distinct from frozen projection retention records.
use super::super::{
    audit_principal_from_proto, audit_principal_to_proto, timestamp_from_proto, timestamp_to_proto,
};
use super::*;
use crate::{
    ReplicationAdministrationActionV1 as Action, ReplicationAdministrationOriginV1 as Origin,
    ReplicationSourceHoldStateV1 as State, StoredReplicationAdministrationV1 as Record,
};
use riffdb_types::{ApprovalId, ReplicationFollowerAuditTargetV1 as Target, RequestId};
use wire::stored_replication_administration_v1 as fields;

const RECORD: &str = "riffdb.storage.v1.StoredReplicationAdministrationV1";

/// Encodes checked evidence at tag75/1; this does not authorize a transition.
pub fn encode_replication_administration_v1(
    value: &Record,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let target = value.target();
    let before = match value.before() {
        None => fields::Before::Absent(wire::UnitV1 {}),
        Some(State::Legacy(v)) => fields::Before::Legacy(source_hold_to_wire(v)),
        Some(State::Registered(v)) => fields::Before::Registered(source_hold_v2::to_wire(v)),
    };
    let origin = match value.origin() {
        Origin::Explicit {
            request_id,
            principal,
            approval_id,
        } => fields::Origin::Explicit(fields::Explicit {
            request_id: request_id.as_bytes().to_vec(),
            principal: Some(audit_principal_to_proto(principal)),
            approval_id: approval_id.as_ref().map(|v| v.as_str().to_owned()),
        }),
        Origin::ConfiguredExpiry { registration } => {
            fields::Origin::ConfiguredExpiry(fields::ConfiguredExpiry {
                registration_administration_sequence: registration.get(),
            })
        }
    };
    encode_message(
        RECORD,
        &wire::StoredReplicationAdministrationV1 {
            administration_sequence: value.administration_sequence().get(),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            action: match value.action() {
                Action::RegisterFollower => 1,
                Action::RetireFollower => 2,
                Action::ExpireFollower => 3,
            },
            target: Some(wire::ReplicationFollowerAuditTargetV3 {
                database_id: target.database_id().as_bytes().to_vec(),
                history_incarnation: target.history_incarnation(),
                leadership_epoch: target.leadership_epoch().get(),
                hold_id: target.hold_id().as_bytes().to_vec(),
            }),
            registration_generation: value.generation().get(),
            observed: Some(encode_position(value.observed())),
            before: Some(before),
            after: Some(source_hold_v2::to_wire(value.after())),
            origin: Some(origin),
        },
    )
}

/// Checks complete typed target, generation, origin and before/after transition.
/// Storage must still verify exact retained receipt and current authorization.
pub fn decode_replication_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<Record>, DurableCodecError> {
    decode_message::<wire::StoredReplicationAdministrationV1, _, _>(RECORD, encoded, |v| {
        let target = require(v.target)?;
        let target = Target::new(
            DatabaseId::from_bytes(fixed(target.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            target.history_incarnation,
            LeadershipEpochV1::new(target.leadership_epoch)
                .ok_or_else(DurableCodecError::corrupt)?,
            ReplicationSourceHoldIdV1::new(fixed(target.hold_id)?)
                .ok_or_else(DurableCodecError::corrupt)?,
        )
        .ok_or_else(DurableCodecError::corrupt)?;
        let before = match require(v.before)? {
            fields::Before::Absent(_) => None,
            fields::Before::Legacy(value) => Some(State::Legacy(source_hold_from_wire(value)?)),
            fields::Before::Registered(value) => {
                Some(State::Registered(source_hold_v2::from_wire(value)?))
            }
        };
        let origin = match require(v.origin)? {
            fields::Origin::Explicit(value) => Origin::Explicit {
                request_id: RequestId::from_bytes(fixed(value.request_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                principal: audit_principal_from_proto(require(value.principal)?)?,
                approval_id: value
                    .approval_id
                    .map(ApprovalId::new)
                    .transpose()
                    .map_err(|_| DurableCodecError::corrupt())?,
            },
            fields::Origin::ConfiguredExpiry(value) => Origin::ConfiguredExpiry {
                registration: AdministrationSequence::new(
                    value.registration_administration_sequence,
                )
                .ok_or_else(DurableCodecError::corrupt)?,
            },
        };
        let after = source_hold_v2::from_wire(require(v.after)?)?;
        if after.generation().get() != v.registration_generation {
            return Err(DurableCodecError::corrupt());
        }
        Record::new(
            AdministrationSequence::new(v.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            timestamp_from_proto(require(v.timestamp)?)?,
            match v.action {
                1 => Action::RegisterFollower,
                2 => Action::RetireFollower,
                3 => Action::ExpireFollower,
                _ => return Err(DurableCodecError::corrupt()),
            },
            target,
            before,
            after,
            decode_position(require(v.observed)?)?,
            origin,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}

/// Reads both accepted source-hold generations while preserving the checked charge.
/// Registered tombstones remain explicit; callers must not treat them as live pins.
pub fn decode_replication_source_hold(
    encoded: &[u8],
) -> Result<EncodedPageItem<State>, DurableCodecError> {
    let envelope = riffdb_proto::durable::readable_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let (value, charge) = match envelope.record_type() {
        SOURCE_HOLD => {
            let (v, c) = decode_replication_source_hold_v1(encoded)?.into_parts();
            (State::Legacy(v), c)
        }
        "riffdb.storage.v1.StoredReplicationSourceHoldV2" => {
            let (v, c) = source_hold_v2::decode_replication_source_hold_v2(encoded)?.into_parts();
            (State::Registered(v), c)
        }
        _ => {
            return Err(DurableCodecError::new(
                DurableCodecErrorKind::UnexpectedRecordType,
            ));
        }
    };
    Ok(EncodedPageItem::new(value, charge))
}
