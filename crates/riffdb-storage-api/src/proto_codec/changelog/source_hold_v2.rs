//! Successor source-local policy codec. Checked bytes carry no release authority.
use super::*;
use crate::{FollowerHoldBudget, FollowerRegistrationPhaseV1 as Phase, ReplicationSourceHoldV2};

const RECORD: &str = "riffdb.storage.v1.StoredReplicationSourceHoldV2";

/// Encodes a checked registration policy without altering frozen V1 hold bytes.
pub fn encode_replication_source_hold_v2(
    value: ReplicationSourceHoldV2,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        RECORD,
        &wire::StoredReplicationSourceHoldV2 {
            hold: Some(source_hold_to_wire(value.hold())),
            registered_at: Some(encode_position(value.registered_at())),
            hold_budget_sequences: value.budget().sequences(),
            expires_at_application_sequence: value.expires_at().map(CommitSequence::get),
            phase: match value.phase() {
                Phase::AwaitingBootstrap => 1,
                Phase::Attached => 2,
                Phase::Retired => 3,
            },
            degraded_at: value.degraded_at().map(encode_position),
        },
    )
}

/// Checks role, phase, budget, expiry and degradation independently of checksum.
/// Storage must additionally bind the key, retained ancestry and audited writer.
pub fn decode_replication_source_hold_v2(
    encoded: &[u8],
) -> Result<EncodedPageItem<ReplicationSourceHoldV2>, DurableCodecError> {
    decode_message::<wire::StoredReplicationSourceHoldV2, _, _>(RECORD, encoded, |v| {
        let phase = match v.phase {
            1 => Phase::AwaitingBootstrap,
            2 => Phase::Attached,
            3 => Phase::Retired,
            _ => return Err(DurableCodecError::corrupt()),
        };
        let expiry = v
            .expires_at_application_sequence
            .map(|raw| CommitSequence::new(raw).ok_or_else(DurableCodecError::corrupt))
            .transpose()?;
        ReplicationSourceHoldV2::new(
            source_hold_from_wire(require(v.hold)?)?,
            decode_position(require(v.registered_at)?)?,
            FollowerHoldBudget::new(v.hold_budget_sequences)
                .ok_or_else(DurableCodecError::corrupt)?,
            expiry,
            phase,
            v.degraded_at.map(decode_position).transpose()?,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}
