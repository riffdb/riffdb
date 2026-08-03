use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    DatabaseId, EventConsumerIdentityHash, EventConsumerName, EventConsumerRevision,
    EventDeliveryAttempt, EventLeaseToken, PartitionKeyHash, QueryParameterHash,
    ReactiveModuleHash, ReactiveOperationName,
};

use crate::{
    ConsumerCheckpointV1, ConsumerDeadLetterReasonV1, ConsumerDeliveryStateV1, EncodedPageItem,
    EventConsumerIdentityV1, SparseConsumerResolutionV1, SparseResolutionKindV1,
    StoredEventConsumerDeliveryV1, StoredEventConsumerV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message,
    event_id_from_proto, event_id_to_proto, fixed, require, timestamp_from_proto,
    timestamp_to_proto,
};

const CONSUMER: &str = "riffdb.storage.v1.StoredEventConsumerV1";
const DELIVERY: &str = "riffdb.storage.v1.StoredEventConsumerDeliveryV1";

fn identity_to_proto(value: &EventConsumerIdentityV1) -> wire::EventConsumerIdentityV1 {
    wire::EventConsumerIdentityV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        reactive_module_hash: value.reactive_module_hash().as_bytes().to_vec(),
        operation_name: value.operation_name().as_str().to_owned(),
        parameter_hash: value.parameter_hash().as_bytes().to_vec(),
        consumer_name: value.consumer_name().as_str().to_owned(),
    }
}

fn identity_from_proto(
    value: wire::EventConsumerIdentityV1,
) -> Result<EventConsumerIdentityV1, DurableCodecError> {
    Ok(EventConsumerIdentityV1::new(
        DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        ReactiveModuleHash::from_bytes(fixed(value.reactive_module_hash)?),
        ReactiveOperationName::new(value.operation_name)
            .map_err(|_| DurableCodecError::corrupt())?,
        QueryParameterHash::from_bytes(fixed(value.parameter_hash)?),
        EventConsumerName::new(value.consumer_name).map_err(|_| DurableCodecError::corrupt())?,
    ))
}

/// Encodes one bounded durable consumer record.
pub fn encode_event_consumer_v1(
    value: &StoredEventConsumerV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        CONSUMER,
        &wire::StoredEventConsumerV1 {
            identity: Some(identity_to_proto(value.identity())),
            partition_hash: value.partition_hash().as_bytes().to_vec(),
            history_incarnation: value.history_incarnation(),
            revision: value.revision().get(),
            checkpoint: match value.checkpoint() {
                ConsumerCheckpointV1::BeforeFirst => None,
                ConsumerCheckpointV1::After(event_id) => Some(event_id_to_proto(event_id)),
            },
            sparse_resolutions: value
                .sparse_resolutions()
                .iter()
                .map(|resolution| wire::SparseConsumerResolutionV1 {
                    event_id: Some(event_id_to_proto(resolution.event_id())),
                    kind: match resolution.kind() {
                        SparseResolutionKindV1::Acknowledged => 1,
                        SparseResolutionKindV1::DeadLettered => 2,
                    },
                })
                .collect(),
        },
    )
}

/// Decodes and validates one bounded durable consumer record.
pub fn decode_event_consumer_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredEventConsumerV1>, DurableCodecError> {
    decode_message::<wire::StoredEventConsumerV1, _, _>(CONSUMER, encoded, |value| {
        let sparse = value
            .sparse_resolutions
            .into_iter()
            .map(|resolution| {
                Ok(SparseConsumerResolutionV1::new(
                    event_id_from_proto(require(resolution.event_id)?)?,
                    match resolution.kind {
                        1 => SparseResolutionKindV1::Acknowledged,
                        2 => SparseResolutionKindV1::DeadLettered,
                        _ => return Err(DurableCodecError::corrupt()),
                    },
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        StoredEventConsumerV1::checked(
            identity_from_proto(require(value.identity)?)?,
            PartitionKeyHash::from_bytes(fixed(value.partition_hash)?),
            value.history_incarnation,
            EventConsumerRevision::new(value.revision).ok_or_else(DurableCodecError::corrupt)?,
            value
                .checkpoint
                .map(event_id_from_proto)
                .transpose()?
                .map_or(
                    ConsumerCheckpointV1::BeforeFirst,
                    ConsumerCheckpointV1::After,
                ),
            sparse,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}

/// Encodes one payload-free durable delivery row.
pub fn encode_event_consumer_delivery_v1(
    value: &StoredEventConsumerDeliveryV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_event_consumer_delivery_v1::State;
    let state = match value.state() {
        ConsumerDeliveryStateV1::Leased {
            attempt,
            token,
            expires_at,
        } => State::Leased(wire::LeasedConsumerDeliveryV1 {
            attempt: u32::from(attempt.get()),
            token: token.as_bytes().to_vec(),
            expires_at: Some(timestamp_to_proto(expires_at)),
        }),
        ConsumerDeliveryStateV1::Retry {
            failed_attempts,
            eligible_at,
        } => State::Retry(wire::RetryConsumerDeliveryV1 {
            failed_attempts: u32::from(failed_attempts.get()),
            eligible_at: Some(timestamp_to_proto(eligible_at)),
        }),
        ConsumerDeliveryStateV1::DeadLettered {
            failed_attempts,
            dead_lettered_at,
            reason,
        } => State::DeadLettered(wire::DeadLetteredConsumerDeliveryV1 {
            failed_attempts: u32::from(failed_attempts.get()),
            dead_lettered_at: Some(timestamp_to_proto(dead_lettered_at)),
            reason: match reason {
                ConsumerDeadLetterReasonV1::LeaseExpired => 1,
                ConsumerDeadLetterReasonV1::NegativeAcknowledgement => 2,
            },
        }),
    };
    encode_message(
        DELIVERY,
        &wire::StoredEventConsumerDeliveryV1 {
            consumer_identity_hash: value.consumer_identity_hash().as_bytes().to_vec(),
            event_id: Some(event_id_to_proto(value.event_id())),
            history_incarnation: value.history_incarnation(),
            state: Some(state),
        },
    )
}

/// Decodes and validates one payload-free durable delivery row.
pub fn decode_event_consumer_delivery_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredEventConsumerDeliveryV1>, DurableCodecError> {
    use wire::stored_event_consumer_delivery_v1::State;
    decode_message::<wire::StoredEventConsumerDeliveryV1, _, _>(DELIVERY, encoded, |value| {
        let state = match require(value.state)? {
            State::Leased(leased) => ConsumerDeliveryStateV1::Leased {
                attempt: attempt(leased.attempt)?,
                token: EventLeaseToken::from_bytes(fixed(leased.token)?),
                expires_at: timestamp_from_proto(require(leased.expires_at)?)?,
            },
            State::Retry(retry) => ConsumerDeliveryStateV1::Retry {
                failed_attempts: attempt(retry.failed_attempts)?,
                eligible_at: timestamp_from_proto(require(retry.eligible_at)?)?,
            },
            State::DeadLettered(dead) => ConsumerDeliveryStateV1::DeadLettered {
                failed_attempts: attempt(dead.failed_attempts)?,
                dead_lettered_at: timestamp_from_proto(require(dead.dead_lettered_at)?)?,
                reason: match dead.reason {
                    1 => ConsumerDeadLetterReasonV1::LeaseExpired,
                    2 => ConsumerDeadLetterReasonV1::NegativeAcknowledgement,
                    _ => return Err(DurableCodecError::corrupt()),
                },
            },
        };
        StoredEventConsumerDeliveryV1::new(
            EventConsumerIdentityHash::from_bytes(fixed(value.consumer_identity_hash)?),
            event_id_from_proto(require(value.event_id)?)?,
            value.history_incarnation,
            state,
        )
        .map_err(|_| DurableCodecError::corrupt())
    })
}

fn attempt(value: u32) -> Result<EventDeliveryAttempt, DurableCodecError> {
    u8::try_from(value)
        .ok()
        .and_then(EventDeliveryAttempt::new)
        .ok_or_else(DurableCodecError::corrupt)
}
