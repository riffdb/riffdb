use std::num::NonZeroU32;

use riffdb_proto::storage::v1 as wire;

use crate::{
    EncodedPageItem, OutboxDeliveryStateV1, OutboxDestinationIdV1, OutboxRetryMetadataV1,
    OutboxSafeErrorV1, StoredOutboxIntentV1, StoredOutboxStatusV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, decode_message, encode_message, event_from_proto,
    event_id_from_proto, event_id_to_proto, event_reference_from_proto, event_reference_to_proto,
    require, timestamp_from_proto, timestamp_to_proto,
};

const INTENT: &str = "riffdb.storage.v1.StoredOutboxIntentV2";
const LEGACY_INTENT: &str = "riffdb.storage.v1.StoredOutboxIntentV1";
const STATUS: &str = "riffdb.storage.v1.StoredOutboxStatusV1";

/// Encodes one payload-free outbox reference to the authoritative event.
pub fn encode_outbox_intent_v1(
    value: &StoredOutboxIntentV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        INTENT,
        &wire::StoredOutboxIntentV2 {
            event_reference: Some(event_reference_to_proto(value.event_reference())),
        },
    )
}

#[cfg(test)]
pub(super) fn encode_outbox_intent_legacy_v1(
    value: &StoredOutboxIntentV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    super::encode_legacy_message(
        LEGACY_INTENT,
        &wire::StoredOutboxIntentV1 {
            event: Some(super::event_to_proto(value.event())),
        },
    )
}

/// Decodes one historical embedded-event outbox intent.
pub fn decode_outbox_intent_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredOutboxIntentV1>, DurableCodecError> {
    decode_message::<wire::StoredOutboxIntentV1, _, _>(LEGACY_INTENT, encoded, |value| {
        Ok(StoredOutboxIntentV1::new(event_from_proto(require(
            value.event,
        )?)?))
    })
}

/// Decodes a current payload-free outbox intent and proves the separately
/// loaded authoritative event row.
pub fn decode_outbox_intent_v2(
    encoded: &[u8],
    event: crate::StoredDurableEventV1,
) -> Result<EncodedPageItem<StoredOutboxIntentV1>, DurableCodecError> {
    decode_message::<wire::StoredOutboxIntentV2, _, _>(INTENT, encoded, |value| {
        let reference = event_reference_from_proto(require(value.event_reference)?)?;
        if !reference.matches(&event) {
            return Err(DurableCodecError::corrupt());
        }
        Ok(StoredOutboxIntentV1::new(event))
    })
}

/// Decodes either historical embedded-event intent or a current reference,
/// proving the supplied authoritative event in both cases.
pub fn decode_outbox_intent_with_event(
    encoded: &[u8],
    event: crate::StoredDurableEventV1,
) -> Result<EncodedPageItem<StoredOutboxIntentV1>, DurableCodecError> {
    match decode_outbox_intent_v2(encoded, event.clone()) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType => {
            let decoded = decode_outbox_intent_v1(encoded)?;
            if decoded.value().event() != &event {
                return Err(DurableCodecError::corrupt());
            }
            Ok(decoded)
        }
        Err(error) => Err(error),
    }
}

/// Decodes only the exact reference needed to load an outbox event.
pub fn decode_outbox_event_reference(
    encoded: &[u8],
) -> Result<EncodedPageItem<crate::EventReferenceV2>, DurableCodecError> {
    match decode_message::<wire::StoredOutboxIntentV2, _, _>(INTENT, encoded, |value| {
        event_reference_from_proto(require(value.event_reference)?)
    }) {
        Ok(value) => Ok(value),
        Err(error) if error.kind() == super::DurableCodecErrorKind::UnexpectedRecordType => {
            let decoded = decode_outbox_intent_v1(encoded)?;
            let (intent, charge) = decoded.into_parts();
            Ok(EncodedPageItem::new(intent.event_reference(), charge))
        }
        Err(error) => Err(error),
    }
}

fn destination(value: String) -> Result<OutboxDestinationIdV1, DurableCodecError> {
    OutboxDestinationIdV1::new(value).map_err(|_| DurableCodecError::corrupt())
}

fn safe_error(value: Option<String>) -> Result<Option<OutboxSafeErrorV1>, DurableCodecError> {
    value
        .map(OutboxSafeErrorV1::new)
        .transpose()
        .map_err(|_| DurableCodecError::corrupt())
}

/// Encodes one present derived outbox status row.
pub fn encode_outbox_status_v1(
    value: &StoredOutboxStatusV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    use wire::stored_outbox_status_v1::State;
    let state = match value.state() {
        OutboxDeliveryStateV1::Pending(value) => State::Pending(wire::OutboxRetryMetadataV1 {
            attempts: value.attempts().get(),
            last_attempt_at: Some(timestamp_to_proto(value.last_attempt_at())),
            next_attempt_at: value.next_attempt_at().map(timestamp_to_proto),
            destination_id: value.destination_id().as_str().to_owned(),
            last_safe_error: value
                .last_safe_error()
                .map(|value| value.as_str().to_owned()),
        }),
        OutboxDeliveryStateV1::Delivering {
            attempt,
            destination_id,
            started_at,
            lease_deadline,
        } => State::Delivering(wire::OutboxDeliveringV1 {
            attempt: attempt.get(),
            destination_id: destination_id.as_str().to_owned(),
            started_at: Some(timestamp_to_proto(*started_at)),
            lease_deadline: Some(timestamp_to_proto(*lease_deadline)),
        }),
        OutboxDeliveryStateV1::Delivered {
            attempts,
            destination_id,
            delivered_at,
        } => State::Delivered(wire::OutboxDeliveredV1 {
            attempts: attempts.get(),
            destination_id: destination_id.as_str().to_owned(),
            delivered_at: Some(timestamp_to_proto(*delivered_at)),
        }),
        OutboxDeliveryStateV1::DeadLetter {
            attempts,
            destination_id,
            failed_at,
            last_safe_error,
        } => State::DeadLetter(wire::OutboxDeadLetterV1 {
            attempts: *attempts,
            destination_id: destination_id.as_str().to_owned(),
            failed_at: Some(timestamp_to_proto(*failed_at)),
            last_safe_error: last_safe_error
                .as_ref()
                .map(|value| value.as_str().to_owned()),
        }),
    };
    encode_message(
        STATUS,
        &wire::StoredOutboxStatusV1 {
            event_id: Some(event_id_to_proto(value.event_id())),
            state: Some(state),
        },
    )
}

/// Decodes one present derived outbox status row.
pub fn decode_outbox_status_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredOutboxStatusV1>, DurableCodecError> {
    use wire::stored_outbox_status_v1::State;
    decode_message::<wire::StoredOutboxStatusV1, _, _>(STATUS, encoded, |value| {
        let event_id = event_id_from_proto(require(value.event_id)?)?;
        Ok(match require(value.state)? {
            State::Pending(value) => StoredOutboxStatusV1::pending(
                event_id,
                OutboxRetryMetadataV1::new(
                    NonZeroU32::new(value.attempts).ok_or_else(DurableCodecError::corrupt)?,
                    timestamp_from_proto(require(value.last_attempt_at)?)?,
                    value
                        .next_attempt_at
                        .map(timestamp_from_proto)
                        .transpose()?,
                    destination(value.destination_id)?,
                    safe_error(value.last_safe_error)?,
                ),
            ),
            State::Delivering(value) => StoredOutboxStatusV1::delivering(
                event_id,
                NonZeroU32::new(value.attempt).ok_or_else(DurableCodecError::corrupt)?,
                destination(value.destination_id)?,
                timestamp_from_proto(require(value.started_at)?)?,
                timestamp_from_proto(require(value.lease_deadline)?)?,
            ),
            State::Delivered(value) => StoredOutboxStatusV1::delivered(
                event_id,
                NonZeroU32::new(value.attempts).ok_or_else(DurableCodecError::corrupt)?,
                destination(value.destination_id)?,
                timestamp_from_proto(require(value.delivered_at)?)?,
            ),
            State::DeadLetter(value) => StoredOutboxStatusV1::dead_letter(
                event_id,
                value.attempts,
                destination(value.destination_id)?,
                timestamp_from_proto(require(value.failed_at)?)?,
                safe_error(value.last_safe_error)?,
            ),
        })
    })
}
