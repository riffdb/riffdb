//! External connector contract and deterministic demonstration connector.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::io::Write;
use std::num::{NonZeroU32, NonZeroUsize};

use riffdb_storage_api::{OutboxDestinationIdV1, OutboxSafeErrorV1, StoredDurableEventV1};
use riffdb_types::{EventHash, EventId, encode_canonical_record};

use crate::ConnectorResultClass;

/// Maximum scripted outcomes or retained observations in the deterministic connector.
pub const MAX_DETERMINISTIC_CONNECTOR_ITEMS: usize = 4_096;

const FILE_TEST_FRAME_PREFIX: &[u8] = b"riffdb.outbox.file-test/v1\n";
const FILE_TEST_SAFE_ERROR: &str = "file-test connector write failed";
#[cfg(feature = "http-webhook")]
const DISABLED_HTTP_WEBHOOK_SAFE_ERROR: &str = "HTTP webhook connector is disabled";

/// Closed POC destination implementation kind.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorDestinationKind {
    /// In-memory deterministic connector.
    Deterministic,
    /// Injected writer suitable for a bounded file or stdout demonstration.
    FileTest,
    /// Optional HTTP webhook connector requiring explicit feature and composition.
    HttpWebhook,
}

/// Whether the destination consumes RiffDB's stable event identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConnectorIdempotency {
    /// The connector presents the exact 12-byte `EventId` to the destination.
    EventId,
    /// The destination has no idempotency-key support.
    Unsupported,
}

/// Static connector behavior needed for operational inspection.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectorDeclaration {
    destination_kind: ConnectorDestinationKind,
    idempotency: ConnectorIdempotency,
}

impl ConnectorDeclaration {
    /// Creates one closed declaration.
    #[must_use]
    pub const fn new(
        destination_kind: ConnectorDestinationKind,
        idempotency: ConnectorIdempotency,
    ) -> Self {
        Self {
            destination_kind,
            idempotency,
        }
    }

    /// Returns the destination implementation kind.
    #[must_use]
    pub const fn destination_kind(self) -> ConnectorDestinationKind {
        self.destination_kind
    }

    /// Returns the downstream idempotency behavior.
    #[must_use]
    pub const fn idempotency(self) -> ConnectorIdempotency {
        self.idempotency
    }
}

/// One claimed event presented to a connector outside the command transaction.
#[derive(Clone, Copy)]
pub struct DeliveryAttempt<'a> {
    event: &'a StoredDurableEventV1,
    attempt: NonZeroU32,
    destination_id: &'a OutboxDestinationIdV1,
    timeout_seconds: NonZeroU32,
}

impl<'a> DeliveryAttempt<'a> {
    /// Constructs one exact, already claimed connector request.
    #[must_use]
    pub const fn new(
        event: &'a StoredDurableEventV1,
        attempt: NonZeroU32,
        destination_id: &'a OutboxDestinationIdV1,
        timeout_seconds: NonZeroU32,
    ) -> Self {
        Self {
            event,
            attempt,
            destination_id,
            timeout_seconds,
        }
    }

    /// Returns the stable downstream deduplication identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event.event_id()
    }

    /// Borrows the complete immutable durable event.
    #[must_use]
    pub const fn event(self) -> &'a StoredDurableEventV1 {
        self.event
    }

    /// Returns the nonzero attempt number.
    #[must_use]
    pub const fn attempt(self) -> NonZeroU32 {
        self.attempt
    }

    /// Borrows the selected destination configuration reference.
    #[must_use]
    pub const fn destination_id(self) -> &'a OutboxDestinationIdV1 {
        self.destination_id
    }

    /// Returns the maximum connector call duration.
    #[must_use]
    pub const fn timeout_seconds(self) -> NonZeroU32 {
        self.timeout_seconds
    }

    /// Returns the exact 12-byte event idempotency key.
    #[must_use]
    pub const fn idempotency_key(self) -> [u8; 12] {
        self.event.event_id().to_be_bytes()
    }
}

impl fmt::Debug for DeliveryAttempt<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeliveryAttempt")
            .field("event_id", &self.event.event_id())
            .field("attempt", &self.attempt)
            .field("destination_id", &"[REDACTED]")
            .field("timeout_seconds", &self.timeout_seconds)
            .field("event", &"[REDACTED]")
            .finish()
    }
}

/// Closed connector classification after response scrubbing.
#[derive(Clone, Eq, PartialEq)]
pub enum ConnectorDisposition {
    /// The destination accepted the event.
    Accepted,
    /// Policy may retry, retaining only an optional bounded safe summary.
    Retryable {
        /// Already scrubbed safe summary suitable for durable retry metadata.
        safe_error: Option<OutboxSafeErrorV1>,
    },
    /// The response is terminal for this destination.
    PermanentFailure {
        /// Already scrubbed safe summary suitable for durable dead-letter metadata.
        safe_error: Option<OutboxSafeErrorV1>,
    },
}

impl ConnectorDisposition {
    /// Returns the redaction-safe result class.
    #[must_use]
    pub const fn class(&self) -> ConnectorResultClass {
        match self {
            Self::Accepted => ConnectorResultClass::Accepted,
            Self::Retryable { .. } => ConnectorResultClass::Retryable,
            Self::PermanentFailure { .. } => ConnectorResultClass::PermanentFailure,
        }
    }
}

impl fmt::Debug for ConnectorDisposition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Accepted => formatter.write_str("Accepted"),
            Self::Retryable { safe_error } => formatter
                .debug_struct("Retryable")
                .field("safe_error", &safe_error.as_ref().map(|_| "[REDACTED]"))
                .finish(),
            Self::PermanentFailure { safe_error } => formatter
                .debug_struct("PermanentFailure")
                .field("safe_error", &safe_error.as_ref().map(|_| "[REDACTED]"))
                .finish(),
        }
    }
}

/// Connector that performs external I/O only after a durable delivery claim.
pub trait OutboxConnector {
    /// Declares destination and downstream idempotency behavior.
    fn declaration(&self) -> ConnectorDeclaration;

    /// Attempts one delivery and returns an already classified, scrubbed result.
    ///
    /// A connector panic or process crash leaves the durable status
    /// `Delivering`; startup recovery later normalizes that attempt.
    fn deliver(&mut self, attempt: DeliveryAttempt<'_>) -> ConnectorDisposition;
}

/// Bounded deterministic-connector construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeterministicConnectorError {
    /// Script or observation capacity exceeds the fixed POC bound.
    LimitExceeded,
}

impl fmt::Display for DeterministicConnectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("deterministic connector exceeds its fixed capacity")
    }
}

impl Error for DeterministicConnectorError {}

/// Payload-free observation retained by the deterministic connector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveryObservation {
    event_id: EventId,
    event_hash: EventHash,
    attempt: NonZeroU32,
}

impl DeliveryObservation {
    /// Returns the stable delivered identity.
    #[must_use]
    pub const fn event_id(self) -> EventId {
        self.event_id
    }

    /// Returns the immutable authoritative event hash.
    #[must_use]
    pub const fn event_hash(self) -> EventHash {
        self.event_hash
    }

    /// Returns the connector attempt.
    #[must_use]
    pub const fn attempt(self) -> NonZeroU32 {
        self.attempt
    }
}

/// Scripted connector for demonstrations and deterministic crash tests.
pub struct DeterministicConnector {
    script: VecDeque<ConnectorDisposition>,
    fallback: ConnectorDisposition,
    observations: Vec<DeliveryObservation>,
    observation_capacity: NonZeroUsize,
}

impl DeterministicConnector {
    /// Creates a bounded scripted connector with an explicit fallback result.
    pub fn new(
        script: Vec<ConnectorDisposition>,
        fallback: ConnectorDisposition,
        observation_capacity: NonZeroUsize,
    ) -> Result<Self, DeterministicConnectorError> {
        if script.len() > MAX_DETERMINISTIC_CONNECTOR_ITEMS
            || observation_capacity.get() > MAX_DETERMINISTIC_CONNECTOR_ITEMS
        {
            return Err(DeterministicConnectorError::LimitExceeded);
        }
        Ok(Self {
            script: script.into(),
            fallback,
            observations: Vec::with_capacity(observation_capacity.get()),
            observation_capacity,
        })
    }

    /// Borrows bounded payload-free connector observations.
    #[must_use]
    pub fn observations(&self) -> &[DeliveryObservation] {
        &self.observations
    }
}

impl fmt::Debug for DeterministicConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeterministicConnector")
            .field("remaining_script_items", &self.script.len())
            .field("observation_count", &self.observations.len())
            .field("observation_capacity", &self.observation_capacity)
            .finish()
    }
}

impl OutboxConnector for DeterministicConnector {
    fn declaration(&self) -> ConnectorDeclaration {
        ConnectorDeclaration::new(
            ConnectorDestinationKind::Deterministic,
            ConnectorIdempotency::EventId,
        )
    }

    fn deliver(&mut self, attempt: DeliveryAttempt<'_>) -> ConnectorDisposition {
        if self.observations.len() >= self.observation_capacity.get() {
            return ConnectorDisposition::Retryable {
                safe_error: OutboxSafeErrorV1::new(
                    "deterministic connector observation capacity exhausted",
                )
                .ok(),
            };
        }
        self.observations.push(DeliveryObservation {
            event_id: attempt.event_id(),
            event_hash: attempt.event().event_hash(),
            attempt: attempt.attempt(),
        });
        self.script
            .pop_front()
            .unwrap_or_else(|| self.fallback.clone())
    }
}

/// Bounded binary-frame connector over an injected file or stdout writer.
///
/// Opening a filesystem path or selecting stdout is a composition concern.
/// This connector receives only an already-open writer and runs after the
/// durable claim. A partial or failed write is classified retryable because the
/// destination may already have observed bytes.
pub struct FileTestConnector<W> {
    writer: W,
}

impl<W> FileTestConnector<W> {
    /// Wraps one explicitly supplied writer.
    #[must_use]
    pub const fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Returns the writer during controlled shutdown.
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> fmt::Debug for FileTestConnector<W> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("FileTestConnector([REDACTED])")
    }
}

impl<W> OutboxConnector for FileTestConnector<W>
where
    W: Write,
{
    fn declaration(&self) -> ConnectorDeclaration {
        ConnectorDeclaration::new(
            ConnectorDestinationKind::FileTest,
            ConnectorIdempotency::EventId,
        )
    }

    fn deliver(&mut self, attempt: DeliveryAttempt<'_>) -> ConnectorDisposition {
        let payload = match encode_canonical_record(attempt.event().payload()) {
            Ok(payload) => payload,
            Err(_) => return permanent_file_test_failure(),
        };
        let payload_length = match u32::try_from(payload.len()) {
            Ok(length) => length,
            Err(_) => return permanent_file_test_failure(),
        };
        let result = (|| {
            self.writer.write_all(FILE_TEST_FRAME_PREFIX)?;
            self.writer.write_all(&attempt.event_id().to_be_bytes())?;
            self.writer
                .write_all(&attempt.event().event_type_id().to_be_bytes())?;
            self.writer
                .write_all(attempt.event().event_hash().as_bytes())?;
            self.writer.write_all(&payload_length.to_be_bytes())?;
            self.writer.write_all(&payload)?;
            self.writer.flush()
        })();
        if result.is_ok() {
            ConnectorDisposition::Accepted
        } else {
            retryable_file_test_failure()
        }
    }
}

fn retryable_file_test_failure() -> ConnectorDisposition {
    ConnectorDisposition::Retryable {
        safe_error: OutboxSafeErrorV1::new(FILE_TEST_SAFE_ERROR).ok(),
    }
}

fn permanent_file_test_failure() -> ConnectorDisposition {
    ConnectorDisposition::PermanentFailure {
        safe_error: OutboxSafeErrorV1::new(FILE_TEST_SAFE_ERROR).ok(),
    }
}

/// Inert webhook connector used until an HTTP implementation is separately reviewed.
///
/// Enabling `http-webhook` adds no network client and grants no ambient I/O.
/// Composition may use this connector to expose the disabled destination
/// explicitly; every attempted dispatch fails permanently without inspecting
/// or transmitting the event, destination, or timeout.
#[cfg(feature = "http-webhook")]
#[derive(Clone, Copy, Default)]
pub struct DisabledHttpWebhookConnector;

#[cfg(feature = "http-webhook")]
impl DisabledHttpWebhookConnector {
    /// Constructs the explicitly disabled, no-I/O connector.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(feature = "http-webhook")]
impl fmt::Debug for DisabledHttpWebhookConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DisabledHttpWebhookConnector")
    }
}

#[cfg(feature = "http-webhook")]
impl OutboxConnector for DisabledHttpWebhookConnector {
    fn declaration(&self) -> ConnectorDeclaration {
        ConnectorDeclaration::new(
            ConnectorDestinationKind::HttpWebhook,
            ConnectorIdempotency::Unsupported,
        )
    }

    fn deliver(&mut self, _attempt: DeliveryAttempt<'_>) -> ConnectorDisposition {
        ConnectorDisposition::PermanentFailure {
            safe_error: OutboxSafeErrorV1::new(DISABLED_HTTP_WEBHOOK_SAFE_ERROR).ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::derive_event_hash_v1;
    use riffdb_types::{CanonicalRecord, CommitSequence, EventTypeId};

    use super::*;

    fn event() -> StoredDurableEventV1 {
        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type = EventTypeId::try_from(1).expect("event type");
        let payload = CanonicalRecord::new(Vec::new()).expect("payload");
        let hash = derive_event_hash_v1(event_id, event_type, &payload).expect("hash");
        StoredDurableEventV1::new(event_id, event_type, payload, hash).expect("event")
    }

    #[test]
    fn deterministic_connector_retains_only_payload_free_evidence() {
        let event = event();
        let destination = OutboxDestinationIdV1::new("test").expect("destination");
        let mut connector = DeterministicConnector::new(
            Vec::new(),
            ConnectorDisposition::Accepted,
            NonZeroUsize::new(1).expect("capacity"),
        )
        .expect("connector");
        let attempt = DeliveryAttempt::new(
            &event,
            NonZeroU32::new(1).expect("attempt"),
            &destination,
            NonZeroU32::new(1).expect("timeout"),
        );

        assert_eq!(connector.deliver(attempt), ConnectorDisposition::Accepted);
        assert_eq!(connector.observations()[0].event_id(), event.event_id());
        assert_eq!(connector.observations()[0].event_hash(), event.event_hash());
        assert_eq!(connector.observations()[0].attempt().get(), 1);
        assert_eq!(
            connector.declaration().idempotency(),
            ConnectorIdempotency::EventId
        );
    }

    #[test]
    fn connector_debug_redacts_safe_error_text() {
        let disposition = ConnectorDisposition::Retryable {
            safe_error: Some(OutboxSafeErrorV1::new("secret-canary").expect("safe error")),
        };

        let debug = format!("{disposition:?}");
        assert!(!debug.contains("secret-canary"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn file_test_connector_writes_stable_id_and_payload_frame() {
        let event = event();
        let destination = OutboxDestinationIdV1::new("file/test").expect("destination");
        let mut connector = FileTestConnector::new(Vec::new());
        let disposition = connector.deliver(DeliveryAttempt::new(
            &event,
            NonZeroU32::new(1).expect("attempt"),
            &destination,
            NonZeroU32::new(1).expect("timeout"),
        ));

        assert_eq!(disposition, ConnectorDisposition::Accepted);
        let bytes = connector.into_inner();
        assert!(bytes.starts_with(FILE_TEST_FRAME_PREFIX));
        assert_eq!(
            &bytes[FILE_TEST_FRAME_PREFIX.len()..FILE_TEST_FRAME_PREFIX.len() + 12],
            &event.event_id().to_be_bytes()
        );
    }

    #[cfg(feature = "http-webhook")]
    #[test]
    fn disabled_http_webhook_is_an_explicit_no_io_failure() {
        const SECRET_CANARY: &str = "webhook-payload-secret-canary";

        let event_id = EventId::new(CommitSequence::first(), 0);
        let event_type = EventTypeId::try_from(1).expect("event type");
        let payload = CanonicalRecord::new(vec![(
            riffdb_types::FieldId::try_from(1).expect("field"),
            riffdb_types::CanonicalValue::string(SECRET_CANARY).expect("bounded value"),
        )])
        .expect("payload");
        let hash = derive_event_hash_v1(event_id, event_type, &payload).expect("hash");
        let event = StoredDurableEventV1::new(event_id, event_type, payload, hash).expect("event");
        let destination =
            OutboxDestinationIdV1::new("https://secret.example.invalid/hook?token=secret")
                .expect("bounded destination");
        let attempt = DeliveryAttempt::new(
            &event,
            NonZeroU32::new(1).expect("attempt"),
            &destination,
            NonZeroU32::new(1).expect("timeout"),
        );
        let mut connector = DisabledHttpWebhookConnector::new();

        assert_eq!(
            connector.declaration(),
            ConnectorDeclaration::new(
                ConnectorDestinationKind::HttpWebhook,
                ConnectorIdempotency::Unsupported,
            )
        );
        let disposition = connector.deliver(attempt);
        assert!(matches!(
            disposition,
            ConnectorDisposition::PermanentFailure { .. }
        ));

        let exported = format!("{connector:?}{attempt:?}{disposition:?}");
        assert!(!exported.contains(SECRET_CANARY));
        assert!(!exported.contains("secret.example.invalid"));
        assert!(!exported.contains("token=secret"));

        let manifest = include_str!("../Cargo.toml");
        for forbidden in ["reqwest", "hyper", "ureq", "curl", "rustls", "native-tls"] {
            assert!(
                !manifest.contains(forbidden),
                "disabled webhook feature must not add network dependency {forbidden}"
            );
        }
    }
}
