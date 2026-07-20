//! Coordinator-owned command provenance identifier source.

use std::error::Error;
use std::fmt;

use riffdb_types::ProvenanceId;

/// Synchronous source for fresh command provenance identifiers.
///
/// The coordinator calls this source once only after a new mutating attempt has
/// evaluated successfully and before opening its authoritative write
/// transaction. It is not called for read-only execution, execution-failure
/// terminalization, unevaluated pending work, or terminal replay. A source call
/// may precede a proven pre-commit abort; that attempt's invisible candidate is
/// discarded, and a later safe reevaluation is a new attempt that may call the
/// source again. After an unknown commit status, durable same-key resolution must
/// complete before another source call. Production providers belong to server
/// composition.
pub trait ProvenanceIdSource: Send + Sync {
    /// Returns one fresh checked UUIDv7 provenance identifier.
    fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError>;
}

/// A redaction-safe failure to obtain a fresh provenance identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProvenanceIdSourceError;

impl fmt::Display for ProvenanceIdSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provenance identifier source failed")
    }
}

impl Error for ProvenanceIdSourceError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedSource(ProvenanceId);

    impl ProvenanceIdSource for FixedSource {
        fn next_provenance_id(&self) -> Result<ProvenanceId, ProvenanceIdSourceError> {
            Ok(self.0)
        }
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    #[test]
    fn source_is_a_synchronous_object_safe_value_port() {
        let expected = ProvenanceId::from_bytes(uuid_bytes(0x31)).expect("valid UUIDv7");
        let source: &dyn ProvenanceIdSource = &FixedSource(expected);
        assert_eq!(source.next_provenance_id(), Ok(expected));
    }

    #[test]
    fn source_failure_exposes_only_static_safe_text() {
        assert_eq!(
            ProvenanceIdSourceError.to_string(),
            "provenance identifier source failed"
        );
        assert_eq!(
            format!("{ProvenanceIdSourceError:?}"),
            "ProvenanceIdSourceError"
        );
        assert!(ProvenanceIdSourceError.source().is_none());
    }
}
