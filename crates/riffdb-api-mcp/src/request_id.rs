use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_types::RequestId;

/// API-MCP-owned checked UUIDv7 request identity.
///
/// The wrapper keeps the foundational identifier owner out of transport
/// adapter manifests while preserving exact public-wire bytes.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct McpRequestId(RequestId);

impl McpRequestId {
    /// Validates one exact 16-byte public UUIDv7 representation.
    pub fn from_public_bytes(bytes: &[u8]) -> Result<Self, RequestIdSourceError> {
        let bytes: [u8; 16] = bytes.try_into().map_err(|_| RequestIdSourceError)?;
        RequestId::from_bytes(bytes)
            .map(Self)
            .map_err(|_| RequestIdSourceError)
    }

    /// Returns the exact public-wire UUID bytes.
    #[must_use]
    pub const fn into_public_bytes(self) -> [u8; 16] {
        self.0.into_bytes()
    }

    #[cfg(feature = "streamable-http")]
    pub(crate) const fn into_riffdb(self) -> RequestId {
        self.0
    }
}

impl fmt::Debug for McpRequestId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpRequestId([REDACTED])")
    }
}

/// Consumer-owned source for one fresh hosted-MCP request identifier.
pub trait RequestIdSource: Send + Sync {
    /// Returns one fresh checked UUIDv7 request identifier.
    fn next_request_id(&self) -> Result<McpRequestId, RequestIdSourceError>;
}

impl<T> RequestIdSource for Arc<T>
where
    T: RequestIdSource + ?Sized,
{
    fn next_request_id(&self) -> Result<McpRequestId, RequestIdSourceError> {
        (**self).next_request_id()
    }
}

/// A bounded failure to obtain a hosted-MCP request identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdSourceError;

impl fmt::Display for RequestIdSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("request identifier source failed")
    }
}

impl Error for RequestIdSourceError {}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedSource;

    impl RequestIdSource for FixedSource {
        fn next_request_id(&self) -> Result<McpRequestId, RequestIdSourceError> {
            let mut bytes = [0x21; 16];
            bytes[6] = 0x71;
            bytes[8] = 0x81;
            McpRequestId::from_public_bytes(&bytes)
        }
    }

    #[test]
    fn arc_preserves_checked_request_identity() {
        let source: Arc<dyn RequestIdSource> = Arc::new(FixedSource);
        assert_eq!(
            source
                .next_request_id()
                .expect("fixture source succeeds")
                .into_public_bytes(),
            [
                0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x71, 0x21, 0x81, 0x21, 0x21, 0x21, 0x21, 0x21,
                0x21, 0x21,
            ]
        );
        assert_eq!(
            McpRequestId::from_public_bytes(&[0_u8; 15]),
            Err(RequestIdSourceError)
        );
    }
}
