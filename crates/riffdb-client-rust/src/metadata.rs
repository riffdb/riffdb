//! Narrow outbound metadata owned by the public client.

use std::error::Error;
use std::fmt;
use std::str::FromStr;

use tonic::Request;
use tonic::metadata::{Ascii, Binary, MetadataValue};
use zeroize::Zeroizing;

const CAPABILITY_TOKEN_BYTES: usize = 43;
const MAX_TRACE_PARENT_BYTES: usize = 512;
const BOOTSTRAP_TOKEN_METADATA_KEY: &str = "riffdb-bootstrap-token-bin";

/// A safe local metadata-construction failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataError {
    /// The capability token is not valid bounded v1 presentation text.
    InvalidCapabilityToken,
    /// Trace propagation metadata is empty, oversized, or not valid ASCII metadata.
    InvalidTraceParent,
}

impl fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidCapabilityToken => "capability credential is invalid",
            Self::InvalidTraceParent => "trace propagation metadata is invalid",
        })
    }
}

impl Error for MetadataError {}

/// One bounded POC capability bearer credential.
///
/// Formatting is always redacted. This transport wrapper validates only the
/// exact length and alphabet needed for safe metadata presentation. The
/// auth-owned server decoder remains the sole authority for canonical token
/// decoding and authentication.
#[derive(Clone)]
pub struct BearerCredential {
    authorization: MetadataValue<Ascii>,
}

impl BearerCredential {
    /// Constructs a bearer credential from bounded v1 presentation text.
    pub fn new(token: &str) -> Result<Self, MetadataError> {
        if !valid_token_presentation(token.as_bytes()) {
            return Err(MetadataError::InvalidCapabilityToken);
        }
        let mut presentation = Zeroizing::new(String::with_capacity("Bearer ".len() + token.len()));
        presentation.push_str("Bearer ");
        presentation.push_str(token);
        let authorization = MetadataValue::from_str(presentation.as_str())
            .map_err(|_| MetadataError::InvalidCapabilityToken)?;
        Ok(Self { authorization })
    }

    /// Reports whether two checked credentials have the same presentation.
    ///
    /// This is a non-exposing equality check for delivery verification, not a
    /// constant-time authentication primitive.
    #[must_use]
    pub fn has_same_presentation(&self, other: &Self) -> bool {
        self.authorization == other.authorization
    }
}

/// One bounded credential for the loopback-only bootstrap call.
///
/// This type cannot be placed in ordinary [`CallMetadata`]. Canonical token
/// decoding remains owned by the server's auth boundary.
pub struct BootstrapCredential {
    value: MetadataValue<Binary>,
}

impl BootstrapCredential {
    /// Constructs the dedicated binary metadata value from bounded token text.
    pub fn new(token: &str) -> Result<Self, MetadataError> {
        if !valid_token_presentation(token.as_bytes()) {
            return Err(MetadataError::InvalidCapabilityToken);
        }
        Ok(Self {
            value: MetadataValue::from_bytes(token.as_bytes()),
        })
    }
}

impl fmt::Debug for BootstrapCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapCredential([REDACTED])")
    }
}

impl fmt::Debug for BearerCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BearerCredential([REDACTED])")
    }
}

/// One bounded outbound `traceparent` metadata value.
#[derive(Clone)]
pub struct TraceParent {
    value: MetadataValue<Ascii>,
}

impl TraceParent {
    /// Validates one trace propagation value as bounded ASCII gRPC metadata.
    pub fn new(value: &str) -> Result<Self, MetadataError> {
        if value.is_empty() || value.len() > MAX_TRACE_PARENT_BYTES {
            return Err(MetadataError::InvalidTraceParent);
        }
        let value =
            MetadataValue::from_str(value).map_err(|_| MetadataError::InvalidTraceParent)?;
        Ok(Self { value })
    }
}

impl fmt::Debug for TraceParent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TraceParent([REDACTED])")
    }
}

/// Checked metadata applied consistently to one SDK call.
#[derive(Clone, Debug, Default)]
pub struct CallMetadata {
    credential: Option<BearerCredential>,
    trace_parent: Option<TraceParent>,
}

impl CallMetadata {
    /// Constructs metadata for an authenticated call.
    #[must_use]
    pub fn authenticated(credential: BearerCredential) -> Self {
        Self {
            credential: Some(credential),
            trace_parent: None,
        }
    }

    /// Adds bounded trace propagation to this call.
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: TraceParent) -> Self {
        self.trace_parent = Some(trace_parent);
        self
    }

    pub(crate) fn apply<T>(&self, request: &mut Request<T>) {
        if let Some(credential) = &self.credential {
            request
                .metadata_mut()
                .insert("authorization", credential.authorization.clone());
        }
        if let Some(trace_parent) = &self.trace_parent {
            request
                .metadata_mut()
                .insert("traceparent", trace_parent.value.clone());
        }
    }
}

/// Checked metadata available only to loopback bootstrap capability creation.
pub struct BootstrapCallMetadata {
    credential: BootstrapCredential,
    trace_parent: Option<TraceParent>,
}

impl BootstrapCallMetadata {
    /// Constructs metadata carrying exactly one bootstrap credential.
    #[must_use]
    pub const fn new(credential: BootstrapCredential) -> Self {
        Self {
            credential,
            trace_parent: None,
        }
    }

    /// Adds bounded trace propagation to the bootstrap call.
    #[must_use]
    pub fn with_trace_parent(mut self, trace_parent: TraceParent) -> Self {
        self.trace_parent = Some(trace_parent);
        self
    }

    pub(crate) fn apply<T>(&self, request: &mut Request<T>) {
        request
            .metadata_mut()
            .insert_bin(BOOTSTRAP_TOKEN_METADATA_KEY, self.credential.value.clone());
        if let Some(trace_parent) = &self.trace_parent {
            request
                .metadata_mut()
                .insert("traceparent", trace_parent.value.clone());
        }
    }
}

impl fmt::Debug for BootstrapCallMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BootstrapCallMetadata([REDACTED])")
    }
}

fn valid_token_presentation(token: &[u8]) -> bool {
    token.len() == CAPABILITY_TOKEN_BYTES
        && token
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    #[test]
    fn credential_has_bounded_presentation_and_is_always_redacted() {
        let credential = BearerCredential::new(TOKEN).expect("bounded token text");
        assert_eq!(format!("{credential:?}"), "BearerCredential([REDACTED])");
        assert!(!format!("{credential:?}").contains(TOKEN));
        assert!(matches!(
            BearerCredential::new("not-a-token"),
            Err(MetadataError::InvalidCapabilityToken)
        ));
    }

    #[test]
    fn call_metadata_uses_only_reviewed_headers() {
        let metadata = CallMetadata::authenticated(BearerCredential::new(TOKEN).expect("token"))
            .with_trace_parent(TraceParent::new("00-abc-def-01").expect("ASCII metadata"));
        let mut request = Request::new(());
        metadata.apply(&mut request);
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .expect("authorization")
                .to_str()
                .expect("ASCII"),
            format!("Bearer {TOKEN}")
        );
        assert_eq!(
            request.metadata().get("traceparent").expect("traceparent"),
            "00-abc-def-01"
        );
        assert_eq!(request.metadata().len(), 2);
    }

    #[test]
    fn bootstrap_metadata_is_binary_and_disjoint_from_authorization() {
        let metadata = BootstrapCallMetadata::new(
            BootstrapCredential::new(TOKEN).expect("bounded token text"),
        );
        let mut request = Request::new(());
        metadata.apply(&mut request);

        assert!(request.metadata().get("authorization").is_none());
        assert_eq!(
            request
                .metadata()
                .get_bin(BOOTSTRAP_TOKEN_METADATA_KEY)
                .expect("bootstrap credential")
                .to_bytes()
                .expect("valid binary metadata")
                .as_ref(),
            TOKEN.as_bytes()
        );
        assert_eq!(request.metadata().len(), 1);
        assert!(!format!("{metadata:?}").contains(TOKEN));
    }
}
