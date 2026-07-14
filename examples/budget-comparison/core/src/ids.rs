//! Strict comparison-workload identifiers.

use std::error::Error;
use std::fmt;

const UUID_TEXT_BYTES: usize = 36;

macro_rules! uuid_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Creates the identifier from exact RFC 4122 network-order bytes.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Parses canonical lowercase hyphenated UUID text.
            pub fn parse(text: &str) -> Result<Self, IdentifierError> {
                parse_uuid(text).map(Self)
            }

            /// Returns the exact network-order bytes.
            pub const fn as_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                format_uuid(self.0, formatter)
            }
        }
    };
}

uuid_id!(
    OrganizationId,
    "An organization UUID in the comparison workload."
);
uuid_id!(MatterId, "A matter UUID in an allocation request.");

/// A bounded stable operation label.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(String);

impl OperationId {
    /// Creates an ASCII operation label in `1..=64` bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 64
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(IdentifierError::InvalidOperationId);
        }
        Ok(Self(value))
    }

    /// Returns the exact label.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bounded command idempotency-key input.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkloadIdempotencyKey(String);

impl WorkloadIdempotencyKey {
    /// Creates an exact UTF-8 key in `1..=128` bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 {
            return Err(IdentifierError::InvalidIdempotencyKey);
        }
        Ok(Self(value))
    }

    /// Returns the exact key text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A safe workload identifier validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifierError {
    /// UUID text was not canonical lowercase hyphenated hexadecimal.
    InvalidUuid,
    /// An operation label violated its ASCII grammar or bound.
    InvalidOperationId,
    /// An idempotency key was empty or exceeded 128 bytes.
    InvalidIdempotencyKey,
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUuid => {
                formatter.write_str("identifier is not canonical lowercase UUID text")
            }
            Self::InvalidOperationId => {
                formatter.write_str("operation ID violates its bounded ASCII grammar")
            }
            Self::InvalidIdempotencyKey => {
                formatter.write_str("idempotency key must contain 1..=128 UTF-8 bytes")
            }
        }
    }
}

impl Error for IdentifierError {}

fn parse_uuid(text: &str) -> Result<[u8; 16], IdentifierError> {
    let bytes = text.as_bytes();
    if bytes.len() != UUID_TEXT_BYTES
        || bytes[8] != b'-'
        || bytes[13] != b'-'
        || bytes[18] != b'-'
        || bytes[23] != b'-'
    {
        return Err(IdentifierError::InvalidUuid);
    }

    let mut output = [0_u8; 16];
    let mut nibble_index = 0_usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return Err(IdentifierError::InvalidUuid),
        };
        let output_index = nibble_index / 2;
        if nibble_index.is_multiple_of(2) {
            output[output_index] = nibble << 4;
        } else {
            output[output_index] |= nibble;
        }
        nibble_index += 1;
    }
    Ok(output)
}

fn format_uuid(bytes: [u8; 16], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            formatter.write_str("-")?;
        }
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_text_round_trips_and_rejects_alternates() {
        let text = "018f22a1-7b3c-7def-8123-456789abcdef";
        let id = OrganizationId::parse(text).expect("canonical UUID");
        assert_eq!(id.to_string(), text);
        assert!(OrganizationId::parse(&text.to_uppercase()).is_err());
        assert!(OrganizationId::parse("018f22a17b3c7def8123456789abcdef").is_err());
    }
}
