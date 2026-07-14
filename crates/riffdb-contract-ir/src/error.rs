//! Safe validation and codec failures for contract IR.

use std::error::Error;
use std::fmt;

/// A safe reason that an IR value or canonical bundle was rejected.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IrValidationError {
    /// A version is not executable by this implementation.
    UnsupportedVersion {
        /// Versioned boundary being decoded.
        kind: &'static str,
        /// Unsupported numeric version.
        value: u32,
    },
    /// A collection or byte string exceeded its immutable v1 limit.
    LimitExceeded {
        /// Bounded value or collection.
        kind: &'static str,
        /// Observed size.
        actual: usize,
        /// Immutable maximum.
        maximum: usize,
    },
    /// A required collection was empty.
    Empty {
        /// Required value or collection.
        kind: &'static str,
    },
    /// A name is invalid for its semantic position.
    InvalidName {
        /// Semantic position of the invalid name.
        kind: &'static str,
    },
    /// Numeric IDs or canonical entries were duplicated or out of order.
    NonCanonicalOrder {
        /// Canonically ordered collection.
        kind: &'static str,
    },
    /// A referenced stable or plan-local ID does not exist.
    InvalidReference {
        /// Referenced semantic object.
        kind: &'static str,
    },
    /// A value does not match its declared static type.
    TypeMismatch {
        /// Checked typing context.
        context: &'static str,
    },
    /// An expression arena is not topologically ordered.
    NonForwardExpression,
    /// A command instruction stream violates v1 phase or terminal rules.
    InvalidInstructionStream {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// A command dependency or locality derivation is not statically visible.
    InvalidDependency {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// A key schema or key value violates the closed v1 codec.
    InvalidKey {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// A projection schema, measure, or bound is invalid.
    InvalidProjection {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// A command tool-name entry violates ADR-0020.
    InvalidMcpName {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// Two command IDs normalize to one public command tool name.
    McpNameCollision,
    /// Stable lineage history is incomplete or internally inconsistent.
    InvalidLineageLedger {
        /// Stable safe rejection reason.
        reason: &'static str,
    },
    /// A compatibility report is not canonical or self-consistent.
    InvalidCompatibilityReport,
    /// Encoded bytes ended before the declared value was complete.
    UnexpectedEnd,
    /// Encoded bytes use an unknown closed-registry tag.
    UnknownTag {
        /// Closed registry being decoded.
        kind: &'static str,
        /// Unknown encoded tag.
        tag: u8,
    },
    /// An encoded string is not canonical UTF-8 or ASCII where required.
    InvalidText {
        /// Text boundary being decoded.
        kind: &'static str,
    },
    /// A stored typed hash differs from the canonical recomputation.
    HashMismatch {
        /// Typed hash boundary.
        kind: &'static str,
    },
    /// Bytes remain after one complete canonical value.
    TrailingBytes,
    /// Checked size arithmetic overflowed.
    SizeOverflow {
        /// Checked size computation.
        kind: &'static str,
    },
}

impl fmt::Display for IrValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { kind, value } => {
                write!(formatter, "unsupported {kind} version {value}")
            }
            Self::LimitExceeded {
                kind,
                actual,
                maximum,
            } => write!(formatter, "{kind} has size {actual}; maximum is {maximum}"),
            Self::Empty { kind } => write!(formatter, "{kind} must not be empty"),
            Self::InvalidName { kind } => write!(formatter, "invalid {kind} name"),
            Self::NonCanonicalOrder { kind } => {
                write!(formatter, "{kind} is duplicated or not canonically ordered")
            }
            Self::InvalidReference { kind } => write!(formatter, "invalid {kind} reference"),
            Self::TypeMismatch { context } => write!(formatter, "type mismatch in {context}"),
            Self::NonForwardExpression => {
                formatter.write_str("expression arena is not topologically ordered")
            }
            Self::InvalidInstructionStream { reason }
            | Self::InvalidDependency { reason }
            | Self::InvalidKey { reason }
            | Self::InvalidProjection { reason }
            | Self::InvalidMcpName { reason }
            | Self::InvalidLineageLedger { reason } => formatter.write_str(reason),
            Self::McpNameCollision => formatter.write_str("normalized MCP command names collide"),
            Self::InvalidCompatibilityReport => formatter.write_str("invalid compatibility report"),
            Self::UnexpectedEnd => formatter.write_str("canonical IR ended unexpectedly"),
            Self::UnknownTag { kind, tag } => write!(formatter, "unknown {kind} tag {tag}"),
            Self::InvalidText { kind } => write!(formatter, "invalid canonical {kind} text"),
            Self::HashMismatch { kind } => write!(formatter, "stored {kind} hash mismatches"),
            Self::TrailingBytes => formatter.write_str("canonical IR has trailing bytes"),
            Self::SizeOverflow { kind } => write!(formatter, "{kind} size overflow"),
        }
    }
}

impl Error for IrValidationError {}

pub(crate) fn checked_len(
    kind: &'static str,
    actual: usize,
    maximum: usize,
) -> Result<(), IrValidationError> {
    if actual > maximum {
        Err(IrValidationError::LimitExceeded {
            kind,
            actual,
            maximum,
        })
    } else {
        Ok(())
    }
}

pub(crate) fn validate_source_name(
    value: &str,
    kind: &'static str,
) -> Result<(), IrValidationError> {
    checked_len(kind, value.len(), 256)?;
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(IrValidationError::InvalidName { kind });
    };
    if value == "tx"
        || !(first.is_ascii_alphabetic() || first == b'_')
        || bytes.any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
    {
        return Err(IrValidationError::InvalidName { kind });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_transaction_identifier_is_rejected() {
        assert_eq!(
            validate_source_name("tx", "test"),
            Err(IrValidationError::InvalidName { kind: "test" })
        );
    }
}
