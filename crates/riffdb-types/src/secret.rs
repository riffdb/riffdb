//! The stable redaction marker for secret-classified fields (ADR-0118).
//!
//! The value-bearing wrapper (`SecretValue`) and the reveal authority live in
//! `riffdb-policy`, because reveal authority is mintable only from a policy
//! evaluation's sealed [`FieldMask`](https://docs.rs) obligation. This module
//! carries only the value-FREE vocabulary shared by display surfaces: the
//! marker text and the withheld-field descriptor.
//!
//! The classification is display-and-visibility metadata only: durable
//! storage, backups, exports, and changelog frames carry secret fields at
//! full fidelity, and no cryptographic property is implied.

use std::fmt;
use std::sync::Arc;

use crate::ids::FieldId;

/// Renders the one stable redaction marker for a secret-classified field.
///
/// The exact shape — `[redacted:field_name]` — is a public contract: every
/// display surface emits this form and the redaction sweep asserts its
/// presence byte-for-byte. Change it nowhere or everywhere.
#[must_use]
pub fn secret_redaction_marker(field_name: &str) -> String {
    format!("[redacted:{field_name}]")
}

/// One secret-classified field withheld from a released record.
///
/// Carries no value — only the field identity and the marker every display
/// surface renders in the value's place. Constructing one is harmless by
/// design (there is nothing inside to protect), which is why it may live
/// here while the value-bearing wrapper is sealed inside `riffdb-policy`.
#[derive(Clone, Eq, PartialEq)]
pub struct RedactedSecretField {
    field: FieldId,
    field_name: Arc<str>,
}

impl RedactedSecretField {
    /// Describes one withheld secret field.
    #[must_use]
    pub fn new(field: FieldId, field_name: impl Into<Arc<str>>) -> Self {
        Self {
            field,
            field_name: field_name.into(),
        }
    }

    /// The withheld field's stable ID.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }

    /// The withheld field's source name.
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// The stable redaction marker for this field.
    #[must_use]
    pub fn redaction_marker(&self) -> String {
        secret_redaction_marker(&self.field_name)
    }
}

impl fmt::Debug for RedactedSecretField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", secret_redaction_marker(&self.field_name))
    }
}

impl fmt::Display for RedactedSecretField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", secret_redaction_marker(&self.field_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact marker text is a public contract, tested once here and
    /// consumed everywhere.
    #[test]
    fn marker_has_the_exact_documented_shape() {
        assert_eq!(
            secret_redaction_marker("token_hash"),
            "[redacted:token_hash]"
        );
    }

    /// The withheld-field descriptor renders only the marker in every form.
    #[test]
    fn descriptor_renders_only_the_marker() {
        let redacted = RedactedSecretField::new(FieldId::first(), "token_hash");
        assert_eq!(format!("{redacted}"), "[redacted:token_hash]");
        assert_eq!(format!("{redacted:?}"), "[redacted:token_hash]");
        assert_eq!(redacted.redaction_marker(), "[redacted:token_hash]");
    }
}
