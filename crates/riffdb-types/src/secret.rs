//! Secret-classified field values and their structural redaction (ADR-0118).
//!
//! A [`SecretValue`] owns a secret-classified field's canonical value between
//! authoritative materialization and display release. Every rendering form it
//! offers — `Display`, `Debug`, and [`SecretValue::redaction_marker`] — emits
//! only the stable redaction marker; the wrapped value can leave the wrapper
//! solely through [`SecretValue::reveal_for_authorized_display`], whose call
//! sites are enumerated by an architecture test. The type deliberately
//! implements neither `serde::Serialize` nor any byte-encoding trait, so a
//! diagnostic or wire serializer cannot consume it at all.
//!
//! The classification is display-and-visibility metadata only: durable
//! storage, backups, exports, and changelog frames carry secret fields at
//! full fidelity, and no cryptographic property is implied.

use std::fmt;
use std::sync::Arc;

use crate::ids::FieldId;
use crate::value::CanonicalValue;

/// Renders the one stable redaction marker for a secret-classified field.
///
/// The exact shape — `[redacted:field_name]` — is a public contract: every
/// display surface emits this form and the redaction sweep asserts its
/// presence byte-for-byte. Change it nowhere or everywhere.
#[must_use]
pub fn secret_redaction_marker(field_name: &str) -> String {
    format!("[redacted:{field_name}]")
}

/// One secret-classified value withheld from display (ADR-0118).
///
/// Constructed at the service's materialization boundary for every
/// secret-classified field of a projected record. The value inside is real —
/// predicates, uniqueness, and index participation already ran against it —
/// but no display surface can obtain it without explicit reveal authority.
#[derive(Clone)]
pub struct SecretValue {
    field: FieldId,
    field_name: Arc<str>,
    value: CanonicalValue,
}

impl SecretValue {
    /// Wraps one secret-classified field value.
    #[must_use]
    pub fn classify(
        field: FieldId,
        field_name: impl Into<Arc<str>>,
        value: CanonicalValue,
    ) -> Self {
        Self {
            field,
            field_name: field_name.into(),
            value,
        }
    }

    /// The classified field's stable ID.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }

    /// The classified field's source name (the classification itself is not
    /// a secret; diagnostics may name the field).
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// The stable redaction marker for this field.
    #[must_use]
    pub fn redaction_marker(&self) -> String {
        secret_redaction_marker(&self.field_name)
    }

    /// Releases the wrapped value for display under explicit field-visibility
    /// authority for this exact field.
    ///
    /// This is the ONLY path out of the wrapper. Call sites are enumerated by
    /// the display-surface architecture test; adding one is a reviewed event.
    /// Authority for a different field fails closed.
    pub fn reveal_for_authorized_display(
        self,
        authority: &SecretRevealAuthority,
    ) -> Result<CanonicalValue, Self> {
        if authority.field() == self.field {
            Ok(self.value)
        } else {
            Err(self)
        }
    }

    /// Discards the wrapped value, keeping only the redacted description.
    #[must_use]
    pub fn into_redacted(self) -> RedactedSecretField {
        RedactedSecretField {
            field: self.field,
            field_name: self.field_name,
        }
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", secret_redaction_marker(&self.field_name))
    }
}

impl fmt::Display for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", secret_redaction_marker(&self.field_name))
    }
}

/// Explicit per-field reveal authority derived from a capability's
/// secret-field visibility naming (ADR-0118 item 3).
///
/// Constructed only from an explicit field-visibility proof; the constructor
/// name is greppable and enumerated by the same architecture test as the
/// reveal method.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecretRevealAuthority {
    field: FieldId,
}

impl SecretRevealAuthority {
    /// Derives reveal authority for one explicitly named secret field.
    #[must_use]
    pub const fn from_explicit_field_visibility(field: FieldId) -> Self {
        Self { field }
    }

    /// The field this authority reveals.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }
}

/// One secret-classified field withheld from a released record.
///
/// Carries no value — only the field identity and the marker every display
/// surface renders in the value's place.
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
    use crate::value::CanonicalString;

    fn canary() -> CanonicalValue {
        CanonicalValue::String(
            CanonicalString::new("wp597-secret-canary-3f9a".to_owned()).expect("canary string"),
        )
    }

    /// The exact marker text is a public contract, tested once here and
    /// consumed everywhere.
    #[test]
    fn marker_has_the_exact_documented_shape() {
        assert_eq!(
            secret_redaction_marker("token_hash"),
            "[redacted:token_hash]"
        );
    }

    /// Every rendering form of the wrapper emits the marker and never the
    /// value.
    #[test]
    fn wrapper_renders_only_the_marker() {
        let secret = SecretValue::classify(FieldId::first(), "token_hash", canary());
        for rendered in [
            format!("{secret}"),
            format!("{secret:?}"),
            format!("{:#?}", secret),
            secret.redaction_marker(),
        ] {
            assert_eq!(rendered, "[redacted:token_hash]");
            assert!(!rendered.contains("canary"));
        }
        let redacted = secret.clone().into_redacted();
        assert_eq!(format!("{redacted}"), "[redacted:token_hash]");
        assert_eq!(format!("{redacted:?}"), "[redacted:token_hash]");
    }

    /// Reveal succeeds under authority for the exact field and fails closed
    /// for any other field, returning the still-wrapped value.
    #[test]
    fn reveal_requires_authority_for_the_exact_field() {
        let field = FieldId::first();
        let other = FieldId::new(field.get() + 1).expect("field id");
        let secret = SecretValue::classify(field, "token_hash", canary());
        let denied = secret
            .clone()
            .reveal_for_authorized_display(&SecretRevealAuthority::from_explicit_field_visibility(
                other,
            ))
            .expect_err("authority for another field must not reveal");
        assert_eq!(format!("{denied}"), "[redacted:token_hash]");
        let revealed = secret
            .reveal_for_authorized_display(&SecretRevealAuthority::from_explicit_field_visibility(
                field,
            ))
            .expect("exact-field authority reveals");
        assert_eq!(revealed, canary());
    }
}
