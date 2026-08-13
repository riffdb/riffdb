//! Secret-classified field values and their sealed reveal authority
//! (ADR-0118).
//!
//! A [`SecretValue`] owns a secret-classified field's canonical value between
//! authoritative materialization and display release. Every rendering form it
//! offers — `Display`, `Debug`, and [`SecretValue::redaction_marker`] — emits
//! only the stable redaction marker, and the wrapped value can leave the
//! wrapper solely through [`SecretValue::reveal_for_authorized_display`].
//!
//! Reveal authority is UNFORGEABLE outside this crate:
//! [`SecretRevealAuthority`] has private fields and no constructor; the only
//! mint is [`FieldMask::secret_reveal_authorities`](crate::FieldMask::secret_reveal_authorities),
//! and a `FieldMask` itself can be constructed only inside this crate, by the
//! authorizer's evaluation of a real capability grant. Holding an authority
//! therefore IS the proof that a policy decision named the field for reveal:
//!
//! ```compile_fail,E0423
//! // The authority cannot be forged by construction…
//! let forged = riffdb_policy::SecretRevealAuthority {
//!     field: riffdb_types::FieldId::first(),
//! };
//! ```
//!
//! ```compile_fail,E0599
//! // …and the pre-seal bare constructor no longer exists.
//! let forged = riffdb_policy::SecretRevealAuthority::from_explicit_field_visibility(
//!     riffdb_types::FieldId::first(),
//! );
//! ```
//!
//! Neither this crate nor `riffdb-types` carries a serde dependency, so by
//! the orphan rule no `Serialize` impl for the wrapper can exist anywhere in
//! or below the workspace, and a diagnostic serializer cannot consume it:
//!
//! ```compile_fail,E0277
//! fn diagnostic_serializer<T: serde::Serialize>(_: &T) {}
//! diagnostic_serializer(&riffdb_policy::SecretValue::classify(
//!     riffdb_types::FieldId::first(),
//!     "token_hash",
//!     riffdb_types::CanonicalValue::Null,
//! ));
//! ```

use std::fmt;
use std::sync::Arc;

use riffdb_types::{CanonicalValue, FieldId, RedactedSecretField, secret_redaction_marker};

/// One secret-classified value withheld from display (ADR-0118).
///
/// Constructed at the service's materialization boundary for every
/// secret-classified field of a projected record. The value inside is real —
/// predicates, uniqueness, and index participation already ran against it —
/// but no display surface can obtain it without a sealed reveal authority.
#[derive(Clone)]
pub struct SecretValue {
    field: FieldId,
    field_name: Arc<str>,
    value: CanonicalValue,
}

impl SecretValue {
    /// Wraps one secret-classified field value. Wrapping is always safe.
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

    /// Releases the wrapped value for display under sealed reveal authority
    /// for this exact field.
    ///
    /// This is the ONLY path out of the wrapper, and the authority passed in
    /// can only have come from [`crate::FieldMask::secret_reveal_authorities`]
    /// — a policy evaluation that proved the grant's explicit secret naming.
    /// Call sites are enumerated by the display-surface architecture test;
    /// adding one is a reviewed event. Authority for a different field fails
    /// closed, returning the still-wrapped value.
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
        RedactedSecretField::new(self.field, self.field_name)
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

/// Sealed per-field reveal authority (ADR-0118 item 3).
///
/// No public constructor exists: the only mint is
/// [`crate::FieldMask::secret_reveal_authorities`], and `FieldMask`
/// construction is confined to this crate's authorizer evaluation. Holding
/// one proves a policy decision resolved the grant's dedicated secret-field
/// naming for this exact field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SecretRevealAuthority {
    field: FieldId,
}

impl SecretRevealAuthority {
    /// Mints inside the policy crate only — reached exclusively from
    /// [`crate::FieldMask::secret_reveal_authorities`].
    pub(crate) const fn sealed(field: FieldId) -> Self {
        Self { field }
    }

    /// Test-fixture mint, unreachable from production builds: the
    /// `test-fixtures` feature exists only as a dev-dependency of test
    /// harnesses (the same pattern the row-policy fixtures use).
    #[cfg(feature = "test-fixtures")]
    #[must_use]
    pub const fn test_fixture(field: FieldId) -> Self {
        Self { field }
    }

    /// The field this authority reveals.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::CanonicalString;

    fn canary() -> CanonicalValue {
        CanonicalValue::String(
            CanonicalString::new("wp597-secret-canary-3f9a".to_owned()).expect("canary string"),
        )
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

    /// Reveal succeeds under authority for the exact field — returning the
    /// EXACT wrapped bytes, not merely a value of the right shape — and
    /// fails closed for any other field, returning the still-wrapped value.
    #[test]
    fn reveal_requires_authority_for_the_exact_field() {
        let field = FieldId::first();
        let other = FieldId::new(field.get() + 1).expect("field id");
        let secret = SecretValue::classify(field, "token_hash", canary());
        let denied = secret
            .clone()
            .reveal_for_authorized_display(&SecretRevealAuthority::sealed(other))
            .expect_err("authority for another field must not reveal");
        assert_eq!(format!("{denied}"), "[redacted:token_hash]");
        let revealed = secret
            .reveal_for_authorized_display(&SecretRevealAuthority::sealed(field))
            .expect("exact-field authority reveals");
        assert_eq!(
            revealed,
            canary(),
            "the released value must be the exact wrapped value"
        );
    }
}
