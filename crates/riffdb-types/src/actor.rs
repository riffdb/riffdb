//! Foundational actor and bounded provenance-component values.

use std::error::Error;
use std::fmt;

use crate::limits::{
    MAX_APPROVAL_ID_BYTES, MAX_APPROVAL_REFERENCE_BYTES, MAX_AUDIENCE_BYTES,
    MAX_PROVENANCE_REASON_BYTES, MAX_SOURCE_COMMIT_BYTES, MAX_SOURCE_REPOSITORY_BYTES,
};
use crate::{ActorId, AgentSessionId, TenantScope};

/// A safe validation failure for a bounded text component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundedTextError {
    /// The supplied value is empty.
    Empty,
    /// The UTF-8 representation exceeds the component's byte limit.
    TooLong {
        /// The applicable maximum byte length.
        maximum: usize,
        /// The supplied byte length.
        actual: usize,
    },
    /// The component contains a byte outside visible ASCII.
    InvalidCharacter {
        /// The zero-based byte index of the invalid character.
        index: usize,
    },
}

impl fmt::Display for BoundedTextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("value must not be empty"),
            Self::TooLong { maximum, actual } => write!(
                formatter,
                "value is {actual} bytes but the maximum is {maximum} bytes"
            ),
            Self::InvalidCharacter { index } => {
                write!(formatter, "value has an invalid character at byte {index}")
            }
        }
    }
}

impl Error for BoundedTextError {}

fn validate_bounded_utf8(value: &str, maximum: usize) -> Result<(), BoundedTextError> {
    if value.is_empty() {
        return Err(BoundedTextError::Empty);
    }
    if value.len() > maximum {
        return Err(BoundedTextError::TooLong {
            maximum,
            actual: value.len(),
        });
    }
    Ok(())
}

fn validate_bounded_visible_ascii(value: &str, maximum: usize) -> Result<(), BoundedTextError> {
    validate_bounded_utf8(value, maximum)?;
    if let Some(index) = value
        .bytes()
        .position(|byte| !(0x21..=0x7e).contains(&byte))
    {
        return Err(BoundedTextError::InvalidCharacter { index });
    }
    Ok(())
}

macro_rules! bounded_text_type {
    ($(#[$meta:meta])* $name:ident, $maximum:ident, $validator:ident) => {
        $(#[$meta])*
        #[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Validates and constructs the component without normalizing it.
            pub fn new(value: impl Into<String>) -> Result<Self, BoundedTextError> {
                let value = value.into();
                $validator(&value, $maximum)?;
                Ok(Self(value))
            }

            /// Borrows the exact validated text.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Borrows the exact validated bytes.
            #[must_use]
            pub fn as_bytes(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "([REDACTED])"))
            }
        }
    };
}

bounded_text_type!(
    /// An exact configured authentication audience.
    Audience,
    MAX_AUDIENCE_BYTES,
    validate_bounded_visible_ascii
);

bounded_text_type!(
    /// An untrusted, bounded source-repository claim.
    SourceRepository,
    MAX_SOURCE_REPOSITORY_BYTES,
    validate_bounded_utf8
);

bounded_text_type!(
    /// An untrusted, bounded source-commit claim.
    SourceCommit,
    MAX_SOURCE_COMMIT_BYTES,
    validate_bounded_visible_ascii
);

bounded_text_type!(
    /// An untrusted, bounded reason claim for provenance review.
    ProvenanceReason,
    MAX_PROVENANCE_REASON_BYTES,
    validate_bounded_utf8
);

bounded_text_type!(
    /// An untrusted, bounded approval reference.
    ApprovalReference,
    MAX_APPROVAL_REFERENCE_BYTES,
    validate_bounded_visible_ascii
);

bounded_text_type!(
    /// A policy-validated approval identity.
    ApprovalId,
    MAX_APPROVAL_ID_BYTES,
    validate_bounded_visible_ascii
);

/// The trusted class of a stable actor principal.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ActorKind {
    /// A human principal.
    Human,
    /// An autonomous or assisted agent principal.
    Agent,
    /// A service principal.
    Service,
}

impl ActorKind {
    /// Every accepted v1 actor kind, in tag order.
    pub const ALL: [Self; 3] = [Self::Human, Self::Agent, Self::Service];

    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Human => 0x01,
            Self::Agent => 0x02,
            Self::Service => 0x03,
        }
    }

    /// Decodes a stable v1 semantic tag, rejecting zero and unknown values.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Human),
            0x02 => Some(Self::Agent),
            0x03 => Some(Self::Service),
            _ => None,
        }
    }
}

/// The immutable actor context admitted for deterministic command execution.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AdmittedActorContext {
    principal_id: ActorId,
    actor_kind: ActorKind,
    tenant_scope: TenantScope,
    agent_session_id: Option<AgentSessionId>,
}

impl AdmittedActorContext {
    /// Constructs an admitted context from already checked and authorized values.
    #[must_use]
    pub const fn new(
        principal_id: ActorId,
        actor_kind: ActorKind,
        tenant_scope: TenantScope,
        agent_session_id: Option<AgentSessionId>,
    ) -> Self {
        Self {
            principal_id,
            actor_kind,
            tenant_scope,
            agent_session_id,
        }
    }

    /// Borrows the stable admitted principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted admitted actor kind.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Borrows the authorization-resolved tenant scope.
    #[must_use]
    pub const fn tenant_scope(&self) -> &TenantScope {
        &self.tenant_scope
    }

    /// Returns the optional policy-admitted agent-session identity.
    #[must_use]
    pub const fn agent_session_id(&self) -> Option<AgentSessionId> {
        self.agent_session_id
    }
}

impl fmt::Debug for AdmittedActorContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AdmittedActorContext([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TenantId;

    fn test_agent_session_id() -> AgentSessionId {
        let mut bytes = [0x22; 16];
        bytes[6] = 0x72;
        bytes[8] = 0xa2;
        AgentSessionId::from_bytes(bytes).expect("test UUID is version 7")
    }

    #[test]
    fn actor_kind_tags_are_closed_and_stable() {
        assert_eq!(ActorKind::ALL.map(ActorKind::tag), [0x01, 0x02, 0x03]);
        for actor_kind in ActorKind::ALL {
            assert_eq!(ActorKind::from_tag(actor_kind.tag()), Some(actor_kind));
        }
        assert_eq!(ActorKind::from_tag(0), None);
        assert_eq!(ActorKind::from_tag(4), None);
        assert_eq!(ActorKind::from_tag(u8::MAX), None);
    }

    #[test]
    fn visible_ascii_components_enforce_exact_alphabet_and_bounds() {
        let audience = Audience::new(format!("!{}~", "a".repeat(510)))
            .expect("visible ASCII at the limit is valid");
        assert_eq!(audience.as_bytes().len(), MAX_AUDIENCE_BYTES);
        assert_eq!(Audience::new(""), Err(BoundedTextError::Empty));
        assert_eq!(
            Audience::new("a".repeat(MAX_AUDIENCE_BYTES + 1)),
            Err(BoundedTextError::TooLong {
                maximum: MAX_AUDIENCE_BYTES,
                actual: MAX_AUDIENCE_BYTES + 1,
            })
        );
        assert_eq!(
            Audience::new("valid except space"),
            Err(BoundedTextError::InvalidCharacter { index: 5 })
        );
        assert_eq!(
            SourceCommit::new("abc\n123"),
            Err(BoundedTextError::InvalidCharacter { index: 3 })
        );
        assert_eq!(
            ApprovalReference::new("\u{7f}"),
            Err(BoundedTextError::InvalidCharacter { index: 0 })
        );
        assert_eq!(
            ApprovalId::new("caf\u{e9}"),
            Err(BoundedTextError::InvalidCharacter { index: 3 })
        );
    }

    #[test]
    fn utf8_components_use_encoded_byte_lengths_without_normalization() {
        let exact = "\u{e9}".repeat(MAX_SOURCE_REPOSITORY_BYTES / 2);
        let repository =
            SourceRepository::new(exact.clone()).expect("exact UTF-8 byte limit is valid");
        assert_eq!(repository.as_str(), exact);
        assert_eq!(repository.as_bytes().len(), MAX_SOURCE_REPOSITORY_BYTES);

        let over = format!("{exact}x");
        assert_eq!(
            SourceRepository::new(over),
            Err(BoundedTextError::TooLong {
                maximum: MAX_SOURCE_REPOSITORY_BYTES,
                actual: MAX_SOURCE_REPOSITORY_BYTES + 1,
            })
        );

        let reason = ProvenanceReason::new("Case and \u{212b} stay exact")
            .expect("bounded UTF-8 reason is valid");
        assert_eq!(reason.as_str(), "Case and \u{212b} stay exact");
    }

    #[test]
    fn every_text_value_rejects_empty_input() {
        assert_eq!(Audience::new(""), Err(BoundedTextError::Empty));
        assert_eq!(SourceRepository::new(""), Err(BoundedTextError::Empty));
        assert_eq!(SourceCommit::new(""), Err(BoundedTextError::Empty));
        assert_eq!(ProvenanceReason::new(""), Err(BoundedTextError::Empty));
        assert_eq!(ApprovalReference::new(""), Err(BoundedTextError::Empty));
        assert_eq!(ApprovalId::new(""), Err(BoundedTextError::Empty));
    }

    #[test]
    fn bounded_values_and_admitted_context_redact_debug_output() {
        let audience = Audience::new("https://database.invalid").expect("valid audience");
        let repository = SourceRepository::new("secret/repository").expect("valid repository");
        let commit = SourceCommit::new("0123456789abcdef").expect("valid commit");
        let reason = ProvenanceReason::new("sensitive reason").expect("valid reason");
        let reference = ApprovalReference::new("approval-reference").expect("valid reference");
        let approval_id = ApprovalId::new("approval-id").expect("valid approval ID");

        for (debug, secret) in [
            (format!("{audience:?}"), "database.invalid"),
            (format!("{repository:?}"), "secret/repository"),
            (format!("{commit:?}"), "0123456789abcdef"),
            (format!("{reason:?}"), "sensitive reason"),
            (format!("{reference:?}"), "approval-reference"),
            (format!("{approval_id:?}"), "approval-id"),
        ] {
            assert!(debug.contains("[REDACTED]"));
            assert!(!debug.contains(secret));
        }

        let principal_id = ActorId::new("principal-secret").expect("valid principal");
        let tenant_id = TenantId::new("tenant-secret").expect("valid tenant");
        let context = AdmittedActorContext::new(
            principal_id,
            ActorKind::Agent,
            TenantScope::Tenant(tenant_id),
            Some(test_agent_session_id()),
        );
        let debug = format!("{context:?}");
        assert_eq!(debug, "AdmittedActorContext([REDACTED])");
        assert!(!debug.contains("principal-secret"));
        assert!(!debug.contains("tenant-secret"));
    }

    #[test]
    fn admitted_actor_context_preserves_exact_authorized_components() {
        let principal_id = ActorId::new("actor-1").expect("valid actor");
        let tenant_id = TenantId::new("tenant-1").expect("valid tenant");
        let session_id = test_agent_session_id();
        let context = AdmittedActorContext::new(
            principal_id.clone(),
            ActorKind::Agent,
            TenantScope::Tenant(tenant_id.clone()),
            Some(session_id),
        );

        assert_eq!(context.principal_id(), &principal_id);
        assert_eq!(context.actor_kind(), ActorKind::Agent);
        assert_eq!(context.tenant_scope(), &TenantScope::Tenant(tenant_id));
        assert_eq!(context.agent_session_id(), Some(session_id));
    }
}
