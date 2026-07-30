//! Versioned, domain-separated SHA-256 and HMAC-SHA-256 digests.

use std::fmt;

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};

use crate::{
    ApplicationLockHash, ApplicationManifestHash, ApplicationRoleDefinitionHash,
    ApplicationRoleHash, ApplicationSourceHash, CanonicalInputHash, CanonicalValueHash,
    CapabilityTokenDigest, ConflictKeyHash, ContractBundleHash, ContractPlanRootHash, DigestKey,
    DigestKeyId, EntityKeyHash, EventHash, GeneratedArtifactHash, OfflineMaintenanceInputHash,
    PartitionKeyHash, PlanHash, ProjectionApplyHash, ProjectionPlanHash, QueryModuleHash,
    QueryParameterHash, QueryPlanHash, QuerySourceHash, SchemaHash, SourceHash,
};

/// Hash framing and algorithm scheme defined by ADR-0011.
pub const DIGEST_SCHEME_V1: u8 = 0x01;

const HASH_PREFIX: &[u8] = b"RIFFDB-HASH\0";
const HMAC_PREFIX: &[u8] = b"RIFFDB-HMAC\0";

/// An unkeyed SHA-256 domain from the accepted central registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum HashDomain {
    /// Canonical value content.
    CanonicalValue,
    /// Canonical contract source.
    Source,
    /// Canonical contract bundle.
    ContractBundle,
    /// Executable command plan.
    Plan,
    /// Validated projection plan.
    ProjectionPlan,
    /// Closed RiffQL query access program.
    QueryPlan,
    /// Canonical immutable query module.
    QueryModule,
    /// Canonical application manifest.
    ApplicationManifest,
    /// Author-owned symbolic application source manifest.
    ApplicationSource,
    /// Compiler-owned exact application lock.
    ApplicationLock,
    /// Compiler-generated application artifact.
    GeneratedArtifact,
    /// Exact tenant-unbound application role definition.
    ApplicationRoleDefinition,
    /// Canonical compiled application role.
    ApplicationRole,
    /// Exact RiffQL source document.
    QuerySource,
    /// Canonical name-addressed RiffQL parameter set.
    QueryParameters,
    /// Ordered semantic plan set for one contract bundle.
    ContractPlanRoot,
    /// Canonical command input.
    CommandInput,
    /// Durable event content.
    Event,
    /// Canonical entity key.
    EntityKey,
    /// Canonical conflict key.
    ConflictKey,
    /// Canonical logical partition key.
    PartitionKey,
    /// Generated schema content.
    Schema,
    /// One complete checked projection apply request.
    ProjectionApply,
    /// One checked offline-maintenance semantic input.
    OfflineMaintenanceInput,
    /// One bounded command-batch source, item, or checkpoint document.
    CommandBatch,
}

impl HashDomain {
    /// Every registered unkeyed domain, for compatibility and collision checks.
    pub const ALL: [Self; 25] = [
        Self::CanonicalValue,
        Self::Source,
        Self::ContractBundle,
        Self::Plan,
        Self::ProjectionPlan,
        Self::QueryPlan,
        Self::QueryModule,
        Self::ApplicationManifest,
        Self::ApplicationSource,
        Self::ApplicationLock,
        Self::GeneratedArtifact,
        Self::ApplicationRoleDefinition,
        Self::ApplicationRole,
        Self::QuerySource,
        Self::QueryParameters,
        Self::ContractPlanRoot,
        Self::CommandInput,
        Self::Event,
        Self::EntityKey,
        Self::ConflictKey,
        Self::PartitionKey,
        Self::Schema,
        Self::ProjectionApply,
        Self::OfflineMaintenanceInput,
        Self::CommandBatch,
    ];

    /// Returns the immutable ASCII v1 domain label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::CanonicalValue => "riffdb.canonical-value/v1",
            Self::Source => "riffdb.source/v1",
            Self::ContractBundle => "riffdb.contract-bundle/v1",
            Self::Plan => "riffdb.plan/v1",
            Self::ProjectionPlan => "riffdb.projection-plan/v1",
            Self::QueryPlan => "riffdb.query-plan/v1",
            Self::QueryModule => "riffdb.query-module/v1",
            Self::ApplicationManifest => "riffdb.application-manifest/v1",
            Self::ApplicationSource => "riffdb.application-source/v1",
            Self::ApplicationLock => "riffdb.application-lock/v1",
            Self::GeneratedArtifact => "riffdb.generated-artifact/v1",
            Self::ApplicationRoleDefinition => "riffdb.application-role-definition/v1",
            Self::ApplicationRole => "riffdb.application-role/v1",
            Self::QuerySource => "riffdb.query-source/v1",
            Self::QueryParameters => "riffdb.query-parameters/v1",
            Self::ContractPlanRoot => "riffdb.contract-plan-root/v1",
            Self::CommandInput => "riffdb.command-input/v1",
            Self::Event => "riffdb.event/v1",
            Self::EntityKey => "riffdb.entity-key/v1",
            Self::ConflictKey => "riffdb.conflict-key/v1",
            Self::PartitionKey => "riffdb.partition-key/v1",
            Self::Schema => "riffdb.schema/v1",
            Self::ProjectionApply => "riffdb.projection-apply/v1",
            Self::OfflineMaintenanceInput => "riffdb.offline-maintenance-input/v1",
            Self::CommandBatch => "riffdb.command-batch/v1",
        }
    }
}

/// A keyed HMAC-SHA-256 domain from the accepted central registry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum KeyedHashDomain {
    /// Caller-provided idempotency key lookup.
    IdempotencyKey,
    /// Decoded opaque capability-token lookup.
    CapabilityToken,
}

impl KeyedHashDomain {
    /// Every registered keyed domain, for compatibility and collision checks.
    pub const ALL: [Self; 2] = [Self::IdempotencyKey, Self::CapabilityToken];

    /// Returns the immutable ASCII v1 domain label.
    pub const fn label(self) -> &'static str {
        match self {
            Self::IdempotencyKey => "riffdb.idempotency-key/v1",
            Self::CapabilityToken => "riffdb.capability-token/v1",
        }
    }
}

/// A versioned unkeyed content digest.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContentDigest {
    scheme: u8,
    domain: HashDomain,
    bytes: [u8; 32],
}

impl ContentDigest {
    const fn new(domain: HashDomain, bytes: [u8; 32]) -> Self {
        Self {
            scheme: DIGEST_SCHEME_V1,
            domain,
            bytes,
        }
    }

    /// Returns the hash scheme version.
    pub const fn scheme(self) -> u8 {
        self.scheme
    }

    /// Returns the immutable semantic domain used to calculate the digest.
    pub const fn domain(self) -> HashDomain {
        self.domain
    }

    /// Returns the 32 digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContentDigest")
            .field("scheme", &self.scheme)
            .field("domain", &self.domain)
            .field("bytes", &HexDigest(&self.bytes))
            .finish()
    }
}

/// A versioned keyed digest that records the selected key ID without exposing
/// key material.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyedDigest {
    scheme: u8,
    key_id: DigestKeyId,
    bytes: [u8; 32],
}

impl KeyedDigest {
    /// Constructs a v1 keyed digest from its key ID and digest bytes.
    pub const fn new(key_id: DigestKeyId, bytes: [u8; 32]) -> Self {
        Self {
            scheme: DIGEST_SCHEME_V1,
            key_id,
            bytes,
        }
    }

    /// Returns the HMAC scheme version.
    pub const fn scheme(self) -> u8 {
        self.scheme
    }

    /// Returns the non-secret digest-key identifier.
    pub const fn key_id(self) -> DigestKeyId {
        self.key_id
    }

    /// Returns the 32 digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

impl fmt::Debug for KeyedDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KeyedDigest")
            .field("scheme", &self.scheme)
            .field("key_id", &self.key_id)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

/// Computes a domain-separated v1 SHA-256 content digest.
pub fn hash(domain: HashDomain, payload: &[u8]) -> ContentDigest {
    let mut hasher = Sha256::new();
    write_frame(&mut hasher, HASH_PREFIX, domain.label(), payload);
    ContentDigest::new(domain, hasher.finalize().into())
}

/// Hashes one bounded command-batch source, item, or checkpoint document.
#[must_use]
pub fn hash_command_batch_document(payload: &[u8]) -> ContentDigest {
    hash(HashDomain::CommandBatch, payload)
}

macro_rules! typed_hash_function {
    ($(#[$meta:meta])* $name:ident, $domain:ident, $output:ident) => {
        $(#[$meta])*
        #[must_use]
        pub fn $name(payload: &[u8]) -> $output {
            $output::from_bytes(*hash(HashDomain::$domain, payload).as_bytes())
        }
    };
}

typed_hash_function!(
    /// Hashes a canonical value document in its immutable v1 domain.
    hash_canonical_value,
    CanonicalValue,
    CanonicalValueHash
);
typed_hash_function!(
    /// Hashes canonical contract source in its immutable v1 domain.
    hash_source,
    Source,
    SourceHash
);
typed_hash_function!(
    /// Hashes an immutable contract bundle in its v1 domain.
    hash_contract_bundle,
    ContractBundle,
    ContractBundleHash
);
typed_hash_function!(
    /// Hashes an executable command plan in its immutable v1 domain.
    hash_plan,
    Plan,
    PlanHash
);
typed_hash_function!(
    /// Hashes a validated projection plan in its immutable v1 domain.
    hash_projection_plan,
    ProjectionPlan,
    ProjectionPlanHash
);
typed_hash_function!(
    /// Hashes a closed RiffQL query access program in its immutable v1 domain.
    hash_query_plan,
    QueryPlan,
    QueryPlanHash
);
typed_hash_function!(
    /// Hashes a canonical immutable query module in its immutable v1 domain.
    hash_query_module,
    QueryModule,
    QueryModuleHash
);
typed_hash_function!(
    /// Hashes one canonical application manifest in its immutable v1 domain.
    hash_application_manifest,
    ApplicationManifest,
    ApplicationManifestHash
);
typed_hash_function!(
    /// Hashes one canonical symbolic application source manifest.
    hash_application_source,
    ApplicationSource,
    ApplicationSourceHash
);
typed_hash_function!(
    /// Hashes one canonical compiler-owned application lock.
    hash_application_lock,
    ApplicationLock,
    ApplicationLockHash
);
typed_hash_function!(
    /// Hashes one compiler-generated application artifact.
    hash_generated_artifact,
    GeneratedArtifact,
    GeneratedArtifactHash
);
typed_hash_function!(
    /// Hashes one exact tenant-unbound application role definition.
    hash_application_role_definition,
    ApplicationRoleDefinition,
    ApplicationRoleDefinitionHash
);
typed_hash_function!(
    /// Hashes one canonical compiled application role in its immutable v1 domain.
    hash_application_role,
    ApplicationRole,
    ApplicationRoleHash
);
typed_hash_function!(
    /// Hashes one exact RiffQL source document in its immutable v1 domain.
    hash_query_source,
    QuerySource,
    QuerySourceHash
);
typed_hash_function!(
    /// Hashes one canonical name-addressed RiffQL parameter set.
    hash_query_parameters,
    QueryParameters,
    QueryParameterHash
);
typed_hash_function!(
    /// Hashes an ordered contract semantic-plan set in its immutable v1 domain.
    hash_contract_plan_root,
    ContractPlanRoot,
    ContractPlanRootHash
);
typed_hash_function!(
    /// Hashes canonical command input in its immutable v1 domain.
    hash_command_input,
    CommandInput,
    CanonicalInputHash
);
typed_hash_function!(
    /// Hashes canonical event content in its immutable v1 domain.
    hash_event,
    Event,
    EventHash
);
typed_hash_function!(
    /// Hashes canonical entity-key bytes in their immutable v1 domain.
    hash_entity_key,
    EntityKey,
    EntityKeyHash
);
typed_hash_function!(
    /// Hashes canonical conflict-key bytes in their immutable v1 domain.
    hash_conflict_key,
    ConflictKey,
    ConflictKeyHash
);
typed_hash_function!(
    /// Hashes canonical partition-key bytes in their immutable v1 domain.
    hash_partition_key,
    PartitionKey,
    PartitionKeyHash
);
typed_hash_function!(
    /// Hashes generated schema content in its immutable v1 domain.
    hash_schema,
    Schema,
    SchemaHash
);
typed_hash_function!(
    /// Hashes the canonical bytes of a complete checked projection apply request.
    ///
    /// This primitive performs domain separation only. The storage API owns the
    /// semantic request validation and canonical preimage construction.
    hash_projection_apply,
    ProjectionApply,
    ProjectionApplyHash
);
typed_hash_function!(
    /// Hashes one canonical offline-maintenance semantic input.
    hash_offline_maintenance_input,
    OfflineMaintenanceInput,
    OfflineMaintenanceInputHash
);

/// Computes a domain-separated v1 HMAC-SHA-256 lookup digest.
pub fn keyed_hash(
    domain: KeyedHashDomain,
    key_id: DigestKeyId,
    key: &DigestKey,
    payload: &[u8],
) -> KeyedDigest {
    keyed_hash_secret(domain, key_id, key.expose_secret(), payload)
}

/// Computes a domain-separated v1 HMAC-SHA-256 lookup digest from borrowed
/// secret key bytes.
pub fn keyed_hash_secret(
    domain: KeyedHashDomain,
    key_id: DigestKeyId,
    key_bytes: &[u8; 32],
    payload: &[u8],
) -> KeyedDigest {
    let mut mac = Hmac::<Sha256>::new_from_slice(key_bytes)
        .expect("HMAC-SHA-256 accepts keys of every length");
    write_frame(&mut mac, HMAC_PREFIX, domain.label(), payload);
    KeyedDigest::new(key_id, mac.finalize().into_bytes().into())
}

/// Computes the v1 capability-token lookup digest from exactly 32 decoded raw
/// token bytes.
#[must_use]
pub fn hash_capability_token(
    key_id: DigestKeyId,
    key: &DigestKey,
    raw_token: &[u8; 32],
) -> CapabilityTokenDigest {
    hash_capability_token_secret(key_id, key.expose_secret(), raw_token)
}

/// Computes the v1 capability-token lookup digest from borrowed secret key
/// bytes and exactly 32 decoded raw token bytes.
#[must_use]
pub fn hash_capability_token_secret(
    key_id: DigestKeyId,
    key_bytes: &[u8; 32],
    raw_token: &[u8; 32],
) -> CapabilityTokenDigest {
    let digest = keyed_hash_secret(
        KeyedHashDomain::CapabilityToken,
        key_id,
        key_bytes,
        raw_token,
    );
    CapabilityTokenDigest::from_hmac_bytes(key_id, *digest.as_bytes())
}

fn write_frame<T: sha2::digest::Update>(
    target: &mut T,
    prefix: &[u8],
    domain: &str,
    payload: &[u8],
) {
    let domain = domain.as_bytes();
    debug_assert!(u16::try_from(domain.len()).is_ok());
    let domain_length = domain.len() as u16;
    let payload_length = payload.len() as u64;
    target.update(prefix);
    target.update(&[DIGEST_SCHEME_V1]);
    target.update(&domain_length.to_be_bytes());
    target.update(domain);
    target.update(&payload_length.to_be_bytes());
    target.update(payload);
}

struct HexDigest<'a>(&'a [u8; 32]);

impl fmt::Debug for HexDigest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn domain_registry_has_no_duplicate_labels() {
        let mut labels = BTreeSet::new();
        for domain in HashDomain::ALL {
            assert!(labels.insert(domain.label()));
        }
        for domain in KeyedHashDomain::ALL {
            assert!(labels.insert(domain.label()));
        }
    }

    #[test]
    fn identical_payloads_are_separated_by_domain() {
        let values = HashDomain::ALL
            .map(|domain| hash(domain, b"same payload"))
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(values.len(), HashDomain::ALL.len());
    }

    #[test]
    fn content_digest_retains_its_domain() {
        for domain in HashDomain::ALL {
            assert_eq!(hash(domain, b"payload").domain(), domain);
        }
    }
}
