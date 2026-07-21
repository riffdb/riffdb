//! Canonical mutating-command idempotency preparation.

use std::{error::Error, fmt};

use riffdb_storage_api::{
    IdempotencyIdentity, IdempotencyKeyDigest, IdempotencyLookupCandidatesV1, StorageValueError,
};
use riffdb_types::{
    ActorId, CanonicalInputHash, CanonicalRecord, CanonicalValue, CommandId, ContractLineage,
    DatabaseId, Environment, FieldId, IdempotencyKey, TenantScope, encode_canonical_record,
    hash_command_input,
};

use crate::{IdempotencyDigestError, IdempotencyDigestProvider};

/// The authorization-resolved, version-independent scope of one mutating command identity.
///
/// Deployment version and invocation request ID are intentionally absent. The
/// stable command ID is lineage-scoped, while the caller-key digest is supplied
/// later by the operational digest provider.
#[derive(Clone, Eq, PartialEq)]
pub struct CommandIdempotencyScopeV1 {
    database_id: DatabaseId,
    environment: Environment,
    tenant_scope: TenantScope,
    principal_id: ActorId,
    contract_lineage: ContractLineage,
    command_id: CommandId,
}

impl CommandIdempotencyScopeV1 {
    /// Constructs an exact scope from checked database and authorization facts.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        environment: Environment,
        tenant_scope: TenantScope,
        principal_id: ActorId,
        contract_lineage: ContractLineage,
        command_id: CommandId,
    ) -> Self {
        Self {
            database_id,
            environment,
            tenant_scope,
            principal_id,
            contract_lineage,
            command_id,
        }
    }

    fn identity(&self, digest: IdempotencyKeyDigest) -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            self.database_id,
            self.environment.clone(),
            self.tenant_scope.clone(),
            self.principal_id.clone(),
            self.contract_lineage.clone(),
            self.command_id,
            digest,
        )
    }
}

impl fmt::Debug for CommandIdempotencyScopeV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandIdempotencyScopeV1([REDACTED])")
    }
}

/// Plan-independent identities used for the first bounded durable-state inspection.
///
/// The service prepares this value before selecting an active or historical plan.
/// Presence selects the exact stored plan; absence permits selection of the
/// requested active or explicit plan. Input normalization and hashing happen only
/// after that selection through [`confirm_command_idempotency`].
pub struct PreparedIdempotencyLookupV1 {
    lookup_candidates: IdempotencyLookupCandidatesV1,
}

impl PreparedIdempotencyLookupV1 {
    /// Borrows the current write-key identity followed by readable previous identities.
    #[must_use]
    pub const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.lookup_candidates
    }

    /// Borrows the identity selected if confirmation observes no durable admission.
    #[must_use]
    pub fn current_identity(&self) -> &IdempotencyIdentity {
        &self.lookup_candidates.as_slice()[0]
    }
}

impl fmt::Debug for PreparedIdempotencyLookupV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedIdempotencyLookupV1([REDACTED])")
    }
}

/// Privately constructed idempotency evidence for one normalized mutating input.
///
/// This value is not durable. The commit coordinator uses its first identity for
/// a new admission and the complete ordered candidate set for rotation-aware
/// lookup. It must not be constructed for grammar-v1 read-only commands.
pub struct PreparedCommandIdempotencyV1 {
    idempotency_field: FieldId,
    canonical_input_hash: CanonicalInputHash,
    lookup_candidates: IdempotencyLookupCandidatesV1,
}

impl PreparedCommandIdempotencyV1 {
    pub(crate) fn matches_idempotency_field(&self, expected: FieldId) -> bool {
        self.idempotency_field == expected
    }

    /// Returns the v1 hash of canonical input with the declared caller-key field omitted.
    #[must_use]
    pub const fn canonical_input_hash(&self) -> CanonicalInputHash {
        self.canonical_input_hash
    }

    /// Borrows the current write-key identity followed by readable previous identities.
    #[must_use]
    pub const fn lookup_candidates(&self) -> &IdempotencyLookupCandidatesV1 {
        &self.lookup_candidates
    }

    /// Borrows the identity selected for a new admission.
    #[must_use]
    pub fn current_identity(&self) -> &IdempotencyIdentity {
        &self.lookup_candidates.as_slice()[0]
    }
}

impl fmt::Debug for PreparedCommandIdempotencyV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PreparedCommandIdempotencyV1([REDACTED])")
    }
}

/// Safe preparation failure that never includes input values or caller-key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyPreparationError {
    /// The declared caller-key field was absent from normalized input.
    MissingIdempotencyField,
    /// The declared caller-key field was not a canonical string.
    IdempotencyFieldNotString,
    /// The canonical field and separately checked caller key differed.
    IdempotencyKeyMismatch,
    /// Canonical input could not be encoded within accepted v1 bounds.
    InvalidCanonicalInput,
    /// The typed operational digest provider failed closed.
    DigestProvider(IdempotencyDigestError),
    /// Digest candidates could not form one storage lookup set.
    InvalidLookupCandidates,
}

impl fmt::Display for IdempotencyPreparationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingIdempotencyField => {
                formatter.write_str("declared idempotency field is missing")
            }
            Self::IdempotencyFieldNotString => {
                formatter.write_str("declared idempotency field must be a string")
            }
            Self::IdempotencyKeyMismatch => {
                formatter.write_str("declared idempotency key does not match checked input")
            }
            Self::InvalidCanonicalInput => {
                formatter.write_str("canonical command input is invalid")
            }
            Self::DigestProvider(error) => error.fmt(formatter),
            Self::InvalidLookupCandidates => {
                formatter.write_str("idempotency lookup candidates are invalid")
            }
        }
    }
}

impl Error for IdempotencyPreparationError {}

impl From<IdempotencyDigestError> for IdempotencyPreparationError {
    fn from(error: IdempotencyDigestError) -> Self {
        Self::DigestProvider(error)
    }
}

/// Prepares canonical input evidence and rotation-aware identities for a mutating command.
///
/// The normalized record must contain the contract-declared caller-key field as
/// an exact canonical string equal to `caller_key`. The field is omitted before
/// canonical record encoding and v1 command-input hashing. Provider order is
/// retained exactly; numeric digest-key IDs are never sorted.
pub fn prepare_command_idempotency(
    scope: &CommandIdempotencyScopeV1,
    normalized_input: &CanonicalRecord,
    idempotency_field: FieldId,
    caller_key: &IdempotencyKey,
    digest_provider: &dyn IdempotencyDigestProvider,
) -> Result<PreparedCommandIdempotencyV1, IdempotencyPreparationError> {
    let lookup = prepare_idempotency_lookup(scope, caller_key, digest_provider)?;
    confirm_command_idempotency(lookup, normalized_input, idempotency_field, caller_key)
}

/// Prepares only rotation-aware identities for plan-independent durable inspection.
///
/// This operation does not accept a plan, normalized input, deployment version,
/// or request ID. Provider order is retained exactly; numeric digest-key IDs are
/// never sorted.
pub fn prepare_idempotency_lookup(
    scope: &CommandIdempotencyScopeV1,
    caller_key: &IdempotencyKey,
    digest_provider: &dyn IdempotencyDigestProvider,
) -> Result<PreparedIdempotencyLookupV1, IdempotencyPreparationError> {
    let digests = digest_provider.digest_candidates(caller_key)?;
    let identities = digests
        .as_slice()
        .iter()
        .map(|digest| scope.identity(*digest))
        .collect();
    let lookup_candidates =
        IdempotencyLookupCandidatesV1::new(identities).map_err(map_lookup_candidate_error)?;

    Ok(PreparedIdempotencyLookupV1 { lookup_candidates })
}

/// Confirms normalized input against identities prepared before plan selection.
///
/// Consuming the lookup evidence prevents confirmation from silently recomputing
/// digest candidates under a different provider configuration. The selected
/// plan's normalized record must contain the exact caller-key field, which is
/// omitted before canonical hashing.
pub fn confirm_command_idempotency(
    lookup: PreparedIdempotencyLookupV1,
    normalized_input: &CanonicalRecord,
    idempotency_field: FieldId,
    caller_key: &IdempotencyKey,
) -> Result<PreparedCommandIdempotencyV1, IdempotencyPreparationError> {
    let field_index = normalized_input
        .fields()
        .binary_search_by_key(&idempotency_field, |(field_id, _)| *field_id)
        .map_err(|_| IdempotencyPreparationError::MissingIdempotencyField)?;

    match &normalized_input.fields()[field_index].1 {
        CanonicalValue::String(value) if value.as_str() == caller_key.expose_secret() => {}
        CanonicalValue::String(_) => {
            return Err(IdempotencyPreparationError::IdempotencyKeyMismatch);
        }
        _ => return Err(IdempotencyPreparationError::IdempotencyFieldNotString),
    }

    let hash_input = CanonicalRecord::new(
        normalized_input
            .fields()
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != field_index)
            .map(|(_, field)| field.clone())
            .collect(),
    )
    .map_err(|_| IdempotencyPreparationError::InvalidCanonicalInput)?;
    let encoded = encode_canonical_record(&hash_input)
        .map_err(|_| IdempotencyPreparationError::InvalidCanonicalInput)?;
    let canonical_input_hash = hash_command_input(&encoded);

    Ok(PreparedCommandIdempotencyV1 {
        idempotency_field,
        canonical_input_hash,
        lookup_candidates: lookup.lookup_candidates,
    })
}

fn map_lookup_candidate_error(_: StorageValueError) -> IdempotencyPreparationError {
    IdempotencyPreparationError::InvalidLookupCandidates
}
