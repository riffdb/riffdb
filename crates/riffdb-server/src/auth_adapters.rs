//! Owning production adapters for authentication, current policy, and secret custody.

use std::{fmt, sync::Arc};

use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure, CapabilityAuthenticator,
    CapabilityDigestKeyProvider, CapabilityReaderCurrentResolver, CredentialAuthenticator,
    IdempotencyDigestKeyProvider, IssueCapabilityTokenError, NewlyIssuedCapabilityToken,
    NoopAuthenticationTelemetry, OpaqueCredential, SystemEntropy, issue_capability_token,
};
use riffdb_idempotency::{
    IdempotencyDigestCandidatesV1, IdempotencyDigestError, IdempotencyDigestProvider,
};
use riffdb_policy::{
    AuthorizationError, CurrentAuthorizer, Decision, NoopAuthorizationTelemetry, OperationRequest,
    TrustedAudienceCatalog,
};
use riffdb_service::{CapabilityTokenIssueError, CapabilityTokenIssuer, CurrentPolicyPort};
use riffdb_storage_api::IdempotencyKeyDigest;
use riffdb_types::{DatabaseId, Environment, IdempotencyKey};

use crate::clocks::{ServerAuthenticationClock, ServerAuthorizationClock};
use crate::storage::SharedRedbOperationalPorts;

/// Owns the production dependencies borrowed by one fresh capability authenticator.
pub(crate) struct ServerCredentialAuthenticator {
    storage: SharedRedbOperationalPorts,
    keys: Arc<CapabilityDigestKeyProvider>,
    clock: ServerAuthenticationClock,
    telemetry: NoopAuthenticationTelemetry,
}

impl ServerCredentialAuthenticator {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        keys: Arc<CapabilityDigestKeyProvider>,
        clock: ServerAuthenticationClock,
    ) -> Self {
        Self {
            storage,
            keys,
            clock,
            telemetry: NoopAuthenticationTelemetry,
        }
    }
}

impl CredentialAuthenticator for ServerCredentialAuthenticator {
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        let authenticator = CapabilityAuthenticator::new(
            &self.storage,
            self.keys.as_ref(),
            &self.clock,
            &self.telemetry,
        );
        CredentialAuthenticator::authenticate(&authenticator, credential, context)
    }
}

impl fmt::Debug for ServerCredentialAuthenticator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCredentialAuthenticator([REDACTED])")
    }
}

/// Owns the trusted boundary needed to rebuild current authorization on every call.
pub(crate) struct ServerCurrentPolicyPort {
    storage: SharedRedbOperationalPorts,
    clock: ServerAuthorizationClock,
    database_id: DatabaseId,
    environment: Environment,
    trusted_audiences: TrustedAudienceCatalog,
    telemetry: NoopAuthorizationTelemetry,
}

impl ServerCurrentPolicyPort {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(
        storage: SharedRedbOperationalPorts,
        clock: ServerAuthorizationClock,
        database_id: DatabaseId,
        environment: Environment,
        trusted_audiences: TrustedAudienceCatalog,
    ) -> Self {
        Self {
            storage,
            clock,
            database_id,
            environment,
            trusted_audiences,
            telemetry: NoopAuthorizationTelemetry,
        }
    }
}

impl CurrentPolicyPort for ServerCurrentPolicyPort {
    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            &self.telemetry,
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize(principal, request)
    }
}

impl fmt::Debug for ServerCurrentPolicyPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCurrentPolicyPort([REDACTED])")
    }
}

/// Owns normal-create token entropy and capability digest-key custody.
pub(crate) struct ServerCapabilityTokenIssuer {
    keys: Arc<CapabilityDigestKeyProvider>,
    entropy: SystemEntropy,
}

impl ServerCapabilityTokenIssuer {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(keys: Arc<CapabilityDigestKeyProvider>) -> Self {
        Self {
            keys,
            entropy: SystemEntropy,
        }
    }
}

impl CapabilityTokenIssuer for ServerCapabilityTokenIssuer {
    fn issue(&self) -> Result<NewlyIssuedCapabilityToken, CapabilityTokenIssueError> {
        issue_capability_token(&self.entropy, self.keys.as_ref())
            .map_err(map_capability_token_issue_error)
    }
}

impl fmt::Debug for ServerCapabilityTokenIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCapabilityTokenIssuer([REDACTED])")
    }
}

fn map_capability_token_issue_error(error: IssueCapabilityTokenError) -> CapabilityTokenIssueError {
    match error {
        IssueCapabilityTokenError::EntropyUnavailable => CapabilityTokenIssueError::Unavailable,
        IssueCapabilityTokenError::EncodingFailure => CapabilityTokenIssueError::Integrity,
    }
}

/// Converts auth-owned keyed digests into idempotency-owned typed candidates.
pub(crate) struct ServerIdempotencyDigestProvider {
    keys: Arc<IdempotencyDigestKeyProvider>,
}

impl ServerIdempotencyDigestProvider {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(keys: Arc<IdempotencyDigestKeyProvider>) -> Self {
        Self { keys }
    }
}

impl IdempotencyDigestProvider for ServerIdempotencyDigestProvider {
    fn digest_candidates(
        &self,
        caller_key: &IdempotencyKey,
    ) -> Result<IdempotencyDigestCandidatesV1, IdempotencyDigestError> {
        let candidates = self
            .keys
            .digest_candidates(caller_key.expose_secret().as_bytes())
            .as_slice()
            .iter()
            .map(|digest| {
                IdempotencyKeyDigest::from_hmac_bytes(digest.key_id(), *digest.as_bytes())
            })
            .collect();
        IdempotencyDigestCandidatesV1::new(candidates)
    }
}

impl fmt::Debug for ServerIdempotencyDigestProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerIdempotencyDigestProvider([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use riffdb_auth::RawCapabilityToken;
    use riffdb_service::CapabilityTokenIssuer;
    use riffdb_types::DIGEST_SCHEME_V1;

    use super::*;

    const KEY_ZERO_TO_31: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
    const KEY_32_TO_63: &str = "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f";

    fn capability_keys() -> Arc<CapabilityDigestKeyProvider> {
        Arc::new(
            CapabilityDigestKeyProvider::parse_document(
                format!("riffdb-capability-digest-keys-v1\n7:{KEY_ZERO_TO_31}\n").as_bytes(),
            )
            .expect("valid capability keys"),
        )
    }

    fn idempotency_keys() -> Arc<IdempotencyDigestKeyProvider> {
        Arc::new(
            IdempotencyDigestKeyProvider::parse_document(
                format!(
                    "riffdb-idempotency-digest-keys-v1\n9:{KEY_32_TO_63}\n8:{KEY_ZERO_TO_31}\n"
                )
                .as_bytes(),
            )
            .expect("valid idempotency keys"),
        )
    }

    #[test]
    fn token_issuer_pairs_canonical_text_with_current_key_digest() {
        let keys = capability_keys();
        let issuer = ServerCapabilityTokenIssuer::new(Arc::clone(&keys));

        let issued = CapabilityTokenIssuer::issue(&issuer).expect("issue token");
        let raw = RawCapabilityToken::parse_canonical(issued.text().expose_secret())
            .expect("issuer returns canonical token text");

        assert_eq!(issued.digest(), keys.current_digest(&raw));
        assert_eq!(
            format!("{issuer:?}"),
            "ServerCapabilityTokenIssuer([REDACTED])"
        );
    }

    #[test]
    fn token_issue_errors_map_to_the_closed_service_classes() {
        assert_eq!(
            map_capability_token_issue_error(IssueCapabilityTokenError::EntropyUnavailable),
            CapabilityTokenIssueError::Unavailable
        );
        assert_eq!(
            map_capability_token_issue_error(IssueCapabilityTokenError::EncodingFailure),
            CapabilityTokenIssueError::Integrity
        );
    }

    #[test]
    fn idempotency_adapter_preserves_current_then_previous_candidates() {
        let keys = idempotency_keys();
        let provider = ServerIdempotencyDigestProvider::new(Arc::clone(&keys));
        let caller_key = IdempotencyKey::new("caller-retained-key").expect("valid caller key");

        let expected = keys.digest_candidates(caller_key.expose_secret().as_bytes());
        let actual = IdempotencyDigestProvider::digest_candidates(&provider, &caller_key)
            .expect("digest candidates");

        assert_eq!(actual.len(), expected.as_slice().len());
        assert_eq!(actual.current().key_id(), expected.current().key_id());
        for (actual, expected) in actual.as_slice().iter().zip(expected.as_slice()) {
            assert_eq!(actual.scheme(), DIGEST_SCHEME_V1);
            assert_eq!(actual.key_id(), expected.key_id());
            assert_eq!(actual.as_bytes(), expected.as_bytes());
        }
        assert_eq!(
            format!("{provider:?}"),
            "ServerIdempotencyDigestProvider([REDACTED])"
        );
    }

    #[test]
    fn owning_adapters_satisfy_shared_service_boundaries() {
        fn assert_send_sync<T: Send + Sync>() {}

        assert_send_sync::<ServerCredentialAuthenticator>();
        assert_send_sync::<ServerCurrentPolicyPort>();
        assert_send_sync::<ServerCapabilityTokenIssuer>();
        assert_send_sync::<ServerIdempotencyDigestProvider>();
    }
}
