//! Protocol-specific credential extraction with auth-owned verification.

use std::net::SocketAddr;

use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure,
    BootstrapDigestCandidates, CAPABILITY_TOKEN_TEXT_BYTES, CapabilityDigestKeyProvider,
    CredentialAuthenticator, OpaqueCredential, RawCapabilityToken, RetainedOpaqueCredential,
};
use tonic::metadata::MetadataMap;
use tonic::{Code, Status};

use crate::EMERGENCY_INTERNAL_MESSAGE;

/// Normal capability credential metadata key.
pub const AUTHORIZATION_METADATA_KEY: &str = "authorization";
/// Exact binary metadata key used only by loopback bootstrap.
pub const BOOTSTRAP_TOKEN_METADATA_KEY: &str = "riffdb-bootstrap-token-bin";
/// Static details-free message for failed credential authentication.
pub const UNAUTHENTICATED_MESSAGE: &str = "authentication failed";

/// Extracts exactly one normal bearer credential and delegates authentication.
///
/// Bootstrap metadata is rejected on this path. Token syntax, digest lookup,
/// lifecycle, and trusted-boundary checks remain owned by the injected
/// [`CredentialAuthenticator`].
pub fn authenticate_normal_request(
    metadata: &MetadataMap,
    authenticator: &dyn CredentialAuthenticator,
    context: &AuthenticationContext,
) -> Result<AuthenticatedPrincipal, Status> {
    let credential = extract_normal_credential(metadata)?;
    authenticator
        .authenticate(OpaqueCredential::new(credential), context)
        .map_err(authentication_failure)
}

/// Authenticates and retains the same exact ordinary bearer for staged restore.
///
/// Retention occurs only after protocol framing and the exact POC presentation
/// bound have been checked. The returned auth-owned value exposes only a
/// borrowed [`OpaqueCredential`] and is never serializable or cloneable.
pub fn authenticate_and_retain_normal_request(
    metadata: &MetadataMap,
    authenticator: &dyn CredentialAuthenticator,
    context: &AuthenticationContext,
) -> Result<(AuthenticatedPrincipal, RetainedOpaqueCredential), Status> {
    let credential = extract_normal_credential(metadata)?;
    let principal = authenticator
        .authenticate(OpaqueCredential::new(credential), context)
        .map_err(authentication_failure)?;
    let retained = RetainedOpaqueCredential::new(credential).map_err(|_| unauthenticated())?;
    Ok((principal, retained))
}

/// Retains one structurally valid bearer for recovery-only staged authentication.
///
/// This performs no authentication and must be used only after the server
/// lifecycle has returned its restricted recovery-service capability.
pub fn retain_normal_request_credential(
    metadata: &MetadataMap,
) -> Result<RetainedOpaqueCredential, Status> {
    let credential = extract_normal_credential(metadata)?;
    RetainedOpaqueCredential::new(credential).map_err(|_| unauthenticated())
}

fn extract_normal_credential(metadata: &MetadataMap) -> Result<&[u8], Status> {
    if metadata
        .get_all_bin(BOOTSTRAP_TOKEN_METADATA_KEY)
        .iter()
        .next()
        .is_some()
    {
        return Err(unauthenticated());
    }

    let mut values = metadata.get_all(AUTHORIZATION_METADATA_KEY).iter();
    let value = values.next().ok_or_else(unauthenticated)?;
    if values.next().is_some() {
        return Err(unauthenticated());
    }
    let value = value.to_str().map_err(|_| unauthenticated())?;
    let credential = value.strip_prefix("Bearer ").ok_or_else(unauthenticated)?;
    if credential.len() != CAPABILITY_TOKEN_TEXT_BYTES {
        return Err(unauthenticated());
    }
    Ok(credential.as_bytes())
}

/// Extracts and consumes the one canonical retained loopback bootstrap token.
///
/// Ordinary authorization metadata is rejected. The raw token is parsed by
/// `riffdb-auth`, converted to checked digest candidates by its key provider,
/// and dropped before this function returns.
pub fn prepare_loopback_bootstrap_token(
    metadata: &MetadataMap,
    peer: Option<SocketAddr>,
    keys: &CapabilityDigestKeyProvider,
) -> Result<BootstrapDigestCandidates, Status> {
    if !peer.is_some_and(|address| address.ip().is_loopback())
        || metadata
            .get_all(AUTHORIZATION_METADATA_KEY)
            .iter()
            .next()
            .is_some()
    {
        return Err(unauthenticated());
    }

    let mut values = metadata.get_all_bin(BOOTSTRAP_TOKEN_METADATA_KEY).iter();
    let value = values.next().ok_or_else(unauthenticated)?;
    if values.next().is_some() {
        return Err(unauthenticated());
    }
    let token_bytes = value.to_bytes().map_err(|_| unauthenticated())?;
    let token =
        RawCapabilityToken::parse_canonical(token_bytes.as_ref()).map_err(|_| unauthenticated())?;
    Ok(keys.prepare_bootstrap_token(token))
}

fn authentication_failure(failure: AuthenticationFailure) -> Status {
    match failure {
        AuthenticationFailure::Unauthenticated => unauthenticated(),
        AuthenticationFailure::Internal => Status::internal(EMERGENCY_INTERNAL_MESSAGE),
    }
}

fn unauthenticated() -> Status {
    Status::new(Code::Unauthenticated, UNAUTHENTICATED_MESSAGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_auth::{CapabilityTokenText, CredentialAuthenticator};
    use riffdb_types::{Audience, DatabaseId, Environment};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::metadata::{Binary, MetadataValue};

    struct RejectingAuthenticator;

    impl CredentialAuthenticator for RejectingAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
            Err(AuthenticationFailure::Unauthenticated)
        }
    }

    struct InternalAuthenticator;

    impl CredentialAuthenticator for InternalAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
            Err(AuthenticationFailure::Internal)
        }
    }

    struct CountingAuthenticator(AtomicUsize);

    impl CredentialAuthenticator for CountingAuthenticator {
        fn authenticate(
            &self,
            _credential: OpaqueCredential<'_>,
            _context: &AuthenticationContext,
        ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(AuthenticationFailure::Unauthenticated)
        }
    }

    #[test]
    fn missing_normal_credential_is_generic_and_details_free() {
        // The trusted context is not inspected before a missing credential is rejected.
        let metadata = MetadataMap::new();
        let context = test_authentication_context();
        let status = authenticate_normal_request(&metadata, &RejectingAuthenticator, &context)
            .expect_err("missing credentials must fail");
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(status.message(), UNAUTHENTICATED_MESSAGE);
        assert!(status.details().is_empty());
    }

    #[test]
    fn bootstrap_rejects_non_loopback_before_token_processing() {
        let metadata = MetadataMap::new();
        let keys = test_keys();
        let peer = "192.0.2.1:8080".parse().expect("test address");
        let status = prepare_loopback_bootstrap_token(&metadata, Some(peer), &keys)
            .expect_err("non-loopback bootstrap must fail");
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[test]
    fn bearer_scheme_is_case_sensitive_and_checked_before_handoff() {
        const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            format!("bearer {TOKEN}")
                .parse()
                .expect("valid ASCII metadata value"),
        );
        let authenticator = CountingAuthenticator(AtomicUsize::new(0));
        let status =
            authenticate_normal_request(&metadata, &authenticator, &test_authentication_context())
                .expect_err("lower-case bearer scheme must fail");
        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(authenticator.0.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn authentication_internal_uses_the_single_emergency_framing() {
        const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            format!("Bearer {TOKEN}")
                .parse()
                .expect("valid ASCII metadata value"),
        );
        let status = authenticate_normal_request(
            &metadata,
            &InternalAuthenticator,
            &test_authentication_context(),
        )
        .expect_err("internal authentication failure must stay redacted");
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), crate::EMERGENCY_INTERNAL_MESSAGE);
        assert!(status.details().is_empty());
    }

    #[test]
    fn restore_extracts_one_exact_presentation_for_auth_owned_retention() {
        const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            format!("Bearer {TOKEN}")
                .parse()
                .expect("valid ASCII metadata value"),
        );
        let retained =
            retain_normal_request_credential(&metadata).expect("exact presentation is bounded");

        assert_eq!(
            format!("{retained:?}"),
            "RetainedOpaqueCredential([REDACTED])"
        );
        assert_eq!(
            format!("{}", retained.borrow()),
            "opaque credential [REDACTED]"
        );
    }

    #[test]
    fn forwarded_identity_and_authorization_headers_never_become_credentials() {
        const TOKEN: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let mut metadata = MetadataMap::new();
        for (name, value) in [
            ("forwarded", "for=127.0.0.1;proto=https"),
            ("x-forwarded-authorization", "Bearer forwarded"),
            ("x-forwarded-user", "administrator"),
            ("x-riffdb-principal", "operator"),
            ("x-riffdb-tenant", "all"),
            ("x-riffdb-database", "default"),
        ] {
            metadata.insert(
                name,
                value.parse().expect("valid ASCII proxy metadata value"),
            );
        }
        assert_eq!(
            extract_normal_credential(&metadata)
                .expect_err("forwarded metadata must not authenticate")
                .code(),
            Code::Unauthenticated
        );

        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            format!("Bearer {TOKEN}")
                .parse()
                .expect("valid direct authorization metadata"),
        );
        assert_eq!(
            extract_normal_credential(&metadata).expect("direct bearer is the sole authority"),
            TOKEN.as_bytes()
        );
    }

    #[test]
    fn bootstrap_rejects_authorization_metadata() {
        let mut metadata = MetadataMap::new();
        metadata.insert(
            AUTHORIZATION_METADATA_KEY,
            "Bearer rejected"
                .parse()
                .expect("valid ASCII metadata value"),
        );
        let keys = test_keys();
        let peer = "127.0.0.1:8080".parse().expect("test address");
        let status = prepare_loopback_bootstrap_token(&metadata, Some(peer), &keys)
            .expect_err("bootstrap authorization metadata must fail");
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[test]
    fn bootstrap_consumes_one_exact_binary_token() {
        const TOKEN: &[u8; 43] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        CapabilityTokenText::parse(TOKEN).expect("canonical test token");
        let mut metadata = MetadataMap::new();
        metadata.insert_bin(
            BOOTSTRAP_TOKEN_METADATA_KEY,
            MetadataValue::<Binary>::from_bytes(TOKEN),
        );
        let keys = test_keys();
        let peer = "[::1]:8080".parse().expect("test address");
        let candidates = prepare_loopback_bootstrap_token(&metadata, Some(peer), &keys)
            .expect("valid loopback bootstrap token");
        assert_eq!(candidates.as_slice().len(), 1);
    }

    fn test_keys() -> CapabilityDigestKeyProvider {
        CapabilityDigestKeyProvider::parse_document(
            b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
        )
        .expect("valid test key document")
    }

    fn test_authentication_context() -> AuthenticationContext {
        AuthenticationContext::new(
            DatabaseId::from_unix_milliseconds_and_random(1, [0; 10])
                .expect("valid test database ID"),
            Environment::new("test").expect("valid test environment"),
            Audience::new("grpc").expect("valid test audience"),
        )
    }
}
