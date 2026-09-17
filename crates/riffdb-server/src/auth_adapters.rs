//! Owning production adapters for authentication, current policy, and secret custody.

use std::{fmt, sync::Arc};

use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, AuthenticationFailure, AuthenticationTelemetry,
    CapabilityAuthenticator, CapabilityDigestKeyProvider, CapabilityReaderCurrentResolver,
    CredentialAuthenticator, IdempotencyDigestKeyProvider, IssueCapabilityTokenError,
    NewlyIssuedCapabilityToken, OpaqueCredential, SystemEntropy, issue_capability_token,
};
use riffdb_idempotency::{
    IdempotencyDigestCandidatesV1, IdempotencyDigestError, IdempotencyDigestProvider,
};
use riffdb_policy::{
    ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
    ApplicationReimportAuthorizationRequestV1, ApplicationReimportDecisionV1, AuthorizationClock,
    AuthorizationError, AuthorizationTelemetry, CapabilityViewCheckpoint,
    ContractMigrationAuthorizationRequest, ContractMigrationDecision, CurrentAuthorizer, Decision,
    OfflineMaintenanceAuthorizationRequest, OfflineMaintenanceDecision, OperationRequest,
    TrustedAudienceCatalog,
};
use riffdb_service::{CapabilityTokenIssueError, CapabilityTokenIssuer, CurrentPolicyPort};
use riffdb_storage_api::{CapabilityReader, IdempotencyKeyDigest};
use riffdb_types::{DatabaseId, Environment, IdempotencyKey};

use crate::clocks::{ServerAuthenticationClock, ServerAuthorizationClock};
use crate::storage::SharedRedbOperationalPorts;

/// Owns the production dependencies borrowed by one fresh capability authenticator.
pub(crate) struct ServerCredentialAuthenticator<S = SharedRedbOperationalPorts> {
    storage: S,
    keys: Arc<CapabilityDigestKeyProvider>,
    clock: ServerAuthenticationClock,
    telemetry: Arc<dyn AuthenticationTelemetry>,
}

impl<S> ServerCredentialAuthenticator<S> {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(
        storage: S,
        keys: Arc<CapabilityDigestKeyProvider>,
        clock: ServerAuthenticationClock,
        telemetry: Arc<dyn AuthenticationTelemetry>,
    ) -> Self {
        Self {
            storage,
            keys,
            clock,
            telemetry,
        }
    }
}

impl<S: CapabilityReader + Send + Sync> CredentialAuthenticator
    for ServerCredentialAuthenticator<S>
{
    fn authenticate(
        &self,
        credential: OpaqueCredential<'_>,
        context: &AuthenticationContext,
    ) -> Result<AuthenticatedPrincipal, AuthenticationFailure> {
        let authenticator = CapabilityAuthenticator::new(
            &self.storage,
            self.keys.as_ref(),
            &self.clock,
            self.telemetry.as_ref(),
        );
        CredentialAuthenticator::authenticate(&authenticator, credential, context)
    }
}

impl<S> fmt::Debug for ServerCredentialAuthenticator<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerCredentialAuthenticator([REDACTED])")
    }
}

/// Owns the trusted boundary needed to rebuild current authorization on every call.
pub(crate) struct ServerCurrentPolicyPort<S = SharedRedbOperationalPorts> {
    storage: S,
    clock: ServerAuthorizationClock,
    database_id: DatabaseId,
    environment: Environment,
    trusted_audiences: TrustedAudienceCatalog,
    telemetry: Arc<dyn AuthorizationTelemetry>,
}

impl<S> ServerCurrentPolicyPort<S> {
    #[allow(
        dead_code,
        reason = "WP-130 composition constructs this adapter after staged storage activation"
    )]
    pub(crate) fn new(
        storage: S,
        clock: ServerAuthorizationClock,
        database_id: DatabaseId,
        environment: Environment,
        trusted_audiences: TrustedAudienceCatalog,
        telemetry: Arc<dyn AuthorizationTelemetry>,
    ) -> Self {
        Self {
            storage,
            clock,
            database_id,
            environment,
            trusted_audiences,
            telemetry,
        }
    }
}

impl<S: CapabilityReader + Send + Sync> CurrentPolicyPort for ServerCurrentPolicyPort<S> {
    fn authorize_replication_administration(
        &self,
        principal: &AuthenticatedPrincipal,
        request: riffdb_auth::ReplicationAdministrationRequestV1,
    ) -> Result<riffdb_policy::ReplicationAdministrationDecision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .authorize_replication_administration(principal, request)
    }

    fn authorize_replication(
        &self,
        principal: &AuthenticatedPrincipal,
    ) -> Result<riffdb_policy::ReplicationDecision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .authorize_replication(principal)
    }

    fn authorize(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OperationRequest,
    ) -> Result<Decision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize(principal, request)
    }

    fn authorize_offline_maintenance(
        &self,
        principal: &AuthenticatedPrincipal,
        request: OfflineMaintenanceAuthorizationRequest,
    ) -> Result<OfflineMaintenanceDecision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize_offline_maintenance(principal, request)
    }

    fn authorize_contract_migration(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ContractMigrationAuthorizationRequest,
    ) -> Result<ContractMigrationDecision, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize_contract_migration(principal, request)
    }

    fn authorize_application_export(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ApplicationExportAuthorizationRequestV1,
    ) -> Result<ApplicationExportDecisionV1, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize_application_export(principal, request)
    }

    fn authorize_application_reimport(
        &self,
        principal: &AuthenticatedPrincipal,
        request: ApplicationReimportAuthorizationRequestV1,
    ) -> Result<ApplicationReimportDecisionV1, AuthorizationError> {
        let resolver = CapabilityReaderCurrentResolver::new(&self.storage);
        let authorizer = CurrentAuthorizer::new(
            &resolver,
            &self.clock,
            self.telemetry.as_ref(),
            self.database_id,
            self.environment.clone(),
        )
        .with_trusted_audience_catalog(&self.trusted_audiences);
        authorizer.authorize_application_reimport(principal, request)
    }

    fn capability_view_generation(&self) -> Option<u64> {
        CapabilityReader::capability_view_generation(&self.storage)
    }

    fn capability_view_checkpoint(&self) -> Option<CapabilityViewCheckpoint> {
        // Sampled from the same authorization clock `authorize` hands to
        // `CurrentAuthorizer`, so the validity-window clause a revision-checked
        // reauthorization applies is exactly the clause a full evaluation would
        // have applied. Either source failing yields `None`, which forces the
        // caller into a full evaluation.
        let generation = CapabilityReader::capability_view_generation(&self.storage)?;
        let now = AuthorizationClock::now(&self.clock).ok()?;
        Some(CapabilityViewCheckpoint::new(generation, now))
    }
}

impl<S> fmt::Debug for ServerCurrentPolicyPort<S> {
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

    /// The production revision-checked reauthorization chain, end to end.
    ///
    /// Real redb database → real `SharedRedbOperationalPorts` current
    /// capability view → real `CurrentCapabilityViewState::publish` under the
    /// view write lock → real `CapabilityReader::capability_view_generation` →
    /// real `ServerCurrentPolicyPort::capability_view_checkpoint` → real
    /// `AuthorizedOperation::reissue_for_unchanged_view`.
    ///
    /// Both dimensions of the reissue rule are exercised against that chain:
    /// a real revocation moves the generation, and a real clock advance past
    /// the capability's expiry invalidates the retained validity window even
    /// though nothing was published.
    #[test]
    // req: REP-006
    fn production_capability_view_chain_refuses_reissue_after_revoke_and_after_expiry() {
        use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
        use std::sync::Arc;
        use std::sync::atomic::{AtomicI64, Ordering};

        use riffdb_auth::{
            CapabilityAuthenticator, CapabilityDigestKeyProvider, CredentialAuthenticator,
            NoopAuthenticationTelemetry, OpaqueCredential,
        };
        use riffdb_policy::{Decision, NoopAuthorizationTelemetry, OperationRequest};
        use riffdb_service::CurrentPolicyPort;
        use riffdb_storage_api::{
            AuditPrincipalV1, BootstrapDigestCandidatesV1, BootstrapServiceAuditStartV1,
            CapabilityAdministrationTransactionPort, CapabilityBootstrapAdministrationRepository,
            CapabilityBootstrapIntentV1, CapabilityBootstrapResult, CapabilityGrantV1,
            CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
            CapabilityRevokeAwaitingDecision, CapabilityRevokeCandidateTransaction,
            CapabilityRevokeCandidateV1, CapabilityRevokeIntentV1, CapabilityRevokeResult,
            PartitionScopeV1,
        };
        use riffdb_types::{
            ActorId, ActorKind, Audience, CapabilityId, CapabilityPermissionKindV1,
            RevocationReasonCodeV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
            ServiceIngressKindV1, TenantScope, Timestamp,
        };

        use crate::clocks::ProductionWallClocks;
        use crate::real_storage_support::RealStorage;

        const ISSUED_SECONDS: i64 = 1_700_000_000;
        const LIFETIME_SECONDS: u32 = 3_600;

        let real = RealStorage::open("capability-view-chain");
        let mut storage = real.storage.clone();
        let environment = Environment::new("capability-view-chain").expect("environment");
        let audience = Audience::new("riffdb-test").expect("audience");

        // A real digest-key document and a real issued capability token, so the
        // record authenticates through the production authenticator.
        let keys = Arc::new(
            CapabilityDigestKeyProvider::parse_document(
                b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
            )
            .expect("digest key document"),
        );
        let issued = riffdb_auth::issue_capability_token(&SystemEntropy, keys.as_ref())
            .expect("issue capability token");
        let token_text = issued.text().expose_secret().to_owned();
        let token_digest = issued.digest();

        let capability_id = CapabilityId::from_unix_milliseconds_and_random(9, [0x9c; 10])
            .expect("capability UUIDv7");
        let request_id = riffdb_types::RequestId::from_unix_milliseconds_and_random(9, [0x9d; 10])
            .expect("request UUIDv7");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::AdministerCapabilities,
                )
                .expect("administer permission"),
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                    .expect("read-contract permission"),
            ])
            .expect("canonical permissions"),
            Vec::new(),
            NonZeroU16::new(10).expect("row limit"),
            Vec::new(),
        )
        .expect("bounded grant");
        let issued_at = Timestamp::new(ISSUED_SECONDS, 0).expect("issued at");
        let expires_at =
            Timestamp::new(ISSUED_SECONDS + i64::from(LIFETIME_SECONDS), 0).expect("expires at");
        let bootstrap = CapabilityBootstrapIntentV1::new(
            capability_id,
            CapabilityRequestedRecordV1::new(
                real.database_id,
                environment.clone(),
                ActorId::new("view-chain-operator").expect("principal"),
                ActorKind::Human,
                NonZeroU32::new(LIFETIME_SECONDS).expect("lifetime"),
                vec![audience.clone()],
                grant,
            )
            .expect("requested record"),
            BootstrapDigestCandidatesV1::new(vec![token_digest], token_digest)
                .expect("digest candidates"),
            issued_at,
            expires_at,
            BootstrapServiceAuditStartV1::new(
                request_id,
                issued_at,
                ServiceIngressKindV1::Grpc,
                ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(capability_id)])
                    .expect("audit targets"),
                None,
            )
            .expect("bootstrap start"),
        )
        .expect("bootstrap intent");

        // Production bootstrap publishes the record under the view write lock.
        let generation_before_bootstrap =
            CapabilityReader::capability_view_generation(&storage).expect("published generation");
        let created = storage
            .bootstrap_capability(&bootstrap)
            .expect("bootstrap the capability in real storage");
        assert!(matches!(
            created,
            CapabilityBootstrapResult::BootstrapCreated { .. }
        ));
        let generation_after_bootstrap =
            CapabilityReader::capability_view_generation(&storage).expect("published generation");
        assert_ne!(
            generation_before_bootstrap, generation_after_bootstrap,
            "a real bootstrap publish must move the capability-view generation"
        );

        let clock_seconds = Arc::new(AtomicI64::new(ISSUED_SECONDS + 10));
        let clocks = ProductionWallClocks::settable(Arc::clone(&clock_seconds));
        let principal = CapabilityAuthenticator::new(
            &storage,
            keys.as_ref(),
            &clocks.authentication(),
            &NoopAuthenticationTelemetry,
        )
        .authenticate(
            OpaqueCredential::new(&token_text),
            &AuthenticationContext::new(real.database_id, environment.clone(), audience.clone()),
        )
        .expect("the issued token authenticates against real storage");

        let port = ServerCurrentPolicyPort::new(
            real.storage.clone(),
            clocks.authorization(),
            real.database_id,
            environment,
            TrustedAudienceCatalog::new(vec![audience]).expect("trusted audience"),
            Arc::new(NoopAuthorizationTelemetry),
        );

        let lifecycle_request = riffdb_auth::ReplicationAdministrationRequestV1::register(
            request_id,
            riffdb_types::ReplicationFollowerAuditTargetV1::new(
                real.database_id,
                1,
                riffdb_types::LeadershipEpochV1::initial(),
                riffdb_types::ReplicationSourceHoldIdV1::new([0x71; 16]).unwrap(),
            )
            .unwrap(),
            riffdb_storage_api::FollowerHoldBudget::new(3).unwrap(),
            None,
        );
        let Ok(riffdb_policy::ReplicationAdministrationDecision::Allow(preparation)) =
            port.authorize_replication_administration(&principal, lifecycle_request)
        else {
            panic!("current global administrator must prepare the exact lifecycle request");
        };
        assert_eq!(preparation.request(), lifecycle_request);
        assert_eq!(
            preparation.authorized_at(),
            Timestamp::new(ISSUED_SECONDS + 10, 0).unwrap()
        );

        // Baseline: capture the generation before evaluating, exactly as
        // `begin_invocation` does, then take the full evaluation.
        let baseline = port
            .capability_view_generation()
            .expect("real port publishes a generation");
        let request = OperationRequest::get_active_contract();
        let Ok(Decision::Allow(proof)) = port.authorize(&principal, request.clone()) else {
            panic!("the bootstrapped capability must be allowed to read the active contract");
        };

        // Unchanged world, unchanged clock: the reissue rule accepts.
        let unchanged = port.capability_view_checkpoint().expect("live checkpoint");
        assert_eq!(unchanged.generation(), baseline);
        assert!(
            proof
                .reissue_for_unchanged_view(baseline, unchanged, &request)
                .is_some(),
            "an unchanged view inside the validity window must reissue"
        );

        // Time dimension: nothing is published, so the generation is unchanged,
        // but the retained validity window no longer admits the clock.
        clock_seconds.store(
            ISSUED_SECONDS + i64::from(LIFETIME_SECONDS) + 1,
            Ordering::Release,
        );
        let expired = port.capability_view_checkpoint().expect("live checkpoint");
        assert!(matches!(
            port.authorize_replication_administration(&principal, lifecycle_request),
            Ok(riffdb_policy::ReplicationAdministrationDecision::Deny(_))
        ));
        assert_eq!(
            expired.generation(),
            baseline,
            "no publish happened, so the generation must not move"
        );
        assert!(
            proof
                .reissue_for_unchanged_view(baseline, expired, &request)
                .is_none(),
            "an expired capability must never be reissued on an unchanged generation"
        );
        assert!(
            matches!(
                port.authorize(&principal, request.clone()),
                Ok(Decision::Deny(_))
            ),
            "full evaluation must also deny the expired capability"
        );
        clock_seconds.store(ISSUED_SECONDS + 10, Ordering::Release);

        // Generation dimension: revoke through the production revoke
        // transaction, which publishes the revoked record under the view write
        // lock inside `commit_revoke`.
        let (awaiting, current) = storage
            .begin_capability_revoke(CapabilityRevokeCandidateV1::new(
                capability_id,
                request_id,
                AuditPrincipalV1::new(
                    ActorId::new("view-chain-operator").expect("principal"),
                    ActorKind::Human,
                    capability_id,
                    NonZeroU64::MIN,
                ),
                None,
                RevocationReasonCodeV1::Requested,
            ))
            .expect("begin the real revoke transaction")
            .read_transaction_current()
            .expect("read transaction-current capability state");
        let revoked = awaiting
            .commit_revoke(CapabilityRevokeIntentV1::new(
                capability_id,
                current
                    .target()
                    .expect("the bootstrapped target is transaction-current")
                    .revision(),
                request_id,
                Timestamp::new(ISSUED_SECONDS + 20, 0).expect("revoked at"),
                AuditPrincipalV1::new(
                    ActorId::new("view-chain-operator").expect("principal"),
                    ActorKind::Human,
                    capability_id,
                    NonZeroU64::MIN,
                ),
                None,
                RevocationReasonCodeV1::Requested,
            ))
            .expect("commit the real revoke");
        assert!(matches!(revoked, CapabilityRevokeResult::Revoked { .. }));

        let after_revoke = port.capability_view_checkpoint().expect("live checkpoint");
        assert!(matches!(
            port.authorize_replication_administration(&principal, lifecycle_request),
            Ok(riffdb_policy::ReplicationAdministrationDecision::Deny(_))
        ));
        assert_ne!(
            after_revoke.generation(),
            baseline,
            "the production revoke publish must move the capability-view generation"
        );
        assert!(
            proof
                .reissue_for_unchanged_view(baseline, after_revoke, &request)
                .is_none(),
            "a moved generation must never reissue"
        );
        assert!(
            matches!(port.authorize(&principal, request), Ok(Decision::Deny(_))),
            "the mandatory full re-evaluation must fail closed after revocation"
        );
    }
}
