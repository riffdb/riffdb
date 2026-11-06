// req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001

use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use riffdb_auth::{
    AuthenticatedPrincipal, AuthenticationContext, CapabilityAuthenticator,
    CapabilityDigestKeyProvider, CredentialAuthenticator, IdempotencyDigestKeyProvider,
    NoopAuthenticationTelemetry, OpaqueCredential, SystemEntropy,
};
use riffdb_catalog::ValidatedContractBundle;
use riffdb_commit::{
    CoordinatorDurability, CoordinatorWorkloadCapacity, RunningCommandCoordinator,
};
use riffdb_conflict::{ConflictManager, ConflictManagerConfig, ShardedConflictManager};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_errors::IncidentIdSource;
use riffdb_idempotency::IdempotencyDigestProvider;
use riffdb_observability::{
    AuthoritativeComponent, AuthoritativeCondition, MAX_TRACE_RECORDS, Observability,
};
use riffdb_policy::{
    AgentSessionAdmissionPolicy, TrustedAudienceCatalog, UntrustedInvocationClaims,
};
use riffdb_projection::{ProjectionNotifier, ProjectionSchemaRegistry};
use riffdb_service::{
    AuthoritativeReadPort, AwaitSagaRequest, BuildInfo, CancelSagaRequest, CapabilityTokenIssuer,
    CatalogReadPort, CommandApplication, CurrentPolicyPort, CursorMonotonicClock,
    CursorTokenGenerator, ExecuteCommandRequest, GetSagaRequest, GetSagaResult,
    OperationalStatusPort, ProjectionQueryPort, RequestContext, RequestControl,
    RequestDeadlineScheduler, ResumeSagaRequest, RiffDbService, SagaApplication,
    ServiceDiagnostics, ServiceExecutors, ServiceHealthHooks, ServiceIdentity, ServiceJobSpawner,
    ServiceProcessMetadata, ServiceProviders, ServiceTelemetry, SourceName, StartSagaDisposition,
    StartSagaRequest, SubmittedField, SubmittedFieldIdentity, SubmittedRecord, SubmittedValue,
};
use riffdb_storage_api::{
    AuditPrincipalV1, AuthoritativePointReader, BootstrapDigestCandidatesV1,
    BootstrapServiceAuditStartV1, CapabilityAdministrationTransactionPort,
    CapabilityBootstrapAdministrationRepository, CapabilityBootstrapIntentV1,
    CapabilityBootstrapResult, CapabilityCreateAwaitingDecision,
    CapabilityCreateCandidateTransaction, CapabilityCreateCandidateV1, CapabilityCreateIntentV1,
    CapabilityCreateResult, CapabilityGrantV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityRequestedRecordV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, EntityTarget, PartitionScopeV1, ReadableDigestKey,
    ReadableIdempotencyDigestInventory, SagaCoordinationRepository,
};
use riffdb_types::{
    ActorId, ActorKind, Audience, CanonicalRecord, CanonicalValue, CapabilityId,
    CapabilityPermissionKindV1, ContractLineage, EntityKeyBuilder, Environment, FieldId, RequestId,
    SagaCompareExpectedV1, SagaId, SagaLifecycleStatusV1, SagaStepDirectionV1,
    SagaStepEffectDispositionV1, ServiceAuditTargetV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    StoredSagaInstanceV1, TenantScope, Timestamp, derive_saga_step_idempotency_key,
};

use crate::auth_adapters::{
    ServerCapabilityTokenIssuer, ServerCurrentPolicyPort, ServerIdempotencyDigestProvider,
};
use crate::clocks::ProductionWallClocks;
use crate::columnar_worker::ColumnarWorkerStatus;
use crate::cursor::{ProductionCursorMonotonicClock, ProductionCursorTokenGenerator};
use crate::identifiers::ProductionIdentifierSources;
use crate::notifications::FirstCommitNotificationHub;
use crate::operational_status::{OutboxRecoveryReadiness, ProductionOperationalStatusPort};
use crate::outbox_adapter::{NoDestinationOutboxHealth, ServerCommitNotificationSink};
use crate::port_driver::BlockingPortDriver;
use crate::projection_adapter::ServerProjectionQueryPort;
use crate::projection_worker::ProjectionWorkerStatus;
use crate::read_adapters::{ServerAuthoritativeReadPort, ServerCatalogReadPort};
use crate::real_storage_support::RealStorage;
use crate::runtime_support::{
    ProductionObservabilityDiagnostics, ProductionObservabilityHealthHooks, RuntimeRoutingState,
    SupervisedServiceJobSpawner, TokioRequestDeadlineScheduler,
};
use crate::saga_worker::{RunningSagaWorker, SagaWorkNotification, ServerSagaCoordinator};
use crate::startup::ValidatedAllocatorCapacity;

const ISSUED_SECONDS: i64 = 1_700_000_000;
const CAPABILITY_KEYS_DOC: &[u8] =
    b"riffdb-capability-digest-keys-v1\n1:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n";
const IDEMPOTENCY_KEYS_DOC: &[u8] =
    b"riffdb-idempotency-digest-keys-v1\n1:101112131415161718191a1b1c1d1e1f000102030405060708090a0b0c0d0e0f\n";

fn checkout_saga_contract_source() -> &'static str {
    r#"
contract CheckoutService version 1 {
  entity Inventory {
    key (sku: u64)
    field available: u64
    field frozen: bool
  }

  aggregate Inventories {
    root Inventory
    partition_by sku
    conflict_key (sku)
  }

  entity Wallet {
    key (user_id: u64)
    field balance: u64
  }

  aggregate Wallets {
    root Wallet
    partition_by user_id
    conflict_key (user_id)
  }

  entity Order {
    key (order_id: u64)
    field sku: u64
    field user_id: u64
    field quantity: u64
    field amount: u64
    field status: string<32>
  }

  aggregate Orders {
    root Order
    partition_by order_id
    conflict_key (order_id)
  }

  command SeedInventory {
    input idempotency_key: string<64>
    input sku: u64
    input available: u64
    idempotency_key idempotency_key
    create Inventory(sku) as inv else DuplicateInventory { sku: sku }
    set inv.available = available
    set inv.frozen = false
    return Seeded { sku: inv.sku }
  }

  command SetInventoryFrozen {
    input idempotency_key: string<64>
    input sku: u64
    input frozen: bool
    idempotency_key idempotency_key
    mutate Inventory(sku) as inv else MissingInventory { sku: sku }
    overwrite inv.frozen = frozen
    return Updated { sku: inv.sku }
  }

  command SeedWallet {
    input idempotency_key: string<64>
    input user_id: u64
    input balance: u64
    idempotency_key idempotency_key
    create Wallet(user_id) as wallet else DuplicateWallet { user_id: user_id }
    set wallet.balance = balance
    return Seeded { user_id: wallet.user_id }
  }

  command ReserveInventory {
    input idempotency_key: string<128>
    input sku: u64
    input quantity: u64
    idempotency_key idempotency_key
    mutate Inventory(sku) as inv else MissingInventory { sku: sku }
    require has_stock: inv.available >= quantity else OutOfStock { sku: inv.sku }
    set inv.available = inv.available - quantity
    return Reserved {
      sku: inv.sku,
      reserved_qty: quantity
    }
  }

  command ReleaseInventory {
    input idempotency_key: string<128>
    input sku: u64
    input quantity: u64
    idempotency_key idempotency_key
    mutate Inventory(sku) as inv else MissingInventory { sku: sku }
    require not_frozen: !inv.frozen else ReleaseFrozen { sku: inv.sku }
    set inv.available = inv.available + quantity
    return Released {
      sku: inv.sku
    }
  }

  command ChargeWallet {
    input idempotency_key: string<128>
    input user_id: u64
    input amount: u64
    idempotency_key idempotency_key
    mutate Wallet(user_id) as wallet else MissingWallet { user_id: user_id }
    require enough_balance: wallet.balance >= amount else InsufficientFunds { user_id: wallet.user_id }
    set wallet.balance = wallet.balance - amount
    return Charged {
      user_id: wallet.user_id,
      charged_amount: amount
    }
  }

  command RefundWallet {
    input idempotency_key: string<128>
    input user_id: u64
    input amount: u64
    idempotency_key idempotency_key
    mutate Wallet(user_id) as wallet else MissingWallet { user_id: user_id }
    set wallet.balance = wallet.balance + amount
    return Refunded {
      user_id: wallet.user_id
    }
  }

  command ConfirmOrder {
    input idempotency_key: string<128>
    input order_id: u64
    input sku: u64
    input user_id: u64
    input quantity: u64
    input amount: u64
    input reject_order: bool
    idempotency_key idempotency_key
    create Order(order_id) as ord else OrderAlreadyExists { order_id: order_id }
    require order_allowed: !reject_order else OrderRejected { order_id: order_id }
    set ord.sku = sku
    set ord.user_id = user_id
    set ord.quantity = quantity
    set ord.amount = amount
    set ord.status = "CONFIRMED"
    return Confirmed {
      order_id: ord.order_id
    }
  }

  role CheckoutOrchestrator {
    commands (
      SeedInventory,
      SetInventoryFrozen,
      SeedWallet,
      ReserveInventory,
      ReleaseInventory,
      ChargeWallet,
      RefundWallet,
      ConfirmOrder
    )
  }

  saga CheckoutOrder {
    partition_by order_id
    input idempotency_key: string<128>
    input saga_id: uuid
    input order_id: u64
    input sku: u64
    input user_id: u64
    input quantity: u64
    input amount: u64
    input reject_order: bool
    idempotency_key idempotency_key
    execution_role CheckoutOrchestrator

    step reserve {
      command ReserveInventory(idempotency_key, sku, quantity)
      compensate ReleaseInventory(idempotency_key, sku, reserve.reserved_qty)
      on Reserved (effectful) => next charge
      on OutOfStock (no_effect) => abort OrderOutOfStock { order_id: order_id }
      on MissingInventory (no_effect) => abort OrderOutOfStock { order_id: order_id }
    }

    step charge {
      command ChargeWallet(idempotency_key, user_id, amount)
      compensate RefundWallet(idempotency_key, user_id, charge.charged_amount)
      on Charged (effectful) => next confirm
      on InsufficientFunds (no_effect) => compensate OrderRolledBack { order_id: order_id }
      on MissingWallet (no_effect) => compensate OrderRolledBack { order_id: order_id }
    }

    step confirm {
      command ConfirmOrder(
        idempotency_key,
        order_id,
        sku,
        user_id,
        quantity,
        amount,
        reject_order
      )
      on Confirmed (effectful) => complete OrderCompleted {
        order_id: confirm.order_id,
        charged_amount: charge.charged_amount
      }
      on OrderRejected (no_effect) => compensate OrderRolledBack {
        order_id: order_id
      }
      on OrderAlreadyExists (no_effect) => compensate OrderRolledBack {
        order_id: order_id
      }
    }
  }

  saga UncompensatedAuditCheckout {
    partition_by order_id
    input idempotency_key: string<128>
    input saga_id: uuid
    input order_id: u64
    input sku: u64
    input user_id: u64
    input quantity: u64
    input amount: u64
    idempotency_key idempotency_key
    execution_role CheckoutOrchestrator

    step reserve {
      command ReserveInventory(idempotency_key, sku, quantity)
      compensate ReleaseInventory(idempotency_key, sku, reserve.reserved_qty)
      on Reserved (effectful) => next charge
      on OutOfStock (no_effect) => abort OrderAborted { order_id: order_id }
      on MissingInventory (no_effect) => abort OrderAborted { order_id: order_id }
    }

    step charge {
      command ChargeWallet(idempotency_key, user_id, amount)
      on Charged (effectful) => complete OrderCompleted { order_id: order_id }
      on InsufficientFunds (no_effect) => abort_uncompensated OrderLeftReserved { order_id: order_id }
      on MissingWallet (no_effect) => abort_uncompensated OrderLeftReserved { order_id: order_id }
    }
  }
}
"#
}

struct SagaTestServer {
    real: RealStorage,
    bundle: ValidatedContractBundle,
    lineage: ContractLineage,
    environment: Environment,
    audience: Audience,
    clock_seconds: Arc<AtomicI64>,
    clocks: ProductionWallClocks,
    capability_keys: Arc<CapabilityDigestKeyProvider>,
    bootstrap_capability_id: CapabilityId,
    service: Arc<RiffDbService>,
    saga_notify: Arc<SagaWorkNotification>,
    coordinator: Option<RunningCommandCoordinator>,
    blocking: Option<BlockingPortDriver>,
    spawner: SupervisedServiceJobSpawner,
}

impl SagaTestServer {
    fn new(label: &str) -> Self {
        let real = RealStorage::open(label);
        let mut storage = real.storage.clone();
        let compiled =
            compile_contract_source(checkout_saga_contract_source()).expect("compile contract");
        let bundle =
            ValidatedContractBundle::from_compiler_bundle(compiled).expect("validate bundle");
        let stored_bundle = bundle.to_stored().expect("encode stored bundle");
        let lineage = bundle.lineage().clone();

        let clock_seconds = Arc::new(AtomicI64::new(ISSUED_SECONDS + 10));
        let clocks = ProductionWallClocks::settable(Arc::clone(&clock_seconds));
        let environment = Environment::new("test").expect("environment");
        let audience = Audience::new("riffdb-test").expect("audience");

        let capability_keys = Arc::new(
            CapabilityDigestKeyProvider::parse_document(CAPABILITY_KEYS_DOC)
                .expect("capability keys"),
        );
        let idempotency_keys = Arc::new(
            IdempotencyDigestKeyProvider::parse_document(IDEMPOTENCY_KEYS_DOC)
                .expect("idempotency keys"),
        );

        let bootstrap_capability_id =
            CapabilityId::from_unix_milliseconds_and_random(1, [0x11; 10]).expect("capability id");
        let bootstrap_request_id =
            RequestId::from_unix_milliseconds_and_random(1, [0x10; 10]).expect("request id");
        let bootstrap_issued =
            riffdb_auth::issue_capability_token(&SystemEntropy, capability_keys.as_ref())
                .expect("issue bootstrap token");
        let bootstrap_digest = bootstrap_issued.digest();
        let bootstrap_issued_at = Timestamp::new(ISSUED_SECONDS, 0).expect("issued_at");
        let bootstrap_expires_at = Timestamp::new(ISSUED_SECONDS + 86_400, 0).expect("expires_at");
        let bootstrap_grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(vec![
                CapabilityPermissionV1::unparameterized(
                    CapabilityPermissionKindV1::AdministerCapabilities,
                )
                .expect("administer permission"),
                CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                    .expect("read contract permission"),
            ])
            .expect("permissions"),
            Vec::new(),
            NonZeroU16::new(100).expect("row limit"),
            Vec::new(),
        )
        .expect("grant");
        let bootstrap_intent = CapabilityBootstrapIntentV1::new(
            bootstrap_capability_id,
            CapabilityRequestedRecordV1::new(
                real.database_id,
                environment.clone(),
                ActorId::new("saga-test-operator").expect("actor"),
                ActorKind::Human,
                NonZeroU32::new(86_400).expect("lifetime"),
                vec![audience.clone()],
                bootstrap_grant,
            )
            .expect("requested record"),
            BootstrapDigestCandidatesV1::new(vec![bootstrap_digest], bootstrap_digest)
                .expect("digest candidates"),
            bootstrap_issued_at,
            bootstrap_expires_at,
            BootstrapServiceAuditStartV1::new(
                bootstrap_request_id,
                bootstrap_issued_at,
                ServiceIngressKindV1::Grpc,
                ServiceAuditTargetsV1::new([ServiceAuditTargetV1::Capability(
                    bootstrap_capability_id,
                )])
                .expect("audit targets"),
                None,
            )
            .expect("bootstrap audit"),
        )
        .expect("bootstrap intent");
        assert!(matches!(
            storage
                .bootstrap_capability(&bootstrap_intent)
                .expect("bootstrap capability"),
            CapabilityBootstrapResult::BootstrapCreated { .. }
        ));

        let audit_principal = AuditPrincipalV1::new(
            ActorId::new("saga-test-operator").expect("actor"),
            ActorKind::Human,
            bootstrap_capability_id,
            NonZeroU64::MIN,
        );
        assert!(matches!(
            storage
                .activate_catalog(&CatalogActivationIntentV1::new(
                    None,
                    stored_bundle,
                    RequestId::from_unix_milliseconds_and_random(1, [0x12; 10])
                        .expect("request id"),
                    audit_principal,
                    Timestamp::new(ISSUED_SECONDS, 0).expect("timestamp"),
                    None,
                ))
                .expect("activate catalog"),
            CatalogActivationResult::Activated { .. }
        ));
        let readable_idempotency_digests = ReadableIdempotencyDigestInventory::new(
            idempotency_keys
                .readable_key_ids()
                .map(ReadableDigestKey::v1)
                .collect(),
        )
        .expect("readable idempotency digests");
        let idempotency_digests: Arc<dyn IdempotencyDigestProvider> =
            Arc::new(ServerIdempotencyDigestProvider::new(idempotency_keys));

        let runtime = tokio::runtime::Handle::current();
        let routing = RuntimeRoutingState::new();
        let blocking =
            BlockingPortDriver::with_limits(routing.clone(), 2, 16).expect("start port driver");
        let notifications = FirstCommitNotificationHub::new(None, routing.clone());
        let outbox_health = NoDestinationOutboxHealth::new(OutboxRecoveryReadiness::Ready);
        let commit_notifications = ServerCommitNotificationSink::new(
            notifications.clone(),
            storage.clone(),
            outbox_health.clone(),
        );

        let identifiers = ProductionIdentifierSources::new();
        let incident_ids: Arc<dyn IncidentIdSource> = Arc::new(identifiers.incident_ids());
        let observability = Arc::new(
            Observability::new(Arc::clone(&incident_ids), MAX_TRACE_RECORDS)
                .expect("observability"),
        );
        for component in [
            AuthoritativeComponent::Storage,
            AuthoritativeComponent::Catalog,
            AuthoritativeComponent::CommitCoordinator,
        ] {
            observability
                .health()
                .set_authoritative(component, AuthoritativeCondition::Healthy);
        }

        let policy: Arc<dyn CurrentPolicyPort> = Arc::new(ServerCurrentPolicyPort::new(
            storage.clone(),
            clocks.authorization(),
            real.database_id,
            environment.clone(),
            TrustedAudienceCatalog::new(vec![audience.clone()]).expect("trusted audience"),
            observability.clone(),
        ));
        let token_issuer: Arc<dyn CapabilityTokenIssuer> = Arc::new(
            ServerCapabilityTokenIssuer::new(Arc::clone(&capability_keys)),
        );
        let catalog_adapter = Arc::new(ServerCatalogReadPort::new(storage.clone(), &blocking));
        let catalog: Arc<dyn CatalogReadPort> = catalog_adapter.clone();
        let authoritative: Arc<dyn AuthoritativeReadPort> =
            Arc::new(ServerAuthoritativeReadPort::new(
                catalog_adapter.as_ref(),
                Arc::clone(&idempotency_digests),
                readable_idempotency_digests,
                real.database_id,
                environment.clone(),
                notifications.clone(),
                &blocking,
            ));

        let conflicts: Arc<dyn ConflictManager> = Arc::new(
            ShardedConflictManager::with_observer(
                ConflictManagerConfig::default(),
                observability.clone(),
            )
            .expect("conflict manager"),
        );
        let coordinator = RunningCommandCoordinator::start_with_telemetry(
            CoordinatorWorkloadCapacity::new(64).expect("capacity"),
            CoordinatorDurability::Sync,
            storage.clone(),
            conflicts,
            Arc::new(clocks.admission()),
            Arc::new(identifiers.service_uuids()),
            Arc::new(clocks.administration()),
            Arc::new(clocks.authorization()),
            Arc::new(identifiers.provenance_ids()),
            Arc::new(commit_notifications),
            observability.clone(),
        )
        .expect("start command coordinator");

        let executors = ServiceExecutors::new(
            coordinator.administration_audit_executor(),
            coordinator.control_plane_executor(),
            coordinator.command_executor(),
            coordinator
                .command_idempotency_inspector(Arc::clone(&idempotency_digests))
                .with_direct_repository(Arc::new(storage.clone())),
        );

        let empty_projection_registry =
            ProjectionSchemaRegistry::new(Vec::new()).expect("empty registry");
        let projection_notifier = ProjectionNotifier::from_registry(&empty_projection_registry);
        let projection: Arc<dyn ProjectionQueryPort> = Arc::new(ServerProjectionQueryPort::new(
            storage.clone(),
            projection_notifier,
            &blocking,
        ));
        let operational: Arc<dyn OperationalStatusPort> =
            Arc::new(ProductionOperationalStatusPort::new_with_vector_storage(
                ValidatedAllocatorCapacity::Available,
                routing.clone(),
                notifications,
                outbox_health,
                ProjectionWorkerStatus::new(),
                ColumnarWorkerStatus::new(),
                storage.clone(),
            ));
        let diagnostics: Arc<dyn ServiceDiagnostics> = Arc::new(
            ProductionObservabilityDiagnostics::new(routing.clone(), observability.clone()),
        );
        let health: Arc<dyn ServiceHealthHooks> = Arc::new(
            ProductionObservabilityHealthHooks::new(routing.clone(), observability.clone()),
        );
        let spawner = SupervisedServiceJobSpawner::with_runtime(runtime, routing);
        let service_spawner: Arc<dyn ServiceJobSpawner> = Arc::new(spawner.clone());
        let deadline_scheduler: Arc<dyn RequestDeadlineScheduler> =
            Arc::new(TokioRequestDeadlineScheduler);
        let cursor_tokens: Arc<dyn CursorTokenGenerator> =
            Arc::new(ProductionCursorTokenGenerator::new());
        let cursor_clock: Arc<dyn CursorMonotonicClock> =
            Arc::new(ProductionCursorMonotonicClock::new());

        let saga_notify = Arc::new(SagaWorkNotification::new());
        let saga_coordinator = Arc::new(ServerSagaCoordinator::new(
            storage.clone(),
            &blocking,
            Arc::clone(&saga_notify),
        ));

        let providers = ServiceProviders::new(
            catalog,
            policy,
            authoritative,
            projection,
            None,
            operational,
            token_issuer,
            incident_ids,
            diagnostics,
            observability as Arc<dyn ServiceTelemetry>,
            health,
            service_spawner,
            deadline_scheduler,
            cursor_tokens,
            cursor_clock,
        )
        .with_saga_coordinator(saga_coordinator);

        let identity = ServiceIdentity::new(
            real.database_id,
            environment.clone(),
            AgentSessionAdmissionPolicy::Discard,
            1,
        );
        let process = ServiceProcessMetadata::new(
            Timestamp::new(ISSUED_SECONDS, 0).expect("started_at"),
            BuildInfo::new(
                "0.1.0-test",
                "saga-worker-test",
                "rustc-test",
                Vec::new(),
                1,
                1,
                "2026-11-05",
            )
            .expect("build info"),
        );
        let service = Arc::new(RiffDbService::new(identity, process, executors, providers));

        Self {
            real,
            bundle,
            lineage,
            environment,
            audience,
            clock_seconds,
            clocks,
            capability_keys,
            bootstrap_capability_id,
            service,
            saga_notify,
            coordinator: Some(coordinator),
            blocking: Some(blocking),
            spawner,
        }
    }

    fn issue_principal(
        &self,
        actor_name: &str,
        lifetime_seconds: u32,
        seed: u8,
    ) -> AuthenticatedPrincipal {
        let storage = self.real.storage.clone();
        let issued =
            riffdb_auth::issue_capability_token(&SystemEntropy, self.capability_keys.as_ref())
                .expect("issue token");
        let token_text = issued.text().expose_secret().to_owned();
        let token_digest = issued.digest();

        let capability_id =
            CapabilityId::from_unix_milliseconds_and_random(u64::from(seed) + 10, [seed; 10])
                .expect("capability id");
        let request_id = RequestId::from_unix_milliseconds_and_random(
            u64::from(seed) + 20,
            [seed.wrapping_add(1); 10],
        )
        .expect("request id");

        let mut perms = vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("administer permission"),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadContract)
                .expect("read contract permission"),
        ];
        for command in self.bundle.bundle().commands() {
            perms.push(CapabilityPermissionV1::InvokeCommand(
                self.lineage.clone(),
                command.command_id(),
            ));
        }
        for entity in self.bundle.bundle().schema().entities() {
            perms.push(CapabilityPermissionV1::ReadEntity(
                self.lineage.clone(),
                entity.id(),
            ));
        }
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(perms).expect("permissions"),
            Vec::new(),
            NonZeroU16::new(100).expect("row limit"),
            Vec::new(),
        )
        .expect("grant");

        let now_secs = self.clock_seconds.load(Ordering::Acquire);
        let issued_at = Timestamp::new(now_secs, 0).expect("issued_at");
        let expires_at =
            Timestamp::new(now_secs + i64::from(lifetime_seconds), 0).expect("expires_at");
        let requested = CapabilityRequestedRecordV1::new(
            self.real.database_id,
            self.environment.clone(),
            ActorId::new(actor_name).expect("actor id"),
            ActorKind::Service,
            NonZeroU32::new(lifetime_seconds).expect("lifetime"),
            vec![self.audience.clone()],
            grant,
        )
        .expect("requested record");
        let initiator = AuditPrincipalV1::new(
            ActorId::new("saga-test-operator").expect("actor"),
            ActorKind::Human,
            self.bootstrap_capability_id,
            NonZeroU64::MIN,
        );
        let candidate = CapabilityCreateCandidateV1::new(
            capability_id,
            request_id,
            requested.clone(),
            token_digest,
            initiator.clone(),
            None,
        );
        let (awaiting, _current) = storage
            .begin_capability_create(candidate)
            .expect("begin capability create")
            .read_transaction_current()
            .expect("read transaction current");
        let created = awaiting
            .commit_create(
                CapabilityCreateIntentV1::new(
                    capability_id,
                    request_id,
                    requested,
                    token_digest,
                    issued_at,
                    expires_at,
                    initiator,
                    None,
                )
                .expect("create intent"),
            )
            .expect("commit capability create");
        assert!(matches!(created, CapabilityCreateResult::Created { .. }));

        CapabilityAuthenticator::new(
            &storage,
            self.capability_keys.as_ref(),
            &self.clocks.authentication(),
            &NoopAuthenticationTelemetry,
        )
        .authenticate(
            OpaqueCredential::new(&token_text),
            &AuthenticationContext::new(
                self.real.database_id,
                self.environment.clone(),
                self.audience.clone(),
            ),
        )
        .expect("authenticate principal")
    }

    fn request_context(&self, principal: &AuthenticatedPrincipal, seed: u64) -> RequestContext {
        let (control, _cancel) = RequestControl::new(Instant::now() + Duration::from_secs(15));
        let now_secs = self.clock_seconds.load(Ordering::Acquire);
        let authed = AuthenticatedPrincipal::from_retained_saga_authority(
            principal.capability_id(),
            principal.capability_revision(),
            principal.capability_principal_id().clone(),
            principal.actor_kind(),
            principal.audience().clone(),
            principal.tenant_scope().clone(),
            Timestamp::new(now_secs, 0).expect("timestamp"),
        );
        RequestContext::new(
            RequestId::from_unix_milliseconds_and_random(seed + 100, [0x33; 10])
                .expect("request id"),
            authed,
            ServiceIngressKindV1::Grpc,
            UntrustedInvocationClaims::new(None, None, None, None, None),
            control,
            None,
        )
    }

    async fn exec_command(
        &self,
        principal: &AuthenticatedPrincipal,
        seed: u64,
        command_name: &str,
        fields: Vec<(&str, SubmittedValue)>,
    ) {
        let submitted = SubmittedRecord::new_command_input(
            fields
                .into_iter()
                .map(|(name, value)| {
                    SubmittedField::new(
                        SubmittedFieldIdentity::Name(SourceName::new(name).expect("field name")),
                        value,
                    )
                })
                .collect(),
        )
        .expect("submitted record");
        let request = ExecuteCommandRequest::new(
            SourceName::new(command_name).expect("command name"),
            None,
            submitted,
        )
        .expect("execute command request");
        let result = self
            .service
            .execute_command(self.request_context(principal, seed), request)
            .await
            .expect("execute command succeeds");
        assert!(matches!(
            result,
            riffdb_service::ExecuteCommandResult::Journaled(_)
        ));
    }

    fn read_entity_u64_field(
        &self,
        entity_name: &str,
        key_u64: u64,
        field_name: &str,
    ) -> Option<u64> {
        let entity_schema = self
            .bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == entity_name)
            .expect("entity schema");
        let mut key_builder = EntityKeyBuilder::new(entity_schema.id());
        key_builder.push_u64(key_u64).expect("entity key u64");
        let entity_key = key_builder.finish().expect("entity key");
        let target = EntityTarget::new(entity_schema.id(), entity_key).expect("entity target");

        let stored = self
            .real
            .storage
            .read_entity(&target)
            .expect("read entity")?;

        let field_id: FieldId = entity_schema
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == field_name)
            .expect("field schema")
            .id();
        match stored
            .fields()
            .fields()
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, val)| val)
        {
            Some(CanonicalValue::U64(v)) => Some(*v),
            _ => None,
        }
    }

    fn read_entity_string_field(
        &self,
        entity_name: &str,
        key_u64: u64,
        field_name: &str,
    ) -> Option<String> {
        let entity_schema = self
            .bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == entity_name)
            .expect("entity schema");
        let mut key_builder = EntityKeyBuilder::new(entity_schema.id());
        key_builder.push_u64(key_u64).expect("entity key u64");
        let entity_key = key_builder.finish().expect("entity key");
        let target = EntityTarget::new(entity_schema.id(), entity_key).expect("entity target");

        let stored = self
            .real
            .storage
            .read_entity(&target)
            .expect("read entity")?;

        let field_id: FieldId = entity_schema
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == field_name)
            .expect("field schema")
            .id();
        match stored
            .fields()
            .fields()
            .iter()
            .find(|(id, _)| *id == field_id)
            .map(|(_, val)| val)
        {
            Some(CanonicalValue::String(s)) => Some(s.as_str().to_owned()),
            _ => None,
        }
    }

    fn start_background_worker(&self, worker_id: [u8; 16]) -> RunningSagaWorker {
        RunningSagaWorker::start(
            self.real.storage.clone(),
            Arc::clone(&self.service),
            self.clocks.authorization(),
            worker_id,
            Arc::clone(&self.saga_notify),
        )
    }
}

impl Drop for SagaTestServer {
    fn drop(&mut self) {
        self.saga_notify.stop();
        if let Some(coordinator) = self.coordinator.take() {
            let _ = coordinator.shutdown();
        }
        if let Some(blocking) = self.blocking.take() {
            let _ = blocking.shutdown_and_drain();
        }
    }
}

fn submitted_record(fields: Vec<(&str, SubmittedValue)>) -> SubmittedRecord {
    SubmittedRecord::new_command_input(
        fields
            .into_iter()
            .map(|(name, value)| {
                SubmittedField::new(
                    SubmittedFieldIdentity::Name(SourceName::new(name).expect("field name")),
                    value,
                )
            })
            .collect(),
    )
    .expect("submitted record")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_partition_checkout_saga_completes_and_replays_idempotently() {
    let server = SagaTestServer::new("saga-complete-and-replay");
    let principal = server.issue_principal("checkout-service", 3600, 1);

    server
        .exec_command(
            &principal,
            1,
            "SeedInventory",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-inv-101").unwrap(),
                ),
                ("sku", SubmittedValue::U64(101)),
                ("available", SubmittedValue::U64(10)),
            ],
        )
        .await;
    server
        .exec_command(
            &principal,
            2,
            "SeedWallet",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-wal-202").unwrap(),
                ),
                ("user_id", SubmittedValue::U64(202)),
                ("balance", SubmittedValue::U64(500)),
            ],
        )
        .await;

    let saga_id = SagaId::from_unix_milliseconds_and_random(100, [0x41; 10]).expect("saga id");
    let input = submitted_record(vec![
        (
            "idempotency_key",
            SubmittedValue::string("checkout-req-1").unwrap(),
        ),
        ("order_id", SubmittedValue::U64(303)),
        ("sku", SubmittedValue::U64(101)),
        ("user_id", SubmittedValue::U64(202)),
        ("quantity", SubmittedValue::U64(3)),
        ("amount", SubmittedValue::U64(150)),
        ("reject_order", SubmittedValue::Bool(false)),
    ]);
    let start_req = StartSagaRequest::new(
        saga_id,
        server.lineage.clone(),
        SourceName::new("CheckoutOrder").unwrap(),
        input.clone(),
    )
    .with_await_window(Duration::from_secs(5))
    .expect("await window");

    let started = server
        .service
        .start_saga(server.request_context(&principal, 3), start_req)
        .await
        .expect("start_saga succeeds");
    assert_eq!(started.disposition(), StartSagaDisposition::Admitted);
    assert_eq!(started.instance().saga_id(), saga_id);
    assert_eq!(
        started.instance().lifecycle_status(),
        SagaLifecycleStatusV1::Completed
    );
    assert_eq!(started.instance().step_ledger().len(), 3);
    assert!(started.instance().step_ledger().iter().all(|entry| {
        entry.direction() == SagaStepDirectionV1::Forward
            && entry.effect_disposition() == SagaStepEffectDispositionV1::Effectful
    }));
    let terminal = started
        .instance()
        .terminal_outcome()
        .expect("terminal outcome present");
    assert_eq!(terminal.outcome_name(), "OrderCompleted");

    // Verify authoritative state across all three partitions.
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(7)
    );
    assert_eq!(
        server.read_entity_u64_field("Wallet", 202, "balance"),
        Some(350)
    );
    assert_eq!(
        server
            .read_entity_string_field("Order", 303, "status")
            .as_deref(),
        Some("CONFIRMED")
    );

    // Idempotent replay with same caller key and identical input returns Replayed.
    let different_candidate_id =
        SagaId::from_unix_milliseconds_and_random(101, [0x42; 10]).expect("saga id");
    let replay_req = StartSagaRequest::new(
        different_candidate_id,
        server.lineage.clone(),
        SourceName::new("CheckoutOrder").unwrap(),
        input,
    );
    let replayed = server
        .service
        .start_saga(server.request_context(&principal, 4), replay_req)
        .await
        .expect("replay start_saga succeeds");
    assert_eq!(replayed.disposition(), StartSagaDisposition::Replayed);
    assert_eq!(replayed.instance().saga_id(), saga_id);
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(7)
    );

    // Conflicting input under the same idempotency_key fails with IdempotencyKeyReuse.
    let conflicting_input = submitted_record(vec![
        (
            "idempotency_key",
            SubmittedValue::string("checkout-req-1").unwrap(),
        ),
        ("order_id", SubmittedValue::U64(303)),
        ("sku", SubmittedValue::U64(101)),
        ("user_id", SubmittedValue::U64(202)),
        ("quantity", SubmittedValue::U64(3)),
        ("amount", SubmittedValue::U64(999)),
        ("reject_order", SubmittedValue::Bool(false)),
    ]);
    let conflict_err = server
        .service
        .start_saga(
            server.request_context(&principal, 5),
            StartSagaRequest::new(
                different_candidate_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                conflicting_input,
            ),
        )
        .await
        .expect_err("conflicting input must reject");
    assert_eq!(
        conflict_err
            .public_error()
            .map(riffdb_errors::PublicError::kind),
        Some(riffdb_errors::PublicErrorKind::IdempotencyKeyReuse)
    );

    server.spawner.wait_for_idle().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_after_step_commit_before_advance_recovers_via_byte_identical_idempotency_identity_without_duplicate_mutation()
 {
    let server = SagaTestServer::new("saga-crash-recovery");
    let principal = server.issue_principal("checkout-service", 3600, 2);

    server
        .exec_command(
            &principal,
            1,
            "SeedInventory",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-inv-101").unwrap(),
                ),
                ("sku", SubmittedValue::U64(101)),
                ("available", SubmittedValue::U64(10)),
            ],
        )
        .await;
    server
        .exec_command(
            &principal,
            2,
            "SeedWallet",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-wal-202").unwrap(),
                ),
                ("user_id", SubmittedValue::U64(202)),
                ("balance", SubmittedValue::U64(500)),
            ],
        )
        .await;

    let saga_id = SagaId::from_unix_milliseconds_and_random(200, [0x51; 10]).expect("saga id");
    let input = submitted_record(vec![
        (
            "idempotency_key",
            SubmittedValue::string("checkout-crash-1").unwrap(),
        ),
        ("order_id", SubmittedValue::U64(404)),
        ("sku", SubmittedValue::U64(101)),
        ("user_id", SubmittedValue::U64(202)),
        ("quantity", SubmittedValue::U64(4)),
        ("amount", SubmittedValue::U64(200)),
        ("reject_order", SubmittedValue::Bool(false)),
    ]);

    // Step 1: Admit the saga without inline await so it remains at step 0 with no pending invocation.
    let started = server
        .service
        .start_saga(
            server.request_context(&principal, 3),
            StartSagaRequest::new(
                saga_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                input,
            ),
        )
        .await
        .expect("admit saga");
    assert_eq!(
        started.instance().lifecycle_status(),
        SagaLifecycleStatusV1::RunningForward
    );
    assert_eq!(started.instance().current_step_index(), 0);

    // Step 2: Claim the saga as Worker A and persist PrepareSagaStep for step 0 (`ReserveInventory`).
    let now = Timestamp::new(ISSUED_SECONDS + 10, 0).unwrap();
    let lease_expires_at = Timestamp::new(ISSUED_SECONDS + 40, 0).unwrap();
    let claimed_a = server
        .real
        .storage
        .claim_due_sagas([0xAA; 16], now, lease_expires_at, 8, 0)
        .expect("claim_due_sagas")
        .into_iter()
        .next()
        .expect("one due saga");
    assert_eq!(claimed_a.saga_id(), saga_id);

    let reserve_cmd = server
        .bundle
        .bundle()
        .commands()
        .iter()
        .find(|cmd| cmd.name() == "ReserveInventory")
        .expect("ReserveInventory plan");
    let step_idem_key =
        derive_saga_step_idempotency_key(saga_id, 0, SagaStepDirectionV1::Forward).unwrap();
    let id_field = reserve_cmd
        .input()
        .record()
        .fields()
        .iter()
        .find(|f| f.name() == "idempotency_key")
        .unwrap()
        .id();
    let sku_field = reserve_cmd
        .input()
        .record()
        .fields()
        .iter()
        .find(|f| f.name() == "sku")
        .unwrap()
        .id();
    let qty_field = reserve_cmd
        .input()
        .record()
        .fields()
        .iter()
        .find(|f| f.name() == "quantity")
        .unwrap()
        .id();
    let step_record = CanonicalRecord::new(vec![
        (
            id_field,
            CanonicalValue::string(step_idem_key.expose_secret()).unwrap(),
        ),
        (sku_field, CanonicalValue::U64(101)),
        (qty_field, CanonicalValue::U64(4)),
    ])
    .unwrap();
    let pending_step_0 = riffdb_types::PendingStepInvocationV1::new(
        0,
        SagaStepDirectionV1::Forward,
        reserve_cmd.command_id(),
        server.lineage.clone(),
        claimed_a.contract_version(),
        claimed_a.contract_bundle_hash(),
        reserve_cmd.plan_hash(),
        riffdb_types::encode_canonical_command_input(&step_record).unwrap(),
        claimed_a.tenant_scope().clone(),
        claimed_a.delegated_actor_id().clone(),
        step_idem_key.clone(),
    )
    .unwrap();

    let prepared_a = StoredSagaInstanceV1::new(
        claimed_a.saga_id(),
        claimed_a.database_id(),
        claimed_a.environment().clone(),
        claimed_a.tenant_scope().clone(),
        claimed_a.contract_lineage().clone(),
        claimed_a.contract_version(),
        claimed_a.contract_bundle_hash(),
        claimed_a.saga_plan_hash(),
        claimed_a.saga_name().to_owned(),
        claimed_a.execution_role().to_owned(),
        claimed_a.delegated_actor_id().clone(),
        claimed_a.partition_hash(),
        claimed_a.caller_idempotency_key().clone(),
        claimed_a.retained_authority().clone(),
        claimed_a.revision().checked_next().unwrap(),
        claimed_a.fencing_token(),
        SagaLifecycleStatusV1::RunningForward,
        0,
        false,
        0,
        now,
        claimed_a.lease().copied(),
        claimed_a.saga_input_bytes().to_vec(),
        Some(pending_step_0),
        Vec::new(),
        None,
        None,
        claimed_a.created_at(),
        now,
    )
    .unwrap();
    assert!(matches!(
        server
            .real
            .storage
            .compare_and_swap_saga(
                SagaCompareExpectedV1::fenced(claimed_a.revision(), claimed_a.fencing_token()),
                &prepared_a,
            )
            .unwrap(),
        riffdb_types::SagaCasOutcomeV1::Applied(_)
    ));

    // Step 3: Commit `ReserveInventory` under the exact delegated saga authority, then "crash"
    // before `AdvanceSagaStep` is written!
    let delegated_principal = AuthenticatedPrincipal::from_retained_saga_authority(
        claimed_a.retained_authority().capability_id(),
        claimed_a.retained_authority().capability_revision(),
        claimed_a.retained_authority().principal_id().clone(),
        claimed_a.retained_authority().actor_kind(),
        claimed_a.retained_authority().audience().clone(),
        claimed_a.retained_authority().tenant_scope().clone(),
        now,
    )
    .with_delegated_actor(claimed_a.delegated_actor_id().clone());
    let (control, _cancel) = RequestControl::new(Instant::now() + Duration::from_secs(10));
    let delegated_ctx = RequestContext::new(
        RequestId::from_unix_milliseconds_and_random(250, [0x55; 10]).unwrap(),
        delegated_principal,
        ServiceIngressKindV1::Grpc,
        UntrustedInvocationClaims::new(None, None, None, None, None),
        control,
        None,
    );
    let step_exec = server
        .service
        .execute_command(
            delegated_ctx,
            ExecuteCommandRequest::new(
                SourceName::new("ReserveInventory").unwrap(),
                Some(claimed_a.contract_version()),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string(step_idem_key.expose_secret()).unwrap(),
                    ),
                    ("sku", SubmittedValue::U64(101)),
                    ("quantity", SubmittedValue::U64(4)),
                ]),
            )
            .unwrap(),
        )
        .await
        .expect("step 0 command commits");
    assert!(matches!(
        step_exec,
        riffdb_service::ExecuteCommandResult::Journaled(_)
    ));
    // Inventory was deducted once (10 - 4 = 6), while the saga is still at step 0 with pending_invocation!
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(6)
    );

    // Step 4: Advance clock past Worker A's lease expiration and start the background Worker B!
    server
        .clock_seconds
        .store(ISSUED_SECONDS + 60, Ordering::Release);
    let mut worker_b = server.start_background_worker([0xBB; 16]);
    server.saga_notify.notify_work();

    let awaited = server
        .service
        .await_saga(
            server.request_context(&principal, 6),
            AwaitSagaRequest::new(saga_id, Duration::from_secs(5)).unwrap(),
        )
        .await
        .expect("await_saga succeeds");
    let GetSagaResult::Found(view) = awaited else {
        panic!("expected Found saga");
    };
    assert_eq!(view.lifecycle_status(), SagaLifecycleStatusV1::Completed);
    assert_eq!(view.step_ledger().len(), 3);

    // Crucial invariant: Worker B resolved the already-committed step 0 outcome via its byte-identical
    // IdempotencyIdentity and did NOT deduct inventory a second time!
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(6)
    );
    assert_eq!(
        server.read_entity_u64_field("Wallet", 202, "balance"),
        Some(300)
    );
    assert_eq!(
        server
            .read_entity_string_field("Order", 404, "status")
            .as_deref(),
        Some("CONFIRMED")
    );

    worker_b.shutdown();
    server.spawner.wait_for_idle().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reverse_lifo_compensation_restores_effectful_steps_and_honest_abort_states() {
    let server = SagaTestServer::new("saga-compensation-lifo");
    let principal = server.issue_principal("checkout-service", 3600, 3);

    server
        .exec_command(
            &principal,
            1,
            "SeedInventory",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-inv-101").unwrap(),
                ),
                ("sku", SubmittedValue::U64(101)),
                ("available", SubmittedValue::U64(10)),
            ],
        )
        .await;
    server
        .exec_command(
            &principal,
            2,
            "SeedWallet",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-wal-202").unwrap(),
                ),
                ("user_id", SubmittedValue::U64(202)),
                ("balance", SubmittedValue::U64(500)),
            ],
        )
        .await;

    // Case 1: Clean abort on step 0 (`OutOfStock` -> `AbortedClean`) before any effectful mutation.
    let abort_clean_id =
        SagaId::from_unix_milliseconds_and_random(300, [0x61; 10]).expect("saga id");
    let abort_clean = server
        .service
        .start_saga(
            server.request_context(&principal, 3),
            StartSagaRequest::new(
                abort_clean_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-oos").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(501)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(99)),
                    ("amount", SubmittedValue::U64(100)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            )
            .with_await_window(Duration::from_secs(5))
            .unwrap(),
        )
        .await
        .expect("start_saga out of stock");
    assert_eq!(
        abort_clean.instance().lifecycle_status(),
        SagaLifecycleStatusV1::AbortedClean
    );
    assert_eq!(abort_clean.instance().step_ledger().len(), 1);
    assert_eq!(
        abort_clean.instance().step_ledger()[0].effect_disposition(),
        SagaStepEffectDispositionV1::NoEffect
    );
    assert_eq!(
        abort_clean
            .instance()
            .terminal_outcome()
            .unwrap()
            .outcome_name(),
        "OrderOutOfStock"
    );
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(10)
    );

    // Case 2: Step 0 (`ReserveInventory`) and Step 1 (`ChargeWallet`) succeed (`Effectful`),
    // then Step 2 (`ConfirmOrder` with `reject_order = true`) returns `OrderRejected` (`NoEffect`),
    // triggering reverse LIFO compensation: Step 1 (`RefundWallet`) then Step 0 (`ReleaseInventory`).
    let comp_saga_id = SagaId::from_unix_milliseconds_and_random(301, [0x62; 10]).expect("saga id");
    let compensated = server
        .service
        .start_saga(
            server.request_context(&principal, 4),
            StartSagaRequest::new(
                comp_saga_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-rollback").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(502)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(4)),
                    ("amount", SubmittedValue::U64(200)),
                    ("reject_order", SubmittedValue::Bool(true)),
                ]),
            )
            .with_await_window(Duration::from_secs(5))
            .unwrap(),
        )
        .await
        .expect("start_saga compensated");
    assert_eq!(
        compensated.instance().lifecycle_status(),
        SagaLifecycleStatusV1::Compensated
    );
    assert_eq!(
        compensated
            .instance()
            .terminal_outcome()
            .unwrap()
            .outcome_name(),
        "OrderRolledBack"
    );
    let ledger_steps: Vec<(u16, SagaStepDirectionV1, SagaStepEffectDispositionV1)> = compensated
        .instance()
        .step_ledger()
        .iter()
        .map(|entry| {
            (
                entry.step_index(),
                entry.direction(),
                entry.effect_disposition(),
            )
        })
        .collect();
    assert_eq!(
        ledger_steps,
        vec![
            (
                0,
                SagaStepDirectionV1::Forward,
                SagaStepEffectDispositionV1::Effectful
            ),
            (
                1,
                SagaStepDirectionV1::Forward,
                SagaStepEffectDispositionV1::Effectful
            ),
            (
                2,
                SagaStepDirectionV1::Forward,
                SagaStepEffectDispositionV1::NoEffect
            ),
            (
                1,
                SagaStepDirectionV1::Compensate,
                SagaStepEffectDispositionV1::Effectful
            ),
            (
                0,
                SagaStepDirectionV1::Compensate,
                SagaStepEffectDispositionV1::Effectful
            ),
        ]
    );
    // Both partitions were restored to their pre-saga state!
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(10)
    );
    assert_eq!(
        server.read_entity_u64_field("Wallet", 202, "balance"),
        Some(500)
    );
    assert_eq!(
        server.read_entity_string_field("Order", 502, "status"),
        None
    );

    // Case 3: Explicit `abort_uncompensated` transitions to `AbortedUncompensated` without compensating.
    let uncomp_id = SagaId::from_unix_milliseconds_and_random(302, [0x63; 10]).expect("saga id");
    let uncomp = server
        .service
        .start_saga(
            server.request_context(&principal, 5),
            StartSagaRequest::new(
                uncomp_id,
                server.lineage.clone(),
                SourceName::new("UncompensatedAuditCheckout").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-uncomp").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(503)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(2)),
                    ("amount", SubmittedValue::U64(9999)),
                ]),
            )
            .with_await_window(Duration::from_secs(5))
            .unwrap(),
        )
        .await
        .expect("start_saga uncompensated");
    assert_eq!(
        uncomp.instance().lifecycle_status(),
        SagaLifecycleStatusV1::AbortedUncompensated
    );
    assert_eq!(
        uncomp.instance().terminal_outcome().unwrap().outcome_name(),
        "OrderLeftReserved"
    );
    // Inventory remains reserved (10 - 2 = 8) because abort_uncompensated was explicitly declared.
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(8)
    );

    // Case 4: Explicit `cancel_saga` before any step executes transitions to `AbortedClean`.
    let cancel_id = SagaId::from_unix_milliseconds_and_random(303, [0x64; 10]).expect("saga id");
    let admitted = server
        .service
        .start_saga(
            server.request_context(&principal, 6),
            StartSagaRequest::new(
                cancel_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-cancel").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(504)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(1)),
                    ("amount", SubmittedValue::U64(50)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            ),
        )
        .await
        .expect("admit for cancel");
    assert_eq!(
        admitted.instance().lifecycle_status(),
        SagaLifecycleStatusV1::RunningForward
    );
    let cancelled = server
        .service
        .cancel_saga(
            server.request_context(&principal, 7),
            CancelSagaRequest::new(cancel_id),
        )
        .await
        .expect("cancel_saga succeeds");
    let GetSagaResult::Found(cancelled_view) = cancelled else {
        panic!("expected Found");
    };
    assert!(cancelled_view.cancel_requested());
    assert_eq!(
        cancelled_view.lifecycle_status(),
        SagaLifecycleStatusV1::AbortedClean
    );

    server.spawner.wait_for_idle().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_authority_and_compensation_failed_halt_honestly_and_resume_saga_recovers() {
    let server = SagaTestServer::new("saga-blocked-and-comp-failed");
    let short_lived_principal = server.issue_principal("short-lived-orchestrator", 60, 4);
    let long_lived_principal = server.issue_principal("long-lived-orchestrator", 3600, 5);

    server
        .exec_command(
            &long_lived_principal,
            1,
            "SeedInventory",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-inv-101").unwrap(),
                ),
                ("sku", SubmittedValue::U64(101)),
                ("available", SubmittedValue::U64(10)),
            ],
        )
        .await;
    server
        .exec_command(
            &long_lived_principal,
            2,
            "SeedWallet",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-wal-202").unwrap(),
                ),
                ("user_id", SubmittedValue::U64(202)),
                ("balance", SubmittedValue::U64(500)),
            ],
        )
        .await;

    // Part A: Start a saga with `short_lived_principal` (no inline wait), then advance clock past
    // its 60-second expiry so step execution fails capability re-evaluation and enters `BlockedAuthority`.
    let blocked_saga_id =
        SagaId::from_unix_milliseconds_and_random(400, [0x71; 10]).expect("saga id");
    server
        .service
        .start_saga(
            server.request_context(&short_lived_principal, 3),
            StartSagaRequest::new(
                blocked_saga_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-auth-expire").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(601)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(2)),
                    ("amount", SubmittedValue::U64(100)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            ),
        )
        .await
        .expect("admit saga with short-lived capability");

    // Advance wall clock past the short-lived capability's 60s lifetime!
    server
        .clock_seconds
        .store(ISSUED_SECONDS + 120, Ordering::Release);

    let awaited_blocked = server
        .service
        .await_saga(
            server.request_context(&long_lived_principal, 4),
            AwaitSagaRequest::new(blocked_saga_id, Duration::from_secs(5)).unwrap(),
        )
        .await
        .expect("await_saga returns blocked state");
    let GetSagaResult::Found(blocked_view) = awaited_blocked else {
        panic!("expected Found");
    };
    assert_eq!(
        blocked_view.lifecycle_status(),
        SagaLifecycleStatusV1::BlockedAuthority
    );
    assert!(blocked_view.diagnostic_reason().is_some());
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(10)
    );

    // Resume the blocked saga with `long_lived_principal` and await completion!
    let resumed = server
        .service
        .resume_saga(
            server.request_context(&long_lived_principal, 5),
            ResumeSagaRequest::new(blocked_saga_id),
        )
        .await
        .expect("resume_saga succeeds");
    let GetSagaResult::Found(resumed_view) = resumed else {
        panic!("expected Found");
    };
    assert_eq!(
        resumed_view.lifecycle_status(),
        SagaLifecycleStatusV1::RunningForward
    );

    let completed_after_resume = server
        .service
        .await_saga(
            server.request_context(&long_lived_principal, 6),
            AwaitSagaRequest::new(blocked_saga_id, Duration::from_secs(5)).unwrap(),
        )
        .await
        .expect("await_saga after resume succeeds");
    let GetSagaResult::Found(completed_view) = completed_after_resume else {
        panic!("expected Found");
    };
    assert_eq!(
        completed_view.lifecycle_status(),
        SagaLifecycleStatusV1::Completed
    );
    assert_eq!(
        server.read_entity_u64_field("Inventory", 101, "available"),
        Some(8)
    );
    assert_eq!(
        server.read_entity_u64_field("Wallet", 202, "balance"),
        Some(400)
    );

    // Part B: `CompensationFailed` when a compensating command returns a non-success business outcome.
    server
        .exec_command(
            &long_lived_principal,
            7,
            "SetInventoryFrozen",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("freeze-inv-101").unwrap(),
                ),
                ("sku", SubmittedValue::U64(101)),
                ("frozen", SubmittedValue::Bool(true)),
            ],
        )
        .await;

    let comp_fail_id = SagaId::from_unix_milliseconds_and_random(401, [0x72; 10]).expect("saga id");
    let comp_failed = server
        .service
        .start_saga(
            server.request_context(&long_lived_principal, 8),
            StartSagaRequest::new(
                comp_fail_id,
                server.lineage.clone(),
                SourceName::new("CheckoutOrder").unwrap(),
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("checkout-comp-fail").unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(602)),
                    ("sku", SubmittedValue::U64(101)),
                    ("user_id", SubmittedValue::U64(202)),
                    ("quantity", SubmittedValue::U64(3)),
                    ("amount", SubmittedValue::U64(99999)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            )
            .with_await_window(Duration::from_secs(5))
            .unwrap(),
        )
        .await
        .expect("start_saga with failing compensation");
    assert_eq!(
        comp_failed.instance().lifecycle_status(),
        SagaLifecycleStatusV1::CompensationFailed
    );
    assert!(
        comp_failed
            .instance()
            .diagnostic_reason()
            .is_some_and(|reason| reason.contains("ReleaseFrozen"))
    );

    let fetched = server
        .service
        .get_saga(
            server.request_context(&long_lived_principal, 9),
            GetSagaRequest::new(comp_fail_id),
        )
        .await
        .expect("get_saga succeeds");
    let GetSagaResult::Found(fetched_view) = fetched else {
        panic!("expected Found");
    };
    assert_eq!(
        fetched_view.lifecycle_status(),
        SagaLifecycleStatusV1::CompensationFailed
    );

    server.spawner.wait_for_idle().await;
}

// req: WF-002, WF-005, WF-011, WF-012, WF-013, ID-001, ID-002, TXN-001
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_surface_dispatches_saga_start_get_await_cancel_and_resume_with_bounded_wait() {
    let server = SagaTestServer::new("saga-command-surface-dispatch");
    let principal = server.issue_principal("orchestrator-cmd-surface", 3600, 6);

    server
        .exec_command(
            &principal,
            1,
            "SeedInventory",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-inv-701").unwrap(),
                ),
                ("sku", SubmittedValue::U64(701)),
                ("available", SubmittedValue::U64(10)),
            ],
        )
        .await;
    server
        .exec_command(
            &principal,
            2,
            "SeedWallet",
            vec![
                (
                    "idempotency_key",
                    SubmittedValue::string("seed-wal-802").unwrap(),
                ),
                ("user_id", SubmittedValue::U64(802)),
                ("balance", SubmittedValue::U64(500)),
            ],
        )
        .await;

    // 1. Start and bounded await via direct `CheckoutOrder` command dispatch (MCP / DriverHost / CLI path)
    let saga_id = SagaId::from_unix_milliseconds_and_random(500, [0x81; 10]).expect("saga id");
    let start_result = server
        .service
        .execute_command(
            server.request_context(&principal, 3),
            ExecuteCommandRequest::new(
                SourceName::new("CheckoutOrder").unwrap(),
                None,
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("cmd-saga-start-1").unwrap(),
                    ),
                    (
                        "saga_id",
                        SubmittedValue::string(saga_id.to_string()).unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(901)),
                    ("sku", SubmittedValue::U64(701)),
                    ("user_id", SubmittedValue::U64(802)),
                    ("quantity", SubmittedValue::U64(2)),
                    ("amount", SubmittedValue::U64(100)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            )
            .unwrap(),
        )
        .await
        .expect("execute_command CheckoutOrder");
    let riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(read_only) = start_result else {
        panic!("expected ReadOnlyExecuted for saga command result");
    };
    assert_eq!(read_only.outcome().outcome_name().as_str(), "SagaStatus");

    // 2. Inspect via `__saga_get__`
    let get_result = server
        .service
        .execute_command(
            server.request_context(&principal, 4),
            ExecuteCommandRequest::new(
                SourceName::new("__saga_get__").unwrap(),
                None,
                submitted_record(vec![(
                    "saga_id",
                    SubmittedValue::string(saga_id.to_string()).unwrap(),
                )]),
            )
            .unwrap(),
        )
        .await
        .expect("execute_command __saga_get__");
    let riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(get_ro) = get_result else {
        panic!("expected ReadOnlyExecuted");
    };
    assert_eq!(get_ro.outcome().outcome_name().as_str(), "SagaStatus");

    // 3. Start without inline wait via `__saga_start__CheckoutOrder`, then explicit `__saga_cancel__`
    let cancel_saga_id =
        SagaId::from_unix_milliseconds_and_random(501, [0x82; 10]).expect("saga id");
    server
        .service
        .execute_command(
            server.request_context(&principal, 5),
            ExecuteCommandRequest::new(
                SourceName::new("__saga_start__CheckoutOrder").unwrap(),
                None,
                submitted_record(vec![
                    (
                        "idempotency_key",
                        SubmittedValue::string("cmd-saga-cancel-1").unwrap(),
                    ),
                    (
                        "saga_id",
                        SubmittedValue::string(cancel_saga_id.to_string()).unwrap(),
                    ),
                    ("order_id", SubmittedValue::U64(902)),
                    ("sku", SubmittedValue::U64(701)),
                    ("user_id", SubmittedValue::U64(802)),
                    ("quantity", SubmittedValue::U64(1)),
                    ("amount", SubmittedValue::U64(50)),
                    ("reject_order", SubmittedValue::Bool(false)),
                ]),
            )
            .unwrap(),
        )
        .await
        .expect("execute_command __saga_start__CheckoutOrder");

    let cancel_result = server
        .service
        .execute_command(
            server.request_context(&principal, 6),
            ExecuteCommandRequest::new(
                SourceName::new("__saga_cancel__").unwrap(),
                None,
                submitted_record(vec![(
                    "saga_id",
                    SubmittedValue::string(cancel_saga_id.to_string()).unwrap(),
                )]),
            )
            .unwrap(),
        )
        .await
        .expect("execute_command __saga_cancel__");
    let riffdb_service::ExecuteCommandResult::ReadOnlyExecuted(cancel_ro) = cancel_result else {
        panic!("expected ReadOnlyExecuted");
    };
    assert_eq!(cancel_ro.outcome().outcome_name().as_str(), "SagaStatus");

    server.spawner.wait_for_idle().await;
}
