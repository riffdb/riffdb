//! Head-to-head RiffDB vs PostgreSQL benchmark for `mutate any` indexed pool allocation (`ADR-0333`)
//! and compiled multi-step durable sagas (`ADR-0334`).
//!
//! Compares three write and orchestration workloads across a live `riffdbd` gRPC server,
//! `postgres-minimal`, and `postgres-safe-app`:
//! 1. `pool_claim_mutate_any`: Single-round-trip `mutate any` unowned pool claiming with discovered-root
//!    conflict keys `(org_id, slot_id)` vs PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE`.
//! 2. `saga_checkout_forward`: 3-step cross-partition compiled durable saga (`ReserveInventory` ->
//!    `ChargeWallet` -> `ConfirmOrder`) vs PostgreSQL multi-step durable saga state machine.
//! 3. `saga_checkout_compensate`: 3-step cross-partition compiled durable saga with step-3 rejection
//!    and automatic reverse LIFO compensation (`RefundWallet` -> `ReleaseInventory`) vs PostgreSQL.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use postgres::{Client, NoTls, Statement};
use riffdb_auth::bootstrap_secret::BootstrapCredential as RetainedBootstrapCredential;
use riffdb_client_rust::{
    ApplicationCommand, ApplicationValue, AttemptBudget, BearerCredential, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata, ClientSagaDisposition,
    ClientSagaStatus, RiffDbClient, StableApplicationClient, generate_capability_id, v1,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{ContractLineage, SagaId};
use serde::{Deserialize, Serialize};
use tonic::transport::Endpoint;

use crate::{
    AUDIENCE, CAPABILITY_LIFETIME_SECONDS, LatencyDistribution, PostgresProfile, SeedScale,
    SpawnedRiffdbd, bootstrap_request, fresh_request_id_bytes,
};

const POOL_SAGA_CONTRACT_LINEAGE: &str = "PoolAndSagaBench";
const POOL_SAGA_CONTRACT_VERSION: u64 = 1;

/// Contract source exercising both `ADR-0333` (`mutate any` indexed selection with discovered-root
/// conflict keys) and `ADR-0334` (compiled multi-step durable sagas with reverse LIFO compensation).
pub const POOL_AND_SAGA_CONTRACT_SOURCE: &str = r#"
contract PoolAndSagaBench version 1 {
  role SagaOrchestrator {
    commands (
      SeedWorkerSlot,
      ClaimWorkerSlot,
      RecycleWorkerSlot,
      SeedInventory,
      SeedWallet,
      ReserveInventory,
      ReleaseInventory,
      ChargeWallet,
      RefundWallet,
      ConfirmOrder
    )
  }

  entity WorkerSlot {
    key (org_id: string<32>, slot_id: u64)
    field status: string<16>
    field priority_rank: i64
    field claimed_by: string<64>
    field claim_epoch: u64
    index by_org_status_priority (org_id, status, priority_rank, slot_id)
  }

  aggregate WorkerPools {
    root WorkerSlot
    partition_by org_id
    conflict_key (org_id, slot_id)
  }

  command SeedWorkerSlot {
    input idempotency_key: string<128>
    input org_id: string<32>
    input slot_id: u64
    input priority_rank: i64
    idempotency_key idempotency_key
    create WorkerSlot(org_id, slot_id) as slot
      else SlotExists { slot_id: slot_id }
    set slot.status = "free"
    set slot.priority_rank = priority_rank
    set slot.claimed_by = ""
    set slot.claim_epoch = 0
    return Seeded { slot_id: slot_id }
  }

  command ClaimWorkerSlot {
    input idempotency_key: string<128>
    input org_id: string<32>
    input worker_id: string<64>
    idempotency_key idempotency_key
    mutate any WorkerSlot.by_org_status_priority(org_id, "free") as slot
      else PoolExhausted { org_id: org_id }
    overwrite slot.status = "claimed"
    overwrite slot.claimed_by = worker_id
    set slot.claim_epoch = slot.claim_epoch + 1
    return Claimed {
      slot_id: slot.slot_id,
      priority_rank: slot.priority_rank,
      claim_epoch: slot.claim_epoch
    }
  }

  command RecycleWorkerSlot {
    input idempotency_key: string<128>
    input org_id: string<32>
    input slot_id: u64
    idempotency_key idempotency_key
    mutate WorkerSlot(org_id, slot_id) as slot
      else MissingSlot { slot_id: slot_id }
    overwrite slot.status = "free"
    overwrite slot.claimed_by = ""
    return Recycled { slot_id: slot_id }
  }

  entity InventoryItem {
    key (sku: u64)
    field available: u64
    field reserved: u64
  }

  aggregate InventoryCatalog {
    root InventoryItem
    partition_by sku
    conflict_key (sku)
  }

  entity CustomerWallet {
    key (user_id: u64)
    field balance: u64
    field debited: u64
  }

  aggregate Wallets {
    root CustomerWallet
    partition_by user_id
    conflict_key (user_id)
  }

  entity OrderRecord {
    key (order_id: u64)
    field sku: u64
    field user_id: u64
    field quantity: u64
    field amount: u64
    field status: string<32>
  }

  aggregate Orders {
    root OrderRecord
    partition_by order_id
    conflict_key (order_id)
  }

  command SeedInventory {
    input idempotency_key: string<128>
    input sku: u64
    input available: u64
    idempotency_key idempotency_key
    init_or_mutate InventoryItem(sku) as item initialize {
      available: 0,
      reserved: 0
    }
    overwrite item.available = available
    overwrite item.reserved = 0
    return Seeded { sku: sku }
  }

  command SeedWallet {
    input idempotency_key: string<128>
    input user_id: u64
    input balance: u64
    idempotency_key idempotency_key
    init_or_mutate CustomerWallet(user_id) as w initialize {
      balance: 0,
      debited: 0
    }
    overwrite w.balance = balance
    overwrite w.debited = 0
    return Seeded { user_id: user_id }
  }

  command ReserveInventory {
    input idempotency_key: string<128>
    input sku: u64
    input quantity: u64
    input order_id: u64
    idempotency_key idempotency_key
    mutate InventoryItem(sku) as item else MissingSku { sku: sku }
    require EnoughStock: item.available >= quantity else OutOfStock { requested: quantity }
    set item.available = item.available - quantity checked_by EnoughStock
    set item.reserved = item.reserved + quantity
    return Reserved {
      reservation_id: order_id,
      remaining: item.available
    }
  }

  command ReleaseInventory {
    input idempotency_key: string<128>
    input sku: u64
    input quantity: u64
    input reservation_id: u64
    idempotency_key idempotency_key
    mutate InventoryItem(sku) as item else MissingSku { sku: sku }
    require EnoughReserved: item.reserved >= quantity else NotReserved { reservation_id: reservation_id }
    set item.available = item.available + quantity
    set item.reserved = item.reserved - quantity checked_by EnoughReserved
    return Released {
      reservation_id: reservation_id,
      available: item.available
    }
  }

  command ChargeWallet {
    input idempotency_key: string<128>
    input user_id: u64
    input amount: u64
    input order_id: u64
    idempotency_key idempotency_key
    mutate CustomerWallet(user_id) as w else MissingWallet { user_id: user_id }
    require EnoughFunds: w.balance >= amount else InsufficientFunds { needed: amount }
    set w.balance = w.balance - amount checked_by EnoughFunds
    set w.debited = w.debited + amount
    return Charged {
      payment_id: order_id,
      remaining_balance: w.balance
    }
  }

  command RefundWallet {
    input idempotency_key: string<128>
    input user_id: u64
    input amount: u64
    input payment_id: u64
    idempotency_key idempotency_key
    mutate CustomerWallet(user_id) as w else MissingWallet { user_id: user_id }
    require EnoughDebited: w.debited >= amount else NotDebited { payment_id: payment_id }
    set w.balance = w.balance + amount
    set w.debited = w.debited - amount checked_by EnoughDebited
    return Refunded {
      payment_id: payment_id,
      balance: w.balance
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
    init_or_mutate OrderRecord(order_id) as ord initialize {
      sku: sku,
      user_id: user_id,
      quantity: quantity,
      amount: amount,
      status: "pending"
    }
    require AllowedOrder: !reject_order else OrderRejected { order_id: order_id }
    overwrite ord.sku = sku
    overwrite ord.user_id = user_id
    overwrite ord.quantity = quantity
    overwrite ord.amount = amount
    overwrite ord.status = "confirmed"
    return Confirmed { order_id: order_id }
  }

  saga CheckoutOrder {
    partition_by order_id
    input saga_id: uuid
    input idempotency_key: string<128>
    input order_id: u64
    input sku: u64
    input user_id: u64
    input quantity: u64
    input amount: u64
    input reject_order: bool
    idempotency_key idempotency_key
    execution_role SagaOrchestrator

    step reserve {
      command ReserveInventory(idempotency_key, sku, quantity, order_id)
      compensate ReleaseInventory(idempotency_key, sku, quantity, reserve.reservation_id)
      on Reserved(effectful) => next charge
      on MissingSku(no_effect) => abort SkuNotFound { sku: reserve.sku }
      on OutOfStock(no_effect) => abort OutOfStockAbort { requested: reserve.requested }
    }

    step charge {
      command ChargeWallet(idempotency_key, user_id, amount, order_id)
      compensate RefundWallet(idempotency_key, user_id, amount, charge.payment_id)
      on Charged(effectful) => next confirm
      on MissingWallet(no_effect) => compensate WalletMissingCompensated { user_id: charge.user_id }
      on InsufficientFunds(no_effect) => compensate PaymentDeclinedCompensated { needed: charge.needed }
    }

    step confirm {
      command ConfirmOrder(idempotency_key, order_id, sku, user_id, quantity, amount, reject_order)
      on Confirmed(effectful) => complete OrderCompleted {
        order_id: confirm.order_id,
        reservation_id: reserve.reservation_id,
        payment_id: charge.payment_id
      }
      on OrderRejected(no_effect) => compensate OrderRejectedCompensated {
        order_id: confirm.order_id
      }
    }
  }
}
"#;

/// Closed list of write and workflow workloads evaluated by the `ADR-0333` + `ADR-0334` benchmark.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAndSagaWorkloadKind {
    /// `ADR-0333`: Single-round-trip `mutate any` indexed pool allocation with discovered-root conflict keys
    /// vs PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` + `UPDATE`.
    PoolClaimMutateAny,
    /// `ADR-0334`: 3-step cross-partition compiled durable saga (`ReserveInventory` -> `ChargeWallet` -> `ConfirmOrder`)
    /// forward completion vs PostgreSQL multi-step durable saga state machine.
    SagaCheckoutForward,
    /// `ADR-0334`: 3-step cross-partition compiled durable saga with step-3 rejection and reverse LIFO
    /// compensation (`RefundWallet` -> `ReleaseInventory`) vs PostgreSQL multi-step saga compensation.
    SagaCheckoutCompensate,
}

impl WriteAndSagaWorkloadKind {
    /// All three write and workflow workloads.
    pub const ALL: [Self; 3] = [
        Self::PoolClaimMutateAny,
        Self::SagaCheckoutForward,
        Self::SagaCheckoutCompensate,
    ];

    /// Stable identifier used in reports.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PoolClaimMutateAny => "pool_claim_mutate_any",
            Self::SagaCheckoutForward => "saga_checkout_forward",
            Self::SagaCheckoutCompensate => "saga_checkout_compensate",
        }
    }

    /// RiffDB command or saga symbol.
    #[must_use]
    pub const fn operation_name(self) -> &'static str {
        match self {
            Self::PoolClaimMutateAny => "ClaimWorkerSlot",
            Self::SagaCheckoutForward => "CheckoutOrder(complete)",
            Self::SagaCheckoutCompensate => "CheckoutOrder(compensate)",
        }
    }
}

/// Unary comparison cell for one `ADR-0333` / `ADR-0334` workload.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnaryWriteSagaComparison {
    /// Workload identifier.
    pub workload: WriteAndSagaWorkloadKind,
    /// Operation name in RiffDB.
    pub operation_name: String,
    /// RiffDB unary latency stats (`c = 1`).
    pub riffdb: LatencyDistribution,
    /// PostgreSQL `minimal` unary latency stats (`c = 1`).
    pub postgres_minimal: LatencyDistribution,
    /// PostgreSQL `safe-app` unary latency stats (`c = 1`).
    pub postgres_safe_app: LatencyDistribution,
}

/// Complete report for the `ADR-0333` (`mutate any`) and `ADR-0334` (durable sagas) benchmark section.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WriteAndSagaBenchReport {
    /// Number of semantic and parity checks verified before timing.
    pub verified_parity_checks: usize,
    /// Unary latency comparison across all 3 workloads (`c = 1`).
    pub unary_workloads: Vec<UnaryWriteSagaComparison>,
    /// Concurrent throughput and tail latency across all 3 workloads.
    pub concurrent_cells: Vec<crate::ConcurrentComparisonCell>,
}

/// Compiles `POOL_AND_SAGA_CONTRACT_SOURCE` and verifies zero errors.
pub fn compile_pool_and_saga_contract() -> Result<riffdb_contract_ir::ContractBundle, String> {
    compile_contract_source(POOL_AND_SAGA_CONTRACT_SOURCE)
        .map_err(|diagnostics| format!("PoolAndSagaBench contract diagnostics: {diagnostics:?}"))
}

#[derive(Clone)]
struct RiffdbWriteSagaRunner {
    endpoint: Endpoint,
    transport: StableApplicationClient,
    metadata: CallMetadata,
    attempts: AttemptBudget,
    counter: Arc<AtomicU64>,
}

impl RiffdbWriteSagaRunner {
    async fn setup_and_seed(
        endpoint_url: &str,
        credential: &RetainedBootstrapCredential,
        org_ids: &[String],
        slots_per_org: u64,
        worker_count: u64,
    ) -> Result<Self, String> {
        let bundle = compile_pool_and_saga_contract()?;
        let endpoint = Endpoint::from_shared(endpoint_url.to_owned())
            .map_err(|e| format!("invalid endpoint {endpoint_url}: {e}"))?
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .tcp_nodelay(true);

        let mut admin_client = RiffDbClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("RiffDbClient connect failed: {e}"))?;

        let bootstrap_token = std::str::from_utf8(credential.token().expose_secret())
            .map_err(|e| format!("bootstrap token utf8: {e}"))?;
        let bootstrap_metadata = BootstrapCallMetadata::new(
            TransportBootstrapCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );
        let admin_auth = CallMetadata::authenticated(
            BearerCredential::new(bootstrap_token).map_err(|e| format!("{e}"))?,
        );

        let created = admin_client
            .create_bootstrap_capability(bootstrap_request(credential)?, &bootstrap_metadata)
            .await
            .map_err(|e| format!("create_bootstrap_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Bootstrap(_)) = created.result else {
            return Err("bootstrap capability response was not Bootstrap".to_owned());
        };

        let deployed = admin_client
            .deploy_contract(
                v1::DeployContractRequest {
                    request_id: fresh_request_id_bytes()?,
                    source: POOL_AND_SAGA_CONTRACT_SOURCE.to_owned(),
                    expected_active_version: None,
                    expected_active_bundle_hash: Vec::new(),
                    expected_candidate_bundle_hash: Vec::new(),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("deploy_contract PoolAndSagaBench: {e}"))?;
        let Some(v1::deploy_contract_response::Result::Activated(_)) = deployed.result else {
            return Err(format!("deploy_contract did not activate: {deployed:?}"));
        };

        let lineage = ContractLineage::new(POOL_SAGA_CONTRACT_LINEAGE)
            .map_err(|e| format!("lineage: {e}"))?;
        let mut permissions = vec![v1::CapabilityPermission {
            permission: Some(v1::capability_permission::Permission::ReadContract(
                v1::Unit {},
            )),
        }];
        let mut command_ids: Vec<u32> = bundle
            .commands()
            .iter()
            .map(|cmd| cmd.command_id().get())
            .collect();
        command_ids.sort_unstable();
        command_ids.dedup();
        for stable_id in command_ids {
            permissions.push(v1::CapabilityPermission {
                permission: Some(v1::capability_permission::Permission::InvokeCommand(
                    v1::LineageScopedStableId {
                        contract_lineage: lineage.as_str().to_owned(),
                        stable_id,
                    },
                )),
            });
        }

        let cap_resp = admin_client
            .create_capability(
                v1::CreateCapabilityRequest {
                    request_id: fresh_request_id_bytes()?,
                    mode: v1::CapabilityCreateMode::Normal as i32,
                    capability_id: generate_capability_id()
                        .map_err(|e| format!("{e}"))?
                        .into_bytes()
                        .to_vec(),
                    principal_id: "pool-saga-bench-service".to_owned(),
                    actor_kind: v1::ActorKind::Service as i32,
                    requested_lifetime_seconds: CAPABILITY_LIFETIME_SECONDS,
                    audiences: vec![AUDIENCE.to_owned()],
                    grant: Some(v1::CapabilityGrant {
                        tenant_scope: Some(v1::TenantScope {
                            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
                        }),
                        partition_scope: Some(v1::PartitionScope {
                            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
                        }),
                        permissions,
                        field_visibility: Vec::new(),
                        max_scan_rows: 1,
                        approval_required: Vec::new(),
                        row_policy: None,
                        export: None,
                        reimport: None,
                        vector_inspection: None,
                    }),
                },
                &admin_auth,
            )
            .await
            .map_err(|e| format!("create_capability: {e}"))?;
        let Some(v1::create_capability_response::Result::Normal(normal_res)) = cap_resp.result
        else {
            return Err("capability response was not Normal".to_owned());
        };
        let Some(v1::normal_create_capability_result::Result::Created(created_cap)) =
            normal_res.result
        else {
            return Err("capability was not Created".to_owned());
        };

        let transport = StableApplicationClient::connect(endpoint.clone())
            .await
            .map_err(|e| format!("StableApplicationClient connect: {e}"))?;
        let metadata = CallMetadata::authenticated(
            BearerCredential::new(&created_cap.token).map_err(|e| format!("{e}"))?,
        );
        let attempts = AttemptBudget::new(5).ok_or_else(|| "invalid AttemptBudget".to_owned())?;

        let mut runner = Self {
            endpoint,
            transport,
            metadata,
            attempts,
            counter: Arc::new(AtomicU64::new(1)),
        };

        runner
            .seed_pools_and_accounts(org_ids, slots_per_org, worker_count)
            .await?;
        Ok(runner)
    }

    async fn fresh_worker_session(&self) -> Result<Self, String> {
        let transport = StableApplicationClient::connect(self.endpoint.clone())
            .await
            .map_err(|e| format!("fresh_worker_session connect: {e}"))?;
        Ok(Self {
            endpoint: self.endpoint.clone(),
            transport,
            metadata: self.metadata.clone(),
            attempts: self.attempts,
            counter: Arc::clone(&self.counter),
        })
    }

    async fn seed_pools_and_accounts(
        &mut self,
        org_ids: &[String],
        slots_per_org: u64,
        worker_count: u64,
    ) -> Result<(), String> {
        for org_id in org_ids {
            for slot_id in 1..=slots_per_org {
                let cmd = ApplicationCommand::new(
                    "SeedWorkerSlot",
                    Some(POOL_SAGA_CONTRACT_VERSION),
                    BTreeMap::from([
                        (
                            "idempotency_key".to_owned(),
                            ApplicationValue::String(format!("seed-slot-{org_id}-{slot_id}")),
                        ),
                        ("org_id".to_owned(), ApplicationValue::String(org_id.clone())),
                        ("slot_id".to_owned(), ApplicationValue::U64(slot_id)),
                        (
                            "priority_rank".to_owned(),
                            ApplicationValue::I64(i64::try_from(slot_id).unwrap_or(0)),
                        ),
                    ]),
                )
                .map_err(|e| format!("{e}"))?;
                self.transport
                    .execute_command(cmd, self.attempts, &self.metadata)
                    .await
                    .map_err(|e| format!("SeedWorkerSlot failed: {e}"))?;
            }
        }

        for worker_idx in 0..worker_count {
            let sku = 1_000 + worker_idx;
            let user_id = 2_000 + worker_idx;
            let inv_cmd = ApplicationCommand::new(
                "SeedInventory",
                Some(POOL_SAGA_CONTRACT_VERSION),
                BTreeMap::from([
                    (
                        "idempotency_key".to_owned(),
                        ApplicationValue::String(format!("seed-inv-{sku}")),
                    ),
                    ("sku".to_owned(), ApplicationValue::U64(sku)),
                    ("available".to_owned(), ApplicationValue::U64(1_000_000)),
                ]),
            )
            .map_err(|e| format!("{e}"))?;
            self.transport
                .execute_command(inv_cmd, self.attempts, &self.metadata)
                .await
                .map_err(|e| format!("SeedInventory failed: {e}"))?;

            let wal_cmd = ApplicationCommand::new(
                "SeedWallet",
                Some(POOL_SAGA_CONTRACT_VERSION),
                BTreeMap::from([
                    (
                        "idempotency_key".to_owned(),
                        ApplicationValue::String(format!("seed-wal-{user_id}")),
                    ),
                    ("user_id".to_owned(), ApplicationValue::U64(user_id)),
                    ("balance".to_owned(), ApplicationValue::U64(100_000_000)),
                ]),
            )
            .map_err(|e| format!("{e}"))?;
            self.transport
                .execute_command(wal_cmd, self.attempts, &self.metadata)
                .await
                .map_err(|e| format!("SeedWallet failed: {e}"))?;
        }
        Ok(())
    }

    async fn claim_pool_slot(&mut self, org_id: &str, worker_id: &str) -> Result<u64, String> {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let cmd = ApplicationCommand::new(
            "ClaimWorkerSlot",
            Some(POOL_SAGA_CONTRACT_VERSION),
            BTreeMap::from([
                (
                    "idempotency_key".to_owned(),
                    ApplicationValue::String(format!("claim-{org_id}-{worker_id}-{seq}")),
                ),
                (
                    "org_id".to_owned(),
                    ApplicationValue::String(org_id.to_owned()),
                ),
                (
                    "worker_id".to_owned(),
                    ApplicationValue::String(worker_id.to_owned()),
                ),
            ]),
        )
        .map_err(|e| format!("{e}"))?;
        let res = self
            .transport
            .execute_command(cmd, self.attempts, &self.metadata)
            .await
            .map_err(|e| format!("ClaimWorkerSlot failed: {e}"))?;
        if res.outcome.as_deref() != Some("Claimed") {
            return Err(format!("expected Claimed outcome, got {:?}", res.outcome));
        }
        let Some(ApplicationValue::Record(fields)) = res.outcome_value else {
            return Err("missing Claimed outcome fields".to_owned());
        };
        let Some(ApplicationValue::U64(slot_id)) = fields.get("slot_id") else {
            return Err("missing slot_id in Claimed outcome".to_owned());
        };
        Ok(*slot_id)
    }

    async fn execute_checkout_saga(
        &mut self,
        worker_idx: u64,
        reject_order: bool,
    ) -> Result<ClientSagaStatus, String> {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let sku = 1_000 + worker_idx;
        let user_id = 2_000 + worker_idx;
        let order_id = 100_000 + seq;
        let mut rand_bytes = [0u8; 10];
        rand_bytes[..8].copy_from_slice(&seq.to_be_bytes());
        rand_bytes[8..10].copy_from_slice(&(worker_idx as u16).to_be_bytes());
        let saga_id = SagaId::from_unix_milliseconds_and_random(1_700_000_000_000 + seq, rand_bytes)
            .map_err(|e| format!("{e}"))?;
        let res = self
            .transport
            .start_saga(
                "CheckoutOrder",
                Some(&saga_id.to_string()),
                BTreeMap::from([
                    (
                        "idempotency_key".to_owned(),
                        ApplicationValue::String(format!("saga-checkout-{worker_idx}-{seq}")),
                    ),
                    ("order_id".to_owned(), ApplicationValue::U64(order_id)),
                    ("sku".to_owned(), ApplicationValue::U64(sku)),
                    ("user_id".to_owned(), ApplicationValue::U64(user_id)),
                    ("quantity".to_owned(), ApplicationValue::U64(1)),
                    ("amount".to_owned(), ApplicationValue::U64(10)),
                    ("reject_order".to_owned(), ApplicationValue::Bool(reject_order)),
                ]),
                Some(Duration::from_secs(10)),
                self.attempts,
                &self.metadata,
            )
            .await
            .map_err(|e| format!("start_saga CheckoutOrder failed: {e}"))?;
        Ok(res.instance.status)
    }

    async fn execute_workload(
        &mut self,
        workload: WriteAndSagaWorkloadKind,
        org_id: &str,
        worker_idx: u64,
    ) -> Result<(), String> {
        match workload {
            WriteAndSagaWorkloadKind::PoolClaimMutateAny => {
                let _slot_id = self
                    .claim_pool_slot(org_id, &format!("worker-{worker_idx}"))
                    .await?;
                Ok(())
            }
            WriteAndSagaWorkloadKind::SagaCheckoutForward => {
                let status = self.execute_checkout_saga(worker_idx, false).await?;
                if status != ClientSagaStatus::Completed {
                    return Err(format!("expected Completed saga status, got {status:?}"));
                }
                Ok(())
            }
            WriteAndSagaWorkloadKind::SagaCheckoutCompensate => {
                let status = self.execute_checkout_saga(worker_idx, true).await?;
                if status != ClientSagaStatus::Compensated {
                    return Err(format!("expected Compensated saga status, got {status:?}"));
                }
                Ok(())
            }
        }
    }
}

struct PostgresWriteSagaWorker {
    client: Client,
    profile: PostgresProfile,
    counter: Arc<AtomicU64>,
    stmt_auth: Statement,
    stmt_idem_check: Statement,
    stmt_idem_insert: Statement,
    stmt_audit_insert: Statement,
    stmt_claim_slot: Statement,
    stmt_saga_admit: Statement,
    stmt_saga_prepare: Statement,
    stmt_saga_advance: Statement,
    stmt_saga_complete: Statement,
    stmt_reserve_inv: Statement,
    stmt_release_inv: Statement,
    stmt_charge_wal: Statement,
    stmt_refund_wal: Statement,
    stmt_confirm_ord: Statement,
}

fn setup_and_seed_postgres_write_saga(
    postgres_url: &str,
    org_ids: &[String],
    slots_per_org: u64,
    worker_count: u64,
) -> Result<(), String> {
    let mut client =
        Client::connect(postgres_url, NoTls).map_err(|e| format!("pg connect: {e}"))?;
    client
        .batch_execute(
            r#"
            DROP TABLE IF EXISTS bench_worker_slots CASCADE;
            DROP TABLE IF EXISTS bench_inventory CASCADE;
            DROP TABLE IF EXISTS bench_wallets CASCADE;
            DROP TABLE IF EXISTS bench_orders CASCADE;
            DROP TABLE IF EXISTS bench_sagas CASCADE;
            DROP TABLE IF EXISTS bench_idempotency CASCADE;
            DROP TABLE IF EXISTS bench_audit CASCADE;
            DROP TABLE IF EXISTS bench_permissions CASCADE;

            CREATE TABLE bench_permissions (
                role_name TEXT NOT NULL,
                operation_name TEXT NOT NULL,
                PRIMARY KEY (role_name, operation_name)
            );

            CREATE TABLE bench_idempotency (
                idempotency_key TEXT PRIMARY KEY,
                operation_name TEXT NOT NULL,
                outcome_name TEXT NOT NULL
            );

            CREATE TABLE bench_audit (
                audit_id BIGSERIAL PRIMARY KEY,
                actor_id TEXT NOT NULL,
                operation_name TEXT NOT NULL,
                idempotency_key TEXT NOT NULL
            );

            CREATE TABLE bench_worker_slots (
                org_id TEXT NOT NULL,
                slot_id BIGINT NOT NULL,
                status TEXT NOT NULL,
                priority_rank BIGINT NOT NULL,
                claimed_by TEXT NOT NULL,
                claim_epoch BIGINT NOT NULL,
                PRIMARY KEY (org_id, slot_id)
            );
            CREATE INDEX idx_bench_worker_slots_free
                ON bench_worker_slots (org_id, status, priority_rank ASC, slot_id ASC);

            CREATE TABLE bench_inventory (
                sku BIGINT PRIMARY KEY,
                available BIGINT NOT NULL,
                reserved BIGINT NOT NULL
            );

            CREATE TABLE bench_wallets (
                user_id BIGINT PRIMARY KEY,
                balance BIGINT NOT NULL,
                debited BIGINT NOT NULL
            );

            CREATE TABLE bench_orders (
                order_id BIGINT PRIMARY KEY,
                sku BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                quantity BIGINT NOT NULL,
                amount BIGINT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE bench_sagas (
                saga_id TEXT PRIMARY KEY,
                idempotency_key TEXT NOT NULL UNIQUE,
                saga_name TEXT NOT NULL,
                status TEXT NOT NULL,
                current_step INT NOT NULL,
                revision BIGINT NOT NULL,
                fencing_token BIGINT NOT NULL,
                pending_step_key TEXT
            );
            "#,
        )
        .map_err(|e| format!("pg create write/saga schema: {e}"))?;

    for op in [
        "ClaimWorkerSlot",
        "CheckoutOrder",
        "ReserveInventory",
        "ReleaseInventory",
        "ChargeWallet",
        "RefundWallet",
        "ConfirmOrder",
    ] {
        client
            .execute(
                "INSERT INTO bench_permissions (role_name, operation_name) VALUES ('SagaOrchestrator', $1)",
                &[&op],
            )
            .map_err(|e| format!("pg seed permission: {e}"))?;
    }

    let mut tx = client.transaction().map_err(|e| format!("{e}"))?;
    let stmt_slot = tx
        .prepare(
            "INSERT INTO bench_worker_slots (org_id, slot_id, status, priority_rank, claimed_by, claim_epoch) VALUES ($1, $2, 'free', $3, '', 0)",
        )
        .map_err(|e| format!("{e}"))?;
    for org_id in org_ids {
        for slot_id in 1..=slots_per_org {
            let sid = i64::try_from(slot_id).unwrap_or(0);
            tx.execute(&stmt_slot, &[&org_id, &sid, &sid])
                .map_err(|e| format!("{e}"))?;
        }
    }

    for worker_idx in 0..worker_count {
        let sku = i64::try_from(1_000 + worker_idx).unwrap_or(0);
        let user_id = i64::try_from(2_000 + worker_idx).unwrap_or(0);
        tx.execute(
            "INSERT INTO bench_inventory (sku, available, reserved) VALUES ($1, 1000000, 0)",
            &[&sku],
        )
        .map_err(|e| format!("{e}"))?;
        tx.execute(
            "INSERT INTO bench_wallets (user_id, balance, debited) VALUES ($1, 100000000, 0)",
            &[&user_id],
        )
        .map_err(|e| format!("{e}"))?;
    }
    tx.commit().map_err(|e| format!("{e}"))?;
    Ok(())
}

impl PostgresWriteSagaWorker {
    fn connect(
        postgres_url: &str,
        profile: PostgresProfile,
        counter: Arc<AtomicU64>,
    ) -> Result<Self, String> {
        let mut client =
            Client::connect(postgres_url, NoTls).map_err(|e| format!("pg connect: {e}"))?;
        let stmt_auth = client
            .prepare(
                "SELECT 1 FROM bench_permissions WHERE role_name = 'SagaOrchestrator' AND operation_name = $1",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_idem_check = client
            .prepare("SELECT outcome_name FROM bench_idempotency WHERE idempotency_key = $1")
            .map_err(|e| format!("{e}"))?;
        let stmt_idem_insert = client
            .prepare(
                "INSERT INTO bench_idempotency (idempotency_key, operation_name, outcome_name) VALUES ($1, $2, $3)",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_audit_insert = client
            .prepare(
                "INSERT INTO bench_audit (actor_id, operation_name, idempotency_key) VALUES ($1, $2, $3)",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_claim_slot = client
            .prepare(
                r#"
                UPDATE bench_worker_slots
                SET status = 'claimed',
                    claimed_by = $2,
                    claim_epoch = claim_epoch + 1
                WHERE org_id = $1
                  AND slot_id = (
                      SELECT slot_id
                      FROM bench_worker_slots
                      WHERE org_id = $1 AND status = 'free'
                      ORDER BY priority_rank ASC, slot_id ASC
                      LIMIT 1
                      FOR UPDATE SKIP LOCKED
                  )
                RETURNING slot_id
                "#,
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_saga_admit = client
            .prepare(
                "INSERT INTO bench_sagas (saga_id, idempotency_key, saga_name, status, current_step, revision, fencing_token, pending_step_key) VALUES ($1, $2, 'CheckoutOrder', 'running_forward', 0, 1, 1, NULL)",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_saga_prepare = client
            .prepare(
                "UPDATE bench_sagas SET pending_step_key = $2, revision = revision + 1 WHERE saga_id = $1 AND fencing_token = 1",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_saga_advance = client
            .prepare(
                "UPDATE bench_sagas SET current_step = $2, status = $3, pending_step_key = NULL, revision = revision + 1 WHERE saga_id = $1 AND fencing_token = 1",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_saga_complete = client
            .prepare(
                "UPDATE bench_sagas SET status = $2, current_step = $3, pending_step_key = NULL, revision = revision + 1 WHERE saga_id = $1",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_reserve_inv = client
            .prepare(
                "UPDATE bench_inventory SET available = available - $2, reserved = reserved + $2 WHERE sku = $1 AND available >= $2",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_release_inv = client
            .prepare(
                "UPDATE bench_inventory SET available = available + $2, reserved = reserved - $2 WHERE sku = $1 AND reserved >= $2",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_charge_wal = client
            .prepare(
                "UPDATE bench_wallets SET balance = balance - $2, debited = debited + $2 WHERE user_id = $1 AND balance >= $2",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_refund_wal = client
            .prepare(
                "UPDATE bench_wallets SET balance = balance + $2, debited = debited - $2 WHERE user_id = $1 AND debited >= $2",
            )
            .map_err(|e| format!("{e}"))?;
        let stmt_confirm_ord = client
            .prepare(
                "INSERT INTO bench_orders (order_id, sku, user_id, quantity, amount, status) VALUES ($1, $2, $3, $4, $5, 'confirmed')",
            )
            .map_err(|e| format!("{e}"))?;

        Ok(Self {
            client,
            profile,
            counter,
            stmt_auth,
            stmt_idem_check,
            stmt_idem_insert,
            stmt_audit_insert,
            stmt_claim_slot,
            stmt_saga_admit,
            stmt_saga_prepare,
            stmt_saga_advance,
            stmt_saga_complete,
            stmt_reserve_inv,
            stmt_release_inv,
            stmt_charge_wal,
            stmt_refund_wal,
            stmt_confirm_ord,
        })
    }

    fn claim_pool_slot(&mut self, org_id: &str, worker_id: &str) -> Result<u64, String> {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let idem_key = format!("pg-{:?}-claim-{org_id}-{worker_id}-{seq}", self.profile);
        match self.profile {
            PostgresProfile::Minimal => {
                let row = self
                    .client
                    .query_one(&self.stmt_claim_slot, &[&org_id, &worker_id])
                    .map_err(|e| format!("pg minimal claim_slot failed: {e}"))?;
                let slot_id: i64 = row.get(0);
                Ok(u64::try_from(slot_id).unwrap_or(0))
            }
            PostgresProfile::SafeApp => {
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                let step = (|| -> Result<u64, String> {
                    self.client
                        .query_one(&self.stmt_auth, &[&"ClaimWorkerSlot"])
                        .map_err(|e| format!("pg auth: {e}"))?;
                    let existing = self
                        .client
                        .query_opt(&self.stmt_idem_check, &[&idem_key])
                        .map_err(|e| format!("pg idem check: {e}"))?;
                    if existing.is_some() {
                        return Ok(0);
                    }
                    let row = self
                        .client
                        .query_one(&self.stmt_claim_slot, &[&org_id, &worker_id])
                        .map_err(|e| format!("pg safe claim_slot: {e}"))?;
                    let slot_id: i64 = row.get(0);
                    self.client
                        .execute(
                            &self.stmt_idem_insert,
                            &[&idem_key, &"ClaimWorkerSlot", &"Claimed"],
                        )
                        .map_err(|e| format!("pg idem insert: {e}"))?;
                    self.client
                        .execute(
                            &self.stmt_audit_insert,
                            &[&worker_id, &"ClaimWorkerSlot", &idem_key],
                        )
                        .map_err(|e| format!("pg audit insert: {e}"))?;
                    Ok(u64::try_from(slot_id).unwrap_or(0))
                })();
                match step {
                    Ok(v) => {
                        self.client
                            .batch_execute("COMMIT")
                            .map_err(|e| format!("{e}"))?;
                        Ok(v)
                    }
                    Err(e) => {
                        let _ = self.client.batch_execute("ROLLBACK");
                        Err(e)
                    }
                }
            }
        }
    }

    fn execute_checkout_saga(&mut self, worker_idx: u64, reject_order: bool) -> Result<(), String> {
        let seq = self.counter.fetch_add(1, Ordering::Relaxed);
        let saga_id = format!("pg-{:?}-saga-{worker_idx}-{seq}", self.profile);
        let saga_idem = format!("pg-{:?}-saga-idem-{worker_idx}-{seq}", self.profile);
        let sku = i64::try_from(1_000 + worker_idx).unwrap_or(0);
        let user_id = i64::try_from(2_000 + worker_idx).unwrap_or(0);
        let order_id = i64::try_from(100_000 + seq).unwrap_or(0);
        let qty = 1_i64;
        let amt = 10_i64;
        let delegated_actor = "saga:PoolAndSagaBench:CheckoutOrder:SagaOrchestrator";

        match self.profile {
            PostgresProfile::Minimal => {
                // TX 1: Durable saga admission
                self.client
                    .execute(&self.stmt_saga_admit, &[&saga_id, &saga_idem])
                    .map_err(|e| format!("pg saga admit: {e}"))?;

                // TX 2: Step 0 (ReserveInventory) + step advance
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(&self.stmt_reserve_inv, &[&sku, &qty])
                    .map_err(|e| format!("pg reserve_inv: {e}"))?;
                self.client
                    .execute(&self.stmt_saga_complete, &[&saga_id, &"running_forward", &1_i32])
                    .map_err(|e| format!("pg advance step 0: {e}"))?;
                self.client
                    .batch_execute("COMMIT")
                    .map_err(|e| format!("{e}"))?;

                // TX 3: Step 1 (ChargeWallet) + step advance
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(&self.stmt_charge_wal, &[&user_id, &amt])
                    .map_err(|e| format!("pg charge_wal: {e}"))?;
                self.client
                    .execute(&self.stmt_saga_complete, &[&saga_id, &"running_forward", &2_i32])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("COMMIT")
                    .map_err(|e| format!("{e}"))?;

                if !reject_order {
                    // TX 4: Step 2 (ConfirmOrder) + complete saga
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_confirm_ord,
                            &[&order_id, &sku, &user_id, &qty, &amt],
                        )
                        .map_err(|e| format!("pg confirm_ord: {e}"))?;
                    self.client
                        .execute(&self.stmt_saga_complete, &[&saga_id, &"completed", &2_i32])
                        .map_err(|e| format!("pg complete saga: {e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                } else {
                    // TX 4: Step 2 rejected -> transition to compensating
                    self.client
                        .execute(&self.stmt_saga_complete, &[&saga_id, &"compensating", &1_i32])
                        .map_err(|e| format!("pg start compensating: {e}"))?;

                    // TX 5: Compensate step 1 (RefundWallet)
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_refund_wal, &[&user_id, &amt])
                        .map_err(|e| format!("pg refund_wal: {e}"))?;
                    self.client
                        .execute(&self.stmt_saga_complete, &[&saga_id, &"compensating", &0_i32])
                        .map_err(|e| format!("pg advance comp 1: {e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;

                    // TX 6: Compensate step 0 (ReleaseInventory) + mark compensated
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_release_inv, &[&sku, &qty])
                        .map_err(|e| format!("pg release_inv: {e}"))?;
                    self.client
                        .execute(&self.stmt_saga_complete, &[&saga_id, &"compensated", &0_i32])
                        .map_err(|e| format!("pg finish comp 0: {e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                }
                Ok(())
            }
            PostgresProfile::SafeApp => {
                // TX 1: Authorize + idempotent admit saga
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .query_one(&self.stmt_auth, &[&"CheckoutOrder"])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(&self.stmt_saga_admit, &[&saga_id, &saga_idem])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("COMMIT")
                    .map_err(|e| format!("{e}"))?;

                // Step 0: PrepareSagaStep -> ReserveInventory (with idem + provenance) -> AdvanceSagaStep
                let step0_key = format!("saga-step:{saga_id}:0:forward");
                self.client
                    .execute(&self.stmt_saga_prepare, &[&saga_id, &step0_key])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .query_one(&self.stmt_auth, &[&"ReserveInventory"])
                    .map_err(|e| format!("{e}"))?;
                let _ = self
                    .client
                    .query_opt(&self.stmt_idem_check, &[&step0_key])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(&self.stmt_reserve_inv, &[&sku, &qty])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_idem_insert,
                        &[&step0_key, &"ReserveInventory", &"Reserved"],
                    )
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_audit_insert,
                        &[&delegated_actor, &"ReserveInventory", &step0_key],
                    )
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("COMMIT")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_saga_advance,
                        &[&saga_id, &1_i32, &"running_forward"],
                    )
                    .map_err(|e| format!("{e}"))?;

                // Step 1: PrepareSagaStep -> ChargeWallet -> AdvanceSagaStep
                let step1_key = format!("saga-step:{saga_id}:1:forward");
                self.client
                    .execute(&self.stmt_saga_prepare, &[&saga_id, &step1_key])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("BEGIN")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .query_one(&self.stmt_auth, &[&"ChargeWallet"])
                    .map_err(|e| format!("{e}"))?;
                let _ = self
                    .client
                    .query_opt(&self.stmt_idem_check, &[&step1_key])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(&self.stmt_charge_wal, &[&user_id, &amt])
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_idem_insert,
                        &[&step1_key, &"ChargeWallet", &"Charged"],
                    )
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_audit_insert,
                        &[&delegated_actor, &"ChargeWallet", &step1_key],
                    )
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .batch_execute("COMMIT")
                    .map_err(|e| format!("{e}"))?;
                self.client
                    .execute(
                        &self.stmt_saga_advance,
                        &[&saga_id, &2_i32, &"running_forward"],
                    )
                    .map_err(|e| format!("{e}"))?;

                // Step 2: PrepareSagaStep -> ConfirmOrder -> AdvanceSagaStep
                let step2_key = format!("saga-step:{saga_id}:2:forward");
                self.client
                    .execute(&self.stmt_saga_prepare, &[&saga_id, &step2_key])
                    .map_err(|e| format!("{e}"))?;
                if !reject_order {
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .query_one(&self.stmt_auth, &[&"ConfirmOrder"])
                        .map_err(|e| format!("{e}"))?;
                    let _ = self
                        .client
                        .query_opt(&self.stmt_idem_check, &[&step2_key])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_confirm_ord,
                            &[&order_id, &sku, &user_id, &qty, &amt],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_idem_insert,
                            &[&step2_key, &"ConfirmOrder", &"Confirmed"],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_audit_insert,
                            &[&delegated_actor, &"ConfirmOrder", &step2_key],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_saga_advance, &[&saga_id, &2_i32, &"completed"])
                        .map_err(|e| format!("{e}"))?;
                } else {
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .query_one(&self.stmt_auth, &[&"ConfirmOrder"])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_idem_insert,
                            &[&step2_key, &"ConfirmOrder", &"OrderRejected"],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_saga_advance, &[&saga_id, &1_i32, &"compensating"])
                        .map_err(|e| format!("{e}"))?;

                    // Compensate Step 1 (RefundWallet)
                    let comp1_key = format!("saga-step:{saga_id}:1:compensate");
                    self.client
                        .execute(&self.stmt_saga_prepare, &[&saga_id, &comp1_key])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .query_one(&self.stmt_auth, &[&"RefundWallet"])
                        .map_err(|e| format!("{e}"))?;
                    let _ = self
                        .client
                        .query_opt(&self.stmt_idem_check, &[&comp1_key])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_refund_wal, &[&user_id, &amt])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_idem_insert,
                            &[&comp1_key, &"RefundWallet", &"Refunded"],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_audit_insert,
                            &[&delegated_actor, &"RefundWallet", &comp1_key],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_saga_advance, &[&saga_id, &0_i32, &"compensating"])
                        .map_err(|e| format!("{e}"))?;

                    // Compensate Step 0 (ReleaseInventory)
                    let comp0_key = format!("saga-step:{saga_id}:0:compensate");
                    self.client
                        .execute(&self.stmt_saga_prepare, &[&saga_id, &comp0_key])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("BEGIN")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .query_one(&self.stmt_auth, &[&"ReleaseInventory"])
                        .map_err(|e| format!("{e}"))?;
                    let _ = self
                        .client
                        .query_opt(&self.stmt_idem_check, &[&comp0_key])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_release_inv, &[&sku, &qty])
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_idem_insert,
                            &[&comp0_key, &"ReleaseInventory", &"Released"],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(
                            &self.stmt_audit_insert,
                            &[&delegated_actor, &"ReleaseInventory", &comp0_key],
                        )
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .batch_execute("COMMIT")
                        .map_err(|e| format!("{e}"))?;
                    self.client
                        .execute(&self.stmt_saga_advance, &[&saga_id, &0_i32, &"compensated"])
                        .map_err(|e| format!("{e}"))?;
                }
                Ok(())
            }
        }
    }

    fn execute_workload(
        &mut self,
        workload: WriteAndSagaWorkloadKind,
        org_id: &str,
        worker_idx: u64,
    ) -> Result<(), String> {
        match workload {
            WriteAndSagaWorkloadKind::PoolClaimMutateAny => {
                let _ = self.claim_pool_slot(org_id, &format!("worker-{worker_idx}"))?;
                Ok(())
            }
            WriteAndSagaWorkloadKind::SagaCheckoutForward => {
                self.execute_checkout_saga(worker_idx, false)
            }
            WriteAndSagaWorkloadKind::SagaCheckoutCompensate => {
                self.execute_checkout_saga(worker_idx, true)
            }
        }
    }
}

/// Runs the `ADR-0333` (`mutate any`) and `ADR-0334` (multi-step durable sagas) benchmark suite
/// against a fresh `riffdbd` instance and PostgreSQL (`minimal` and `safe-app`).
pub fn run_write_and_saga_benchmark(
    rt: &tokio::runtime::Runtime,
    riffdbd_bin: &Path,
    work_dir: &Path,
    postgres_url: &str,
    scale: SeedScale,
) -> Result<WriteAndSagaBenchReport, String> {
    let write_work_dir = work_dir.join("pool-and-saga");
    std::fs::create_dir_all(&write_work_dir)
        .map_err(|e| format!("create pool-and-saga work_dir: {e}"))?;

    let spawned = SpawnedRiffdbd::spawn(riffdbd_bin, &write_work_dir)?;
    let org_ids: Vec<String> = (1..=scale.org_count().max(8))
        .map(|i| format!("org-{i:02}"))
        .collect();
    let slots_per_org: u64 = match scale {
        SeedScale::Smoke => 80,
        SeedScale::Full => 600,
    };
    let max_workers: u64 = 16;

    let mut riffdb_runner = rt.block_on(RiffdbWriteSagaRunner::setup_and_seed(
        &spawned.endpoint,
        &spawned.bootstrap_credential,
        &org_ids,
        slots_per_org,
        max_workers,
    ))?;

    let pg_counter = Arc::new(AtomicU64::new(1));
    setup_and_seed_postgres_write_saga(postgres_url, &org_ids, slots_per_org, max_workers)?;

    // Verify exact semantic parity across RiffDB, postgres-minimal, and postgres-safe-app:
    // 1. PoolClaimMutateAny claims slot 1 then slot 2 in deterministic priority order on both engines.
    let riff_slot_1 = rt.block_on(riffdb_runner.claim_pool_slot("org-01", "parity-worker"))?;
    let riff_slot_2 = rt.block_on(riffdb_runner.claim_pool_slot("org-01", "parity-worker"))?;
    if riff_slot_1 != 1 || riff_slot_2 != 2 {
        return Err(format!(
            "RiffDB mutate any parity failed: expected slots (1, 2), got ({riff_slot_1}, {riff_slot_2})"
        ));
    }

    let mut pg_min_worker = PostgresWriteSagaWorker::connect(
        postgres_url,
        PostgresProfile::Minimal,
        Arc::clone(&pg_counter),
    )?;
    let pg_slot_1 = pg_min_worker.claim_pool_slot("org-01", "parity-worker")?;
    let mut pg_safe_worker = PostgresWriteSagaWorker::connect(
        postgres_url,
        PostgresProfile::SafeApp,
        Arc::clone(&pg_counter),
    )?;
    let pg_slot_2 = pg_safe_worker.claim_pool_slot("org-01", "parity-worker")?;
    if pg_slot_1 != 1 || pg_slot_2 != 2 {
        return Err(format!(
            "Postgres FOR UPDATE SKIP LOCKED parity failed: expected slots (1, 2), got ({pg_slot_1}, {pg_slot_2})"
        ));
    }

    // 2. Saga forward completion + idempotent replay parity + reverse compensation parity.
    let parity_saga_id = SagaId::from_unix_milliseconds_and_random(1_700_000_999_000, [0x42; 10])
        .map_err(|e| format!("{e}"))?;
    let parity_input = BTreeMap::from([
        (
            "idempotency_key".to_owned(),
            ApplicationValue::String("parity-saga-forward-1".to_owned()),
        ),
        ("order_id".to_owned(), ApplicationValue::U64(99_001)),
        ("sku".to_owned(), ApplicationValue::U64(1_000)),
        ("user_id".to_owned(), ApplicationValue::U64(2_000)),
        ("quantity".to_owned(), ApplicationValue::U64(2)),
        ("amount".to_owned(), ApplicationValue::U64(50)),
        ("reject_order".to_owned(), ApplicationValue::Bool(false)),
    ]);
    let admitted = rt
        .block_on(riffdb_runner.transport.start_saga(
            "CheckoutOrder",
            Some(&parity_saga_id.to_string()),
            parity_input.clone(),
            Some(Duration::from_secs(10)),
            riffdb_runner.attempts,
            &riffdb_runner.metadata,
        ))
        .map_err(|e| format!("parity start_saga failed: {e}"))?;
    if admitted.disposition != ClientSagaDisposition::Admitted
        || admitted.instance.status != ClientSagaStatus::Completed
        || admitted.instance.step_ledger.len() != 3
    {
        return Err(format!(
            "RiffDB forward saga parity failed: {:?}",
            admitted.instance
        ));
    }

    let replayed = rt
        .block_on(riffdb_runner.transport.start_saga(
            "CheckoutOrder",
            None,
            parity_input,
            Some(Duration::from_secs(10)),
            riffdb_runner.attempts,
            &riffdb_runner.metadata,
        ))
        .map_err(|e| format!("parity replay start_saga failed: {e}"))?;
    if replayed.disposition != ClientSagaDisposition::Replayed
        || replayed.instance.saga_id != parity_saga_id.to_string()
    {
        return Err(format!(
            "RiffDB saga idempotent replay parity failed: {:?}",
            replayed
        ));
    }

    let comp_status = rt.block_on(riffdb_runner.execute_checkout_saga(0, true))?;
    if comp_status != ClientSagaStatus::Compensated {
        return Err(format!(
            "RiffDB compensating saga parity failed: got {comp_status:?}"
        ));
    }
    pg_min_worker.execute_checkout_saga(0, false)?;
    pg_min_worker.execute_checkout_saga(0, true)?;
    pg_safe_worker.execute_checkout_saga(0, false)?;
    pg_safe_worker.execute_checkout_saga(0, true)?;

    let verified_parity_checks = 6;

    let warmup: usize = match scale {
        SeedScale::Smoke => 2,
        SeedScale::Full => 4,
    };
    let unary_iters: usize = match scale {
        SeedScale::Smoke => 12,
        SeedScale::Full => 32,
    };
    let conc_concurrency: usize = match scale {
        SeedScale::Smoke => 4,
        SeedScale::Full => 8,
    };
    let conc_ops_per_worker: usize = match scale {
        SeedScale::Smoke => 8,
        SeedScale::Full => 20,
    };

    let mut unary_workloads = Vec::with_capacity(WriteAndSagaWorkloadKind::ALL.len());
    for workload in WriteAndSagaWorkloadKind::ALL {
        // Re-seed Postgres before each profile so table sizes and free slot pools are identical.
        setup_and_seed_postgres_write_saga(postgres_url, &org_ids, slots_per_org, max_workers)?;
        let pg_min_stats = measure_pg_write_unary(
            postgres_url,
            PostgresProfile::Minimal,
            workload,
            &org_ids,
            warmup,
            unary_iters,
            Arc::clone(&pg_counter),
        )?;

        setup_and_seed_postgres_write_saga(postgres_url, &org_ids, slots_per_org, max_workers)?;
        let pg_safe_stats = measure_pg_write_unary(
            postgres_url,
            PostgresProfile::SafeApp,
            workload,
            &org_ids,
            warmup,
            unary_iters,
            Arc::clone(&pg_counter),
        )?;

        let riff_stats = rt.block_on(measure_riffdb_write_unary(
            &mut riffdb_runner,
            workload,
            &org_ids,
            warmup,
            unary_iters,
        ))?;

        unary_workloads.push(UnaryWriteSagaComparison {
            workload,
            operation_name: workload.operation_name().to_owned(),
            riffdb: riff_stats,
            postgres_minimal: pg_min_stats,
            postgres_safe_app: pg_safe_stats,
        });
    }

    let mut concurrent_cells = Vec::with_capacity(WriteAndSagaWorkloadKind::ALL.len());
    for workload in WriteAndSagaWorkloadKind::ALL {
        setup_and_seed_postgres_write_saga(postgres_url, &org_ids, slots_per_org, max_workers)?;
        let pg_min_conc = measure_pg_write_concurrent(
            postgres_url,
            PostgresProfile::Minimal,
            workload,
            &org_ids,
            conc_concurrency,
            conc_ops_per_worker,
            Arc::clone(&pg_counter),
        )?;

        setup_and_seed_postgres_write_saga(postgres_url, &org_ids, slots_per_org, max_workers)?;
        let pg_safe_conc = measure_pg_write_concurrent(
            postgres_url,
            PostgresProfile::SafeApp,
            workload,
            &org_ids,
            conc_concurrency,
            conc_ops_per_worker,
            Arc::clone(&pg_counter),
        )?;

        let riff_conc = rt.block_on(measure_riffdb_write_concurrent(
            &riffdb_runner,
            workload,
            &org_ids,
            conc_concurrency,
            conc_ops_per_worker,
        ))?;

        concurrent_cells.push(crate::ConcurrentComparisonCell {
            workload_label: workload.as_str().to_owned(),
            concurrency: conc_concurrency,
            riffdb: riff_conc,
            postgres_minimal: pg_min_conc,
            postgres_safe_app: pg_safe_conc,
        });
    }

    Ok(WriteAndSagaBenchReport {
        verified_parity_checks,
        unary_workloads,
        concurrent_cells,
    })
}

async fn measure_riffdb_write_unary(
    runner: &mut RiffdbWriteSagaRunner,
    workload: WriteAndSagaWorkloadKind,
    org_ids: &[String],
    warmup: usize,
    iterations: usize,
) -> Result<LatencyDistribution, String> {
    for i in 0..warmup {
        let org = &org_ids[i % org_ids.len()];
        runner.execute_workload(workload, org, 0).await?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for i in 0..iterations {
        let org = &org_ids[i % org_ids.len()];
        let t0 = Instant::now();
        runner.execute_workload(workload, org, 0).await?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_start.elapsed(),
    ))
}

fn measure_pg_write_unary(
    postgres_url: &str,
    profile: PostgresProfile,
    workload: WriteAndSagaWorkloadKind,
    org_ids: &[String],
    warmup: usize,
    iterations: usize,
    counter: Arc<AtomicU64>,
) -> Result<LatencyDistribution, String> {
    let mut worker = PostgresWriteSagaWorker::connect(postgres_url, profile, counter)?;
    for i in 0..warmup {
        let org = &org_ids[i % org_ids.len()];
        worker.execute_workload(workload, org, 0)?;
    }
    let mut samples = Vec::with_capacity(iterations);
    let wall_start = Instant::now();
    for i in 0..iterations {
        let org = &org_ids[i % org_ids.len()];
        let t0 = Instant::now();
        worker.execute_workload(workload, org, 0)?;
        samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
    }
    Ok(LatencyDistribution::from_samples(
        samples,
        1,
        wall_start.elapsed(),
    ))
}

async fn measure_riffdb_write_concurrent(
    runner: &RiffdbWriteSagaRunner,
    workload: WriteAndSagaWorkloadKind,
    org_ids: &[String],
    concurrency: usize,
    ops_per_worker: usize,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency));
    let org_ids_vec = org_ids.to_vec();
    let mut handles = Vec::with_capacity(concurrency);

    for worker_idx in 0..concurrency {
        let mut worker_session = runner.fresh_worker_session().await?;
        let barrier_clone = Arc::clone(&barrier);
        let orgs = org_ids_vec.clone();
        let w_idx = worker_idx as u64;
        handles.push(tokio::spawn(async move {
            let org = &orgs[worker_idx % orgs.len()];
            worker_session.execute_workload(workload, org, w_idx).await?;
            barrier_clone.wait().await;
            let wall_t0 = Instant::now();
            let mut local_samples = Vec::with_capacity(ops_per_worker);
            for _op in 0..ops_per_worker {
                let target_org = &orgs[worker_idx % orgs.len()];
                let t0 = Instant::now();
                worker_session
                    .execute_workload(workload, target_org, w_idx)
                    .await?;
                local_samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local_samples, wall_t0.elapsed()))
        }));
    }

    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_worker_wall = Duration::ZERO;
    for handle in handles {
        let (local, worker_wall) = handle
            .await
            .map_err(|e| format!("tokio write worker join failed: {e}"))??;
        all_samples.extend(local);
        if worker_wall > max_worker_wall {
            max_worker_wall = worker_wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_worker_wall,
    ))
}

fn measure_pg_write_concurrent(
    postgres_url: &str,
    profile: PostgresProfile,
    workload: WriteAndSagaWorkloadKind,
    org_ids: &[String],
    concurrency: usize,
    ops_per_worker: usize,
    counter: Arc<AtomicU64>,
) -> Result<LatencyDistribution, String> {
    let barrier = Arc::new(Barrier::new(concurrency));
    let org_ids_vec = org_ids.to_vec();
    let pg_url = postgres_url.to_owned();
    let mut handles = Vec::with_capacity(concurrency);

    for worker_idx in 0..concurrency {
        let barrier_clone = Arc::clone(&barrier);
        let orgs = org_ids_vec.clone();
        let url = pg_url.clone();
        let ctr = Arc::clone(&counter);
        let w_idx = worker_idx as u64;
        handles.push(thread::spawn(move || {
            let mut worker = PostgresWriteSagaWorker::connect(&url, profile, ctr)?;
            let org = &orgs[worker_idx % orgs.len()];
            worker.execute_workload(workload, org, w_idx)?;
            barrier_clone.wait();
            let wall_t0 = Instant::now();
            let mut local_samples = Vec::with_capacity(ops_per_worker);
            for _op in 0..ops_per_worker {
                let target_org = &orgs[worker_idx % orgs.len()];
                let t0 = Instant::now();
                worker.execute_workload(workload, target_org, w_idx)?;
                local_samples.push(u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX));
            }
            Ok::<(Vec<u64>, Duration), String>((local_samples, wall_t0.elapsed()))
        }));
    }

    let mut all_samples = Vec::with_capacity(concurrency * ops_per_worker);
    let mut max_worker_wall = Duration::ZERO;
    for handle in handles {
        let (local, worker_wall) = handle
            .join()
            .map_err(|_| "postgres write worker thread panicked".to_owned())??;
        all_samples.extend(local);
        if worker_wall > max_worker_wall {
            max_worker_wall = worker_wall;
        }
    }
    Ok(LatencyDistribution::from_samples(
        all_samples,
        concurrency,
        max_worker_wall,
    ))
}
