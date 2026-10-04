//! Real catalog evidence for authoritative commit notification reads.
use super::*;
use crate::real_storage_support::RealStorage;
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_storage_api::{
    AuditPrincipalV1, CatalogActivationIntentV1, CatalogAdministrationRepository, DeclaredOutcome,
    StoredContractBundleV1, StoredContractMigrationEdgeV1, StoredReadDependenciesV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, CanonicalInputHash, CanonicalRecord,
    ContractBundleHash, LogicalTime, PartitionKeyHash, ProvenanceId, RequestId, TenantScope,
    Timestamp,
};
use std::num::NonZeroU64;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;

const SOURCE: &str = r#"
contract CommitRead version 1 {
  entity Row { key (id: uuid) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
  command Ping {
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    create Row(id) as row else Exists {}
    return Done {}
  }
}
"#;

struct CountedCatalog {
    storage: SharedRedbOperationalPorts,
    bundle_reads: AtomicU64,
    hide_pointer: AtomicBool,
    bundle_hook: Mutex<Option<(Arc<Barrier>, Arc<Barrier>)>>,
}
impl CountedCatalog {
    fn new(storage: SharedRedbOperationalPorts) -> Self {
        Self {
            storage,
            bundle_reads: AtomicU64::new(0),
            hide_pointer: AtomicBool::new(false),
            bundle_hook: Mutex::new(None),
        }
    }
}
impl CatalogRepository for CountedCatalog {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        if self.hide_pointer.load(Ordering::Relaxed) {
            Ok(None)
        } else {
            self.storage.read_active_catalog()
        }
    }
    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        self.bundle_reads.fetch_add(1, Ordering::Relaxed);
        let hook = self.bundle_hook.lock().unwrap().take();
        if let Some((captured, resume)) = hook {
            captured.wait();
            resume.wait();
        }
        self.storage.read_contract_bundle(lineage, version)
    }
    fn read_contract_migration_edge(
        &self,
        predecessor: ContractBundleHash,
    ) -> Result<Option<StoredContractMigrationEdgeV1>, StorageError> {
        self.storage.read_contract_migration_edge(predecessor)
    }
}

fn activate(storage: &mut SharedRedbOperationalPorts, bundle: &ValidatedContractBundle) {
    let previous = storage
        .read_active_catalog()
        .unwrap()
        .map(|p| p.contract_version());
    let version = bundle.contract_version().get();
    storage
        .activate_catalog(&CatalogActivationIntentV1::new(
            previous,
            bundle.to_stored().unwrap(),
            RequestId::from_unix_milliseconds_and_random(version, [3; 10]).unwrap(),
            AuditPrincipalV1::new(
                ActorId::new("commit-read-test").unwrap(),
                ActorKind::Human,
                CapabilityId::from_unix_milliseconds_and_random(1, [4; 10]).unwrap(),
                NonZeroU64::MIN,
            ),
            Timestamp::new(1_000, 0).unwrap(),
            None,
        ))
        .unwrap();
}

fn record(bundle: &ValidatedContractBundle) -> StoredCommitRecordV1 {
    let plan = &bundle.bundle().commands()[0];
    StoredCommitRecordV1::new(
        CommitSequence::first(),
        RequestId::from_unix_milliseconds_and_random(1, [5; 10]).unwrap(),
        ExecutablePlanRef::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            plan.command_id(),
            plan.plan_hash(),
        ),
        CanonicalInputHash::from_bytes([6; 32]),
        AdmittedActorContext::new(
            ActorId::new("commit-read-test").unwrap(),
            ActorKind::Human,
            TenantScope::Global,
            None,
        ),
        LogicalTime::new(Timestamp::new(1_000, 0).unwrap()),
        PartitionKeyHash::from_bytes([7; 32]),
        Vec::new(),
        StoredReadDependenciesV1::new(Vec::new()).unwrap(),
        Vec::new(),
        Vec::new(),
        DeclaredOutcome::new(
            plan.outcomes()[0].id(),
            CanonicalRecord::new(Vec::new()).unwrap(),
        )
        .unwrap(),
        ProvenanceId::from_unix_milliseconds_and_random(1, [8; 10]).unwrap(),
        Vec::new(),
        DurabilityMode::Sync,
    )
    .unwrap()
}

// req: END-008
#[test]
fn authoritative_commit_reads_reuse_proved_plans_and_revalidate_after_deployment() {
    let real = RealStorage::open("commit-plan-reads");
    let first =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(SOURCE).unwrap())
            .unwrap();
    let mut storage = real.storage.clone();
    activate(&mut storage, &first);
    let catalog = CountedCatalog::new(storage);
    let cache = Mutex::new(ExecutablePlanView::default());
    let original = record(&first);
    let cold = map_commit_record(&catalog, &cache, original.clone()).unwrap();
    let baseline = catalog.bundle_reads.load(Ordering::Relaxed);
    assert!(
        baseline > 0,
        "cold resolution must validate real durable lineage evidence"
    );
    for _ in 0..16 {
        assert_eq!(
            map_commit_record(&catalog, &cache, original.clone()).unwrap(),
            cold
        );
    }
    assert_eq!(
        catalog.bundle_reads.load(Ordering::Relaxed),
        baseline,
        "each native notification must reuse the already proved plan, rather than reload the complete lineage"
    );

    let second = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_successor(
            &SOURCE.replacen("version 1", "version 2", 1),
            first.bundle(),
        )
        .unwrap(),
    )
    .unwrap();
    activate(&mut catalog.storage.clone(), &second);
    assert_eq!(
        map_commit_record(&catalog, &cache, original).unwrap(),
        cold,
        "historical commits must retain their exact original outcome after deployment"
    );
    assert!(
        catalog.bundle_reads.load(Ordering::Relaxed) > baseline,
        "a changed durable pointer must require renewed lineage validation"
    );
    let successor = map_commit_record(&catalog, &cache, record(&second)).unwrap();
    assert_eq!(successor.contract_version(), second.contract_version());
    let reference = record(&second);
    let reference = reference.plan();
    let forged = ExecutablePlanRef::new(
        reference.contract_lineage().clone(),
        reference.contract_version(),
        reference.contract_bundle_hash(),
        reference.command_id(),
        riffdb_types::PlanHash::from_bytes([0; 32]),
    );
    assert_eq!(
        resolve_executable_plan_cached(&catalog, &cache, &forged)
            .unwrap_err()
            .kind(),
        CatalogErrorKind::UnknownExecutablePlan,
        "an exact identity mismatch cannot reuse a proved plan"
    );
    catalog.hide_pointer.store(true, Ordering::Relaxed);
    assert!(
        map_commit_record(&catalog, &cache, record(&second)).is_err(),
        "a cached plan cannot substitute for an absent current durable pointer"
    );
}

// req: END-008
#[test]
fn competing_commit_reads_validate_one_cold_plan() {
    let real = RealStorage::open("commit-plan-competing");
    let first =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(SOURCE).unwrap())
            .unwrap();
    let mut storage = real.storage.clone();
    activate(&mut storage, &first);
    let catalog = CountedCatalog::new(storage);
    let cache = Mutex::new(ExecutablePlanView::default());
    let start = Barrier::new(8);
    let original = record(&first);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    map_commit_record(&catalog, &cache, original.clone()).unwrap()
                })
            })
            .collect();
        for reader in readers {
            assert_eq!(
                reader.join().unwrap().contract_version(),
                first.contract_version()
            );
        }
    });
    assert_eq!(
        catalog.bundle_reads.load(Ordering::Relaxed),
        1,
        "competing readers must not repeat complete cold lineage validation"
    );
}

// req: END-008
#[test]
fn deployment_during_commit_resolution_cannot_publish_an_unqualified_cache_entry() {
    let real = RealStorage::open("commit-plan-deployment-race");
    let first =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(SOURCE).unwrap())
            .unwrap();
    let mut storage = real.storage.clone();
    activate(&mut storage, &first);
    let catalog = CountedCatalog::new(storage);
    let cache = Mutex::new(ExecutablePlanView::default());
    let captured = Arc::new(Barrier::new(2));
    let resume = Arc::new(Barrier::new(2));
    *catalog.bundle_hook.lock().unwrap() = Some((Arc::clone(&captured), Arc::clone(&resume)));
    let second = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_successor(
            &SOURCE.replacen("version 1", "version 2", 1),
            first.bundle(),
        )
        .unwrap(),
    )
    .unwrap();
    std::thread::scope(|scope| {
        let reader = scope.spawn(|| map_commit_record(&catalog, &cache, record(&first)).unwrap());
        captured.wait();
        activate(&mut catalog.storage.clone(), &second);
        resume.wait();
        assert_eq!(
            reader.join().unwrap().contract_version(),
            first.contract_version()
        );
    });
    assert!(
        cache.lock().unwrap().plans.is_empty(),
        "a coherent raced read is not a proof reusable under a different active pointer"
    );
    map_commit_record(&catalog, &cache, record(&first)).unwrap();
    assert_eq!(
        catalog.bundle_reads.load(Ordering::Relaxed),
        3,
        "the next read must validate the successor and its historical predecessor"
    );
}
