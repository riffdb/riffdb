//! Catalog deployment old-or-new and uncertain-response recovery semantics.

use std::num::NonZeroU64;

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogActivationNotification, CatalogDeploymentHooks,
    CatalogFailpointTriggered, CatalogNotificationError, CatalogNotificationSink,
    CatalogPostDurabilityResult, CatalogPreparationResult, CatalogTelemetryEvent,
    NoopCatalogDeploymentHooks, ValidatedContractBundle, observe_catalog_activation,
    prepare_catalog_activation,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, CatalogRepository, StorageError, StorageErrorKind,
    StoredContractBundleV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, ContractLineage, ContractVersion,
    RequestId, Timestamp,
};

const BUDGET: &str = include_str!("../contracts/examples/budget.riff");

#[derive(Clone, Copy)]
enum CommitFault {
    None,
    BeforeCommit,
    AfterCommitBeforeResponse,
}

struct RecoveryRepository {
    bundles: Vec<StoredContractBundleV1>,
    active: Option<ActiveCatalogPointerV1>,
    active_sequence: Option<AdministrationSequence>,
    next_sequence: AdministrationSequence,
    fault: CommitFault,
}

impl Default for RecoveryRepository {
    fn default() -> Self {
        Self {
            bundles: Vec::new(),
            active: None,
            active_sequence: None,
            next_sequence: AdministrationSequence::first(),
            fault: CommitFault::None,
        }
    }
}

impl RecoveryRepository {
    fn fail_once(&mut self, fault: CommitFault) {
        self.fault = fault;
    }

    fn snapshot(&self) -> Option<ActiveCatalogSnapshot> {
        ActiveCatalogSnapshot::read(self).expect("valid recovery repository")
    }

    fn apply_activation(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        if self.bundles.iter().any(|bundle| {
            bundle.lineage() == intent.bundle().lineage()
                && bundle.contract_version() == intent.bundle().contract_version()
                && bundle != intent.bundle()
        }) {
            return Ok(CatalogActivationResult::BundleConflict);
        }

        let requested = intent.requested_active();
        if self.active.as_ref() == Some(&requested) {
            return Ok(CatalogActivationResult::AlreadyActive {
                active: requested,
                administration_sequence: self
                    .active_sequence
                    .expect("an active pointer always retains its original sequence"),
            });
        }

        let actual = self
            .active
            .as_ref()
            .map(ActiveCatalogPointerV1::contract_version);
        if actual != intent.expected_active_version() {
            return Ok(CatalogActivationResult::ExpectedActiveVersionMismatch { actual });
        }

        let sequence = self.next_sequence;
        self.next_sequence = sequence
            .checked_next()
            .ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))?;
        if !self.bundles.iter().any(|bundle| bundle == intent.bundle()) {
            self.bundles.push(intent.bundle().clone());
        }
        self.active = Some(requested.clone());
        self.active_sequence = Some(sequence);
        Ok(CatalogActivationResult::Activated {
            active: requested,
            administration_sequence: sequence,
        })
    }
}

impl CatalogRepository for RecoveryRepository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(self.active.clone())
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(self
            .bundles
            .iter()
            .find(|bundle| {
                bundle.lineage() == lineage && bundle.contract_version() == contract_version
            })
            .cloned())
    }
}

impl CatalogAdministrationRepository for RecoveryRepository {
    fn activate_catalog(
        &mut self,
        intent: &CatalogActivationIntentV1,
    ) -> Result<CatalogActivationResult, StorageError> {
        let fault = self.fault;
        self.fault = CommitFault::None;
        if matches!(fault, CommitFault::BeforeCommit) {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }

        let result = self.apply_activation(intent)?;
        if matches!(fault, CommitFault::AfterCommitBeforeResponse)
            && matches!(result, CatalogActivationResult::Activated { .. })
        {
            return Err(storage_error(StorageErrorKind::CommitStatusUnknown));
        }
        Ok(result)
    }
}

#[derive(Default)]
struct NotificationRecorder(Vec<CatalogActivationNotification>);

impl CatalogNotificationSink for NotificationRecorder {
    fn notify_catalog_activated(
        &mut self,
        notification: &CatalogActivationNotification,
    ) -> Result<(), CatalogNotificationError> {
        self.0.push(notification.clone());
        Ok(())
    }
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn uuid_v7(seed: u8) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..10].copy_from_slice(&[0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2]);
    bytes[15] = seed;
    bytes
}

fn principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("maintainer").expect("actor"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_v7(1)).expect("capability"),
        NonZeroU64::MIN,
    )
}

fn intent(
    candidate: riffdb_contract_ir::ContractBundle,
    expected: Option<ContractVersion>,
    active: Option<&ActiveCatalogSnapshot>,
    seed: u8,
) -> CatalogActivationIntentV1 {
    let CatalogPreparationResult::Prepared(prepared) =
        prepare_catalog_activation(candidate, expected, active).expect("catalog preparation")
    else {
        panic!("candidate must prepare");
    };
    prepared
        .into_storage_intent(
            RequestId::from_bytes(uuid_v7(seed)).expect("request"),
            principal(),
            Timestamp::new(i64::from(seed), 0).expect("timestamp"),
            None,
        )
        .expect("storage intent")
}

fn version_two() -> String {
    BUDGET.replacen("version 1", "version 2", 1)
}

#[test]
fn failure_before_commit_leaves_old_state_and_retry_commits_once() {
    let compiled = compile_contract_source(BUDGET).expect("genesis");
    let checked =
        ValidatedContractBundle::from_compiler_bundle(compiled.clone()).expect("checked genesis");
    let activation = intent(compiled, None, None, 2);
    let mut repository = RecoveryRepository::default();
    repository.fail_once(CommitFault::BeforeCommit);

    let error = repository
        .activate_catalog(&activation)
        .expect_err("pre-commit failure");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(repository.active.is_none());
    assert!(repository.bundles.is_empty());
    assert_eq!(repository.next_sequence, AdministrationSequence::first());

    let result = repository.activate_catalog(&activation).expect("retry");
    assert!(matches!(
        result,
        CatalogActivationResult::Activated {
            administration_sequence,
            ..
        } if administration_sequence == AdministrationSequence::first()
    ));
    assert_eq!(
        repository.active,
        Some(ActiveCatalogPointerV1::new(
            checked.lineage().clone(),
            checked.contract_version(),
            checked.bundle_hash(),
        ))
    );
    assert_eq!(repository.bundles.len(), 1);
}

#[test]
fn uncertain_response_leaves_new_state_and_exact_retry_reuses_sequence() {
    let genesis = compile_contract_source(BUDGET).expect("genesis");
    let first = intent(genesis, None, None, 3);
    let mut repository = RecoveryRepository::default();
    repository
        .activate_catalog(&first)
        .expect("initial activation");
    let old = repository.snapshot().expect("active genesis");

    let successor = compile_contract_successor(&version_two(), old.bundle().bundle())
        .expect("compatible successor");
    let second = intent(
        successor,
        Some(old.pointer().contract_version()),
        Some(&old),
        4,
    );
    repository.fail_once(CommitFault::AfterCommitBeforeResponse);
    let error = repository
        .activate_catalog(&second)
        .expect_err("response lost after durability");
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);

    let new_snapshot = repository.snapshot().expect("new active pointer");
    assert_eq!(new_snapshot.pointer(), &second.requested_active());
    let committed_sequence = repository.active_sequence.expect("durable sequence");
    assert_eq!(repository.bundles.len(), 2);

    // Recovery re-reads the active catalog, then prepares the exact candidate
    // with the original stale expected version. Exact replay wins before CAS.
    let replay_bundle =
        compile_contract_successor(&version_two(), old.bundle().bundle()).expect("same successor");
    let replay = intent(
        replay_bundle,
        Some(old.pointer().contract_version()),
        Some(&new_snapshot),
        4,
    );
    let recovered = repository.activate_catalog(&replay).expect("exact retry");
    assert!(matches!(
        &recovered,
        CatalogActivationResult::AlreadyActive {
            administration_sequence,
            ..
        } if *administration_sequence == committed_sequence
    ));
    assert_eq!(repository.bundles.len(), 2);
    assert_eq!(repository.active_sequence, Some(committed_sequence));

    let mut notifications = NotificationRecorder::default();
    assert_eq!(
        observe_catalog_activation(
            &recovered,
            &mut notifications,
            &mut NoopCatalogDeploymentHooks,
        ),
        CatalogPostDurabilityResult::NoCatalogChange
    );
    assert!(notifications.0.is_empty());
}

#[test]
fn post_durable_notification_interruption_never_rolls_back_or_duplicates_on_retry() {
    let compiled = compile_contract_source(BUDGET).expect("genesis");
    let activation = intent(compiled.clone(), None, None, 5);
    let mut repository = RecoveryRepository::default();
    let result = repository
        .activate_catalog(&activation)
        .expect("durable activation");

    struct StopAfterDurability;
    impl CatalogDeploymentHooks for StopAfterDurability {
        fn record(&mut self, _event: CatalogTelemetryEvent) {}

        fn reach(
            &mut self,
            failpoint: riffdb_catalog::CatalogDeploymentFailpoint,
        ) -> Result<(), CatalogFailpointTriggered> {
            if failpoint
                == riffdb_catalog::CatalogDeploymentFailpoint::AfterDurableActivationBeforeNotification
            {
                Err(CatalogFailpointTriggered::new(failpoint))
            } else {
                Ok(())
            }
        }
    }

    let mut notifications = NotificationRecorder::default();
    assert!(matches!(
        observe_catalog_activation(&result, &mut notifications, &mut StopAfterDurability),
        CatalogPostDurabilityResult::Interrupted(_)
    ));
    assert!(repository.active.is_some());
    assert!(notifications.0.is_empty());

    let snapshot = repository.snapshot().expect("durable active");
    let replay = intent(compiled, None, Some(&snapshot), 5);
    let replayed = repository.activate_catalog(&replay).expect("replay");
    assert!(matches!(
        replayed,
        CatalogActivationResult::AlreadyActive { .. }
    ));
    assert_eq!(
        observe_catalog_activation(
            &replayed,
            &mut notifications,
            &mut NoopCatalogDeploymentHooks,
        ),
        CatalogPostDurabilityResult::NoCatalogChange
    );
    assert!(notifications.0.is_empty());
}
