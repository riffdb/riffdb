#![forbid(unsafe_code)]

//! Real-redb restart and transaction-boundary evidence for durable consumers.

use std::num::NonZeroU64;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_catalog::{ValidatedContractBundle, ValidatedReactiveModule, validate_catalog_history};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::{
    AuditPrincipalV1, CatalogActivationIntentV1, CatalogAdministrationRepository,
    ConsumerCheckpointV1, ConsumerDeliveryStateV1, CoordinateConsumerAcknowledgementV1,
    CoordinateConsumerLeaseV1, CoordinateConsumerNegativeAcknowledgementV1,
    DatabaseInitializationPort, DatabaseInitializationResult, EventConsumerRepository,
    EvidencePageLimit, ReactiveModuleAdministrationRepository, ReactiveModulePublicationIntentV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    StartupValidationInputs, StorageErrorKind, StructuralEvidenceCursor, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, coordinate_consumer_acknowledgement,
    coordinate_consumer_lease, coordinate_consumer_negative_acknowledgement,
    coordinate_consumer_retire, coordinate_consumer_seek, coordinate_consumer_status,
    recover_event_consumers,
};
use riffdb_storage_redb::{
    RedbDormantPorts, RedbOperationalPorts, RedbStore, RedbTestController, RedbTestOperation,
};
use riffdb_types::{
    ActorId, ActorKind, CapabilityId, CommitSequence, DatabaseId, DigestKeyId, EventConsumerName,
    EventId, EventLeaseToken, PartitionKeyHash, QueryParameterHash, ReactiveModuleHash,
    ReactiveOperationName, RequestId, Timestamp,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);
const CHILD_MODE: &str = "RIFFDB_CONSUMER_RECOVERY_CHILD_MODE";
const CHILD_PATH: &str = "RIFFDB_CONSUMER_RECOVERY_CHILD_PATH";

const CONTRACT: &str = r#"
contract ConsumerTest version 1 {
  entity Row { key (organization_id: uuid, row_id: uuid) field value: i64 }
  event RowChanged {
    partition_by (organization_id)
    organization_id: uuid
    row_id: uuid
    value: i64
  }
  aggregate Rows { root Row partition_by organization_id conflict_key (organization_id, row_id) }
  command ChangeRow {
    input idempotency_key: string<128>
    input organization_id: uuid
    input row_id: uuid
    idempotency_key idempotency_key
    mutate Row(organization_id, row_id) as row else Missing {}
    set row.value = 1
    emit RowChanged { organization_id: organization_id, row_id: row_id, value: 1 }
    return Changed {}
  }
}
"#;

const REACTIVE_MODULE: &str =
    include_str!("../fixtures/reactive/module/reactive-v1/row_activity.riffr");

struct ConsumerDatabase {
    path: PathBuf,
    module_hash: ReactiveModuleHash,
}

impl ConsumerDatabase {
    fn create(label: &str) -> Self {
        let root = std::env::current_dir()
            .expect("current directory")
            .join("target")
            .join("wp417-consumer-tests");
        std::fs::create_dir_all(&root).expect("create WP-417 test root");
        let path = root.join(format!(
            "{label}-{}-{}.redb",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let mut store = RedbStore::open(&path).expect("create consumer database");
        assert_eq!(
            store
                .initialize_database(database_id())
                .expect("initialize consumer database"),
            DatabaseInitializationResult::Installed(database_id())
        );
        let mut ports = open_operational(store);
        let compiled = compile_contract_source(CONTRACT).expect("consumer contract compiles");
        let checked = ValidatedContractBundle::from_compiler_bundle(compiled)
            .expect("consumer contract validates");
        let stored = checked.to_stored().expect("consumer contract stores");
        ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                stored.clone(),
                request_id(1),
                principal(),
                time(1),
                None,
            ))
            .expect("activate consumer contract");
        let module = ValidatedReactiveModule::compile(REACTIVE_MODULE, &checked, &[])
            .expect("consumer reactive module compiles");
        let module_hash = module.identity();
        ports
            .publish_reactive_module(&ReactiveModulePublicationIntentV1::new(
                module.to_stored().expect("consumer reactive module stores"),
                request_id(2),
                principal(),
                time(1),
                None,
            ))
            .expect("publish reactive module");
        drop(ports);
        Self { path, module_hash }
    }

    fn open(&self) -> RedbOperationalPorts {
        open_operational(RedbStore::open(&self.path).expect("open consumer database"))
    }

    fn open_with_controller(&self, controller: RedbTestController) -> RedbOperationalPorts {
        open_operational(
            RedbStore::open_with_test_controller(&self.path, controller)
                .expect("open controlled consumer database"),
        )
    }
}

impl Drop for ConsumerDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn ordered_delivery_retry_checkpoint_seek_and_retire_survive_reopen() {
    let database = ConsumerDatabase::create("ordered-lifecycle");
    let identity = identity("worker-a", database.module_hash);
    let first = event(1);
    let second = event(2);
    let first_token = token(1);
    let second_token = token(2);

    {
        let mut ports = database.open();
        let leased = coordinate_consumer_lease(
            &mut ports,
            lease_request(
                identity.clone(),
                vec![first, second],
                vec![first_token, second_token],
                1,
                5,
            ),
        )
        .expect("create and lease");
        assert_eq!(leased.leases.len(), 2);
        assert_eq!(leased.leases[0].event_id, first);
        assert_eq!(leased.leases[1].event_id, second);
    }

    {
        let mut ports = database.open();
        let status = coordinate_consumer_status(&ports, &identity)
            .expect("status")
            .expect("consumer exists");
        assert_eq!(status.live_leases, 2);
        assert_eq!(status.checkpoint, ConsumerCheckpointV1::BeforeFirst);
        coordinate_consumer_acknowledgement(
            &mut ports,
            CoordinateConsumerAcknowledgementV1 {
                identity: identity.clone(),
                event_id: second,
                token: second_token,
                history_incarnation: 1,
                observed_at: time(2),
                selected_prefix: vec![first, second],
            },
        )
        .expect("out-of-order acknowledgement");
    }

    {
        let mut ports = database.open();
        let snapshot = ports
            .inspect_event_consumer(identity.identity_hash())
            .expect("inspect")
            .expect("consumer exists");
        assert_eq!(
            snapshot.consumer().checkpoint(),
            ConsumerCheckpointV1::BeforeFirst
        );
        assert_eq!(snapshot.consumer().sparse_resolutions().len(), 1);
        coordinate_consumer_negative_acknowledgement(
            &mut ports,
            CoordinateConsumerNegativeAcknowledgementV1 {
                identity: identity.clone(),
                event_id: first,
                token: first_token,
                observed_at: time(2),
                eligible_at: time(3),
                selected_prefix: vec![first, second],
            },
        )
        .expect("negative acknowledgement");
    }

    let retry_token = token(3);
    {
        let mut ports = database.open();
        let leased = coordinate_consumer_lease(
            &mut ports,
            lease_request(
                identity.clone(),
                vec![first, second],
                vec![retry_token],
                4,
                8,
            ),
        )
        .expect("retry lease");
        assert_eq!(leased.leases.len(), 1);
        assert_eq!(leased.leases[0].attempt.get(), 2);
        coordinate_consumer_acknowledgement(
            &mut ports,
            CoordinateConsumerAcknowledgementV1 {
                identity: identity.clone(),
                event_id: first,
                token: retry_token,
                history_incarnation: 1,
                observed_at: time(5),
                selected_prefix: vec![first, second],
            },
        )
        .expect("contiguous acknowledgement");
    }

    {
        let mut ports = database.open();
        let status = coordinate_consumer_status(&ports, &identity)
            .expect("status")
            .expect("consumer exists");
        assert_eq!(status.checkpoint, ConsumerCheckpointV1::After(second));
        assert_eq!(status.live_leases, 0);
        assert_eq!(status.retries, 0);
        assert_eq!(
            ports
                .event_consumer_retention_low_water()
                .expect("low water"),
            Some(1)
        );
        coordinate_consumer_seek(&mut ports, &identity, ConsumerCheckpointV1::After(first))
            .expect("seek");
    }

    {
        let mut ports = database.open();
        assert_eq!(
            coordinate_consumer_status(&ports, &identity)
                .expect("status")
                .expect("consumer exists")
                .checkpoint,
            ConsumerCheckpointV1::After(first)
        );
        coordinate_consumer_retire(&mut ports, &identity).expect("retire");
    }

    let ports = database.open();
    assert!(
        coordinate_consumer_status(&ports, &identity)
            .expect("status")
            .is_none()
    );
    assert_eq!(
        ports
            .event_consumer_retention_low_water()
            .expect("low water"),
        None
    );
}

#[test]
fn restart_restore_and_sibling_identity_recovery_are_exact() {
    let database = ConsumerDatabase::create("recovery");
    let first_identity = identity("worker-a", database.module_hash);
    let sibling_identity = identity("worker-b", database.module_hash);
    let first = event(1);
    let sibling = event(2);

    {
        let mut ports = database.open();
        coordinate_consumer_lease(
            &mut ports,
            lease_request(first_identity.clone(), vec![first], vec![token(1)], 1, 5),
        )
        .expect("first lease");
        coordinate_consumer_lease(
            &mut ports,
            lease_request(
                sibling_identity.clone(),
                vec![sibling],
                vec![token(2)],
                1,
                20,
            ),
        )
        .expect("sibling lease");
    }

    {
        let mut ports = database.open();
        recover_event_consumers(&mut ports, time(10), 1).expect("ordinary restart recovery");
        let first = ports
            .inspect_event_consumer(first_identity.identity_hash())
            .expect("first inspect")
            .expect("first exists");
        assert!(matches!(
            first.deliveries()[0].state(),
            ConsumerDeliveryStateV1::Retry { .. }
        ));
        let sibling = ports
            .inspect_event_consumer(sibling_identity.identity_hash())
            .expect("sibling inspect")
            .expect("sibling exists");
        assert!(matches!(
            sibling.deliveries()[0].state(),
            ConsumerDeliveryStateV1::Leased { .. }
        ));
    }

    {
        let mut ports = database.open();
        recover_event_consumers(&mut ports, time(11), 2).expect("restore recovery");
    }

    let ports = database.open();
    for selected in [&first_identity, &sibling_identity] {
        let snapshot = ports
            .inspect_event_consumer(selected.identity_hash())
            .expect("inspect restored consumer")
            .expect("restored consumer exists");
        assert_eq!(snapshot.consumer().history_incarnation(), 2);
        assert!(snapshot.deliveries().iter().all(|delivery| {
            delivery.history_incarnation() == 2
                && !matches!(delivery.state(), ConsumerDeliveryStateV1::Leased { .. })
        }));
    }
}

#[test]
fn transaction_failpoints_distinguish_absence_from_committed_uncertainty() {
    let absent = ConsumerDatabase::create("precommit");
    let selected = identity("worker-a", absent.module_hash);
    let request = || lease_request(selected.clone(), vec![event(1)], vec![token(1)], 1, 5);
    let controller =
        RedbTestController::return_before_commit(RedbTestOperation::EventConsumerTransition);
    let error = coordinate_consumer_lease(&mut absent.open_with_controller(controller), request())
        .expect_err("precommit failpoint");
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(
        coordinate_consumer_status(&absent.open(), &selected)
            .expect("status")
            .is_none()
    );

    let uncertain = ConsumerDatabase::create("postcommit");
    let uncertain_selected = identity("worker-a", uncertain.module_hash);
    let uncertain_request = || {
        lease_request(
            uncertain_selected.clone(),
            vec![event(1)],
            vec![token(1)],
            1,
            5,
        )
    };
    let controller =
        RedbTestController::return_unknown_after_commit(RedbTestOperation::EventConsumerTransition);
    let error = coordinate_consumer_lease(
        &mut uncertain.open_with_controller(controller),
        uncertain_request(),
    )
    .expect_err("postcommit failpoint");
    assert_eq!(error.kind(), StorageErrorKind::CommitStatusUnknown);
    let status = coordinate_consumer_status(&uncertain.open(), &uncertain_selected)
        .expect("status")
        .expect("committed consumer exists");
    assert_eq!(status.live_leases, 1);
}

#[test]
fn process_crash_matrix_preserves_every_consumer_transition() {
    for phase in ["before", "after"] {
        let committed = phase == "after";

        let lease = ConsumerDatabase::create("crash-lease");
        let lease_identity = identity("worker-a", lease.module_hash);
        run_crashing_child(&format!("lease-{phase}"), &lease);
        let lease_status =
            coordinate_consumer_status(&lease.open(), &lease_identity).expect("lease crash status");
        assert_eq!(lease_status.is_some(), committed);
        if let Some(status) = lease_status {
            assert_eq!(status.live_leases, 1);
        }

        let acknowledgement = ConsumerDatabase::create("crash-ack");
        let acknowledgement_identity = identity("worker-a", acknowledgement.module_hash);
        seed_lease(&acknowledgement, acknowledgement_identity.clone(), 5);
        run_crashing_child(&format!("ack-{phase}"), &acknowledgement);
        let acknowledgement_status =
            coordinate_consumer_status(&acknowledgement.open(), &acknowledgement_identity)
                .expect("ack crash status")
                .expect("ack consumer");
        assert_eq!(
            acknowledgement_status.checkpoint,
            if committed {
                ConsumerCheckpointV1::After(event(1))
            } else {
                ConsumerCheckpointV1::BeforeFirst
            }
        );
        assert_eq!(acknowledgement_status.live_leases, u8::from(!committed));

        let negative_acknowledgement = ConsumerDatabase::create("crash-nack");
        let negative_acknowledgement_identity =
            identity("worker-a", negative_acknowledgement.module_hash);
        seed_lease(
            &negative_acknowledgement,
            negative_acknowledgement_identity.clone(),
            5,
        );
        run_crashing_child(&format!("nack-{phase}"), &negative_acknowledgement);
        let negative_acknowledgement_status = coordinate_consumer_status(
            &negative_acknowledgement.open(),
            &negative_acknowledgement_identity,
        )
        .expect("nack crash status")
        .expect("nack consumer");
        assert_eq!(
            negative_acknowledgement_status.live_leases,
            u8::from(!committed)
        );
        assert_eq!(
            negative_acknowledgement_status.retries,
            u16::from(committed)
        );

        let seek = ConsumerDatabase::create("crash-seek");
        let seek_identity = identity("worker-a", seek.module_hash);
        seed_acknowledged(&seek, seek_identity.clone());
        run_crashing_child(&format!("seek-{phase}"), &seek);
        let seek_status = coordinate_consumer_status(&seek.open(), &seek_identity)
            .expect("seek crash status")
            .expect("seek consumer");
        assert_eq!(
            seek_status.checkpoint,
            if committed {
                ConsumerCheckpointV1::BeforeFirst
            } else {
                ConsumerCheckpointV1::After(event(1))
            }
        );

        let retire = ConsumerDatabase::create("crash-retire");
        let retire_identity = identity("worker-a", retire.module_hash);
        seed_acknowledged(&retire, retire_identity.clone());
        run_crashing_child(&format!("retire-{phase}"), &retire);
        assert_eq!(
            coordinate_consumer_status(&retire.open(), &retire_identity)
                .expect("retire crash status")
                .is_none(),
            committed
        );

        let recovery = ConsumerDatabase::create("crash-recovery");
        let recovery_identity = identity("worker-a", recovery.module_hash);
        seed_lease(&recovery, recovery_identity.clone(), 5);
        run_crashing_child(&format!("recovery-{phase}"), &recovery);
        let snapshot = recovery
            .open()
            .inspect_event_consumer(recovery_identity.identity_hash())
            .expect("recovery crash inspect")
            .expect("recovery consumer");
        assert!(
            matches!(
                snapshot.deliveries()[0].state(),
                ConsumerDeliveryStateV1::Retry { .. }
                    if committed
            ) || matches!(
                snapshot.deliveries()[0].state(),
                ConsumerDeliveryStateV1::Leased { .. }
                    if !committed
            )
        );
    }
}

#[test]
fn consumer_process_recovery_child() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(CHILD_PATH).expect("child database path"));
    let controller = if mode.ends_with("-before") {
        RedbTestController::abort_before_commit(RedbTestOperation::EventConsumerTransition)
    } else if mode.ends_with("-after") {
        RedbTestController::abort_after_commit(RedbTestOperation::EventConsumerTransition)
    } else {
        panic!("unknown consumer crash phase");
    };
    let mut ports = open_operational(
        RedbStore::open_with_test_controller(path, controller)
            .expect("open controlled child consumer database"),
    );
    let selected = identity("worker-a", fixture_module_hash());
    match mode.split_once('-').map(|(operation, _)| operation) {
        Some("lease") => {
            let _ = coordinate_consumer_lease(
                &mut ports,
                lease_request(selected, vec![event(1)], vec![token(1)], 1, 5),
            );
        }
        Some("ack") => {
            let _ = coordinate_consumer_acknowledgement(
                &mut ports,
                CoordinateConsumerAcknowledgementV1 {
                    identity: selected,
                    event_id: event(1),
                    token: token(1),
                    history_incarnation: 1,
                    observed_at: time(2),
                    selected_prefix: vec![event(1)],
                },
            );
        }
        Some("nack") => {
            let _ = coordinate_consumer_negative_acknowledgement(
                &mut ports,
                CoordinateConsumerNegativeAcknowledgementV1 {
                    identity: selected,
                    event_id: event(1),
                    token: token(1),
                    observed_at: time(2),
                    eligible_at: time(3),
                    selected_prefix: vec![event(1)],
                },
            );
        }
        Some("seek") => {
            let _ =
                coordinate_consumer_seek(&mut ports, &selected, ConsumerCheckpointV1::BeforeFirst);
        }
        Some("retire") => {
            let _ = coordinate_consumer_retire(&mut ports, &selected);
        }
        Some("recovery") => {
            let _ = recover_event_consumers(&mut ports, time(10), 1);
        }
        _ => panic!("unknown consumer crash operation"),
    }
    panic!("the armed consumer failpoint did not terminate the child");
}

fn run_crashing_child(mode: &str, database: &ConsumerDatabase) {
    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("consumer_process_recovery_child")
        .arg("--nocapture")
        .env(CHILD_MODE, mode)
        .env(CHILD_PATH, &database.path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run consumer recovery child");
    assert!(!status.success(), "the armed child must terminate abruptly");
}

fn seed_lease(
    database: &ConsumerDatabase,
    selected: riffdb_storage_api::EventConsumerIdentityV1,
    expires_at: i64,
) {
    coordinate_consumer_lease(
        &mut database.open(),
        lease_request(selected, vec![event(1)], vec![token(1)], 1, expires_at),
    )
    .expect("seed consumer lease");
}

fn seed_acknowledged(
    database: &ConsumerDatabase,
    selected: riffdb_storage_api::EventConsumerIdentityV1,
) {
    seed_lease(database, selected.clone(), 5);
    coordinate_consumer_acknowledgement(
        &mut database.open(),
        CoordinateConsumerAcknowledgementV1 {
            identity: selected,
            event_id: event(1),
            token: token(1),
            history_incarnation: 1,
            observed_at: time(2),
            selected_prefix: vec![event(1)],
        },
    )
    .expect("seed acknowledged consumer");
}

fn fixture_module_hash() -> ReactiveModuleHash {
    let compiled = compile_contract_source(CONTRACT).expect("consumer contract compiles");
    let checked = ValidatedContractBundle::from_compiler_bundle(compiled)
        .expect("consumer contract validates");
    ValidatedReactiveModule::compile(REACTIVE_MODULE, &checked, &[])
        .expect("consumer reactive module compiles")
        .identity()
}

fn open_operational(store: RedbStore) -> RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(startup_inputs())
        .expect("begin structural validation");
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let limit = EvidencePageLimit::new(64).expect("evidence page limit");
    let mut cursor = StructuralEvidenceCursor::start(database_id, open_session_id);
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("read structural evidence")
        {
            StructuralEvidencePage::Page { findings, next, .. } => {
                assert!(findings.is_empty(), "unexpected findings: {findings:?}");
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (history, historical_end) = validate_catalog_history(&mut session)
        .expect("validate catalog history")
        .into_parts();
    let opened = session
        .finish(structural_end, historical_end)
        .expect("finish structural validation");
    let riffdb_catalog::CatalogHistoryOutcome::Ready(history) = history else {
        panic!("consumer fixture must not require catalog migration");
    };
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = opened else {
        panic!("consumer fixture must finish with a clean structural open");
    };
    assert!(history.matches(opened.database_id(), opened.open_session_id()));
    let (_, _, _, dormant): (_, _, _, RedbDormantPorts) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate redb ports")
}

fn startup_inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key ID"));
    StartupValidationInputs::new(
        time(1),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability key inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency key inventory"),
    )
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).expect("database ID")
}

fn principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("wp417-maintainer").expect("actor ID"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_bytes(3)).expect("capability ID"),
        NonZeroU64::MIN,
    )
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
}

fn uuid_bytes(fill: u8) -> [u8; 16] {
    let mut bytes = [fill; 16];
    bytes[6] = 0x70 | (fill & 0x0f);
    bytes[8] = 0x80 | (fill & 0x3f);
    bytes
}

fn identity(
    name: &str,
    module_hash: ReactiveModuleHash,
) -> riffdb_storage_api::EventConsumerIdentityV1 {
    riffdb_storage_api::EventConsumerIdentityV1::new(
        database_id(),
        module_hash,
        ReactiveOperationName::new("RowChanges").expect("operation name"),
        QueryParameterHash::from_bytes([2; 32]),
        EventConsumerName::new(name).expect("consumer name"),
    )
}

fn event(sequence: u64) -> EventId {
    EventId::new(CommitSequence::new(sequence).expect("commit sequence"), 0)
}

fn token(value: u8) -> EventLeaseToken {
    EventLeaseToken::from_bytes([value; 32])
}

fn time(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("timestamp")
}

fn lease_request(
    identity: riffdb_storage_api::EventConsumerIdentityV1,
    selected_events: Vec<EventId>,
    tokens: Vec<EventLeaseToken>,
    observed_at: i64,
    expires_at: i64,
) -> CoordinateConsumerLeaseV1 {
    let batch_limit = u8::try_from(tokens.len()).expect("bounded token count");
    CoordinateConsumerLeaseV1 {
        identity,
        partition_hash: PartitionKeyHash::from_bytes([3; 32]),
        history_incarnation: 1,
        observed_at: time(observed_at),
        expires_at: time(expires_at),
        selected_events,
        tokens,
        batch_limit,
        in_flight_limit: 2,
    }
}
