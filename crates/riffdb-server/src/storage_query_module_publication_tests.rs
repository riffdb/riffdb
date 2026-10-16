//! Real-storage regression for a cold reader publishing an obsolete pointer.

use super::*;
use std::num::NonZeroU64;
use std::sync::mpsc;
use std::time::Duration;

use riffdb_catalog::{ValidatedContractBundle, ValidatedQueryModule};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{NamedQuerySource, QueryModuleCandidate};
use riffdb_storage_api::{AuditPrincipalV1, QueryModuleActiveExpectationV1};
use riffdb_types::{ActorId, ActorKind, QueryModuleName, QueryModuleVersion, RequestId, Timestamp};

use crate::real_storage_support::RealStorage;

fn principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("query-publication-operator").expect("bounded actor"),
        ActorKind::Human,
        CapabilityId::from_unix_milliseconds_and_random(3, [0x6c; 10])
            .expect("capability identity"),
        NonZeroU64::MIN,
    )
}

fn request(seed: u8) -> RequestId {
    RequestId::from_unix_milliseconds_and_random(u64::from(seed), [seed; 10])
        .expect("request identity")
}

fn module(contract: &ValidatedContractBundle, version: u64) -> StoredQueryModuleV1 {
    ValidatedQueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("module name"),
            QueryModuleVersion::new(version).expect("module version"),
            vec![
                NamedQuerySource::new(
                    "ListTickets",
                    include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
                )
                .expect("query source"),
            ],
        )
        .expect("module candidate"),
        contract,
    )
    .expect("compile genuine module")
    .to_stored()
    .expect("encode module")
}

fn activate_module(
    storage: &mut SharedRedbOperationalPorts,
    module: StoredQueryModuleV1,
    expected: QueryModuleActiveExpectationV1,
    seed: u8,
) -> Result<QueryModuleActivationResult, StorageError> {
    storage.activate_query_module(&QueryModuleActivationIntentV1::new(
        expected,
        module,
        request(seed),
        principal(),
        Timestamp::new(i64::from(seed), 0).expect("timestamp"),
        None,
    ))
}

// req: END-007, END-008, REC-001
#[test]
fn cold_query_module_fill_cannot_replace_a_concurrently_activated_pointer() {
    let real = RealStorage::open("query-module-cold-publication");
    let mut writer = real.storage.clone();
    let contract = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("compile contract"),
    )
    .expect("validate contract");
    assert!(matches!(
        writer
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                contract.to_stored().expect("encode contract"),
                request(0x51),
                principal(),
                Timestamp::new(1_000, 0).expect("timestamp"),
                None,
            ))
            .expect("activate contract"),
        CatalogActivationResult::Activated { .. }
    ));
    let first = module(&contract, 1);
    let second = module(&contract, 2);
    assert_ne!(first.module_hash(), second.module_hash());
    assert!(matches!(
        activate_module(
            &mut writer,
            first.clone(),
            QueryModuleActiveExpectationV1::Absent,
            0x52,
        )
        .expect("activate first module"),
        QueryModuleActivationResult::Activated { .. }
    ));

    // Drop only rebuildable process-cache state so the actual cold branch is
    // exercised. Durable pointers, audits and module bodies remain untouched.
    *real.storage.query_modules.write().expect("view lock") =
        CurrentQueryModuleViewState::default();
    let reader = real.storage.clone();
    let selected = contract.clone();
    let (entered, captured) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let cold = std::thread::spawn(move || {
        QUERY_MODULE_COLD_READ_BEFORE_PUBLICATION.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                entered.send(()).expect("announce exact cold-read boundary");
                // Timeout is a deadlock watchdog, not schedule synchronization.
                resume
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release cold publication");
            }));
        });
        reader.read_active_query_module(
            selected.lineage(),
            selected.contract_version(),
            selected.bundle_hash(),
        )
    });
    let reached = captured.recv_timeout(Duration::from_secs(10));
    let activation = if reached.is_ok() {
        Some(activate_module(
            &mut writer,
            second.clone(),
            QueryModuleActiveExpectationV1::Exact(first.module_hash()),
            0x53,
        ))
    } else {
        None
    };
    // Release and join before inspecting outcomes so failed setup cannot leave
    // a test-owned thread waiting at the explicit boundary.
    let _ = release.send(());
    let cold_result = cold.join().expect("cold reader joins");
    reached.expect("cold lookup reaches the publication boundary");
    assert!(matches!(
        activation
            .expect("activation attempted")
            .expect("activate successor module"),
        QueryModuleActivationResult::Activated { .. }
    ));
    assert!(cold_result.expect("cold read succeeds").is_some());
    let current = real
        .storage
        .read_active_query_module(
            contract.lineage(),
            contract.contract_version(),
            contract.bundle_hash(),
        )
        .expect("subsequent production lookup succeeds")
        .expect("successor pointer remains active");
    assert_eq!(
        current,
        ActiveQueryModulePointerV1::from_module(&second),
        "an earlier cold fill must not regress the exact current pointer"
    );
}

// req: END-007, END-008, REC-001
#[test]
fn cold_absent_query_module_fill_observes_a_concurrently_activated_pointer() {
    let real = RealStorage::open("query-module-cold-absence-publication");
    let mut writer = real.storage.clone();
    let contract = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("compile contract"),
    )
    .expect("validate contract");
    assert!(matches!(
        writer
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                contract.to_stored().expect("encode contract"),
                request(0x61),
                principal(),
                Timestamp::new(1_000, 0).expect("timestamp"),
                None,
            ))
            .expect("activate contract without a module"),
        CatalogActivationResult::Activated { .. }
    ));
    let first = module(&contract, 1);
    assert!(
        real.storage
            .query_modules
            .read()
            .expect("read view")
            .active(
                contract.lineage(),
                contract.contract_version(),
                contract.bundle_hash(),
            )
            .is_none(),
        "a new exact contract starts with a genuine cold module lookup"
    );
    let reader = real.storage.clone();
    let selected = contract.clone();
    let (entered, captured) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let cold = std::thread::spawn(move || {
        QUERY_MODULE_COLD_READ_BEFORE_PUBLICATION.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                entered.send(()).expect("announce captured absence");
                // Timeout is a deadlock watchdog, not schedule synchronization.
                resume
                    .recv_timeout(Duration::from_secs(10))
                    .expect("release absent fill");
            }));
        });
        reader.read_active_query_module(
            selected.lineage(),
            selected.contract_version(),
            selected.bundle_hash(),
        )
    });
    let reached = captured.recv_timeout(Duration::from_secs(10));
    let activation = if reached.is_ok() {
        Some(activate_module(
            &mut writer,
            first.clone(),
            QueryModuleActiveExpectationV1::Absent,
            0x62,
        ))
    } else {
        None
    };
    let _ = release.send(());
    let cold_result = cold.join().expect("cold reader joins");
    reached.expect("absent lookup reaches the publication boundary");
    assert!(matches!(
        activation
            .expect("activation attempted")
            .expect("activate first module"),
        QueryModuleActivationResult::Activated { .. }
    ));
    let expected = Some(ActiveQueryModulePointerV1::from_module(&first));
    assert_eq!(
        cold_result.expect("cold read succeeds"),
        expected,
        "the publication observed under the fill guard supersedes old absence"
    );
    assert_eq!(
        real.storage
            .read_active_query_module(
                contract.lineage(),
                contract.contract_version(),
                contract.bundle_hash(),
            )
            .expect("subsequent production lookup succeeds"),
        expected,
        "an absent fill must not retain a negative after genuine activation"
    );
}
