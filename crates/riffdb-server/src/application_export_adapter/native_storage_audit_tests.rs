//! Public audit uses the existing exporter and retains only symbolic metadata.
use super::*;
use riffdb_storage_api::{StoredAdministrationAuditRecordV1 as Audit, proto_codec::*};

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len() <= 32 * 1024 && text.len().is_multiple_of(2));
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn bundle() -> ValidatedContractBundle {
    ValidatedContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin"))
        .unwrap()
}

fn assert_symbolic_audit(record: Audit, kind: &str) -> Vec<u8> {
    let sequence = record.administration_sequence().get();
    let source = ApplicationExportSourceRecordV1::PublicAudit(Box::new(record));
    assert_eq!(source.class(), ApplicationExportClassV1::PublicAudit);
    let line = serialize_record(&source, &bundle(), None).unwrap();
    let expected = format!(
        "{{\"administration_sequence\":\"{sequence}\",\"class\":\"public_audit\",\"kind\":\"{kind}\"}}"
    );
    assert_eq!(line.as_bytes(), expected.as_bytes());
    let json: Value = serde_json::from_slice(line.as_bytes()).unwrap();
    let object = json.as_object().unwrap();
    assert_eq!(object.len(), 3);
    assert_eq!(
        object.keys().map(String::as_str).collect::<Vec<_>>(),
        ["administration_sequence", "class", "kind"]
    );
    // An exhaustive three-field shape excludes origin, predecessor scalars,
    // source locators/witnesses, commitments and any native payload or path.
    line.as_bytes().to_vec()
}

// req: EXP-003, EXP-004, EXP-007, EXP-010
#[test]
fn native_storage_public_audit_exports_only_exact_symbolic_storage_metadata() {
    let vectors =
        include_str!("../../../../fixtures/proto/durable-native-transition-v1-wire-vectors.txt");
    let expected = include_str!("../../../../fixtures/export/native-storage-public-audit-v1.jsonl")
        .lines()
        .collect::<Vec<_>>();
    assert_eq!(expected.len(), 2);
    let budget = riffdb_types::ProjectionSourceWorkingBudgetV1::new();
    let mut cases = Vec::new();
    for row in vectors.lines().skip(2) {
        let fields = row.split('\t').collect::<Vec<_>>();
        assert_eq!(fields.len(), 5);
        let mut actual = None;
        for framing in &fields[3..5] {
            let encoded = unhex(framing);
            let item = decode_native_control_audit_admitted_v1(&encoded, &budget)
                .unwrap()
                .unwrap();
            assert_eq!(item.encoded_content_charge().get(), encoded.len());
            let (record, charge) = item.into_parts();
            match &record {
                Audit::NativeCatalogTransition(value) => {
                    assert!(matches!(fields[0], "fresh-v3" | "v2-to-v3"));
                    assert_eq!(value.encoded_content_charge(), charge);
                }
                Audit::SourceInheritance(value) => {
                    assert!(matches!(
                        fields[0],
                        "restore-inheritance" | "promotion-inheritance"
                    ));
                    assert_eq!(value.encoded_content_charge(), charge);
                }
                _ => panic!("frozen closed native control inventory"),
            }
            let line = assert_symbolic_audit(record, "storage");
            let golden = if fields[0] == "fresh-v3" {
                expected[0]
            } else {
                expected[1]
            };
            assert_eq!(line, golden.as_bytes());
            if let Some(previous) = &actual {
                assert_eq!(&line, previous);
            }
            actual = Some(line);
        }
        cases.push(fields[0]);
    }
    assert_eq!(
        cases,
        [
            "fresh-v3",
            "v2-to-v3",
            "restore-inheritance",
            "promotion-inheritance"
        ]
    );
    let observation = budget.observation().unwrap();
    assert_eq!(observation.owned_bytes, 0);
    assert_eq!(observation.reserved_bytes, 0);
}

fn durable_vector(record_type: &str) -> Vec<u8> {
    let vectors = include_str!("../../../../fixtures/proto/durable-wire-vectors-v2.txt");
    let rows = vectors
        .lines()
        .skip(2)
        .filter_map(|line| {
            let fields = line.split('\t').collect::<Vec<_>>();
            assert_eq!(fields.len(), 3);
            (fields[0] == record_type).then(|| unhex(fields[2]))
        })
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    rows.into_iter().next().unwrap()
}

// req: EXP-003, EXP-004
#[test]
fn native_storage_public_audit_preserves_every_historical_kind_and_shape() {
    let catalog = decode_catalog_administration_v1(&durable_vector(
        "riffdb.storage.v1.StoredCatalogAdministrationV1",
    ))
    .unwrap()
    .into_parts()
    .0;
    let query = riffdb_storage_api::StoredQueryModuleAdministrationV1::from_stored_parts(
        catalog.administration_sequence(),
        catalog.request_id(),
        catalog.timestamp(),
        catalog.principal().clone(),
        None,
        riffdb_storage_api::ActiveQueryModulePointerV1::new(
            riffdb_types::QueryModuleName::new("synthetic_audit").unwrap(),
            riffdb_types::QueryModuleVersion::new(1).unwrap(),
            QueryModuleHash::from_bytes([0x51; 32]),
            catalog.activated().lineage().clone(),
            catalog.activated().contract_version(),
            catalog.activated().bundle_hash(),
        ),
        None,
    );
    let query_bytes = encode_query_module_administration_v1(&query).unwrap();
    let query = decode_query_module_administration_v1(query_bytes.as_bytes())
        .unwrap()
        .into_parts()
        .0;
    let reactive = riffdb_storage_api::StoredReactiveModuleAdministrationV1::from_stored_parts(
        catalog.administration_sequence(),
        catalog.request_id(),
        catalog.timestamp(),
        catalog.principal().clone(),
        ReactiveModuleHash::from_bytes([0x52; 32]),
        None,
    );
    let reactive_bytes = encode_reactive_module_administration_v1(&reactive).unwrap();
    let reactive = decode_reactive_module_administration_v1(reactive_bytes.as_bytes())
        .unwrap()
        .into_parts()
        .0;
    let retention = riffdb_storage_api::StoredRetentionAdministrationV1::new(
        catalog.administration_sequence(),
        riffdb_storage_api::RetentionAdministrationAction::ProjectionDetach,
        riffdb_types::ProjectionId::first(),
        "synthetic retention reason",
        catalog.timestamp(),
    )
    .unwrap();
    let retention_bytes = encode_retention_administration_v1(&retention).unwrap();
    let retention = decode_retention_administration_v1(retention_bytes.as_bytes())
        .unwrap()
        .into_parts()
        .0;
    let replication = unhex(
        include_str!("../../../../fixtures/proto/durable-replication-administration-v1.hex")
            .lines()
            .next()
            .unwrap(),
    );
    let fence = include_str!("../../../../fixtures/replication/primary-fence-v1.hex")
        .lines()
        .find_map(|line| line.strip_prefix("receipt "))
        .map(unhex)
        .unwrap();
    let promotion =
        include_str!("../../../../fixtures/replication/promotion-administration-v1.hex")
            .lines()
            .find_map(|line| line.strip_prefix("cutover "))
            .map(unhex)
            .unwrap();
    let records = [
        (Audit::Catalog(catalog), "contract"),
        (Audit::QueryModule(query), "query_module"),
        (Audit::ReactiveModule(reactive), "reactive_module"),
        (
            Audit::Capability(
                decode_capability_administration_v1(&durable_vector(
                    "riffdb.storage.v1.CapabilityAdministrationAuditV1",
                ))
                .unwrap()
                .into_parts()
                .0,
            ),
            "capability",
        ),
        (
            Audit::Service(
                decode_service_audit_record_v1(&durable_vector(
                    "riffdb.storage.v1.ServiceAuditRecordV1",
                ))
                .unwrap()
                .into_parts()
                .0,
            ),
            "service",
        ),
        (Audit::Retention(retention), "retention"),
        (
            Audit::Replication(Box::new(
                decode_replication_administration_v1(&replication)
                    .unwrap()
                    .into_parts()
                    .0,
            )),
            "replication",
        ),
        (
            Audit::PrimaryFence(Box::new(
                decode_primary_fence_administration_v1(&fence)
                    .unwrap()
                    .into_parts()
                    .0,
            )),
            "replication",
        ),
        (
            Audit::Promotion(Box::new(
                decode_promotion_administration_v1(&promotion)
                    .unwrap()
                    .into_parts()
                    .0,
            )),
            "replication",
        ),
    ];
    assert_eq!(records.len(), 9);
    for (record, kind) in records {
        assert_symbolic_audit(record, kind);
    }
}

struct AuditAuthorizationClock;
impl riffdb_policy::AuthorizationClock for AuditAuthorizationClock {
    fn now(&self) -> Result<Timestamp, riffdb_policy::AuthorizationClockError> {
        Ok(Timestamp::new(150, 0).unwrap())
    }
}

// req: EXP-004
#[test]
fn native_storage_public_audit_keeps_current_explicit_grant_at_every_release() {
    use riffdb_policy::{
        ApplicationExportAuthorizationRequestV1, ApplicationExportDecisionV1,
        ApplicationExportPolicyOperationV1, CurrentAuthorizer, NoopAuthorizationTelemetry,
        PolicyCode,
    };
    use riffdb_testkit::authorization::{
        AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
    };
    use riffdb_types::{
        Audience, CapabilityApplicationExportGrantV1, CapabilityExportGrantV1, CapabilityGrantV1,
        CapabilityPermissionsV1, Environment, PartitionScopeV1, TenantScope,
    };
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [0x63; 10]).unwrap();
    let environment = Environment::new("test").unwrap();
    let lineage = ContractLineage::new("TicketDesk").unwrap();
    for public_audit in [false, true] {
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            PartitionScopeV1::All,
            CapabilityPermissionsV1::new(Vec::new()).unwrap(),
            Vec::new(),
            std::num::NonZeroU16::new(100).unwrap(),
            Vec::new(),
        )
        .unwrap()
        .with_export(
            CapabilityExportGrantV1::new(vec![
                CapabilityApplicationExportGrantV1::new(
                    lineage.clone(),
                    CapabilityApplicationExportScopeV1::WholeApplication,
                    true,
                    false,
                    false,
                    public_audit,
                )
                .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap();
        let fixture = AuthorizationFixture::new(AuthorizationFixtureConfig::new(
            database,
            environment.clone(),
            ActorId::new("synthetic-audit-operator").unwrap(),
            ActorKind::Service,
            Audience::new("grpc").unwrap(),
            AuthorizationFixtureTimes::new(
                Timestamp::new(100, 0).unwrap(),
                Timestamp::new(1_000, 0).unwrap(),
                Timestamp::new(150, 0).unwrap(),
            ),
            grant,
        ))
        .unwrap();
        let resolver = fixture.current_capability_resolver();
        let clock = AuditAuthorizationClock;
        let telemetry = NoopAuthorizationTelemetry;
        let authorizer =
            CurrentAuthorizer::new(&resolver, &clock, &telemetry, database, environment.clone());
        let request = |operation| {
            ApplicationExportAuthorizationRequestV1::new(
                ApplicationExportOperationId::from_unix_milliseconds_and_random(2, [0x64; 10])
                    .unwrap(),
                ApplicationExportSelectionV1::new(
                    lineage.clone(),
                    CapabilityApplicationExportScopeV1::WholeApplication,
                    true,
                    false,
                    false,
                    true,
                )
                .unwrap(),
                operation,
            )
        };
        for operation in [
            ApplicationExportPolicyOperationV1::Start,
            ApplicationExportPolicyOperationV1::Page,
            ApplicationExportPolicyOperationV1::Status,
            ApplicationExportPolicyOperationV1::Cancel,
        ] {
            let decision = authorizer
                .authorize_application_export(fixture.authenticated_principal(), request(operation))
                .unwrap();
            if public_audit {
                assert!(matches!(decision, ApplicationExportDecisionV1::Allow(_)));
            } else {
                assert!(matches!(
                    decision,
                    ApplicationExportDecisionV1::Deny(PolicyCode::MissingPermission)
                ));
            }
        }
        fixture
            .revoke_current(Timestamp::new(150, 0).unwrap())
            .unwrap();
        assert!(matches!(
            authorizer
                .authorize_application_export(
                    fixture.authenticated_principal(),
                    request(ApplicationExportPolicyOperationV1::Page),
                )
                .unwrap(),
            ApplicationExportDecisionV1::Deny(PolicyCode::InactiveOrStaleCapability)
        ));
    }
    // The existing current authorizer selects PublicAudit before any storage
    // scan or serialization. Storage kind adds neither a grant nor a bypass.
}
