//! Exact captured active-pointer semantics and Source64 admission.
// req: PRJ-004, STO-012, STO-020, STO-032
use super::*;
use crate::proto_codec::{
    decode_active_catalog_pointer_v1, encode_active_catalog_pointer_v1,
    encode_history_incarnation_v1, encode_message,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, ProjectionSourceWorkingBudgetV1,
};
// Release counters describe completed historical destruction; every other
// gauge must return exactly to its prior live ownership/charge state.
fn assert_live_usage_matches(
    budget: &ProjectionSourceWorkingBudgetV1,
    before: riffdb_types::exact_provider_budget_v1::BudgetObservation,
) {
    let after = budget.observation().unwrap();
    assert_eq!(
        (
            after.fixed_owner_bytes,
            after.reserved_bytes,
            after.owned_bytes,
            after.reservations,
            after.allocations,
            after.active_owners,
            after.retiring_owners,
            after.pinned_owners,
            after.pins
        ),
        (
            before.fixed_owner_bytes,
            before.reserved_bytes,
            before.owned_bytes,
            before.reservations,
            before.allocations,
            before.active_owners,
            before.retiring_owners,
            before.pinned_owners,
            before.pins
        )
    );
    assert!(after.released_allocations >= before.released_allocations);
    assert!(after.released_owners >= before.released_owners);
}
fn pointer(lineage: &str) -> crate::ActiveCatalogPointerV1 {
    crate::ActiveCatalogPointerV1::new(
        ContractLineage::new(lineage).unwrap(),
        ContractVersion::new(1).unwrap(),
        ContractBundleHash::from_bytes([0x32; 32]),
    )
}
#[test]
fn source_active_pointer_refuses_pressure_before_decode_without_affecting_ordinary_reads() {
    let expected = pointer("captured-source-contract");
    let encoded = encode_active_catalog_pointer_v1(&expected).unwrap();
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let baseline = budget.observation().unwrap();
    let available = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - baseline.fixed_owner_bytes
        - baseline.owned_bytes
        - baseline.reserved_bytes;
    let blocked = budget.reserve(available).unwrap();
    let before = budget.observation().unwrap();
    assert!(matches!(
        decode_projection_source_active_catalog_admitted_v1(encoded.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(budget.observation().unwrap(), before);
    assert_eq!(
        decode_active_catalog_pointer_v1(encoded.as_bytes())
            .unwrap()
            .value(),
        &expected
    );
    drop(blocked);
    assert_eq!(budget.observation().unwrap(), baseline);
    let prepared = ProjectionSourceActiveCatalogDecodeAdmissionV1::new(&budget).unwrap();
    assert!(
        budget.observation().unwrap().reserved_bytes > baseline.reserved_bytes,
        "semantic maximum is admitted before any native gate/read"
    );
    let retained = prepared.decode(encoded.as_bytes()).unwrap();
    assert_eq!(&**retained.value(), &expected);
    let alias = retained.value().clone();
    let charged = budget.observation().unwrap();
    assert!(charged.owned_bytes > baseline.owned_bytes);
    drop(retained);
    assert_eq!(
        budget.observation().unwrap(),
        charged,
        "actual shared pointer graph stays admitted to final allocation alias"
    );
    drop(alias);
    assert_live_usage_matches(&budget, baseline);
    assert!(
        budget.observation().unwrap().released_allocations > baseline.released_allocations,
        "settled graph destruction advances the cumulative counter"
    );
}
#[test]
fn source_active_pointer_preserves_full_existing_canonical_and_semantic_checks() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let expected = pointer(&"x".repeat(riffdb_types::MAX_CONTRACT_LINEAGE_BYTES));
    let encoded = encode_active_catalog_pointer_v1(&expected).unwrap();
    let baseline = budget.observation().unwrap();
    let read =
        decode_projection_source_active_catalog_admitted_v1(encoded.as_bytes(), &budget).unwrap();
    assert_eq!(&**read.value(), &expected);
    drop(read);
    assert_live_usage_matches(&budget, baseline);
    let malformed = wire::ActiveCatalogPointerV1 {
        contract_lineage: "captured-source-contract".to_owned(),
        contract_version: 0,
        contract_bundle_hash: vec![0x32; 32],
    };
    let bytes = encode_message("riffdb.storage.v1.ActiveCatalogPointerV1", &malformed).unwrap();
    let before_semantic = budget.observation().unwrap();
    assert!(decode_active_catalog_pointer_v1(bytes.as_bytes()).is_err());
    assert!(matches!(
        decode_projection_source_active_catalog_admitted_v1(bytes.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::Codec(_))
    ));
    assert_live_usage_matches(&budget, baseline);
    assert!(
        budget.observation().unwrap().released_allocations >= before_semantic.released_allocations,
        "semantic refusal never rewinds cumulative destruction accounting"
    );
    let before_checksum = budget.observation().unwrap();
    let mut damaged = encoded.as_bytes().to_vec();
    *damaged.last_mut().unwrap() ^= 1;
    assert!(decode_active_catalog_pointer_v1(&damaged).is_err());
    assert!(matches!(
        decode_projection_source_active_catalog_admitted_v1(&damaged, &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::Codec(_))
    ));
    assert_eq!(
        budget.observation().unwrap(),
        before_checksum,
        "checksum refusal does not allocate or settle any graph"
    );
    let wire = wire::ActiveCatalogPointerV1 {
        contract_lineage: expected.lineage().as_str().to_owned(),
        contract_version: expected.contract_version().get(),
        contract_bundle_hash: expected.bundle_hash().as_bytes().to_vec(),
    };
    let legacy=riffdb_proto::envelope::encode_v1(<wire::ActiveCatalogPointerV1 as riffdb_proto::durable::ReadableRecordMessage>::record_schema(),&prost::Message::encode_to_vec(&wire)).unwrap();
    let legacy_read =
        decode_projection_source_active_catalog_admitted_v1(&legacy, &budget).unwrap();
    assert_eq!(&**legacy_read.value(), &expected);
    drop(legacy_read);
    assert_live_usage_matches(&budget, baseline);
    let wrong_role = encode_history_incarnation_v1(1).unwrap();
    let before_role = budget.observation().unwrap();
    assert!(matches!(
        decode_projection_source_active_catalog_admitted_v1(wrong_role.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::Codec(_))
    ));
    assert_eq!(
        budget.observation().unwrap(),
        before_role,
        "wrong-role refusal precedes graph allocation"
    );
}
