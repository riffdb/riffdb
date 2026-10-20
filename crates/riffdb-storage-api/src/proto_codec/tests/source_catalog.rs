//! Opaque Source-envelope heap diagnostics on the ordinary test stack.
//! Actual unchanged compiler fixtures; no Native/stack/Source64 authority claim.
// req: OQ-040, PRJ-004
use super::*;
use crate::StoredContractBundleV1;
use riffdb_types::{
    OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1, PROJECTION_SOURCE_WORKING_MEMORY_BYTES,
    ProjectionSourceWorkAllocationV1, ProjectionSourceWorkingBudgetV1,
};

const TICKETDESK: &[u8] =
    include_bytes!("../../../../../examples/ticketdesk/generated/riffdb.contract.bundle");
const LIFECYCLE: &[u8] =
    include_bytes!("../../../../../examples/ticketdesk/lifecycle/generated/riffdb.contract.bundle");
fn genuine_record(bytes: &[u8]) -> StoredContractBundleV1 {
    // Ordinary fixture decode earns the actual compiler identity; this test
    // never inserts a row, supplies Native authority or interprets opaque bytes
    // in the Source envelope mapper under test.
    let bundle = riffdb_contract_ir::ContractBundle::decode(bytes).unwrap();
    StoredContractBundleV1::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        bytes.to_vec(),
    )
    .unwrap()
}
fn assert_original_gauges(source: &ProjectionSourceWorkingBudgetV1, owned: u64, reserved: u64) {
    let observed = source.observation().unwrap();
    assert_eq!(observed.owned_bytes, owned);
    assert_eq!(observed.reserved_bytes, reserved);
}
fn decode(
    bytes: &[u8],
    source: &ProjectionSourceWorkingBudgetV1,
) -> Result<
    ProjectionSourceWorkAllocationV1<StoredContractBundleV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    decode_source_catalog_bundle_admitted_v1(bytes, source)
}

#[test]
fn source_catalog_envelope_genuine_both_framings_keep_original_ledger_and_last_lineage() {
    for canonical in [TICKETDESK, LIFECYCLE] {
        let record = genuine_record(canonical);
        let legacy = encode_contract_bundle_v1(&record).unwrap();
        let compact = transcode_durable_record_to_v2(legacy.as_bytes()).unwrap();
        for bytes in [legacy.as_bytes(), compact.as_bytes()] {
            let source = ProjectionSourceWorkingBudgetV1::new();
            // This numerical reservation represents already-live SAME-ledger
            // work, not a fabricated Native stage/resource profile theorem.
            let existing = source.reserve(2 * 1024 * 1024).unwrap();
            let before = source.observation().unwrap();
            let ordinary = decode_contract_bundle_v1(bytes).unwrap();
            let value = decode(bytes, &source).unwrap();
            assert!(value.belongs_to(&source));
            assert_eq!(&*value, ordinary.value());
            assert_eq!(value.canonical_bytes(), canonical);
            let live = source.observation().unwrap();
            assert!(live.owned_bytes > before.owned_bytes);
            assert_eq!(live.reserved_bytes, before.reserved_bytes);
            let lineage = value.lineage().clone();
            let alias = value.clone();
            drop(value);
            assert_original_gauges(&source, live.owned_bytes, before.reserved_bytes);
            drop(alias);
            assert!(
                source.observation().unwrap().owned_bytes > before.owned_bytes,
                "the actual shared lineage remains charged after bundle retirement"
            );
            drop(lineage);
            assert_original_gauges(&source, before.owned_bytes, before.reserved_bytes);
            assert!(
                source.peak_admitted_bytes().unwrap() <= PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            );
            drop(existing);
            assert_original_gauges(&source, 0, 0);
        }
    }
}

#[test]
fn source_catalog_envelope_refuses_complete_work_pressure_before_ownership_and_retries() {
    for canonical in [TICKETDESK, LIFECYCLE] {
        let record = genuine_record(canonical);
        let legacy = encode_contract_bundle_v1(&record).unwrap();
        let compact = transcode_durable_record_to_v2(legacy.as_bytes()).unwrap();
        for bytes in [legacy.as_bytes(), compact.as_bytes()] {
            let source = ProjectionSourceWorkingBudgetV1::new();
            let before = source.observation().unwrap();
            let free = PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes;
            let available = OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 - 1;
            // The genuine opaque output is small, but its complete construction
            // also requires the actual census. No temporary fresh pool can help.
            let output = riffdb_types::owned_allocation_capacity_bound_v1(canonical.len(), 1)
                .unwrap()
                + riffdb_types::OwnedCapacityTextV1::retained_allocation_upper_bound(
                    record.lineage().as_bytes().len(),
                )
                .unwrap()
                + riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner::<
                    StoredContractBundleV1,
                >::settlement_root_capacity_upper_bound()
                .unwrap();
            assert!(output < available);
            let pressure = source.reserve(free - available).unwrap();
            let blocked = source.observation().unwrap();
            assert!(matches!(
                decode(bytes, &source),
                Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
            ));
            assert_eq!(
                source.observation().unwrap(),
                blocked,
                "refused work neither constructs nor settles a partial graph"
            );
            assert_eq!(
                decode_contract_bundle_v1(bytes).unwrap().value(),
                &record,
                "ordinary decode stays independent of Source pressure"
            );
            assert_eq!(source.observation().unwrap(), blocked);
            drop(pressure);
            assert_original_gauges(&source, before.owned_bytes, before.reserved_bytes);
            let value = decode(bytes, &source).unwrap();
            assert_eq!(&*value, &record);
            drop(value);
            assert_original_gauges(&source, before.owned_bytes, before.reserved_bytes);
        }
    }
}

#[test]
fn source_catalog_envelope_malformed_and_semantic_errors_match_ordinary_and_refund() {
    use prost::Message;
    use riffdb_proto::storage::v1 as wire;
    for canonical in [TICKETDESK, LIFECYCLE] {
        let record = genuine_record(canonical);
        let valid = encode_contract_bundle_v1(&record).unwrap();
        let compact = transcode_durable_record_to_v2(valid.as_bytes()).unwrap();
        let mut cases = Vec::new();
        for bytes in [valid.as_bytes(), compact.as_bytes()] {
            cases.push(bytes[..bytes.len() - 1].to_vec());
            let mut checksum = bytes.to_vec();
            let last = checksum.len() - 1;
            checksum[last] ^= 0x80;
            cases.push(checksum);
        }
        for (lineage, version, hash, body) in [
            (
                record.lineage().as_str().to_owned(),
                0,
                record.bundle_hash().as_bytes().to_vec(),
                canonical.to_vec(),
            ),
            (
                String::new(),
                record.contract_version().get(),
                record.bundle_hash().as_bytes().to_vec(),
                canonical.to_vec(),
            ),
            (
                record.lineage().as_str().to_owned(),
                record.contract_version().get(),
                vec![0],
                canonical.to_vec(),
            ),
            (
                record.lineage().as_str().to_owned(),
                record.contract_version().get(),
                record.bundle_hash().as_bytes().to_vec(),
                Vec::new(),
            ),
        ] {
            let payload = wire::StoredContractBundleV1 {
                contract_lineage: lineage,
                contract_version: version,
                contract_bundle_hash: hash,
                canonical_bundle: body,
            }
            .encode_to_vec();
            let legacy = malformed_semantic::raw_envelope(
                "riffdb.storage.v1.StoredContractBundleV1",
                payload,
            );
            // These are deliberately rejected wire/semantic mutations of the
            // real fixture, never stored or treated as successful Source input.
            cases.push(legacy);
        }
        let source = ProjectionSourceWorkingBudgetV1::new();
        let existing = source.reserve(1024 * 1024).unwrap();
        let before = source.observation().unwrap();
        for bytes in cases {
            let ordinary = decode_contract_bundle_v1(&bytes).unwrap_err();
            match decode(&bytes, &source) {
                Err(ProjectionSourceRecordDecodeErrorV1::Codec(error)) => {
                    assert_eq!(error.kind(), ordinary.kind())
                }
                Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable) => {
                    panic!("malformed fixture must not masquerade as pressure")
                }
                Ok(_) => panic!("ordinary-rejected envelope must remain rejected"),
            }
            assert_original_gauges(&source, before.owned_bytes, before.reserved_bytes);
        }
        // An error cannot poison the same originating ledger for a later real
        // fixture. Error/scratch/data destruction precedes all refunds.
        let value = decode(valid.as_bytes(), &source).unwrap();
        assert_eq!(&*value, &record);
        drop(value);
        assert_original_gauges(&source, before.owned_bytes, before.reserved_bytes);
        drop(existing);
        assert_original_gauges(&source, 0, 0);
    }
}
