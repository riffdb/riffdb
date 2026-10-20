//! Private full heap/parser component diagnostics; no public Source entry.
//! Ordinary test-harness stack is NOT a Source-admitted physical stack proof.
//! Numerical IR ownership tests only, never native source/cut authority.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{
    PROJECTION_SOURCE_WORKING_MEMORY_BYTES, ProjectionSourceWorkingBudgetV1,
};

const TICKETDESK: &[u8] =
    include_bytes!("../../../../examples/ticketdesk/generated/riffdb.contract.bundle");
const LIFECYCLE: &[u8] =
    include_bytes!("../../../../examples/ticketdesk/lifecycle/generated/riffdb.contract.bundle");

#[test]
fn private_source_heap_kernel_real_fixture_canonical_graph_keeps_original_ledger() {
    for (fixture, encoded) in [("TicketDesk", TICKETDESK), ("lifecycle", LIFECYCLE)] {
        let source = ProjectionSourceWorkingBudgetV1::new();
        let before = source.observation().unwrap();
        eprintln!("private_source_heap_kernel_start fixture={fixture}");
        let decoded = SourceBundleConstructionV1::new(&source)
            .and_then(|construction| construction.decode_all(encoded))
            .unwrap_or_else(|error| {
                eprintln!("private_source_heap_kernel_refused fixture={fixture} unavailable={} invalid={:?}",
                    error.is_admission_unavailable(), error.validation_error());
                panic!("private heap/parser component could not materialize unchanged fixture");
            });
        assert!(decoded.belongs_to(&source));
        assert_eq!(decoded.canonical_bytes(), encoded);
        assert!(!decoded.commands().is_empty());
        let held = source.observation().unwrap();
        assert!(
            held.reserved_bytes + held.owned_bytes > before.reserved_bytes + before.owned_bytes
        );
        assert!(source.peak_admitted_bytes().unwrap() <= PROJECTION_SOURCE_WORKING_MEMORY_BYTES);
        // Phase-order diagnostic: keep the real settled graph live, then try
        // the same numerical40MiB reservation after constructor work retires.
        // This is not a real native staging factory or combined physical quote.
        let decode_peak = source.peak_admitted_bytes().unwrap();
        let stage = source.reserve(40 * 1024 * 1024);
        let post_admitted = stage.is_ok();
        let post_peak = source.peak_admitted_bytes().unwrap();
        eprintln!(
            "private_source_heap_settled_phase fixture={fixture} decode_peak={decode_peak} final_owned={} final_reserved={} post40_admitted={post_admitted} post_peak={post_peak}",
            held.owned_bytes, held.reserved_bytes
        );
        drop(stage);
        assert_eq!(source.observation().unwrap().owned_bytes, held.owned_bytes);
        assert_eq!(
            source.observation().unwrap().reserved_bytes,
            held.reserved_bytes
        );
        let alias = decoded.clone();
        assert!(alias.belongs_to(&source));
        drop(decoded);
        assert_eq!(source.observation().unwrap().owned_bytes, held.owned_bytes);
        drop(alias);
        let after = source.observation().unwrap();
        assert_eq!(after.owned_bytes, before.owned_bytes);
        assert_eq!(after.reserved_bytes, before.reserved_bytes);
        assert_eq!(after.fixed_owner_bytes, before.fixed_owner_bytes);
    }
}

#[test]
fn private_source_heap_kernel_full_original_ledger_refuses_before_materialization() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let before = source.observation().unwrap();
    let already = before
        .owned_bytes
        .checked_add(before.reserved_bytes)
        .unwrap()
        .checked_add(before.fixed_owner_bytes)
        .unwrap();
    let pressure = source
        .reserve(PROJECTION_SOURCE_WORKING_MEMORY_BYTES - already)
        .unwrap();
    let error = SourceBundleConstructionV1::new(&source)
        .and_then(|construction| construction.decode_all(LIFECYCLE))
        .err()
        .expect("full originating allowance cannot construct IR");
    assert!(error.is_admission_unavailable());
    drop(error);
    let held = source.observation().unwrap();
    assert_eq!(held.owned_bytes, before.owned_bytes);
    assert_eq!(
        held.reserved_bytes,
        pressure.bytes() + before.reserved_bytes
    );
    drop(pressure);
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
}

const COMMANDLESS: &[u8] = include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin");

#[test]
fn private_source_heap_kernel_commandless_frozen_bundle_preserves_canonical_bytes() {
    let source = ProjectionSourceWorkingBudgetV1::new();
    let before = source.observation().unwrap();
    let decoded = SourceBundleConstructionV1::new(&source)
        .and_then(|construction| construction.decode_all(COMMANDLESS))
        .unwrap();
    assert!(decoded.belongs_to(&source));
    assert!(decoded.commands().is_empty());
    assert_eq!(decoded.canonical_bytes(), COMMANDLESS);
    drop(decoded);
    assert_eq!(
        source.observation().unwrap().owned_bytes,
        before.owned_bytes
    );
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
}

#[test]
fn private_source_heap_kernel_reports_forty_mib_numeric_pressure_without_leaking() {
    // This reservation is artificial numerical pressure, not a native/staging
    // resource quote or a theorem about a combined physical Source phase.
    for (fixture, encoded) in [("TicketDesk", TICKETDESK), ("lifecycle", LIFECYCLE)] {
        let source = ProjectionSourceWorkingBudgetV1::new();
        let before = source.observation().unwrap();
        let pressure = source.reserve(40 * 1024 * 1024).unwrap();
        let result = SourceBundleConstructionV1::new(&source)
            .and_then(|construction| construction.decode_all(encoded));
        let admitted = result.is_ok();
        match result {
            Ok(decoded) => {
                assert!(decoded.belongs_to(&source));
                assert_eq!(decoded.canonical_bytes(), encoded);
                drop(decoded);
            }
            Err(error) => {
                assert!(error.is_admission_unavailable());
                assert!(error.validation_error().is_none());
                drop(error);
            }
        }
        let peak = source.peak_admitted_bytes().unwrap();
        eprintln!(
            "private_source_heap_40m_outcome fixture={fixture} admitted={admitted} peak={peak}"
        );
        assert!(peak <= PROJECTION_SOURCE_WORKING_MEMORY_BYTES);
        let held = source.observation().unwrap();
        assert_eq!(held.owned_bytes, before.owned_bytes);
        assert_eq!(
            held.reserved_bytes,
            before.reserved_bytes + pressure.bytes()
        );
        drop(pressure);
        assert_eq!(
            source.observation().unwrap().reserved_bytes,
            before.reserved_bytes
        );
    }
}

#[test]
fn private_source_heap_kernel_preserves_ordinary_corruption_errors_and_refunds() {
    // Locate the actual frozen root digest rather than hardcoding a wire offset
    // or inventing a second semantic parser for this corruption control.
    let ordinary = ContractBundle::decode(LIFECYCLE).unwrap();
    let root = ordinary.plan_root_hash();
    let offset = LIFECYCLE
        .windows(root.as_bytes().len())
        .position(|bytes| bytes == root.as_bytes())
        .unwrap();
    assert_eq!(
        Some(offset),
        LIFECYCLE
            .windows(root.as_bytes().len())
            .rposition(|bytes| bytes == root.as_bytes())
    );
    let mut wrong_root = LIFECYCLE.to_vec();
    wrong_root[offset] ^= 1;
    assert!(matches!(
        ContractBundle::decode(&wrong_root),
        Err(IrValidationError::HashMismatch { .. })
    ));
    let mut malformed = LIFECYCLE.to_vec();
    malformed[0] ^= 1;
    for encoded in [
        malformed.as_slice(),
        &LIFECYCLE[..LIFECYCLE.len() - 1],
        wrong_root.as_slice(),
    ] {
        let expected = ContractBundle::decode(encoded).err().unwrap();
        let source = ProjectionSourceWorkingBudgetV1::new();
        let before = source.observation().unwrap();
        let error = SourceBundleConstructionV1::new(&source)
            .and_then(|construction| construction.decode_all(encoded))
            .err()
            .unwrap();
        assert!(!error.is_admission_unavailable());
        assert_eq!(error.validation_error(), Some(&expected));
        drop(error);
        let after = source.observation().unwrap();
        assert_eq!(after.owned_bytes, before.owned_bytes);
        assert_eq!(after.reserved_bytes, before.reserved_bytes);
        assert_eq!(after.fixed_owner_bytes, before.fixed_owner_bytes);
    }
}

#[test]
fn private_source_shared_binding_large_initializer_is_preadmitted_and_refunds() {
    use std::io::Read;
    // The job-owned emitter uses the real compiler and complete ordinary
    // contract validation. Input files/test stack are ordinary harness work;
    // this test qualifies the closed binding decoder's newly owned work only.
    let path = std::env::var_os("RIFFDB_PRIVATE_WIDE_BINDING_BUNDLE_PATH").unwrap();
    let file = std::fs::File::open(path).unwrap();
    let length = file.metadata().unwrap().len();
    assert!(length > 0 && length <= 1024 * 1024);
    let mut encoded = Vec::with_capacity(usize::try_from(length).unwrap());
    let mut limited = (&file).take(length + 1);
    limited.read_to_end(&mut encoded).unwrap();
    assert_eq!(encoded.len() as u64, length);
    assert_eq!(file.metadata().unwrap().len(), length);
    let catalog = ContractBundle::decode(&encoded).unwrap();
    let command = catalog
        .commands()
        .iter()
        .find(|command| {
            command
                .bindings()
                .iter()
                .any(|binding| binding.mode() == crate::BindingMode::InitOrMutate)
        })
        .unwrap();
    let binding = command
        .bindings()
        .iter()
        .find(|binding| binding.mode() == crate::BindingMode::InitOrMutate)
        .unwrap();
    let count = binding.initializer().len();
    assert_eq!(count, 600);
    assert!(count > 20);
    assert!(std::mem::size_of_val(binding.initializer()) > 4096);
    let mut writer = Writer::new(MAX_BUNDLE_BYTES);
    binding::encode_binding(&mut writer, binding, true, catalog.ir_version()).unwrap();
    let wire = writer.finish();
    let mut ordinary_reader = Reader::new(&wire);
    let ordinary = binding::decode_binding(
        &mut ordinary_reader,
        command.outcomes(),
        command.expressions(),
        catalog.ir_version(),
    )
    .unwrap();
    ordinary_reader.finish().unwrap();
    assert_eq!(&ordinary, binding);
    let root_controls = riffdb_types::exact_provider_budget_v1::ProviderAllocationOwner::<
        crate::BindingPlan,
    >::settlement_root_capacity_upper_bound()
    .unwrap();
    let source = ProjectionSourceWorkingBudgetV1::new();
    let before = source.observation().unwrap();
    let mut sink = SourceDecodeSinkV1::new(&source, root_controls).unwrap();
    let mut reader = Reader::new(&wire);
    let decoded = binding::decode_binding_with_admission(
        &mut reader,
        command.outcomes(),
        command.expressions(),
        catalog.ir_version(),
        &mut sink,
    )
    .unwrap();
    reader.finish().unwrap();
    assert_eq!(&decoded, binding);
    let (observed_count, workspace, pre_sort, post_sort) = sink
        .binding_sort_observation_for_test()
        .expect("actual shared binding decoder must preadmit stable-sort workspace");
    assert_eq!(observed_count, count);
    assert_eq!(post_sort.unwrap() - pre_sort, workspace);
    assert!(workspace > 4096);
    let decoded = sink.settle(decoded).unwrap();
    assert!(decoded.belongs_to(&source));
    assert_eq!(&*decoded, binding);
    drop(decoded);
    assert_eq!(
        source.observation().unwrap().owned_bytes,
        before.owned_bytes
    );
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
    // Leave enough for every actual parsed field/control, but exactly one
    // byte less than the measured sort workspace. The actual shared decoder
    // must refuse at that preadmission, before entering BindingPlan's sort.
    let source = ProjectionSourceWorkingBudgetV1::new();
    let before = source.observation().unwrap();
    let available = pre_sort.checked_add(workspace).unwrap() - 1;
    let pressure = source
        .reserve(PROJECTION_SOURCE_WORKING_MEMORY_BYTES - before.fixed_owner_bytes - available)
        .unwrap();
    let mut sink = SourceDecodeSinkV1::new(&source, root_controls).unwrap();
    let mut reader = Reader::new(&wire);
    let error = binding::decode_binding_with_admission(
        &mut reader,
        command.outcomes(),
        command.expressions(),
        catalog.ir_version(),
        &mut sink,
    )
    .err()
    .unwrap();
    assert_eq!(error, ProviderBundleEncodingErrorV1::Unavailable);
    let (observed_count, requested, before_failed_sort, admitted) = sink
        .binding_sort_observation_for_test()
        .expect("actual shared binding decoder must preadmit stable-sort workspace");
    assert_eq!(observed_count, count);
    assert_eq!(requested, workspace);
    assert_eq!(before_failed_sort, pressure.bytes() + pre_sort);
    assert!(admitted.is_none());
    drop(sink);
    assert_eq!(
        source.observation().unwrap().owned_bytes,
        before.owned_bytes
    );
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes + pressure.bytes()
    );
    drop(pressure);
    assert_eq!(
        source.observation().unwrap().reserved_bytes,
        before.reserved_bytes
    );
    eprintln!(
        "private_source_binding_sort count={count} workspace={workspace} pre_sort={pre_sort} pressure_refused_before_constructor=true"
    );
}
