// req: REP-002, REP-003, REP-005, REP-007, REC-001, REC-005, STO-012, STO-020, PRJ-004
use super::*;
use crate::native_transition_tests::{cut, fresh, inherited};
use prost::Message;
use riffdb_types::InspectOwnedCapacityV1;
use wire::stored_source_inheritance_administration_v1 as inherited_wire;

fn own<T: InspectOwnedCapacityV1>(
    budget: &ProjectionSourceWorkingBudgetV1,
    build: impl FnOnce() -> T,
) -> ProjectionSourceWorkAllocationV1<T> {
    let work = budget
        .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1 + 256 * 1024)
        .unwrap();
    work.settle_owned_value(build()).unwrap()
}
fn scalar<const MAX: usize>(
    value: CanonicalStoredEnvelopeV1,
) -> NativeTransitionEnvelopeBytesV1<MAX> {
    NativeTransitionEnvelopeBytesV1::new(value.into_bytes().into_boxed_slice()).unwrap()
}
fn inheritance_parts(
    cause: SourceInheritanceCauseV1,
    count: usize,
) -> SourceInheritanceAdministrationPartsV1 {
    let mut parts = inherited(cause, count);
    let lineage = parts.predecessor_history.lineage();
    let registry = super::super::metadata::StoredRecordRegistry::completed(
        SchemaHash::from_bytes([6; 32]),
        lineage.database_id(),
        lineage.history_incarnation(),
    )
    .unwrap();
    parts.predecessor_scalars.source_allocator = Some(scalar(
        crate::encode_projection_checkpoint_ordinal_allocator_v1(
            &crate::ProjectionCheckpointOrdinalAllocatorV1::initial(lineage.history_incarnation())
                .unwrap(),
        )
        .unwrap(),
    ));
    parts.predecessor_scalars.incarnation = Some(scalar(
        crate::encode_history_incarnation_v1(lineage.history_incarnation()).unwrap(),
    ));
    parts.predecessor_scalars.registry =
        Some(scalar(crate::encode_record_registry_v3(registry).unwrap()));
    parts
}
fn legacy<M: WritableRecordMessage + Message>(message: &M) -> Vec<u8> {
    let payload = message.encode_to_vec();
    riffdb_proto::envelope::encode_v1(M::record_schema(), &payload).unwrap()
}
fn catalog_wire(encoded: &[u8]) -> wire::StoredAuthoritativeCatalogTransitionV1 {
    let mut scratch = Vec::with_capacity(encoded.len());
    caller_decode(encoded, &mut scratch).unwrap()
}
fn inheritance_wire(encoded: &[u8]) -> wire::StoredSourceInheritanceAdministrationV1 {
    let mut scratch = Vec::with_capacity(encoded.len());
    caller_decode(encoded, &mut scratch).unwrap()
}

#[test]
fn native_transition_codecs_roundtrip_exact_originals_and_both_canonical_framings() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    for is_fresh in [true, false] {
        let original = own(&budget, || {
            NativeCatalogTransitionV1::new(if is_fresh { fresh() } else { cut() }).unwrap()
        });
        let encoded = encode_native_catalog_transition_admitted_v1(&original, &budget).unwrap();
        for bytes in [
            encoded.as_bytes().to_vec(),
            legacy(&catalog_wire(encoded.as_bytes())),
        ] {
            let decoded = decode_native_catalog_transition_admitted_v1(&bytes, &budget).unwrap();
            assert_eq!(&**decoded.value(), &*original);
            assert!(decoded.value().belongs_to(&budget));
            assert_eq!(decoded.encoded_content_charge().get(), bytes.len());
            assert_eq!(
                encode_native_catalog_transition_admitted_v1(decoded.value(), &budget)
                    .unwrap()
                    .as_bytes(),
                encoded.as_bytes()
            );
        }
    }
    for cause in [
        SourceInheritanceCauseV1::Restore,
        SourceInheritanceCauseV1::Promotion,
    ] {
        let original = own(&budget, || {
            SourceInheritanceAdministrationV1::new(inheritance_parts(cause, 128)).unwrap()
        });
        let encoded = encode_source_inheritance_admitted_v1(&original, &budget).unwrap();
        for bytes in [
            encoded.as_bytes().to_vec(),
            legacy(&inheritance_wire(encoded.as_bytes())),
        ] {
            assert!(bytes.len() <= INHERITANCE_CAP);
            let decoded = decode_source_inheritance_admitted_v1(&bytes, &budget).unwrap();
            assert_eq!(&**decoded.value(), &*original);
            let parts = decoded.value().parts();
            assert_eq!(parts.witnesses.len(), 128);
            assert_eq!(
                parts.witnesses[0].canonical_witness_bytes(),
                original.parts().witnesses[0].canonical_witness_bytes()
            );
            assert_eq!(
                parts
                    .predecessor_scalars
                    .registry
                    .as_ref()
                    .unwrap()
                    .as_bytes(),
                original
                    .parts()
                    .predecessor_scalars
                    .registry
                    .as_ref()
                    .unwrap()
                    .as_bytes()
            );
            assert_eq!(
                encode_source_inheritance_admitted_v1(decoded.value(), &budget)
                    .unwrap()
                    .as_bytes(),
                encoded.as_bytes()
            );
        }
    }
}

#[test]
fn native_catalog_codec_refuses_checksum_correct_context_original_role_and_binding_substitutions() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || NativeCatalogTransitionV1::new(cut()).unwrap());
    let encoded = encode_native_catalog_transition_admitted_v1(&original, &budget).unwrap();
    for case in 0..8 {
        let mut value = catalog_wire(encoded.as_bytes());
        match case {
            0 => value.target_catalog_digest = crate::AuthoritativeStateCatalogV2.digest().to_vec(),
            1 => {
                value
                    .publication_binding
                    .as_mut()
                    .unwrap()
                    .prior_history_hash[0] ^= 1
            }
            2 => {
                value.initial_source_allocator.as_mut().unwrap().position = Some(
                    wire::stored_projection_checkpoint_ordinal_allocator_v1::Position::NextOrdinal(
                        2,
                    ),
                )
            }
            3 => value.origin = None,
            4..=7 => {
                let wire::stored_authoritative_catalog_transition_v1::Origin::V2ToV3(origin) =
                    value.origin.as_mut().unwrap()
                else {
                    unreachable!()
                };
                match case {
                    4 => {
                        origin.original_active_admission_envelope =
                            crate::encode_history_incarnation_v1(2)
                                .unwrap()
                                .into_bytes()
                    }
                    5 => {
                        origin.original_registry_envelope = crate::encode_history_incarnation_v1(2)
                            .unwrap()
                            .into_bytes()
                    }
                    6 => {
                        origin
                            .predecessor_history
                            .as_mut()
                            .unwrap()
                            .history_incarnation += 1
                    }
                    7 => origin
                        .original_active_admission_envelope
                        .extend_from_slice(&[0x98, 0x06, 1]),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
        assert!(
            decode_native_catalog_transition_admitted_v1(&legacy(&value), &budget).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn native_inheritance_codec_refuses129_duplicate_foreign_and_scalar_substitutions() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || {
        SourceInheritanceAdministrationV1::new(inheritance_parts(
            SourceInheritanceCauseV1::Promotion,
            128,
        ))
        .unwrap()
    });
    let encoded = encode_source_inheritance_admitted_v1(&original, &budget).unwrap();
    for case in 0..9 {
        let mut value = inheritance_wire(encoded.as_bytes());
        match case {
            0 => value
                .source_witnesses
                .push(value.source_witnesses[0].clone()),
            1 => value.source_witnesses[1] = value.source_witnesses[0].clone(),
            2 => value.source_witnesses.swap(0, 1),
            3 => value.source_witnesses[0][15] ^= 1,
            4 => value.cause = 99,
            5 => {
                value
                    .anchor_binding
                    .as_mut()
                    .unwrap()
                    .covered_frontier
                    .as_mut()
                    .unwrap()
                    .administration_sequence = Some(7)
            }
            6 => {
                value.source_allocator.as_mut().unwrap().state =
                    Some(inherited_wire::predecessor_scalar::State::PresentEnvelope(
                        crate::encode_history_incarnation_v1(2)
                            .unwrap()
                            .into_bytes(),
                    ))
            }
            7 => {
                value.incarnation.as_mut().unwrap().state =
                    Some(inherited_wire::predecessor_scalar::State::PresentEnvelope(
                        crate::encode_history_incarnation_v1(3)
                            .unwrap()
                            .into_bytes(),
                    ))
            }
            8 => value.registry = None,
            _ => unreachable!(),
        }
        // The129 case is rejected by structural preflight during legacy framing;
        // other cases carry valid framing/checksum and fail semantic reconstruction.
        let payload = value.encode_to_vec();
        let raw = wire::StoredEnvelope {
            storage_format_version: 1, record_type: INHERITANCE.to_owned(),
            schema_hash: <wire::StoredSourceInheritanceAdministrationV1 as ReadableRecordMessage>::record_schema().schema_hash().as_bytes().to_vec(),
            payload_crc32c: riffdb_proto::envelope::payload_crc32c(&payload), payload,
        }.encode_to_vec();
        assert!(
            decode_source_inheritance_admitted_v1(&raw, &budget).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn native_transition_codecs_refuse_quota_pressure_and_foreign_ledger_without_leaks() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || {
        SourceInheritanceAdministrationV1::new(inheritance_parts(
            SourceInheritanceCauseV1::Restore,
            1,
        ))
        .unwrap()
    });
    let encoded = encode_source_inheritance_admitted_v1(&original, &budget).unwrap();
    let before = budget.observation().unwrap();
    let pressure = budget
        .reserve(
            riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes,
        )
        .unwrap();
    let saturated = budget.observation().unwrap();
    assert!(matches!(
        encode_source_inheritance_admitted_v1(&original, &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert!(matches!(
        decode_source_inheritance_admitted_v1(encoded.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        saturated.owned_bytes
    );
    assert_eq!(
        budget.observation().unwrap().reserved_bytes,
        saturated.reserved_bytes
    );
    drop(pressure);
    let other_engine = ProjectionSourceWorkingBudgetV1::new();
    assert!(matches!(
        encode_source_inheritance_admitted_v1(&original, &other_engine),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(
        budget.observation().unwrap().owned_bytes,
        before.owned_bytes
    );
    let decoded = decode_source_inheritance_admitted_v1(encoded.as_bytes(), &budget).unwrap();
    let retained = decoded.value().clone();
    drop(decoded);
    assert!(retained.belongs_to(&budget));
    let while_retained = budget.observation().unwrap().owned_bytes;
    drop(retained);
    assert!(budget.observation().unwrap().owned_bytes < while_retained);
}

#[test]
fn native_catalog_origin_digest_binds_complete_original_framing_and_refuses_substitution() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || NativeCatalogTransitionV1::new(fresh()).unwrap());
    let compact = encode_native_catalog_transition_admitted_v1(&original, &budget).unwrap();
    let old = legacy(&catalog_wire(compact.as_bytes()));
    let a = native_catalog_origin_envelope_digest_admitted_v1(compact.as_bytes(), &budget).unwrap();
    let b = native_catalog_origin_envelope_digest_admitted_v1(&old, &budget).unwrap();
    assert_ne!(a, b);
    assert_eq!(
        a.as_bytes(),
        &<[u8; 32]>::from(Sha256::digest(compact.as_bytes()))
    );
    verify_native_catalog_origin_envelope_digest_admitted_v1(compact.as_bytes(), a, &budget)
        .unwrap();
    assert!(
        verify_native_catalog_origin_envelope_digest_admitted_v1(compact.as_bytes(), b, &budget)
            .is_err()
    );
}

#[test]
fn native_transition_canonical_wire_vectors_preserve_exact_original_envelopes() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let fresh = own(&budget, || NativeCatalogTransitionV1::new(fresh()).unwrap());
    let cut = own(&budget, || NativeCatalogTransitionV1::new(cut()).unwrap());
    let restore = own(&budget, || {
        SourceInheritanceAdministrationV1::new(inheritance_parts(
            SourceInheritanceCauseV1::Restore,
            1,
        ))
        .unwrap()
    });
    let promotion = own(&budget, || {
        SourceInheritanceAdministrationV1::new(inheritance_parts(
            SourceInheritanceCauseV1::Promotion,
            1,
        ))
        .unwrap()
    });
    let cases = [
        (
            "fresh-v3",
            encode_native_catalog_transition_admitted_v1(&fresh, &budget).unwrap(),
        ),
        (
            "v2-to-v3",
            encode_native_catalog_transition_admitted_v1(&cut, &budget).unwrap(),
        ),
        (
            "restore-inheritance",
            encode_source_inheritance_admitted_v1(&restore, &budget).unwrap(),
        ),
        (
            "promotion-inheritance",
            encode_source_inheritance_admitted_v1(&promotion, &budget).unwrap(),
        ),
    ];
    fn hex(bytes: &[u8]) -> String {
        let mut text = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            use std::fmt::Write;
            write!(&mut text, "{byte:02x}").unwrap();
        }
        text
    }
    let mut vector = "riffdb-native-transition-wire-vectors-v1\nrecords 4\n".to_owned();
    for (name, compact) in cases {
        let (role, payload, old) = if name == "fresh-v3" || name == "v2-to-v3" {
            let value = catalog_wire(compact.as_bytes());
            (CATALOG, value.encode_to_vec(), legacy(&value))
        } else {
            let value = inheritance_wire(compact.as_bytes());
            (INHERITANCE, value.encode_to_vec(), legacy(&value))
        };
        vector.push_str(&format!(
            "{name}\t{role}\t{}\t{}\t{}\n",
            hex(&payload),
            hex(compact.as_bytes()),
            hex(&old)
        ));
    }
    // Only designated-host execution creates a synthetic concrete candidate.
    // Runtime native initialization/inheritance proof is separately required.
    if let Some(path) = std::env::var_os("RIFFDB_NATIVE_TRANSITION_WIRE_VECTOR_OUTPUT") {
        std::fs::write(path, vector).unwrap();
        return;
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/proto/durable-native-transition-v1-wire-vectors.txt");
    assert_eq!(vector, std::fs::read_to_string(path).unwrap());
}

#[test]
fn native_transition_encode_allowance_covers_independent_owned_wire_inspection() {
    fn prove<M: riffdb_types::InspectOwnedCapacityV1 + WritableRecordMessage>(
        allowance: u64,
        message: M,
        role: &'static str,
        mut scope: riffdb_types::OwnedCapacityInspectionScopeV1<'_>,
    ) {
        let encoded = encode_message(role, &message).unwrap();
        let message_bytes = {
            let report =
                riffdb_types::OwnedCapacityInspectionV1::preview(&message, scope.reborrow())
                    .unwrap();
            assert!(report.shared_allocations().is_empty());
            report.unshared_bytes()
        };
        let output_bytes = {
            let report =
                riffdb_types::OwnedCapacityInspectionV1::preview(&encoded, scope.reborrow())
                    .unwrap();
            assert!(report.shared_allocations().is_empty());
            report.unshared_bytes()
        };
        assert!(
            allowance
                >= OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
                    + message_bytes
                    + output_bytes
                    + super::encode_bound::settlement_inline_bound().unwrap()
        );
    }
    let budget = ProjectionSourceWorkingBudgetV1::new();
    for is_fresh in [true, false] {
        let value = own(&budget, || {
            NativeCatalogTransitionV1::new(if is_fresh { fresh() } else { cut() }).unwrap()
        });
        let bound = super::encode_bound::catalog_encode_work_bound(&value).unwrap();
        let mut work = budget.reserve(bound).unwrap();
        prove(
            bound,
            super::catalog::catalog_to_wire(&value).unwrap(),
            CATALOG,
            work.inspection_scope().unwrap(),
        );
        drop(work);
    }
    let mut previous = 0;
    for count in [0, 1, 128] {
        let value = own(&budget, || {
            SourceInheritanceAdministrationV1::new(inheritance_parts(
                SourceInheritanceCauseV1::Restore,
                count,
            ))
            .unwrap()
        });
        let bound = super::encode_bound::inheritance_encode_work_bound(&value).unwrap();
        assert!(bound > previous);
        let mut work = budget.reserve(bound).unwrap();
        prove(
            bound,
            super::inheritance::inheritance_to_wire(&value).unwrap(),
            INHERITANCE,
            work.inspection_scope().unwrap(),
        );
        drop(work);
        previous = bound;
    }
}

#[test]
fn native_catalog_semantic_history_box_is_admitted_before_reconstruction_and_inspected() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || NativeCatalogTransitionV1::new(cut()).unwrap());
    let NativeCatalogTransitionOriginV1::V2ToV3 {
        predecessor_history,
        ..
    } = &original.parts().origin
    else {
        panic!("actual cut fixture")
    };
    {
        // own() settled its construction work. Admit this independent census
        // on the same actual Source ledger and retire it before pressure checks.
        let mut census = budget
            .reserve(OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let history_report = riffdb_types::OwnedCapacityInspectionV1::preview(
            predecessor_history,
            census.inspection_scope().unwrap(),
        )
        .unwrap();
        assert!(history_report.shared_allocations().is_empty());
        assert_eq!(
            history_report.unshared_bytes(),
            super::encode_bound::catalog_semantic_history_box_work_bound().unwrap(),
        );
        drop(history_report);
        let fixed_report = riffdb_types::OwnedCapacityInspectionV1::preview(
            &**predecessor_history,
            census.inspection_scope().unwrap(),
        )
        .unwrap();
        assert!(fixed_report.shared_allocations().is_empty());
        assert_eq!(fixed_report.unshared_bytes(), 0);
    }
    let encoded = encode_native_catalog_transition_admitted_v1(&original, &budget).unwrap();
    let before = budget.observation().unwrap();
    let constructed = std::cell::Cell::new(false);
    let refused = super::super::projection_source::decode_admitted_with_additional_work::<
        wire::StoredAuthoritativeCatalogTransitionV1,
        NativeCatalogTransitionV1,
    >(
        encoded.as_bytes(),
        &budget,
        riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES,
        |_| {
            constructed.set(true);
            Err(DurableCodecError::corrupt())
        },
    );
    assert!(matches!(
        refused,
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert!(!constructed.get());
    assert_eq!(budget.observation().unwrap(), before);
    let decoded =
        decode_native_catalog_transition_admitted_v1(encoded.as_bytes(), &budget).unwrap();
    assert_eq!(&**decoded.value(), &*original);
    let retained = budget.observation().unwrap();
    assert!(retained.owned_bytes > before.owned_bytes);
    drop(decoded);
    let mut after = budget.observation().unwrap();
    // Freed owners advance this lifetime counter; every live charge and live
    // allocation must return exactly to the prior observation.
    assert!(after.released_allocations > before.released_allocations);
    after.released_allocations = before.released_allocations;
    assert_eq!(after, before);
}

#[test]
fn native_operator_audit_views_clone_only_the_admitted_owner_and_keep_original_charge() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let origin = own(&budget, || NativeCatalogTransitionV1::new(fresh()).unwrap());
    let encoded = encode_native_catalog_transition_admitted_v1(&origin, &budget).unwrap();
    let compact_origin = encoded.as_bytes().to_vec();
    let legacy_origin = legacy(&catalog_wire(encoded.as_bytes()));
    drop(encoded);
    drop(origin);
    let inherited = own(&budget, || {
        SourceInheritanceAdministrationV1::new(inheritance_parts(
            SourceInheritanceCauseV1::Restore,
            128,
        ))
        .unwrap()
    });
    let encoded = encode_source_inheritance_admitted_v1(&inherited, &budget).unwrap();
    let compact_inherited = encoded.as_bytes().to_vec();
    let legacy_inherited = legacy(&inheritance_wire(encoded.as_bytes()));
    drop(encoded);
    drop(inherited);
    let empty = budget.observation().unwrap();
    for encoded in [
        compact_origin,
        legacy_origin,
        compact_inherited,
        legacy_inherited,
    ] {
        let item = decode_native_control_audit_admitted_v1(&encoded, &budget)
            .unwrap()
            .unwrap();
        assert_eq!(item.encoded_content_charge().get(), encoded.len());
        let retained = budget.observation().unwrap();
        assert!(retained.owned_bytes > empty.owned_bytes);
        let alias = item.clone();
        assert_eq!(budget.observation().unwrap(), retained);
        assert_eq!(item, alias);
        assert!(format!("{:?}", item.value()).contains("[redacted]"));
        match alias.value() {
            crate::StoredAdministrationAuditRecordV1::NativeCatalogTransition(value) => {
                assert_eq!(
                    value.encoded_content_charge(),
                    alias.encoded_content_charge()
                );
                assert_eq!(
                    value.administration_sequence(),
                    value.value().parts().administration_sequence
                );
            }
            crate::StoredAdministrationAuditRecordV1::SourceInheritance(value) => {
                assert_eq!(
                    value.encoded_content_charge(),
                    alias.encoded_content_charge()
                );
                assert_eq!(value.value().parts().witnesses.len(), 128);
            }
            _ => panic!("closed two-role view"),
        }
        drop(item);
        assert_eq!(budget.observation().unwrap(), retained);
        drop(alias);
        let mut released = budget.observation().unwrap();
        assert!(released.released_allocations > retained.released_allocations);
        released.released_allocations = empty.released_allocations;
        assert_eq!(released, empty);
    }
}

#[test]
fn native_operator_audit_decoder_refuses_known_corruption_and_source_pressure_without_fallback() {
    let budget = ProjectionSourceWorkingBudgetV1::new();
    let original = own(&budget, || NativeCatalogTransitionV1::new(fresh()).unwrap());
    let encoded = encode_native_catalog_transition_admitted_v1(&original, &budget).unwrap();
    let mut damaged = encoded.as_bytes().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    assert!(matches!(
        decode_native_control_audit_admitted_v1(&damaged, &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::Codec(_))
    ));
    let normal = crate::encode_history_incarnation_v1(1).unwrap();
    assert!(
        decode_native_control_audit_admitted_v1(normal.as_bytes(), &budget)
            .unwrap()
            .is_none()
    );
    let observed = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - observed.fixed_owner_bytes
        - observed.owned_bytes
        - observed.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(matches!(
        decode_native_control_audit_admitted_v1(encoded.as_bytes(), &budget),
        Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
    ));
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), observed);
}
