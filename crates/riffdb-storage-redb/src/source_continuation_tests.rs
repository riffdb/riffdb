//! Closed source-footprint qualification and read-only diagnosis.
use super::*;

fn command_segment() -> (Vec<u8>, riffdb_storage_api::StoredCommandSegmentV1) {
    let wire = include_str!("../../../fixtures/proto/durable-command-prefix-v7-wire-vectors.txt");
    let hex = wire
        .lines()
        .find_map(|line| line.strip_prefix("riffdb.storage.v1.StoredCommandSegmentV6\t"))
        .unwrap();
    let bytes = (0..hex.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&hex[offset..offset + 2], 16).unwrap())
        .collect::<Vec<_>>();
    let segment = crate::history_stub::decode_segment_value(&bytes)
        .unwrap()
        .into_parts()
        .0;
    (bytes, segment)
}

// req: STO-030, STO-032, END-008
#[test]
fn absent_canonical_service_backfill_preserves_every_existing_source_audit() {
    let (_, segment) = command_segment();
    let audit = segment.commands()[0].base().started_audit();
    let bytes = crate::codec::encode_administration_audit_record_v1(
        &riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(audit.clone()),
    )
    .unwrap();
    let mutation = CompositeMutationV1::put(
        T::Audit,
        crate::keys::encode_audit_key(audit.administration_sequence()).to_vec(),
        bytes.as_bytes().to_vec(),
    )
    .unwrap();
    let prior = Some(audit.administration_sequence());
    assert!(
        !SourceContinuationFootprint::composite(prior, std::slice::from_ref(&mutation))
            .is_append_only()
    );
    assert!(
        SourceContinuationFootprint::composite_checked(
            prior,
            std::slice::from_ref(&mutation),
            |_| Ok(None),
            |_| panic!("absent service audit needs no command lookup")
        )
        .unwrap()
        .is_append_only()
    );
    // A conflicting physical row cannot be mistaken for absence, even when
    // its logical service value happens to match the proposed insertion.
    assert!(
        !SourceContinuationFootprint::composite_checked(
            prior,
            std::slice::from_ref(&mutation),
            |_| Ok(Some(bytes.as_bytes().to_vec())),
            |_| panic!("reject mismatched preimage first")
        )
        .unwrap()
        .is_append_only()
    );
    let invalid = CompositeMutationV1::put(
        T::Audit,
        mutation.key().to_vec(),
        b"not-an-envelope".to_vec(),
    )
    .unwrap();
    assert!(
        !SourceContinuationFootprint::composite_checked(
            prior,
            &[invalid],
            |_| Ok(None),
            |_| panic!("reject malformed backfill first")
        )
        .unwrap()
        .is_append_only()
    );
}

// req: STO-030, STO-032, END-008
#[test]
fn locator_materialization_requires_exact_command_audit_and_reuses_one_native_segment() {
    use riffdb_storage_api::{StoredCommandAuditLocatorV1, StoredCommandAuditMemberV1 as Member};
    let (bytes, segment) = command_segment();
    let command = &segment.commands()[0];
    let mut originals = std::collections::BTreeMap::new();
    let mut mutations = Vec::new();
    for member in [Member::Started, Member::Terminal] {
        let audit = command.base().audit(member);
        let locator = riffdb_storage_api::encode_command_audit_locator_v1(
            StoredCommandAuditLocatorV1::new(command.commit_sequence(), member),
        )
        .unwrap();
        let after = crate::codec::encode_administration_audit_record_v1(
            &riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(audit.clone()),
        )
        .unwrap();
        let key = crate::keys::encode_audit_key(audit.administration_sequence()).to_vec();
        originals.insert(key.clone(), locator.as_bytes().to_vec());
        mutations.push(
            CompositeMutationV1::replace(
                T::Audit,
                key,
                locator.as_bytes(),
                after.as_bytes().to_vec(),
            )
            .unwrap(),
        );
    }
    let prior = Some(command.base().terminal_audit().administration_sequence());
    let mut reads = 0;
    assert!(
        SourceContinuationFootprint::composite_checked(
            prior,
            &mutations,
            |mutation| Ok(originals.get(mutation.key()).cloned()),
            |_| {
                reads += 1;
                Ok(Some((
                    crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                        .to_vec(),
                    bytes.clone(),
                )))
            }
        )
        .unwrap()
        .is_append_only()
    );
    assert_eq!(
        reads, 1,
        "reuse one bounded decoded native segment for both members"
    );
    // An otherwise canonical wrong member is not a representation change.
    let wrong = riffdb_storage_api::encode_command_audit_locator_v1(
        StoredCommandAuditLocatorV1::new(command.commit_sequence(), Member::Terminal),
    )
    .unwrap();
    let substitution = CompositeMutationV1::replace(
        T::Audit,
        mutations[0].key().to_vec(),
        wrong.as_bytes(),
        mutations[0].value().unwrap().to_vec(),
    )
    .unwrap();
    assert!(
        !SourceContinuationFootprint::composite_checked(
            prior,
            &[substitution],
            |_| Ok(Some(wrong.as_bytes().to_vec())),
            |_| Ok(Some((
                crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                    .to_vec(),
                bytes.clone()
            )))
        )
        .unwrap()
        .is_append_only()
    );
    assert!(
        !SourceContinuationFootprint::composite_checked(
            prior,
            &mutations,
            |mutation| Ok(originals.get(mutation.key()).cloned()),
            |_| Ok(None)
        )
        .unwrap()
        .is_append_only()
    );
}

// req: STO-030, END-008
#[test]
fn source_continuation_read_only_forensic_census_child() {
    let Some(path) = std::env::var_os("RIFFDB_TEST_SOURCE_FOOTPRINT_CENSUS") else {
        return;
    };
    use redb::{ReadableDatabase, ReadableTable};
    let path = std::path::PathBuf::from(path);
    assert!(path.is_absolute() && path.as_os_str().len() <= 4096);
    assert!(
        std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_file()
    );
    let database = redb::ReadOnlyDatabase::open(&path).unwrap();
    let read = database.begin_read().unwrap();
    let _resolver = crate::history_stub::HistoryResolver::register_beside(&path, &read).unwrap();
    let rows = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let mut rejected = std::collections::BTreeMap::<String, u64>::new();
    let mut receipts = 0_u64;
    let mut invalid_receipts = 0_u64;
    for row in rows.iter().unwrap() {
        receipts += 1;
        assert!(
            receipts <= 10_000_000,
            "forensic receipt scan exceeds its bound"
        );
        let (_, value) = row.unwrap();
        let bytes = crate::history_stub::resolve_history_value(value.value()).unwrap();
        let receipt = riffdb_storage_api::AuthoritativeTransactionV3::decode(&bytes).unwrap();
        let mut invalid = false;
        for mutation in receipt.mutations() {
            if !unchanged_source_or_append(
                mutation.namespace(),
                mutation.key(),
                mutation.expected_hash(),
                mutation.value(),
                receipt.binding().predecessor_frontier.administration(),
            ) {
                invalid = true;
                let namespace = format!("{:?}", mutation.namespace());
                assert!(namespace.len() <= 128 && rejected.len() <= 256);
                *rejected.entry(namespace).or_default() += 1;
            }
        }
        invalid_receipts += u64::from(invalid);
    }
    println!(
        "riffdb-source-footprint-forensic-v1 receipts={receipts} rejected_receipts={invalid_receipts}"
    );
    for (namespace, count) in rejected {
        println!("riffdb-source-footprint-forensic-v1 namespace={namespace} count={count}");
    }
}

// req: STO-030, STO-031, STO-032, END-008
#[test]
fn continuation_footprint_refuses_old_audit_preimages_and_control_changes() {
    let prior = AdministrationSequence::new(7).unwrap();
    for invalid in [8_u64.to_be_bytes().to_vec(), vec![0; 9], vec![255; 9]] {
        assert!(!unchanged_source_or_append(
            N::Audit,
            &invalid,
            None,
            Some(b"row"),
            Some(prior)
        ));
    }
    assert!(unchanged_source_or_append(
        N::Audit,
        &crate::keys::encode_audit_key(AdministrationSequence::new(8).unwrap()),
        None,
        Some(b"row"),
        Some(prior)
    ));
    for sequence in [6_u64, 7] {
        assert!(!unchanged_source_or_append(
            N::Audit,
            &crate::keys::encode_audit_key(AdministrationSequence::new(sequence).unwrap()),
            None,
            Some(b"row"),
            Some(prior)
        ));
    }
    assert!(!unchanged_source_or_append(
        N::Audit,
        &crate::keys::encode_audit_key(AdministrationSequence::new(8).unwrap()),
        Some([1; 32]),
        Some(b"row"),
        Some(prior)
    ));
    assert!(!unchanged_source_or_append(
        N::AuditByRequest,
        b"key",
        Some([1; 32]),
        None,
        Some(prior)
    ));
    for namespace in [
        N::CatalogActive,
        N::RecordRegistry,
        N::HistoryIncarnation,
        N::ReplicationSourceHolds,
        N::ChangelogHistory,
        N::ChangelogHistoryState,
        N::LeadershipEpoch,
        N::AuthoritativeStateCatalog,
        N::CleanCloseLifecycle,
    ] {
        assert!(!unchanged_source_or_append(
            namespace,
            b"key",
            None,
            Some(b"row"),
            Some(prior)
        ));
    }
    let mut unknown = SourceContinuationFootprint::fresh();
    unknown.combine(SourceContinuationFootprint::unknown());
    unknown.combine(SourceContinuationFootprint::fresh());
    assert!(!unknown.is_append_only());
}
