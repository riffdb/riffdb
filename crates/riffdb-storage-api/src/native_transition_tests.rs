// req: REP-002, REP-003, REP-005, REP-007, REC-001, REC-005, STO-012
use super::*;
use riffdb_types::{
    AdministrationSequence as Admin, CommitSequence, ContentDigest, DIGEST_SCHEME_V1, DatabaseId,
    DualFrontier, HashDomain, LeadershipEpochV1, ProjectionCheckpointOrdinal,
    ProjectionSourceLocatorV1, SchemaHash,
};

fn db() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1234, [1; 10]).unwrap()
}
fn lineage(catalog: [u8; 32], incarnation: u64, epoch: u64) -> ChangelogLineageV3 {
    ChangelogLineageV3::new_with_catalog(
        db(),
        incarnation,
        LeadershipEpochV1::new(epoch).unwrap(),
        catalog,
    )
    .unwrap()
}
fn history(catalog: [u8; 32]) -> ChangelogHistoryStateV3 {
    let anchor = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(1).unwrap(),
        [1; 32],
        DualFrontier::new(None, None),
    );
    let tail = ChangelogHistoryPointV3::new(
        ChangelogTransactionSequence::new(9).unwrap(),
        [9; 32],
        DualFrontier::new(CommitSequence::new(11), Admin::new(4)),
    );
    ChangelogHistoryStateV3::new(lineage(catalog, 2, 7), anchor, tail, anchor).unwrap()
}
fn raw<const MAX: usize>(bytes: Vec<u8>) -> NativeTransitionEnvelopeBytesV1<MAX> {
    NativeTransitionEnvelopeBytesV1::new(bytes.into_boxed_slice()).unwrap()
}
pub(super) fn cut() -> NativeCatalogTransitionPartsV1 {
    let before = history(AuthoritativeStateCatalogV2.digest());
    let active = ReplicationPrimaryAdmissionV1::active(before.lineage()).unwrap();
    NativeCatalogTransitionPartsV1 {
        database_id: db(),
        administration_sequence: Admin::new(5).unwrap(),
        publication_binding: AuthoritativeTransactionBindingV3 {
            database_id: db(),
            history_incarnation: 2,
            predecessor: Some(before.tail().sequence()),
            sequence: before.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: before.tail().frontier(),
            covered_frontier: DualFrontier::new(CommitSequence::new(11), Admin::new(5)),
            prior_history_hash: before.tail().history_hash(),
        },
        target_catalog_digest: AuthoritativeStateCatalogV3.digest(),
        target_registry_digest: SchemaHash::from_bytes([7; 32]),
        initial_source_allocator: ProjectionCheckpointOrdinalAllocatorV1::initial(2).unwrap(),
        origin: NativeCatalogTransitionOriginV1::V2ToV3 {
            predecessor_history: Box::new(before),
            original_registry: raw(
                proto_codec::encode_record_registry_v2(SchemaHash::from_bytes([6; 32]))
                    .unwrap()
                    .into_bytes(),
            ),
            original_active_admission: raw(proto_codec::encode_replication_primary_admission_v1(
                &active,
            )
            .unwrap()
            .into_bytes()),
            legacy_controls: NativeLegacyControlsCommitmentV1::from_bytes([8; 32]),
        },
    }
}
pub(super) fn fresh() -> NativeCatalogTransitionPartsV1 {
    let mut value = cut();
    value.administration_sequence = Admin::first();
    value.publication_binding.predecessor = None;
    value.publication_binding.sequence = ChangelogTransactionSequence::new(1).unwrap();
    value.publication_binding.predecessor_frontier = DualFrontier::new(None, None);
    value.publication_binding.covered_frontier = DualFrontier::new(None, Some(Admin::first()));
    value.publication_binding.prior_history_hash = [0; 32];
    value.origin = NativeCatalogTransitionOriginV1::FreshV3;
    value
}
fn witness(ordinal: u64) -> SourceInheritanceWitnessV1 {
    SourceInheritanceWitnessV1::new(
        ProjectionSourceLocatorV1::new(db(), 2, ProjectionCheckpointOrdinal::new(ordinal).unwrap())
            .unwrap(),
        ContentDigest::from_stored_parts(
            DIGEST_SCHEME_V1,
            HashDomain::ProjectionSourceDescriptor,
            [3; 32],
        )
        .unwrap(),
        ContentDigest::from_stored_parts(
            DIGEST_SCHEME_V1,
            HashDomain::ProjectionSourceOrigin,
            [4; 32],
        )
        .unwrap(),
    )
    .unwrap()
}
pub(super) fn inherited(
    cause: SourceInheritanceCauseV1,
    count: usize,
) -> SourceInheritanceAdministrationPartsV1 {
    let before = history(AuthoritativeStateCatalogV3.digest());
    let promotion = cause == SourceInheritanceCauseV1::Promotion;
    SourceInheritanceAdministrationPartsV1 {
        administration_sequence: Admin::new(5).unwrap(),
        cause,
        predecessor_history: before,
        successor_lineage: lineage(
            AuthoritativeStateCatalogV3.digest(),
            3,
            if promotion { 8 } else { 7 },
        ),
        anchor_binding: AuthoritativeTransactionBindingV3 {
            database_id: db(),
            history_incarnation: 3,
            predecessor: None,
            sequence: ChangelogTransactionSequence::new(1).unwrap(),
            predecessor_frontier: before.tail().frontier(),
            covered_frontier: DualFrontier::new(
                CommitSequence::new(11),
                Admin::new(if promotion { 8 } else { 5 }),
            ),
            prior_history_hash: [0; 32],
        },
        original_origin: NativeCatalogOriginReferenceV1::new(
            Admin::first(),
            NativeCatalogOriginEnvelopeDigestV1::from_bytes([5; 32]),
        ),
        authorized_selection: SourceInheritanceSelectionCommitmentV1::from_bytes([6; 32]),
        witnesses: (1..=count)
            .map(|n| witness(u64::try_from(n).unwrap()))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        predecessor_scalars: SourceInheritancePredecessorScalarsV1 {
            source_allocator: None,
            incarnation: None,
            registry: None,
            watermark: None,
        },
    }
}

#[test]
fn native_catalog_fact_checks_exact_fresh_and_cut_bindings_without_inferring_authority() {
    let empty = NativeCatalogTransitionV1::new(fresh()).unwrap();
    assert!(matches!(
        empty.parts().origin,
        NativeCatalogTransitionOriginV1::FreshV3
    ));
    let value = NativeCatalogTransitionV1::new(cut()).unwrap();
    value
        .validate_original_active(
            &ReplicationPrimaryAdmissionV1::active(
                history(AuthoritativeStateCatalogV2.digest()).lineage(),
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        value
            .validate_original_active(
                &ReplicationPrimaryAdmissionV1::active(lineage(
                    AuthoritativeStateCatalogV2.digest(),
                    3,
                    7
                ))
                .unwrap()
            )
            .is_err()
    );
    assert!(
        empty
            .validate_original_active(
                &ReplicationPrimaryAdmissionV1::active(
                    history(AuthoritativeStateCatalogV2.digest()).lineage()
                )
                .unwrap()
            )
            .is_err()
    );
    // Historical registry identity remains exact evidence after future additions.
    assert_eq!(
        value.parts().target_registry_digest,
        SchemaHash::from_bytes([7; 32])
    );
}

#[test]
fn native_catalog_fact_refuses_foreign_chain_frontier_allocator_and_implicit_fresh_shapes() {
    for case in 0..8 {
        let mut parts = cut();
        match case {
            0 => parts.target_catalog_digest = AuthoritativeStateCatalogV2.digest(),
            1 => {
                parts.initial_source_allocator =
                    ProjectionCheckpointOrdinalAllocatorV1::initial(3).unwrap()
            }
            2 => parts.publication_binding.predecessor = None,
            3 => parts.publication_binding.prior_history_hash[0] ^= 1,
            4 => parts.publication_binding.history_incarnation = 3,
            5 => {
                parts.publication_binding.covered_frontier =
                    DualFrontier::new(CommitSequence::new(12), Admin::new(5))
            }
            6 => parts.administration_sequence = Admin::new(6).unwrap(),
            7 => parts.origin = NativeCatalogTransitionOriginV1::FreshV3,
            _ => unreachable!(),
        }
        assert!(
            NativeCatalogTransitionV1::new(parts).is_err(),
            "case {case}"
        );
    }
}

#[test]
fn native_inheritance_witnesses_preserve_full_locator_order_and_bound128() {
    let value =
        SourceInheritanceAdministrationV1::new(inherited(SourceInheritanceCauseV1::Restore, 128))
            .unwrap();
    assert_eq!(value.parts().witnesses.len(), 128);
    let first = value.parts().witnesses[0];
    let key = first.canonical_locator_key();
    assert_eq!(&key[..16], db().as_bytes());
    assert_eq!(&key[16..24], &2_u64.to_be_bytes());
    assert_eq!(&key[24..], &1_u64.to_be_bytes());
    assert_eq!(&first.canonical_witness_bytes()[..32], &key);
    assert!(
        SourceInheritanceAdministrationV1::new(inherited(SourceInheritanceCauseV1::Restore, 129))
            .is_err()
    );
    let mut duplicate = inherited(SourceInheritanceCauseV1::Restore, 2);
    duplicate.witnesses[1] = duplicate.witnesses[0];
    assert!(SourceInheritanceAdministrationV1::new(duplicate).is_err());
    let mut reversed = inherited(SourceInheritanceCauseV1::Restore, 2);
    reversed.witnesses.swap(0, 1);
    assert!(SourceInheritanceAdministrationV1::new(reversed).is_err());
}

#[test]
fn native_inheritance_anchor_binds_exact_predecessor_and_closed_admin_recipe() {
    for cause in [
        SourceInheritanceCauseV1::Restore,
        SourceInheritanceCauseV1::Promotion,
    ] {
        let value = SourceInheritanceAdministrationV1::new(inherited(cause, 1)).unwrap();
        assert_eq!(
            value.parts().anchor_binding.predecessor_frontier,
            value.parts().predecessor_history.tail().frontier()
        );
        assert_eq!(value.parts().witnesses[0].locator(), witness(1).locator());
        let mut changed = inherited(cause, 1);
        changed.anchor_binding.covered_frontier =
            DualFrontier::new(CommitSequence::new(11), Admin::new(7));
        assert!(SourceInheritanceAdministrationV1::new(changed).is_err());
        let mut changed = inherited(cause, 1);
        changed.anchor_binding.predecessor_frontier =
            DualFrontier::new(CommitSequence::new(12), Admin::new(4));
        assert!(SourceInheritanceAdministrationV1::new(changed).is_err());
        let mut changed = inherited(cause, 1);
        changed.successor_lineage = lineage(AuthoritativeStateCatalogV3.digest(), 2, 7);
        assert!(SourceInheritanceAdministrationV1::new(changed).is_err());
    }
}
