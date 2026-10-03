//! Production registration churn and protected-saturation proofs.
// req: OQ-040, PRJ-001, PRJ-004
use super::*;
use riffdb_query_module::{NamedQuerySource, QueryModule, QueryModuleCandidate};
use riffdb_service::CursorClockError;
use riffdb_storage_api::{OwnedSnapshotReader, StorageError};
use riffdb_types::{ApplicationRoleHash, PartitionKeyBuilder, QueryModuleName, QueryModuleVersion};
use std::sync::atomic::{AtomicU64, Ordering};

struct Clock(AtomicU64);
impl CursorMonotonicClock for Clock {
    fn now(&self) -> Result<CursorTick, CursorClockError> {
        CursorTick::from_process_elapsed(Duration::from_secs(self.0.load(Ordering::SeqCst)))
            .map_err(|_| CursorClockError)
    }
}
struct UnobservedStorage;
impl OwnedSnapshotReader for UnobservedStorage {
    type Snapshot<'a> = riffdb_storage_redb::RedbOwnedSnapshot;
    fn open_owned_snapshot(&self) -> Result<Self::Snapshot<'_>, StorageError> {
        panic!("registration retirement must not observe authoritative data or scan as a fallback");
    }
}
fn isolated(name: &str, body: impl FnOnce()) {
    if std::env::var("RIFFDB_RETIREMENT_CHILD").as_deref() == Ok(name) {
        body();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("RIFFDB_RETIREMENT_CHILD", name)
        .status()
        .unwrap();
    assert!(status.success());
}
struct Fixture {
    runtime: Arc<ExactTextRuntime>,
    clock: Arc<Clock>,
    query: Arc<riffdb_query_module::CompiledExactTextResultSetV1>,
    module: QueryModule,
    _root: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let clock = Arc::new(Clock(AtomicU64::new(0)));
        let runtime = ExactTextRuntime::open(
            ProjectionReadSource::new(UnobservedStorage),
            root.path(),
            1,
            ProjectionGeneration::first(),
            clock.clone(),
        )
        .unwrap();
        let source = include_str!("../../../fixtures/compiler/text-index/contract.riff").replace(
            "field title: string<256>",
            r#"field title: string<256>
    field state: string<32>
    field rank: optional<u64>
    index by_title (org_id, title, doc_id) text_key(title, binary_utf8_v1)
    index by_state (org_id, state, doc_id)
    index by_rank (org_id, rank, doc_id) presence(rank)
    pattern_index by_pattern(
      title, profile unicode_fold_v1,
      operators (equals, starts_with, ends_with, contains, like, ilike, not_like, not_ilike),
      max_source_bytes 500, max_matched_bytes 9000, max_rows 500,
      max_total_matched_bytes 268435456, max_grams_per_row 500,
      max_distinct_grams 65535, max_postings 1048576, max_postings_bytes 67108864,
      max_pattern_bytes 500, max_pattern_atoms 500, max_candidates 500,
      max_verification_bytes 268435456, max_results 499, staleness_slo 60,
      replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000,
      retained_generations 8
    )"#,
        );
        let bundle = riffdb_contract_compiler::compile_contract_source(&source).unwrap();
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("retirement_fixture").unwrap(),
                QueryModuleVersion::new(1).unwrap(),
                vec![
                    NamedQuerySource::new(
                        "ContainsDocuments",
                        r#"
query ContainsDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<10> = 10, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id && title contains $needle
    order by title asc, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title } total: total { value } }
  outcomes Found
}"#,
                    )
                    .unwrap(),
                    NamedQuerySource::new(
                        "PredicateDocuments",
                        r#"
query PredicateDocuments($org_id: Document.org_id, $needle: Document.title,
    $states: Set<Document.state>, $limit: Limit<10> = 10, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    && title contains $needle && state not_in $states
    order by state asc, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title } total: total { value } }
  outcomes Found
}"#,
                    )
                    .unwrap(),
                    NamedQuerySource::new(
                        "NullableDocuments",
                        r#"
query NullableDocuments($org_id: Document.org_id, $needle: Document.title,
    $states: Set<Document.state>, $limit: Limit<10> = 10, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    && title contains $needle && state not_in $states
    order by rank desc nulls last, doc_id asc take $limit offset $offset
  aggregate total from documents { exact_count() as value }
  return Found { documents: documents { org_id doc_id title rank } total: total { value } }
  outcomes Found
}"#,
                    )
                    .unwrap(),
                    NamedQuerySource::new(
                        "SearchDocuments",
                        r#"
query SearchDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<10> = 10, $offset: u64 = 0) {
  many documents from Document where org_id == $org_id
    matching(search, conjunction, $needle) order by doc_id asc take $limit offset $offset
  return Found { documents: documents { org_id doc_id title } } outcomes Found
}"#,
                    )
                    .unwrap(),
                    NamedQuerySource::new(
                        "PatternDocuments",
                        r#"
query PatternDocuments($org_id: Document.org_id, $needle: Document.title,
    $limit: Limit<10> = 10, $after: Cursor?) {
  candidates matching_documents: Document.doc_id
    from intersect {
      Document.doc_id using by_state where org_id == $org_id && state == "visible",
      Document.doc_id using by_pattern where org_id == $org_id && title contains $needle,
    } within 500 else IntegrityFailure
  many documents from Document where org_id == $org_id && doc_id in matching_documents
    order by title asc, doc_id asc take $limit after $after else IntegrityFailure
  return Found { documents: documents { org_id doc_id title } } outcomes Found | IntegrityFailure
}"#,
                    )
                    .unwrap(),
                ],
            )
            .unwrap(),
            &bundle,
        )
        .unwrap();
        Self {
            runtime,
            clock,
            query: module
                .queries()
                .iter()
                .find(|query| query.name() == "ContainsDocuments")
                .unwrap()
                .shared_exact_text_result()
                .unwrap(),
            module,
            _root: root,
        }
    }
    fn register(&self, family: ProviderFamily, seed: u16) {
        let base = self.request(seed);
        let key = base.partition_key().clone();
        let value = base.partition_value().clone();
        let role = base.policy_shape();
        match family {
            ProviderFamily::Text => {
                drop(self.runtime.slot_for(&base).unwrap());
            }
            ProviderFamily::Predicate => {
                let query = self
                    .module
                    .queries()
                    .iter()
                    .find_map(|query| query.shared_exact_predicate_result())
                    .unwrap();
                let member = query.program().members()[0];
                drop(
                    self.runtime
                        .predicate_slot_for(&ExactPredicateProjectionRequest::new(
                            query,
                            key,
                            value,
                            role,
                            None,
                            BTreeMap::new(),
                            member,
                            0,
                            std::num::NonZeroU16::MIN,
                            None,
                        ))
                        .unwrap(),
                );
            }
            ProviderFamily::NullablePredicate => {
                let query = self
                    .module
                    .queries()
                    .iter()
                    .find_map(|query| query.shared_nullable_exact_predicate_result())
                    .unwrap();
                let member = query.program().members()[0];
                drop(
                    self.runtime
                        .nullable_predicate_slot_for(&NullableExactPredicateProjectionRequest::new(
                            query,
                            key,
                            value,
                            role,
                            None,
                            BTreeMap::new(),
                            member,
                            0,
                            std::num::NonZeroU16::MIN,
                            None,
                        ))
                        .unwrap(),
                );
            }
            ProviderFamily::Tokenized => {
                let query = self
                    .module
                    .queries()
                    .iter()
                    .find_map(|query| query.shared_tokenized_text_result())
                    .unwrap();
                drop(
                    self.runtime
                        .tokenized_slot_for(&TokenizedTextProjectionRequest::new(
                            query,
                            key,
                            value,
                            role,
                            None,
                            "alpha".to_owned(),
                            0,
                            std::num::NonZeroU16::MIN,
                            None,
                            None,
                        ))
                        .unwrap(),
                );
            }
            ProviderFamily::LongPattern => {
                let program = self
                    .module
                    .queries()
                    .iter()
                    .find_map(|query| query.shared_ordinary_program())
                    .unwrap();
                let step = program
                    .steps()
                    .iter()
                    .find(|step| {
                        matches!(step.access(), QueryAccessKind::LongPatternCandidate { .. })
                    })
                    .unwrap()
                    .clone();
                let QueryAccessKind::LongPatternCandidate { pattern, .. } = step.access() else {
                    unreachable!();
                };
                let compiled = riffdb_types::CompiledLongPatternV1::compile_bounded(
                    pattern.operator(),
                    pattern.profile(),
                    "alpha",
                    pattern.bounds(),
                )
                .unwrap();
                drop(
                    self.runtime
                        .long_pattern_slot_for(&LongPatternProjectionRequest::new(
                            program, step, key, value, role, None, compiled, None, None,
                        ))
                        .unwrap(),
                );
            }
        }
    }
    fn request(&self, seed: u16) -> ExactTextProjectionRequest {
        let mut org = [0x11; 16];
        org[..2].copy_from_slice(&seed.to_be_bytes());
        let mut partition = PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
        partition.push_uuid(&org).unwrap();
        ExactTextProjectionRequest::new(
            Arc::clone(&self.query),
            partition.finish().unwrap(),
            CanonicalValue::Uuid(org),
            ApplicationRoleHash::from_bytes([0x33; 32]),
            None,
            ExactTextProfileV1::BinaryUtf8V1
                .bind_needle("alpha")
                .unwrap(),
            None,
            0,
            std::num::NonZeroU16::MIN,
            None,
        )
    }
}

#[test]
fn sequential_registration_churn_recycles_more_than_256_expired_owners() {
    isolated(
        "exact_text_adapter::retirement_tests::sequential_registration_churn_recycles_more_than_256_expired_owners",
        || {
            let fixture = Fixture::new();
            for seed in 0..1024 {
                if seed >= 256 {
                    fixture
                        .clock
                        .0
                        .store(u64::from(seed - 255) * 300, Ordering::SeqCst);
                }
                let captured = fixture
                    .runtime
                    .slot_for(&fixture.request(seed))
                    .unwrap_or_else(|error| panic!("registration {seed} failed: {error:?}"));
                assert!(matches!(
                    *captured.state.lock().unwrap(),
                    ExactTextSlotState::Building
                ));
                assert!(fixture.runtime.registries.lock().unwrap().slots.len() <= 256);
            }
            assert_eq!(fixture.runtime.registries.lock().unwrap().slots.len(), 256);
        },
    );
}

#[test]
fn protected_saturation_refuses_and_releases_only_the_unpinned_owner() {
    isolated(
        "exact_text_adapter::retirement_tests::protected_saturation_refuses_and_releases_only_the_unpinned_owner",
        || {
            let fixture = Fixture::new();
            let mut captures = Vec::new();
            for seed in 0..256 {
                captures.push(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            let oldest = Arc::downgrade(&captures[0].owner);
            let before = fixture
                .runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap();
            fixture.clock.0.store(300, Ordering::SeqCst);
            assert!(matches!(
                fixture.runtime.slot_for(&fixture.request(256)),
                Err(ExactTextProjectionPortError::Unavailable)
            ));
            assert_eq!(fixture.runtime.registries.lock().unwrap().slots.len(), 256);
            assert_eq!(
                fixture
                    .runtime
                    .ownership_graph
                    .budget
                    .observation()
                    .unwrap(),
                before
            );
            for capture in &captures {
                assert!(capture.pin.publication().is_ok());
            }
            drop(captures.remove(0));
            let successor = fixture.runtime.slot_for(&fixture.request(256)).unwrap();
            assert!(oldest.upgrade().is_none());
            assert!(successor.registration.matches(&fixture.request(256)));
            for (index, capture) in captures.iter().enumerate() {
                assert!(
                    capture
                        .registration
                        .matches(&fixture.request((index + 1) as u16))
                );
                assert!(capture.pin.publication().is_ok());
            }
            assert_eq!(fixture.runtime.registries.lock().unwrap().slots.len(), 256);
        },
    );
}

#[test]
fn minimum_retention_and_lru_touch_protect_the_original_exact_owner() {
    isolated(
        "exact_text_adapter::retirement_tests::minimum_retention_and_lru_touch_protect_the_original_exact_owner",
        || {
            let fixture = Fixture::new();
            for seed in 0..256 {
                drop(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            fixture.clock.0.store(299, Ordering::SeqCst);
            assert!(matches!(
                fixture.runtime.slot_for(&fixture.request(256)),
                Err(ExactTextProjectionPortError::Unavailable)
            ));
            let recent = fixture.runtime.slot_for(&fixture.request(0)).unwrap();
            let recent_owner = Arc::downgrade(&recent.owner);
            drop(recent);
            fixture.clock.0.store(300, Ordering::SeqCst);
            drop(fixture.runtime.slot_for(&fixture.request(256)).unwrap());
            assert!(recent_owner.upgrade().is_some());
            let same = fixture.runtime.slot_for(&fixture.request(0)).unwrap();
            assert!(Arc::ptr_eq(&recent_owner.upgrade().unwrap(), &same.owner));
            assert!(same.registration.matches(&fixture.request(0)));
        },
    );
}

#[test]
fn native_retirement_uses_canonical_key_to_break_equal_access_ordinals() {
    isolated(
        "exact_text_adapter::retirement_tests::native_retirement_uses_canonical_key_to_break_equal_access_ordinals",
        || {
            let fixture = Fixture::new();
            for seed in 0..16 {
                drop(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            let (expected, weak) = {
                let mut registries = fixture.runtime.registries.lock().unwrap();
                for entry in registries.slots.values_mut() {
                    entry.access.rebase(1);
                }
                let (key, entry) = registries.slots.first_key_value().unwrap();
                (*key, Arc::downgrade(&entry.slot.owner))
            };
            fixture.clock.0.store(300, Ordering::SeqCst);
            assert!(
                fixture
                    .runtime
                    .retire_one(Some(ProviderFamily::Text))
                    .unwrap()
            );
            assert!(
                !fixture
                    .runtime
                    .registries
                    .lock()
                    .unwrap()
                    .slots
                    .contains_key(&expected)
            );
            assert!(weak.upgrade().is_none());
            assert_eq!(fixture.runtime.registries.lock().unwrap().slots.len(), 15);
        },
    );
}

#[test]
fn cleanup_lane_contention_refuses_without_retiring_an_owner() {
    isolated(
        "exact_text_adapter::retirement_tests::cleanup_lane_contention_refuses_without_retiring_an_owner",
        || {
            let fixture = Fixture::new();
            for seed in 0..256 {
                drop(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            fixture.clock.0.store(300, Ordering::SeqCst);
            let cleanup = fixture.runtime.cleanup_work.lock().unwrap();
            assert!(matches!(
                fixture.runtime.slot_for(&fixture.request(256)),
                Err(ExactTextProjectionPortError::Unavailable)
            ));
            assert_eq!(fixture.runtime.registries.lock().unwrap().slots.len(), 256);
            for entry in fixture.runtime.registries.lock().unwrap().slots.values() {
                assert!(entry.slot.owner.is_active().unwrap());
            }
            drop(cleanup);
            assert!(fixture.runtime.slot_for(&fixture.request(256)).is_ok());
        },
    );
}

#[test]
fn saturated_budget_can_use_pre_admitted_cleanup_and_recover_census_capacity() {
    isolated(
        "exact_text_adapter::retirement_tests::saturated_budget_can_use_pre_admitted_cleanup_and_recover_census_capacity",
        || {
            use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;
            let fixture = Fixture::new();
            for seed in 0..256 {
                drop(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            let before = fixture
                .runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap();
            let available = EXACT_PROVIDER_BUDGET_BYTES
                - before.fixed_owner_bytes
                - before.owned_bytes
                - before.reserved_bytes;
            let protected = fixture
                .runtime
                .ownership_graph
                .budget
                .reserve(available)
                .unwrap();
            fixture.clock.0.store(300, Ordering::SeqCst);
            assert!(fixture.runtime.slot_for(&fixture.request(256)).is_ok());
            let after = fixture
                .runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap();
            assert!(after.owned_bytes < before.owned_bytes);
            assert!(after.released_allocations > before.released_allocations);
            assert!(fixture.runtime.registries.lock().unwrap().slots.len() < 256);
            assert!(
                after.fixed_owner_bytes + after.owned_bytes + after.reserved_bytes
                    <= EXACT_PROVIDER_BUDGET_BYTES
            );
            drop(protected);
        },
    );
}

#[test]
fn fixed_worker_inventory_does_not_pin_removed_owners_or_stop_healthy_capture() {
    isolated(
        "exact_text_adapter::retirement_tests::fixed_worker_inventory_does_not_pin_removed_owners_or_stop_healthy_capture",
        || {
            let fixture = Fixture::new();
            for seed in 0..16 {
                drop(fixture.runtime.slot_for(&fixture.request(seed)).unwrap());
            }
            let keys = fixture
                .runtime
                .registered_keys(ProviderFamily::Text)
                .unwrap();
            let oldest = slot_key(
                fixture.query.identity(),
                hash_partition_key(fixture.request(0).partition_key().as_bytes()),
                ApplicationRoleHash::from_bytes([0x33; 32]),
                None,
            );
            fixture.clock.0.store(300, Ordering::SeqCst);
            assert!(
                fixture
                    .runtime
                    .retire_one(Some(ProviderFamily::Text))
                    .unwrap()
            );
            assert!(fixture.runtime.registered_slots(oldest).unwrap().is_none());
            let mut healthy = 0;
            for key in keys.iter().copied() {
                if fixture.runtime.registered_slots(key).unwrap().is_some() {
                    healthy += 1;
                }
            }
            assert_eq!(healthy, 15);
        },
    );
}

#[test]
fn every_family_recycles_sequential_registrations_with_its_original_256_ceiling() {
    isolated(
        "exact_text_adapter::retirement_tests::every_family_recycles_sequential_registrations_with_its_original_256_ceiling",
        || {
            let fixture = Fixture::new();
            let mut time = 0;
            for family in [
                ProviderFamily::Predicate,
                ProviderFamily::Text,
                ProviderFamily::LongPattern,
                ProviderFamily::NullablePredicate,
                ProviderFamily::Tokenized,
            ] {
                for seed in 0..512 {
                    if seed >= 256 {
                        time += 300;
                        fixture.clock.0.store(time, Ordering::SeqCst);
                    }
                    fixture.register(family, seed);
                }
                assert_eq!(
                    fixture
                        .runtime
                        .registered_keys(family)
                        .unwrap()
                        .iter()
                        .count(),
                    256
                );
            }
            let registries = fixture.runtime.registries.lock().unwrap();
            assert_eq!(registries.slots.len(), 256);
            assert_eq!(registries.predicate_slots.len(), 256);
            assert_eq!(registries.nullable_predicate_slots.len(), 256);
            assert_eq!(registries.tokenized_slots.len(), 256);
            assert_eq!(registries.long_pattern_slots.len(), 256);
        },
    );
}

#[test]
fn retirement_removes_only_claimed_checkpoint_and_rebuild_has_fresh_generation_and_exact_rows() {
    isolated(
        "exact_text_adapter::retirement_tests::retirement_removes_only_claimed_checkpoint_and_rebuild_has_fresh_generation_and_exact_rows",
        || {
            let fixture = Fixture::new();
            let request = fixture.request(0);
            let mut rows = BTreeMap::new();
            for (index, title) in ["alpha Z", "unrelated", "alpha A", "alpha A"]
                .into_iter()
                .enumerate()
            {
                let mut key = riffdb_types::EntityKeyBuilder::new(
                    riffdb_types::EntityTypeId::new(1).unwrap(),
                );
                key.push_u32(index as u32).unwrap();
                rows.insert(
                    key.finish().unwrap(),
                    (
                        title.to_owned(),
                        CanonicalRecord::new(vec![(
                            riffdb_types::FieldId::new(1).unwrap(),
                            CanonicalValue::U64(index as u64),
                        )])
                        .unwrap(),
                    ),
                );
            }
            let mut expected: Vec<_> = rows
                .iter()
                .filter(|(_, (title, _))| title.contains("alpha"))
                .collect();
            expected.sort_by(|(a_key, (a, _)), (b_key, (b, _))| {
                a.cmp(b)
                    .then_with(|| a_key.as_bytes().cmp(b_key.as_bytes()))
            });
            let expected: Vec<_> = expected
                .into_iter()
                .map(|(key, (_, record))| (key.clone(), record.clone()))
                .collect();
            let mut prior = None;
            for attempt in 0..2 {
                let slot = fixture.runtime.slot_for(&request).unwrap();
                assert!(matches!(
                    *slot.state.lock().unwrap(),
                    ExactTextSlotState::Building
                ));
                let generation = fixture.runtime.generations.next(None).unwrap();
                if let Some(prior) = prior {
                    assert!(generation > prior);
                }
                prior = Some(generation);
                let provider = ExactTextPartitionIndexV2::rebuild_admitted_v1(
                    hash_partition_key(request.partition_key().as_bytes()),
                    generation,
                    CommitSequence::new(7).unwrap(),
                    ExactTextProfileV1::BinaryUtf8V1,
                    &rows,
                    &fixture.runtime.ownership_graph.budget,
                )
                .unwrap();
                let page = provider
                    .result_page(
                        riffdb_types::ExactTextOperatorV1::Contains,
                        request.needle(),
                        riffdb_types::ExactTextOrderV1::ValueAscEntityKey,
                        0,
                        std::num::NonZeroU16::new(10).unwrap(),
                    )
                    .unwrap();
                assert_eq!(page.exact_total(), 3);
                assert_eq!(
                    page.rows()
                        .iter()
                        .map(|row| (row.key().clone(), row.output().clone()))
                        .collect::<Vec<_>>(),
                    expected
                );
                persist_checked_slot(
                    &fixture.runtime,
                    &slot,
                    &ExactTextProviderState::V2(provider.clone()),
                )
                .unwrap();
                assert!(slot.checkpoint.exists());
                assert!(!slot.checkpoint.with_extension("pending").exists());
                let path = slot.checkpoint.clone();
                let owner = Arc::downgrade(&slot.owner);
                drop(provider);
                drop(slot);
                fixture.clock.0.store((attempt + 1) * 300, Ordering::SeqCst);
                assert!(
                    fixture
                        .runtime
                        .retire_one(Some(ProviderFamily::Text))
                        .unwrap()
                );
                assert!(!path.exists());
                assert!(owner.upgrade().is_none());
                assert!(fixture.runtime.registries.lock().unwrap().slots.is_empty());
            }
        },
    );
}

#[cfg(target_os = "linux")]
#[test]
fn replaced_checkpoint_preserves_foreign_object_and_retiring_owner_charge() {
    use std::os::unix::fs::PermissionsExt;
    isolated(
        "exact_text_adapter::retirement_tests::replaced_checkpoint_preserves_foreign_object_and_retiring_owner_charge",
        || {
            let fixture = Fixture::new();
            let request = fixture.request(0);
            let slot = fixture.runtime.slot_for(&request).unwrap();
            persist_predicate_checkpoint_bytes(
                &slot.checkpoint,
                &slot.registration.key(),
                1,
                b"derived",
                &fixture.runtime.ownership_graph.budget,
                &slot.owner,
            )
            .unwrap();
            let path = slot.checkpoint.clone();
            let replacement = path.with_extension("foreign");
            std::fs::write(&replacement, b"foreign evidence").unwrap();
            std::fs::set_permissions(&replacement, std::fs::Permissions::from_mode(0o600)).unwrap();
            std::fs::rename(&replacement, &path).unwrap();
            let owner = Arc::downgrade(&slot.owner);
            drop(slot);
            let before = fixture
                .runtime
                .ownership_graph
                .budget
                .observation()
                .unwrap();
            fixture.clock.0.store(300, Ordering::SeqCst);
            assert!(
                fixture
                    .runtime
                    .retire_one(Some(ProviderFamily::Text))
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), b"foreign evidence");
            let mut expected = before;
            assert_eq!(
                (
                    expected.active_owners,
                    expected.retiring_owners,
                    expected.pinned_owners,
                    expected.pins
                ),
                (1, 0, 0, 0)
            );
            expected.active_owners = 0;
            expected.retiring_owners = 1;
            assert_eq!(
                fixture
                    .runtime
                    .ownership_graph
                    .budget
                    .observation()
                    .unwrap(),
                expected
            );
            assert!(owner.upgrade().is_some());
            assert!(fixture.runtime.slot_for(&request).is_err());
            let healthy = fixture.runtime.slot_for(&fixture.request(1)).unwrap();
            assert!(healthy.pin.publication().is_ok());
            assert_eq!(
                fixture
                    .runtime
                    .registered_keys(ProviderFamily::Text)
                    .unwrap()
                    .iter()
                    .count(),
                1
            );
            assert!(
                fixture
                    .runtime
                    .retire_one(Some(ProviderFamily::Text))
                    .is_err()
            );
            assert_eq!(std::fs::read(&path).unwrap(), b"foreign evidence");
        },
    );
}

#[test]
fn captured_directory_custody_retains_graph_charge_after_runtime_destruction() {
    isolated(
        "exact_text_adapter::retirement_tests::captured_directory_custody_retains_graph_charge_after_runtime_destruction",
        || {
            let Fixture {
                runtime,
                clock,
                query,
                module,
                _root,
            } = Fixture::new();
            let mut org = [0x11; 16];
            org[..2].copy_from_slice(&0_u16.to_be_bytes());
            let mut partition = PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
            partition.push_uuid(&org).unwrap();
            let request = ExactTextProjectionRequest::new(
                query,
                partition.finish().unwrap(),
                CanonicalValue::Uuid(org),
                ApplicationRoleHash::from_bytes([0x33; 32]),
                None,
                ExactTextProfileV1::BinaryUtf8V1
                    .bind_needle("alpha")
                    .unwrap(),
                None,
                0,
                std::num::NonZeroU16::MIN,
                None,
            );
            let captured = runtime.slot_for(&request).unwrap();
            let budget = runtime.ownership_graph.budget.clone();
            drop(request);
            drop(module);
            drop(clock);
            drop(runtime);
            let retained = budget.observation().unwrap();
            assert_eq!(retained.reserved_bytes, 0);
            assert!(retained.owned_bytes >= 8 * 1024 * 1024);
            assert!(captured.pin.publication().is_ok());
            drop(captured);
            assert_eq!(budget.observation().unwrap().owned_bytes, 0);
            drop(_root);
        },
    );
}
