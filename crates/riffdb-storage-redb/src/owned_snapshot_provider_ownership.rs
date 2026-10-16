//! Native source ownership charged before retaining it in an exact provider.
use std::sync::Arc;

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{OwnershipCharge, ProviderBudget};

use super::RedbOwnedSnapshot;
use crate::error::storage_error;
use crate::store::RedbReadAccess;

pub(crate) struct ProviderSnapshotOwnership {
    // Source descriptors and fixed engine caches remain charged through every
    // snapshot/source-pin clone; generation owners separately deduplicate data.
    _generation: Arc<OwnershipCharge>,
    // A complete checkpoint's generation charge, or a pending checkpoint's
    // fixed ownership/graph marker. Pending completion admits its full data
    // charge before it can become visible through the captured capability.
    _checkpoint_generation: Option<Arc<OwnershipCharge>>,
    // The final retirement ticket also retains this fixed source charge.
    _source: Arc<OwnershipCharge>,
    // Last: queued native descriptors retain the numerical source charge until
    // bounded retirement has released their actual physical allocations.
    _retirement: crate::locator_memory::LocatorOwner,
}

impl ProviderSnapshotOwnership {
    pub(crate) fn belongs_to(&self, budget: &ProviderBudget) -> bool {
        self._source.belongs_to(budget)
    }

    pub(crate) fn admit(
        access: &RedbReadAccess,
        derived_cache: bool,
        budget: &ProviderBudget,
    ) -> Result<Arc<Self>, StorageError> {
        let generation = access.locator_capability().require_complete()?.clone();
        let (source_bytes, checkpoint_capability) = match &access {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (
                u64::try_from(
                    root.locator_capability()
                        .descriptor_charge_bound()?
                        .checked_add(root.retained_history_custody_bound())
                        .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?,
                )
                .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
                None,
            ),
            RedbReadAccess::Composite(view) => {
                let root = view.checkpoint_root_shared();
                let checkpoint = root.locator_capability().clone();
                (view.retained_provider_nonlocator_bound()?, Some(checkpoint))
            }
        };
        // The primary cache is compiled at 12 MiB; an optional derived read
        // retains its compiled 4-MiB cache. Include source and accounting control
        // allocations and lazily resolved fixed table handles in the allowance.
        let fixed = crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES as u64
            + if derived_cache {
                crate::store::REDB_CACHE_SIZE_BYTES as u64
            } else {
                0
            }
            + 32 * 1024;
        let allowance = source_bytes
            .checked_add(fixed)
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        // Reserve final-release ownership before attaching any provider charge.
        // This uses the existing native owner quota and allocation-free queue.
        let retirement = crate::locator_memory::LocatorOwner::try_new_with_bytes(
            &crate::locator_memory::NativeLocatorPool::global(),
            crate::locator_memory::LocatorAllocationClass::Ordinary,
            std::mem::size_of::<ProviderSnapshotOwnership>() + 64,
        )?;
        // Completion installation uses this same bounded gate. A checkpoint
        // still pending at preflight cannot grow between this complete plan's
        // reserve and its immutable provider-graph attachment.
        let _admission = crate::locator_capability::native_provider_admission_gate()?;
        let same_checkpoint = checkpoint_capability.as_ref().is_some_and(|checkpoint| {
            checkpoint.require_complete().is_ok_and(|checkpoint| {
                checkpoint
                    .owner_handle()
                    .same_owner(&generation.owner_handle())
            })
        });
        let needed = generation.provider_ownership_needed(budget)?;
        let checkpoint_needed = match &checkpoint_capability {
            Some(checkpoint) if !same_checkpoint => checkpoint.provider_ownership_needed(budget)?,
            _ => 0,
        };
        let total = allowance
            .checked_add(needed)
            .and_then(|bytes| bytes.checked_add(checkpoint_needed))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        // The entire incremental plan fits before its first immutable lifecycle
        // attachment. Every later operation only splits this admitted allowance.
        let mut work = budget
            .reserve(total)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let generation_owner = generation.admit_provider_ownership_from(&mut work, budget)?;
        let checkpoint_owner = match &checkpoint_capability {
            Some(_) if same_checkpoint => Some(Arc::clone(&generation_owner)),
            Some(checkpoint) => checkpoint.admit_provider_ownership_from(&mut work, budget)?,
            None => None,
        };
        let source = work
            .commit()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        retirement.attach_provider_charge(Arc::clone(&source))?;
        drop(_admission);
        Ok(Arc::new(ProviderSnapshotOwnership {
            _source: source,
            _generation: generation_owner,
            _checkpoint_generation: checkpoint_owner,
            _retirement: retirement,
        }))
    }
}

impl RedbOwnedSnapshot {
    pub(super) fn admit_provider_ownership_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<Arc<ProviderSnapshotOwnership>, StorageError> {
        if let Some(ownership) = &self.provider_ownership {
            if ownership.belongs_to(budget) {
                return Ok(Arc::clone(ownership));
            }
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.request_locator_construction()?;
        ProviderSnapshotOwnership::admit(&self.access, self.derived.is_some(), budget)
    }

    /// First-party exact providers retain native source ownership in the same
    /// aggregate budget as their own state. The requested logical source must
    /// be complete; a retained checkpoint may remain pending with its future
    /// completion bound to this same budget graph.
    #[doc(hidden)]
    pub fn with_provider_ownership_admitted_v1(
        mut self,
        budget: &ProviderBudget,
    ) -> Result<Self, StorageError> {
        let ownership = self.admit_provider_ownership_v1(budget)?;
        self.provider_ownership = Some(ownership);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locator_memory::LocatorAllocationClass;
    use crate::transient::LocatorGeneration;
    use riffdb_storage_api::{DatabaseInitializationPort, OwnedSnapshotReader};
    use riffdb_types::exact_provider_budget_v1::EXACT_PROVIDER_BUDGET_BYTES;

    fn fixture() -> (
        crate::test_path::ScopedDirectory,
        crate::RedbStore,
        RedbOwnedSnapshot,
    ) {
        let (segment, bytes) = crate::derived::follower_indexes::segment(
            vec![crate::derived::follower_indexes::capsule(
                riffdb_types::CommitSequence::first(),
            )],
            None,
        );
        let scope = crate::test_path::ScopedDirectory::new("provider-native-generation");
        let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        store.initialize_database(segment.database_id()).unwrap();
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(crate::layout::COMMITS)
            .unwrap()
            .insert(
                crate::keys::encode_application_sequence_key(riffdb_types::CommitSequence::first())
                    .as_slice(),
                bytes.as_bytes(),
            )
            .unwrap();
        write.commit().unwrap();
        let generation = LocatorGeneration::rebuild_from_segments(
            [Ok(Arc::new(segment))],
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        store
            .shared
            .bind_test_locator_generation(generation)
            .unwrap();
        let ports = crate::RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let snapshot = ports.open_owned_snapshot().unwrap();
        drop(ports);
        (scope, store, snapshot)
    }

    fn pending_checkpoint_composite_fixture() -> (
        crate::test_path::ScopedDirectory,
        crate::RedbStore,
        RedbOwnedSnapshot,
        Arc<crate::checkpoint_root::CheckpointRoot>,
    ) {
        use crate::derived::follower_indexes::{capsule, segment};
        use riffdb_types::{AdministrationSequence, CommitSequence};
        let first = CommitSequence::first();
        let second = CommitSequence::new(2).unwrap();
        let (base_segment, base_bytes) = segment(vec![capsule(first)], None);
        let (suffix_segment, suffix_bytes) =
            segment(vec![capsule(second)], Some(base_segment.segment_digest()));
        let scope = crate::test_path::ScopedDirectory::new("provider-pending-checkpoint-composite");
        let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        // Use the supported pre-V3 physical fixture constructor, as the
        // canonical composite-prune fixtures do. This unit fixture writes
        // command authority directly; it must not claim a V3 receipt chain
        // whose tail still describes an empty database.
        store
            .initialize_legacy_fixture(base_segment.database_id())
            .unwrap();
        let write = store.shared.database.begin_write().unwrap();
        write
            .open_table(crate::layout::COMMITS)
            .unwrap()
            .insert(
                crate::keys::encode_application_sequence_key(first).as_slice(),
                base_bytes.as_bytes(),
            )
            .unwrap();
        let application = crate::codec::encode_application_sequence_allocator_v1(
            riffdb_storage_api::ApplicationSequenceAllocator::Next(second),
        )
        .unwrap();
        let administration = crate::codec::encode_administration_sequence_allocator_v1(
            riffdb_storage_api::AdministrationSequenceAllocator::Next(
                AdministrationSequence::new(3).unwrap(),
            ),
        )
        .unwrap();
        {
            let mut metadata = write.open_table(crate::layout::META).unwrap();
            metadata
                .insert(
                    crate::layout::META_APPLICATION_SEQUENCE,
                    application.as_bytes(),
                )
                .unwrap();
            metadata
                .insert(
                    crate::layout::META_ADMINISTRATION_SEQUENCE,
                    administration.as_bytes(),
                )
                .unwrap();
        }
        write.commit().unwrap();
        let ports = crate::RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let root = ports.begin_read().unwrap().into_shared().unwrap();
        assert!(root.locator_capability().is_pending());
        let base_view = Arc::new(
            crate::composite_view::RedbCompositeViewBuilder::from_root(Arc::clone(&root), [0; 32])
                .unwrap()
                .freeze(),
        );
        let generation = LocatorGeneration::rebuild_from_segments(
            [Ok(Arc::new(base_segment)), Ok(Arc::new(suffix_segment))],
            LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        let mut stage =
            crate::composite_view::RedbCompositeMutationStage::from_published(&base_view).unwrap();
        let next_application = crate::codec::encode_application_sequence_allocator_v1(
            riffdb_storage_api::ApplicationSequenceAllocator::Next(CommitSequence::new(3).unwrap()),
        )
        .unwrap();
        let next_administration = crate::codec::encode_administration_sequence_allocator_v1(
            riffdb_storage_api::AdministrationSequenceAllocator::Next(
                AdministrationSequence::new(5).unwrap(),
            ),
        )
        .unwrap();
        for (key, prior, next) in [
            (
                crate::layout::META_APPLICATION_SEQUENCE,
                application.as_bytes(),
                next_application.as_bytes(),
            ),
            (
                crate::layout::META_ADMINISTRATION_SEQUENCE,
                administration.as_bytes(),
                next_administration.as_bytes(),
            ),
        ] {
            stage
                .apply(
                    &crate::journal::JournalMutation::replace(
                        crate::journal::JournalTable::Meta,
                        key.as_bytes().to_vec(),
                        prior,
                        next.to_vec(),
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        stage
            .apply(
                &crate::journal::JournalMutation::put(
                    crate::journal::JournalTable::Commits,
                    crate::keys::encode_application_sequence_key(second).to_vec(),
                    suffix_bytes.as_bytes().to_vec(),
                )
                .unwrap(),
            )
            .unwrap();
        stage.set_locator_generation(generation).unwrap();
        let view = stage
            .seal_encoded_frame(
                riffdb_storage_api::CompositeFrameKindV1::Command,
                root.locator_capability().source_binding().unwrap().0,
                Some(first),
                Some(second),
                AdministrationSequence::new(2),
                AdministrationSequence::new(4),
                1,
                suffix_bytes.as_bytes().len(),
                [0; 32],
                [1; 32],
            )
            .unwrap();
        let snapshot =
            RedbOwnedSnapshot::from_read_access(RedbReadAccess::Composite(Arc::new(view)));
        (scope, store, snapshot, root)
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn complete_provider_composite_admits_pending_checkpoint_and_charges_future_completion() {
        let (_scope, _store, snapshot, root) = pending_checkpoint_composite_fixture();
        let budget = ProviderBudget::new();
        let snapshot = snapshot
            .with_provider_ownership_admitted_v1(&budget)
            .unwrap();
        let view_generation = snapshot
            .access
            .locator_capability()
            .require_complete()
            .unwrap();
        assert!(
            view_generation
                .has_command_at(riffdb_types::CommitSequence::first())
                .unwrap()
        );
        assert!(
            view_generation
                .has_command_at(riffdb_types::CommitSequence::new(2).unwrap())
                .unwrap()
        );
        assert!(root.locator_capability().is_pending());
        assert_eq!(
            root.locator_capability()
                .require_complete()
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert!(
            snapshot
                .provider_ownership
                .as_ref()
                .unwrap()
                ._checkpoint_generation
                .is_some()
        );
        let base = RedbReadAccess::Current(Arc::clone(&root));
        let generation = LocatorGeneration::rebuild_from_access_with_owner(
            &base,
            root.locator_capability().construction_owner().unwrap(),
            LocatorAllocationClass::Ordinary,
            || false,
        )
        .unwrap();
        let before = budget.observation().unwrap();
        let occupied = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - before.fixed_owner_bytes - before.owned_bytes)
            .unwrap();
        assert_eq!(
            root.locator_capability()
                .install_generation(generation.clone())
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert!(root.locator_capability().is_pending());
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            before.owned_bytes
        );
        drop(occupied);
        root.locator_capability()
            .install_generation(generation)
            .unwrap();
        let complete = root.locator_capability().require_complete().unwrap();
        assert!(
            complete
                .has_command_at(riffdb_types::CommitSequence::first())
                .unwrap()
        );
        assert!(
            !complete
                .has_command_at(riffdb_types::CommitSequence::new(2).unwrap())
                .unwrap()
        );
        assert!(budget.observation().unwrap().owned_bytes > before.owned_bytes);
        assert!(
            view_generation
                .has_command_at(riffdb_types::CommitSequence::new(2).unwrap())
                .unwrap()
        );
    }

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn pending_checkpoint_foreign_graph_refuses_before_attaching_complete_view() {
        let (_scope, _store, snapshot, root) = pending_checkpoint_composite_fixture();
        let attached = ProviderBudget::new();
        {
            let _admission = crate::locator_capability::native_provider_admission_gate().unwrap();
            let pending = root.locator_capability();
            let mut work = attached
                .reserve(pending.provider_ownership_needed(&attached).unwrap() + 128)
                .unwrap();
            pending
                .admit_provider_ownership_from(&mut work, &attached)
                .unwrap();
            drop(work);
        }
        let foreign = ProviderBudget::new();
        let generation = snapshot
            .access
            .locator_capability()
            .require_complete()
            .unwrap()
            .clone();
        assert_eq!(
            snapshot
                .with_provider_ownership_admitted_v1(&foreign)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
        assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
        // Failed checkpoint preflight must leave the view free to join the
        // checkpoint's original graph, rather than strand a foreign charge.
        assert!(generation.provider_ownership_needed(&attached).unwrap() > 0);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn provider_native_snapshot_admission_counts_generation_and_retains_alias_charge() {
        let (_scope, _store, snapshot) = fixture();
        let budget = ProviderBudget::new();
        let generation = snapshot
            .access
            .locator_capability()
            .require_complete()
            .unwrap();
        let native_bound = generation.retained_ownership_bound().unwrap();
        let snapshot = snapshot
            .with_provider_ownership_admitted_v1(&budget)
            .unwrap();
        let owned = budget.observation().unwrap().owned_bytes;
        assert!(owned >= native_bound + crate::store::PRIMARY_REDB_CACHE_SIZE_BYTES as u64);
        let alias = snapshot.clone();
        assert_eq!(budget.observation().unwrap().owned_bytes, owned);
        drop(snapshot);
        assert_eq!(budget.observation().unwrap().owned_bytes, owned);
        // A second source descriptor shares the immutable generation owner.
        let next = RedbOwnedSnapshot::from_read_access(alias.access.clone())
            .with_provider_ownership_admitted_v1(&budget)
            .unwrap();
        let next_owned = budget.observation().unwrap().owned_bytes;
        let source_charge = next.provider_ownership.as_ref().unwrap()._source.bytes();
        assert_eq!(next_owned, owned + source_charge);
        drop(next);
        assert!(
            budget.observation().unwrap().owned_bytes >= owned,
            "the live alias remains charged while retired source cleanup may also be pending"
        );
        drop(alias);
        // The database's actual generation still owns its one attached charge;
        // queued source descriptors may conservatively retain additional bytes.
        assert!(budget.observation().unwrap().owned_bytes >= native_bound);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn competing_provider_families_share_one_native_generation_attachment() {
        let (_scope, _store, snapshot) = fixture();
        let budget = ProviderBudget::new();
        let start = Arc::new(std::sync::Barrier::new(3));
        let mut jobs = Vec::new();
        for _ in 0..2 {
            let source = snapshot.clone();
            let budget = budget.clone();
            let start = Arc::clone(&start);
            jobs.push(std::thread::spawn(move || {
                start.wait();
                source.with_provider_ownership_admitted_v1(&budget).unwrap()
            }));
        }
        start.wait();
        let first = jobs.remove(0).join().unwrap();
        let second = jobs.remove(0).join().unwrap();
        let first_owner = first.provider_ownership.as_ref().unwrap();
        let second_owner = second.provider_ownership.as_ref().unwrap();
        assert!(Arc::ptr_eq(
            &first_owner._generation,
            &second_owner._generation
        ));
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            first_owner._generation.bytes()
                + first_owner._source.bytes()
                + second_owner._source.bytes()
        );
    }

    // req: OQ-040, TXN-042
    #[test]
    fn foreign_provider_graph_cannot_reuse_an_attached_native_generation_charge() {
        let (_scope, _store, snapshot) = fixture();
        let first = ProviderBudget::new();
        let owner = snapshot
            .clone()
            .with_provider_ownership_admitted_v1(&first)
            .unwrap();
        let before = first.observation().unwrap().owned_bytes;
        let foreign = ProviderBudget::new();
        assert_eq!(
            snapshot
                .with_provider_ownership_admitted_v1(&foreign)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(foreign.observation().unwrap().owned_bytes, 0);
        assert_eq!(foreign.observation().unwrap().reserved_bytes, 0);
        assert_eq!(first.observation().unwrap().owned_bytes, before);
        drop(owner);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn provider_native_snapshot_pressure_refuses_without_retaining_a_source() {
        let (_scope, _store, snapshot) = fixture();
        let budget = ProviderBudget::new();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget.reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed).unwrap();
        assert_eq!(
            snapshot
                .with_provider_ownership_admitted_v1(&budget)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn provider_native_partial_capacity_refuses_before_attaching_generation() {
        let (_scope, _store, snapshot) = fixture();
        let budget = ProviderBudget::new();
        let generation_bytes = snapshot
            .access
            .locator_capability()
            .require_complete()
            .unwrap()
            .retained_ownership_bound()
            .unwrap();
        let fixed = budget.observation().unwrap().fixed_owner_bytes;
        let occupied = budget
            .reserve(EXACT_PROVIDER_BUDGET_BYTES - fixed - generation_bytes)
            .unwrap();
        assert_eq!(
            snapshot
                .with_provider_ownership_admitted_v1(&budget)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            0,
            "the complete native capture must fit before its first immutable attachment"
        );
        drop(occupied);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn provider_source_charge_survives_bounded_native_descriptor_retirement() {
        use crate::locator_memory::{
            ChargedLocatorAllocation, LocatorOwner, LocatorRetireContents, NativeLocatorPool,
        };
        struct DeferredSource {
            remaining: usize,
        }
        impl LocatorRetireContents for DeferredSource {
            fn retire_one(&mut self) -> bool {
                self.remaining = self.remaining.saturating_sub(1);
                self.remaining == 0
            }
            fn take_for_retirement(&mut self) -> Self {
                Self {
                    remaining: std::mem::take(&mut self.remaining),
                }
            }
        }
        let pool = NativeLocatorPool::isolated_for_tests();
        let native = ChargedLocatorAllocation::try_new(
            &pool,
            LocatorAllocationClass::Ordinary,
            2048,
            || Ok(DeferredSource { remaining: 513 }),
        )
        .unwrap();
        let budget = ProviderBudget::new();
        let source = budget.reserve(512).unwrap().commit().unwrap();
        let retirement = LocatorOwner::try_new(&pool, LocatorAllocationClass::Ordinary).unwrap();
        retirement
            .attach_provider_charge(Arc::clone(&source))
            .unwrap();
        let ownership = ProviderSnapshotOwnership {
            _generation: budget.reserve(1024).unwrap().commit().unwrap(),
            _checkpoint_generation: None,
            _source: source,
            _retirement: retirement,
        };
        drop(native);
        drop(ownership);
        assert_eq!(budget.observation().unwrap().owned_bytes, 512);
        assert!(pool.drain_retired_step());
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            512,
            "final source release cannot uncharge physical descriptors still in bounded retirement"
        );
        for _ in 0..8 {
            pool.drain_retired_step();
        }
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    }

    // req: OQ-040, TXN-042
    #[test]
    fn provider_native_pending_directory_never_admits_zero_owned_bytes() {
        let scope = crate::test_path::ScopedDirectory::new("provider-pending-generation");
        let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
        store
            .initialize_database(
                riffdb_types::DatabaseId::from_unix_milliseconds_and_random(
                    1_700_000_000_000,
                    [0x76; 10],
                )
                .unwrap(),
            )
            .unwrap();
        let ports = crate::RedbOperationalPorts {
            shared: Arc::clone(&store.shared),
        };
        let snapshot = ports.open_owned_snapshot().unwrap();
        let access = snapshot.complete_logical_access_for_test();
        let budget = ProviderBudget::new();
        assert_eq!(
            ProviderSnapshotOwnership::admit(access, false, &budget)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
