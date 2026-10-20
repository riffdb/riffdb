//! Native source ownership charged before retaining it in an exact provider.
#[cfg(test)]
use std::sync::Arc;

use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_types::exact_provider_budget_v1::{ProviderBudget, SharedOwnershipCharge};

use super::RedbOwnedSnapshot;
use crate::error::storage_error;
use crate::store::RedbReadAccess;

pub(crate) struct ProviderSnapshotOwnership {
    // Source descriptors remain charged through every snapshot/source-pin
    // clone. Actual engine owners separately share each physical cache charge.
    _generation: SharedOwnershipCharge,
    // A complete checkpoint's generation charge, or a pending checkpoint's
    // fixed ownership/graph marker. Pending completion admits its full data
    // charge before it can become visible through the captured capability.
    _checkpoint_generation: Option<SharedOwnershipCharge>,
    // The final retirement ticket also retains this fixed source charge.
    _source: SharedOwnershipCharge,
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
        derived_engine: Option<
            &riffdb_types::OwnedCapacitySharedV1<crate::engine_owner::EngineOwner<redb::Database>>,
        >,
        budget: &ProviderBudget,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<Self>, StorageError> {
        let generation = access.locator_capability().require_complete()?.clone();
        let root = match access {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (root).clone(),
            RedbReadAccess::Composite(view) => view.checkpoint_root_shared(),
        };
        let primary_engine = root
            .engine_custody()
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let checkpoint_capability = match access {
            RedbReadAccess::Composite(view) => {
                Some(view.checkpoint_root_shared().locator_capability().clone())
            }
            _ => None,
        };
        let resolver = root.history_resolver_shared();
        let resolver_bound = resolver
            .map(|resolver| {
                u64::try_from(resolver.retained_charge_bound())
                    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))
            })
            .transpose()?;
        let view_metadata = match access {
            RedbReadAccess::Composite(view) => Some(view.as_ref()),
            _ => None,
        };
        // Only this newly allocated source capsule is unique. Actual engines,
        // cached root handles, extent custody and views own shared attachments.
        let allowance: u64 = 32 * 1024;
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
        let current_pool = (generation.provider_family_pool()).clone();
        let capability_pool = access.locator_capability().provider_family_pool()?;
        if !current_pool.same_provider_family(&capability_pool) {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let checkpoint_pool = checkpoint_capability
            .as_ref()
            .map(|capability| capability.provider_family_pool())
            .transpose()?;
        // Four closed sharing domains at most: current/checkpoint locator
        // generations, original receipt sources, and the composite overlay.
        // Each admission consults cached physical counters, never a graph walk.
        let current_family = current_pool.provider_accounting_family()?;
        let (suffix_family, overlay_family) = match access {
            RedbReadAccess::Composite(view) => (
                view.changelog_suffix()
                    .provider_ownership_family()?
                    .cloned(),
                view.overlay().provider_ownership_family().cloned(),
            ),
            _ => (None, None),
        };
        let checkpoint_family = checkpoint_pool
            .as_ref()
            .map(|pool| pool.provider_accounting_family().cloned())
            .transpose()?;
        // Fixed arrays keep the complete preflight free of unadmitted heaps.
        let mut families = [
            Some((current_family).clone()),
            checkpoint_family,
            suffix_family,
            overlay_family,
        ];
        families.sort_unstable_by_key(|family| {
            family
                .as_ref()
                .map_or(usize::MAX, |family| family.identity())
        });
        for index in 1..4 {
            if matches!((&families[index - 1], &families[index]),
                (Some(first), Some(second)) if first.same_family(second))
            {
                families[index - 1] = None;
            }
        }
        let [first, second, third, fourth] = &families;
        let mut family_guards = [
            first
                .as_ref()
                .map(|family| {
                    family
                        .lock()
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
                })
                .transpose()?,
            second
                .as_ref()
                .map(|family| {
                    family
                        .lock()
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
                })
                .transpose()?,
            third
                .as_ref()
                .map(|family| {
                    family
                        .lock()
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
                })
                .transpose()?,
            fourth
                .as_ref()
                .map(|family| {
                    family
                        .lock()
                        .map_err(|_| storage_error(StorageErrorKind::Unavailable))
                })
                .transpose()?,
        ];
        let family_needed = family_guards
            .iter()
            .flatten()
            .try_fold(0_u64, |bytes, family| {
                let needed = family
                    .provider_ownership_needed(budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
                bytes
                    .checked_add(needed)
                    .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))
            })?;
        let current_binding = access
            .locator_capability()
            .prepare_provider_binding(budget)?;
        let checkpoint_binding = match &checkpoint_capability {
            Some(checkpoint) if !checkpoint.same_construction(access.locator_capability()) => {
                Some(checkpoint.prepare_provider_binding(budget)?)
            }
            _ => None,
        };
        let primary_needed = primary_engine.provider_ownership_needed(budget)?;
        let derived_needed = match derived_engine {
            Some(derived) if !primary_engine.is_writable_owner(derived) => {
                derived.provider_ownership_needed(budget)?
            }
            _ => 0,
        };
        let root_needed = root.provider_metadata_needed(budget)?;
        let resolver_needed = resolver
            .zip(resolver_bound)
            .map(|(resolver, bound)| {
                resolver
                    .provider_metadata_cell()
                    .provider_ownership_needed(bound, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?
            .unwrap_or(0);
        let view_needed = view_metadata
            .map(|view| view.provider_metadata_needed(budget))
            .transpose()?
            .unwrap_or(0);
        let total = allowance
            .checked_add(family_needed)
            .and_then(|bytes| bytes.checked_add(primary_needed))
            .and_then(|bytes| bytes.checked_add(derived_needed))
            .and_then(|bytes| bytes.checked_add(root_needed))
            .and_then(|bytes| bytes.checked_add(resolver_needed))
            .and_then(|bytes| bytes.checked_add(view_needed))
            .ok_or_else(|| storage_error(StorageErrorKind::LimitExceeded))?;
        // The entire incremental plan fits before its first immutable lifecycle
        // attachment. Every later operation only splits this admitted allowance.
        let mut work = budget
            .reserve(total)
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        let [first, second, third, fourth] = &mut family_guards;
        let first_plan = first
            .as_mut()
            .map(|family| {
                family
                    .prepare_provider_ownership(&mut work, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?;
        let second_plan = second
            .as_mut()
            .map(|family| {
                family
                    .prepare_provider_ownership(&mut work, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?;
        let third_plan = third
            .as_mut()
            .map(|family| {
                family
                    .prepare_provider_ownership(&mut work, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?;
        let fourth_plan = fourth
            .as_mut()
            .map(|family| {
                family
                    .prepare_provider_ownership(&mut work, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?;
        let family_plans = [first_plan, second_plan, third_plan, fourth_plan];
        let marker_for =
            |family: &riffdb_types::exact_provider_budget_v1::PhysicalOwnershipFamily| {
                families
                    .iter()
                    .position(|candidate| {
                        candidate
                            .as_ref()
                            .is_some_and(|candidate| candidate.same_family(family))
                    })
                    .and_then(|index| family_plans[index].as_ref().map(|plan| plan.marker()))
                    .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))
            };
        let generation_owner = marker_for(current_family)?;
        let checkpoint_owner = checkpoint_pool
            .as_ref()
            .map(|pool| marker_for(pool.provider_accounting_family()?))
            .transpose()?;
        // Keep both bounded engine slots locked and all new charges PRIVATE
        // until every remaining plan piece has passed its fallible preparation.
        let primary_plan = primary_engine.prepare_provider_ownership_from(&mut work, budget)?;
        let derived_plan = match derived_engine {
            Some(derived) if !primary_engine.is_writable_owner(derived) => {
                Some(derived.prepare_provider_ownership_from(&mut work, budget)?)
            }
            _ => None,
        };
        let root_plan = root.prepare_provider_metadata_from(&mut work, budget)?;
        let resolver_plan = resolver
            .zip(resolver_bound)
            .map(|(resolver, bound)| {
                resolver
                    .provider_metadata_cell()
                    .prepare_provider_ownership_from(bound, &mut work, budget)
                    .map_err(|_| storage_error(StorageErrorKind::Unavailable))
            })
            .transpose()?;
        let view_plan = view_metadata
            .map(|view| view.prepare_provider_metadata_from(&mut work, budget))
            .transpose()?;
        let source = work
            .commit()
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        retirement.attach_provider_charge(source.clone())?;
        // Allocate the complete source owner before the first visible cut.
        // Every remaining operation only transfers already prepared ownership.
        let ownership = riffdb_types::OwnedCapacitySharedV1::new(ProviderSnapshotOwnership {
            _source: source,
            _generation: generation_owner.clone(),
            _checkpoint_generation: checkpoint_owner.clone(),
            _retirement: retirement,
        });
        for plan in family_plans.into_iter().flatten() {
            plan.commit();
        }
        current_binding.commit(generation_owner);
        if let (Some(binding), Some(marker)) = (checkpoint_binding, checkpoint_owner) {
            binding.commit(marker);
        }
        primary_plan.commit();
        if let Some(derived_plan) = derived_plan {
            derived_plan.commit();
        }
        root_plan.commit();
        if let Some(plan) = resolver_plan {
            plan.commit();
        }
        if let Some(plan) = view_plan {
            plan.commit();
        }
        Ok(ownership)
    }
}

impl RedbOwnedSnapshot {
    pub(super) fn admit_provider_ownership_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<riffdb_types::OwnedCapacitySharedV1<ProviderSnapshotOwnership>, StorageError> {
        if let Some(ownership) = &self.provider_ownership {
            if ownership.belongs_to(budget) {
                return Ok((ownership).clone());
            }
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        self.request_locator_construction()?;
        if self.derived.is_some() != self._derived_engine.is_some() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        ProviderSnapshotOwnership::admit(&self.access, self._derived_engine.as_ref(), budget)
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
    include!("owned_snapshot_overlay_ownership_tests.rs");
    include!("owned_snapshot_metadata_ownership_tests.rs");
    include!("owned_snapshot_root_weak_tests.rs");
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
            shared: store.shared.clone(),
        };
        let snapshot = ports.open_owned_snapshot().unwrap();
        drop(ports);
        (scope, store, snapshot)
    }

    fn pending_checkpoint_composite_fixture() -> (
        crate::test_path::ScopedDirectory,
        crate::RedbStore,
        RedbOwnedSnapshot,
        crate::checkpoint_root::CheckpointRootOwner,
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
        store
            .shared
            .register_history_resolver(base_segment.database_id());
        let ports = crate::RedbOperationalPorts {
            shared: store.shared.clone(),
        };
        let root = ports.begin_read().unwrap().into_shared().unwrap();
        assert!(root.locator_capability().is_pending());
        let base_view = riffdb_types::OwnedCapacitySharedV1::new(
            crate::composite_view::RedbCompositeViewBuilder::from_root(root.clone(), [0; 32])
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
        let snapshot = RedbOwnedSnapshot::from_read_access(RedbReadAccess::Composite(
            riffdb_types::OwnedCapacitySharedV1::new(view),
        ));
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
        let base = RedbReadAccess::Current(root.clone());
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

    // req: OQ-040, TXN-042, STO-032
    #[test]
    fn separate_provider_sources_share_one_physical_engine_cache_owner() {
        use riffdb_storage_api::AuthoritativePointReader;
        const SOURCE_CONTROL_ROOM: u64 = 64 * 1024;
        let (_scope, store, snapshot) = fixture();
        let command = crate::command_authority::command_member_at_access(
            &snapshot.access,
            riffdb_types::CommitSequence::first(),
        )
        .unwrap()
        .unwrap();
        let outcome = command.base().outcome().clone();
        let provenance = command.base().provenance().clone();
        drop(command);
        let budget = ProviderBudget::new();
        let first = snapshot
            .with_provider_ownership_admitted_v1(&budget)
            .unwrap();
        let before = budget.observation().unwrap();
        // The second source shares the exact existing root, generation and
        // actual engine. Only its separate bounded control owner is new.
        let second = RedbOwnedSnapshot::from_read_access(first.access.clone());
        let other_build = budget
            .reserve(
                EXACT_PROVIDER_BUDGET_BYTES
                    - before.fixed_owner_bytes
                    - before.owned_bytes
                    - before.reserved_bytes
                    - SOURCE_CONTROL_ROOM,
            )
            .unwrap();
        let second = second
            .with_provider_ownership_admitted_v1(&budget)
            .expect("the same physical engine cache has already been admitted");
        let after = budget.observation().unwrap();
        assert!(after.owned_bytes >= before.owned_bytes);
        assert!(after.owned_bytes - before.owned_bytes <= SOURCE_CONTROL_ROOM);
        drop(other_build);
        drop(first);
        drop(store);
        assert_eq!(
            second.read_stored_outcome(outcome.identity()).unwrap(),
            Some(outcome)
        );
        assert_eq!(
            second.read_provenance(provenance.provenance_id()).unwrap(),
            Some(provenance)
        );
        assert!(budget.observation().unwrap().owned_bytes > 0);
        drop(second);
        let pool = crate::locator_memory::NativeLocatorPool::global();
        for _ in 0..4096 {
            if budget.observation().unwrap().owned_bytes == 0 {
                break;
            }
            pool.drain_retired_step();
        }
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
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
            let start = start.clone();
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
        assert!(
            first_owner
                ._generation
                .same_allocation(&second_owner._generation)
        );
        let _admission = crate::locator_capability::native_provider_admission_gate().unwrap();
        let family = first
            .access
            .locator_capability()
            .provider_family_pool()
            .unwrap();
        let checkpoint = match &first.access {
            RedbReadAccess::Composite(view) => Some(
                view.checkpoint_root_shared()
                    .locator_capability()
                    .provider_family_pool()
                    .unwrap(),
            ),
            _ => None,
        };
        // Physical reclamation legitimately changes the family balance. Hold
        // its bounded guards until the exact whole-budget observation completes.
        let family_guard = family.lock_provider_family().unwrap();
        let checkpoint_guard = match &checkpoint {
            Some(checkpoint) if !family.same_provider_family(checkpoint) => {
                Some(checkpoint.lock_provider_family().unwrap())
            }
            _ => None,
        };
        let native_owned = family_guard.provider_owned_bytes()
            + checkpoint_guard
                .as_ref()
                .map_or(0, |guard| guard.provider_owned_bytes());
        let metadata_root = match &first.access {
            RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (root).clone(),
            RedbReadAccess::Composite(view) => view.checkpoint_root_shared(),
        };
        let shared_metadata = metadata_root.provider_metadata_charge_bytes()
            + metadata_root
                .history_resolver_shared()
                .map_or(0, |resolver| {
                    resolver.provider_metadata_cell().owned_bytes().unwrap()
                })
            + match &first.access {
                RedbReadAccess::Composite(view) => view.provider_metadata_charge_bytes(),
                _ => 0,
            };
        assert_eq!(
            budget.observation().unwrap().owned_bytes,
            native_owned
                + shared_metadata
                + first_owner._source.bytes()
                + second_owner._source.bytes()
                + match &first.access {
                    RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) =>
                        root.engine_custody().unwrap().provider_charge_bytes(),
                    RedbReadAccess::Composite(view) => view
                        .checkpoint_root_shared()
                        .engine_custody()
                        .unwrap()
                        .provider_charge_bytes(),
                }
                + first
                    ._derived_engine
                    .as_ref()
                    .map_or(0, |engine| engine.provider_charge_bytes())
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
                .clone()
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
        let later_graph = ProviderBudget::new();
        let admitted = snapshot.with_provider_ownership_admitted_v1(&later_graph)
            .expect("refusal cannot leave either engine, capability or native family bound to the failed graph");
        assert!(
            admitted
                .provider_ownership
                .as_ref()
                .unwrap()
                .belongs_to(&later_graph)
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
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
        retirement.attach_provider_charge(source.clone()).unwrap();
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
            shared: store.shared.clone(),
        };
        let snapshot = ports.open_owned_snapshot().unwrap();
        let access = snapshot.complete_logical_access_for_test();
        let budget = ProviderBudget::new();
        assert_eq!(
            ProviderSnapshotOwnership::admit(access, None, &budget)
                .err()
                .unwrap()
                .kind(),
            StorageErrorKind::Unavailable
        );
        assert_eq!(budget.observation().unwrap().owned_bytes, 0);
        assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
    }
}
