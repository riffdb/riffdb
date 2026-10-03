//! Bounded discovery and access order shared by all exact-provider families.

use super::*;

pub(super) struct RegistryEntry<T> {
    pub(super) slot: Arc<T>,
    pub(super) access: ProviderAccess,
    pub(super) cleanup_in_progress: bool,
}

/// Canonical family namespace order, also used by ordinal rebasing.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ProviderFamily {
    Predicate,
    Text,
    LongPattern,
    NullablePredicate,
    Tokenized,
}

pub(super) enum RetiringProviderSlot {
    Predicate(Arc<ExactPredicateSlot>),
    Text(Arc<ExactTextSlot>),
    LongPattern(Arc<LongPatternSlot>),
    NullablePredicate(Arc<NullableExactPredicateSlot>),
    Tokenized(Arc<TokenizedTextSlot>),
}
impl RetiringProviderSlot {
    pub(super) fn owner_and_checkpoint(&self) -> (&ProviderOwner, &Path) {
        match self {
            Self::Predicate(slot) => (&slot.owner, &slot.checkpoint),
            Self::Text(slot) => (&slot.owner, &slot.checkpoint),
            Self::LongPattern(slot) => (&slot.owner, &slot.checkpoint),
            Self::NullablePredicate(slot) => (&slot.owner, &slot.checkpoint),
            Self::Tokenized(slot) => (&slot.owner, &slot.checkpoint),
        }
    }
}
pub(super) struct RetirementCandidate {
    family: ProviderFamily,
    key: SlotKey,
    pub(super) slot: RetiringProviderSlot,
}

pub(super) struct RegisteredSlotKeys(
    Arc<riffdb_types::exact_provider_budget_v1::ProviderAllocation<Vec<SlotKey>>>,
);
impl RegisteredSlotKeys {
    pub(super) fn iter(&self) -> std::slice::Iter<'_, SlotKey> {
        self.0.iter()
    }
    // Closed vector/root/header capacity; no census or extra scratch allocation.
    pub(super) const WORK_BYTES: u64 =
        (4 * MAX_REGISTERED_EXACT_PARTITIONS * std::mem::size_of::<SlotKey>() + 4096) as u64;
}

/// One discovery lock establishes a total access order across all families.
#[derive(Default)]
pub(super) struct ExactProviderRegistries {
    pub(super) observed_clock: Option<CursorTick>,
    ordinal: u64,
    pub(super) slots: BTreeMap<SlotKey, RegistryEntry<ExactTextSlot>>,
    pub(super) predicate_slots: BTreeMap<SlotKey, RegistryEntry<ExactPredicateSlot>>,
    pub(super) nullable_predicate_slots:
        BTreeMap<SlotKey, RegistryEntry<NullableExactPredicateSlot>>,
    pub(super) tokenized_slots: BTreeMap<SlotKey, RegistryEntry<TokenizedTextSlot>>,
    pub(super) long_pattern_slots: BTreeMap<SlotKey, RegistryEntry<LongPatternSlot>>,
}

impl ExactProviderRegistries {
    /// Admitted fixed-capacity inventory holds no pins. Each worker captures one live
    /// owner immediately before its work, leaving unrelated owners eligible.
    pub(super) fn registered_keys(
        &self,
        family: ProviderFamily,
        budget: &riffdb_types::exact_provider_budget_v1::ProviderBudget,
    ) -> Result<RegisteredSlotKeys, ExactTextProjectionPortError> {
        let work = budget
            .reserve(RegisteredSlotKeys::WORK_BYTES)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        let mut keys = Vec::with_capacity(MAX_REGISTERED_EXACT_PARTITIONS);
        macro_rules! capture {
            ($map:ident) => {
                for (key, entry) in &self.$map {
                    if !entry
                        .slot
                        .owner
                        .is_active()
                        .map_err(|_| ExactTextProjectionPortError::Unavailable)?
                    {
                        continue;
                    }
                    if keys.len() == MAX_REGISTERED_EXACT_PARTITIONS {
                        return Err(ExactTextProjectionPortError::Integrity);
                    }
                    keys.push(*key);
                }
            };
        }
        match family {
            ProviderFamily::Predicate => capture!(predicate_slots),
            ProviderFamily::Text => capture!(slots),
            ProviderFamily::LongPattern => capture!(long_pattern_slots),
            ProviderFamily::NullablePredicate => capture!(nullable_predicate_slots),
            ProviderFamily::Tokenized => capture!(tokenized_slots),
        }
        work.commit_value(keys)
            .map(RegisteredSlotKeys)
            .map_err(|_| ExactTextProjectionPortError::Unavailable)
    }

    /// Selection and admission closure happen under discovery; the returned
    /// references retain all data while cleanup executes outside this lock.
    pub(super) fn begin_retirement(
        &mut self,
        required_family: Option<ProviderFamily>,
        observed: CursorTick,
    ) -> Result<Option<RetirementCandidate>, ExactTextProjectionPortError> {
        let mut selected: Option<(u64, ProviderFamily, SlotKey)> = None;
        macro_rules! consider {
            ($family:ident, $map:ident) => {
                if required_family.is_none_or(|family| family == ProviderFamily::$family) {
                    for (key, entry) in &self.$map {
                        if entry.cleanup_in_progress {
                            continue;
                        }
                        let Some(retiring) = entry
                            .slot
                            .owner
                            .retirement_candidate()
                            .map_err(|_| ExactTextProjectionPortError::Unavailable)?
                        else {
                            continue;
                        };
                        if !retiring && !entry.access.retention_expired_at(observed)? {
                            continue;
                        }
                        let candidate = (entry.access.ordinal(), ProviderFamily::$family, *key);
                        if selected.is_none_or(|prior| candidate < prior) {
                            selected = Some(candidate);
                        }
                    }
                }
            };
        }
        consider!(Predicate, predicate_slots);
        consider!(Text, slots);
        consider!(LongPattern, long_pattern_slots);
        consider!(NullablePredicate, nullable_predicate_slots);
        consider!(Tokenized, tokenized_slots);
        let Some((_, family, key)) = selected else {
            return Ok(None);
        };
        macro_rules! claim {
            ($variant:ident, $map:ident) => {{
                let entry = self
                    .$map
                    .get_mut(&key)
                    .ok_or(ExactTextProjectionPortError::Integrity)?;
                let already_retiring = entry
                    .slot
                    .owner
                    .retirement_candidate()
                    .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
                if already_retiring.is_none() {
                    return Ok(None);
                }
                if already_retiring == Some(false)
                    && !entry
                        .slot
                        .owner
                        .retire_if_unpinned()
                        .map_err(|_| ExactTextProjectionPortError::Unavailable)?
                {
                    return Ok(None);
                }
                entry.cleanup_in_progress = true;
                RetiringProviderSlot::$variant(Arc::clone(&entry.slot))
            }};
        }
        let slot = match family {
            ProviderFamily::Predicate => claim!(Predicate, predicate_slots),
            ProviderFamily::Text => claim!(Text, slots),
            ProviderFamily::LongPattern => claim!(LongPattern, long_pattern_slots),
            ProviderFamily::NullablePredicate => {
                claim!(NullablePredicate, nullable_predicate_slots)
            }
            ProviderFamily::Tokenized => claim!(Tokenized, tokenized_slots),
        };
        Ok(Some(RetirementCandidate { family, key, slot }))
    }

    /// The removed Arc is returned so final data/charge destruction happens
    /// after discovery unlocks. Failure retains the tombstone for a safe retry.
    pub(super) fn finish_retirement(
        &mut self,
        candidate: &RetirementCandidate,
        cleaned: bool,
    ) -> Result<Option<RetiringProviderSlot>, ExactTextProjectionPortError> {
        macro_rules! finish {
            ($variant:ident, $map:ident, $slot:ident) => {{
                let entry = self
                    .$map
                    .get_mut(&candidate.key)
                    .ok_or(ExactTextProjectionPortError::Integrity)?;
                if !entry.cleanup_in_progress || !Arc::ptr_eq(&entry.slot, $slot) {
                    return Err(ExactTextProjectionPortError::Integrity);
                }
                if cleaned {
                    self.$map
                        .remove(&candidate.key)
                        .map(|entry| RetiringProviderSlot::$variant(entry.slot))
                } else {
                    entry.cleanup_in_progress = false;
                    None
                }
            }};
        }
        let matches_family = matches!(
            (&candidate.slot, candidate.family),
            (
                RetiringProviderSlot::Predicate(_),
                ProviderFamily::Predicate
            ) | (RetiringProviderSlot::Text(_), ProviderFamily::Text)
                | (
                    RetiringProviderSlot::LongPattern(_),
                    ProviderFamily::LongPattern
                )
                | (
                    RetiringProviderSlot::NullablePredicate(_),
                    ProviderFamily::NullablePredicate
                )
                | (
                    RetiringProviderSlot::Tokenized(_),
                    ProviderFamily::Tokenized
                )
        );
        if !matches_family {
            return Err(ExactTextProjectionPortError::Integrity);
        }
        Ok(match &candidate.slot {
            RetiringProviderSlot::Predicate(slot) => finish!(Predicate, predicate_slots, slot),
            RetiringProviderSlot::Text(slot) => finish!(Text, slots, slot),
            RetiringProviderSlot::LongPattern(slot) => {
                finish!(LongPattern, long_pattern_slots, slot)
            }
            RetiringProviderSlot::NullablePredicate(slot) => {
                finish!(NullablePredicate, nullable_predicate_slots, slot)
            }
            RetiringProviderSlot::Tokenized(slot) => finish!(Tokenized, tokenized_slots, slot),
        })
    }

    pub(super) fn next_access(
        &mut self,
        clock: &dyn CursorMonotonicClock,
    ) -> Result<(u64, CursorTick), ExactTextProjectionPortError> {
        let observed = self.observe_access(clock)?;
        if self.ordinal == u64::MAX {
            self.rebase()?;
        }
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .ok_or(ExactTextProjectionPortError::Unavailable)?;
        Ok((self.ordinal, observed))
    }

    /// The stack scratch has a fixed ceiling and never allocates. Sort by old
    /// access order, then canonical family/key, before changing any ordinal.
    fn rebase(&mut self) -> Result<(), ExactTextProjectionPortError> {
        let mut order: [AccessReference<'_>; 5 * MAX_REGISTERED_EXACT_PARTITIONS] =
            [const { None }; 5 * MAX_REGISTERED_EXACT_PARTITIONS];
        let mut count = 0;
        macro_rules! capture {
            ($family:expr, $map:ident) => {
                for (key, entry) in &mut self.$map {
                    let place = order
                        .get_mut(count)
                        .ok_or(ExactTextProjectionPortError::Integrity)?;
                    *place = Some((&mut entry.access, $family, *key));
                    count += 1;
                }
            };
        }
        capture!(0_u8, predicate_slots);
        capture!(1_u8, slots);
        capture!(2_u8, long_pattern_slots);
        capture!(3_u8, nullable_predicate_slots);
        capture!(4_u8, tokenized_slots);
        self.ordinal = rebase_accesses(&mut order[..count])?;
        Ok(())
    }

    /// Observe the service clock while holding the shared discovery lock. A
    /// rejected observation cannot shorten any family's retention obligation.
    pub(super) fn observe_access(
        &mut self,
        clock: &dyn CursorMonotonicClock,
    ) -> Result<CursorTick, ExactTextProjectionPortError> {
        let observed = clock
            .now()
            .map_err(|_| ExactTextProjectionPortError::Unavailable)?;
        if self
            .observed_clock
            .is_some_and(|previous| observed < previous)
            || observed.checked_add(CURSOR_LIFETIME).is_none()
        {
            return Err(ExactTextProjectionPortError::Unavailable);
        }
        self.observed_clock = Some(observed);
        Ok(observed)
    }
}

type AccessReference<'a> = Option<(&'a mut ProviderAccess, u8, SlotKey)>;

fn rebase_accesses(order: &mut [AccessReference<'_>]) -> Result<u64, ExactTextProjectionPortError> {
    if order.len() > 5 * MAX_REGISTERED_EXACT_PARTITIONS || order.iter().any(Option::is_none) {
        return Err(ExactTextProjectionPortError::Integrity);
    }
    order.sort_unstable_by_key(|value| {
        value
            .as_ref()
            .map(|(access, family, key)| (access.ordinal(), *family, *key))
    });
    for (index, (access, _, _)) in order.iter_mut().flatten().enumerate() {
        access
            .rebase(u64::try_from(index + 1).map_err(|_| ExactTextProjectionPortError::Integrity)?);
    }
    u64::try_from(order.len()).map_err(|_| ExactTextProjectionPortError::Integrity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_service::CursorClockError;

    struct Clock(CursorTick);
    impl CursorMonotonicClock for Clock {
        fn now(&self) -> Result<CursorTick, CursorClockError> {
            Ok(self.0)
        }
    }

    fn tick(seconds: u64) -> CursorTick {
        CursorTick::from_process_elapsed(Duration::from_secs(seconds)).unwrap()
    }

    fn key(seed: u8) -> SlotKey {
        SlotKey::new(
            riffdb_types::QueryPlanHash::from_bytes([seed; 32]),
            PartitionKeyHash::from_bytes([0; 32]),
            riffdb_types::ApplicationRoleHash::from_bytes([0; 32]),
            None,
        )
    }

    // req: OQ-040
    #[test]
    fn exact_access_rebase_preserves_lru_canonical_ties_and_retention_deadlines() {
        let mut later = ProviderAccess::new(u64::MAX - 1, tick(10)).unwrap();
        let mut oldest = ProviderAccess::new(u64::MAX - 3, tick(20)).unwrap();
        let mut tied = ProviderAccess::new(u64::MAX - 1, tick(30)).unwrap();
        let mut order = [
            Some((&mut later, 1, key(2))),
            Some((&mut oldest, 4, key(3))),
            Some((&mut tied, 1, key(1))),
        ];
        assert_eq!(rebase_accesses(&mut order), Ok(3));
        assert_eq!(order[0].as_ref().unwrap().2, key(3));
        assert_eq!(order[1].as_ref().unwrap().2, key(1));
        assert_eq!(order[2].as_ref().unwrap().2, key(2));
        assert_eq!(later.ordinal(), 3);
        assert_eq!(oldest.ordinal(), 1);
        assert_eq!(tied.ordinal(), 2);
        for (access, expires) in [(&later, 310), (&oldest, 320), (&tied, 330)] {
            assert!(!access.retention_expired_at(tick(expires - 1)).unwrap());
            assert!(access.retention_expired_at(tick(expires)).unwrap());
        }
    }

    // req: OQ-040
    #[test]
    fn incomplete_rebase_refuses_before_changing_any_ordinal() {
        let mut access = ProviderAccess::new(u64::MAX - 1, tick(10)).unwrap();
        let mut order = [Some((&mut access, 1, key(1))), None];
        assert_eq!(
            rebase_accesses(&mut order),
            Err(ExactTextProjectionPortError::Integrity)
        );
        assert_eq!(access.ordinal(), u64::MAX - 1);
    }

    // req: OQ-040
    #[test]
    fn maximum_registry_rebase_fits_the_production_stack_and_preserves_all_leases() {
        let count = 5 * MAX_REGISTERED_EXACT_PARTITIONS;
        let mut accesses = (0..count)
            .map(|index| {
                ProviderAccess::new(u64::MAX - u64::try_from(index).unwrap(), tick(10)).unwrap()
            })
            .collect::<Vec<_>>();
        std::thread::Builder::new()
            .stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)
            .spawn(move || {
                let mut order: [AccessReference<'_>; 5 * MAX_REGISTERED_EXACT_PARTITIONS] =
                    [const { None }; 5 * MAX_REGISTERED_EXACT_PARTITIONS];
                for (index, access) in accesses.iter_mut().enumerate() {
                    order[index] = Some((access, 1, key(1)));
                }
                assert_eq!(
                    rebase_accesses(&mut order),
                    Ok(u64::try_from(count).unwrap())
                );
                for (index, access) in accesses.iter().enumerate() {
                    assert_eq!(access.ordinal(), u64::try_from(count - index).unwrap());
                    assert!(!access.retention_expired_at(tick(309)).unwrap());
                    assert!(access.retention_expired_at(tick(310)).unwrap());
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }

    // req: OQ-040
    #[test]
    fn exact_access_order_rebases_before_overflow_without_changing_clock_origin() {
        let mut registry = ExactProviderRegistries {
            ordinal: u64::MAX - 1,
            ..Default::default()
        };
        assert_eq!(
            registry.next_access(&Clock(tick(10))).unwrap(),
            (u64::MAX, tick(10))
        );
        assert_eq!(
            registry.next_access(&Clock(tick(11))).unwrap(),
            (1, tick(11))
        );
        assert_eq!(
            registry.next_access(&Clock(tick(12))).unwrap(),
            (2, tick(12))
        );
        assert_eq!(registry.observed_clock, Some(tick(12)));
        assert_eq!(
            registry.next_access(&Clock(tick(9))),
            Err(ExactTextProjectionPortError::Unavailable)
        );
        assert_eq!(registry.ordinal, 2);
    }
}
