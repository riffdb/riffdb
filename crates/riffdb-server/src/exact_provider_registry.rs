//! Bounded discovery and access order shared by all exact-provider families.

use super::*;

pub(super) struct RegistryEntry<T> {
    pub(super) slot: Arc<T>,
    pub(super) access: ProviderAccess,
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
