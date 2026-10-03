//! Fresh derived generations across registration and worker churn.
// req: PRJ-001, PRJ-004, OQ-040

use riffdb_types::{ProjectionGeneration, exact_provider_budget_v1::BudgetUnavailable};
use std::sync::atomic::{AtomicU64, Ordering};

/// One runtime issuer shared by all five families, independent of rebased access ordinals. Failed or
/// cancelled builds consume their generation; no released owner can reuse it.
pub(crate) struct ProviderGenerationIssuer {
    last: AtomicU64,
}
impl ProviderGenerationIssuer {
    pub(crate) fn new(initial: ProjectionGeneration) -> Self {
        Self {
            last: AtomicU64::new(initial.get() - 1),
        }
    }

    pub(crate) fn next(
        &self,
        prior: Option<ProjectionGeneration>,
    ) -> Result<ProjectionGeneration, BudgetUnavailable> {
        let prior = prior.map_or(0, ProjectionGeneration::get);
        let previous = self
            .last
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                last.max(prior).checked_add(1)
            })
            .map_err(|_| BudgetUnavailable)?;
        previous
            .max(prior)
            .checked_add(1)
            .and_then(ProjectionGeneration::new)
            .ok_or(BudgetUnavailable)
    }

    /// Recovery still verifies exact slot, history and provider identity first.
    /// Its valid generation raises the issuer floor without changing that epoch.
    pub(crate) fn observe(&self, generation: ProjectionGeneration) {
        self.last.fetch_max(generation.get(), Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::ProjectionGeneration;
    use std::sync::{Arc, Barrier};

    #[test]
    fn generation_issuer_preserves_floor_and_advances_past_recovered_and_failed_work() {
        let issuer = ProviderGenerationIssuer::new(ProjectionGeneration::new(17).unwrap());
        assert_eq!(issuer.next(None).unwrap().get(), 17);
        assert_eq!(issuer.next(None).unwrap().get(), 18);
        issuer.observe(ProjectionGeneration::new(100).unwrap());
        assert_eq!(issuer.next(None).unwrap().get(), 101);
        assert_eq!(
            issuer
                .next(Some(ProjectionGeneration::new(200).unwrap()))
                .unwrap()
                .get(),
            201
        );
        assert_eq!(
            issuer
                .next(Some(ProjectionGeneration::first()))
                .unwrap()
                .get(),
            202
        );
    }
    #[test]
    fn concurrent_generation_issuance_has_no_duplicate_and_observation_never_lowers_floor() {
        let issuer = Arc::new(ProviderGenerationIssuer::new(ProjectionGeneration::first()));
        let first = issuer.next(None).unwrap().get();
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let issuer = issuer.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                (0..8)
                    .map(|_| {
                        issuer.observe(ProjectionGeneration::first());
                        issuer.next(None).unwrap().get()
                    })
                    .collect::<Vec<_>>()
            }));
        }
        barrier.wait();
        let mut generations = threads
            .into_iter()
            .flat_map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>();
        generations.push(first);
        generations.sort_unstable();
        assert_eq!(generations, (1..=65).collect::<Vec<_>>());
        assert_eq!(issuer.next(None).unwrap().get(), 66);
    }
    #[test]
    fn generation_exhaustion_refuses_without_wrapping_or_reusing_a_generation() {
        let issuer = ProviderGenerationIssuer::new(ProjectionGeneration::new(u64::MAX).unwrap());
        assert_eq!(issuer.next(None).unwrap().get(), u64::MAX);
        assert!(issuer.next(None).is_err());
        issuer.observe(ProjectionGeneration::first());
        assert!(issuer.next(None).is_err());
        let issuer = ProviderGenerationIssuer::new(ProjectionGeneration::first());
        assert!(
            issuer
                .next(Some(ProjectionGeneration::new(u64::MAX).unwrap()))
                .is_err()
        );
        assert_eq!(issuer.next(None).unwrap().get(), 1);
    }

    #[test]
    fn generation_observation_and_issuance_follow_an_explicit_two_thread_schedule() {
        use std::sync::mpsc;
        let issuer = Arc::new(ProviderGenerationIssuer::new(ProjectionGeneration::first()));
        let (observe_tx, observe_rx) = mpsc::sync_channel(0);
        let (observed_tx, observed_rx) = mpsc::sync_channel(0);
        let (issued_tx, issued_rx) = mpsc::sync_channel(0);
        let captured = issuer.clone();
        let observer = std::thread::spawn(move || {
            observe_rx.recv().unwrap();
            captured.observe(ProjectionGeneration::new(500).unwrap());
            observed_tx.send(()).unwrap();
            issued_rx.recv().unwrap();
            captured.observe(ProjectionGeneration::new(2).unwrap());
        });
        assert_eq!(issuer.next(None).unwrap().get(), 1);
        observe_tx.send(()).unwrap();
        observed_rx.recv().unwrap();
        assert_eq!(issuer.next(None).unwrap().get(), 501);
        issued_tx.send(()).unwrap();
        observer.join().unwrap();
        assert_eq!(issuer.next(None).unwrap().get(), 502);
    }
}
