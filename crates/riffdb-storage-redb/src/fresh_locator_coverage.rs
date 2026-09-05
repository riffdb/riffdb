use riffdb_storage_api::ApplicationSequenceAllocator;
use riffdb_types::{AdministrationSequence, CommitSequence};

/// Fixed-size identity of one process-local published or writer-private view.
///
/// `root_identity` is the address identity of the process-local immutable
/// checkpoint root. Standard journal successors share that root but differ in
/// one of the two frontiers; Immediate commits and checkpoint rebases replace
/// it. The value is never serialized or exposed outside the storage crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoverageStamp {
    root_identity: usize,
    application: Option<CommitSequence>,
    administration: Option<AdministrationSequence>,
    allocator: ApplicationSequenceAllocator,
}

impl CoverageStamp {
    pub(crate) const fn new(
        root_identity: usize,
        application: Option<CommitSequence>,
        administration: Option<AdministrationSequence>,
        allocator: ApplicationSequenceAllocator,
    ) -> Self {
        Self {
            root_identity,
            application,
            administration,
            allocator,
        }
    }

    fn allocator_matches_frontier(self) -> bool {
        let expected =
            self.application
                .map_or(ApplicationSequenceAllocator::initial(), |frontier| {
                    frontier.checked_next().map_or(
                        ApplicationSequenceAllocator::Exhausted,
                        ApplicationSequenceAllocator::next,
                    )
                });
        self.allocator == expected
    }

    pub(crate) fn allocator_is_initial(self) -> bool {
        self.allocator == ApplicationSequenceAllocator::initial()
    }

    pub(crate) fn application_is_none(self) -> bool {
        self.application.is_none()
    }
}

/// Bounded facts read from the first mutation-gated admission transaction.
#[derive(Clone, Copy)]
pub(crate) struct EmptyAuthorityProof {
    frontier_none: bool,
    commits_empty: bool,
    idempotency_empty: bool,
    pending_empty: bool,
    locators_empty: bool,
    allocator_initial: bool,
    transient_dormant: bool,
    unfenced: bool,
}

impl EmptyAuthorityProof {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        frontier_none: bool,
        commits_empty: bool,
        idempotency_empty: bool,
        pending_empty: bool,
        locators_empty: bool,
        allocator_initial: bool,
        transient_dormant: bool,
        unfenced: bool,
    ) -> Self {
        Self {
            frontier_none,
            commits_empty,
            idempotency_empty,
            pending_empty,
            locators_empty,
            allocator_initial,
            transient_dormant,
            unfenced,
        }
    }

    fn is_exact(self) -> bool {
        self.frontier_none
            && self.commits_empty
            && self.idempotency_empty
            && self.pending_empty
            && self.locators_empty
            && self.allocator_initial
            && self.transient_dormant
            && self.unfenced
    }
}

struct PublicPrefixProof {
    stamp: CoverageStamp,
}

struct PrivateChainWitness {
    stamp: CoverageStamp,
}

pub(crate) struct CommandPublicationWitness {
    predecessor: CoverageStamp,
    successor: CoverageStamp,
}

pub(crate) struct PreservePublicationWitness {
    predecessor: CoverageStamp,
    successor: CoverageStamp,
}

struct PairToken {
    epoch: u64,
    public: PublicPrefixProof,
    private: PrivateChainWitness,
}

pub(crate) struct DirectCommandWitness {
    pair: PairToken,
    expected: CoverageStamp,
}
pub(crate) struct PreservingImmediateWitness(PairToken);
pub(crate) struct CoverageRebaseWitness(PairToken);

enum CoverageState {
    Uninitialized,
    Disabled,
    Armed {
        epoch: u64,
        public: PublicPrefixProof,
        private: PrivateChainWitness,
    },
    Rebinding {
        epoch: u64,
        old_public_stamp: CoverageStamp,
    },
}

/// Process-local affine proof that every command in one fresh history prefix
/// owns its exact durable idempotency locator.
pub(crate) struct FreshLocatorCoverage {
    state: CoverageState,
}

impl FreshLocatorCoverage {
    pub(crate) const fn new() -> Self {
        Self {
            state: CoverageState::Uninitialized,
        }
    }

    pub(crate) fn try_arm(&mut self, proof: EmptyAuthorityProof, stamp: CoverageStamp) -> bool {
        if !matches!(self.state, CoverageState::Uninitialized) {
            return false;
        }
        if !proof.is_exact() || !stamp.allocator_matches_frontier() || stamp.application.is_some() {
            self.disable();
            return false;
        }
        self.state = CoverageState::Armed {
            epoch: 1,
            public: PublicPrefixProof { stamp },
            private: PrivateChainWitness { stamp },
        };
        true
    }

    #[cfg(test)]
    pub(crate) const fn is_disabled(&self) -> bool {
        matches!(self.state, CoverageState::Disabled)
    }

    pub(crate) fn disable(&mut self) {
        self.state = CoverageState::Disabled;
    }

    pub(crate) fn proves_public_absence(&self, captured: CoverageStamp) -> bool {
        matches!(
            &self.state,
            CoverageState::Armed { public, .. } if public.stamp == captured
        ) || matches!(
            &self.state,
            CoverageState::Rebinding { old_public_stamp, .. } if *old_public_stamp == captured
        )
    }

    pub(crate) fn allows_private_miss(&mut self, captured: CoverageStamp) -> bool {
        let matches = matches!(
            &self.state,
            CoverageState::Armed { private, .. } if private.stamp == captured
        );
        if !matches {
            self.disable();
        }
        matches
    }

    pub(crate) fn private_stamp(&self) -> Option<CoverageStamp> {
        match &self.state {
            CoverageState::Armed { private, .. } => Some(private.stamp),
            CoverageState::Uninitialized
            | CoverageState::Disabled
            | CoverageState::Rebinding { .. } => None,
        }
    }

    pub(crate) fn seal_command(
        &mut self,
        predecessor: CoverageStamp,
        successor: CoverageStamp,
        count: u16,
    ) -> Option<CommandPublicationWitness> {
        if count == 0 || !successor.allocator_matches_frontier() {
            self.disable();
            return None;
        }
        let expected_first = predecessor
            .application
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next);
        let expected_last = expected_first.and_then(|first| {
            first
                .get()
                .checked_add(u64::from(count) - 1)
                .and_then(CommitSequence::new)
        });
        if expected_last != successor.application
            || predecessor.root_identity != successor.root_identity
        {
            self.disable();
            return None;
        }
        let CoverageState::Armed { private, .. } = &mut self.state else {
            self.disable();
            return None;
        };
        if private.stamp != predecessor {
            self.disable();
            return None;
        }
        private.stamp = successor;
        Some(CommandPublicationWitness {
            predecessor,
            successor,
        })
    }

    pub(crate) fn seal_preserve(
        &mut self,
        predecessor: CoverageStamp,
        successor: CoverageStamp,
    ) -> Option<PreservePublicationWitness> {
        if predecessor.root_identity != successor.root_identity
            || predecessor.application != successor.application
            || predecessor.allocator != successor.allocator
        {
            self.disable();
            return None;
        }
        let CoverageState::Armed { private, .. } = &mut self.state else {
            self.disable();
            return None;
        };
        if private.stamp != predecessor {
            self.disable();
            return None;
        }
        private.stamp = successor;
        Some(PreservePublicationWitness {
            predecessor,
            successor,
        })
    }

    pub(crate) fn publish_command(&mut self, witness: CommandPublicationWitness) -> bool {
        self.publish(witness.predecessor, witness.successor)
    }

    pub(crate) fn publish_preserve(&mut self, witness: PreservePublicationWitness) -> bool {
        self.publish(witness.predecessor, witness.successor)
    }

    fn publish(&mut self, predecessor: CoverageStamp, successor: CoverageStamp) -> bool {
        let CoverageState::Armed { public, .. } = &mut self.state else {
            self.disable();
            return false;
        };
        if public.stamp != predecessor {
            self.disable();
            return false;
        }
        public.stamp = successor;
        true
    }

    pub(crate) fn begin_direct(
        &mut self,
        predecessor: CoverageStamp,
        expected: CoverageStamp,
    ) -> Option<DirectCommandWitness> {
        let contiguous = match (predecessor.application, expected.application) {
            (None, Some(last)) => last.get() >= 1,
            (Some(first), Some(last)) => last > first,
            _ => false,
        };
        if predecessor.root_identity != expected.root_identity
            || !expected.allocator_matches_frontier()
            || !contiguous
        {
            self.disable();
            return None;
        }
        self.begin_pair(predecessor)
            .map(|pair| DirectCommandWitness { pair, expected })
    }

    pub(crate) fn finish_direct(
        &mut self,
        witness: DirectCommandWitness,
        successor: CoverageStamp,
    ) -> bool {
        let predecessor = witness.pair.public.stamp;
        if successor.root_identity == predecessor.root_identity
            || !successor.allocator_matches_frontier()
            || successor.application != witness.expected.application
            || successor.administration != witness.expected.administration
            || successor.allocator != witness.expected.allocator
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.pair, successor)
    }

    pub(crate) fn begin_preserving_immediate(
        &mut self,
        predecessor: CoverageStamp,
    ) -> Option<PreservingImmediateWitness> {
        self.begin_pair(predecessor).map(PreservingImmediateWitness)
    }

    pub(crate) fn finish_preserving_immediate(
        &mut self,
        witness: PreservingImmediateWitness,
        successor: CoverageStamp,
    ) -> bool {
        let predecessor = witness.0.public.stamp;
        if predecessor.root_identity == successor.root_identity
            || predecessor.application != successor.application
            || predecessor.allocator != successor.allocator
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.0, successor)
    }

    pub(crate) fn begin_rebase(
        &mut self,
        predecessor: CoverageStamp,
    ) -> Option<CoverageRebaseWitness> {
        self.begin_pair(predecessor).map(CoverageRebaseWitness)
    }

    pub(crate) fn finish_rebase(
        &mut self,
        witness: CoverageRebaseWitness,
        successor: CoverageStamp,
    ) -> bool {
        let predecessor = witness.0.public.stamp;
        if predecessor.root_identity == successor.root_identity
            || predecessor.application != successor.application
            || predecessor.administration != successor.administration
            || predecessor.allocator != successor.allocator
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.0, successor)
    }

    pub(crate) fn abort_rebase(&mut self, witness: CoverageRebaseWitness) -> bool {
        self.restore_pair(witness.0)
    }

    fn begin_pair(&mut self, predecessor: CoverageStamp) -> Option<PairToken> {
        let prior = std::mem::replace(&mut self.state, CoverageState::Disabled);
        if matches!(prior, CoverageState::Uninitialized) {
            self.state = CoverageState::Uninitialized;
            return None;
        }
        let CoverageState::Armed {
            epoch,
            public,
            private,
        } = prior
        else {
            return None;
        };
        if public.stamp != predecessor || private.stamp != predecessor || epoch == u64::MAX {
            return None;
        }
        let next = epoch + 1;
        self.state = CoverageState::Rebinding {
            epoch: next,
            old_public_stamp: public.stamp,
        };
        Some(PairToken {
            epoch: next,
            public,
            private,
        })
    }

    fn finish_pair(&mut self, mut token: PairToken, successor: CoverageStamp) -> bool {
        if !matches!(self.state, CoverageState::Rebinding { epoch, .. } if epoch == token.epoch) {
            self.disable();
            return false;
        }
        token.public.stamp = successor;
        token.private.stamp = successor;
        self.state = CoverageState::Armed {
            epoch: token.epoch,
            public: token.public,
            private: token.private,
        };
        true
    }

    fn restore_pair(&mut self, token: PairToken) -> bool {
        if !matches!(self.state, CoverageState::Rebinding { epoch, .. } if epoch == token.epoch) {
            self.disable();
            return false;
        }
        self.state = CoverageState::Armed {
            epoch: token.epoch,
            public: token.public,
            private: token.private,
        };
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::ApplicationSequenceAllocator;
    use riffdb_types::{AdministrationSequence, CommitSequence};

    fn stamp(root: usize, application: Option<u64>, administration: Option<u64>) -> CoverageStamp {
        CoverageStamp::new(
            root,
            application.and_then(CommitSequence::new),
            administration.and_then(AdministrationSequence::new),
            application.map_or(ApplicationSequenceAllocator::initial(), |value| {
                CommitSequence::new(value)
                    .and_then(CommitSequence::checked_next)
                    .map_or(
                        ApplicationSequenceAllocator::Exhausted,
                        ApplicationSequenceAllocator::next,
                    )
            }),
        )
    }

    fn empty_authority() -> EmptyAuthorityProof {
        EmptyAuthorityProof::new(true, true, true, true, true, true, true, true)
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn fresh_locator_coverage_arms_at_first_command_write_only_from_exact_empty_authority() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(7, None, None)));
        assert!(coverage.proves_public_absence(stamp(7, None, None)));
        assert!(coverage.allows_private_miss(stamp(7, None, None)));

        for invalid in [
            EmptyAuthorityProof::new(false, true, true, true, true, true, true, true),
            EmptyAuthorityProof::new(true, false, true, true, true, true, true, true),
            EmptyAuthorityProof::new(true, true, false, true, true, true, true, true),
            EmptyAuthorityProof::new(true, true, true, false, true, true, true, true),
            EmptyAuthorityProof::new(true, true, true, true, false, true, true, true),
            EmptyAuthorityProof::new(true, true, true, true, true, false, true, true),
            EmptyAuthorityProof::new(true, true, true, true, true, true, false, true),
            EmptyAuthorityProof::new(true, true, true, true, true, true, true, false),
        ] {
            let mut refused = FreshLocatorCoverage::new();
            assert!(!refused.try_arm(invalid, stamp(9, None, None)));
            assert!(refused.is_disabled());
            assert!(!refused.try_arm(empty_authority(), stamp(9, None, None)));
        }
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn fresh_locator_private_chain_and_fifo_publications_are_linear() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(1, None, None)));
        let publish_a = coverage
            .seal_command(stamp(1, None, None), stamp(1, Some(1), Some(2)), 1)
            .expect("seal A");
        let preserve = coverage
            .seal_preserve(stamp(1, Some(1), Some(2)), stamp(1, Some(1), Some(3)))
            .expect("seal audit");
        let publish_b = coverage
            .seal_command(stamp(1, Some(1), Some(3)), stamp(1, Some(2), Some(5)), 1)
            .expect("seal B");

        assert!(coverage.publish_command(publish_a));
        assert!(coverage.publish_preserve(preserve));
        assert!(coverage.publish_command(publish_b));
        assert!(coverage.proves_public_absence(stamp(1, Some(2), Some(5))));

        let mut wrong_order = FreshLocatorCoverage::new();
        assert!(wrong_order.try_arm(empty_authority(), stamp(1, None, None)));
        let publish_a = wrong_order
            .seal_command(stamp(1, None, None), stamp(1, Some(1), Some(2)), 1)
            .expect("seal A");
        let publish_b = wrong_order
            .seal_command(stamp(1, Some(1), Some(2)), stamp(1, Some(2), Some(4)), 1)
            .expect("seal B");
        assert!(!wrong_order.publish_command(publish_b));
        assert!(wrong_order.is_disabled());
        drop(publish_a);
    }

    // req: OUT-001, OUT-002, TXN-042
    #[test]
    fn fresh_locator_write_miss_retains_current_semantics_and_gates_only_coverage() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(2, None, None)));
        assert!(coverage.allows_private_miss(stamp(2, None, None)));
        assert!(!coverage.allows_private_miss(stamp(3, None, None)));
        assert!(coverage.is_disabled());
    }

    // req: OUT-001, OUT-002, TXN-042
    #[test]
    fn fresh_locator_miss_preserves_prior_identity_and_rejects_malformed_locators() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(3, None, None)));
        assert!(!coverage.proves_public_absence(stamp(4, None, None)));
        assert!(coverage.proves_public_absence(stamp(3, None, None)));
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart() {
        let mut direct = FreshLocatorCoverage::new();
        assert!(direct.try_arm(empty_authority(), stamp(4, None, None)));
        let token = direct
            .begin_direct(stamp(4, None, None), stamp(4, Some(1), Some(2)))
            .expect("direct token");
        assert!(direct.finish_direct(token, stamp(5, Some(1), Some(2))));
        assert!(direct.proves_public_absence(stamp(5, Some(1), Some(2))));
        assert!(direct.allows_private_miss(stamp(5, Some(1), Some(2))));

        let mut wrong = FreshLocatorCoverage::new();
        assert!(wrong.try_arm(empty_authority(), stamp(4, None, None)));
        let token = wrong
            .begin_direct(stamp(4, None, None), stamp(4, Some(1), Some(2)))
            .expect("token");
        assert!(!wrong.finish_direct(token, stamp(5, Some(2), Some(2))));
        assert!(wrong.is_disabled());

        let mut rebase = FreshLocatorCoverage::new();
        assert!(rebase.try_arm(empty_authority(), stamp(6, None, None)));
        let token = rebase.begin_rebase(stamp(6, None, None)).expect("rebase");
        assert!(rebase.abort_rebase(token));
        let token = rebase.begin_rebase(stamp(6, None, None)).expect("rebase");
        assert!(rebase.finish_rebase(token, stamp(7, None, None)));
        assert!(rebase.proves_public_absence(stamp(7, None, None)));
        assert!(rebase.allows_private_miss(stamp(7, None, None)));

        let reopened = FreshLocatorCoverage::new();
        assert!(!reopened.proves_public_absence(stamp(7, None, None)));
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn cold_fresh_database_publications_complete_without_history_scans() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(10, None, None)));
        for sequence in 1..=288_u64 {
            let predecessor = (sequence > 1).then(|| sequence - 1);
            let witness = coverage
                .seal_command(
                    stamp(10, predecessor, predecessor.map(|value| value * 2)),
                    stamp(10, Some(sequence), Some(sequence * 2)),
                    1,
                )
                .expect("bounded contiguous seal");
            assert!(coverage.publish_command(witness));
        }
        assert!(coverage.proves_public_absence(stamp(10, Some(288), Some(576))));
    }
}
