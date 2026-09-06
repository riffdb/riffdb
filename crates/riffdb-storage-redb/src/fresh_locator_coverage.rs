use riffdb_storage_api::ApplicationSequenceAllocator;
use riffdb_types::{AdministrationSequence, CommitSequence};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::sync::Barrier;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub(crate) fn application_authority_digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub(crate) fn preserving_permit_digest<'a>(
    mutations: impl IntoIterator<Item = (&'a str, u8, &'a [u8])>,
) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"riffdb-fresh-locator-preserving-permit-v1");
    for (table, kind, key) in mutations {
        digest.update((table.len() as u64).to_le_bytes());
        digest.update(table.as_bytes());
        digest.update([kind]);
        digest.update((key.len() as u64).to_le_bytes());
        digest.update(key);
    }
    digest.finalize().into()
}

/// Fixed-size identity of one process-local published or writer-private view.
///
/// `root_identity` is the stable even generation of the process-local immutable
/// checkpoint root. Standard journal successors share that root but differ in
/// one of the two frontiers; Immediate commits and checkpoint rebases replace
/// it. The value is never serialized or exposed outside the storage crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CoverageStamp {
    root_identity: u64,
    application: Option<CommitSequence>,
    administration: Option<AdministrationSequence>,
    allocator: ApplicationSequenceAllocator,
    application_authority_digest: [u8; 32],
}

impl CoverageStamp {
    pub(crate) const fn new(
        root_identity: u64,
        application: Option<CommitSequence>,
        administration: Option<AdministrationSequence>,
        allocator: ApplicationSequenceAllocator,
        application_authority_digest: [u8; 32],
    ) -> Self {
        Self {
            root_identity,
            application,
            administration,
            allocator,
            application_authority_digest,
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

impl PrivateChainWitness {
    fn replace(self, successor: CoverageStamp) -> Self {
        Self { stamp: successor }
    }
}

pub(crate) struct CommandPublicationWitness {
    predecessor: CoverageStamp,
    successor: CoverageStamp,
    loss: WitnessLossGuard,
}

pub(crate) struct PreservePublicationWitness {
    predecessor: CoverageStamp,
    successor: CoverageStamp,
    loss: WitnessLossGuard,
}

struct WitnessLossGuard {
    lost: Arc<AtomicBool>,
    armed: bool,
}

impl WitnessLossGuard {
    fn new(lost: &Arc<AtomicBool>) -> Self {
        Self {
            lost: Arc::clone(lost),
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for WitnessLossGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lost.store(true, Ordering::Release);
        }
    }
}

struct PairToken {
    epoch: u64,
    public: PublicPrefixProof,
    private: PrivateChainWitness,
    loss: WitnessLossGuard,
}

pub(crate) struct DirectCommandWitness {
    pair: PairToken,
    expected: CoverageStamp,
}
pub(crate) struct DirectCommandSpanEvidence {
    first: CommitSequence,
    last: CommitSequence,
    count: u16,
    first_administration: AdministrationSequence,
    last_administration: AdministrationSequence,
}
pub(crate) struct PreservingImmediatePermit {
    mutation_count: u16,
    exact_set_digest: [u8; 32],
}
pub(crate) struct PreservingImmediateWitness {
    pair: PairToken,
    permit: PreservingImmediatePermit,
}
pub(crate) struct CoverageRebaseWitness(PairToken);

enum CoverageState {
    Uninitialized,
    Disabled,
    Armed {
        epoch: u64,
        public: PublicPrefixProof,
        private: Option<PrivateChainWitness>,
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
    lost_witness: Arc<AtomicBool>,
    #[cfg(test)]
    loss_schedule: Option<Arc<WitnessLossScheduleInner>>,
}

#[cfg(test)]
struct WitnessLossScheduleInner {
    armed: AtomicBool,
    observed: Barrier,
    release: Barrier,
}

#[cfg(test)]
struct WitnessLossSchedule {
    inner: Arc<WitnessLossScheduleInner>,
}

#[cfg(test)]
impl WitnessLossSchedule {
    fn wait_until_operation_acquires_live(&self) {
        self.inner.observed.wait();
    }

    fn release_operation(&self) {
        self.inner.release.wait();
    }
}

impl FreshLocatorCoverage {
    pub(crate) fn new() -> Self {
        Self {
            state: CoverageState::Uninitialized,
            lost_witness: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            loss_schedule: None,
        }
    }

    fn witness_is_live(&self) -> bool {
        let live = !self.lost_witness.load(Ordering::Acquire);
        #[cfg(test)]
        if live
            && self.loss_schedule.as_ref().is_some_and(|schedule| {
                schedule
                    .armed
                    .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            })
        {
            let schedule = self.loss_schedule.as_ref().expect("checked schedule");
            schedule.observed.wait();
            schedule.release.wait();
        }
        live
    }

    fn reconcile_witness_loss(&mut self) -> bool {
        if !self.witness_is_live() {
            self.disable();
            false
        } else {
            true
        }
    }

    pub(crate) fn try_arm(&mut self, proof: EmptyAuthorityProof, stamp: CoverageStamp) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
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
            private: Some(PrivateChainWitness { stamp }),
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

    pub(crate) fn is_armed(&self) -> bool {
        self.witness_is_live() && matches!(self.state, CoverageState::Armed { .. })
    }

    pub(crate) fn proves_public_absence(&self, captured: CoverageStamp) -> bool {
        if !self.witness_is_live() {
            return false;
        }
        matches!(
            &self.state,
            CoverageState::Armed { public, .. } if public.stamp == captured
        ) || matches!(
            &self.state,
            CoverageState::Rebinding { old_public_stamp, .. } if *old_public_stamp == captured
        )
    }

    pub(crate) fn allows_private_miss(&mut self, captured: CoverageStamp) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
        let matches = matches!(
            &self.state,
            CoverageState::Armed { private: Some(private), .. } if private.stamp == captured
        );
        if !matches {
            self.disable();
        }
        matches
    }

    pub(crate) fn seal_command(
        &mut self,
        successor: CoverageStamp,
        count: u16,
    ) -> Option<CommandPublicationWitness> {
        if !self.reconcile_witness_loss() {
            return None;
        }
        if count == 0 || !successor.allocator_matches_frontier() {
            self.disable();
            return None;
        }
        let CoverageState::Armed { private, .. } = &mut self.state else {
            self.disable();
            return None;
        };
        let Some(predecessor_witness) = private.take() else {
            self.disable();
            return None;
        };
        let predecessor = predecessor_witness.stamp;
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
        *private = Some(predecessor_witness.replace(successor));
        Some(CommandPublicationWitness {
            predecessor,
            successor,
            loss: WitnessLossGuard::new(&self.lost_witness),
        })
    }

    pub(crate) fn seal_preserve(
        &mut self,
        successor: CoverageStamp,
    ) -> Option<PreservePublicationWitness> {
        if !self.reconcile_witness_loss() {
            return None;
        }
        let CoverageState::Armed { private, .. } = &mut self.state else {
            self.disable();
            return None;
        };
        let Some(predecessor_witness) = private.take() else {
            self.disable();
            return None;
        };
        let predecessor = predecessor_witness.stamp;
        if predecessor.root_identity != successor.root_identity
            || predecessor.application != successor.application
            || predecessor.allocator != successor.allocator
            || predecessor.application_authority_digest != successor.application_authority_digest
        {
            self.disable();
            return None;
        }
        *private = Some(predecessor_witness.replace(successor));
        Some(PreservePublicationWitness {
            predecessor,
            successor,
            loss: WitnessLossGuard::new(&self.lost_witness),
        })
    }

    pub(crate) fn publish_command(&mut self, witness: CommandPublicationWitness) -> bool {
        let CommandPublicationWitness {
            predecessor,
            successor,
            loss,
        } = witness;
        let published = self.publish(predecessor, successor);
        loss.disarm();
        published
    }

    pub(crate) fn publish_preserve(&mut self, witness: PreservePublicationWitness) -> bool {
        let PreservePublicationWitness {
            predecessor,
            successor,
            loss,
        } = witness;
        let published = self.publish(predecessor, successor);
        loss.disarm();
        published
    }

    fn publish(&mut self, predecessor: CoverageStamp, successor: CoverageStamp) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
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

    pub(crate) fn direct_command_span_evidence(
        first: CommitSequence,
        last: CommitSequence,
        count: u16,
        first_administration: AdministrationSequence,
        last_administration: AdministrationSequence,
    ) -> Option<DirectCommandSpanEvidence> {
        if count == 0 {
            return None;
        }
        let command_last = first
            .get()
            .checked_add(u64::from(count) - 1)
            .and_then(CommitSequence::new)?;
        let audit_count = u64::from(count).checked_mul(2)?;
        let audit_last = first_administration
            .get()
            .checked_add(audit_count - 1)
            .and_then(AdministrationSequence::new)?;
        if command_last != last || audit_last != last_administration {
            return None;
        }
        Some(DirectCommandSpanEvidence {
            first,
            last,
            count,
            first_administration,
            last_administration,
        })
    }

    pub(crate) fn begin_direct(
        &mut self,
        expected: CoverageStamp,
        span: DirectCommandSpanEvidence,
    ) -> Option<DirectCommandWitness> {
        if !self.reconcile_witness_loss() {
            return None;
        }
        let pair = self.begin_pair()?;
        let predecessor = pair.public.stamp;
        let expected_first = predecessor
            .application
            .map_or(Some(CommitSequence::first()), CommitSequence::checked_next);
        let expected_first_administration = predecessor.administration.map_or(
            Some(AdministrationSequence::first()),
            AdministrationSequence::checked_next,
        );
        if predecessor.root_identity != expected.root_identity
            || !expected.allocator_matches_frontier()
            || expected_first != Some(span.first)
            || expected.application != Some(span.last)
            || expected_first_administration != Some(span.first_administration)
            || expected.administration != Some(span.last_administration)
            || span.count == 0
        {
            self.disable();
            return None;
        }
        Some(DirectCommandWitness { pair, expected })
    }

    pub(crate) fn finish_direct(
        &mut self,
        witness: DirectCommandWitness,
        successor: CoverageStamp,
    ) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
        let predecessor = witness.pair.public.stamp;
        if successor.root_identity == predecessor.root_identity
            || !successor.allocator_matches_frontier()
            || successor.application != witness.expected.application
            || successor.administration != witness.expected.administration
            || successor.allocator != witness.expected.allocator
            || successor.application_authority_digest
                != witness.expected.application_authority_digest
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.pair, successor)
    }

    pub(crate) fn begin_preserving_immediate(
        &mut self,
        permit: PreservingImmediatePermit,
    ) -> Option<PreservingImmediateWitness> {
        if !self.reconcile_witness_loss() {
            return None;
        }
        if permit.mutation_count == 0 {
            self.disable();
            return None;
        }
        self.begin_pair()
            .map(|pair| PreservingImmediateWitness { pair, permit })
    }

    pub(crate) fn finish_preserving_immediate(
        &mut self,
        witness: PreservingImmediateWitness,
        successor: CoverageStamp,
    ) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
        let predecessor = witness.pair.public.stamp;
        if predecessor.root_identity == successor.root_identity
            || predecessor.application != successor.application
            || predecessor.allocator != successor.allocator
            || predecessor.application_authority_digest != successor.application_authority_digest
            || witness.permit.mutation_count == 0
            || witness.permit.exact_set_digest == [0; 32]
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.pair, successor)
    }

    pub(crate) const fn preserving_permit(
        mutation_count: u16,
        exact_set_digest: [u8; 32],
    ) -> PreservingImmediatePermit {
        PreservingImmediatePermit {
            mutation_count,
            exact_set_digest,
        }
    }

    pub(crate) fn begin_rebase(
        &mut self,
        predecessor: CoverageStamp,
    ) -> Option<CoverageRebaseWitness> {
        if !self.reconcile_witness_loss() {
            return None;
        }
        let pair = self.begin_pair()?;
        if pair.public.stamp != predecessor {
            self.disable();
            return None;
        }
        Some(CoverageRebaseWitness(pair))
    }

    pub(crate) fn finish_rebase(
        &mut self,
        witness: CoverageRebaseWitness,
        successor: CoverageStamp,
    ) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
        let predecessor = witness.0.public.stamp;
        if predecessor.root_identity == successor.root_identity
            || predecessor.application != successor.application
            || predecessor.administration != successor.administration
            || predecessor.allocator != successor.allocator
            || predecessor.application_authority_digest != successor.application_authority_digest
        {
            self.disable();
            return false;
        }
        self.finish_pair(witness.0, successor)
    }

    pub(crate) fn abort_rebase(&mut self, witness: CoverageRebaseWitness) -> bool {
        if !self.reconcile_witness_loss() {
            return false;
        }
        self.restore_pair(witness.0)
    }

    fn begin_pair(&mut self) -> Option<PairToken> {
        let prior = std::mem::replace(&mut self.state, CoverageState::Disabled);
        if matches!(prior, CoverageState::Uninitialized) {
            self.state = CoverageState::Uninitialized;
            return None;
        }
        let CoverageState::Armed {
            epoch,
            public,
            mut private,
        } = prior
        else {
            return None;
        };
        let private = private.take()?;
        if public.stamp != private.stamp || epoch == u64::MAX {
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
            loss: WitnessLossGuard::new(&self.lost_witness),
        })
    }

    fn finish_pair(&mut self, mut token: PairToken, successor: CoverageStamp) -> bool {
        if !matches!(self.state, CoverageState::Rebinding { epoch, .. } if epoch == token.epoch) {
            self.disable();
            return false;
        }
        token.public.stamp = successor;
        self.state = CoverageState::Armed {
            epoch: token.epoch,
            public: token.public,
            private: Some(token.private.replace(successor)),
        };
        token.loss.disarm();
        true
    }

    fn restore_pair(&mut self, token: PairToken) -> bool {
        if !matches!(self.state, CoverageState::Rebinding { epoch, .. } if epoch == token.epoch) {
            self.disable();
            return false;
        }
        let PairToken {
            epoch,
            public,
            private,
            loss,
        } = token;
        self.state = CoverageState::Armed {
            epoch,
            public,
            private: Some(private),
        };
        loss.disarm();
        true
    }

    #[cfg(test)]
    fn schedule_live_witness_observation(&mut self) -> WitnessLossSchedule {
        let inner = Arc::new(WitnessLossScheduleInner {
            armed: AtomicBool::new(true),
            observed: Barrier::new(2),
            release: Barrier::new(2),
        });
        self.loss_schedule = Some(Arc::clone(&inner));
        WitnessLossSchedule { inner }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::ApplicationSequenceAllocator;
    use riffdb_types::{AdministrationSequence, CommitSequence};

    fn direct_span(
        first: u64,
        last: u64,
        count: u16,
        first_administration: u64,
        last_administration: u64,
    ) -> DirectCommandSpanEvidence {
        FreshLocatorCoverage::direct_command_span_evidence(
            CommitSequence::new(first).expect("first command"),
            CommitSequence::new(last).expect("last command"),
            count,
            AdministrationSequence::new(first_administration).expect("first audit"),
            AdministrationSequence::new(last_administration).expect("last audit"),
        )
        .expect("exact direct span")
    }

    fn stamp(root: u64, application: Option<u64>, administration: Option<u64>) -> CoverageStamp {
        stamp_with(
            root,
            application,
            administration,
            application.map_or(ApplicationSequenceAllocator::initial(), |value| {
                CommitSequence::new(value)
                    .and_then(CommitSequence::checked_next)
                    .map_or(
                        ApplicationSequenceAllocator::Exhausted,
                        ApplicationSequenceAllocator::next,
                    )
            }),
            [0xa5; 32],
        )
    }

    fn stamp_with(
        root: u64,
        application: Option<u64>,
        administration: Option<u64>,
        allocator: ApplicationSequenceAllocator,
        application_authority_digest: [u8; 32],
    ) -> CoverageStamp {
        CoverageStamp::new(
            root,
            application.and_then(CommitSequence::new),
            administration.and_then(AdministrationSequence::new),
            allocator,
            application_authority_digest,
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

        let mut exhausted = FreshLocatorCoverage::new();
        assert!(!exhausted.try_arm(
            empty_authority(),
            stamp_with(
                9,
                Some(u64::MAX),
                None,
                ApplicationSequenceAllocator::Exhausted,
                [0xa5; 32],
            ),
        ));
        assert!(exhausted.is_disabled());
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn fresh_locator_private_chain_and_fifo_publications_are_linear() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(1, None, None)));
        let publish_a = coverage
            .seal_command(stamp(1, Some(1), Some(2)), 1)
            .expect("seal A");
        let preserve = coverage
            .seal_preserve(stamp(1, Some(1), Some(3)))
            .expect("seal audit");
        let publish_b = coverage
            .seal_command(stamp(1, Some(2), Some(5)), 1)
            .expect("seal B");

        assert!(coverage.publish_command(publish_a));
        assert!(coverage.publish_preserve(preserve));
        assert!(coverage.publish_command(publish_b));
        assert!(coverage.proves_public_absence(stamp(1, Some(2), Some(5))));

        let mut wrong_order = FreshLocatorCoverage::new();
        assert!(wrong_order.try_arm(empty_authority(), stamp(1, None, None)));
        let publish_a = wrong_order
            .seal_command(stamp(1, Some(1), Some(2)), 1)
            .expect("seal A");
        let publish_b = wrong_order
            .seal_command(stamp(1, Some(2), Some(4)), 1)
            .expect("seal B");
        assert!(!wrong_order.publish_command(publish_b));
        assert!(wrong_order.is_disabled());
        let _lost_publication_witness = publish_a;

        let mut wrong_preserve_order = FreshLocatorCoverage::new();
        assert!(wrong_preserve_order.try_arm(empty_authority(), stamp(1, None, None)));
        let publish_a = wrong_preserve_order
            .seal_command(stamp(1, Some(1), Some(2)), 1)
            .expect("seal A");
        let publish_audit = wrong_preserve_order
            .seal_preserve(stamp(1, Some(1), Some(3)))
            .expect("seal audit after A");
        assert!(!wrong_preserve_order.publish_preserve(publish_audit));
        assert!(wrong_preserve_order.is_disabled());
        let _lost_command_witness = publish_a;

        for invalid_successor in [
            stamp(1, None, None),
            stamp(1, Some(2), Some(2)),
            stamp_with(
                2,
                Some(1),
                Some(2),
                ApplicationSequenceAllocator::next(
                    CommitSequence::new(2).expect("allocator sequence"),
                ),
                [0xa5; 32],
            ),
        ] {
            let mut invalid = FreshLocatorCoverage::new();
            assert!(invalid.try_arm(empty_authority(), stamp(1, None, None)));
            assert!(invalid.seal_command(invalid_successor, 1).is_none());
            assert!(invalid.is_disabled());
        }
        let mut zero_count = FreshLocatorCoverage::new();
        assert!(zero_count.try_arm(empty_authority(), stamp(1, None, None)));
        assert!(
            zero_count
                .seal_command(stamp(1, Some(1), Some(2)), 0)
                .is_none()
        );
        assert!(zero_count.is_disabled());

        let mut maximum_frame = FreshLocatorCoverage::new();
        assert!(maximum_frame.try_arm(empty_authority(), stamp(1, None, None)));
        let maximum_count = u16::try_from(riffdb_storage_api::MAX_STAGED_COMMANDS)
            .expect("the command-frame bound fits its durable count");
        let maximum = maximum_frame
            .seal_command(
                stamp(
                    1,
                    Some(u64::from(maximum_count)),
                    Some(u64::from(maximum_count) * 2),
                ),
                maximum_count,
            )
            .expect("the exact 256-command frame is admitted");
        assert!(maximum_frame.publish_command(maximum));

        let mut oversized_frame = FreshLocatorCoverage::new();
        assert!(oversized_frame.try_arm(empty_authority(), stamp(1, None, None)));
        let oversized_count = maximum_count.checked_add(1).expect("one over bound");
        assert!(
            oversized_frame
                .seal_command(
                    stamp(
                        1,
                        Some(u64::from(oversized_count)),
                        Some(u64::from(oversized_count) * 2),
                    ),
                    oversized_count,
                )
                .is_none()
        );
        assert!(oversized_frame.is_disabled());

        let maximum_predecessor = stamp_with(
            1,
            Some(u64::MAX - 1),
            None,
            ApplicationSequenceAllocator::next(
                CommitSequence::new(u64::MAX).expect("maximum allocator successor"),
            ),
            [0xa5; 32],
        );
        let mut exhaustion = FreshLocatorCoverage {
            state: CoverageState::Armed {
                epoch: 1,
                public: PublicPrefixProof {
                    stamp: maximum_predecessor,
                },
                private: Some(PrivateChainWitness {
                    stamp: maximum_predecessor,
                }),
            },
            lost_witness: Arc::new(AtomicBool::new(false)),
            loss_schedule: None,
        };
        let exhausted_successor = stamp_with(
            1,
            Some(u64::MAX),
            None,
            ApplicationSequenceAllocator::Exhausted,
            [0xa5; 32],
        );
        let maximum = exhaustion
            .seal_command(exhausted_successor, 1)
            .expect("the exact final sequence is admitted");
        assert!(exhaustion.publish_command(maximum));
        assert!(exhaustion.seal_command(exhausted_successor, 1).is_none());
        assert!(exhaustion.is_disabled());
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn fresh_locator_coverage_classifies_every_publication_rebase_failure_and_restart() {
        for invalid_span in [(1, 1, 0, 1, 2), (1, 2, 1, 1, 2), (1, 1, 1, 1, 3)] {
            assert!(
                FreshLocatorCoverage::direct_command_span_evidence(
                    CommitSequence::new(invalid_span.0).expect("first command"),
                    CommitSequence::new(invalid_span.1).expect("last command"),
                    invalid_span.2,
                    AdministrationSequence::new(invalid_span.3).expect("first audit"),
                    AdministrationSequence::new(invalid_span.4).expect("last audit"),
                )
                .is_none()
            );
        }
        let mut direct = FreshLocatorCoverage::new();
        assert!(direct.try_arm(empty_authority(), stamp(4, None, None)));
        let token = direct
            .begin_direct(stamp(4, Some(1), Some(2)), direct_span(1, 1, 1, 1, 2))
            .expect("direct token");
        assert!(direct.finish_direct(token, stamp(5, Some(1), Some(2))));
        assert!(direct.proves_public_absence(stamp(5, Some(1), Some(2))));
        assert!(direct.allows_private_miss(stamp(5, Some(1), Some(2))));

        let mut wrong = FreshLocatorCoverage::new();
        assert!(wrong.try_arm(empty_authority(), stamp(4, None, None)));
        let token = wrong
            .begin_direct(stamp(4, Some(1), Some(2)), direct_span(1, 1, 1, 1, 2))
            .expect("token");
        assert!(!wrong.finish_direct(token, stamp(5, Some(2), Some(2))));
        assert!(wrong.is_disabled());

        let mut wrong_direct_authority = FreshLocatorCoverage::new();
        assert!(wrong_direct_authority.try_arm(empty_authority(), stamp(4, None, None)));
        let token = wrong_direct_authority
            .begin_direct(stamp(4, Some(1), Some(2)), direct_span(1, 1, 1, 1, 2))
            .expect("direct token");
        assert!(!wrong_direct_authority.finish_direct(
            token,
            stamp_with(
                5,
                Some(1),
                Some(2),
                ApplicationSequenceAllocator::next(
                    CommitSequence::new(2).expect("allocator sequence"),
                ),
                [0x5a; 32],
            ),
        ));
        assert!(wrong_direct_authority.is_disabled());

        for invalid_expected in [
            stamp(5, Some(1), Some(2)),
            stamp(4, None, Some(2)),
            stamp(4, Some(2), Some(4)),
            stamp(4, Some(1), Some(3)),
            stamp_with(
                4,
                Some(1),
                Some(2),
                ApplicationSequenceAllocator::initial(),
                [0xa5; 32],
            ),
        ] {
            let mut invalid = FreshLocatorCoverage::new();
            assert!(invalid.try_arm(empty_authority(), stamp(4, None, None)));
            assert!(
                invalid
                    .begin_direct(invalid_expected, direct_span(1, 1, 1, 1, 2))
                    .is_none()
            );
            assert!(invalid.is_disabled());
        }

        let mut preserving = FreshLocatorCoverage::new();
        assert!(preserving.try_arm(empty_authority(), stamp(6, None, None)));
        let token = preserving
            .begin_preserving_immediate(FreshLocatorCoverage::preserving_permit(1, [1; 32]))
            .expect("preserving token");
        assert!(!preserving.finish_preserving_immediate(
            token,
            stamp_with(
                7,
                None,
                Some(1),
                ApplicationSequenceAllocator::initial(),
                [0x5a; 32],
            ),
        ));
        assert!(preserving.is_disabled());

        for (case, invalid_successor) in [
            stamp(6, None, Some(1)),
            stamp(7, Some(1), Some(1)),
            stamp_with(
                7,
                None,
                Some(1),
                ApplicationSequenceAllocator::next(
                    CommitSequence::new(2).expect("wrong allocator sequence"),
                ),
                [0xa5; 32],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let mut invalid = FreshLocatorCoverage::new();
            assert!(invalid.try_arm(empty_authority(), stamp(6, None, None)));
            let token = invalid
                .begin_preserving_immediate(FreshLocatorCoverage::preserving_permit(1, [1; 32]))
                .expect("preserving token");
            assert!(
                !invalid.finish_preserving_immediate(token, invalid_successor),
                "invalid preserving successor case {case}"
            );
            assert!(invalid.is_disabled());
        }
        for invalid_permit in [
            FreshLocatorCoverage::preserving_permit(0, [1; 32]),
            FreshLocatorCoverage::preserving_permit(1, [0; 32]),
        ] {
            let mut invalid = FreshLocatorCoverage::new();
            assert!(invalid.try_arm(empty_authority(), stamp(6, None, None)));
            match invalid.begin_preserving_immediate(invalid_permit) {
                None => assert!(invalid.is_disabled()),
                Some(token) => {
                    assert!(!invalid.finish_preserving_immediate(token, stamp(7, None, Some(1))));
                    assert!(invalid.is_disabled());
                }
            }
        }

        let mut rebase = FreshLocatorCoverage::new();
        assert!(rebase.try_arm(empty_authority(), stamp(6, None, None)));
        let token = rebase.begin_rebase(stamp(6, None, None)).expect("rebase");
        assert!(rebase.proves_public_absence(stamp(6, None, None)));
        assert!(rebase.abort_rebase(token));
        assert!(rebase.allows_private_miss(stamp(6, None, None)));
        let token = rebase.begin_rebase(stamp(6, None, None)).expect("rebase");
        assert!(rebase.finish_rebase(token, stamp(7, None, None)));
        assert!(rebase.proves_public_absence(stamp(7, None, None)));
        assert!(rebase.allows_private_miss(stamp(7, None, None)));

        let mut wrong_rebase_authority = FreshLocatorCoverage::new();
        assert!(wrong_rebase_authority.try_arm(empty_authority(), stamp(8, None, None)));
        let token = wrong_rebase_authority
            .begin_rebase(stamp(8, None, None))
            .expect("rebase token");
        assert!(!wrong_rebase_authority.finish_rebase(
            token,
            stamp_with(
                9,
                None,
                None,
                ApplicationSequenceAllocator::initial(),
                [0x5a; 32],
            ),
        ));
        assert!(wrong_rebase_authority.is_disabled());

        for (case, invalid_successor) in [
            stamp(8, None, None),
            stamp(9, Some(1), None),
            stamp(9, None, Some(1)),
            stamp_with(
                9,
                None,
                None,
                ApplicationSequenceAllocator::next(
                    CommitSequence::new(2).expect("wrong allocator sequence"),
                ),
                [0xa5; 32],
            ),
        ]
        .into_iter()
        .enumerate()
        {
            let mut invalid = FreshLocatorCoverage::new();
            assert!(invalid.try_arm(empty_authority(), stamp(8, None, None)));
            let token = invalid
                .begin_rebase(stamp(8, None, None))
                .expect("rebase token");
            assert!(
                !invalid.finish_rebase(token, invalid_successor),
                "invalid rebase successor case {case}"
            );
            assert!(invalid.is_disabled());
        }

        let mut wrong_rebase_predecessor = FreshLocatorCoverage::new();
        assert!(wrong_rebase_predecessor.try_arm(empty_authority(), stamp(10, None, None)));
        assert!(
            wrong_rebase_predecessor
                .begin_rebase(stamp(11, None, None))
                .is_none()
        );
        assert!(wrong_rebase_predecessor.is_disabled());

        let mut uncertain_rebase = FreshLocatorCoverage::new();
        assert!(uncertain_rebase.try_arm(empty_authority(), stamp(12, None, None)));
        let token = uncertain_rebase
            .begin_rebase(stamp(12, None, None))
            .expect("rebase token");
        uncertain_rebase.disable();
        assert!(!uncertain_rebase.abort_rebase(token));
        assert!(uncertain_rebase.is_disabled());

        let reopened = FreshLocatorCoverage::new();
        assert!(!reopened.proves_public_absence(stamp(7, None, None)));
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
    #[test]
    fn cold_fresh_database_publications_complete_without_history_scans() {
        let mut coverage = FreshLocatorCoverage::new();
        assert!(coverage.try_arm(empty_authority(), stamp(10, None, None)));
        for sequence in 1..=288_u64 {
            let witness = coverage
                .seal_command(stamp(10, Some(sequence), Some(sequence * 2)), 1)
                .expect("bounded contiguous seal");
            assert!(coverage.publish_command(witness));
        }
        assert!(coverage.proves_public_absence(stamp(10, Some(288), Some(576))));
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004
    #[test]
    fn dropping_each_sole_live_coverage_witness_disables_fail_closed() {
        let mut command = FreshLocatorCoverage::new();
        assert!(command.try_arm(empty_authority(), stamp(1, None, None)));
        drop(
            command
                .seal_command(stamp(1, Some(1), Some(2)), 1)
                .expect("command publication witness"),
        );
        assert!(!command.is_armed());
        assert!(!command.allows_private_miss(stamp(1, Some(1), Some(2))));
        assert!(command.is_disabled());

        let mut preserve = FreshLocatorCoverage::new();
        assert!(preserve.try_arm(empty_authority(), stamp(1, None, None)));
        drop(
            preserve
                .seal_preserve(stamp(1, None, Some(1)))
                .expect("preserve publication witness"),
        );
        assert!(!preserve.allows_private_miss(stamp(1, None, Some(1))));
        assert!(preserve.is_disabled());

        let mut direct = FreshLocatorCoverage::new();
        assert!(direct.try_arm(empty_authority(), stamp(4, None, None)));
        drop(
            direct
                .begin_direct(stamp(4, Some(1), Some(2)), direct_span(1, 1, 1, 1, 2))
                .expect("direct witness"),
        );
        assert!(!direct.allows_private_miss(stamp(4, None, None)));
        assert!(direct.is_disabled());

        let mut immediate = FreshLocatorCoverage::new();
        assert!(immediate.try_arm(empty_authority(), stamp(6, None, None)));
        drop(
            immediate
                .begin_preserving_immediate(FreshLocatorCoverage::preserving_permit(1, [1; 32]))
                .expect("preserving Immediate witness"),
        );
        assert!(!immediate.allows_private_miss(stamp(6, None, None)));
        assert!(immediate.is_disabled());

        let mut rebase = FreshLocatorCoverage::new();
        assert!(rebase.try_arm(empty_authority(), stamp(7, None, None)));
        drop(
            rebase
                .begin_rebase(stamp(7, None, None))
                .expect("rebase witness"),
        );
        assert!(!rebase.allows_private_miss(stamp(7, None, None)));
        assert!(rebase.is_disabled());
    }

    // req: OUT-001, OUT-002, TXN-042, REC-004
    #[test]
    fn concurrent_witness_loss_linearizes_before_or_after_each_coverage_use() {
        let mut public = FreshLocatorCoverage::new();
        assert!(public.try_arm(empty_authority(), stamp(1, None, None)));
        let lost = public
            .seal_command(stamp(1, Some(1), Some(2)), 1)
            .expect("outstanding publication witness");
        let schedule = public.schedule_live_witness_observation();
        let public_use = std::thread::spawn(move || {
            let allowed = public.proves_public_absence(stamp(1, None, None));
            (public, allowed)
        });
        schedule.wait_until_operation_acquires_live();
        drop(lost);
        schedule.release_operation();
        let (mut public, allowed) = public_use.join().expect("public proof joins");
        assert!(
            allowed,
            "the pre-loss operation uses only the old public view"
        );
        assert!(!public.allows_private_miss(stamp(1, Some(1), Some(2))));
        assert!(public.is_disabled());

        let mut private = FreshLocatorCoverage::new();
        assert!(private.try_arm(empty_authority(), stamp(2, None, None)));
        let lost = private
            .seal_command(stamp(2, Some(1), Some(2)), 1)
            .expect("outstanding private successor witness");
        let schedule = private.schedule_live_witness_observation();
        let private_use = std::thread::spawn(move || {
            let allowed = private.allows_private_miss(stamp(2, Some(1), Some(2)));
            (private, allowed)
        });
        schedule.wait_until_operation_acquires_live();
        drop(lost);
        schedule.release_operation();
        let (mut private, allowed) = private_use.join().expect("private proof joins");
        assert!(
            allowed,
            "the pre-loss operation uses only the existing private successor"
        );
        assert!(!private.allows_private_miss(stamp(2, Some(1), Some(2))));
        assert!(private.is_disabled());

        let mut publication = FreshLocatorCoverage::new();
        assert!(publication.try_arm(empty_authority(), stamp(3, None, None)));
        let first = publication
            .seal_command(stamp(3, Some(1), Some(2)), 1)
            .expect("first publication witness");
        let later = publication
            .seal_command(stamp(3, Some(2), Some(4)), 1)
            .expect("later publication witness");
        let schedule = publication.schedule_live_witness_observation();
        let publication_use = std::thread::spawn(move || {
            let advanced = publication.publish_command(first);
            (publication, advanced)
        });
        schedule.wait_until_operation_acquires_live();
        drop(later);
        schedule.release_operation();
        let (mut publication, advanced) =
            publication_use.join().expect("publication attempt joins");
        assert!(
            advanced,
            "the pre-loss publication may advance only its exact predecessor"
        );
        assert!(!publication.allows_private_miss(stamp(3, Some(2), Some(4))));
        assert!(publication.is_disabled());
    }
}
