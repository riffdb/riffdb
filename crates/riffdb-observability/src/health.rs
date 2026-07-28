//! Authoritative and derived process-health aggregation.

use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use riffdb_service::{AuthoritativeReadinessFailure, ServiceHealthHooks};
use riffdb_types::CommitSequence;

/// Maximum closed findings retained for either derived subsystem.
pub const MAX_DERIVED_FINDINGS_PER_COMPONENT: usize = 16;

/// Overall process-health classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthClassification {
    /// At least one authoritative prerequisite is unavailable.
    NotReady,
    /// Authoritative operation is ready and all derived workers are ready.
    Ready,
    /// Authoritative operation is ready while derived work is impaired.
    Degraded,
}

/// Authoritative components whose joint state controls write readiness.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AuthoritativeComponent {
    /// Durable storage and structural integrity.
    Storage,
    /// Checked active catalog.
    Catalog,
    /// Sole-writer commit coordinator.
    CommitCoordinator,
}

impl AuthoritativeComponent {
    const ALL: [Self; 3] = [Self::Storage, Self::Catalog, Self::CommitCoordinator];

    const fn index(self) -> usize {
        match self {
            Self::Storage => 0,
            Self::Catalog => 1,
            Self::CommitCoordinator => 2,
        }
    }
}

/// Closed authoritative component condition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthoritativeCondition {
    /// The component has completed its required proof and accepts work.
    Healthy,
    /// The component is unavailable or its required proof failed.
    Unavailable,
}

/// Derived subsystem identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DerivedComponent {
    /// Event-delivery worker state.
    Outbox,
    /// Projection worker and rebuildable state.
    Projection,
}

impl DerivedComponent {
    const ALL: [Self; 2] = [Self::Outbox, Self::Projection];

    const fn index(self) -> usize {
        match self {
            Self::Outbox => 0,
            Self::Projection => 1,
        }
    }
}

/// Closed derived subsystem condition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DerivedCondition {
    /// Retained state is valid and no recovery is required.
    Healthy,
    /// Rebuildable state or a worker is impaired.
    Degraded,
    /// The subsystem cannot currently provide a trustworthy observation.
    Unavailable,
}

/// Closed, payload-free derived health finding.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DerivedFinding {
    /// Worker startup or recovery has not completed.
    RecoveryPending,
    /// Retained derived state failed its integrity check.
    Integrity,
    /// A bounded status source is unavailable.
    StatusUnavailable,
    /// The worker stopped after startup.
    WorkerStopped,
}

/// One authoritative component snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthoritativeComponentSnapshot {
    component: AuthoritativeComponent,
    condition: AuthoritativeCondition,
}

impl AuthoritativeComponentSnapshot {
    /// Returns the component identity.
    #[must_use]
    pub const fn component(self) -> AuthoritativeComponent {
        self.component
    }

    /// Returns its effective condition.
    #[must_use]
    pub const fn condition(self) -> AuthoritativeCondition {
        self.condition
    }
}

/// One derived subsystem snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedComponentSnapshot {
    component: DerivedComponent,
    condition: DerivedCondition,
    worker_ready: bool,
    findings: Vec<DerivedFinding>,
}

impl DerivedComponentSnapshot {
    /// Returns the component identity.
    #[must_use]
    pub const fn component(&self) -> DerivedComponent {
        self.component
    }

    /// Returns its state-integrity condition.
    #[must_use]
    pub const fn condition(&self) -> DerivedCondition {
        self.condition
    }

    /// Reports whether the owning worker completed startup and recovery.
    #[must_use]
    pub const fn worker_ready(&self) -> bool {
        self.worker_ready
    }

    /// Borrows canonical closed findings.
    #[must_use]
    pub fn findings(&self) -> &[DerivedFinding] {
        &self.findings
    }
}

/// One immutable bounded health observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    classification: HealthClassification,
    liveness: bool,
    authoritative: [AuthoritativeComponentSnapshot; 3],
    derived: [DerivedComponentSnapshot; 2],
    last_commit_sequence: Option<CommitSequence>,
}

impl HealthSnapshot {
    /// Returns the overall classification.
    #[must_use]
    pub const fn classification(&self) -> HealthClassification {
        self.classification
    }

    /// Reports process event-loop liveness.
    #[must_use]
    pub const fn liveness(&self) -> bool {
        self.liveness
    }

    /// Reports whether all authoritative prerequisites remain ready.
    #[must_use]
    pub fn authoritative_ready(&self) -> bool {
        self.authoritative
            .iter()
            .all(|component| component.condition == AuthoritativeCondition::Healthy)
    }

    /// Borrows the three authoritative component observations.
    #[must_use]
    pub const fn authoritative_components(&self) -> &[AuthoritativeComponentSnapshot; 3] {
        &self.authoritative
    }

    /// Borrows the outbox and projection observations.
    #[must_use]
    pub const fn derived_components(&self) -> &[DerivedComponentSnapshot; 2] {
        &self.derived
    }

    /// Returns the last authoritative application commit, or `None` before the first commit.
    #[must_use]
    pub const fn last_commit_sequence(&self) -> Option<CommitSequence> {
        self.last_commit_sequence
    }
}

#[derive(Clone)]
struct DerivedState {
    condition: DerivedCondition,
    worker_ready: bool,
    findings: Vec<DerivedFinding>,
}

struct HealthState {
    liveness: bool,
    authoritative: [AuthoritativeCondition; 3],
    readiness_failed: bool,
    derived: [DerivedState; 2],
    last_commit_sequence: Option<CommitSequence>,
}

/// A cloneable, process-local owner of health observations.
///
/// Authoritative readiness failure is monotonic for one process lifecycle.
/// Derived state may recover without rewriting or reclassifying source commits.
#[derive(Clone)]
pub struct HealthRegistry {
    inner: Arc<Mutex<HealthState>>,
}

impl HealthRegistry {
    /// Creates an initializing registry with no fabricated commit position.
    #[must_use]
    pub fn new() -> Self {
        let initial_derived = DerivedState {
            condition: DerivedCondition::Unavailable,
            worker_ready: false,
            findings: vec![DerivedFinding::RecoveryPending],
        };
        Self {
            inner: Arc::new(Mutex::new(HealthState {
                liveness: true,
                authoritative: [AuthoritativeCondition::Unavailable; 3],
                readiness_failed: false,
                derived: [initial_derived.clone(), initial_derived],
                last_commit_sequence: None,
            })),
        }
    }

    /// Updates process liveness without changing readiness evidence.
    pub fn set_liveness(&self, liveness: bool) {
        self.lock_state().liveness = liveness;
    }

    /// Updates one authoritative component before any monotonic failure.
    pub fn set_authoritative(
        &self,
        component: AuthoritativeComponent,
        condition: AuthoritativeCondition,
    ) {
        self.lock_state().authoritative[component.index()] = condition;
    }

    /// Replaces one bounded derived observation.
    pub fn set_derived(
        &self,
        component: DerivedComponent,
        condition: DerivedCondition,
        worker_ready: bool,
        mut findings: Vec<DerivedFinding>,
    ) -> Result<(), HealthUpdateError> {
        if findings.len() > MAX_DERIVED_FINDINGS_PER_COMPONENT {
            return Err(HealthUpdateError::TooManyFindings);
        }
        findings.sort_unstable();
        findings.dedup();
        if condition == DerivedCondition::Healthy && !findings.is_empty() {
            return Err(HealthUpdateError::InconsistentDerivedState);
        }
        self.lock_state().derived[component.index()] = DerivedState {
            condition,
            worker_ready,
            findings,
        };
        Ok(())
    }

    /// Observes an authoritative application sequence without permitting regression.
    pub fn observe_commit(&self, sequence: CommitSequence) -> Result<(), HealthUpdateError> {
        let mut state = self.lock_state();
        if state
            .last_commit_sequence
            .is_some_and(|current| sequence < current)
        {
            state.readiness_failed = true;
            return Err(HealthUpdateError::CommitSequenceRegressed);
        }
        state.last_commit_sequence = Some(sequence);
        Ok(())
    }

    /// Monotonically fails authoritative readiness for a local internal condition.
    pub fn fail_authoritative_readiness(&self) {
        self.lock_state().readiness_failed = true;
    }

    /// Returns one bounded immutable observation.
    #[must_use]
    pub fn snapshot(&self) -> HealthSnapshot {
        let state = self.lock_state();
        let effective = AuthoritativeComponent::ALL.map(|component| {
            let condition = if state.readiness_failed {
                AuthoritativeCondition::Unavailable
            } else {
                state.authoritative[component.index()]
            };
            AuthoritativeComponentSnapshot {
                component,
                condition,
            }
        });
        let derived = DerivedComponent::ALL.map(|component| {
            let value = &state.derived[component.index()];
            DerivedComponentSnapshot {
                component,
                condition: value.condition,
                worker_ready: value.worker_ready,
                findings: value.findings.clone(),
            }
        });
        let authoritative_ready = effective
            .iter()
            .all(|component| component.condition == AuthoritativeCondition::Healthy);
        let derived_ready = derived.iter().all(|component| {
            component.condition == DerivedCondition::Healthy && component.worker_ready
        });
        let classification = if !authoritative_ready {
            HealthClassification::NotReady
        } else if !derived_ready {
            HealthClassification::Degraded
        } else {
            HealthClassification::Ready
        };
        HealthSnapshot {
            classification,
            liveness: state.liveness,
            authoritative: effective,
            derived,
            last_commit_sequence: state.last_commit_sequence,
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, HealthState> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            let mut state = poisoned.into_inner();
            state.readiness_failed = true;
            state
        })
    }
}

impl Default for HealthRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ServiceHealthHooks for HealthRegistry {
    fn fail_authoritative_readiness(&self, _reason: AuthoritativeReadinessFailure) {
        self.fail_authoritative_readiness();
    }
}

impl fmt::Debug for HealthRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("HealthRegistry([REDACTED])")
    }
}

/// A rejected health update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthUpdateError {
    /// More closed findings were supplied than one component may retain.
    TooManyFindings,
    /// A healthy derived component cannot simultaneously carry findings.
    InconsistentDerivedState,
    /// An observed authoritative commit position moved backward.
    CommitSequenceRegressed,
}

impl fmt::Display for HealthUpdateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TooManyFindings => "derived health finding limit exceeded",
            Self::InconsistentDerivedState => "derived health state is inconsistent",
            Self::CommitSequenceRegressed => "authoritative commit position regressed",
        })
    }
}

impl Error for HealthUpdateError {}
