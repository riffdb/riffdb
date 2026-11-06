//! Monotonic primary quorum lease monitor and automatic follower election coordinator (`ADR-0337`).

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use riffdb_service::{AuthoritativeReadinessFailure, ServiceHealthHooks};
use riffdb_storage_api::ReplicationSourceHoldIdV1;

use crate::runtime_support::{RuntimeRoutingState, RuntimeStopReason};

/// Deterministic priority step per voter index so higher-priority voters bid first.
pub(crate) const ELECTION_RANK_STEP: Duration = Duration::from_millis(50);

/// Monotonic primary quorum lease monitor (`ADR-0337` Decision 3).
///
/// Tracks monotonic `Instant` timestamps of voter follower acknowledgements and
/// stream contacts. When fewer than the required voter quorum have refreshed
/// contact within `lease_duration`, immediately self-fences `RuntimeRoutingState`
/// (`RuntimeStopReason::CoordinatorFenced`) before any follower's
/// `election_timeout` (`> lease_duration`) can elapse.
pub(crate) struct PrimaryQuorumLeaseTracker {
    voters: Vec<ReplicationSourceHoldIdV1>,
    required_voters: usize,
    lease_duration: Duration,
    election_timeout: Duration,
    last_voter_contact: Mutex<BTreeMap<ReplicationSourceHoldIdV1, Instant>>,
    routing: RuntimeRoutingState,
}

impl PrimaryQuorumLeaseTracker {
    pub(crate) fn new(
        voters: Vec<ReplicationSourceHoldIdV1>,
        lease_duration: Duration,
        election_timeout: Duration,
        routing: RuntimeRoutingState,
        started_at: Instant,
    ) -> Option<Arc<Self>> {
        if voters.is_empty()
            || voters.len() > riffdb_types::MAX_REPLICATION_VOTERS_V1
            || lease_duration.is_zero()
            || election_timeout <= lease_duration
        {
            return None;
        }
        let mut last_voter_contact = BTreeMap::new();
        for &voter in &voters {
            if last_voter_contact.insert(voter, started_at).is_some() {
                return None;
            }
        }
        let required_voters = (voters.len() / 2).max(1);
        Some(Arc::new(Self {
            voters,
            required_voters,
            lease_duration,
            election_timeout,
            last_voter_contact: Mutex::new(last_voter_contact),
            routing,
        }))
    }

    pub(crate) const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) const fn election_timeout(&self) -> Duration {
        self.election_timeout
    }

    pub(crate) const fn required_voters(&self) -> usize {
        self.required_voters
    }

    /// Records a monotonic voter acknowledgement or stream poll contact if the
    /// primary has not already self-fenced.
    pub(crate) fn record_voter_contact(
        &self,
        hold_id: ReplicationSourceHoldIdV1,
        at: Instant,
    ) -> bool {
        if !self.routing.is_routing_allowed() {
            return false;
        }
        if !self.voters.contains(&hold_id) {
            return false;
        }
        let Ok(mut map) = self.last_voter_contact.lock() else {
            self.routing
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            return false;
        };
        map.entry(hold_id)
            .and_modify(|prev| {
                if at > *prev {
                    *prev = at;
                }
            })
            .or_insert(at);
        true
    }

    /// Evaluates whether the primary still holds a valid quorum lease at `now`.
    /// If the lease has expired, immediately self-fences `RuntimeRoutingState`
    /// (`CoordinatorFenced`) and returns `false`.
    pub(crate) fn check_or_fence(&self, now: Instant) -> bool {
        if !self.routing.is_routing_allowed() {
            return false;
        }
        let Ok(map) = self.last_voter_contact.lock() else {
            self.routing
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            return false;
        };
        let active_voters = self
            .voters
            .iter()
            .filter(|voter| {
                map.get(voter).is_some_and(|&last_contact| {
                    now.saturating_duration_since(last_contact) < self.lease_duration
                })
            })
            .count();
        drop(map);
        if active_voters < self.required_voters {
            self.routing
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::CoordinatorFenced);
            return false;
        }
        true
    }

    pub(crate) fn is_fenced(&self) -> bool {
        !self.routing.is_routing_allowed()
            && self.routing.stop_reason() == Some(RuntimeStopReason::CoordinatorFenced)
    }

    /// Spawns a background watchdog that periodically checks the monotonic
    /// quorum lease and self-fences immediately on expiry.
    pub(crate) fn spawn_monitor(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let tracker = Arc::clone(self);
        let tick_interval = (self.lease_duration / 4).max(Duration::from_millis(10));
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = tracker.routing.stopped() => break,
                    _ = interval.tick() => {
                        if !tracker.check_or_fence(Instant::now()) {
                            break;
                        }
                    }
                }
            }
        })
    }
}

/// Follower election timeout and automatic failover coordinator (`ADR-0337` Decision 3).
#[derive(Clone, Debug)]
pub(crate) struct AutoFailoverCoordinator {
    voters: Vec<ReplicationSourceHoldIdV1>,
    candidate_hold: ReplicationSourceHoldIdV1,
    lease_duration: Duration,
    base_election_timeout: Duration,
    effective_election_timeout: Duration,
    last_primary_activity: Instant,
}

impl AutoFailoverCoordinator {
    pub(crate) fn new(
        voters: Vec<ReplicationSourceHoldIdV1>,
        candidate_hold: ReplicationSourceHoldIdV1,
        lease_duration: Duration,
        base_election_timeout: Duration,
        started_at: Instant,
    ) -> Option<Self> {
        if voters.is_empty()
            || voters.len() > riffdb_types::MAX_REPLICATION_VOTERS_V1
            || lease_duration.is_zero()
            || base_election_timeout <= lease_duration
        {
            return None;
        }
        let rank = voters.iter().position(|&v| v == candidate_hold)?;
        let rank_offset = Duration::from_millis(
            (rank as u64).saturating_mul(ELECTION_RANK_STEP.as_millis() as u64),
        );
        let effective_election_timeout = base_election_timeout.saturating_add(rank_offset);
        Some(Self {
            voters,
            candidate_hold,
            lease_duration,
            base_election_timeout,
            effective_election_timeout,
            last_primary_activity: started_at,
        })
    }

    pub(crate) const fn candidate_hold(&self) -> ReplicationSourceHoldIdV1 {
        self.candidate_hold
    }

    pub(crate) fn voters(&self) -> &[ReplicationSourceHoldIdV1] {
        &self.voters
    }

    pub(crate) const fn lease_duration(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) const fn base_election_timeout(&self) -> Duration {
        self.base_election_timeout
    }

    pub(crate) const fn effective_election_timeout(&self) -> Duration {
        self.effective_election_timeout
    }

    pub(crate) fn record_primary_activity(&mut self, at: Instant) {
        if at > self.last_primary_activity {
            self.last_primary_activity = at;
        }
    }

    pub(crate) fn should_start_election(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.last_primary_activity) >= self.effective_election_timeout
    }
}
