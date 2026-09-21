//! Crate-private source maintenance at the existing drained writer barrier.
//! No public exports, application mutation, callback or bootstrap-release permit.
#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "WP-772 proves the internal substrate; WP-746 owns activation through authorized replication composition"
    )
)]

use super::*;
use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::changelog_v3_write::{PreparedHistoryAdvance, value_error};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3 as Source, ChangelogCursorErrorV3 as Refusal,
    ChangelogHistoryStateV3 as History, FollowerRegistrationPhaseV1 as Phase,
    MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldStateV1 as State, ReplicationSourceHoldV1 as Hold,
    proto_codec::{
        decode_replication_source_hold, decode_replication_source_hold_v1,
        encode_replication_source_hold_v1, encode_replication_source_hold_v2,
    },
};

#[path = "changelog_source_control_transaction.rs"]
mod transaction;
use transaction::Barrier;
pub(crate) use transaction::ControlWrite;
#[path = "replication_administration_write.rs"]
mod administration;
#[path = "changelog_bootstrap_attachment.rs"]
mod bootstrap_attachment;
#[path = "primary_fence_control.rs"]
mod primary_fence;
#[path = "changelog_registered_ack.rs"]
mod registered_ack;
#[path = "replication_registration_maintenance.rs"]
mod registration_maintenance;
#[path = "changelog_source_control_retention.rs"]
mod retention;

#[derive(Clone, Copy)]
struct Observation {
    publication: u64,
    durable_epoch: u64,
    history: History,
}

/// Internal owner; inaccessible to application/library consumers. WP-746 owns
/// authorization and remote durability evidence before activating any caller.
/// There is no generic hold removal, bootstrap release or raw row editor.
pub(crate) struct ReplicationSourceControl {
    shared: Arc<SharedRedb>,
    observed: Option<Observation>,
}

impl RedbOperationalPorts {
    /// A retained artifact handle is not permission to continue after retirement
    /// or custody loss. Outbound bytes use one current, completely validated pin.
    pub(crate) fn validate_bootstrap_custody(&self, bootstrap: Hold) -> Result<(), StorageError> {
        if self.shared.write_fenced.load(Ordering::Acquire) || self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let root = self.shared.capture_checkpoint_root()?;
        let history = crate::changelog_v3_roots::validate_retained_history(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        if bootstrap.kind() != Kind::Bootstrap || bootstrap.lineage() != history.lineage() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let table = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let row = table
            .get(bootstrap.storage_key().as_slice())
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::Unavailable))?;
        let held = *decode_replication_source_hold_v1(row.value())
            .map_err(crate::error::codec_error)?
            .value();
        if held != bootstrap {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let key = Hold::storage_key_for(bootstrap.id(), Kind::FollowerAcknowledgement);
        if let Some(row) = table.get(key.as_slice()).map_err(precommit_storage_error)? {
            let state = *decode_replication_source_hold(row.value())
                .map_err(crate::error::codec_error)?
                .value();
            if let State::Registered(policy) = state
                && !registered_ack::permits_bootstrap(policy, bootstrap)
            {
                return Err(storage_error(StorageErrorKind::Unavailable));
            }
        }
        Ok(())
    }

    /// Bounded source-only custody probe. Cleanup already owns the artifact's
    /// actual engine exclusion, so its registration cannot race this read.
    pub(crate) fn bootstrap_id_is_held(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
    ) -> Result<bool, StorageError> {
        self.bootstrap_id_has_custody(id, true)
    }

    /// A pending registration permits its first artifact, but cannot authorize
    /// replacement of any existing bootstrap, attached follower or archive hold.
    pub(crate) fn bootstrap_id_blocks_new_artifact(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
    ) -> Result<bool, StorageError> {
        self.bootstrap_id_has_custody(id, false)
    }

    fn bootstrap_id_has_custody(
        &self,
        id: riffdb_storage_api::ReplicationSourceHoldIdV1,
        include_pending_registration: bool,
    ) -> Result<bool, StorageError> {
        if self.shared.write_fenced.load(Ordering::Acquire) || self.shared.is_follower_mode() {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        let root = self.shared.capture_checkpoint_root()?;
        // Excluding a retired policy can permit artifact cleanup. Prove the
        // original registration and exact release from this same pin first.
        crate::changelog_v3_roots::validate_retained_history(&root)?
            .ok_or_else(|| storage_error(StorageErrorKind::IncompatibleFormat))?;
        let table = root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        for kind in [
            Kind::Bootstrap,
            Kind::FollowerAcknowledgement,
            Kind::ArchiveAcknowledgement,
        ] {
            let key = Hold::storage_key_for(id, kind);
            if let Some(row) = table.get(key.as_slice()).map_err(precommit_storage_error)? {
                let state = *decode_replication_source_hold(row.value())
                    .map_err(crate::error::codec_error)?
                    .value();
                let (hold, retired) = match state {
                    State::Legacy(hold) => (hold, false),
                    State::Registered(policy) => (policy.hold(), policy.phase() == Phase::Retired),
                };
                if hold.id() != id || hold.kind() != kind {
                    return Err(storage_error(StorageErrorKind::CorruptData));
                }
                if retired
                    || (!include_pending_registration
                        && matches!(state, State::Registered(policy) if policy.phase() == Phase::AwaitingBootstrap))
                {
                    continue;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// A new handle loses only conservative observations, never durable holds.
    pub(crate) fn replication_source_control(&self) -> ReplicationSourceControl {
        ReplicationSourceControl {
            shared: Arc::clone(&self.shared),
            observed: None,
        }
    }
}

impl ReplicationSourceControl {
    /// Exact registration retries allocate no receipt; replacement is refused.
    pub(crate) fn register(&mut self, hold: Hold) -> Result<bool, Refusal> {
        self.change_hold(hold, false)
    }

    /// The internal caller owns remote durable-ack evidence. Bootstrap is fixed.
    pub(crate) fn advance_acknowledgement(&mut self, hold: Hold) -> Result<bool, Refusal> {
        if hold.kind() == Kind::Bootstrap {
            return Err(Refusal::InvalidPosition);
        }
        self.change_hold(hold, true)
    }

    fn change_hold(&mut self, hold: Hold, advancing: bool) -> Result<bool, Refusal> {
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        // Reuse the sole exact-position checker on the already drained pin.
        drop(crate::changelog_v3_cursor::open(
            &RedbReadAccess::Durable(Arc::clone(&barrier.root)),
            hold.lineage(),
            hold.fence(),
        )?);
        let table = barrier.root.open_table(SOURCE_HOLDS).map_err(table_error)?;
        let prior = table
            .get(hold.storage_key().as_slice())
            .map_err(precommit_storage_error)?
            .map(|row| {
                decode_replication_source_hold(row.value())
                    .map(|decoded| *decoded.value())
                    .map_err(crate::error::codec_error)
            })
            .transpose()?;
        let (prior, policy) = match prior {
            None => (None, None),
            Some(State::Legacy(hold)) => (Some(hold), None),
            Some(State::Registered(policy)) => {
                // Neither a legacy registration retry nor an equal ack may
                // bypass the registered lifecycle or downgrade its policy.
                if !advancing || policy.phase() != Phase::Attached {
                    return Err(Refusal::InvalidPosition);
                }
                (Some(policy.hold()), Some(policy))
            }
        };
        if hold.kind() == Kind::Bootstrap {
            let key = Hold::storage_key_for(hold.id(), Kind::FollowerAcknowledgement);
            let follower = table
                .get(key.as_slice())
                .map_err(precommit_storage_error)?
                .map(|row| {
                    decode_replication_source_hold(row.value())
                        .map(|decoded| *decoded.value())
                        .map_err(crate::error::codec_error)
                })
                .transpose()?;
            match follower {
                Some(State::Registered(policy)) => {
                    // Check even an equal job retry: retirement must never
                    // renew custody or expose an old artifact under this ID.
                    if !registered_ack::permits_bootstrap(policy, hold) {
                        return Err(Refusal::InvalidPosition);
                    }
                }
                Some(State::Legacy(_)) if prior.is_none() => {
                    return Err(Refusal::InvalidPosition);
                }
                _ => {}
            }
        }
        if prior == Some(hold) {
            return Ok(false);
        }
        match (advancing, prior) {
            (false, None) => {
                if table.len().map_err(precommit_storage_error)? >= MAX_REPLICATION_SOURCE_HOLDS_V1
                {
                    return Err(storage_error(StorageErrorKind::LimitExceeded).into());
                }
            }
            (true, Some(prior)) if prior.fence().sequence() < hold.fence().sequence() => {}
            _ => return Err(Refusal::InvalidPosition),
        }
        drop(table);
        let encoded = if let Some(policy) = policy {
            let successor = registered_ack::advance(policy, hold, barrier.observation().history)
                .map_err(value_error)?;
            encode_replication_source_hold_v2(successor).map_err(crate::error::codec_error)?
        } else {
            encode_replication_source_hold_v1(hold).map_err(crate::error::codec_error)?
        };
        let write = barrier.begin()?;
        // Exhaustion and complete preflight precede the first mutation.
        let (receipt, _) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::ReplicationSourceHold,
        )?;
        write
            .transaction()?
            .open_table(SOURCE_HOLDS)
            .map_err(table_error)?
            .insert(hold.storage_key().as_slice(), encoded.as_bytes())
            .map_err(precommit_storage_error)?;
        crash_edge("hold-staged");
        receipt.stage(write.transaction()?)?;
        write.validate()?;
        crash_edge("hold-receipted");
        write.commit()?;
        crash_edge("hold-committed");
        Ok(true)
    }

    /// Deletes at most 256 receipts, strictly below the prior observed durable
    /// checkpoint AND all consumer fences, only after a later durable checkpoint.
    /// A first/same-root observation or a no-op creates no control transaction.
    pub(crate) fn reclaim_history(&mut self) -> Result<bool, Refusal> {
        let barrier = Barrier::acquire(Arc::clone(&self.shared))?;
        let current = barrier.observation();
        let Some(previous) = self.observed else {
            self.observed = Some(current);
            return Ok(false);
        };
        if previous.publication == current.publication
            || previous.durable_epoch >= current.durable_epoch
            || previous.history.lineage() != current.history.lineage()
            || previous.history.tail().sequence() < current.history.minimum_resume().sequence()
        {
            self.observed = Some(current);
            return Ok(false);
        }
        drop(crate::changelog_v3_cursor::open(
            &RedbReadAccess::Durable(Arc::clone(&barrier.root)),
            previous.history.lineage(),
            previous.history.tail(),
        )?);
        let mut derived = self.shared.derived_source_pins.reclamation()?;
        let derived_floor = derived.oldest(current.history.lineage());
        let Some(floor) = retention::floor(
            &barrier.root,
            current.history,
            previous.history.tail(),
            derived_floor,
        )?
        else {
            self.observed = Some(current);
            return Ok(false);
        };
        let write = barrier.begin()?;
        let (receipt, successor) = prepare_control_receipt(
            write.transaction()?,
            write.history(),
            Source::HistoryReclamation,
        )?;
        crash_edge("reclamation-planned");
        retention::stage(
            write.transaction()?,
            current.history,
            successor,
            floor,
            receipt,
        )?;
        write.validate()?;
        crash_edge("reclamation-staged");
        derived.reserve(current.history.lineage(), floor);
        // Observe the post-commit root so this receipt cannot stimulate itself.
        self.observed = Some(write.commit()?);
        crash_edge("reclamation-committed");
        Ok(true)
    }
}

fn prepare_control_receipt(
    transaction: &WriteTransaction,
    history: History,
    source: Source,
) -> Result<(PreparedHistoryAdvance, History), StorageError> {
    if !matches!(
        source,
        Source::ReplicationSourceHold | Source::HistoryReclamation
    ) {
        return Err(storage_error(StorageErrorKind::InvariantViolation));
    }
    let (sequence, _) = history
        .expected_allocator()
        .allocate_one()
        .map_err(value_error)?;
    let receipt = AuthoritativeTransactionV3::new_for_catalog(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence,
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: history.tail().frontier(),
            prior_history_hash: history.tail().history_hash(),
        },
        source,
        Vec::new(),
        history.lineage().catalog_digest(),
    )
    .map_err(value_error)?;
    Ok((
        PreparedHistoryAdvance::prepare(transaction, &receipt)?,
        history.advance(&receipt).map_err(value_error)?,
    ))
}

fn crash_edge(_edge: &str) {
    #[cfg(test)]
    if std::env::var("RIFFDB_V3_SOURCE_CONTROL_EDGE")
        .ok()
        .as_deref()
        == Some(_edge)
    {
        std::process::exit(93);
    }
}
