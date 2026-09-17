//! Advisory source statistics over one immutable publication. Only the bounded
//! source-control table is visited; no receipt, entity or command population.
use super::*;
use redb::{ReadableTable, ReadableTableMetadata};
use riffdb_storage_api::{
    MAX_REPLICATION_SOURCE_HOLDS_V1, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceProgressV3, proto_codec::decode_replication_source_hold,
};

pub(crate) fn observe(
    access: &RedbReadAccess,
) -> Result<ReplicationSourceProgressV3, ChangelogCursorErrorV3> {
    let (root, history) = match access {
        RedbReadAccess::Current(root) | RedbReadAccess::Durable(root) => (
            Arc::clone(root),
            read_checkpoint_roots(root)?.ok_or_else(corrupt)?,
        ),
        RedbReadAccess::Composite(view) => {
            let history = view.changelog_suffix().history.ok_or_else(corrupt)?;
            if history.tail().frontier()
                != riffdb_types::DualFrontier::new(
                    view.overlay().published_application(),
                    view.overlay().published_administration(),
                )
            {
                return Err(corrupt().into());
            }
            (view.checkpoint_root_shared(), history)
        }
    };
    let table = root
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .map_err(table_error)?;
    if table.len().map_err(precommit_storage_error)? > MAX_REPLICATION_SOURCE_HOLDS_V1 {
        return Err(limit().into());
    }
    let mut count = 0_u32;
    let mut degraded = 0_u32;
    let mut maintenance_pending = false;
    let mut oldest: Option<ChangelogHistoryPointV3> = None;
    for (index, row) in table.iter().map_err(precommit_storage_error)?.enumerate() {
        if u64::try_from(index).map_err(|_| limit())? >= MAX_REPLICATION_SOURCE_HOLDS_V1 {
            return Err(limit().into());
        }
        let (key, value) = row.map_err(precommit_storage_error)?;
        let state = *decode_replication_source_hold(value.value())
            .map_err(crate::error::codec_error)?
            .value();
        let hold = match state {
            riffdb_storage_api::ReplicationSourceHoldStateV1::Legacy(hold) => hold,
            riffdb_storage_api::ReplicationSourceHoldStateV1::Registered(policy) => policy.hold(),
        };
        if key.value() != hold.storage_key() || hold.lineage() != history.lineage() {
            return Err(corrupt().into());
        }
        if let riffdb_storage_api::ReplicationSourceHoldStateV1::Registered(policy) = state {
            if !policy.registered_at().precedes_or_equals(history.tail())
                || policy.generation() > history.tail().sequence()
                || !hold.fence().precedes_or_equals(history.tail())
                || policy
                    .degraded_at()
                    .is_some_and(|point| !point.precedes_or_equals(history.tail()))
            {
                return Err(corrupt().into());
            }
            if policy.phase() == riffdb_storage_api::FollowerRegistrationPhaseV1::Retired {
                continue;
            }
            let exhausted = policy
                .budget()
                .observe(hold, history)
                .map_err(value_error)?
                .is_exhausted();
            let expired = policy
                .expires_at()
                .is_some_and(|expiry| Some(expiry) <= history.tail().frontier().application());
            maintenance_pending |= expired || (exhausted && policy.degraded_at().is_none());
            if exhausted || expired {
                degraded = degraded.checked_add(1).ok_or_else(limit)?;
            }
        }
        // Startup and the source-control writer prove exact receipt ancestry.
        // This observational read checks shape/frontiers, never upgrades decoded
        // holds into retention permission or replays their receipt payloads.
        ChangelogHistoryStateV3::new(
            history.lineage(),
            history.minimum_resume(),
            history.tail(),
            hold.fence(),
        )
        .map_err(value_error)?;
        if hold.kind() == Kind::FollowerAcknowledgement {
            count = count.checked_add(1).ok_or_else(limit)?;
            if oldest.is_none_or(|point| hold.fence().sequence() < point.sequence()) {
                oldest = Some(hold.fence());
            } else if oldest.is_some_and(|point| {
                point.sequence() == hold.fence().sequence() && point != hold.fence()
            }) {
                return Err(corrupt().into());
            }
        }
    }
    ReplicationSourceProgressV3::new(history, count, oldest)
        .and_then(|progress| progress.with_degraded_followers(degraded))
        .and_then(|progress| progress.with_registration_maintenance_pending(maintenance_pending))
        .map_err(|e| value_error(e).into())
}
