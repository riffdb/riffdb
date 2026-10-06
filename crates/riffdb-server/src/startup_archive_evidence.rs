//! Exact restore comparison across the native successor's completion receipt.
//! Keeps one bounded receipt, a bounded net certificate delta and one row from
//! each immutable state cursor; interrupted retries preserve every prior edge.

use riffdb_storage_api::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N,
    AuthoritativeStateCursorV3, AuthoritativeStateRowV3, AuthoritativeStateStepV3 as Step,
    ChangelogAttributionV3 as A, ChangelogHistoryStateV3, CompositeTableV1,
    PublishedDurableSnapshot, current_record_registry_digest, decode_record_registry,
    decode_validated_prefix_checkpoint_v2,
};
use riffdb_storage_redb::RedbOperationalPorts;
use std::cmp::Ordering;

#[cfg(test)]
#[path = "startup_archive_evidence_tests.rs"]
mod tests;

pub(super) fn matches(
    ports: &RedbOperationalPorts,
    mut expected: Box<dyn AuthoritativeStateCursorV3>,
) -> Result<bool, ()> {
    let snapshot = ports.published_changelog_snapshot_v3().map_err(|_| ())?;
    let mut actual = snapshot.authoritative_state_v3().map_err(|_| ())?;
    let before = expected.history();
    let after = actual.history();
    if before.lineage() != after.lineage()
        || before.anchor() != after.anchor()
        || before.minimum_resume() != after.minimum_resume()
        || before.tail().frontier() != after.tail().frontier()
    {
        return Ok(false);
    }
    let mut receipts = snapshot
        .changelog_receipts_v3(before.lineage(), before.tail())
        .map_err(|_| ())?;
    let mut completion = AuthoritativeMutationAccumulatorV3::default();
    let mut last_checkpoint = None;
    let mut count = 0;
    while let Some(receipt) = receipts.next_receipt().map_err(|_| ())? {
        count += 1;
        if count > riffdb_storage_api::MAX_CHANGELOG_FRAME_ENTRIES
            || receipt.binding().predecessor_frontier != before.tail().frontier()
            || receipt.binding().covered_frontier != before.tail().frontier()
        {
            return Ok(false);
        }
        match receipt.attribution() {
            A::DirtyActivation if receipt.mutations().is_empty() => {}
            A::ValidatedPrefixCheckpoint => {
                if !valid_completion(snapshot.as_ref(), before, receipt.mutations())? {
                    return Ok(false);
                }
                for mutation in receipt.mutations() {
                    if mutation.namespace() == N::ValidatedPrefixCheckpoint {
                        last_checkpoint = mutation.value().map(<[u8]>::to_vec);
                    }
                    completion.record(mutation.clone()).map_err(|_| ())?;
                }
            }
            _ => return Ok(false),
        }
    }
    if let Some(checkpoint) = last_checkpoint {
        // The sequential fold may cancel a transition to its original hash.
        // Validate the actual final post-image even in that case; cancellation
        // cannot conceal an invented prior or a non-published checkpoint.
        if snapshot
            .read_value(
                CompositeTableV1::Meta,
                N::ValidatedPrefixCheckpoint
                    .metadata_key()
                    .ok_or(())?
                    .as_bytes(),
            )
            .map_err(|_| ())?
            .as_deref()
            != Some(checkpoint.as_slice())
        {
            return Ok(false);
        }
    }
    let mutations = completion.finish().map_err(|_| ())?;
    compare(expected.as_mut(), actual.as_mut(), &mutations)
}

fn valid_completion(
    snapshot: &dyn PublishedDurableSnapshot,
    history: ChangelogHistoryStateV3,
    mutations: &[AuthoritativeMutationV3],
) -> Result<bool, ()> {
    let lineage = history.lineage();
    let registry = snapshot
        .read_value(
            CompositeTableV1::Meta,
            N::RecordRegistry.metadata_key().ok_or(())?.as_bytes(),
        )
        .map_err(|_| ())?
        .ok_or(())?;
    if !decode_record_registry(&registry)
        .map_err(|_| ())?
        .value()
        .completed_for(
            lineage.database_id(),
            lineage.history_incarnation(),
            current_record_registry_digest(),
        )
    {
        return Ok(false);
    }
    let mut checkpoint_seen = false;
    for mutation in mutations {
        match mutation.namespace() {
            N::ValidatedPrefixEntityHeads => {
                // A renewed certificate may snapshot only the exact live chain
                // heads at this same publication pin, including exact absence.
                if snapshot
                    .read_value(CompositeTableV1::EntityChainHeads, mutation.key())
                    .map_err(|_| ())?
                    .as_deref()
                    != mutation.value()
                {
                    return Ok(false);
                }
            }
            N::RecordRegistry if mutation.value() == Some(registry.as_slice()) => {}
            N::ValidatedPrefixCheckpoint => {
                let checkpoint = decode_validated_prefix_checkpoint_v2(mutation.value().ok_or(())?)
                    .map_err(|_| ())?;
                let base = checkpoint.value().base();
                if base.database_id() != lineage.database_id()
                    || base.history_incarnation() != lineage.history_incarnation()
                    || base.registry_digest() != current_record_registry_digest()
                    || base.checkpoint_commit_sequence()
                        != history
                            .tail()
                            .frontier()
                            .application()
                            .map_or(0, |s| s.get())
                    || base.audit_sequence_bound()
                        > history
                            .tail()
                            .frontier()
                            .administration()
                            .map_or(0, |s| s.get())
                {
                    return Ok(false);
                }
                checkpoint_seen = true;
            }
            // Commands, catalog changes, allocators and every other row remain
            // byte-exact restore evidence; no new namespace can be normalized.
            _ => return Ok(false),
        }
    }
    Ok(checkpoint_seen)
}

fn compare(
    expected: &mut dyn AuthoritativeStateCursorV3,
    actual: &mut dyn AuthoritativeStateCursorV3,
    mutations: &[AuthoritativeMutationV3],
) -> Result<bool, ()> {
    let mut left = expected.next_item().map_err(|_| ())?;
    let mut changes = mutations.iter().peekable();
    loop {
        let mut consume_left = true;
        let mut consume_change = false;
        let mut prior = None;
        let apply = match (left.as_ref(), changes.peek()) {
            (Some(Step::Row(row)), Some(change)) => {
                match (row.namespace(), row.key()).cmp(&(change.namespace(), change.key())) {
                    Ordering::Less => false,
                    Ordering::Equal => {
                        prior = Some(row.value());
                        true
                    }
                    Ordering::Greater => {
                        consume_left = false;
                        true
                    }
                }
            }
            (Some(Step::EndNamespace(namespace)), Some(change))
                if *namespace == change.namespace() =>
            {
                consume_left = false;
                true
            }
            (None, Some(_)) => return Ok(false),
            _ => false,
        };
        let output = if apply {
            let change = changes.peek().ok_or(())?;
            if !change.matches_prior(prior) {
                return Ok(false);
            }
            consume_change = true;
            change
                .value()
                .map(|value| {
                    AuthoritativeStateRowV3::new(change.namespace(), change.key(), value)
                        .map(Step::Row)
                        .map_err(|_| ())
                })
                .transpose()?
        } else {
            left.take()
        };
        if consume_change {
            changes.next();
        }
        if let Some(output) = output {
            if actual.next_item().map_err(|_| ())? != Some(output) {
                return Ok(false);
            }
        } else if !apply {
            return Ok(actual.next_item().map_err(|_| ())?.is_none());
        }
        if consume_left {
            left = expected.next_item().map_err(|_| ())?;
        }
    }
}
