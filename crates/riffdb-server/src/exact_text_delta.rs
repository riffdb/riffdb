//! Bounded, complete receipt-prefix evaluation. No current row is substituted
//! for an earlier post-image, and no physical receipt is split.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_policy::{AuthorizedQueryRowPolicyContextV1, ProjectedPolicyCandidateObservationV1};
use riffdb_query_executor::QueryExecutionPort;
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, ChangelogCursorErrorV3, ChangelogHistoryPointV3,
    MAX_CHANGELOG_FRAME_BYTES, StorageErrorKind, StoredEntityRecordV1,
    proto_codec::decode_entity_record_v1,
};
use riffdb_types::{CanonicalValue, EntityKey, ProjectionGeneration};

use super::super::{
    MAX_PROJECTED_POLICY_CANDIDATES_V1, RebuildFailure, map_policy_admission_error,
};
use super::{CapturedSource, SelectedSource};

const MAX_APPLICATION_COMMITS: u64 = 64;
// Administrative-only traffic must also have a fixed per-pass ceiling.
const MAX_RECEIPTS: usize = 64;

pub(super) struct DeltaPlan<'a> {
    pub(super) program: &'a QueryAccessProgramV1,
    pub(super) partition_value: &'a CanonicalValue,
    pub(super) max_candidates: u32,
    pub(super) policy: Option<&'a AuthorizedQueryRowPolicyContextV1>,
    pub(super) generation: ProjectionGeneration,
}

pub(super) struct Delta {
    pub(super) changes: BTreeMap<EntityKey, Option<StoredEntityRecordV1>>,
    pub(super) source: SelectedSource,
}

impl Delta {
    /// None means the existing full rebuild is required (unavailable history,
    /// an unsplittable oversized group, or a dependency requiring full admission).
    /// Corruption and source substitution are never represented by None.
    pub(super) fn read(
        captured: &CapturedSource,
        previous: &SelectedSource,
        plan: DeltaPlan<'_>,
    ) -> Result<Option<Self>, RebuildFailure> {
        let (Some(next), Some(prior)) = (&captured.pin, &previous.pin) else {
            return Ok(None);
        };
        if previous.head
            != prior
                .position()
                .frontier()
                .application()
                .ok_or(RebuildFailure::Integrity)?
        {
            return Err(RebuildFailure::Integrity);
        }
        let mut cursor = match next.receipts_after(prior) {
            Ok(cursor) => cursor,
            Err(ChangelogCursorErrorV3::Storage(error))
                if error.kind() == StorageErrorKind::HistoryPruned =>
            {
                return Ok(None);
            }
            Err(_) => return Err(RebuildFailure::Integrity),
        };
        let [step] = plan.program.steps() else {
            return Err(RebuildFailure::Integrity);
        };
        let prefix = step
            .internal_entity_key_schema()
            .encode_entity_prefix(std::slice::from_ref(plan.partition_value))
            .map_err(|_| RebuildFailure::Integrity)?;
        let dependencies = plan
            .policy
            .into_iter()
            .flat_map(|policy| policy.internal_relationship_entities(step.internal_entity_id()))
            .collect::<BTreeSet<_>>();
        let maximum = usize::try_from(plan.max_candidates)
            .map_err(|_| RebuildFailure::Integrity)?
            .min(MAX_PROJECTED_POLICY_CANDIDATES_V1);
        if previous.candidates.len() > maximum {
            return Err(RebuildFailure::Integrity);
        }
        let mut population = previous.candidates.clone();
        let mut changes = BTreeMap::new();
        let mut point = prior.position();
        let mut bytes = 0_usize;
        let mut commits = 0_u64;
        let mut normalized_bytes = 0_usize;
        for _ in 0..MAX_RECEIPTS {
            let Some(receipt) = cursor
                .next_receipt()
                .map_err(|error| cursor_failure(error, plan.generation))?
            else {
                break;
            };
            let binding = receipt.binding();
            let count = binding
                .covered_frontier
                .application()
                .map_or(0, |value| value.get())
                .checked_sub(
                    binding
                        .predecessor_frontier
                        .application()
                        .map_or(0, |value| value.get()),
                )
                .ok_or(RebuildFailure::Integrity)?;
            let charge = receipt
                .encoded_len()
                .map_err(|_| RebuildFailure::Integrity)?;
            if count > MAX_APPLICATION_COMMITS || charge > MAX_CHANGELOG_FRAME_BYTES {
                return Ok(None);
            }
            if commits + count > MAX_APPLICATION_COMMITS
                || bytes.saturating_add(charge) > MAX_CHANGELOG_FRAME_BYTES
            {
                break;
            }
            commits += count;
            bytes += charge;
            // Deletes precede inserts in the population accounting for this
            // atomic receipt. Sorted physical keys must not impose a false
            // temporary over-capacity result on a replacement at the bound.
            let mut inserted = Vec::new();
            for mutation in receipt.mutations() {
                if mutation.namespace() != AuthoritativeNamespaceV1::Entities {
                    continue;
                }
                let key = EntityKey::from_bytes(mutation.key().to_vec())
                    .map_err(|_| RebuildFailure::Integrity)?;
                if dependencies.contains(&key.entity_type_id()) {
                    return Ok(None);
                }
                if !key.as_bytes().starts_with(&prefix) {
                    continue;
                }
                step.internal_entity_key_schema()
                    .decode_entity(&key)
                    .map_err(|_| RebuildFailure::Integrity)?;
                // Migration changes without an application frontier cannot be
                // replayed as an irrelevant commit or assigned a false epoch.
                if count == 0 {
                    return Ok(None);
                }
                if let Some(value) = mutation.value() {
                    if mutation.expected_hash().is_some() != population.contains(&key) {
                        return Err(RebuildFailure::Integrity);
                    }
                    let (record, _) = decode_entity_record_v1(value)
                        .map_err(|_| RebuildFailure::Integrity)?
                        .into_parts();
                    if record.target().key() != &key {
                        return Err(RebuildFailure::Integrity);
                    }
                    let record = captured.materialize(plan.program, record)?;
                    let charge = riffdb_types::canonical_record_encoded_len(record.fields())
                        .map_err(|_| RebuildFailure::Integrity)?;
                    normalized_bytes = normalized_bytes
                        .checked_add(key.as_bytes().len())
                        .and_then(|bytes| bytes.checked_add(charge))
                        .ok_or(RebuildFailure::Capacity(plan.generation))?;
                    if normalized_bytes > MAX_CHANGELOG_FRAME_BYTES {
                        return Ok(None);
                    }
                    inserted.push(key.clone());
                    changes.insert(key, Some(record));
                } else {
                    if !population.remove(&key) {
                        return Err(RebuildFailure::Integrity);
                    }
                    if previous.candidates.contains(&key) {
                        changes.insert(key, None);
                    } else {
                        changes.remove(&key);
                    }
                }
            }
            population.extend(inserted);
            if population.len() > maximum {
                return Err(RebuildFailure::Capacity(plan.generation));
            }
            point = ChangelogHistoryPointV3::from_receipt(&receipt)
                .map_err(|_| RebuildFailure::Integrity)?;
        }
        let head = point
            .frontier()
            .application()
            .ok_or(RebuildFailure::Integrity)?;
        if head < previous.head || changes.len() > maximum.saturating_mul(2) {
            return Err(RebuildFailure::Integrity);
        }
        if let Some(policy) = plan.policy {
            let keys = changes
                .iter()
                .filter(|(_, record)| record.is_some())
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>();
            let admission = if dependencies.is_empty() {
                let observations = changes
                    .iter()
                    .filter_map(|(key, record)| {
                        record.as_ref().map(|record| {
                            ProjectedPolicyCandidateObservationV1::current(
                                key.clone(),
                                record.fields().clone(),
                                Vec::new(),
                            )
                        })
                    })
                    .collect();
                policy
                    .authorize_projected_candidates(step.internal_entity_id(), observations)
                    .map_err(|_| RebuildFailure::Integrity)?
            } else {
                // Administrative receipts can change relationship evidence
                // without advancing the application head. Only the exact
                // captured physical prefix proves that this evidence belongs
                // to our post-images, rather than to a later same-head state.
                if point != next.position() || head != captured.head {
                    return Ok(None);
                }
                let keys = keys.iter().cloned().collect::<Vec<_>>();
                QueryExecutionPort::authorize_projected_candidates(
                    &captured.storage.query_executor(),
                    step.internal_entity_id(),
                    &keys,
                    policy,
                )
                .map_err(|error| map_policy_admission_error(error, plan.generation))?
            };
            if !admission.covers(step.internal_entity_id(), &keys) {
                return Err(RebuildFailure::Integrity);
            }
            for (key, record) in &mut changes {
                if record.is_some() && !admission.admits(key) {
                    *record = None;
                }
            }
        }
        let pin = next
            .at(point)
            .map_err(|error| cursor_failure(error, plan.generation))?;
        Ok(Some(Self {
            changes,
            source: SelectedSource {
                pin: Some(pin),
                head,
                candidates: population,
            },
        }))
    }
}

fn cursor_failure(
    error: ChangelogCursorErrorV3,
    generation: ProjectionGeneration,
) -> RebuildFailure {
    match error {
        ChangelogCursorErrorV3::Storage(error) if error.kind() == StorageErrorKind::Unavailable => {
            RebuildFailure::Transient
        }
        ChangelogCursorErrorV3::Storage(error)
            if error.kind() == StorageErrorKind::LimitExceeded =>
        {
            RebuildFailure::Capacity(generation)
        }
        _ => RebuildFailure::Integrity,
    }
}
