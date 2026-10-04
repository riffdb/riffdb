//! Bounded, complete receipt-prefix evaluation. No current row is substituted
//! for an earlier post-image, and no physical receipt is split.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_policy::{AuthorizedQueryRowPolicyContextV1, ProjectedPolicyCandidateObservationV1};
use riffdb_query_ir::QueryAccessProgramV1;
use riffdb_storage_api::{
    AdmittedProviderEntityRecordV1, AuthoritativeNamespaceV1, ChangelogCursorErrorV3,
    ChangelogHistoryPointV3, MAX_CHANGELOG_FRAME_BYTES, StorageErrorKind,
    proto_codec::decode_provider_entity_record_admitted_v1,
};
use riffdb_types::{CanonicalValue, EntityKey, ProjectionGeneration};

use super::super::{
    MAX_PROJECTED_POLICY_CANDIDATES_V1, RebuildFailure, map_provider_policy_admission_error,
};
use super::{CandidatePopulation, CapturedSource, SelectedSource};

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
    pub(super) changes: BTreeMap<EntityKey, Option<AdmittedProviderEntityRecordV1>>,
    pub(super) source: SelectedSource,
    // Last: delta conversion forests are destroyed before admission releases.
    pub(super) work: super::super::SourceRowsWork,
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
        captured.ensure_active()?;
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
        let mut cursor = match next.receipts_after_admitted_v1(prior, &captured.budget) {
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
        let mut work = super::super::SourceRowsWork::new(&captured.budget)?;
        let prefix = step
            .internal_entity_key_schema()
            .encode_entity_prefix(std::slice::from_ref(plan.partition_value))
            .map_err(|_| RebuildFailure::Integrity)?;
        work.reserve_tree::<riffdb_types::EntityTypeId, ()>(plan.policy.map_or(0, |policy| {
            policy
                .internal_relationship_entities(step.internal_entity_id())
                .count()
        }))?;
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
        let mut population =
            CandidatePopulation::copy_from(&captured.budget, &previous.candidates)?;
        let mut changes = BTreeMap::new();
        let mut point = prior.position();
        let mut bytes = 0_usize;
        let mut commits = 0_u64;
        let mut normalized_bytes = 0_usize;
        for _ in 0..MAX_RECEIPTS {
            captured.ensure_active()?;
            let Some(receipt) = cursor
                .next_receipt_admitted_v1(&captured.budget)
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
                work.reserve_key(mutation.key().len())?;
                let _key_work = work.key_decode(mutation.key().len(), mutation.key().len())?;
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
                    let record = decode_provider_entity_record_admitted_v1(value, &captured.budget)
                        .map_err(|error| match error {
                            riffdb_storage_api::ProviderRecordDecodeErrorV1::AdmissionUnavailable => RebuildFailure::Transient,
                            riffdb_storage_api::ProviderRecordDecodeErrorV1::Codec(_) => RebuildFailure::Integrity,
                        })?;
                    if record.target().key() != &key {
                        return Err(RebuildFailure::Integrity);
                    }
                    let record = captured.materialize(plan.program, &record)?;
                    work.reserve_row(record.fields(), 0, 0, key.as_bytes().len())?;
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
            for key in inserted {
                if !population.contains(&key) && population.len() >= maximum {
                    return Err(RebuildFailure::Capacity(plan.generation));
                }
                population.insert(&key)?;
            }
            if population.len() > maximum {
                return Err(RebuildFailure::Capacity(plan.generation));
            }
            let hash_bytes = u64::try_from(charge)
                .ok()
                .and_then(|bytes| bytes.checked_mul(2))
                .and_then(|bytes| bytes.checked_add(4096))
                .ok_or(RebuildFailure::Transient)?;
            let _receipt_hash_work = captured
                .budget
                .reserve(hash_bytes)
                .map_err(|_| RebuildFailure::Transient)?;
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
            work.reserve_keys(changes.keys())?;
            let keys = changes
                .iter()
                .filter(|(_, record)| record.is_some())
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>();
            let proof_work = captured
                .budget
                .reserve(
                    riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1
                        + 32 * 1024
                        + keys
                            .iter()
                            .map(|key| key.as_bytes().len() as u64 + 4096)
                            .sum::<u64>(),
                )
                .map_err(|_| RebuildFailure::Transient)?;
            let admission = if dependencies.is_empty() {
                for record in changes.values().flatten() {
                    work.reserve_policy(policy, step.internal_entity_id(), record.fields())?;
                }
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
                let proof = policy
                    .authorize_projected_candidates(step.internal_entity_id(), observations)
                    .map_err(|_| RebuildFailure::Integrity)?;
                proof_work
                    .settle_owned_value(proof)
                    .map_err(|_| RebuildFailure::Transient)?
            } else {
                // Administrative receipts can change relationship evidence
                // without advancing the application head. Only the exact
                // captured physical prefix proves that this evidence belongs
                // to our post-images, rather than to a later same-head state.
                if point != next.position() || head != captured.head {
                    return Ok(None);
                }
                let keys = keys.iter().cloned().collect::<Vec<_>>();
                captured
                    .authorize_projected_candidates(step.internal_entity_id(), &keys, policy)
                    .map_err(|error| map_provider_policy_admission_error(error, plan.generation))?
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
            .at_admitted_v1(point, &captured.budget)
            .map_err(|error| cursor_failure(error, plan.generation))?;
        Ok(Some(Self {
            changes,
            work,
            source: SelectedSource {
                pin: Some(pin),
                head,
                candidates: population.finish()?,
                _metadata: captured._metadata.clone(),
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
