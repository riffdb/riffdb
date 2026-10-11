//! One captured Immediate transaction for a checked claim group (ADR-0284).

use super::*;
use riffdb_storage_api::{
    OutboxClaimGroupCompletionV1, OutboxClaimGroupResultV1, OutboxClaimGroupV1,
};

pub(super) fn claim(
    ports: &RedbOperationalPorts,
    group: &OutboxClaimGroupV1,
) -> Result<OutboxClaimGroupResultV1, StorageError> {
    // Recheck the semantic boundary and all encodings before admission.
    OutboxClaimGroupV1::new(group.claims().to_vec()).map_err(request_value_error)?;
    let encoded = group
        .claims()
        .iter()
        .map(|claim| encode_outbox_status_v1(claim.updated()))
        .collect::<Result<Vec<_>, _>>()?;
    let access = ports
        .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition)?;
    access.ensure_outbox_indexes_available()?;
    let mut results = Vec::with_capacity(group.claims().len());
    let mut matched = Vec::with_capacity(group.claims().len());
    let mut missing_authority = false;
    {
        let transaction = access.transaction()?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        for (index, claim) in group.claims().iter().enumerate() {
            let Some(current) = reciprocal_outbox_item(
                ports,
                &events,
                &intents,
                &statuses,
                &commits,
                claim.event_id(),
            )?
            else {
                missing_authority = true;
                break;
            };
            if &current.status != claim.expected() {
                results.push(OutboxTransitionResultV1::StateChanged(current.status));
            } else {
                matched.push(index);
                results.push(OutboxTransitionResultV1::Applied(claim.updated().clone()));
            }
        }
    }
    if missing_authority {
        access.abort()?;
        return Ok(OutboxClaimGroupResultV1::AuthoritativeIntentMissing);
    }
    let completion =
        OutboxClaimGroupCompletionV1::new(group, results).map_err(request_value_error)?;
    if matched.is_empty() {
        access.abort()?;
        return Ok(OutboxClaimGroupResultV1::Complete(completion));
    }
    {
        let transaction = access.transaction()?;
        let mut statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        for &index in &matched {
            let claim = &group.claims()[index];
            let key = encode_event_key(claim.event_id());
            let previous = statuses
                .insert(key.as_slice(), encoded[index].as_bytes())
                .map_err(precommit_storage_error)?;
            // The preimage must be exactly the observed one: absent for an
            // initial pending event, the expected bytes for an explicit one.
            match (claim.expected(), previous.as_ref()) {
                (OutboxStatusObservationV1::AbsentInitialPending, None) => {}
                (OutboxStatusObservationV1::Present(expected), Some(previous)) => {
                    let prior = decode_outbox_status_v1(previous.value())?.into_parts().0;
                    if &prior != expected || prior.event_id() != claim.event_id() {
                        return Err(corrupt());
                    }
                }
                (
                    OutboxStatusObservationV1::AbsentInitialPending
                    | OutboxStatusObservationV1::Present(_),
                    None | Some(_),
                ) => return Err(corrupt()),
            }
        }
    }
    access.commit_for_with_delta(
        RedbTestOperation::OutboxTransition,
        Some(TransientIndexDelta::OutboxClaimed(
            matched
                .iter()
                .map(|&index| group.claims()[index].event_id())
                .collect(),
        )),
    )?;
    Ok(OutboxClaimGroupResultV1::Complete(completion))
}
