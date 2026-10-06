//! One captured Immediate transaction for a checked success group.

use super::*;
use riffdb_storage_api::{
    OutboxSucceedGroupCompletionV1, OutboxSucceedGroupResultV1, OutboxSucceedGroupV1,
};

pub(super) fn complete(
    ports: &RedbOperationalPorts,
    group: &OutboxSucceedGroupV1,
) -> Result<OutboxSucceedGroupResultV1, StorageError> {
    // Recheck the semantic boundary and all encodings before admission.
    OutboxSucceedGroupV1::new(group.transitions().to_vec()).map_err(request_value_error)?;
    let encoded = group
        .transitions()
        .iter()
        .map(|transition| encode_outbox_status_v1(transition.updated()))
        .collect::<Result<Vec<_>, _>>()?;
    let access = ports
        .begin_attributed_write(riffdb_storage_api::ChangelogAttributionV3::OutboxTransition)?;
    access.ensure_outbox_indexes_available()?;
    let mut results = Vec::with_capacity(group.transitions().len());
    let mut matched = Vec::with_capacity(group.transitions().len());
    let mut missing_authority = false;
    {
        let transaction = access.transaction()?;
        let events = transaction.open_table(EVENTS).map_err(table_error)?;
        let intents = transaction.open_table(OUTBOX).map_err(table_error)?;
        let statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        let commits = transaction.open_table(COMMITS).map_err(table_error)?;
        for (index, transition) in group.transitions().iter().enumerate() {
            let Some(current) = reciprocal_outbox_item(
                ports,
                &events,
                &intents,
                &statuses,
                &commits,
                transition.event_id(),
            )?
            else {
                missing_authority = true;
                break;
            };
            if current.status != OutboxStatusObservationV1::Present(transition.expected().clone()) {
                results.push(OutboxTransitionResultV1::StateChanged(current.status));
            } else {
                matched.push(index);
                results.push(OutboxTransitionResultV1::Applied(
                    transition.updated().clone(),
                ));
            }
        }
    }
    if missing_authority {
        access.abort()?;
        return Ok(OutboxSucceedGroupResultV1::AuthoritativeIntentMissing);
    }
    let completion =
        OutboxSucceedGroupCompletionV1::new(group, results).map_err(request_value_error)?;
    if matched.is_empty() {
        access.abort()?;
        return Ok(OutboxSucceedGroupResultV1::Complete(completion));
    }
    {
        let transaction = access.transaction()?;
        let mut statuses = transaction.open_table(OUTBOX_STATUS).map_err(table_error)?;
        for &index in &matched {
            let transition = &group.transitions()[index];
            let key = encode_event_key(transition.event_id());
            let previous = statuses
                .insert(key.as_slice(), encoded[index].as_bytes())
                .map_err(precommit_storage_error)?
                .ok_or_else(corrupt)?;
            let prior = decode_outbox_status_v1(previous.value())?.into_parts().0;
            if &prior != transition.expected() || prior.event_id() != transition.event_id() {
                return Err(corrupt());
            }
        }
    }
    access.commit_for_with_delta(
        RedbTestOperation::OutboxTransition,
        Some(TransientIndexDelta::OutboxSucceeded(
            matched
                .iter()
                .map(|&index| group.transitions()[index].event_id())
                .collect(),
        )),
    )?;
    Ok(OutboxSucceedGroupResultV1::Complete(completion))
}
