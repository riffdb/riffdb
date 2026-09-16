//! Compact progress preserves the legacy public documents and immutable bindings.

use super::*;
use riffdb_storage_api::{
    ApplicationExportPageCommitmentV1, ApplicationExportPageOrdinalV1,
    StoredApplicationExportOperationV2, encode_application_export_head,
    encode_application_export_page_commitment_v1,
};

const COMPACT_STATE_SCHEMA: &str = "riffdb.application-export-operation/v3";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompactWire {
    schema: String,
    state: ExportStateWireV1,
    // Store canonical document text, not JSON arrays of individual byte values.
    manifest: Option<String>,
    receipt: Option<String>,
}

fn body(state: &ExportState) -> Result<Vec<u8>, ApplicationExportMutationPortErrorV1> {
    let mut wire = state_to_wire(state);
    wire.page_hashes.clear();
    let manifest = wire
        .manifest
        .take()
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let receipt = wire
        .receipt
        .take()
        .map(String::from_utf8)
        .transpose()
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    serde_json::to_vec(&CompactWire {
        schema: COMPACT_STATE_SCHEMA.to_owned(),
        state: wire,
        manifest,
        receipt,
    })
    .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

fn immutable_binding(state: &ExportState) -> Result<Vec<u8>, ApplicationExportMutationPortErrorV1> {
    let mut wire = state_to_wire(state);
    // Reset only progress. All authority, snapshot, selection, lease and portable
    // schedule identities remain in the canonical genesis preimage.
    wire.phase = phase_tag(ApplicationExportPhaseV1::Accepted);
    wire.failure = None;
    wire.current_class = first_selected_class(&state.selection).map(ApplicationExportClassV1::tag);
    wire.continuation = None;
    wire.pages_released = 0;
    wire.rows_released = 0;
    wire.bytes_released = 0;
    wire.class_pages = [0; 4];
    wire.class_rows = [0; 4];
    wire.class_bytes = [0; 4];
    wire.page_hashes.clear();
    wire.manifest = None;
    wire.receipt = None;
    wire.portability_entity_schedule_index = 0;
    for workflow in &mut wire.workflow_quiescence {
        workflow.checked_rows = 0;
        workflow.quiescent_rows = 0;
        workflow.non_quiescent_rows = 0;
    }
    serde_json::to_vec(&wire).map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)
}

pub(super) fn initialize(
    state: &mut ExportState,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    state.ledger = Some(
        ApplicationExportLedgerPrefixV1::genesis(state.operation_id, &immutable_binding(state)?)
            .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?,
    );
    Ok(())
}

pub(super) fn stored(
    state: &ExportState,
) -> Result<StoredApplicationExportOperation, ApplicationExportMutationPortErrorV1> {
    let prefix = state
        .ledger
        .ok_or(ApplicationExportMutationPortErrorV1::Integrity)?;
    StoredApplicationExportOperationV2::new(
        state.operation_id,
        state.selection.lineage().clone(),
        immutable_binding(state)?,
        body(state)?,
        prefix,
    )
    .map(StoredApplicationExportOperation::Compact)
    .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)
}

pub(super) fn decode(record: &StoredApplicationExportOperationV2) -> Result<ExportState, ()> {
    let mut wire: CompactWire = serde_json::from_slice(record.canonical_state()).map_err(|_| ())?;
    if wire.schema != COMPACT_STATE_SCHEMA
        || wire.state.manifest.is_some()
        || wire.state.receipt.is_some()
    {
        return Err(());
    }
    wire.state.manifest = wire.manifest.take().map(String::into_bytes);
    wire.state.receipt = wire.receipt.take().map(String::into_bytes);
    let state = wire_to_state(wire.state, Some(*record.prefix()))?;
    if state.operation_id != record.operation_id()
        || state.selection.lineage() != record.lineage()
        || immutable_binding(&state).map_err(|_| ())? != record.immutable_binding()
        || body(&state).map_err(|_| ())? != record.canonical_state()
    {
        return Err(());
    }
    Ok(state)
}

pub(super) fn replay_identity(
    record: &StoredApplicationExportOperation,
) -> Result<Vec<u8>, ApplicationExportMutationPortErrorV1> {
    match record {
        StoredApplicationExportOperation::Legacy(value) => Ok(value.canonical_state().to_vec()),
        StoredApplicationExportOperation::Compact(_) => encode_application_export_head(record)
            .map(|value| value.as_bytes().to_vec())
            .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity),
    }
}

pub(super) fn append(
    state: &mut ExportState,
    class: ApplicationExportClassV1,
    page_hash: ApplicationExportPageHash,
    rows: u64,
    bytes: u64,
) -> Result<Option<ApplicationExportPageCommitmentV1>, ApplicationExportMutationPortErrorV1> {
    let Some(prefix) = state.ledger else {
        state.page_hashes.push(page_hash);
        return Ok(None);
    };
    let entry = ApplicationExportPageCommitmentV1::new(
        state.operation_id,
        ApplicationExportPageOrdinalV1::new(state.pages_released)
            .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?,
        class,
        page_hash,
        rows,
        bytes,
    )
    .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    let encoded = encode_application_export_page_commitment_v1(&entry)
        .map_err(|_| ApplicationExportMutationPortErrorV1::Integrity)?;
    let charge = entry
        .canonical_key()
        .len()
        .checked_add(encoded.as_bytes().len())
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    state.ledger = Some(
        prefix
            .advance(&entry, charge)
            .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?,
    );
    Ok(Some(entry))
}

pub(super) fn load_terminal_hashes(
    storage: &SharedRedbOperationalPorts,
    expected: Option<&StoredApplicationExportOperation>,
    state: &mut ExportState,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    if let Some(StoredApplicationExportOperation::Compact(head)) = expected {
        state.page_hashes = storage
            .verify_application_export_ledger(head)
            .map_err(map_mutation_storage)?;
    }
    Ok(())
}

/// A bounded, page-count-independent size calculation. Hex page hashes contribute
/// at most 69 bytes each inside an escaped manifest string (64 hex bytes, two
/// escaped quotes, one comma). All variable decimal counters are widened to 20
/// digits; every terminal phase/failure is measured, without prior hashes. Six
/// extra bytes cover growth of the body and envelope length varints. The actual
/// encoded head and ledger are charged separately by the closed storage owner.
pub(super) fn terminal_reserve(
    state: &ExportState,
) -> Result<usize, ApplicationExportMutationPortErrorV1> {
    if state.ledger.is_none() || state.phase.is_terminal() {
        return Ok(0);
    }
    let current = body(state)?.len();
    let mut template = state.clone();
    template.page_hashes.clear();
    template.pages_released = u64::MAX;
    template.rows_released = u64::MAX;
    template.bytes_released = u64::MAX;
    template.class_pages = [u64::MAX; 4];
    template.class_rows = [u64::MAX; 4];
    template.class_bytes = [u64::MAX; 4];
    for workflow in &mut template.workflow_quiescence {
        workflow.checked_rows = u64::MAX;
        workflow.quiescent_rows = u64::MAX;
        workflow.non_quiescent_rows = u64::MAX;
    }
    complete_state(&mut template)?;
    let mut terminal = body(&template)?.len();
    for tag in 1..=8 {
        if let Some(failure) = failure_from_tag(tag).ok().flatten() {
            fail_state(&mut template, failure)?;
            terminal = terminal.max(body(&template)?.len());
        }
    }
    let hashes = usize::try_from(state.pages_released)
        .ok()
        .and_then(|pages| pages.checked_mul(69))
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    terminal
        .checked_add(hashes)
        .and_then(|value| value.checked_add(6))
        .map(|value| value.saturating_sub(current))
        .ok_or(ApplicationExportMutationPortErrorV1::LimitExceeded)
}

pub(super) fn check_budget(
    state: &ExportState,
    record: &StoredApplicationExportOperation,
    reserve: usize,
) -> Result<(), ApplicationExportMutationPortErrorV1> {
    if let Some(prefix) = state.ledger {
        let bytes = encode_application_export_head(record)
            .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
        prefix
            .check_budget(16 + bytes.as_bytes().len(), reserve)
            .map_err(|_| ApplicationExportMutationPortErrorV1::LimitExceeded)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// Lost replies are reconciled through immutable reads. Never retry a write to
/// infer success, and never return a released page on a merely matching counter.
pub(super) fn commit(
    storage: &mut impl ApplicationExportLedgerRepository,
    expected: Option<&StoredApplicationExportOperation>,
    replacement: &StoredApplicationExportOperation,
    append: Option<&ApplicationExportPageCommitmentV1>,
    reserve: usize,
) -> Result<ApplicationExportOperationWriteResultV1, StorageError> {
    match storage.compare_and_swap_application_export_head(expected, replacement, append, reserve) {
        Err(error) if error.kind() == StorageErrorKind::CommitStatusUnknown => {
            if storage
                .read_application_export_head(replacement.operation_id())?
                .as_ref()
                != Some(replacement)
            {
                return Err(error);
            }
            if let StoredApplicationExportOperation::Compact(head) = replacement {
                // Complete prefix verification also proves the exact appended
                // member via its domain-separated head commitment.
                storage.verify_application_export_ledger(head)?;
            }
            Ok(ApplicationExportOperationWriteResultV1::Unchanged)
        }
        result => result,
    }
}
