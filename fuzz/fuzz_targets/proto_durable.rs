#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use riffdb_proto::{durable::current_record_registry, envelope::MAX_STORED_ENVELOPE_BYTES};
use riffdb_storage_api::proto_codec::{
    decode_active_catalog_pointer_v1, decode_administration_sequence_allocator_v1,
    decode_application_sequence_allocator_v1, decode_capability_administration_v1,
    decode_capability_bootstrap_marker_v1, decode_capability_record_v1,
    decode_capability_token_lookup_v1, decode_catalog_administration_v1, decode_commit_record_v1,
    decode_contract_bundle_v1, decode_database_identity_v1, decode_durable_event_v1,
    decode_entity_record_v1, decode_execution_failed_v1, decode_index_entry_v1,
    decode_index_epoch_v1, decode_outbox_intent_v1, decode_outbox_status_v1,
    decode_pending_admission_v1, decode_projection_apply_v1, decode_projection_control_v1,
    decode_projection_state_structural_v1, decode_provenance_record_v1,
    decode_service_audit_record_v1, decode_storage_format_version_v1, decode_stored_outcome_v1,
};

fuzz_target!(|input: &[u8]| {
    if input.len() > MAX_STORED_ENVELOPE_BYTES {
        return;
    }

    let _ = current_record_registry().decode(input);

    let _ = decode_storage_format_version_v1(input);
    let _ = decode_database_identity_v1(input);
    let _ = decode_application_sequence_allocator_v1(input);
    let _ = decode_administration_sequence_allocator_v1(input);
    let _ = decode_contract_bundle_v1(input);
    let _ = decode_active_catalog_pointer_v1(input);
    let _ = decode_catalog_administration_v1(input);
    let _ = decode_entity_record_v1(input);
    let _ = decode_index_entry_v1(input);
    let _ = decode_index_epoch_v1(input);
    let _ = decode_pending_admission_v1(input);
    let _ = decode_execution_failed_v1(input);
    let _ = decode_stored_outcome_v1(input);
    let _ = decode_durable_event_v1(input);
    let _ = decode_outbox_intent_v1(input);
    let _ = decode_provenance_record_v1(input);
    let _ = decode_commit_record_v1(input);
    let _ = decode_capability_record_v1(input);
    let _ = decode_capability_token_lookup_v1(input);
    let _ = decode_capability_bootstrap_marker_v1(input);
    let _ = decode_capability_administration_v1(input);
    let _ = decode_service_audit_record_v1(input);
    let _ = decode_outbox_status_v1(input);
    let _ = decode_projection_state_structural_v1(input);
    let _ = decode_projection_apply_v1(input);
    let _ = decode_projection_control_v1(input);
});
