//! Offline operator evidence using the existing validated archive owner.

use riffdb_contract_ir::ContractBundle;
use riffdb_storage_api::{
    DatabaseIdentityProbe, DatabaseIdentityProbePort, LocalOutboxArchiveRecordV1,
};
use riffdb_types::{CanonicalRecord, CanonicalString, CanonicalValue, EntityKeyBuilder};

use super::{Result, model, witness};

fn uuid(kind: u16, ordinal: u64) -> [u8; 16] {
    let mut bytes = [0x01, 0x90, 0, 0, 0, 0, 0x70, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    bytes[8..10].copy_from_slice(&kind.to_be_bytes());
    bytes[10..].copy_from_slice(&ordinal.to_be_bytes()[2..]);
    bytes
}

fn validate(
    record: &LocalOutboxArchiveRecordV1,
    row: &witness::Acknowledged,
    bundle: &ContractBundle,
    created: bool,
) -> Result<()> {
    let ticket = row.ordinal % model::POPULATION;
    let organization = ticket / super::TICKETS_PER_ORGANIZATION;
    let revision = if created {
        0
    } else {
        row.ordinal / model::POPULATION + 1
    };
    let event = bundle
        .schema()
        .events()
        .iter()
        .find(|event| {
            event.name()
                == if created {
                    "TicketCreated"
                } else {
                    "TicketUpdated"
                }
        })
        .ok_or("archive_event_schema")?;
    let status = bundle
        .schema()
        .enums()
        .iter()
        .find(|value| value.name() == "TicketStatus")
        .ok_or("archive_status_schema")?;
    let variant = status
        .variants()
        .iter()
        .find(|variant| variant.name() == model::status(revision))
        .ok_or("archive_status_schema")?;
    let mut fields = Vec::with_capacity(8);
    for field in event.payload().fields() {
        let value = match field.name() {
            "organization_id" => CanonicalValue::Uuid(uuid(0x8001, organization)),
            "ticket_id" => CanonicalValue::Uuid(uuid(0x8004, ticket)),
            "project_id" => CanonicalValue::Uuid(uuid(0x8003, organization)),
            "reporter_id" | "assignee_id" => CanonicalValue::Uuid(uuid(0x8002, organization)),
            "title" => CanonicalValue::String(
                CanonicalString::new(format!("Lifecycle ticket {ticket}"))
                    .map_err(|_| "archive_payload")?,
            ),
            "revision" => CanonicalValue::U64(revision),
            "status" => CanonicalValue::Enum {
                type_id: status.id(),
                variant_id: variant.id(),
            },
            "increment" => CanonicalValue::I64(1),
            _ => return Err("archive_event_schema"),
        };
        fields.push((field.id(), value));
    }
    let payload = CanonicalRecord::new(fields).map_err(|_| "archive_payload")?;
    let stored = record.event();
    if stored.event_type_id() != event.id()
        || stored.payload() != &payload
        || record.history_incarnation().get() != row.incarnation
    {
        return Err("archive_reconciliation");
    }
    let (anchor, declared) = match (stored.policy_anchor(), event.policy_anchor()) {
        (None, None) => return Ok(()),
        (Some(anchor), Some(declared)) => (anchor, declared),
        _ => return Err("archive_policy_anchor"),
    };
    let mut key = EntityKeyBuilder::new(declared.source_entity());
    key.push_uuid(&uuid(0x8001, organization))
        .map_err(|_| "archive_policy_anchor")?;
    key.push_uuid(&uuid(0x8004, ticket))
        .map_err(|_| "archive_policy_anchor")?;
    let key = key.finish().map_err(|_| "archive_policy_anchor")?;
    if anchor.contract().lineage().as_str() != "TicketDeskLifecycle"
        || anchor.contract().contract_version().get() != row.version
        || anchor.contract().bundle_hash() != bundle.bundle_hash()
        || anchor.source().entity_type_id() != declared.source_entity()
        || anchor.source().key() != &key
        || anchor.read_policy().as_str() != declared.read_policy()
    {
        return Err("archive_reconciliation");
    }
    Ok(())
}

pub(super) fn run() -> Result<()> {
    let root = std::env::var_os("RIFFDB_LIFECYCLE_WITNESS_ROOT").ok_or("archive_configuration")?;
    let root = std::path::PathBuf::from(root);
    if !root.is_absolute() || root.is_symlink() || !root.is_dir() {
        return Err("archive_configuration");
    }
    let database = root.join("data/ticketdesk.redb");
    if database.is_symlink() || !database.is_file() {
        return Err("archive_source_identity");
    }
    // Dormant source-free identity probing. No command runtime or initialization.
    let store =
        riffdb_storage_redb::RedbStore::open(database).map_err(|_| "archive_source_custody")?;
    let DatabaseIdentityProbe::Existing(binding) = store
        .probe_database_identity()
        .map_err(|_| "archive_source_identity")?
    else {
        return Err("archive_source_identity");
    };
    drop(store);
    let mut archive =
        riffdb_outbox::DurableLocalArchive::open_existing(&root.join("archive"), binding)
            .map_err(|_| "archive_custody")?;
    let ceiling = model::MAX_COMMANDS + model::POPULATION;
    let observation = archive.observation().map_err(|_| "archive_integrity")?;
    if observation.records() > ceiling {
        return Err("archive_record_bound");
    }
    let mut complete = false;
    // Every incomplete native page must consume at least one bounded record.
    for _ in 0..=observation.records() {
        if archive
            .validate_next_page()
            .map_err(|_| "archive_integrity")?
        {
            complete = true;
            break;
        }
    }
    if !complete {
        return Err("archive_validation_bound");
    }
    let parent = riffdb_contract_compiler::compile_contract_source(include_str!(
        "../../lifecycle/riffdb/contract.riff"
    ))
    .map_err(|_| "archive_contract")?;
    let successor = riffdb_contract_compiler::compile_contract_successor(
        include_str!("../../../../fixtures/ticketdesk_lifecycle/additive-v2.riff"),
        &parent,
    )
    .map_err(|_| "archive_contract")?;
    if parent.bundle_hash().as_bytes() != &super::application::CONTRACT_BUNDLE_HASH
        || successor.bundle_hash().as_bytes() != &super::application_v2::CONTRACT_BUNDLE_HASH
    {
        return Err("archive_contract_identity");
    }
    let mut checked = 0_u64;
    for (name, created) in [("seed.witness", true), ("updates.witness", false)] {
        let mut reader = witness::Reader::open(&root.join(name))?;
        while let Some(row) = reader.next()? {
            if created
                && (row.ordinal >= model::POPULATION || row.version != 1 || row.incarnation != 1)
            {
                return Err("archive_seed_identity");
            }
            let mut key = [0; 36];
            key[..16].copy_from_slice(binding.as_bytes());
            key[16..24].copy_from_slice(&row.incarnation.to_be_bytes());
            key[24..32].copy_from_slice(&row.sequence.to_be_bytes());
            let record = archive
                .read(&key)
                .map_err(|_| "archive_integrity")?
                .ok_or("archive_missing_event")?;
            let bundle = if row.version == 1 {
                &parent
            } else {
                &successor
            };
            validate(&record, &row, bundle, created)?;
            checked = checked.checked_add(1).ok_or("archive_record_bound")?;
        }
    }
    if checked != observation.records() {
        return Err("archive_unaccounted_event");
    }
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-archive-reconciliation/v1\",\"records_checked\":{checked},\"encoded_bytes\":{},\"physical_bytes\":{},\"payloads_reconciled\":true,\"policy_anchors_reconciled\":true}}",
        observation.encoded_bytes(),
        archive.physical_bytes().map_err(|_| "archive_custody")?
    );
    Ok(())
}
