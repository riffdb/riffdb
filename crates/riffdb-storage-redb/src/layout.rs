//! Frozen redb table and singleton-key layout for the POC storage format.

use redb::{TableDefinition, TableError, WriteTransaction};

pub(crate) const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
pub(crate) const CONTRACT_BUNDLES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_bundles");
pub(crate) const CATALOG_ACTIVE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("catalog_active");
pub(crate) const QUERY_MODULES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("query_modules");
pub(crate) const QUERY_MODULE_ACTIVE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("query_module_active");
pub(crate) const ENTITIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("entities");
pub(crate) const ENTITY_CHAIN_HEADS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("entity_chain_heads");
/// Exact entity-chain heads anchored by the active validated-prefix checkpoint.
///
/// Values reuse the frozen `StoredEntityChainHeadV1` encoding. The table is
/// replaced atomically with `META_VALIDATED_PREFIX_CHECKPOINT`; it is proof
/// material, never application-visible authoritative current state.
pub(crate) const VALIDATED_PREFIX_ENTITY_HEADS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("validated_prefix_entity_heads");
pub(crate) const SECONDARY_INDEXES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("secondary_indexes");
pub(crate) const INDEX_EPOCHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index_epochs");
pub(crate) const IDEMPOTENCY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("idempotency");
pub(crate) const IDEMPOTENCY_PENDING: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("idempotency_pending");
pub(crate) const COMMITS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("commits");
pub(crate) const PROVENANCE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("provenance");
pub(crate) const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("events");
pub(crate) const EVENT_ROUTES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("event_routes");
pub(crate) const OUTBOX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outbox");
pub(crate) const OUTBOX_STATUS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("outbox_status");
/// ADR-0165 durable command-derived locators.
///
/// A command's outcome, provenance and audits live inside its command segment
/// in `COMMITS`, which is keyed by commit sequence, so an idempotency identity
/// key, a provenance id and an audit request id had no durable path to their
/// owning segment. These tables supply it.
///
/// They are deliberately NOT rows inside `IDEMPOTENCY`, `PROVENANCE` and
/// `AUDIT_BY_REQUEST`: ADR-0085 derives its O(1) validated-prefix checkpoint
/// counts from those tables' raw row counts, so a locator row there becomes a
/// third counted class and silently corrupts the checkpoint.
pub(crate) const IDEMPOTENCY_LOCATORS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("idempotency_locators");
pub(crate) const PROVENANCE_LOCATORS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("provenance_locators");
pub(crate) const AUDIT_BY_REQUEST_LOCATORS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("audit_by_request_locators");
pub(crate) const PROJECTION_STATE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("projection_state");
pub(crate) const PROJECTION_FRONTIER: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("projection_frontier");
pub(crate) const PROJECTION_APPLIED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("projection_applied");
pub(crate) const CAPABILITIES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("capabilities");
pub(crate) const CAPABILITY_TOKENS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("capability_tokens");
pub(crate) const AUDIT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("audit");
pub(crate) const AUDIT_BY_REQUEST: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("audit_by_request");
pub(crate) const CONTRACT_MIGRATION_JOURNAL: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_migration_journal");
pub(crate) const CONTRACT_MIGRATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_migrations");
pub(crate) const CONTRACT_WRITE_RETIREMENTS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("contract_write_retirements");
pub(crate) const RETIRED_ENTITIES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("retired_entities");
pub(crate) const HISTORY_TOMBSTONES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("history_tombstones");
pub(crate) const REACTIVE_MODULES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("reactive_modules");
pub(crate) const EVENT_CONSUMERS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("event_consumers");
pub(crate) const EVENT_CONSUMER_DELIVERIES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("event_consumer_deliveries");
pub(crate) const APPLICATION_INSTALLATION_CAMPAIGNS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("application_installation_campaigns");
pub(crate) const APPLICATION_EXPORT_OPERATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("application_export_operations");
/// Authoritative per-entity vector source/model evidence (ADR-0136).
pub(crate) const VECTOR_EVIDENCE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_evidence");
/// Authoritative maintained vector counts (ADR-0136).
pub(crate) const VECTOR_OBSERVATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_observations");
/// Authoritative partition-ordered vector evidence index (ADR-0136).
pub(crate) const VECTOR_EVIDENCE_INDEX: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_evidence_index");
/// Authoritative vector projection lifecycle and retention controls (ADR-0136).
pub(crate) const VECTOR_PROJECTION_CONTROLS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_projection_controls");
/// Sole schema-bound scalar/vector columnar selector and retention fence (ADR-0192).
pub(crate) const COLUMNAR_PROJECTION_CONTROLS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("columnar_projection_controls");

pub(crate) const TABLE_NAMES: [&str; 43] = [
    "meta",
    "contract_bundles",
    "catalog_active",
    "query_modules",
    "query_module_active",
    "entities",
    "entity_chain_heads",
    "secondary_indexes",
    "index_epochs",
    "idempotency",
    "idempotency_pending",
    "commits",
    "provenance",
    "events",
    "event_routes",
    "outbox",
    "outbox_status",
    "projection_state",
    "projection_frontier",
    "projection_applied",
    "capabilities",
    "capability_tokens",
    "audit",
    "audit_by_request",
    "contract_migration_journal",
    "contract_migrations",
    "contract_write_retirements",
    "retired_entities",
    "history_tombstones",
    "reactive_modules",
    "event_consumers",
    "event_consumer_deliveries",
    "application_installation_campaigns",
    "application_export_operations",
    "validated_prefix_entity_heads",
    "vector_evidence",
    "vector_observations",
    "vector_evidence_index",
    "vector_projection_controls",
    "columnar_projection_controls",
    "idempotency_locators",
    "provenance_locators",
    "audit_by_request_locators",
];

pub(crate) const BYTE_TABLES: [TableDefinition<&[u8], &[u8]>; 38] = [
    CONTRACT_BUNDLES,
    CATALOG_ACTIVE,
    QUERY_MODULES,
    QUERY_MODULE_ACTIVE,
    ENTITIES,
    ENTITY_CHAIN_HEADS,
    SECONDARY_INDEXES,
    INDEX_EPOCHS,
    IDEMPOTENCY,
    IDEMPOTENCY_PENDING,
    COMMITS,
    PROVENANCE,
    EVENTS,
    EVENT_ROUTES,
    OUTBOX,
    OUTBOX_STATUS,
    PROJECTION_STATE,
    PROJECTION_FRONTIER,
    PROJECTION_APPLIED,
    CAPABILITIES,
    CAPABILITY_TOKENS,
    AUDIT,
    AUDIT_BY_REQUEST,
    CONTRACT_MIGRATION_JOURNAL,
    CONTRACT_MIGRATIONS,
    CONTRACT_WRITE_RETIREMENTS,
    RETIRED_ENTITIES,
    HISTORY_TOMBSTONES,
    REACTIVE_MODULES,
    EVENT_CONSUMERS,
    EVENT_CONSUMER_DELIVERIES,
    APPLICATION_INSTALLATION_CAMPAIGNS,
    APPLICATION_EXPORT_OPERATIONS,
    VECTOR_EVIDENCE,
    VECTOR_OBSERVATIONS,
    VECTOR_EVIDENCE_INDEX,
    VECTOR_PROJECTION_CONTROLS,
    COLUMNAR_PROJECTION_CONTROLS,
];

pub(crate) const META_FORMAT_VERSION: &str = "format_version";
pub(crate) const META_DATABASE_ID: &str = "database_id";
pub(crate) const META_APPLICATION_SEQUENCE: &str = "next_application_sequence";
pub(crate) const META_ADMINISTRATION_SEQUENCE: &str = "next_administration_sequence";
pub(crate) const META_CAPABILITY_BOOTSTRAP: &str = "capability_bootstrap/v1";
pub(crate) const META_RECORD_REGISTRY: &str = "record_registry/v2";
pub(crate) const META_HISTORY_INCARNATION: &str = "history_incarnation/v1";
/// One-shot marker: legacy INDEX_EPOCHS prefix-keyed rows have been repaired
/// (or proven absent) under the current registry digest. Optional; not required
/// on open. Written after a successful `migrate_partition_index_generations`
/// while the digest is current so reopen can skip the full secondary-index scan.
pub(crate) const META_INDEX_EPOCH_ROWS_REPAIRED: &str = "index_epoch_rows_repaired/v1";
/// Optional proof-carrying validated-prefix startup checkpoint (ADR-0085 A1).
pub(crate) const META_VALIDATED_PREFIX_CHECKPOINT: &str = "validated_prefix_checkpoint/v1";
/// Bound retention watermark (ADR-0085 Amendment 2). Optional; absent means sequence 0.
pub(crate) const META_RETENTION_WATERMARK: &str = "retention_watermark/v1";
/// Operator retention holds (ADR-0085 Amendment 2). Optional; absent means empty holds.
pub(crate) const META_RETENTION_HOLDS: &str = "retention_holds/v1";
/// One-shot binding from the V1 changelog frontier to delete-aware V2 state.
pub(crate) const META_CHANGELOG_V2_ROTATION_RECEIPT: &str = "changelog_v2_rotation_receipt/v1";
/// Private clean-close lifecycle evidence (ADR-0157).
pub(crate) const META_CLEAN_CLOSE_LIFECYCLE: &str = "clean_close_certificate/v1";

pub(crate) const META_KEYS: [&str; 13] = [
    META_FORMAT_VERSION,
    META_DATABASE_ID,
    META_APPLICATION_SEQUENCE,
    META_ADMINISTRATION_SEQUENCE,
    META_CAPABILITY_BOOTSTRAP,
    META_RECORD_REGISTRY,
    META_HISTORY_INCARNATION,
    META_INDEX_EPOCH_ROWS_REPAIRED,
    META_VALIDATED_PREFIX_CHECKPOINT,
    META_RETENTION_WATERMARK,
    META_RETENTION_HOLDS,
    META_CHANGELOG_V2_ROTATION_RECEIPT,
    META_CLEAN_CLOSE_LIFECYCLE,
];

#[allow(dead_code, reason = "WP-070 catalog ports consume this frozen key")]
pub(crate) const CATALOG_ACTIVE_KEY: [u8; 1] = [0x01];

pub(crate) fn create_all_tables(tx: &WriteTransaction) -> Result<(), TableError> {
    drop(tx.open_table(META)?);
    drop(tx.open_table(CONTRACT_BUNDLES)?);
    drop(tx.open_table(CATALOG_ACTIVE)?);
    drop(tx.open_table(QUERY_MODULES)?);
    drop(tx.open_table(QUERY_MODULE_ACTIVE)?);
    drop(tx.open_table(ENTITIES)?);
    drop(tx.open_table(ENTITY_CHAIN_HEADS)?);
    drop(tx.open_table(VALIDATED_PREFIX_ENTITY_HEADS)?);
    drop(tx.open_table(SECONDARY_INDEXES)?);
    drop(tx.open_table(INDEX_EPOCHS)?);
    drop(tx.open_table(IDEMPOTENCY)?);
    drop(tx.open_table(IDEMPOTENCY_PENDING)?);
    drop(tx.open_table(COMMITS)?);
    drop(tx.open_table(PROVENANCE)?);
    drop(tx.open_table(EVENTS)?);
    drop(tx.open_table(EVENT_ROUTES)?);
    drop(tx.open_table(OUTBOX)?);
    drop(tx.open_table(OUTBOX_STATUS)?);
    drop(tx.open_table(PROJECTION_STATE)?);
    drop(tx.open_table(PROJECTION_FRONTIER)?);
    drop(tx.open_table(PROJECTION_APPLIED)?);
    drop(tx.open_table(CAPABILITIES)?);
    drop(tx.open_table(CAPABILITY_TOKENS)?);
    drop(tx.open_table(AUDIT)?);
    drop(tx.open_table(AUDIT_BY_REQUEST)?);
    drop(tx.open_table(CONTRACT_MIGRATION_JOURNAL)?);
    drop(tx.open_table(CONTRACT_MIGRATIONS)?);
    drop(tx.open_table(CONTRACT_WRITE_RETIREMENTS)?);
    drop(tx.open_table(RETIRED_ENTITIES)?);
    drop(tx.open_table(HISTORY_TOMBSTONES)?);
    drop(tx.open_table(REACTIVE_MODULES)?);
    drop(tx.open_table(EVENT_CONSUMERS)?);
    drop(tx.open_table(EVENT_CONSUMER_DELIVERIES)?);
    drop(tx.open_table(APPLICATION_INSTALLATION_CAMPAIGNS)?);
    drop(tx.open_table(APPLICATION_EXPORT_OPERATIONS)?);
    drop(tx.open_table(VECTOR_EVIDENCE)?);
    drop(tx.open_table(VECTOR_OBSERVATIONS)?);
    drop(tx.open_table(VECTOR_EVIDENCE_INDEX)?);
    drop(tx.open_table(VECTOR_PROJECTION_CONTROLS)?);
    drop(tx.open_table(COLUMNAR_PROJECTION_CONTROLS)?);
    drop(tx.open_table(IDEMPOTENCY_LOCATORS)?);
    drop(tx.open_table(PROVENANCE_LOCATORS)?);
    drop(tx.open_table(AUDIT_BY_REQUEST_LOCATORS)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use redb::TableHandle;

    use super::*;

    #[test]
    fn table_definition_inventory_is_exact_and_unique() {
        let definition_names = [
            META.name(),
            CONTRACT_BUNDLES.name(),
            CATALOG_ACTIVE.name(),
            QUERY_MODULES.name(),
            QUERY_MODULE_ACTIVE.name(),
            ENTITIES.name(),
            ENTITY_CHAIN_HEADS.name(),
            SECONDARY_INDEXES.name(),
            INDEX_EPOCHS.name(),
            IDEMPOTENCY.name(),
            IDEMPOTENCY_PENDING.name(),
            COMMITS.name(),
            PROVENANCE.name(),
            EVENTS.name(),
            EVENT_ROUTES.name(),
            OUTBOX.name(),
            OUTBOX_STATUS.name(),
            PROJECTION_STATE.name(),
            PROJECTION_FRONTIER.name(),
            PROJECTION_APPLIED.name(),
            CAPABILITIES.name(),
            CAPABILITY_TOKENS.name(),
            AUDIT.name(),
            AUDIT_BY_REQUEST.name(),
            CONTRACT_MIGRATION_JOURNAL.name(),
            CONTRACT_MIGRATIONS.name(),
            CONTRACT_WRITE_RETIREMENTS.name(),
            RETIRED_ENTITIES.name(),
            HISTORY_TOMBSTONES.name(),
            REACTIVE_MODULES.name(),
            EVENT_CONSUMERS.name(),
            EVENT_CONSUMER_DELIVERIES.name(),
            APPLICATION_INSTALLATION_CAMPAIGNS.name(),
            APPLICATION_EXPORT_OPERATIONS.name(),
            VALIDATED_PREFIX_ENTITY_HEADS.name(),
            VECTOR_EVIDENCE.name(),
            VECTOR_OBSERVATIONS.name(),
            VECTOR_EVIDENCE_INDEX.name(),
            VECTOR_PROJECTION_CONTROLS.name(),
            COLUMNAR_PROJECTION_CONTROLS.name(),
            IDEMPOTENCY_LOCATORS.name(),
            PROVENANCE_LOCATORS.name(),
            AUDIT_BY_REQUEST_LOCATORS.name(),
        ];

        assert_eq!(definition_names, TABLE_NAMES);
        assert_eq!(TABLE_NAMES.len(), 43);
        assert_eq!(
            TABLE_NAMES.into_iter().collect::<BTreeSet<_>>().len(),
            TABLE_NAMES.len()
        );
    }

    #[test]
    fn retained_meta_key_inventory_is_exact_and_unique() {
        assert_eq!(
            META_KEYS,
            [
                "format_version",
                "database_id",
                "next_application_sequence",
                "next_administration_sequence",
                "capability_bootstrap/v1",
                "record_registry/v2",
                "history_incarnation/v1",
                "index_epoch_rows_repaired/v1",
                "validated_prefix_checkpoint/v1",
                "retention_watermark/v1",
                "retention_holds/v1",
                "changelog_v2_rotation_receipt/v1",
                "clean_close_certificate/v1",
            ]
        );
        assert_eq!(META_KEYS.len(), 13);
        assert_eq!(
            META_KEYS.into_iter().collect::<BTreeSet<_>>().len(),
            META_KEYS.len()
        );
    }

    #[test]
    fn active_catalog_uses_the_singleton_key() {
        assert_eq!(CATALOG_ACTIVE_KEY, [0x01]);
    }
}
