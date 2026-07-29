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
pub(crate) const SECONDARY_INDEXES: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("secondary_indexes");
pub(crate) const INDEX_EPOCHS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("index_epochs");
pub(crate) const IDEMPOTENCY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("idempotency");
pub(crate) const IDEMPOTENCY_PENDING: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("idempotency_pending");
pub(crate) const COMMITS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("commits");
pub(crate) const PROVENANCE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("provenance");
pub(crate) const EVENTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("events");
pub(crate) const OUTBOX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outbox");
pub(crate) const OUTBOX_STATUS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("outbox_status");
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

pub(crate) const TABLE_NAMES: [&str; 21] = [
    "meta",
    "contract_bundles",
    "catalog_active",
    "query_modules",
    "query_module_active",
    "entities",
    "secondary_indexes",
    "index_epochs",
    "idempotency",
    "idempotency_pending",
    "commits",
    "provenance",
    "events",
    "outbox",
    "outbox_status",
    "projection_state",
    "projection_frontier",
    "projection_applied",
    "capabilities",
    "capability_tokens",
    "audit",
];

pub(crate) const META_FORMAT_VERSION: &str = "format_version";
pub(crate) const META_DATABASE_ID: &str = "database_id";
pub(crate) const META_APPLICATION_SEQUENCE: &str = "next_application_sequence";
pub(crate) const META_ADMINISTRATION_SEQUENCE: &str = "next_administration_sequence";
pub(crate) const META_CAPABILITY_BOOTSTRAP: &str = "capability_bootstrap/v1";

pub(crate) const META_KEYS: [&str; 5] = [
    META_FORMAT_VERSION,
    META_DATABASE_ID,
    META_APPLICATION_SEQUENCE,
    META_ADMINISTRATION_SEQUENCE,
    META_CAPABILITY_BOOTSTRAP,
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
    drop(tx.open_table(SECONDARY_INDEXES)?);
    drop(tx.open_table(INDEX_EPOCHS)?);
    drop(tx.open_table(IDEMPOTENCY)?);
    drop(tx.open_table(IDEMPOTENCY_PENDING)?);
    drop(tx.open_table(COMMITS)?);
    drop(tx.open_table(PROVENANCE)?);
    drop(tx.open_table(EVENTS)?);
    drop(tx.open_table(OUTBOX)?);
    drop(tx.open_table(OUTBOX_STATUS)?);
    drop(tx.open_table(PROJECTION_STATE)?);
    drop(tx.open_table(PROJECTION_FRONTIER)?);
    drop(tx.open_table(PROJECTION_APPLIED)?);
    drop(tx.open_table(CAPABILITIES)?);
    drop(tx.open_table(CAPABILITY_TOKENS)?);
    drop(tx.open_table(AUDIT)?);
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
            SECONDARY_INDEXES.name(),
            INDEX_EPOCHS.name(),
            IDEMPOTENCY.name(),
            IDEMPOTENCY_PENDING.name(),
            COMMITS.name(),
            PROVENANCE.name(),
            EVENTS.name(),
            OUTBOX.name(),
            OUTBOX_STATUS.name(),
            PROJECTION_STATE.name(),
            PROJECTION_FRONTIER.name(),
            PROJECTION_APPLIED.name(),
            CAPABILITIES.name(),
            CAPABILITY_TOKENS.name(),
            AUDIT.name(),
        ];

        assert_eq!(definition_names, TABLE_NAMES);
        assert_eq!(TABLE_NAMES.len(), 21);
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
            ]
        );
        assert_eq!(META_KEYS.len(), 5);
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
