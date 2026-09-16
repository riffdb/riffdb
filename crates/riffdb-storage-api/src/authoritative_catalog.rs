//! Closed storage authority inventory for changelog V3 (ADR-0186).
//!
//! This catalog is not an application configuration or a raw mutation port.
//! Names and classes are fixed by the storage owner. Unknown metadata keys do
//! not inherit a table-wide default. Unproven rebuildability stays authoritative.

use sha2::{Digest, Sha256};

/// Transfer behavior of a replication-control namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationTransferV1 {
    /// Copied exactly at the same lineage fence.
    LineageShared,
    /// Retained only by the source; never follower application authority.
    SourceOnly,
    /// Owned by the follower after durable apply.
    FollowerLocal,
    /// Owned by an offline bootstrap stage, never a serving database.
    Staged,
}

/// One disjoint authority class. Only control state has a transfer selector.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAuthorityClassV1 {
    /// Must compare and transfer exact canonical key/value bytes.
    ReplicatedAuthoritative,
    /// Derived under an accepted rebuild owner; validate before serving.
    RebuildableLocal,
    /// Ordering/lifecycle/replication evidence, excluded from recursive receipts.
    ReplicationControl(ReplicationTransferV1),
}

use ReplicationAuthorityClassV1::{RebuildableLocal, ReplicatedAuthoritative, ReplicationControl};
use ReplicationTransferV1::{FollowerLocal, LineageShared, SourceOnly};

macro_rules! namespaces {
    ($( $name:ident = $tag:literal, $table:literal, $key:expr, $class:expr, $activation:literal; )+) => {
        /// Closed physical table or exact key domain in the mixed metadata table.
        #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
        #[repr(u16)]
        pub enum AuthoritativeNamespaceV1 {
            $(
                #[doc = concat!("The fixed `", $table, "` namespace domain.")]
                $name = $tag,
            )+
        }

        impl AuthoritativeNamespaceV1 {
            /// Every domain, in frozen ascending tag order.
            pub const ALL: [Self; 63] = [$(Self::$name,)+];

            /// Closed namespace tag, distinct from journal or record tags.
            #[must_use]
            pub const fn tag(self) -> u16 { self as u16 }

            /// Exact physical table name.
            #[must_use]
            pub const fn table(self) -> &'static str {
                match self { $(Self::$name => $table,)+ }
            }

            /// Exact singleton key for mixed metadata; no prefix wildcards.
            #[must_use]
            pub const fn metadata_key(self) -> Option<&'static str> {
                match self { $(Self::$name => $key,)+ }
            }

            /// Fixed authority classification; never caller-selectable.
            #[must_use]
            pub const fn class(self) -> ReplicationAuthorityClassV1 {
                match self { $(Self::$name => $class,)+ }
            }

            /// Domains installed only by the receipted V3 activation.
            /// This distinguishes schema inventory stages, not negotiated support.
            #[must_use]
            pub const fn requires_v3_activation(self) -> bool {
                match self { $(Self::$name => $activation,)+ }
            }
        }
    };
}

// Projection rows and apply markers have the accepted ADR-0017 rebuild owner:
// exact historical plan plus contiguous authoritative log, with typed refusal
// when unavailable. Outbox delivery, locators, validated-prefix proofs and
// projection retention controls are NOT inferred rebuildable from their names.
// In particular projection_frontier is a mixed control namespace: its highest
// generation, lifecycle and retention fence cannot be derived from event bytes.
namespaces! {
    ContractBundles = 1, "contract_bundles", None, ReplicatedAuthoritative, false;
    CatalogActive = 2, "catalog_active", None, ReplicatedAuthoritative, false;
    QueryModules = 3, "query_modules", None, ReplicatedAuthoritative, false;
    QueryModuleActive = 4, "query_module_active", None, ReplicatedAuthoritative, false;
    Entities = 5, "entities", None, ReplicatedAuthoritative, false;
    EntityChainHeads = 6, "entity_chain_heads", None, ReplicatedAuthoritative, false;
    SecondaryIndexes = 7, "secondary_indexes", None, ReplicatedAuthoritative, false;
    IndexEpochs = 8, "index_epochs", None, ReplicatedAuthoritative, false;
    Idempotency = 9, "idempotency", None, ReplicatedAuthoritative, false;
    IdempotencyPending = 10, "idempotency_pending", None, ReplicatedAuthoritative, false;
    Commits = 11, "commits", None, ReplicatedAuthoritative, false;
    Provenance = 12, "provenance", None, ReplicatedAuthoritative, false;
    Events = 13, "events", None, ReplicatedAuthoritative, false;
    EventRoutes = 14, "event_routes", None, ReplicatedAuthoritative, false;
    Outbox = 15, "outbox", None, ReplicatedAuthoritative, false;
    OutboxStatus = 16, "outbox_status", None, ReplicatedAuthoritative, false;
    ProjectionState = 17, "projection_state", None, RebuildableLocal, false;
    ProjectionFrontier = 18, "projection_frontier", None, ReplicatedAuthoritative, false;
    ProjectionApplied = 19, "projection_applied", None, RebuildableLocal, false;
    Capabilities = 20, "capabilities", None, ReplicatedAuthoritative, false;
    CapabilityTokens = 21, "capability_tokens", None, ReplicatedAuthoritative, false;
    Audit = 22, "audit", None, ReplicatedAuthoritative, false;
    AuditByRequest = 23, "audit_by_request", None, ReplicatedAuthoritative, false;
    ContractMigrationJournal = 24, "contract_migration_journal", None, ReplicatedAuthoritative, false;
    ContractMigrations = 25, "contract_migrations", None, ReplicatedAuthoritative, false;
    ContractWriteRetirements = 26, "contract_write_retirements", None, ReplicatedAuthoritative, false;
    RetiredEntities = 27, "retired_entities", None, ReplicatedAuthoritative, false;
    HistoryTombstones = 28, "history_tombstones", None, ReplicatedAuthoritative, false;
    ReactiveModules = 29, "reactive_modules", None, ReplicatedAuthoritative, false;
    EventConsumers = 30, "event_consumers", None, ReplicatedAuthoritative, false;
    EventConsumerDeliveries = 31, "event_consumer_deliveries", None, ReplicatedAuthoritative, false;
    ApplicationInstallationCampaigns = 32, "application_installation_campaigns", None, ReplicatedAuthoritative, false;
    ApplicationExportOperations = 33, "application_export_operations", None, ReplicatedAuthoritative, false;
    ValidatedPrefixEntityHeads = 34, "validated_prefix_entity_heads", None, ReplicatedAuthoritative, false;
    VectorEvidence = 35, "vector_evidence", None, ReplicatedAuthoritative, false;
    VectorObservations = 36, "vector_observations", None, ReplicatedAuthoritative, false;
    VectorEvidenceIndex = 37, "vector_evidence_index", None, ReplicatedAuthoritative, false;
    VectorProjectionControls = 38, "vector_projection_controls", None, ReplicatedAuthoritative, false;
    ColumnarProjectionControls = 39, "columnar_projection_controls", None, ReplicatedAuthoritative, false;
    IdempotencyLocators = 40, "idempotency_locators", None, ReplicatedAuthoritative, false;
    ProvenanceLocators = 41, "provenance_locators", None, ReplicatedAuthoritative, false;
    AuditByRequestLocators = 42, "audit_by_request_locators", None, ReplicatedAuthoritative, false;
    ApplicationExportPageCommitments = 43, "application_export_page_commitments", None, ReplicatedAuthoritative, false;
    FormatVersion = 101, "meta", Some("format_version"), ReplicatedAuthoritative, false;
    DatabaseIdentity = 102, "meta", Some("database_id"), ReplicatedAuthoritative, false;
    NextApplicationSequence = 103, "meta", Some("next_application_sequence"), ReplicatedAuthoritative, false;
    NextAdministrationSequence = 104, "meta", Some("next_administration_sequence"), ReplicatedAuthoritative, false;
    CapabilityBootstrap = 105, "meta", Some("capability_bootstrap/v1"), ReplicatedAuthoritative, false;
    RecordRegistry = 106, "meta", Some("record_registry/v2"), ReplicatedAuthoritative, false;
    HistoryIncarnation = 107, "meta", Some("history_incarnation/v1"), ReplicatedAuthoritative, false;
    IndexEpochRowsRepaired = 108, "meta", Some("index_epoch_rows_repaired/v1"), ReplicatedAuthoritative, false;
    ValidatedPrefixCheckpoint = 109, "meta", Some("validated_prefix_checkpoint/v1"), ReplicatedAuthoritative, false;
    RetentionWatermark = 110, "meta", Some("retention_watermark/v1"), ReplicatedAuthoritative, false;
    RetentionHolds = 111, "meta", Some("retention_holds/v1"), ReplicatedAuthoritative, false;
    ChangelogV2RotationReceipt = 112, "meta", Some("changelog_v2_rotation_receipt/v1"), ReplicatedAuthoritative, false;
    CleanCloseLifecycle = 113, "meta", Some("clean_close_certificate/v1"), ReplicationControl(SourceOnly), false;
    AuthoritativeStateCatalog = 201, "meta", Some("authoritative_state_catalog/v1"), ReplicationControl(LineageShared), true;
    LeadershipEpoch = 202, "meta", Some("leadership_epoch/v1"), ReplicationControl(LineageShared), true;
    ChangelogHistoryState = 203, "meta", Some("changelog_history_state/v3"), ReplicationControl(LineageShared), true;
    NextChangelogTransaction = 204, "meta", Some("next_changelog_transaction/v3"), ReplicationControl(LineageShared), true;
    ReplicationFollowerState = 205, "meta", Some("replication_follower_state/v3"), ReplicationControl(FollowerLocal), true;
    ReplicationSourceHolds = 206, "replication_source_holds/v1", None, ReplicationControl(SourceOnly), true;
    ChangelogHistory = 207, "changelog_history/v3", None, ReplicationControl(SourceOnly), true;
}

/// The sole closed declaration; construction never accepts an alternate inventory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AuthoritativeStateCatalogV1;

impl AuthoritativeStateCatalogV1 {
    /// Exact catalog identity carried by activation and V3 frame bindings.
    pub const IDENTITY: &'static str = "riffdb.authoritative-state-catalog/v1";

    /// Resolves a closed tag. Unknown tags are never translated.
    #[must_use]
    pub fn by_tag(self, tag: u16) -> Option<AuthoritativeNamespaceV1> {
        AuthoritativeNamespaceV1::ALL
            .into_iter()
            .find(|namespace| namespace.tag() == tag)
    }

    /// Classifies only a known table or an exact known mixed-table key.
    /// The fixed scan allocates nothing and does not retain caller bytes.
    #[must_use]
    pub fn lookup(self, table: &str, key: &[u8]) -> Option<AuthoritativeNamespaceV1> {
        AuthoritativeNamespaceV1::ALL.into_iter().find(|namespace| {
            namespace.table() == table
                && namespace
                    .metadata_key()
                    .is_none_or(|expected| expected.as_bytes() == key)
        })
    }

    /// Canonical, bounded, value-free catalog fixture; generated from this declaration.
    #[must_use]
    pub fn canonical_fixture(self) -> String {
        let mut result = format!("{}\n", Self::IDENTITY);
        for namespace in AuthoritativeNamespaceV1::ALL {
            let class = match namespace.class() {
                ReplicatedAuthoritative => "replicated-authoritative",
                RebuildableLocal => "rebuildable-local",
                ReplicationControl(LineageShared) => "replication-control:lineage-shared",
                ReplicationControl(SourceOnly) => "replication-control:source-only",
                ReplicationControl(FollowerLocal) => "replication-control:follower-local",
                ReplicationControl(ReplicationTransferV1::Staged) => "replication-control:staged",
            };
            result.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\n",
                namespace.tag(),
                namespace.table(),
                namespace.metadata_key().unwrap_or("-"),
                class,
                if namespace.requires_v3_activation() {
                    "v3-activation"
                } else {
                    "existing"
                }
            ));
        }
        result
    }

    /// SHA-256 of the canonical declaration, including its identity line.
    #[must_use]
    pub fn digest(self) -> [u8; 32] {
        Sha256::digest(self.canonical_fixture().as_bytes()).into()
    }
}
