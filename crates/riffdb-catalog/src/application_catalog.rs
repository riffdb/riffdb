//! Closed symbolic application-catalog response types.

use std::error::Error;
use std::fmt;

use riffdb_types::{ContractBundleHash, ContractLineage, ContractVersion, QueryModuleHash};

/// Versioned public schema name for symbolic application-catalog pages.
pub const APPLICATION_CATALOG_SCHEMA_V1: &str = "riffdb.application-catalog/v1";
/// Hard maximum visible symbols in one catalog page.
pub const MAX_APPLICATION_CATALOG_PAGE_ITEMS: usize = 100;
/// Maximum components in one public symbolic path.
pub const MAX_APPLICATION_CATALOG_PATH_COMPONENTS: usize = 8;
/// Maximum UTF-8 bytes in one public path component or rendered type.
pub const MAX_APPLICATION_CATALOG_TEXT_BYTES: usize = 256;

/// Closed public symbol classes. Numeric compiler/storage identities have no variant.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApplicationCatalogSymbolKindV1 {
    /// Selected contract lineage/version.
    Contract,
    /// Public enumeration declaration.
    Enum,
    /// Public entity declaration.
    Entity,
    /// Visible entity field.
    Field,
    /// Visible declared relationship.
    Relationship,
    /// Visible declared index.
    Index,
    /// Invocable compiled command.
    Command,
    /// Declared command outcome.
    CommandOutcome,
    /// Streamable domain event.
    Event,
    /// Visible immutable query module.
    QueryModule,
    /// Invocable named query.
    Query,
    /// Bindable application role.
    Role,
    /// Generated application operation schema.
    Operation,
}

impl ApplicationCatalogSymbolKindV1 {
    /// Complete stable registry in wire order.
    pub const ALL: [Self; 13] = [
        Self::Contract,
        Self::Enum,
        Self::Entity,
        Self::Field,
        Self::Relationship,
        Self::Index,
        Self::Command,
        Self::CommandOutcome,
        Self::Event,
        Self::QueryModule,
        Self::Query,
        Self::Role,
        Self::Operation,
    ];

    /// Stable wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contract => "contract",
            Self::Enum => "enum",
            Self::Entity => "entity",
            Self::Field => "field",
            Self::Relationship => "relationship",
            Self::Index => "index",
            Self::Command => "command",
            Self::CommandOutcome => "command_outcome",
            Self::Event => "event",
            Self::QueryModule => "query_module",
            Self::Query => "query",
            Self::Role => "role",
            Self::Operation => "operation",
        }
    }
}

/// Closed feature-preflight vocabulary. It reports product semantics, never internals.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApplicationCatalogFeatureV1 {
    /// Finite optional-parameter plan families.
    OperationalOptionalPredicates,
    /// Snapshot-bound stable cursor pages.
    StableCursorPages,
    /// Declared missing/null/non-null discriminator predicates.
    NullExistencePredicates,
    /// Exact binary UTF-8 text-key prefix lookup.
    BinaryTextPrefix,
    /// Frozen Unicode normalization-and-fold text-key prefix lookup.
    UnicodeFoldTextPrefixV1,
    /// Shared exact count/sum/min/max aggregate algebra.
    ExactAggregates,
}

impl ApplicationCatalogFeatureV1 {
    /// Complete stable feature registry in wire order.
    pub const ALL: [Self; 6] = [
        Self::OperationalOptionalPredicates,
        Self::StableCursorPages,
        Self::NullExistencePredicates,
        Self::BinaryTextPrefix,
        Self::UnicodeFoldTextPrefixV1,
        Self::ExactAggregates,
    ];

    /// Stable wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OperationalOptionalPredicates => "operational_optional_predicates",
            Self::StableCursorPages => "stable_cursor_pages",
            Self::NullExistencePredicates => "null_existence_predicates",
            Self::BinaryTextPrefix => "binary_text_prefix",
            Self::UnicodeFoldTextPrefixV1 => "unicode_fold_text_prefix_v1",
            Self::ExactAggregates => "exact_aggregates",
        }
    }
}

/// Availability of one closed feature under the selected exact application identity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApplicationCatalogFeatureStateV1 {
    /// The selected compiler/runtime surface implements the feature.
    Available,
    /// The feature is understood but unavailable for this exact application surface.
    Unavailable,
}

/// Source-relative byte span. No filesystem path or source text is carried.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationCatalogSourceSpanV1 {
    start: u32,
    end: u32,
}

impl ApplicationCatalogSourceSpanV1 {
    /// Checks a nonempty bounded byte span.
    pub const fn checked(start: u32, end: u32) -> Option<Self> {
        if start < end {
            Some(Self { start, end })
        } else {
            None
        }
    }

    /// Inclusive start byte.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Exclusive end byte.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.end
    }
}

/// One authorized name-only catalog symbol.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationCatalogSymbolV1 {
    kind: ApplicationCatalogSymbolKindV1,
    path: Vec<String>,
    public_type: Option<String>,
    source_span: Option<ApplicationCatalogSourceSpanV1>,
}

impl ApplicationCatalogSymbolV1 {
    /// Validates one bounded symbolic result after authorization filtering.
    pub fn checked(
        kind: ApplicationCatalogSymbolKindV1,
        path: Vec<String>,
        public_type: Option<String>,
        source_span: Option<ApplicationCatalogSourceSpanV1>,
    ) -> Result<Self, ApplicationCatalogError> {
        if path.is_empty()
            || path.len() > MAX_APPLICATION_CATALOG_PATH_COMPONENTS
            || path.iter().any(|component| !valid_symbol(component))
            || public_type.as_ref().is_some_and(|value| {
                value.is_empty() || value.len() > MAX_APPLICATION_CATALOG_TEXT_BYTES
            })
        {
            return Err(ApplicationCatalogError);
        }
        Ok(Self {
            kind,
            path,
            public_type,
            source_span,
        })
    }

    /// Symbol class.
    #[must_use]
    pub const fn kind(&self) -> ApplicationCatalogSymbolKindV1 {
        self.kind
    }

    /// Fully symbolic path such as `Ticket.title`.
    #[must_use]
    pub fn path(&self) -> &[String] {
        &self.path
    }

    /// Public type spelling, when the symbol has one.
    #[must_use]
    pub fn public_type(&self) -> Option<&str> {
        self.public_type.as_deref()
    }

    /// Source-relative span, when public for this declaration.
    #[must_use]
    pub const fn source_span(&self) -> Option<ApplicationCatalogSourceSpanV1> {
        self.source_span
    }
}

/// One feature observation for the selected exact application surface.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationCatalogFeatureViewV1 {
    feature: ApplicationCatalogFeatureV1,
    state: ApplicationCatalogFeatureStateV1,
}

impl ApplicationCatalogFeatureViewV1 {
    /// Constructs one closed feature observation.
    #[must_use]
    pub const fn new(
        feature: ApplicationCatalogFeatureV1,
        state: ApplicationCatalogFeatureStateV1,
    ) -> Self {
        Self { feature, state }
    }

    /// Feature.
    #[must_use]
    pub const fn feature(self) -> ApplicationCatalogFeatureV1 {
        self.feature
    }

    /// Availability.
    #[must_use]
    pub const fn state(self) -> ApplicationCatalogFeatureStateV1 {
        self.state
    }
}

/// Exact immutable application identity bound to one catalog page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogIdentityV1 {
    lineage: ContractLineage,
    version: ContractVersion,
    contract_hash: ContractBundleHash,
    module_hashes: Vec<QueryModuleHash>,
}

impl ApplicationCatalogIdentityV1 {
    /// Checks canonically ordered unique module identities.
    pub fn checked(
        lineage: ContractLineage,
        version: ContractVersion,
        contract_hash: ContractBundleHash,
        module_hashes: Vec<QueryModuleHash>,
    ) -> Result<Self, ApplicationCatalogError> {
        if module_hashes.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ApplicationCatalogError);
        }
        Ok(Self {
            lineage,
            version,
            contract_hash,
            module_hashes,
        })
    }

    /// Contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Exact contract bundle identity.
    #[must_use]
    pub const fn contract_hash(&self) -> ContractBundleHash {
        self.contract_hash
    }

    /// Exact visible module identities.
    #[must_use]
    pub fn module_hashes(&self) -> &[QueryModuleHash] {
        &self.module_hashes
    }
}

/// One bounded authorization-filtered symbolic catalog page.
///
/// There is deliberately no total/hidden count, visibility bit, numeric ID,
/// raw plan, or storage identity. Unauthorized and nonexistent symbols are both
/// represented only by absence from `symbols`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogPageV1 {
    identity: ApplicationCatalogIdentityV1,
    symbols: Vec<ApplicationCatalogSymbolV1>,
    features: Vec<ApplicationCatalogFeatureViewV1>,
    has_more: bool,
}

impl ApplicationCatalogPageV1 {
    /// Checks deterministic ordering, uniqueness, and page bounds.
    pub fn checked(
        identity: ApplicationCatalogIdentityV1,
        symbols: Vec<ApplicationCatalogSymbolV1>,
        features: Vec<ApplicationCatalogFeatureViewV1>,
        has_more: bool,
    ) -> Result<Self, ApplicationCatalogError> {
        if symbols.len() > MAX_APPLICATION_CATALOG_PAGE_ITEMS
            || symbols.windows(2).any(|pair| pair[0] >= pair[1])
            || features.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(ApplicationCatalogError);
        }
        Ok(Self {
            identity,
            symbols,
            features,
            has_more,
        })
    }

    /// Exact identity used for authorization and cursor binding.
    #[must_use]
    pub const fn identity(&self) -> &ApplicationCatalogIdentityV1 {
        &self.identity
    }

    /// Authorized symbols only.
    #[must_use]
    pub fn symbols(&self) -> &[ApplicationCatalogSymbolV1] {
        &self.symbols
    }

    /// Closed feature observations.
    #[must_use]
    pub fn features(&self) -> &[ApplicationCatalogFeatureViewV1] {
        &self.features
    }

    /// Whether another authorized page exists.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }
}

/// Invalid or noncanonical public catalog shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogError;

impl fmt::Display for ApplicationCatalogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application catalog shape is invalid")
    }
}

impl Error for ApplicationCatalogError {}

fn valid_symbol(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_APPLICATION_CATALOG_TEXT_BYTES
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
        && value
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ApplicationCatalogIdentityV1 {
        ApplicationCatalogIdentityV1::checked(
            ContractLineage::new("TicketDesk").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([1; 32]),
            vec![QueryModuleHash::from_bytes([2; 32])],
        )
        .expect("identity")
    }

    fn symbol(name: &str) -> ApplicationCatalogSymbolV1 {
        ApplicationCatalogSymbolV1::checked(
            ApplicationCatalogSymbolKindV1::Field,
            vec!["Ticket".to_owned(), name.to_owned()],
            Some("string<200>".to_owned()),
            ApplicationCatalogSourceSpanV1::checked(10, 20),
        )
        .expect("symbol")
    }

    #[test]
    fn page_is_bounded_sorted_and_contains_no_hidden_count_surface() {
        let page = ApplicationCatalogPageV1::checked(
            identity(),
            vec![symbol("status"), symbol("title")],
            vec![ApplicationCatalogFeatureViewV1::new(
                ApplicationCatalogFeatureV1::OperationalOptionalPredicates,
                ApplicationCatalogFeatureStateV1::Available,
            )],
            false,
        )
        .expect("page");
        assert_eq!(page.symbols().len(), 2);
        assert!(!page.has_more());

        assert!(
            ApplicationCatalogPageV1::checked(
                identity(),
                vec![symbol("title"), symbol("status")],
                Vec::new(),
                false,
            )
            .is_err(),
            "caller order cannot influence canonical catalog pages"
        );
        assert!(
            ApplicationCatalogPageV1::checked(
                identity(),
                (0..=MAX_APPLICATION_CATALOG_PAGE_ITEMS)
                    .map(|index| symbol(&format!("field_{index:03}")))
                    .collect(),
                Vec::new(),
                true,
            )
            .is_err()
        );
    }

    #[test]
    fn symbolic_paths_and_exact_identity_fail_closed() {
        assert!(
            ApplicationCatalogSymbolV1::checked(
                ApplicationCatalogSymbolKindV1::Field,
                vec!["Ticket".to_owned(), "field-id".to_owned()],
                None,
                None,
            )
            .is_err()
        );
        assert!(
            ApplicationCatalogIdentityV1::checked(
                ContractLineage::new("TicketDesk").expect("lineage"),
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([1; 32]),
                vec![
                    QueryModuleHash::from_bytes([3; 32]),
                    QueryModuleHash::from_bytes([2; 32]),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn frozen_public_schema_names_every_closed_variant() {
        let schema = include_str!("../../../fixtures/riffql/application-catalog-schema-v1.json");
        assert!(schema.contains("riffdb.application-catalog-schema/v1"));
        assert!(schema.contains(APPLICATION_CATALOG_SCHEMA_V1));
        assert!(schema.contains(&MAX_APPLICATION_CATALOG_PAGE_ITEMS.to_string()));
        for kind in ApplicationCatalogSymbolKindV1::ALL {
            assert!(schema.contains(&format!("\"{}\"", kind.as_str())));
        }
        for feature in ApplicationCatalogFeatureV1::ALL {
            assert!(schema.contains(&format!("\"{}\"", feature.as_str())));
        }
        for forbidden in [
            "entity_type_id",
            "field_id",
            "index_id",
            "capability",
            "raw_ir",
            "storage_key",
        ] {
            assert!(
                schema.contains(&format!("\"{forbidden}\"")),
                "the frozen schema must retain its explicit forbidden-field ledger"
            );
        }
    }
}
