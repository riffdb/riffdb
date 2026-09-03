//! Closed symbolic application-catalog response types.

use std::error::Error;
use std::fmt;

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{ContractBundle, Instruction, ValueType, ValueTypeTag};
use riffdb_query_module::QueryModule;
use riffdb_types::{
    CommandId, ContractBundleHash, ContractLineage, ContractVersion, QueryModuleHash,
    QueryOperationName,
};

/// Versioned public schema name for symbolic application-catalog pages.
pub const APPLICATION_CATALOG_SCHEMA_V1: &str = "riffdb.application-catalog/v1";
/// Hard maximum visible symbols in one catalog page.
pub const MAX_APPLICATION_CATALOG_PAGE_ITEMS: usize = 100;
/// Hard maximum compiler-owned symbols retained for one exact application catalog.
pub const MAX_APPLICATION_CATALOG_CANDIDATES: usize = 8_192;
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

    const fn state(self) -> ApplicationCatalogFeatureStateV1 {
        match self {
            Self::OperationalOptionalPredicates
            | Self::StableCursorPages
            | Self::NullExistencePredicates
            | Self::BinaryTextPrefix
            | Self::UnicodeFoldTextPrefixV1
            | Self::ExactAggregates => ApplicationCatalogFeatureStateV1::Available,
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

/// Compiler-owned authority candidate used to filter one symbolic catalog.
///
/// This type is deliberately absent from the public page schema. Numeric command
/// identities exist only on this trusted side of the policy boundary and are
/// discarded before a page can be serialized.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ApplicationCatalogAuthorityV1 {
    /// Baseline contract visibility.
    Contract,
    /// Exact invocable compiled command.
    Command {
        /// Contract lineage.
        lineage: ContractLineage,
        /// Compiler-private command identity.
        command_id: CommandId,
    },
    /// Exact deployed named query.
    Query {
        /// Contract lineage.
        lineage: ContractLineage,
        /// Immutable module identity.
        module_hash: QueryModuleHash,
        /// Exact symbolic query name.
        query_name: QueryOperationName,
    },
}

/// One compiler-owned catalog candidate and the least operation authorities
/// that can make it visible.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogCandidateV1 {
    symbol: ApplicationCatalogSymbolV1,
    authorities: Vec<ApplicationCatalogAuthorityV1>,
}

impl ApplicationCatalogCandidateV1 {
    /// Name-only public symbol.
    #[must_use]
    pub const fn symbol(&self) -> &ApplicationCatalogSymbolV1 {
        &self.symbol
    }

    /// Canonical disjunction of exact authorities that may reveal this symbol.
    #[must_use]
    pub fn authorities(&self) -> &[ApplicationCatalogAuthorityV1] {
        &self.authorities
    }
}

/// Immutable compiler-owned candidate catalog for one exact contract/module pair.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationCatalogCandidatesV1 {
    lineage: ContractLineage,
    version: ContractVersion,
    contract_hash: ContractBundleHash,
    module_hash: Option<QueryModuleHash>,
    candidates: Vec<ApplicationCatalogCandidateV1>,
    features: Vec<ApplicationCatalogFeatureViewV1>,
}

impl ApplicationCatalogCandidatesV1 {
    /// Builds the complete bounded symbolic candidate set before policy filtering.
    pub fn from_exact_application(
        contract: &ContractBundle,
        module: Option<&QueryModule>,
    ) -> Result<Self, ApplicationCatalogError> {
        if module.is_some_and(|module| {
            module.contract_lineage() != contract.lineage()
                || module.contract_version() != contract.contract_version()
                || module.contract_hash() != contract.bundle_hash()
        }) {
            return Err(ApplicationCatalogError);
        }

        let contract_authority = ApplicationCatalogAuthorityV1::Contract;
        let mut symbols =
            BTreeMap::<ApplicationCatalogSymbolV1, BTreeSet<ApplicationCatalogAuthorityV1>>::new();
        insert_candidate(
            &mut symbols,
            ApplicationCatalogSymbolV1::checked(
                ApplicationCatalogSymbolKindV1::Contract,
                vec![contract.lineage().as_str().to_owned()],
                None,
                None,
            )?,
            contract_authority,
        );

        for command in contract
            .commands()
            .iter()
            .filter(|command| !command.is_reimport())
        {
            let authority = ApplicationCatalogAuthorityV1::Command {
                lineage: contract.lineage().clone(),
                command_id: command.command_id(),
            };
            insert_candidate(
                &mut symbols,
                ApplicationCatalogSymbolV1::checked(
                    ApplicationCatalogSymbolKindV1::Command,
                    vec![command.name().to_owned()],
                    None,
                    None,
                )?,
                authority.clone(),
            );
            insert_candidate(
                &mut symbols,
                ApplicationCatalogSymbolV1::checked(
                    ApplicationCatalogSymbolKindV1::Operation,
                    vec![command.name().to_owned()],
                    Some("command".to_owned()),
                    None,
                )?,
                authority.clone(),
            );
            for outcome in command.outcomes() {
                insert_candidate(
                    &mut symbols,
                    ApplicationCatalogSymbolV1::checked(
                        ApplicationCatalogSymbolKindV1::CommandOutcome,
                        vec![command.name().to_owned(), outcome.name().to_owned()],
                        render_record_type(outcome.payload(), contract)?,
                        None,
                    )?,
                    authority.clone(),
                );
                collect_record_enums(&mut symbols, outcome.payload(), contract, authority.clone())?;
            }
            collect_record_enums(
                &mut symbols,
                command.input().record(),
                contract,
                authority.clone(),
            )?;
            for instruction in command.instructions() {
                if let Instruction::EmitEvent(event) = instruction {
                    let schema = contract
                        .schema()
                        .event(event.event_type())
                        .ok_or(ApplicationCatalogError)?;
                    insert_candidate(
                        &mut symbols,
                        ApplicationCatalogSymbolV1::checked(
                            ApplicationCatalogSymbolKindV1::Event,
                            vec![schema.name().to_owned()],
                            render_record_type(schema.payload(), contract)?,
                            None,
                        )?,
                        authority.clone(),
                    );
                    collect_record_enums(
                        &mut symbols,
                        schema.payload(),
                        contract,
                        authority.clone(),
                    )?;
                }
            }
        }

        if let Some(module) = module {
            for query in module.queries() {
                let query_name = QueryOperationName::new(query.name().to_owned())
                    .map_err(|_| ApplicationCatalogError)?;
                let authority = ApplicationCatalogAuthorityV1::Query {
                    lineage: contract.lineage().clone(),
                    module_hash: module.identity(),
                    query_name,
                };
                insert_candidate(
                    &mut symbols,
                    ApplicationCatalogSymbolV1::checked(
                        ApplicationCatalogSymbolKindV1::QueryModule,
                        vec![module.name().as_str().to_owned()],
                        None,
                        None,
                    )?,
                    authority.clone(),
                );
                insert_candidate(
                    &mut symbols,
                    ApplicationCatalogSymbolV1::checked(
                        ApplicationCatalogSymbolKindV1::Query,
                        vec![module.name().as_str().to_owned(), query.name().to_owned()],
                        None,
                        None,
                    )?,
                    authority.clone(),
                );
                insert_candidate(
                    &mut symbols,
                    ApplicationCatalogSymbolV1::checked(
                        ApplicationCatalogSymbolKindV1::Operation,
                        vec![query.name().to_owned()],
                        Some("query".to_owned()),
                        None,
                    )?,
                    authority.clone(),
                );
                for access in query.plan().authorization() {
                    let entity = contract
                        .schema()
                        .entities()
                        .iter()
                        .find(|entity| entity.name() == access.entity())
                        .ok_or(ApplicationCatalogError)?;
                    insert_candidate(
                        &mut symbols,
                        ApplicationCatalogSymbolV1::checked(
                            ApplicationCatalogSymbolKindV1::Entity,
                            vec![entity.name().to_owned()],
                            None,
                            None,
                        )?,
                        authority.clone(),
                    );
                    for field_name in access.fields() {
                        let field = entity
                            .record()
                            .fields()
                            .iter()
                            .find(|field| field.name() == field_name)
                            .ok_or(ApplicationCatalogError)?;
                        insert_candidate(
                            &mut symbols,
                            ApplicationCatalogSymbolV1::checked(
                                ApplicationCatalogSymbolKindV1::Field,
                                vec![entity.name().to_owned(), field.name().to_owned()],
                                Some(render_value_type(field.value_type(), contract)?),
                                None,
                            )?,
                            authority.clone(),
                        );
                        collect_value_enum(
                            &mut symbols,
                            field.value_type(),
                            contract,
                            authority.clone(),
                        )?;
                    }
                    for index_name in access.indexes() {
                        if !entity
                            .indexes()
                            .iter()
                            .any(|index| index.name() == index_name)
                        {
                            return Err(ApplicationCatalogError);
                        }
                        insert_candidate(
                            &mut symbols,
                            ApplicationCatalogSymbolV1::checked(
                                ApplicationCatalogSymbolKindV1::Index,
                                vec![entity.name().to_owned(), index_name.clone()],
                                None,
                                None,
                            )?,
                            authority.clone(),
                        );
                    }
                }
            }
        }

        let candidates: Vec<ApplicationCatalogCandidateV1> = symbols
            .into_iter()
            .map(|(symbol, authorities)| ApplicationCatalogCandidateV1 {
                symbol,
                authorities: authorities.into_iter().collect(),
            })
            .collect();
        if candidates.len() > MAX_APPLICATION_CATALOG_CANDIDATES {
            return Err(ApplicationCatalogError);
        }
        let features = ApplicationCatalogFeatureV1::ALL
            .into_iter()
            .map(|feature| ApplicationCatalogFeatureViewV1::new(feature, feature.state()))
            .collect();
        Ok(Self {
            lineage: contract.lineage().clone(),
            version: contract.contract_version(),
            contract_hash: contract.bundle_hash(),
            module_hash: module.map(QueryModule::identity),
            candidates,
            features,
        })
    }

    /// Canonical compiler-owned candidates.
    #[must_use]
    pub fn candidates(&self) -> &[ApplicationCatalogCandidateV1] {
        &self.candidates
    }

    /// Exact distinct authority candidates in canonical order.
    #[must_use]
    pub fn authorities(&self) -> Vec<ApplicationCatalogAuthorityV1> {
        self.candidates
            .iter()
            .flat_map(|candidate| candidate.authorities.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Filters first, then pages visible symbols without exposing hidden totals.
    pub fn authorized_page(
        &self,
        visible: &BTreeSet<ApplicationCatalogAuthorityV1>,
        after_candidate: Option<usize>,
        limit: usize,
    ) -> Result<(ApplicationCatalogPageV1, Option<usize>), ApplicationCatalogError> {
        if limit == 0 || limit > MAX_APPLICATION_CATALOG_PAGE_ITEMS {
            return Err(ApplicationCatalogError);
        }
        if after_candidate.is_some_and(|candidate| candidate >= self.candidates.len()) {
            return Err(ApplicationCatalogError);
        }
        let start = after_candidate.map_or(0, |candidate| candidate + 1);
        let visible_symbols = self
            .candidates
            .iter()
            .enumerate()
            .skip(start)
            .filter(|candidate| {
                candidate
                    .1
                    .authorities
                    .iter()
                    .any(|authority| visible.contains(authority))
            })
            .map(|(candidate, entry)| (candidate, entry.symbol.clone()))
            .take(limit + 1)
            .collect::<Vec<_>>();
        let has_more = visible_symbols.len() > limit;
        let symbols = visible_symbols
            .iter()
            .take(limit)
            .map(|(_, symbol)| symbol.clone())
            .collect::<Vec<_>>();
        let continuation_after_candidate = has_more
            .then(|| {
                visible_symbols
                    .get(limit - 1)
                    .map(|(candidate, _)| *candidate)
            })
            .flatten();
        let module_hashes = self
            .module_hash
            .filter(|module_hash| {
                visible.iter().any(|authority| {
                    matches!(
                        authority,
                        ApplicationCatalogAuthorityV1::Query {
                            module_hash: candidate,
                            ..
                        } if candidate == module_hash
                    )
                })
            })
            .into_iter()
            .collect();
        let identity = ApplicationCatalogIdentityV1::checked(
            self.lineage.clone(),
            self.version,
            self.contract_hash,
            module_hashes,
        )?;
        let page =
            ApplicationCatalogPageV1::checked(identity, symbols, self.features.clone(), has_more)?;
        Ok((page, continuation_after_candidate))
    }
}

fn insert_candidate(
    symbols: &mut BTreeMap<ApplicationCatalogSymbolV1, BTreeSet<ApplicationCatalogAuthorityV1>>,
    symbol: ApplicationCatalogSymbolV1,
    authority: ApplicationCatalogAuthorityV1,
) {
    symbols.entry(symbol).or_default().insert(authority);
}

fn collect_record_enums(
    symbols: &mut BTreeMap<ApplicationCatalogSymbolV1, BTreeSet<ApplicationCatalogAuthorityV1>>,
    record: &riffdb_contract_ir::RecordSchema,
    contract: &ContractBundle,
    authority: ApplicationCatalogAuthorityV1,
) -> Result<(), ApplicationCatalogError> {
    for field in record.fields() {
        collect_value_enum(symbols, field.value_type(), contract, authority.clone())?;
    }
    Ok(())
}

fn collect_value_enum(
    symbols: &mut BTreeMap<ApplicationCatalogSymbolV1, BTreeSet<ApplicationCatalogAuthorityV1>>,
    value_type: &ValueType,
    contract: &ContractBundle,
    authority: ApplicationCatalogAuthorityV1,
) -> Result<(), ApplicationCatalogError> {
    let value_type = match value_type.tag() {
        ValueTypeTag::Optional => value_type.optional_inner().ok_or(ApplicationCatalogError)?,
        ValueTypeTag::List => value_type.list_parts().ok_or(ApplicationCatalogError)?.0,
        _ => value_type,
    };
    if value_type.tag() != ValueTypeTag::Enum {
        return Ok(());
    }
    let enum_id = value_type.enum_type_id().ok_or(ApplicationCatalogError)?;
    let enumeration = contract
        .schema()
        .enums()
        .iter()
        .find(|enumeration| enumeration.id() == enum_id)
        .ok_or(ApplicationCatalogError)?;
    insert_candidate(
        symbols,
        ApplicationCatalogSymbolV1::checked(
            ApplicationCatalogSymbolKindV1::Enum,
            vec![enumeration.name().to_owned()],
            None,
            None,
        )?,
        authority,
    );
    Ok(())
}

fn render_record_type(
    record: &riffdb_contract_ir::RecordSchema,
    contract: &ContractBundle,
) -> Result<Option<String>, ApplicationCatalogError> {
    let fields = record
        .fields()
        .iter()
        .map(|field| {
            Ok(format!(
                "{}: {}",
                field.name(),
                render_value_type(field.value_type(), contract)?
            ))
        })
        .collect::<Result<Vec<_>, ApplicationCatalogError>>()?;
    let rendered = format!("{{ {} }}", fields.join(", "));
    if rendered.len() > MAX_APPLICATION_CATALOG_TEXT_BYTES {
        return Ok(Some("record".to_owned()));
    }
    Ok(Some(rendered))
}

fn render_value_type(
    value_type: &ValueType,
    contract: &ContractBundle,
) -> Result<String, ApplicationCatalogError> {
    let rendered = match value_type.tag() {
        ValueTypeTag::Bool => "bool".to_owned(),
        ValueTypeTag::I64 => "i64".to_owned(),
        ValueTypeTag::U64 => "u64".to_owned(),
        ValueTypeTag::Decimal => {
            let spec = value_type.decimal_spec().ok_or(ApplicationCatalogError)?;
            format!("decimal<{},{}>", spec.precision(), spec.scale())
        }
        ValueTypeTag::Money => format!(
            "money<{}>",
            value_type.currency().ok_or(ApplicationCatalogError)?
        ),
        ValueTypeTag::String => format!(
            "string<{}>",
            value_type.byte_bound().ok_or(ApplicationCatalogError)?
        ),
        ValueTypeTag::Bytes => format!(
            "bytes<{}>",
            value_type.byte_bound().ok_or(ApplicationCatalogError)?
        ),
        ValueTypeTag::Timestamp => "timestamp".to_owned(),
        ValueTypeTag::Date => "date".to_owned(),
        ValueTypeTag::Uuid => "uuid".to_owned(),
        ValueTypeTag::Enum => contract
            .schema()
            .enums()
            .iter()
            .find(|enumeration| Some(enumeration.id()) == value_type.enum_type_id())
            .map(|enumeration| enumeration.name().to_owned())
            .ok_or(ApplicationCatalogError)?,
        ValueTypeTag::Optional => format!(
            "{}?",
            render_value_type(
                value_type.optional_inner().ok_or(ApplicationCatalogError)?,
                contract
            )?
        ),
        ValueTypeTag::List => {
            let (element, maximum) = value_type.list_parts().ok_or(ApplicationCatalogError)?;
            format!("[{}; {maximum}]", render_value_type(element, contract)?)
        }
        ValueTypeTag::Record => "record".to_owned(),
        ValueTypeTag::Vector => {
            format!(
                "vector<{}>",
                value_type
                    .vector_dimension()
                    .ok_or(ApplicationCatalogError)?
                    .get()
            )
        }
    };
    if rendered.is_empty() || rendered.len() > MAX_APPLICATION_CATALOG_TEXT_BYTES {
        return Err(ApplicationCatalogError);
    }
    Ok(rendered)
}

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
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_query_module::{
        NamedQuerySource, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    };

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

    // req: OQ-032, OQ-043
    #[test]
    fn operational_feature_preflight_matches_the_executable_surface() {
        for feature in [
            ApplicationCatalogFeatureV1::NullExistencePredicates,
            ApplicationCatalogFeatureV1::BinaryTextPrefix,
            ApplicationCatalogFeatureV1::UnicodeFoldTextPrefixV1,
        ] {
            assert_eq!(feature.state(), ApplicationCatalogFeatureStateV1::Available);
        }
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

    #[test]
    fn hidden_named_operation_removes_every_symbol_reachable_only_through_it() {
        let contract = compile_contract_source(include_str!(
            "../../../examples/app-baseline/contracts/ticketdesk.riff"
        ))
        .expect("ticketdesk contract");
        let module = QueryModule::compile(
            QueryModuleCandidate::new(
                QueryModuleName::new("ticketdesk").expect("module name"),
                QueryModuleVersion::new(1).expect("module version"),
                vec![
                    NamedQuerySource::new(
                        "ListTickets",
                        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
                    )
                    .expect("list query"),
                    NamedQuerySource::new(
                        "TicketPage",
                        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
                    )
                    .expect("page query"),
                ],
            )
            .expect("module candidate"),
            &contract,
        )
        .expect("compiled module");
        let candidates =
            ApplicationCatalogCandidatesV1::from_exact_application(&contract, Some(&module))
                .expect("catalog candidates");
        let mut visible = BTreeSet::from([ApplicationCatalogAuthorityV1::Contract]);
        visible.insert(ApplicationCatalogAuthorityV1::Query {
            lineage: contract.lineage().clone(),
            module_hash: module.identity(),
            query_name: QueryOperationName::new("ListTickets").expect("query name"),
        });
        let (page, continuation) = candidates
            .authorized_page(&visible, None, MAX_APPLICATION_CATALOG_PAGE_ITEMS)
            .expect("authorized page");
        let paths = page
            .symbols()
            .iter()
            .map(|symbol| symbol.path().join("."))
            .collect::<BTreeSet<_>>();

        assert!(paths.contains("ticketdesk.ListTickets"));
        assert!(!paths.contains("ticketdesk.TicketPage"));
        assert!(paths.contains("Ticket.title"));
        assert!(!paths.contains("Ticket.description"));
        assert!(!paths.contains("Comment.body"));
        assert_eq!(page.identity().module_hashes(), &[module.identity()]);
        assert!(!page.has_more());
        assert_eq!(continuation, None);

        let all_visible = BTreeSet::from([
            ApplicationCatalogAuthorityV1::Contract,
            ApplicationCatalogAuthorityV1::Query {
                lineage: contract.lineage().clone(),
                module_hash: module.identity(),
                query_name: QueryOperationName::new("ListTickets").expect("query name"),
            },
            ApplicationCatalogAuthorityV1::Query {
                lineage: contract.lineage().clone(),
                module_hash: module.identity(),
                query_name: QueryOperationName::new("TicketPage").expect("query name"),
            },
        ]);
        let (first, continuation) = candidates
            .authorized_page(&all_visible, None, 3)
            .expect("first authorized page");
        assert!(first.has_more());
        let continuation = continuation.expect("raw candidate continuation");
        let (narrowed, continuation) = candidates
            .authorized_page(
                &BTreeSet::from([ApplicationCatalogAuthorityV1::Contract]),
                Some(continuation),
                3,
            )
            .expect("authorization narrowing remains a valid final page");
        assert!(narrowed.symbols().is_empty());
        assert!(!narrowed.has_more());
        assert_eq!(continuation, None);
    }
}
