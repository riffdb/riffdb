/// Name-addressed public query type schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NamedTypeSchema {
    /// Closed scalar or contract enum source name.
    Scalar(String),
    /// Nullable value.
    Optional(Box<Self>),
    /// Query-only submitted set.
    Set(Box<Self>),
    /// Nested returned object.
    Record(Vec<NamedFieldSchema>),
    /// Bounded returned list.
    List {
        /// Element schema.
        element: Box<Self>,
        /// Exact source-declared page bound.
        maximum: PageBound,
    },
    /// Opaque cursor.
    Cursor,
    /// Positive service-bounded row limit.
    Limit,
    /// Positive row limit with a compiler-declared inclusive maximum.
    BoundedLimit {
        /// Inclusive maximum accepted runtime value.
        maximum: u64,
    },
}

/// Exact explicit page bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PageBound {
    /// Positive literal bound.
    Literal(u64),
    /// Name of a typed `Limit` parameter.
    Parameter(String),
    /// Name and inclusive maximum of a typed `Limit<MAX>` parameter.
    BoundedParameter {
        /// Parameter name without `$`.
        name: String,
        /// Inclusive maximum accepted runtime value.
        maximum: u64,
    },
}

/// One name-addressed field schema.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedFieldSchema {
    name: String,
    value_type: NamedTypeSchema,
}

impl NamedFieldSchema {
    pub(crate) fn new(name: String, value_type: NamedTypeSchema) -> Self {
        Self { name, value_type }
    }

    /// Exact output field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Complete name-addressed field type.
    #[must_use]
    pub const fn value_type(&self) -> &NamedTypeSchema {
        &self.value_type
    }
}

/// One typed name-addressed parameter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedParameterSchema {
    name: String,
    value_type: NamedTypeSchema,
    has_default: bool,
}

impl NamedParameterSchema {
    pub(crate) fn new(name: String, value_type: NamedTypeSchema, has_default: bool) -> Self {
        Self {
            name,
            value_type,
            has_default,
        }
    }

    /// Exact parameter name without `$`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Complete name-addressed type.
    #[must_use]
    pub const fn value_type(&self) -> &NamedTypeSchema {
        &self.value_type
    }

    /// Whether the declaration supplies a literal default.
    #[must_use]
    pub const fn has_default(&self) -> bool {
        self.has_default
    }
}

/// One declared result-union branch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedResultBranchSchema {
    name: String,
    fields: Vec<NamedFieldSchema>,
}

impl NamedResultBranchSchema {
    pub(crate) fn new(name: String, fields: Vec<NamedFieldSchema>) -> Self {
        Self { name, fields }
    }

    /// Exact branch name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Fields in source return order.
    #[must_use]
    pub fn fields(&self) -> &[NamedFieldSchema] {
        &self.fields
    }
}

/// Complete parameter and result schemas for one resolved query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamedQuerySchemas {
    parameters: Vec<NamedParameterSchema>,
    results: Vec<NamedResultBranchSchema>,
}

impl NamedQuerySchemas {
    pub(crate) fn new(
        parameters: Vec<NamedParameterSchema>,
        results: Vec<NamedResultBranchSchema>,
    ) -> Self {
        Self {
            parameters,
            results,
        }
    }

    /// Parameters in source declaration order.
    #[must_use]
    pub fn parameters(&self) -> &[NamedParameterSchema] {
        &self.parameters
    }

    /// Declared result branches in source order.
    #[must_use]
    pub fn results(&self) -> &[NamedResultBranchSchema] {
        &self.results
    }
}
