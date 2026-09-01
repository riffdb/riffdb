use std::collections::BTreeSet;

/// Maximum ordinary/provider leaves in one V1 candidate expression.
pub const MAX_CANDIDATE_SOURCES_V1: usize = 8;
/// Maximum distinct complete root keys in one V1 candidate binding.
pub const MAX_CANDIDATE_KEYS_V1: u16 = u16::MAX;

/// Closed candidate-set algebra retained in checked query identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateSetOperatorV1 {
    /// One source, with only canonical deduplication.
    Single,
    /// Keys present in every source.
    Intersection,
    /// Keys present in any source.
    Union,
    /// Authorized positive universe minus every negative source.
    Difference,
}

impl CandidateSetOperatorV1 {
    /// Stable V1 codec tag.
    #[must_use]
    pub const fn durable_tag(self) -> u8 {
        match self {
            Self::Single => 1,
            Self::Intersection => 2,
            Self::Union => 3,
            Self::Difference => 4,
        }
    }
}

/// One checked ordinary/provider candidate source.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateSourceV1 {
    entity: String,
    projected_key: String,
    access: String,
}

impl CandidateSourceV1 {
    /// Constructs a non-empty checked source identity.
    #[must_use]
    pub fn checked(entity: String, projected_key: String, access: String) -> Option<Self> {
        if entity.is_empty() || projected_key.is_empty() || access.is_empty() {
            return None;
        }
        Some(Self {
            entity,
            projected_key,
            access,
        })
    }

    /// Exact source entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Field projected as the complete root key.
    #[must_use]
    pub fn projected_key(&self) -> &str {
        &self.projected_key
    }

    /// Declared access/provider name.
    #[must_use]
    pub fn access(&self) -> &str {
        &self.access
    }
}

/// One compiler-checked, non-output candidate binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CandidateBindingV1 {
    name: String,
    root_entity: String,
    root_key: String,
    operator: CandidateSetOperatorV1,
    sources: Vec<CandidateSourceV1>,
    positive_source_count: u8,
    maximum_distinct_keys: u16,
    refusal_outcome: String,
}

impl CandidateBindingV1 {
    /// Constructs one closed V1 binding after symbolic proof.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn checked(
        name: String,
        root_entity: String,
        root_key: String,
        operator: CandidateSetOperatorV1,
        sources: Vec<CandidateSourceV1>,
        positive_source_count: u8,
        maximum_distinct_keys: u16,
        refusal_outcome: String,
    ) -> Option<Self> {
        let valid_arity = match operator {
            CandidateSetOperatorV1::Single => sources.len() == 1,
            CandidateSetOperatorV1::Intersection | CandidateSetOperatorV1::Union => {
                (2..=MAX_CANDIDATE_SOURCES_V1).contains(&sources.len())
            }
            CandidateSetOperatorV1::Difference => {
                (2..=MAX_CANDIDATE_SOURCES_V1).contains(&sources.len())
                    && positive_source_count == 1
            }
        };
        if name.is_empty()
            || root_entity.is_empty()
            || root_key.is_empty()
            || refusal_outcome.is_empty()
            || maximum_distinct_keys == 0
            || !valid_arity
            || (operator != CandidateSetOperatorV1::Difference
                && positive_source_count as usize != sources.len())
        {
            return None;
        }
        Some(Self {
            name,
            root_entity,
            root_key,
            operator,
            sources,
            positive_source_count,
            maximum_distinct_keys,
            refusal_outcome,
        })
    }

    /// Query-local non-output name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Root entity consumed after candidate completion.
    #[must_use]
    pub fn root_entity(&self) -> &str {
        &self.root_entity
    }
    /// Complete root-key field.
    #[must_use]
    pub fn root_key(&self) -> &str {
        &self.root_key
    }
    /// Closed algebra operator.
    #[must_use]
    pub const fn operator(&self) -> CandidateSetOperatorV1 {
        self.operator
    }
    /// Sources in canonical source order; the positive difference universe is first.
    #[must_use]
    pub fn sources(&self) -> &[CandidateSourceV1] {
        &self.sources
    }
    /// Number of leading positive sources.
    #[must_use]
    pub const fn positive_source_count(&self) -> u8 {
        self.positive_source_count
    }
    /// Whole-binding distinct-key refusal ceiling.
    #[must_use]
    pub const fn maximum_distinct_keys(&self) -> u16 {
        self.maximum_distinct_keys
    }
    /// Declared safe refusal outcome.
    #[must_use]
    pub fn refusal_outcome(&self) -> &str {
        &self.refusal_outcome
    }
}

/// Candidate reference-evaluator refusal. No partial set accompanies an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CandidateSetRefusal {
    /// Source count or expression arity is invalid.
    InvalidShape,
    /// A complete source or final algebra result exceeded the declared distinct-key ceiling.
    DistinctKeyLimit,
    /// Canonical key bytes exceeded the independent byte ceiling.
    KeyByteLimit,
}

/// Evaluates complete source keys with deterministic deduplication and all-or-refusal semantics.
pub fn evaluate_candidate_set_v1(
    operator: CandidateSetOperatorV1,
    sources: &[Vec<Vec<u8>>],
    maximum_distinct_keys: u16,
    maximum_key_bytes: u64,
) -> Result<Vec<Vec<u8>>, CandidateSetRefusal> {
    if maximum_distinct_keys == 0
        || sources.is_empty()
        || sources.len() > MAX_CANDIDATE_SOURCES_V1
        || matches!(operator, CandidateSetOperatorV1::Single) && sources.len() != 1
        || matches!(
            operator,
            CandidateSetOperatorV1::Intersection
                | CandidateSetOperatorV1::Union
                | CandidateSetOperatorV1::Difference
        ) && sources.len() < 2
    {
        return Err(CandidateSetRefusal::InvalidShape);
    }
    let mut complete = Vec::with_capacity(sources.len());
    for source in sources {
        let set = source.iter().cloned().collect::<BTreeSet<_>>();
        check_candidate_bound(&set, maximum_distinct_keys, maximum_key_bytes)?;
        complete.push(set);
    }
    let mut result = match operator {
        CandidateSetOperatorV1::Single => complete.remove(0),
        CandidateSetOperatorV1::Intersection => {
            let mut result = complete.remove(0);
            for source in complete {
                result.retain(|key| source.contains(key));
            }
            result
        }
        CandidateSetOperatorV1::Union => {
            let mut result = BTreeSet::new();
            for source in complete {
                result.extend(source);
                check_candidate_bound(&result, maximum_distinct_keys, maximum_key_bytes)?;
            }
            result
        }
        CandidateSetOperatorV1::Difference => {
            let mut result = complete.remove(0);
            for source in complete {
                result.retain(|key| !source.contains(key));
            }
            result
        }
    };
    check_candidate_bound(&result, maximum_distinct_keys, maximum_key_bytes)?;
    Ok(std::mem::take(&mut result).into_iter().collect())
}

fn check_candidate_bound(
    set: &BTreeSet<Vec<u8>>,
    maximum_distinct_keys: u16,
    maximum_key_bytes: u64,
) -> Result<(), CandidateSetRefusal> {
    if set.len() > usize::from(maximum_distinct_keys) {
        return Err(CandidateSetRefusal::DistinctKeyLimit);
    }
    let bytes = set.iter().try_fold(0_u64, |total, key| {
        total.checked_add(u64::try_from(key.len()).ok()?)
    });
    if bytes.is_none_or(|bytes| bytes > maximum_key_bytes) {
        return Err(CandidateSetRefusal::KeyByteLimit);
    }
    Ok(())
}
