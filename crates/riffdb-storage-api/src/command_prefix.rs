//! Checked mutation evidence for exact application stops inside a command group.
//!
//! This is an internal semantic building block for capsule V7, not a receipt,
//! replication position, durable codec, or permission to publish a restore.
//! Command facts, locators and allocators are reconstructed by their existing
//! owners; this value retains the independently stored command state.

use riffdb_types::DualFrontier;

use crate::{
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3, AuthoritativeNamespaceV1,
    AuthoritativeTransactionV3, ChangelogAttributionV3, ChangelogV3Error,
    MAX_CHANGELOG_FRAME_ENTRIES, MAX_STAGED_COMMANDS, MAX_STAGED_WRITE_BYTES,
    StoredCommandCapsuleV1,
};

/// One application's logical before/after frontier and canonical independent
/// mutations. A complete post-image is retained for every put, including values
/// overwritten or deleted by a later command in the same physical transaction.
/// Construction checks shape; the storage owner must still prove first-observed
/// preconditions, command reciprocity and the complete original receipt.
#[derive(Clone, Eq, PartialEq)]
pub struct CommandPrefixEvidenceV1 {
    predecessor: DualFrontier,
    covered: DualFrontier,
    mutations: Vec<AuthoritativeMutationV3>,
    semantic_bytes: usize,
}

impl CommandPrefixEvidenceV1 {
    /// Checks one successful command's sequence step and canonical mutation set.
    /// Its audit contributes either a terminal or a fused start and terminal.
    /// Empty independent mutations are valid; the command graph still advances.
    pub fn new(
        predecessor: DualFrontier,
        covered: DualFrontier,
        mutations: Vec<AuthoritativeMutationV3>,
    ) -> Result<Self, ChangelogV3Error> {
        let prior_app = predecessor.application().map_or(0, |value| value.get());
        let prior_admin = predecessor.administration().map_or(0, |value| value.get());
        let next_app = covered.application().map_or(0, |value| value.get());
        let next_admin = covered.administration().map_or(0, |value| value.get());
        if prior_app.checked_add(1) != Some(next_app)
            || !matches!(next_admin.checked_sub(prior_admin), Some(1 | 2))
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        if mutations.len() > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        // Both explicit dual frontiers, the count, and complete mutation values.
        // This is semantic accounting; the durable codec must additionally charge
        // actual envelope bytes together with the rest of the command graph.
        let semantic_bytes = mutations.iter().try_fold(40_usize, |bytes, mutation| {
            bytes
                .checked_add(mutation.encoded_len())
                .ok_or(ChangelogV3Error::LimitExceeded)
        })?;
        if semantic_bytes > MAX_STAGED_WRITE_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if mutations
            .iter()
            .any(|mutation| !Self::supports_namespace(mutation.namespace()))
        {
            return Err(ChangelogV3Error::InvalidNamespace);
        }
        if mutations.windows(2).any(|pair| {
            (pair[0].namespace(), pair[0].key()) >= (pair[1].namespace(), pair[1].key())
        }) || mutations
            .iter()
            .any(|mutation| mutation.matches_prior(mutation.value()))
        {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Ok(Self {
            predecessor,
            covered,
            mutations,
            semantic_bytes,
        })
    }

    /// The independent state changed by the existing successful-command owner.
    /// Recursive segment bytes, lifecycle/control state and unrelated authority
    /// are excluded. Graph-owned locators and allocators use their checked owners.
    #[must_use]
    pub const fn supports_namespace(namespace: AuthoritativeNamespaceV1) -> bool {
        use AuthoritativeNamespaceV1 as N;
        matches!(
            namespace,
            N::Entities
                | N::EntityChainHeads
                | N::SecondaryIndexes
                | N::IndexEpochs
                | N::IdempotencyPending
                | N::VectorEvidence
                | N::VectorObservations
                | N::VectorEvidenceIndex
        )
    }

    /// Joins the frontier to the already checked reciprocal command and audit
    /// facts. Existing durable starts consume one new audit sequence; fused starts
    /// consume two. Neither path may skip or relabel an administration sequence.
    pub fn validate_command(
        &self,
        command: &StoredCommandCapsuleV1,
    ) -> Result<(), ChangelogV3Error> {
        let prior = self
            .predecessor
            .administration()
            .map_or(0, |value| value.get());
        let started = command.started_audit().administration_sequence().get();
        let terminal = command.terminal_audit().administration_sequence().get();
        let valid_audit = if started <= prior {
            prior.checked_add(1) == Some(terminal)
        } else {
            prior.checked_add(1) == Some(started) && started.checked_add(1) == Some(terminal)
        };
        if self.covered.application() != Some(command.commit_sequence())
            || self.covered.administration()
                != Some(command.terminal_audit().administration_sequence())
            || !valid_audit
        {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        Ok(())
    }

    /// Exact logical predecessor, not a physical replication position.
    #[must_use]
    pub const fn predecessor(&self) -> DualFrontier {
        self.predecessor
    }

    /// Exact logical successor, not an acknowledged replication position.
    #[must_use]
    pub const fn covered(&self) -> DualFrontier {
        self.covered
    }

    /// Strict namespace/key order; each put owns its complete value.
    #[must_use]
    pub fn mutations(&self) -> &[AuthoritativeMutationV3] {
        &self.mutations
    }

    /// Bounded semantic charge, not a durable envelope size or reservation.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }
}

impl std::fmt::Debug for CommandPrefixEvidenceV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandPrefixEvidenceV1([redacted])")
    }
}

/// Checks complete logical order and reduces independent evidence to the exact
/// original physical transaction's corresponding net mutations. The original
/// receipt remains unchanged. All evidence bytes are charged before reduction,
/// even when later commands erase their earlier values from the net receipt.
///
/// This is one validation layer, not complete restore validation: the caller
/// must validate the original frame, join every capsule, check every first
/// observation against the validated predecessor, and validate all graph-owned
/// authority. In particular a net-zero change still needs that predecessor check.
/// Returns the first mutation of each distinct namespace/key in canonical order,
/// including keys erased by net-zero reduction. These borrow the bounded input;
/// the storage owner must check each one's `matches_prior` against its pinned
/// predecessor before using any prefix. No row payloads are copied for this list.
pub fn validate_command_prefix_mutations_v1<'a, P: std::borrow::Borrow<CommandPrefixEvidenceV1>>(
    evidence: &'a [P],
    original: &AuthoritativeTransactionV3,
) -> Result<Vec<&'a AuthoritativeMutationV3>, ChangelogV3Error> {
    if evidence.is_empty() || evidence.len() > MAX_STAGED_COMMANDS {
        return Err(ChangelogV3Error::LimitExceeded);
    }
    if !matches!(
        original.attribution(),
        ChangelogAttributionV3::JournaledApplicationGroup
            | ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup
    ) {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    let mut frontier = original.binding().predecessor_frontier;
    let mut bytes = 0_usize;
    let mut mutations = 0_usize;
    for command in evidence {
        let command = command.borrow();
        if command.predecessor() != frontier {
            return Err(ChangelogV3Error::PredecessorMismatch);
        }
        frontier = command.covered();
        bytes = bytes
            .checked_add(command.semantic_bytes())
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        mutations = mutations
            .checked_add(command.mutations().len())
            .ok_or(ChangelogV3Error::LimitExceeded)?;
        if bytes > MAX_STAGED_WRITE_BYTES || mutations > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
    }
    if frontier != original.binding().covered_frontier {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    let mut accumulator = AuthoritativeMutationAccumulatorV3::default();
    let mut first_observations = std::collections::BTreeMap::new();
    for command in evidence {
        let command = command.borrow();
        for mutation in command.mutations() {
            first_observations
                .entry((mutation.namespace(), mutation.key()))
                .or_insert(mutation);
            accumulator.record(mutation.clone())?;
        }
    }
    let net = accumulator.finish()?;
    if !net.iter().eq(original
        .mutations()
        .iter()
        .filter(|mutation| CommandPrefixEvidenceV1::supports_namespace(mutation.namespace())))
    {
        return Err(ChangelogV3Error::PredecessorMismatch);
    }
    Ok(first_observations.into_values().collect())
}
