//! Explicit semantic guarantee profiles for comparison adapters.

/// Whether an adapter matches a named RiffDB guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuaranteeLevel {
    /// The adapter exercises an equivalent guarantee for this workload.
    Matched,
    /// The adapter offers a narrower or implementation-specific substitute.
    Partial,
    /// The adapter deliberately does not implement the guarantee.
    Unsupported,
}

impl GuaranteeLevel {
    /// Returns the stable fixture spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Matched => "matched",
            Self::Partial => "partial",
            Self::Unsupported => "unsupported",
        }
    }
}

/// Machine-readable assumptions and semantic gaps for an adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuaranteeProfile {
    /// Stable adapter name.
    pub adapter: &'static str,
    /// Transaction isolation used by commands.
    pub isolation: &'static str,
    /// Acknowledgement/durability setting.
    pub durability: &'static str,
    /// Concurrency mechanism for a budget aggregate.
    pub conflict_control: &'static str,
    /// Entity/invariant semantics covered by the workload.
    pub command_invariants: GuaranteeLevel,
    /// Idempotent uncertainty recovery.
    pub idempotency: GuaranteeLevel,
    /// Contract durable events.
    pub durable_events: GuaranteeLevel,
    /// Commit provenance.
    pub provenance: GuaranteeLevel,
    /// Durable outbox intent.
    pub outbox: GuaranteeLevel,
    /// Derived projections/frontiers.
    pub projections: GuaranteeLevel,
    /// Shared capability/policy enforcement.
    pub authorization: GuaranteeLevel,
    /// Timestamp comparison policy.
    pub timestamp_observation: &'static str,
    /// Exact assumptions that bound the claim.
    pub assumptions: &'static [&'static str],
}

/// Returns the frozen PostgreSQL baseline guarantee profile.
pub const fn postgres_guarantee_profile() -> GuaranteeProfile {
    GuaranteeProfile {
        adapter: "postgresql-explicit-sql-v1",
        isolation: "READ COMMITTED per explicit transaction",
        durability: "PostgreSQL 18.4 with synchronous_commit=on, fsync=on, and full_page_writes=on; server-acknowledged commit only",
        conflict_control: "SELECT ... FOR UPDATE on the annual budget row",
        command_invariants: GuaranteeLevel::Matched,
        idempotency: GuaranteeLevel::Unsupported,
        durable_events: GuaranteeLevel::Unsupported,
        provenance: GuaranteeLevel::Unsupported,
        outbox: GuaranteeLevel::Unsupported,
        projections: GuaranteeLevel::Unsupported,
        authorization: GuaranteeLevel::Unsupported,
        timestamp_observation: "transaction_timestamp() is required but normalized out",
        assumptions: &[
            "The adapter is the only writer to its dedicated comparison table.",
            "Required-live evidence probes server_version_num=180004, synchronous_commit=on, fsync=on, and full_page_writes=on.",
            "Commands bound lock waits to 5 seconds and statements/idle transactions to 10 seconds.",
            "Connection loss after COMMIT remains uncertain because this baseline has no idempotency ledger.",
            "The comparison table is reset only inside a dedicated test database.",
            "Correctness preflight must pass before any timing result is reported.",
        ],
    }
}
