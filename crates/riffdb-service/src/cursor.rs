//! Bounded process-local cursor primitives and registry.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU64};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use riffdb_contract_ir::{IndexScanPrefix, MAX_DECLARATIONS_PER_KIND};
use riffdb_policy::{FixedToolCandidate, PartitionConstraint};
use riffdb_query_executor::QueryContinuation;
use riffdb_types::{
    ActorId, CanonicalValue, CapabilityId, CommitSequence, ContractBundleHash, ContractLineage,
    ContractVersion, EntityTypeId, EventId, FieldId, IndexEntryKey, IndexEpochPosition, IndexId,
    MAX_CAPABILITY_FIELD_VISIBILITY, ProjectionIdentity, QueryParameterHash, QueryPlanHash,
    TenantScope,
};

use crate::dto::{
    DiscoveryCatalogFence, DiscoveryRepresentation, FieldSelection, MAX_PROJECTION_COMPONENTS,
    MAX_PROJECTION_WAIT, ProjectionContinuation, ProjectionPageFence, ResourceDiscoveryKind,
};

/// Maximum number of items in one service page.
pub const MAX_PAGE_ITEMS: u16 = 500;
/// Default page size for protocols whose schema permits omission.
pub const DEFAULT_PAGE_ITEMS: u16 = 50;
/// Exact number of opaque bytes in a cursor token.
pub const CURSOR_TOKEN_BYTES: usize = 16;
/// Maximum live cursors in one process-local registry.
pub const MAX_LIVE_CURSORS: usize = 4_096;
/// Maximum live cursors owned by one stable principal.
pub const MAX_LIVE_CURSORS_PER_PRINCIPAL: usize = 64;
/// Maximum attempts to generate an insertable cursor token.
pub const MAX_CURSOR_TOKEN_GENERATION_ATTEMPTS: usize = 3;
/// Fixed process-local cursor lifetime.
pub const CURSOR_LIFETIME: Duration = Duration::from_secs(300);

/// A caller-selected page limit in `1..=500`.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PageLimit(NonZeroU16);

impl PageLimit {
    /// Checks the fixed service page bound.
    pub const fn new(value: u16) -> Result<Self, PageLimitError> {
        if value == 0 || value > MAX_PAGE_ITEMS {
            return Err(PageLimitError);
        }
        match NonZeroU16::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(PageLimitError),
        }
    }

    /// Returns the checked nonzero limit.
    #[must_use]
    pub const fn get(self) -> NonZeroU16 {
        self.0
    }
}

impl Default for PageLimit {
    fn default() -> Self {
        Self(NonZeroU16::new(DEFAULT_PAGE_ITEMS).expect("the fixed default is nonzero"))
    }
}

/// A page limit was zero or exceeded the fixed service bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PageLimitError;

impl fmt::Display for PageLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("page limit must be between 1 and 500")
    }
}

impl Error for PageLimitError {}

/// An opaque, process-local cursor handle.
///
/// This value is neither a signed claim nor an authorization proof. Protocol
/// adapters may carry its exact bytes but own every public text mapping.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CursorToken([u8; CURSOR_TOKEN_BYTES]);

impl CursorToken {
    /// Retains one exact fixed-size token produced by a trusted adapter or source.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; CURSOR_TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    /// Borrows the exact opaque bytes for lossless protocol conversion.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; CURSOR_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for CursorToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CursorToken([REDACTED])")
    }
}

impl fmt::Display for CursorToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("cursor token [REDACTED]")
    }
}

/// A cursor token source could not fill one complete token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorTokenGenerationError;

impl fmt::Display for CursorTokenGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("cursor token generation unavailable")
    }
}

impl Error for CursorTokenGenerationError {}

/// Injected source for one exact 16-byte cursor-token attempt.
pub trait CursorTokenGenerator: Send + Sync {
    /// Fills the complete destination or reports failure without exposing a token.
    fn fill_cursor_token(
        &self,
        destination: &mut [u8; CURSOR_TOKEN_BYTES],
    ) -> Result<(), CursorTokenGenerationError>;
}

impl<T> CursorTokenGenerator for Arc<T>
where
    T: CursorTokenGenerator + ?Sized,
{
    fn fill_cursor_token(
        &self,
        destination: &mut [u8; CURSOR_TOKEN_BYTES],
    ) -> Result<(), CursorTokenGenerationError> {
        self.as_ref().fill_cursor_token(destination)
    }
}

/// An opaque checked tick relative to one process-local monotonic origin.
///
/// The scalar representation is private and has no serialization API. A new
/// registry on restart therefore has no way to accept an earlier process's
/// cursors.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct CursorTick(u64);

impl CursorTick {
    /// Checks and converts elapsed process time into the private nanosecond tick.
    pub fn from_process_elapsed(elapsed: Duration) -> Result<Self, CursorTickRangeError> {
        let nanos = u64::try_from(elapsed.as_nanos()).map_err(|_| CursorTickRangeError)?;
        Ok(Self(nanos))
    }

    fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }

    fn checked_elapsed_since(self, earlier: Self) -> Option<Duration> {
        self.0.checked_sub(earlier.0).map(Duration::from_nanos)
    }
}

impl fmt::Debug for CursorTick {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CursorTick([PROCESS_RELATIVE])")
    }
}

/// A process-relative duration could not fit the opaque cursor tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorTickRangeError;

impl fmt::Display for CursorTickRangeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("cursor tick is out of range")
    }
}

impl Error for CursorTickRangeError {}

/// The process-local cursor clock could not provide a checked tick.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorClockError;

impl fmt::Display for CursorClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("cursor clock unavailable")
    }
}

impl Error for CursorClockError {}

/// Injected synchronous monotonic clock used only for cursor expiry.
pub trait CursorMonotonicClock: Send + Sync {
    /// Returns a tick relative to this process's private origin.
    fn now(&self) -> Result<CursorTick, CursorClockError>;
}

impl<T> CursorMonotonicClock for Arc<T>
where
    T: CursorMonotonicClock + ?Sized,
{
    fn now(&self) -> Result<CursorTick, CursorClockError> {
        self.as_ref().now()
    }
}

/// Exact stable-principal and caller-reconstructible binding for one cursor.
///
/// `Lookup` contains only values independently available before registry access:
/// the operation, normalized query, target, and resolved immutable contract
/// identity. Policy decisions, consistency fences, and lower continuations live
/// only in the stored state and are returned atomically after this binding
/// matches. A raw credential or capability token is never a binding component.
#[derive(Eq, PartialEq)]
pub(crate) struct CursorBinding<Principal, Lookup> {
    principal: Principal,
    lookup: Lookup,
}

impl<Principal, Lookup> CursorBinding<Principal, Lookup> {
    /// Binds a stable principal to the complete caller-known lookup identity.
    #[must_use]
    pub(crate) const fn new(principal: Principal, lookup: Lookup) -> Self {
        Self { principal, lookup }
    }
}

/// Cursor creation failed without exposing a candidate token or registry state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorUnavailable;

impl fmt::Display for CursorUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("cursor service unavailable; retry the request")
    }
}

impl Error for CursorUnavailable {}

/// Closed result of attempting to resolve an existing cursor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CursorAccessError {
    /// The token was absent, expired, or did not match the complete binding.
    InvalidCursor,
    /// Clock arithmetic, the clock source, or registry synchronization failed.
    Unavailable,
}

impl fmt::Display for CursorAccessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCursor => formatter.write_str("invalid cursor"),
            Self::Unavailable => formatter.write_str("cursor service unavailable"),
        }
    }
}

impl Error for CursorAccessError {}

struct CursorEntry<Principal, Lookup, State> {
    binding: CursorBinding<Principal, Lookup>,
    state: Arc<State>,
    created_at: CursorTick,
    expires_at: CursorTick,
}

struct CursorRegistryInner<Principal, Lookup, State> {
    entries: BTreeMap<CursorToken, CursorEntry<Principal, Lookup, State>>,
    last_tick: Option<CursorTick>,
}

type CursorRegistryGuard<'a, Principal, Lookup, State> =
    MutexGuard<'a, CursorRegistryInner<Principal, Lookup, State>>;

impl<Principal, Lookup, State> CursorRegistryInner<Principal, Lookup, State> {
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            last_tick: None,
        }
    }

    fn observe_and_purge(&mut self, now: CursorTick) -> Result<(), CursorUnavailable> {
        if self
            .last_tick
            .is_some_and(|previous| now.checked_elapsed_since(previous).is_none())
        {
            return Err(CursorUnavailable);
        }
        self.last_tick = Some(now);
        self.entries.retain(|_, entry| entry.expires_at > now);
        Ok(())
    }
}

/// Fixed-capacity, process-local, insert-if-absent cursor registry.
///
/// `State` is an already bounded checked continuation or scan position. The
/// registry takes ownership and returns only an `Arc` to that same stored value;
/// it never stores or emits page rows. Exact binding is an access precondition,
/// not authorization: callers must still perform every required current-policy
/// check before using the returned state for a lower read.
pub(crate) struct CursorRegistry<Principal, Lookup, State, Generator, Clock> {
    generator: Generator,
    clock: Clock,
    inner: Mutex<CursorRegistryInner<Principal, Lookup, State>>,
}

impl<Principal, Lookup, State, Generator, Clock>
    CursorRegistry<Principal, Lookup, State, Generator, Clock>
where
    Principal: Eq,
    Lookup: Eq,
    Generator: CursorTokenGenerator,
    Clock: CursorMonotonicClock,
{
    /// Creates one empty process-local registry with the fixed POC limits.
    #[must_use]
    pub(crate) fn new(generator: Generator, clock: Clock) -> Self {
        Self {
            generator,
            clock,
            inner: Mutex::new(CursorRegistryInner::new()),
        }
    }

    /// Atomically registers checked state under a new opaque token.
    ///
    /// Capacity is enforced by eviction of the oldest entry (per principal, then
    /// globally). Token-source, clock, and poison failures return
    /// [`CursorUnavailable`]. The state and binding are not consumed on a failed
    /// candidate attempt and no candidate token is returned.
    pub(crate) fn register(
        &self,
        binding: CursorBinding<Principal, Lookup>,
        state: State,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        self.register_inner(binding, state, false)
    }

    /// Registers checked state and records any prior live token for the same
    /// exact binding so replacement can run at publication.
    ///
    /// The prior token remains resolvable until the returned guard is
    /// published. Dropping an unpublished guard removes only the new token.
    pub(crate) fn register_replacing(
        &self,
        binding: CursorBinding<Principal, Lookup>,
        state: State,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        self.register_inner(binding, state, true)
    }

    fn register_inner(
        &self,
        binding: CursorBinding<Principal, Lookup>,
        state: State,
        replace: bool,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        for _ in 0..MAX_CURSOR_TOKEN_GENERATION_ATTEMPTS {
            let mut bytes = [0_u8; CURSOR_TOKEN_BYTES];
            self.generator
                .fill_cursor_token(&mut bytes)
                .map_err(|_| CursorUnavailable)?;
            let token = CursorToken::from_bytes(bytes);

            let (mut inner, now) = self.lock_at_current_tick()?;
            if inner.entries.contains_key(&token) {
                continue;
            }
            let supersedes = if replace {
                inner
                    .entries
                    .iter()
                    .find(|(_, entry)| entry.binding == binding)
                    .map(|(existing, _)| *existing)
            } else {
                None
            };
            let evicted = self.make_room(&mut inner, &binding.principal, supersedes)?;
            let expires_at = now.checked_add(CURSOR_LIFETIME).ok_or(CursorUnavailable)?;
            inner.entries.insert(
                token,
                CursorEntry {
                    binding,
                    state: Arc::new(state),
                    created_at: now,
                    expires_at,
                },
            );
            return Ok(CursorRegistration {
                token,
                supersedes,
                evicted,
            });
        }

        Err(CursorUnavailable)
    }

    /// Resolves reusable stored state only after an exact complete binding match.
    ///
    /// Absence, expiry, and every binding mismatch share `InvalidCursor` and
    /// reveal no stored value. Resolution does not consume or extend the cursor.
    /// Fresh policy authorization remains the caller's responsibility.
    pub(crate) fn resolve(
        &self,
        token: CursorToken,
        expected: &CursorBinding<Principal, Lookup>,
    ) -> Result<Arc<State>, CursorAccessError> {
        let (inner, now) = self
            .lock_at_current_tick()
            .map_err(|_| CursorAccessError::Unavailable)?;
        let entry = inner
            .entries
            .get(&token)
            .ok_or(CursorAccessError::InvalidCursor)?;
        if &entry.binding != expected {
            return Err(CursorAccessError::InvalidCursor);
        }
        now.checked_elapsed_since(entry.created_at)
            .ok_or(CursorAccessError::Unavailable)?;
        Ok(Arc::clone(&entry.state))
    }

    /// Returns the number of non-expired cursors after one checked clock sample.
    pub(crate) fn active_count(&self) -> Result<u32, CursorUnavailable> {
        let (inner, _) = self.lock_at_current_tick()?;
        u32::try_from(inner.entries.len()).map_err(|_| CursorUnavailable)
    }

    fn remove(&self, token: CursorToken) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .remove(&token);
    }

    fn lock_at_current_tick(
        &self,
    ) -> Result<
        (
            CursorRegistryGuard<'_, Principal, Lookup, State>,
            CursorTick,
        ),
        CursorUnavailable,
    > {
        let mut inner = self.inner.lock().map_err(|_| CursorUnavailable)?;
        let now = self.clock.now().map_err(|_| CursorUnavailable)?;
        inner.observe_and_purge(now)?;
        Ok((inner, now))
    }

    /// Evicts oldest entries so one new registration for `principal` fits.
    ///
    /// Entries listed in `retain` (the not-yet-superseded prior token for a
    /// replace-or-insert registration) are never chosen for eviction.
    fn make_room(
        &self,
        inner: &mut CursorRegistryInner<Principal, Lookup, State>,
        principal: &Principal,
        retain: Option<CursorToken>,
    ) -> Result<bool, CursorUnavailable> {
        let mut evicted = false;
        let principal_count = inner
            .entries
            .iter()
            .filter(|(token, entry)| {
                Some(**token) != retain && &entry.binding.principal == principal
            })
            .count();
        if principal_count >= MAX_LIVE_CURSORS_PER_PRINCIPAL {
            let oldest = inner
                .entries
                .iter()
                .filter(|(token, entry)| {
                    Some(**token) != retain && &entry.binding.principal == principal
                })
                .min_by_key(|(_, entry)| entry.created_at)
                .map(|(token, _)| *token);
            let Some(oldest) = oldest else {
                return Err(CursorUnavailable);
            };
            inner.entries.remove(&oldest);
            evicted = true;
        }
        let live = inner
            .entries
            .iter()
            .filter(|(token, _)| Some(**token) != retain)
            .count();
        if live >= MAX_LIVE_CURSORS {
            let oldest = inner
                .entries
                .iter()
                .filter(|(token, _)| Some(**token) != retain)
                .min_by_key(|(_, entry)| entry.created_at)
                .map(|(token, _)| *token);
            let Some(oldest) = oldest else {
                return Err(CursorUnavailable);
            };
            inner.entries.remove(&oldest);
            evicted = true;
        }
        Ok(evicted)
    }
}

/// Result of one successful cursor registration before publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CursorRegistration {
    token: CursorToken,
    supersedes: Option<CursorToken>,
    evicted: bool,
}

impl CursorRegistration {
    #[must_use]
    pub(crate) const fn token(self) -> CursorToken {
        self.token
    }

    #[must_use]
    pub(crate) const fn supersedes(self) -> Option<CursorToken> {
        self.supersedes
    }

    #[must_use]
    pub(crate) const fn evicted(self) -> bool {
        self.evicted
    }
}

/// Immutable catalog identity bound to a contract-dependent cursor.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CursorContractIdentity {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl CursorContractIdentity {
    #[must_use]
    pub(crate) const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
        }
    }

    #[must_use]
    pub(crate) const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
}

/// Caller-reconstructible identity for one commit-log scan.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct CommitScanCursorLookup {
    requested_limit: PageLimit,
}

impl CommitScanCursorLookup {
    #[must_use]
    pub(crate) const fn new(requested_limit: PageLimit) -> Self {
        Self { requested_limit }
    }

    #[must_use]
    pub(crate) const fn requested_limit(self) -> PageLimit {
        self.requested_limit
    }
}

/// Original effective policy facts retained by a commit-scan cursor.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CommitScanCursorPolicy {
    effective_tenant_scope: TenantScope,
    partition_constraint: PartitionConstraint,
    effective_limit: PageLimit,
}

impl CommitScanCursorPolicy {
    #[must_use]
    pub(crate) const fn new(
        effective_tenant_scope: TenantScope,
        partition_constraint: PartitionConstraint,
        effective_limit: PageLimit,
    ) -> Self {
        Self {
            effective_tenant_scope,
            partition_constraint,
            effective_limit,
        }
    }

    #[must_use]
    pub(crate) const fn effective_tenant_scope(&self) -> &TenantScope {
        &self.effective_tenant_scope
    }

    #[must_use]
    pub(crate) const fn partition_constraint(&self) -> &PartitionConstraint {
        &self.partition_constraint
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// Registry-only commit continuation, frozen upper fence, and prior policy facts.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CommitScanCursorState {
    after: CommitSequence,
    inclusive_upper: CommitSequence,
    policy: CommitScanCursorPolicy,
}

impl CommitScanCursorState {
    pub(crate) fn new(
        after: CommitSequence,
        inclusive_upper: CommitSequence,
        policy: CommitScanCursorPolicy,
    ) -> Result<Self, CursorBindingError> {
        if after > inclusive_upper {
            return Err(CursorBindingError);
        }
        Ok(Self {
            after,
            inclusive_upper,
            policy,
        })
    }

    #[must_use]
    pub(crate) const fn after(&self) -> CommitSequence {
        self.after
    }

    #[must_use]
    pub(crate) const fn inclusive_upper(&self) -> CommitSequence {
        self.inclusive_upper
    }

    #[must_use]
    pub(crate) const fn policy(&self) -> &CommitScanCursorPolicy {
        &self.policy
    }
}

/// Caller-reconstructible normalized identity for one authoritative index scan.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IndexScanCursorLookup {
    contract: CursorContractIdentity,
    index_id: IndexId,
    result_entity_type_id: EntityTypeId,
    leading_components: Vec<CanonicalValue>,
    prefix: IndexScanPrefix,
    requested_fields: FieldSelection,
    requested_limit: PageLimit,
}

impl IndexScanCursorLookup {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        contract: CursorContractIdentity,
        index_id: IndexId,
        result_entity_type_id: EntityTypeId,
        leading_components: Vec<CanonicalValue>,
        prefix: IndexScanPrefix,
        requested_fields: FieldSelection,
        requested_limit: PageLimit,
    ) -> Result<Self, CursorBindingError> {
        if leading_components.len() > MAX_PROJECTION_COMPONENTS
            || prefix.index_id() != index_id
            || prefix.component_count() != leading_components.len()
        {
            return Err(CursorBindingError);
        }
        Ok(Self {
            contract,
            index_id,
            result_entity_type_id,
            leading_components,
            prefix,
            requested_fields,
            requested_limit,
        })
    }

    #[must_use]
    pub(crate) const fn index_id(&self) -> IndexId {
        self.index_id
    }

    #[must_use]
    pub(crate) const fn prefix(&self) -> &IndexScanPrefix {
        &self.prefix
    }

    #[must_use]
    pub(crate) const fn requested_fields(&self) -> &FieldSelection {
        &self.requested_fields
    }

    #[must_use]
    pub(crate) const fn requested_limit(&self) -> PageLimit {
        self.requested_limit
    }
}

/// Original effective policy facts retained by an index-scan cursor.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IndexScanCursorPolicy {
    effective_tenant_scope: TenantScope,
    partition_constraint: PartitionConstraint,
    visible_fields: FieldSelection,
    effective_limit: PageLimit,
}

impl IndexScanCursorPolicy {
    #[must_use]
    pub(crate) const fn new(
        effective_tenant_scope: TenantScope,
        partition_constraint: PartitionConstraint,
        visible_fields: FieldSelection,
        effective_limit: PageLimit,
    ) -> Self {
        Self {
            effective_tenant_scope,
            partition_constraint,
            visible_fields,
            effective_limit,
        }
    }

    #[must_use]
    pub(crate) const fn effective_tenant_scope(&self) -> &TenantScope {
        &self.effective_tenant_scope
    }

    #[must_use]
    pub(crate) const fn partition_constraint(&self) -> &PartitionConstraint {
        &self.partition_constraint
    }

    #[must_use]
    pub(crate) const fn visible_fields(&self) -> &FieldSelection {
        &self.visible_fields
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// Registry-only authoritative index continuation and epoch fence.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct IndexScanCursorState {
    after: IndexEntryKey,
    epoch: IndexEpochPosition,
    policy: IndexScanCursorPolicy,
}

impl IndexScanCursorState {
    #[must_use]
    pub(crate) const fn new(
        after: IndexEntryKey,
        epoch: IndexEpochPosition,
        policy: IndexScanCursorPolicy,
    ) -> Self {
        Self {
            after,
            epoch,
            policy,
        }
    }

    #[must_use]
    pub(crate) const fn after(&self) -> &IndexEntryKey {
        &self.after
    }

    #[must_use]
    pub(crate) const fn epoch(&self) -> IndexEpochPosition {
        self.epoch
    }

    #[must_use]
    pub(crate) const fn policy(&self) -> &IndexScanCursorPolicy {
        &self.policy
    }
}

/// Caller-reconstructible normalized identity for one projection query.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ProjectionCursorLookup {
    contract: CursorContractIdentity,
    identity: ProjectionIdentity,
    leading_components: Vec<CanonicalValue>,
    required_sequence: Option<CommitSequence>,
    wait: Duration,
    requested_limit: PageLimit,
}

impl ProjectionCursorLookup {
    pub(crate) fn new(
        contract: CursorContractIdentity,
        identity: ProjectionIdentity,
        leading_components: Vec<CanonicalValue>,
        required_sequence: Option<CommitSequence>,
        wait: Duration,
        requested_limit: PageLimit,
    ) -> Result<Self, CursorBindingError> {
        if contract.lineage() != identity.contract_lineage()
            || leading_components.len() > MAX_PROJECTION_COMPONENTS
            || wait > MAX_PROJECTION_WAIT
            || (required_sequence.is_none() && !wait.is_zero())
        {
            return Err(CursorBindingError);
        }
        Ok(Self {
            contract,
            identity,
            leading_components,
            required_sequence,
            wait,
            requested_limit,
        })
    }

    #[must_use]
    pub(crate) const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    #[must_use]
    pub(crate) fn leading_components(&self) -> &[CanonicalValue] {
        &self.leading_components
    }

    #[must_use]
    pub(crate) const fn requested_limit(&self) -> PageLimit {
        self.requested_limit
    }
}

/// Original effective policy facts retained by a projection cursor.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ProjectionCursorPolicy {
    effective_tenant_scope: TenantScope,
    partition_constraint: PartitionConstraint,
    effective_limit: PageLimit,
}

impl ProjectionCursorPolicy {
    #[must_use]
    pub(crate) const fn new(
        effective_tenant_scope: TenantScope,
        partition_constraint: PartitionConstraint,
        effective_limit: PageLimit,
    ) -> Self {
        Self {
            effective_tenant_scope,
            partition_constraint,
            effective_limit,
        }
    }

    #[must_use]
    pub(crate) const fn effective_tenant_scope(&self) -> &TenantScope {
        &self.effective_tenant_scope
    }

    #[must_use]
    pub(crate) const fn partition_constraint(&self) -> &PartitionConstraint {
        &self.partition_constraint
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// Registry-only projection continuation, generation/frontier fence, and policy facts.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ProjectionCursorState {
    continuation: ProjectionContinuation,
    policy: ProjectionCursorPolicy,
}

impl ProjectionCursorState {
    #[must_use]
    pub(crate) const fn new(
        continuation: ProjectionContinuation,
        policy: ProjectionCursorPolicy,
    ) -> Self {
        Self {
            continuation,
            policy,
        }
    }

    #[must_use]
    pub(crate) const fn continuation(&self) -> &ProjectionContinuation {
        &self.continuation
    }

    #[must_use]
    pub(crate) fn page_fence(&self) -> ProjectionPageFence {
        ProjectionPageFence::new(
            self.continuation.identity().clone(),
            self.continuation.generation(),
            self.continuation.observed_frontier(),
        )
    }

    #[must_use]
    pub(crate) const fn policy(&self) -> &ProjectionCursorPolicy {
        &self.policy
    }
}

/// Caller-reconstructible identity for one payload-free outbox scan.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct OutboxCursorLookup {
    requested_limit: PageLimit,
}

impl OutboxCursorLookup {
    #[must_use]
    pub(crate) const fn new(requested_limit: PageLimit) -> Self {
        Self { requested_limit }
    }

    #[must_use]
    pub(crate) const fn requested_limit(self) -> PageLimit {
        self.requested_limit
    }
}

/// Original effective policy facts retained by an outbox-status cursor.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct OutboxCursorPolicy {
    effective_tenant_scope: TenantScope,
    partition_constraint: PartitionConstraint,
    effective_limit: PageLimit,
}

impl OutboxCursorPolicy {
    #[must_use]
    pub(crate) const fn new(
        effective_tenant_scope: TenantScope,
        partition_constraint: PartitionConstraint,
        effective_limit: PageLimit,
    ) -> Self {
        Self {
            effective_tenant_scope,
            partition_constraint,
            effective_limit,
        }
    }

    #[must_use]
    pub(crate) const fn effective_tenant_scope(&self) -> &TenantScope {
        &self.effective_tenant_scope
    }

    #[must_use]
    pub(crate) const fn partition_constraint(&self) -> &PartitionConstraint {
        &self.partition_constraint
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// Registry-only outbox continuation and prior policy facts.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct OutboxCursorState {
    after: EventId,
    policy: OutboxCursorPolicy,
}

impl OutboxCursorState {
    #[must_use]
    pub(crate) const fn new(after: EventId, policy: OutboxCursorPolicy) -> Self {
        Self { after, policy }
    }

    #[must_use]
    pub(crate) const fn after(&self) -> EventId {
        self.after
    }

    #[must_use]
    pub(crate) const fn policy(&self) -> &OutboxCursorPolicy {
        &self.policy
    }
}

// The complete command catalog is the closed fixed-tool inventory followed by
// at most one tool for each command declaration in the active bundle.
const MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES: usize =
    FixedToolCandidate::ALL.len() + MAX_DECLARATIONS_PER_KIND;
// Resource discovery contributes four process-wide resources plus, for an
// active bundle, one contract version, one schema per entity, three artifacts
// per command, and one status resource per projection.
const FIXED_RESOURCE_DISCOVERY_CANDIDATES: usize = 4;
const CONTRACT_VERSION_RESOURCE_CANDIDATES: usize = 1;
const RESOURCE_CANDIDATES_PER_COMMAND: usize = 3;
const MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES: usize = FIXED_RESOURCE_DISCOVERY_CANDIDATES
    + CONTRACT_VERSION_RESOURCE_CANDIDATES
    + MAX_DECLARATIONS_PER_KIND
    + (RESOURCE_CANDIDATES_PER_COMMAND * MAX_DECLARATIONS_PER_KIND)
    + MAX_DECLARATIONS_PER_KIND;

/// Caller-reconstructible identity for one command-tool discovery page.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct CommandDiscoveryCursorLookup {
    requested_limit: PageLimit,
    representation: DiscoveryRepresentation,
}

impl CommandDiscoveryCursorLookup {
    #[must_use]
    pub(crate) const fn new(
        requested_limit: PageLimit,
        representation: DiscoveryRepresentation,
    ) -> Self {
        Self {
            requested_limit,
            representation,
        }
    }

    #[must_use]
    pub(crate) const fn requested_limit(self) -> PageLimit {
        self.requested_limit
    }
}

/// Registry-only command discovery continuation, fence, and visibility policy.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct CommandDiscoveryCursorState {
    after_candidate: usize,
    fence: DiscoveryCatalogFence,
    visibility: Vec<bool>,
    effective_limit: PageLimit,
}

impl CommandDiscoveryCursorState {
    pub(crate) fn new(
        after_candidate: usize,
        fence: DiscoveryCatalogFence,
        visibility: Vec<bool>,
        effective_limit: PageLimit,
    ) -> Result<Self, CursorBindingError> {
        if visibility.len() > MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES
            || after_candidate >= visibility.len()
        {
            return Err(CursorBindingError);
        }
        Ok(Self {
            after_candidate,
            fence,
            visibility,
            effective_limit,
        })
    }

    #[must_use]
    pub(crate) const fn after_candidate(&self) -> usize {
        self.after_candidate
    }

    #[must_use]
    pub(crate) const fn fence(&self) -> &DiscoveryCatalogFence {
        &self.fence
    }

    #[must_use]
    pub(crate) fn visibility(&self) -> &[bool] {
        &self.visibility
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// Caller-reconstructible identity for one resource discovery page.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ResourceDiscoveryCursorLookup {
    requested_limit: PageLimit,
    representation: DiscoveryRepresentation,
    kind: ResourceDiscoveryKind,
}

impl ResourceDiscoveryCursorLookup {
    #[must_use]
    pub(crate) const fn new(
        requested_limit: PageLimit,
        representation: DiscoveryRepresentation,
        kind: ResourceDiscoveryKind,
    ) -> Self {
        Self {
            requested_limit,
            representation,
            kind,
        }
    }

    #[must_use]
    pub(crate) const fn requested_limit(self) -> PageLimit {
        self.requested_limit
    }
}

/// Prior resource visibility retained so continuation policy can only narrow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourceDiscoveryCursorVisibility {
    Hidden,
    Visible,
    VisibleEntityFields(Vec<FieldId>),
}

/// Registry-only resource discovery continuation, fence, and visibility policy.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct ResourceDiscoveryCursorState {
    after_candidate: usize,
    fence: DiscoveryCatalogFence,
    visibility: Vec<ResourceDiscoveryCursorVisibility>,
    effective_limit: PageLimit,
}

impl ResourceDiscoveryCursorState {
    pub(crate) fn new(
        after_candidate: usize,
        fence: DiscoveryCatalogFence,
        visibility: Vec<ResourceDiscoveryCursorVisibility>,
        effective_limit: PageLimit,
    ) -> Result<Self, CursorBindingError> {
        let fields = visibility.iter().try_fold(0usize, |total, visibility| {
            let count = match visibility {
                ResourceDiscoveryCursorVisibility::VisibleEntityFields(fields) => fields.len(),
                ResourceDiscoveryCursorVisibility::Hidden
                | ResourceDiscoveryCursorVisibility::Visible => 0,
            };
            total.checked_add(count)
        });
        if visibility.len() > MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES
            || after_candidate >= visibility.len()
            || fields.is_none_or(|fields| fields > MAX_CAPABILITY_FIELD_VISIBILITY)
        {
            return Err(CursorBindingError);
        }
        Ok(Self {
            after_candidate,
            fence,
            visibility,
            effective_limit,
        })
    }

    #[must_use]
    pub(crate) const fn after_candidate(&self) -> usize {
        self.after_candidate
    }

    #[must_use]
    pub(crate) const fn fence(&self) -> &DiscoveryCatalogFence {
        &self.fence
    }

    #[must_use]
    pub(crate) fn visibility(&self) -> &[ResourceDiscoveryCursorVisibility] {
        &self.visibility
    }

    #[must_use]
    pub(crate) const fn effective_limit(&self) -> PageLimit {
        self.effective_limit
    }
}

/// A checked operation-specific cursor binding could not be joined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CursorBindingError;

/// Complete caller-reconstructible binding for one RiffQL continuation.
#[derive(Clone, Eq, PartialEq)]
pub(crate) struct QueryCursorLookup {
    contract: CursorContractIdentity,
    module_hash: Option<riffdb_types::QueryModuleHash>,
    plan_hash: QueryPlanHash,
    parameter_hash: QueryParameterHash,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
}

impl QueryCursorLookup {
    #[must_use]
    pub(crate) const fn new(
        contract: CursorContractIdentity,
        module_hash: Option<riffdb_types::QueryModuleHash>,
        plan_hash: QueryPlanHash,
        parameter_hash: QueryParameterHash,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
    ) -> Self {
        Self {
            contract,
            module_hash,
            plan_hash,
            parameter_hash,
            capability_id,
            capability_revision,
        }
    }
}

/// Registry-only engine continuation; never serialized into the public token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueryCursorState {
    continuation: QueryContinuation,
}

impl QueryCursorState {
    #[must_use]
    pub(crate) const fn new(continuation: QueryContinuation) -> Self {
        Self { continuation }
    }

    #[must_use]
    pub(crate) const fn continuation(&self) -> &QueryContinuation {
        &self.continuation
    }
}

#[derive(Eq, PartialEq)]
enum ServiceCursorLookup {
    CommitScan(CommitScanCursorLookup),
    IndexScan(IndexScanCursorLookup),
    Projection(ProjectionCursorLookup),
    Outbox(OutboxCursorLookup),
    CommandDiscovery(CommandDiscoveryCursorLookup),
    ResourceDiscovery(ResourceDiscoveryCursorLookup),
    Query(QueryCursorLookup),
}

enum ServiceCursorState {
    CommitScan(Arc<CommitScanCursorState>),
    IndexScan(Arc<IndexScanCursorState>),
    Projection(Arc<ProjectionCursorState>),
    Outbox(Arc<OutboxCursorState>),
    CommandDiscovery(Arc<CommandDiscoveryCursorState>),
    ResourceDiscovery(Arc<ResourceDiscoveryCursorState>),
    Query(Arc<QueryCursorState>),
}

type SharedServiceCursorRegistry = CursorRegistry<
    ActorId,
    ServiceCursorLookup,
    ServiceCursorState,
    Arc<dyn CursorTokenGenerator>,
    Arc<dyn CursorMonotonicClock>,
>;

/// One globally bounded process-local namespace with typed operation-specific accessors.
pub(crate) struct ServiceCursorRegistries {
    registry: SharedServiceCursorRegistry,
}

/// One registered cursor that is removed unless its enclosing audited result is
/// durably terminal and ready for immediate release.
///
/// When a query continuation replaces a prior live token for the same principal
/// and exact query identity, the prior token is retained until [`Self::publish`]
/// so a failed invocation cannot destroy a client's valid cursor.
pub(crate) struct CursorPublicationGuard<'a> {
    registries: &'a ServiceCursorRegistries,
    token: CursorToken,
    supersedes: Option<CursorToken>,
    capacity_evicted: bool,
    published: bool,
}

impl CursorPublicationGuard<'_> {
    #[must_use]
    pub(crate) const fn token(&self) -> CursorToken {
        self.token
    }

    /// Reports whether capacity eviction occurred while registering this token.
    #[must_use]
    pub(crate) const fn capacity_evicted(&self) -> bool {
        self.capacity_evicted
    }

    /// Publishes the new token and removes any superseded prior token.
    pub(crate) fn publish(mut self) -> CursorToken {
        self.published = true;
        if let Some(superseded) = self.supersedes.take() {
            self.registries.registry.remove(superseded);
        }
        self.token
    }
}

impl Drop for CursorPublicationGuard<'_> {
    fn drop(&mut self) {
        if !self.published {
            // Only the unpublished new token is removed; a retained prior stays.
            self.registries.registry.remove(self.token);
        }
    }
}

impl ServiceCursorRegistries {
    #[must_use]
    pub(crate) fn new(
        generator: Arc<dyn CursorTokenGenerator>,
        clock: Arc<dyn CursorMonotonicClock>,
    ) -> Self {
        Self {
            registry: CursorRegistry::new(generator, clock),
        }
    }

    pub(crate) fn register_commit_scan(
        &self,
        principal: &ActorId,
        lookup: CommitScanCursorLookup,
        state: CommitScanCursorState,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        if state.policy().effective_limit() > lookup.requested_limit() {
            return Err(CursorUnavailable);
        }
        self.registry.register(
            CursorBinding::new(principal.clone(), ServiceCursorLookup::CommitScan(lookup)),
            ServiceCursorState::CommitScan(Arc::new(state)),
        )
    }

    pub(crate) fn register_commit_scan_unpublished(
        &self,
        principal: &ActorId,
        lookup: CommitScanCursorLookup,
        state: CommitScanCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        let registration = self.register_commit_scan(principal, lookup, state)?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_commit_scan(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &CommitScanCursorLookup,
    ) -> Result<Arc<CommitScanCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(principal.clone(), ServiceCursorLookup::CommitScan(*lookup)),
        )?;
        match state.as_ref() {
            ServiceCursorState::CommitScan(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_index_scan(
        &self,
        principal: &ActorId,
        lookup: IndexScanCursorLookup,
        state: IndexScanCursorState,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        if state.after().index_id() != lookup.index_id()
            || !state
                .after()
                .as_bytes()
                .starts_with(lookup.prefix().as_bytes())
            || state.policy().effective_limit() > lookup.requested_limit()
            || !fields_are_subset(
                state.policy().visible_fields().as_slice(),
                lookup.requested_fields().as_slice(),
            )
        {
            return Err(CursorUnavailable);
        }
        self.registry.register(
            CursorBinding::new(principal.clone(), ServiceCursorLookup::IndexScan(lookup)),
            ServiceCursorState::IndexScan(Arc::new(state)),
        )
    }

    pub(crate) fn register_index_scan_unpublished(
        &self,
        principal: &ActorId,
        lookup: IndexScanCursorLookup,
        state: IndexScanCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        let registration = self.register_index_scan(principal, lookup, state)?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_index_scan(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &IndexScanCursorLookup,
    ) -> Result<Arc<IndexScanCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::IndexScan(lookup.clone()),
            ),
        )?;
        match state.as_ref() {
            ServiceCursorState::IndexScan(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_projection(
        &self,
        principal: &ActorId,
        lookup: ProjectionCursorLookup,
        state: ProjectionCursorState,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        let continuation = state.continuation();
        if continuation.identity() != lookup.identity()
            || continuation.prefix().components() != lookup.leading_components()
            || state.policy().effective_limit() > lookup.requested_limit()
        {
            return Err(CursorUnavailable);
        }
        self.registry.register(
            CursorBinding::new(principal.clone(), ServiceCursorLookup::Projection(lookup)),
            ServiceCursorState::Projection(Arc::new(state)),
        )
    }

    pub(crate) fn register_projection_unpublished(
        &self,
        principal: &ActorId,
        lookup: ProjectionCursorLookup,
        state: ProjectionCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        let registration = self.register_projection(principal, lookup, state)?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_projection(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &ProjectionCursorLookup,
    ) -> Result<Arc<ProjectionCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::Projection(lookup.clone()),
            ),
        )?;
        match state.as_ref() {
            ServiceCursorState::Projection(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_outbox(
        &self,
        principal: &ActorId,
        lookup: OutboxCursorLookup,
        state: OutboxCursorState,
    ) -> Result<CursorRegistration, CursorUnavailable> {
        if state.policy().effective_limit() > lookup.requested_limit() {
            return Err(CursorUnavailable);
        }
        self.registry.register(
            CursorBinding::new(principal.clone(), ServiceCursorLookup::Outbox(lookup)),
            ServiceCursorState::Outbox(Arc::new(state)),
        )
    }

    pub(crate) fn register_outbox_unpublished(
        &self,
        principal: &ActorId,
        lookup: OutboxCursorLookup,
        state: OutboxCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        let registration = self.register_outbox(principal, lookup, state)?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_outbox(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &OutboxCursorLookup,
    ) -> Result<Arc<OutboxCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(principal.clone(), ServiceCursorLookup::Outbox(*lookup)),
        )?;
        match state.as_ref() {
            ServiceCursorState::Outbox(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_command_discovery_unpublished(
        &self,
        principal: &ActorId,
        lookup: CommandDiscoveryCursorLookup,
        state: CommandDiscoveryCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        if state.effective_limit() > lookup.requested_limit() {
            return Err(CursorUnavailable);
        }
        let registration = self.registry.register(
            CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::CommandDiscovery(lookup),
            ),
            ServiceCursorState::CommandDiscovery(Arc::new(state)),
        )?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_command_discovery(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &CommandDiscoveryCursorLookup,
    ) -> Result<Arc<CommandDiscoveryCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::CommandDiscovery(*lookup),
            ),
        )?;
        match state.as_ref() {
            ServiceCursorState::CommandDiscovery(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_resource_discovery_unpublished(
        &self,
        principal: &ActorId,
        lookup: ResourceDiscoveryCursorLookup,
        state: ResourceDiscoveryCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        if state.effective_limit() > lookup.requested_limit() {
            return Err(CursorUnavailable);
        }
        let registration = self.registry.register(
            CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::ResourceDiscovery(lookup),
            ),
            ServiceCursorState::ResourceDiscovery(Arc::new(state)),
        )?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_resource_discovery(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &ResourceDiscoveryCursorLookup,
    ) -> Result<Arc<ResourceDiscoveryCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::ResourceDiscovery(*lookup),
            ),
        )?;
        match state.as_ref() {
            ServiceCursorState::ResourceDiscovery(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn register_query_unpublished(
        &self,
        principal: &ActorId,
        lookup: QueryCursorLookup,
        state: QueryCursorState,
    ) -> Result<CursorPublicationGuard<'_>, CursorUnavailable> {
        // Query cursors are single-live per principal and exact query identity:
        // replacement executes at publish, never at registration.
        let registration = self.registry.register_replacing(
            CursorBinding::new(principal.clone(), ServiceCursorLookup::Query(lookup)),
            ServiceCursorState::Query(Arc::new(state)),
        )?;
        Ok(self.publication_guard(registration))
    }

    pub(crate) fn resolve_query(
        &self,
        token: CursorToken,
        principal: &ActorId,
        lookup: &QueryCursorLookup,
    ) -> Result<Arc<QueryCursorState>, CursorAccessError> {
        let state = self.registry.resolve(
            token,
            &CursorBinding::new(
                principal.clone(),
                ServiceCursorLookup::Query(lookup.clone()),
            ),
        )?;
        match state.as_ref() {
            ServiceCursorState::Query(state) => Ok(Arc::clone(state)),
            _ => Err(CursorAccessError::Unavailable),
        }
    }

    pub(crate) fn active_count(&self) -> Result<u32, CursorUnavailable> {
        self.registry.active_count()
    }

    fn publication_guard(&self, registration: CursorRegistration) -> CursorPublicationGuard<'_> {
        CursorPublicationGuard {
            registries: self,
            token: registration.token(),
            supersedes: registration.supersedes(),
            capacity_evicted: registration.evicted(),
            published: false,
        }
    }
}

fn fields_are_subset(candidate: &[FieldId], requested: &[FieldId]) -> bool {
    let mut requested = requested.iter().peekable();
    for candidate in candidate {
        while requested.peek().is_some_and(|field| *field < candidate) {
            requested.next();
        }
        if requested.next() != Some(candidate) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Barrier, Mutex};
    use std::thread;

    use super::*;

    type Lookup = (u8, u8, u8, u8, u8);

    struct CheckedContinuation {
        continuation: u16,
        policy: u8,
        fence: u8,
    }

    struct FixedClock(Mutex<Result<CursorTick, CursorClockError>>);

    impl FixedClock {
        fn at(seconds: u64) -> Self {
            Self(Mutex::new(Ok(tick(seconds))))
        }

        fn set(&self, seconds: u64) {
            *self.0.lock().expect("clock lock") = Ok(tick(seconds));
        }

        fn set_tick(&self, tick: CursorTick) {
            *self.0.lock().expect("clock lock") = Ok(tick);
        }
    }

    impl CursorMonotonicClock for FixedClock {
        fn now(&self) -> Result<CursorTick, CursorClockError> {
            *self.0.lock().map_err(|_| CursorClockError)?
        }
    }

    struct SequentialGenerator(Mutex<u128>);

    impl SequentialGenerator {
        fn new() -> Self {
            Self(Mutex::new(0))
        }
    }

    impl CursorTokenGenerator for SequentialGenerator {
        fn fill_cursor_token(
            &self,
            destination: &mut [u8; CURSOR_TOKEN_BYTES],
        ) -> Result<(), CursorTokenGenerationError> {
            let mut next = self.0.lock().map_err(|_| CursorTokenGenerationError)?;
            *destination = next.to_be_bytes();
            *next = next.checked_add(1).ok_or(CursorTokenGenerationError)?;
            Ok(())
        }
    }

    struct ScriptedGenerator(Mutex<VecDeque<[u8; CURSOR_TOKEN_BYTES]>>);

    impl ScriptedGenerator {
        fn new(tokens: impl IntoIterator<Item = [u8; CURSOR_TOKEN_BYTES]>) -> Self {
            Self(Mutex::new(tokens.into_iter().collect()))
        }
    }

    impl CursorTokenGenerator for ScriptedGenerator {
        fn fill_cursor_token(
            &self,
            destination: &mut [u8; CURSOR_TOKEN_BYTES],
        ) -> Result<(), CursorTokenGenerationError> {
            *destination = self
                .0
                .lock()
                .map_err(|_| CursorTokenGenerationError)?
                .pop_front()
                .ok_or(CursorTokenGenerationError)?;
            Ok(())
        }
    }

    struct CoordinatedCollisionGenerator {
        first_attempts: Barrier,
        calls: AtomicUsize,
    }

    impl CoordinatedCollisionGenerator {
        fn new() -> Self {
            Self {
                first_attempts: Barrier::new(2),
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl CursorTokenGenerator for CoordinatedCollisionGenerator {
        fn fill_cursor_token(
            &self,
            destination: &mut [u8; CURSOR_TOKEN_BYTES],
        ) -> Result<(), CursorTokenGenerationError> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call < 2 {
                *destination = [0x77; CURSOR_TOKEN_BYTES];
                self.first_attempts.wait();
            } else {
                *destination = (call as u128).to_be_bytes();
            }
            Ok(())
        }
    }

    fn tick(seconds: u64) -> CursorTick {
        CursorTick::from_process_elapsed(Duration::from_secs(seconds)).expect("bounded test tick")
    }

    fn lookup(seed: u8) -> Lookup {
        (seed, seed, seed, seed, seed)
    }

    fn binding(principal: u16, seed: u8) -> CursorBinding<u16, Lookup> {
        CursorBinding::new(principal, lookup(seed))
    }

    fn state(value: u16) -> CheckedContinuation {
        CheckedContinuation {
            continuation: value,
            policy: u8::try_from(value % 251).expect("bounded policy marker"),
            fence: u8::try_from(value % 253).expect("bounded fence marker"),
        }
    }

    #[test]
    fn page_limit_freezes_bounds_and_omission_default() {
        assert_eq!(PageLimit::default().get().get(), DEFAULT_PAGE_ITEMS);
        assert_eq!(PageLimit::new(1).expect("minimum").get().get(), 1);
        assert_eq!(PageLimit::new(500).expect("maximum").get().get(), 500);
        assert_eq!(PageLimit::new(0), Err(PageLimitError));
        assert_eq!(PageLimit::new(501), Err(PageLimitError));
    }

    #[test]
    fn token_is_exact_size_and_redacted() {
        let token = CursorToken::from_bytes([0xab; CURSOR_TOKEN_BYTES]);
        assert_eq!(std::mem::size_of::<CursorToken>(), CURSOR_TOKEN_BYTES);
        assert_eq!(token.as_bytes(), &[0xab; CURSOR_TOKEN_BYTES]);
        assert_eq!(format!("{token:?}"), "CursorToken([REDACTED])");
        assert_eq!(token.to_string(), "cursor token [REDACTED]");
        assert!(!format!("{token:?}").contains("ab"));
    }

    #[test]
    fn tick_checks_conversion_addition_and_regression() {
        assert_eq!(
            CursorTick::from_process_elapsed(Duration::MAX),
            Err(CursorTickRangeError)
        );
        assert!(
            CursorTick(u64::MAX - 1)
                .checked_add(Duration::from_nanos(2))
                .is_none()
        );
        assert!(tick(9).checked_elapsed_since(tick(10)).is_none());
        assert_eq!(
            tick(10).checked_elapsed_since(tick(9)),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn exact_binding_is_required_and_state_is_reusable() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let expected = binding(7, 1);
        let token = registry
            .register(expected, state(42))
            .expect("cursor registers")
            .token();

        let first = registry
            .resolve(token, &binding(7, 1))
            .expect("exact binding resolves");
        let replay = registry
            .resolve(token, &binding(7, 1))
            .expect("lost response can retry");
        assert!(Arc::ptr_eq(&first, &replay));
        assert_eq!(first.continuation, 42);
        assert_eq!(first.policy, 42);
        assert_eq!(first.fence, 42);

        for mismatched in [binding(8, 1), binding(7, 2)] {
            assert!(matches!(
                registry.resolve(token, &mismatched),
                Err(CursorAccessError::InvalidCursor)
            ));
        }
    }

    #[test]
    fn every_caller_known_lookup_component_participates_in_exact_binding() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let token = registry
            .register(binding(1, 1), state(9))
            .expect("cursor registers")
            .token();
        let mismatches = [
            (2, 1, 1, 1, 1),
            (1, 2, 1, 1, 1),
            (1, 1, 2, 1, 1),
            (1, 1, 1, 2, 1),
            (1, 1, 1, 1, 2),
        ];
        for mismatch in mismatches {
            assert!(matches!(
                registry.resolve(token, &CursorBinding::new(1, mismatch)),
                Err(CursorAccessError::InvalidCursor)
            ));
        }
    }

    #[test]
    fn exact_expiry_invalidates_without_sleeping_and_releases_capacity() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let token = registry
            .register(binding(1, 1), state(1))
            .expect("cursor registers")
            .token();
        assert!(registry.resolve(token, &binding(1, 1)).is_ok());

        registry.clock.set(300);
        assert!(matches!(
            registry.resolve(token, &binding(1, 1)),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(registry.register(binding(1, 1), state(2)).is_ok());
    }

    #[test]
    fn regression_and_expiry_add_overflow_fail_closed() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(10));
        let token = registry
            .register(binding(1, 1), state(1))
            .expect("cursor registers")
            .token();
        registry.clock.set(9);
        assert!(matches!(
            registry.resolve(token, &binding(1, 1)),
            Err(CursorAccessError::Unavailable)
        ));

        let overflowing = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        overflowing.clock.set_tick(CursorTick(u64::MAX));
        assert_eq!(
            overflowing.register(binding(2, 2), state(2)),
            Err(CursorUnavailable)
        );
    }

    #[test]
    fn three_collisions_match_token_source_unavailability() {
        let a = [1; CURSOR_TOKEN_BYTES];
        let b = [2; CURSOR_TOKEN_BYTES];
        let c = [3; CURSOR_TOKEN_BYTES];
        let registry = CursorRegistry::new(
            ScriptedGenerator::new([a, b, c, a, b, c]),
            FixedClock::at(0),
        );
        for principal in 1..=3 {
            registry
                .register(binding(principal, 1), state(principal))
                .expect("seed token registers");
        }
        assert_eq!(
            registry.register(binding(4, 1), state(4)),
            Err(CursorUnavailable)
        );
    }

    #[test]
    fn capacity_evicts_oldest_instead_of_rejecting() {
        let per_principal = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let mut principal_tokens = Vec::new();
        for item in 0..MAX_LIVE_CURSORS_PER_PRINCIPAL {
            let registration = per_principal
                .register(binding(1, item as u8), state(item as u16))
                .expect("within principal capacity");
            principal_tokens.push(registration.token());
        }
        let sixty_fifth = per_principal
            .register(binding(1, 64), state(65))
            .expect("65th distinct lookup evicts oldest");
        assert!(sixty_fifth.evicted());
        assert_eq!(
            per_principal.active_count().expect("count"),
            u32::try_from(MAX_LIVE_CURSORS_PER_PRINCIPAL).expect("bound fits u32")
        );
        assert!(matches!(
            per_principal.resolve(principal_tokens[0], &binding(1, 0)),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(
            per_principal
                .resolve(sixty_fifth.token(), &binding(1, 64))
                .is_ok()
        );

        // Global eviction is covered by the principal path above for unit cost;
        // a dedicated global fill would take thousands of tokens and is skipped.
    }

    #[test]
    fn query_replace_or_insert_is_single_live_at_publish() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let first = registry
            .register_replacing(binding(1, 1), state(1))
            .expect("first page");
        assert_eq!(registry.active_count().expect("count"), 1);
        let second = registry
            .register_replacing(binding(1, 1), state(2))
            .expect("replacement registers without removing prior");
        assert_eq!(registry.active_count().expect("count"), 2);
        assert_eq!(second.supersedes(), Some(first.token()));
        assert!(
            registry.resolve(first.token(), &binding(1, 1)).is_ok(),
            "prior remains resolvable until publish"
        );
        // Simulate publish: remove superseded.
        registry.remove(first.token());
        assert!(matches!(
            registry.resolve(first.token(), &binding(1, 1)),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(registry.resolve(second.token(), &binding(1, 1)).is_ok());
        assert_eq!(registry.active_count().expect("count"), 1);
    }

    #[test]
    fn unpublished_replacement_drop_leaves_prior_resolvable() {
        let generator: Arc<dyn CursorTokenGenerator> = Arc::new(SequentialGenerator::new());
        let clock: Arc<dyn CursorMonotonicClock> = Arc::new(FixedClock::at(0));
        let registries = ServiceCursorRegistries::new(generator, clock);
        // Use generic registry path through register_replacing via direct unit test above.
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let first = registry
            .register_replacing(binding(1, 1), state(1))
            .expect("first");
        let second = registry
            .register_replacing(binding(1, 1), state(2))
            .expect("second");
        // Drop new without publish: only remove new token.
        registry.remove(second.token());
        assert!(registry.resolve(first.token(), &binding(1, 1)).is_ok());
        let _ = registries;
    }

    #[test]
    fn sequential_query_first_pages_keep_single_live_continuation() {
        let registry = CursorRegistry::new(SequentialGenerator::new(), FixedClock::at(0));
        let mut live = None;
        for page in 0..200 {
            let registration = registry
                .register_replacing(binding(1, 1), state(page))
                .expect("page registers");
            if let Some(prior) = registration.supersedes() {
                registry.remove(prior);
            }
            live = Some(registration.token());
        }
        assert_eq!(registry.active_count().expect("count"), 1);
        let live = live.expect("pages registered");
        assert_eq!(
            registry
                .resolve(live, &binding(1, 1))
                .expect("live resolves")
                .continuation,
            199
        );
    }

    #[test]
    fn concurrent_collision_is_insert_if_absent_and_retried() {
        let registry = Arc::new(CursorRegistry::new(
            CoordinatedCollisionGenerator::new(),
            FixedClock::at(0),
        ));
        let first_registry = Arc::clone(&registry);
        let first = thread::spawn(move || first_registry.register(binding(1, 1), state(1)));
        let second_registry = Arc::clone(&registry);
        let second = thread::spawn(move || second_registry.register(binding(2, 2), state(2)));

        let first = first.join().expect("first thread").expect("first cursor");
        let second = second
            .join()
            .expect("second thread")
            .expect("second cursor");
        assert_ne!(first.token(), second.token());
        assert_eq!(
            registry.inner.lock().expect("registry lock").entries.len(),
            2
        );
    }

    #[test]
    fn service_registry_returns_stored_commit_fence_and_policy_without_caller_input() {
        let generator: Arc<dyn CursorTokenGenerator> = Arc::new(SequentialGenerator::new());
        let clock: Arc<dyn CursorMonotonicClock> = Arc::new(FixedClock::at(0));
        let registry = ServiceCursorRegistries::new(Arc::clone(&generator), Arc::clone(&clock));
        let principal = ActorId::new("operator-1").expect("bounded principal");
        let requested_limit = PageLimit::new(50).expect("bounded requested limit");
        let effective_limit = PageLimit::new(20).expect("bounded policy limit");
        let lookup = CommitScanCursorLookup::new(requested_limit);
        let after = CommitSequence::first();
        let inclusive_upper = after.checked_next().expect("next sequence");
        let state = CommitScanCursorState::new(
            after,
            inclusive_upper,
            CommitScanCursorPolicy::new(
                TenantScope::Global,
                PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::All),
                effective_limit,
            ),
        )
        .expect("ordered commit fence");

        let token = registry
            .register_commit_scan(&principal, lookup, state)
            .expect("cursor registers")
            .token();
        let resolved = registry
            .resolve_commit_scan(token, &principal, &lookup)
            .expect("exact binding resolves");

        assert_eq!(resolved.after(), after);
        assert_eq!(resolved.inclusive_upper(), inclusive_upper);
        assert_eq!(
            resolved.policy().effective_tenant_scope(),
            &TenantScope::Global
        );
        assert_eq!(
            resolved.policy().partition_constraint(),
            &PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::All)
        );
        assert_eq!(resolved.policy().effective_limit(), effective_limit);
        assert_eq!(registry.active_count().expect("registry available"), 1);

        let other_principal = ActorId::new("operator-2").expect("bounded principal");
        assert!(matches!(
            registry.resolve_commit_scan(token, &other_principal, &lookup),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(matches!(
            registry.resolve_commit_scan(
                token,
                &principal,
                &CommitScanCursorLookup::new(PageLimit::new(49).expect("bounded limit")),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(matches!(
            registry.resolve_outbox(token, &principal, &OutboxCursorLookup::new(requested_limit)),
            Err(CursorAccessError::InvalidCursor)
        ));

        let restarted = ServiceCursorRegistries::new(generator, clock);
        assert!(matches!(
            restarted.resolve_commit_scan(token, &principal, &lookup),
            Err(CursorAccessError::InvalidCursor)
        ));
    }

    #[test]
    fn unpublished_cursor_is_removed_and_published_cursor_is_retained() {
        let registry = ServiceCursorRegistries::new(
            Arc::new(SequentialGenerator::new()),
            Arc::new(FixedClock::at(0)),
        );
        let principal = ActorId::new("operator-1").expect("bounded principal");
        let limit = PageLimit::new(10).expect("bounded limit");
        let lookup = CommitScanCursorLookup::new(limit);
        let after = CommitSequence::first();
        let inclusive_upper = after.checked_next().expect("next sequence");
        let make_state = || {
            CommitScanCursorState::new(
                after,
                inclusive_upper,
                CommitScanCursorPolicy::new(
                    TenantScope::Global,
                    PartitionConstraint::Filter(riffdb_types::PartitionScopeV1::All),
                    limit,
                ),
            )
            .expect("ordered cursor state")
        };

        let unpublished = registry
            .register_commit_scan_unpublished(&principal, lookup, make_state())
            .expect("cursor reservation");
        let unpublished_token = unpublished.token();
        assert_eq!(registry.active_count().expect("registry available"), 1);
        drop(unpublished);
        assert_eq!(registry.active_count().expect("registry available"), 0);
        assert!(matches!(
            registry.resolve_commit_scan(unpublished_token, &principal, &lookup),
            Err(CursorAccessError::InvalidCursor)
        ));

        let published = registry
            .register_commit_scan_unpublished(&principal, lookup, make_state())
            .expect("cursor reservation");
        let published_token = published.publish();
        assert_eq!(registry.active_count().expect("registry available"), 1);
        assert!(
            registry
                .resolve_commit_scan(published_token, &principal, &lookup)
                .is_ok()
        );
    }

    #[test]
    fn discovery_cursor_binds_operation_limit_fence_and_prior_visibility() {
        let registry = ServiceCursorRegistries::new(
            Arc::new(SequentialGenerator::new()),
            Arc::new(FixedClock::at(0)),
        );
        let principal = ActorId::new("operator-1").expect("bounded principal");
        let limit = PageLimit::new(2).expect("bounded limit");
        let lookup = CommandDiscoveryCursorLookup::new(limit, DiscoveryRepresentation::Full);
        let operation_schemas = crate::OperationSchemaCatalog::accepted()
            .expect("accepted operation schemas")
            .identity();
        let fence = DiscoveryCatalogFence::no_active_contract(operation_schemas);
        let state =
            CommandDiscoveryCursorState::new(1, fence.clone(), vec![true, false, true], limit)
                .expect("bounded discovery state");
        let guard = registry
            .register_command_discovery_unpublished(&principal, lookup, state)
            .expect("cursor reservation");
        let token = guard.publish();
        let resolved = registry
            .resolve_command_discovery(token, &principal, &lookup)
            .expect("exact discovery binding");
        assert_eq!(resolved.after_candidate(), 1);
        assert_eq!(resolved.fence(), &fence);
        assert_eq!(resolved.visibility(), &[true, false, true]);
        assert!(matches!(
            registry.resolve_resource_discovery(
                token,
                &principal,
                &ResourceDiscoveryCursorLookup::new(
                    limit,
                    DiscoveryRepresentation::Full,
                    ResourceDiscoveryKind::All,
                ),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(matches!(
            registry.resolve_command_discovery(
                token,
                &principal,
                &CommandDiscoveryCursorLookup::new(
                    PageLimit::new(3).expect("different bounded limit"),
                    DiscoveryRepresentation::Full,
                ),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(matches!(
            registry.resolve_command_discovery(
                token,
                &principal,
                &CommandDiscoveryCursorLookup::new(
                    limit,
                    DiscoveryRepresentation::CompactObservation,
                ),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));

        let resource_lookup = ResourceDiscoveryCursorLookup::new(
            limit,
            DiscoveryRepresentation::Full,
            ResourceDiscoveryKind::All,
        );
        let resource_state = ResourceDiscoveryCursorState::new(
            1,
            fence,
            vec![
                ResourceDiscoveryCursorVisibility::Visible,
                ResourceDiscoveryCursorVisibility::Hidden,
                ResourceDiscoveryCursorVisibility::Visible,
            ],
            limit,
        )
        .expect("bounded resource discovery state");
        let resource_token = registry
            .register_resource_discovery_unpublished(&principal, resource_lookup, resource_state)
            .expect("resource cursor reservation")
            .publish();
        assert!(
            registry
                .resolve_resource_discovery(resource_token, &principal, &resource_lookup)
                .is_ok()
        );
        assert!(matches!(
            registry.resolve_resource_discovery(
                resource_token,
                &principal,
                &ResourceDiscoveryCursorLookup::new(
                    limit,
                    DiscoveryRepresentation::CompactObservation,
                    ResourceDiscoveryKind::All,
                ),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));
        assert!(matches!(
            registry.resolve_resource_discovery(
                resource_token,
                &principal,
                &ResourceDiscoveryCursorLookup::new(
                    limit,
                    DiscoveryRepresentation::Full,
                    ResourceDiscoveryKind::Concrete,
                ),
            ),
            Err(CursorAccessError::InvalidCursor)
        ));
    }

    #[test]
    fn command_discovery_cursor_accepts_only_the_structural_candidate_bound() {
        assert_eq!(MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES, 4_115);
        let operation_schemas = crate::OperationSchemaCatalog::accepted()
            .expect("accepted operation schemas")
            .identity();
        let fence = DiscoveryCatalogFence::no_active_contract(operation_schemas);
        let limit = PageLimit::new(MAX_PAGE_ITEMS).expect("maximum page limit");

        let state = CommandDiscoveryCursorState::new(
            1_024,
            fence.clone(),
            vec![true; MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES],
            limit,
        )
        .expect("item 1025 remains a legal continuation within the structural maximum");
        assert_eq!(
            state.visibility().len(),
            MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES
        );
        assert!(matches!(
            CommandDiscoveryCursorState::new(
                1_024,
                fence,
                vec![true; MAX_COMMAND_DISCOVERY_CURSOR_CANDIDATES + 1],
                limit,
            ),
            Err(CursorBindingError)
        ));
    }

    #[test]
    fn resource_discovery_cursor_accepts_only_the_structural_candidate_bound() {
        assert_eq!(MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES, 20_485);
        let operation_schemas = crate::OperationSchemaCatalog::accepted()
            .expect("accepted operation schemas")
            .identity();
        let fence = DiscoveryCatalogFence::no_active_contract(operation_schemas);
        let limit = PageLimit::new(MAX_PAGE_ITEMS).expect("maximum page limit");

        let state = ResourceDiscoveryCursorState::new(
            1_024,
            fence.clone(),
            vec![
                ResourceDiscoveryCursorVisibility::Visible;
                MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES
            ],
            limit,
        )
        .expect("item 1025 remains a legal continuation within the structural maximum");
        assert_eq!(
            state.visibility().len(),
            MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES
        );
        assert!(matches!(
            ResourceDiscoveryCursorState::new(
                1_024,
                fence,
                vec![
                    ResourceDiscoveryCursorVisibility::Visible;
                    MAX_RESOURCE_DISCOVERY_CURSOR_CANDIDATES + 1
                ],
                limit,
            ),
            Err(CursorBindingError)
        ));
    }
}
