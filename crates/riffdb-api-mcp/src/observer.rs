//! Bounded transport-neutral MCP observation state.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard, TryLockError, Weak,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
    },
    task::{Context, Poll},
    time::Duration,
};

use futures::task::AtomicWaker;
#[cfg(feature = "stdio")]
use tokio::time::Instant;

use crate::{
    McpAdmissionError, McpListChangeKind, McpObserverSemaphore, McpResourceLocator, McpTelemetry,
    McpTelemetryEvent, NoopMcpTelemetry, parse_resource_locator,
};

/// Maximum distinct subscribed resource locators per MCP session.
pub const MAX_MCP_SUBSCRIPTIONS: usize = 8;
/// Maximum retained visible items in either compact discovery inventory.
pub const MAX_MCP_VISIBLE_FINGERPRINTS: usize = 1_024;
/// Maximum bytes retained for one compact MCP-visible fingerprint.
pub const MAX_MCP_COMPACT_FINGERPRINT_BYTES: usize = 4_096;
/// Maximum notification markers retained by one session.
pub const MAX_MCP_PENDING_NOTIFICATION_MARKERS: usize = 10;
/// Exact compact-observation service page limit.
pub const MCP_OBSERVER_DISCOVERY_PAGE_LIMIT: u16 = 500;
/// Maximum compact pages read for either inventory during one tick.
pub const MAX_MCP_OBSERVER_DISCOVERY_CALLS: u8 = 3;
/// Maximum compact items returned and structurally validated per inventory/tick.
pub const MAX_MCP_OBSERVER_RETURNED_ITEMS: usize = 1_500;
/// Exact monotonic cadence injected by transport composition.
pub const MCP_OBSERVER_TICK_INTERVAL: Duration = Duration::from_secs(5);
/// Maximum absolute observation lifetime for one session.
pub const MAX_MCP_OBSERVER_LIFETIME: Duration = Duration::from_secs(900);
/// Maximum admitted observation ticks for one session.
pub const MAX_MCP_OBSERVER_TICKS: u16 = 180;
/// Maximum watcher-generated logical observer operations for one tick.
pub const MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_TICK: u32 = 14;
/// Maximum watcher-generated logical observer operations for one session.
pub const MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_SESSION: u32 = 2_520;

const MCP_VISIBLE_FINGERPRINT_PREFIX: &[u8] = b"riffdb.mcp-visible-fingerprint/v1\0";

#[cfg(any(feature = "stdio", feature = "streamable-http"))]
pub(crate) type McpObserverSchedulerFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Duration, McpObserverLoopError>> + Send + 'a>>;

/// Adapter-local monotonic scheduler injected into the common observer driver.
#[cfg(any(feature = "stdio", feature = "streamable-http"))]
pub(crate) trait McpObserverScheduler: Send + 'static {
    fn next_tick(&mut self) -> McpObserverSchedulerFuture<'_>;
}

/// Production Tokio adapter for the injected observer scheduler.
#[cfg(feature = "stdio")]
pub(crate) struct SystemMcpObserverScheduler {
    origin: Instant,
    next_tick_at: Instant,
}

#[cfg(feature = "stdio")]
impl SystemMcpObserverScheduler {
    pub(crate) fn new() -> Result<Self, McpObserverLoopError> {
        let origin = Instant::now();
        let next_tick_at = origin
            .checked_add(MCP_OBSERVER_TICK_INTERVAL)
            .ok_or(McpObserverLoopError::ClockUnavailable)?;
        Ok(Self {
            origin,
            next_tick_at,
        })
    }
}

#[cfg(feature = "stdio")]
impl McpObserverScheduler for SystemMcpObserverScheduler {
    fn next_tick(&mut self) -> McpObserverSchedulerFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep_until(self.next_tick_at).await;
            let now = self.origin.elapsed();
            self.next_tick_at = Instant::now()
                .checked_add(MCP_OBSERVER_TICK_INTERVAL)
                .ok_or(McpObserverLoopError::ClockUnavailable)?;
            Ok(now)
        })
    }
}

#[cfg(feature = "stdio")]
impl fmt::Debug for SystemMcpObserverScheduler {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemMcpObserverScheduler")
    }
}

/// One bounded exact compact MCP-visible descriptor fingerprint.
#[derive(Clone, Eq, PartialEq)]
pub struct McpVisibleFingerprint(Vec<u8>);

struct McpCommandPlanFingerprintInput<'a> {
    uri: &'a str,
    contract_lineage: &'a str,
    contract_version: u64,
    command_id: u32,
    source_command: &'a str,
    plan_hash: &'a [u8],
    input_schema_hash: &'a [u8],
    outcome_schema_hash: &'a [u8],
}

impl McpVisibleFingerprint {
    /// Retains one nonempty bounded canonical structural fingerprint.
    pub fn new(bytes: Vec<u8>) -> Result<Self, McpObserverError> {
        if bytes.is_empty() || bytes.len() > MAX_MCP_COMPACT_FINGERPRINT_BYTES {
            return Err(McpObserverError::LimitExceeded);
        }
        Ok(Self(bytes))
    }

    /// Borrows the canonical structural bytes without interpreting them.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Constructs the common identity of one accepted fixed tool.
    pub fn fixed_tool(kind: u8) -> Result<Self, McpObserverError> {
        let registry = crate::fixed_tool_registry().map_err(|_| McpObserverError::LimitExceeded)?;
        if !registry.tools().iter().any(|tool| tool.kind() == kind) {
            return Err(McpObserverError::Unsupported);
        }
        Self::from_components(1, &[&[kind]])
    }

    /// Constructs the common visible identity of one compiled command tool.
    pub fn command_tool(
        name: &str,
        source_command: &str,
        contract_lineage: &str,
        contract_version: u64,
        command_id: u32,
        input_schema_hash: &[u8],
        outcome_schema_hash: &[u8],
    ) -> Result<Self, McpObserverError> {
        crate::validate_command_tool_name(name).map_err(|_| McpObserverError::Unsupported)?;
        crate::format_command_plan_locator_from_public(contract_lineage, command_id)
            .map_err(|_| McpObserverError::Unsupported)?;
        if source_command.is_empty()
            || contract_version == 0
            || input_schema_hash.len() != 32
            || outcome_schema_hash.len() != 32
        {
            return Err(McpObserverError::Unsupported);
        }
        Self::from_components(
            2,
            &[
                name.as_bytes(),
                source_command.as_bytes(),
                contract_lineage.as_bytes(),
                &contract_version.to_be_bytes(),
                &command_id.to_be_bytes(),
                input_schema_hash,
                outcome_schema_hash,
            ],
        )
    }

    /// Constructs the common visible identity of one deployed named-query tool.
    #[allow(clippy::too_many_arguments)]
    pub fn named_query_tool(
        name: &str,
        source_query: &str,
        contract_lineage: &str,
        contract_version: u64,
        module_name: &str,
        module_version: u64,
        module_hash: &[u8],
        input_schema_hash: &[u8],
        result_schema_hash: &[u8],
    ) -> Result<Self, McpObserverError> {
        if name.is_empty()
            || name.len() > 128
            || !name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || source_query.is_empty()
            || contract_lineage.is_empty()
            || contract_version == 0
            || module_name.is_empty()
            || module_version == 0
            || module_hash.len() != 32
            || input_schema_hash.len() != 32
            || result_schema_hash.len() != 32
        {
            return Err(McpObserverError::Unsupported);
        }
        Self::from_components(
            5,
            &[
                name.as_bytes(),
                source_query.as_bytes(),
                contract_lineage.as_bytes(),
                &contract_version.to_be_bytes(),
                module_name.as_bytes(),
                &module_version.to_be_bytes(),
                module_hash,
                input_schema_hash,
                result_schema_hash,
            ],
        )
    }

    /// Constructs the common visible identity of one checked resource descriptor.
    pub fn resource_descriptor(
        descriptor: &crate::McpResourceDescriptor,
    ) -> Result<Self, McpObserverError> {
        Self::from_components(
            3,
            &[
                descriptor.descriptor_branch().as_bytes(),
                descriptor.uri().as_bytes(),
            ],
        )
    }

    /// Constructs compact typed comparison state for one exact command plan.
    ///
    /// The existing plan and compiler-schema hashes replace the potentially
    /// large rendered explanation and schema documents. No new digest is
    /// computed, and the complete resource body is not retained.
    pub fn command_plan_resource(
        uri: &str,
        command: &crate::McpExplainedCommandPresentation,
    ) -> Result<Self, McpObserverError> {
        let plan_hash = command.plan_hash();
        let input_schema_hash = command.input_schema().schema_hash_bytes();
        let outcome_schema_hash = command.outcome_schema().schema_hash_bytes();
        command_plan_fingerprint(McpCommandPlanFingerprintInput {
            uri,
            contract_lineage: command.contract().contract_lineage(),
            contract_version: command.contract().contract_version(),
            command_id: command.explanation().command_id(),
            source_command: command.source_command(),
            plan_hash: &plan_hash,
            input_schema_hash: &input_schema_hash,
            outcome_schema_hash: &outcome_schema_hash,
        })
    }

    pub(crate) fn subscribed_resource_content(
        uri: &str,
        canonical_content: &[u8],
    ) -> Result<Self, McpObserverError> {
        let locator = parse_resource_locator(uri).map_err(|_| McpObserverError::Unsupported)?;
        if !subscribable(&locator) || canonical_content.is_empty() {
            return Err(McpObserverError::Unsupported);
        }
        Self::from_components(4, &[uri.as_bytes(), canonical_content])
    }

    fn from_components(kind: u8, components: &[&[u8]]) -> Result<Self, McpObserverError> {
        let mut encoded_len = MCP_VISIBLE_FINGERPRINT_PREFIX
            .len()
            .checked_add(1)
            .ok_or(McpObserverError::LimitExceeded)?;
        for component in components {
            encoded_len = encoded_len
                .checked_add(4)
                .and_then(|length| length.checked_add(component.len()))
                .ok_or(McpObserverError::LimitExceeded)?;
            if encoded_len > MAX_MCP_COMPACT_FINGERPRINT_BYTES {
                return Err(McpObserverError::LimitExceeded);
            }
        }
        let mut bytes = Vec::with_capacity(encoded_len);
        bytes.extend_from_slice(MCP_VISIBLE_FINGERPRINT_PREFIX);
        bytes.push(kind);
        for component in components {
            let length =
                u32::try_from(component.len()).map_err(|_| McpObserverError::LimitExceeded)?;
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(component);
            if bytes.len() > MAX_MCP_COMPACT_FINGERPRINT_BYTES {
                return Err(McpObserverError::LimitExceeded);
            }
        }
        Self::new(bytes)
    }
}

fn command_plan_fingerprint(
    input: McpCommandPlanFingerprintInput<'_>,
) -> Result<McpVisibleFingerprint, McpObserverError> {
    match parse_resource_locator(input.uri).map_err(|_| McpObserverError::Unsupported)? {
        McpResourceLocator::CommandPlan {
            lineage,
            command_id: locator_command_id,
        } if lineage.as_str() == input.contract_lineage
            && locator_command_id.get() == input.command_id => {}
        _ => return Err(McpObserverError::Unsupported),
    }
    if input.contract_version == 0
        || !is_source_name(input.source_command)
        || input.plan_hash.len() != 32
        || input.input_schema_hash.len() != 32
        || input.outcome_schema_hash.len() != 32
    {
        return Err(McpObserverError::Unsupported);
    }
    McpVisibleFingerprint::from_components(
        5,
        &[
            input.uri.as_bytes(),
            input.contract_lineage.as_bytes(),
            &input.contract_version.to_be_bytes(),
            &input.command_id.to_be_bytes(),
            input.source_command.as_bytes(),
            input.plan_hash,
            input.input_schema_hash,
            input.outcome_schema_hash,
        ],
    )
}

fn is_source_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && value.len() <= 256
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

impl fmt::Debug for McpVisibleFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpVisibleFingerprint([REDACTED])")
    }
}

/// Inventory selected for one compact watcher discovery operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObservedInventory {
    /// Policy-filtered fixed and compiled command tools.
    Tools,
    /// Policy-filtered complete resource and template inventory.
    Resources,
}

/// One initial or continuation compact-observation request.
///
/// A backend maps its associated presentation-fence type directly to the
/// service or public-gRPC boundary. The common loop never serializes, invents,
/// or interprets that fence.
#[derive(Clone, Copy)]
pub struct McpCompactObservationRequest<'a, Fence> {
    cursor: Option<[u8; 16]>,
    prior_fence: Option<&'a Fence>,
}

impl<'a, Fence> McpCompactObservationRequest<'a, Fence> {
    fn initial(prior_fence: Option<&'a Fence>) -> Self {
        Self {
            cursor: None,
            prior_fence,
        }
    }

    fn continuation(cursor: [u8; 16]) -> Self {
        Self {
            cursor: Some(cursor),
            prior_fence: None,
        }
    }

    #[cfg(feature = "streamable-http")]
    pub(crate) fn from_owned_parts(
        cursor: Option<[u8; 16]>,
        prior_fence: Option<&'a Fence>,
    ) -> Self {
        Self {
            cursor,
            prior_fence,
        }
    }

    /// Returns the exact opaque continuation cursor.
    #[must_use]
    pub const fn cursor(&self) -> Option<[u8; 16]> {
        self.cursor
    }

    /// Borrows the prior common presentation fence on an initial request.
    #[must_use]
    pub const fn prior_fence(&self) -> Option<&Fence> {
        self.prior_fence
    }

    /// Returns the invariant compact-observation page limit.
    #[must_use]
    pub const fn limit(&self) -> u16 {
        MCP_OBSERVER_DISCOVERY_PAGE_LIMIT
    }
}

impl<Fence> fmt::Debug for McpCompactObservationRequest<'_, Fence> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCompactObservationRequest([REDACTED])")
    }
}

/// One checked compact discovery page under an exact presentation fence.
#[derive(Clone, Eq, PartialEq)]
pub struct McpCompactObservationPage<Fence> {
    fingerprints: Vec<McpVisibleFingerprint>,
    next_cursor: Option<[u8; 16]>,
    observed_fence: Fence,
}

impl<Fence> McpCompactObservationPage<Fence> {
    /// Creates one structurally bounded compact page.
    pub fn new(
        fingerprints: Vec<McpVisibleFingerprint>,
        next_cursor: Option<[u8; 16]>,
        observed_fence: Fence,
    ) -> Result<Self, McpObserverError> {
        if fingerprints.len() > usize::from(MCP_OBSERVER_DISCOVERY_PAGE_LIMIT)
            || (fingerprints.is_empty() && next_cursor.is_some())
        {
            return Err(McpObserverError::LimitExceeded);
        }
        Ok(Self {
            fingerprints,
            next_cursor,
            observed_fence,
        })
    }

    /// Borrows the ordered compact MCP-visible fingerprints.
    #[must_use]
    pub fn fingerprints(&self) -> &[McpVisibleFingerprint] {
        &self.fingerprints
    }

    /// Returns the exact opaque continuation cursor, when present.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<[u8; 16]> {
        self.next_cursor
    }

    /// Borrows the exact presentation fence observed by this page.
    #[must_use]
    pub const fn observed_fence(&self) -> &Fence {
        &self.observed_fence
    }
}

impl<Fence> fmt::Debug for McpCompactObservationPage<Fence> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCompactObservationPage([REDACTED])")
    }
}

/// Closed result of one compact conditional discovery call.
#[derive(Clone, Eq, PartialEq)]
pub enum McpCompactObservationResult<Fence> {
    /// The current fence exactly equals the supplied initial prior fence.
    CatalogUnchanged(Fence),
    /// A first or continuation compact page.
    Page(McpCompactObservationPage<Fence>),
}

impl<Fence> fmt::Debug for McpCompactObservationResult<Fence> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCompactObservationResult([REDACTED])")
    }
}

/// Current authorized observation of one exact subscribed resource.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpSubscribedResourceObservation {
    /// The resource remains visible with this bounded current-content identity.
    Visible(McpVisibleFingerprint),
    /// The resource is no longer visible and must be removed existence-blind.
    Hidden,
}

/// Closed backend failure class needed by the common observation loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObserverBackendError {
    /// A cursor, fence, transport, policy, or response failure aborts this pass.
    RetryNextTick,
    /// Fresh authentication failed and the owning session must close.
    AuthenticationLost,
    /// Session or server cancellation permanently ended backend work.
    Cancelled,
}

impl fmt::Display for McpObserverBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP observer backend is unavailable")
    }
}

impl Error for McpObserverBackendError {}

/// Object-safe future returned by a transport-specific observer backend.
pub type McpObserverBackendFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, McpObserverBackendError>> + Send + 'a>>;

/// Fresh-call backend port used by both MCP transports.
///
/// Every method call must create a fresh RiffDB request identity and repeat
/// authentication/current-policy evaluation. Implementations must not retain a
/// begun service invocation, cursor, capability proof, or resource content
/// after the returned future completes.
pub trait McpObserverBackend: Send + Sync + 'static {
    /// Transport-specific checked presentation fence.
    type Fence: Clone + Eq + Send + Sync + 'static;

    /// Performs one fresh compact discovery call for the selected inventory.
    fn discover_compact<'a>(
        &'a self,
        inventory: McpObservedInventory,
        request: McpCompactObservationRequest<'a, Self::Fence>,
    ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>>;

    /// Reauthenticates and reads one exact subscribed resource for comparison.
    fn observe_subscribed_resource<'a>(
        &'a self,
        uri: &'a str,
    ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation>;
}

/// One authority-free marker accepted by the bounded observer output sink.
///
/// The common observer emits a marker only after the current pass has obtained
/// fresh authenticated/admitted service evidence. Mailbox acceptance retains
/// neither content nor authority, and a successful receiver dequeue is the
/// notification's emission linearization point. Unsubscribe, hiding, or
/// cancellation removes a marker that remains queued. Once dequeued, the
/// authority-free marker is not revocable; a transport may deliver it later,
/// after its own fresh credential and session-binding check, without adding an
/// unbudgeted notification-only service call.
#[derive(Clone, Eq, PartialEq)]
pub enum McpObserverNotification {
    /// The complete policy-visible tool inventory changed.
    ToolsListChanged,
    /// The complete policy-visible resource inventory changed.
    ResourcesListChanged,
    /// One exact subscribed URI changed after a fresh authorized read.
    ResourceUpdated(McpObserverResourceUpdate),
}

/// One generation-bound, authority-free subscribed-resource marker.
#[derive(Clone, Eq, PartialEq)]
pub struct McpObserverResourceUpdate {
    uri: String,
    subscription_generation: u64,
}

impl McpObserverResourceUpdate {
    pub(crate) fn new(uri: String, subscription_generation: u64) -> Self {
        Self {
            uri,
            subscription_generation,
        }
    }

    /// Borrows the exact canonical subscribed URI.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Releases the exact canonical URI for transport delivery.
    #[must_use]
    pub fn into_uri(self) -> String {
        self.uri
    }
}

impl fmt::Debug for McpObserverResourceUpdate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverResourceUpdate([REDACTED])")
    }
}

impl fmt::Debug for McpObserverNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ToolsListChanged => formatter.write_str("ToolsListChanged"),
            Self::ResourcesListChanged => formatter.write_str("ResourcesListChanged"),
            Self::ResourceUpdated(_) => formatter.write_str("ResourceUpdated([REDACTED])"),
        }
    }
}

/// Closed result of one nonblocking observer notification attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObserverNotificationError {
    /// The bounded transport output is full; the marker must be coalesced.
    Backpressured,
    /// The transport is permanently closed and the session must terminate.
    Closed,
}

impl fmt::Display for McpObserverNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP observer notification output is unavailable")
    }
}

impl Error for McpObserverNotificationError {}

/// Transport-owned nonblocking marker acceptance for one MCP session.
///
/// Implementations must use a nonblocking capacity/lock attempt and must not
/// await capacity. The observer owns coalescing and retry; the sink must not add
/// an unbounded queue.
///
/// Acceptance here is the observer's authorized marker emission.
/// A queued marker remains coalescible or revocable; successful
/// receiver dequeue is its final, irrevocable handoff linearization point and is
/// distinct from a potentially delayed SDK transport send.
pub trait McpObserverNotificationSink: Send + Sync + 'static {
    /// Attempts to accept one already authorized, bounded notification.
    fn try_emit(
        &self,
        notification: McpObserverNotification,
    ) -> Result<(), McpObserverNotificationError>;
}

/// Cloneable producer half of the exact keyed observer notification mailbox.
#[derive(Clone)]
pub struct McpObserverNotificationSender {
    lease: Arc<McpObserverNotificationSenderLease>,
}

struct McpObserverNotificationSenderLease {
    mailbox: Arc<McpObserverNotificationMailbox>,
}

impl Drop for McpObserverNotificationSenderLease {
    fn drop(&mut self) {
        self.mailbox.close_producer();
    }
}

impl McpObserverNotificationSink for McpObserverNotificationSender {
    fn try_emit(
        &self,
        notification: McpObserverNotification,
    ) -> Result<(), McpObserverNotificationError> {
        self.lease.mailbox.try_emit(notification)
    }
}

impl fmt::Debug for McpObserverNotificationSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverNotificationSender([REDACTED])")
    }
}

/// Sole consumer half of one bounded keyed observer notification mailbox.
pub struct McpObserverNotificationReceiver {
    mailbox: Arc<McpObserverNotificationMailbox>,
    closed: bool,
}

impl McpObserverNotificationReceiver {
    /// Waits for one accepted notification or the permanent producer close.
    pub async fn recv(&mut self) -> Option<McpObserverNotification> {
        poll_fn(|context| self.poll_recv(context)).await
    }

    pub(crate) fn poll_recv(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<McpObserverNotification>> {
        if self.closed {
            return Poll::Ready(None);
        }
        let result = self.mailbox.poll_recv(context);
        if matches!(result, Poll::Ready(None)) {
            self.closed = true;
        }
        result
    }
}

impl Drop for McpObserverNotificationReceiver {
    fn drop(&mut self) {
        self.mailbox.close_consumer();
    }
}

impl fmt::Debug for McpObserverNotificationReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverNotificationReceiver([REDACTED])")
    }
}

#[derive(Default)]
struct McpObserverNotificationMailboxState {
    tools_list_changed: bool,
    resources_list_changed: bool,
    resource_updates: BTreeMap<String, u64>,
    producer_open: bool,
    consumer_open: bool,
}

struct McpObserverNotificationMailbox {
    observer: Weak<McpObserverState>,
    state: Mutex<McpObserverNotificationMailboxState>,
    observer_closed: AtomicBool,
    receiver_waker: AtomicWaker,
}

impl McpObserverNotificationMailbox {
    fn new(observer: &Arc<McpObserverState>) -> Self {
        Self {
            observer: Arc::downgrade(observer),
            state: Mutex::new(McpObserverNotificationMailboxState {
                producer_open: true,
                consumer_open: true,
                ..McpObserverNotificationMailboxState::default()
            }),
            observer_closed: AtomicBool::new(false),
            receiver_waker: AtomicWaker::new(),
        }
    }

    fn try_emit(
        &self,
        notification: McpObserverNotification,
    ) -> Result<(), McpObserverNotificationError> {
        if self.observer_closed.load(AtomicOrdering::Acquire) {
            return Err(McpObserverNotificationError::Closed);
        }
        let mut state = try_notification_lock(&self.state)?;
        if self.observer_closed.load(AtomicOrdering::Acquire) || !state.consumer_open {
            return Err(McpObserverNotificationError::Closed);
        }
        let Some(observer) = self.observer.upgrade() else {
            self.observer_closed.store(true, AtomicOrdering::Release);
            clear_mailbox(&mut state);
            drop(state);
            self.receiver_waker.wake();
            return Err(McpObserverNotificationError::Closed);
        };
        let observer_state = match try_notification_lock(&observer.inner) {
            Ok(observer_state) => observer_state,
            Err(error) => {
                drop(state);
                return Err(error);
            }
        };
        if observer_state.cancelled {
            self.observer_closed.store(true, AtomicOrdering::Release);
            clear_mailbox(&mut state);
            drop(state);
            drop(observer_state);
            self.receiver_waker.wake();
            return Err(McpObserverNotificationError::Closed);
        }

        let (accepted, result) = match notification {
            McpObserverNotification::ToolsListChanged => {
                state.tools_list_changed = true;
                (true, Ok(()))
            }
            McpObserverNotification::ResourcesListChanged => {
                state.resources_list_changed = true;
                (true, Ok(()))
            }
            McpObserverNotification::ResourceUpdated(update) => {
                if !subscription_generation_is_current(
                    &observer_state,
                    update.uri(),
                    update.subscription_generation,
                ) {
                    (false, Ok(()))
                } else if !state.resource_updates.contains_key(update.uri())
                    && mailbox_marker_count(&state) >= MAX_MCP_PENDING_NOTIFICATION_MARKERS
                {
                    (false, Err(McpObserverNotificationError::Backpressured))
                } else {
                    state
                        .resource_updates
                        .insert(update.uri, update.subscription_generation);
                    (true, Ok(()))
                }
            }
        };
        drop(state);
        drop(observer_state);
        if accepted {
            self.receiver_waker.wake();
        }
        result
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Option<McpObserverNotification>> {
        let mut state = self.lock();
        self.receiver_waker.register(context.waker());
        if self.observer_closed.load(AtomicOrdering::Acquire) {
            clear_mailbox(&mut state);
            return Poll::Ready(None);
        }
        let Some(observer) = self.observer.upgrade() else {
            self.observer_closed.store(true, AtomicOrdering::Release);
            clear_mailbox(&mut state);
            drop(state);
            self.receiver_waker.wake();
            return Poll::Ready(None);
        };
        let observer_state = observer.lock();
        if observer_state.cancelled {
            self.observer_closed.store(true, AtomicOrdering::Release);
            clear_mailbox(&mut state);
            drop(state);
            drop(observer_state);
            drop(observer);
            self.receiver_waker.wake();
            return Poll::Ready(None);
        }

        let result = loop {
            if state.tools_list_changed {
                state.tools_list_changed = false;
                break Poll::Ready(Some(McpObserverNotification::ToolsListChanged));
            }
            if state.resources_list_changed {
                state.resources_list_changed = false;
                break Poll::Ready(Some(McpObserverNotification::ResourcesListChanged));
            }
            let Some((uri, generation)) = state
                .resource_updates
                .first_key_value()
                .map(|(uri, generation)| (uri.clone(), *generation))
            else {
                break if state.producer_open {
                    Poll::Pending
                } else {
                    Poll::Ready(None)
                };
            };
            state.resource_updates.remove(&uri);
            if subscription_generation_is_current(&observer_state, &uri, generation) {
                break Poll::Ready(Some(McpObserverNotification::ResourceUpdated(
                    McpObserverResourceUpdate::new(uri, generation),
                )));
            }
        };
        drop(state);
        drop(observer_state);
        drop(observer);
        result
    }

    fn remove_resource(&self, uri: &str) {
        self.lock().resource_updates.remove(uri);
    }

    fn cancel(&self) {
        self.observer_closed.store(true, AtomicOrdering::Release);
        let mut state = self.lock();
        clear_mailbox(&mut state);
        drop(state);
        self.receiver_waker.wake();
    }

    fn close_from_observer_drop(&self) {
        self.observer_closed.store(true, AtomicOrdering::Release);
        match self.state.try_lock() {
            Ok(mut state) => clear_mailbox(&mut state),
            Err(TryLockError::Poisoned(error)) => clear_mailbox(&mut error.into_inner()),
            Err(TryLockError::WouldBlock) => {}
        }
        self.receiver_waker.wake();
    }

    fn close_producer(&self) {
        let mut state = self.lock();
        state.producer_open = false;
        drop(state);
        self.receiver_waker.wake();
    }

    fn close_consumer(&self) {
        let mut state = self.lock();
        state.consumer_open = false;
        clear_mailbox(&mut state);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, McpObserverNotificationMailboxState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn try_notification_lock<T>(
    lock: &Mutex<T>,
) -> Result<MutexGuard<'_, T>, McpObserverNotificationError> {
    match lock.try_lock() {
        Ok(state) => Ok(state),
        Err(TryLockError::Poisoned(error)) => Ok(error.into_inner()),
        Err(TryLockError::WouldBlock) => Err(McpObserverNotificationError::Backpressured),
    }
}

fn mailbox_marker_count(state: &McpObserverNotificationMailboxState) -> usize {
    usize::from(state.tools_list_changed)
        + usize::from(state.resources_list_changed)
        + state.resource_updates.len()
}

fn clear_mailbox(state: &mut McpObserverNotificationMailboxState) {
    state.tools_list_changed = false;
    state.resources_list_changed = false;
    state.resource_updates.clear();
}

fn subscription_generation_is_current(
    observer: &ObserverInner,
    uri: &str,
    generation: u64,
) -> bool {
    observer
        .subscriptions
        .get(uri)
        .is_some_and(|subscription| subscription.generation == generation)
}

/// Creates the exact keyed ten-marker nonblocking observer output boundary.
pub fn mcp_observer_notification_channel(
    observer: &Arc<McpObserverState>,
) -> Result<
    (
        McpObserverNotificationSender,
        McpObserverNotificationReceiver,
    ),
    McpObserverError,
> {
    let mailbox = Arc::new(McpObserverNotificationMailbox::new(observer));
    observer.attach_notification_mailbox(&mailbox)?;
    Ok((
        McpObserverNotificationSender {
            lease: Arc::new(McpObserverNotificationSenderLease {
                mailbox: Arc::clone(&mailbox),
            }),
        },
        McpObserverNotificationReceiver {
            mailbox,
            closed: false,
        },
    ))
}

/// Result of an idempotent subscription request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpSubscribeResult {
    /// A new exact canonical URI was retained.
    Added,
    /// The exact canonical URI was already retained.
    AlreadyPresent,
}

/// Result of an idempotent unsubscribe request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpUnsubscribeResult {
    /// The exact canonical URI was removed.
    Removed,
    /// The exact canonical URI was not subscribed.
    Absent,
}

/// Coalesced pending notification state removed atomically for emission.
#[derive(Clone, Eq, PartialEq)]
pub struct McpPendingNotifications {
    /// Whether the visible tools inventory changed.
    pub tools_list_changed: bool,
    /// Whether the visible resources inventory changed.
    pub resources_list_changed: bool,
    resource_updates: Vec<String>,
}

impl fmt::Debug for McpPendingNotifications {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpPendingNotifications([REDACTED])")
    }
}

impl McpPendingNotifications {
    /// Returns exact canonical resource URIs requiring fresh authorized reads.
    #[must_use]
    pub fn resource_updates(&self) -> &[String] {
        &self.resource_updates
    }

    /// Returns the complete bounded marker count.
    #[must_use]
    pub fn marker_count(&self) -> usize {
        usize::from(self.tools_list_changed)
            + usize::from(self.resources_list_changed)
            + self.resource_updates.len()
    }
}

struct SubscriptionObservation {
    locator: McpResourceLocator,
    fingerprint: Option<McpVisibleFingerprint>,
    generation: u64,
}

struct SubscriptionSnapshot {
    uri: String,
    locator: McpResourceLocator,
    pending_epoch: Option<u64>,
    needs_baseline: bool,
    generation: u64,
}

struct ReadyResourceUpdate {
    uri: String,
    subscription_generation: u64,
}

struct McpPendingEmission {
    tools_list_changed: bool,
    resources_list_changed: bool,
    resource_updates: Vec<ReadyResourceUpdate>,
}

impl McpPendingEmission {
    fn marker_count(&self) -> usize {
        usize::from(self.tools_list_changed)
            + usize::from(self.resources_list_changed)
            + self.resource_updates.len()
    }
}

#[derive(Default)]
struct ObserverInner {
    subscriptions: BTreeMap<String, SubscriptionObservation>,
    tools: Option<Vec<McpVisibleFingerprint>>,
    resources: Option<Vec<McpVisibleFingerprint>>,
    pending_tools_changed: bool,
    pending_resources_changed: bool,
    pending_resource_updates: BTreeMap<String, u64>,
    next_resource_marker_epoch: u64,
    next_subscription_generation: u64,
    dirty_catalog: bool,
    due: bool,
    cancelled: bool,
    notification_mailbox: Option<Weak<McpObserverNotificationMailbox>>,
}

/// One session's bounded subscriptions, fingerprints, and coalesced markers.
pub struct McpObserverState {
    inner: Mutex<ObserverInner>,
    telemetry: Arc<dyn McpTelemetry>,
}

impl McpObserverState {
    /// Creates empty observer state with no authority or background task.
    #[must_use]
    pub fn new() -> Self {
        Self::with_telemetry(Arc::new(NoopMcpTelemetry))
    }

    /// Creates empty observer state with one closed semantic telemetry sink.
    #[must_use]
    pub fn with_telemetry(telemetry: Arc<dyn McpTelemetry>) -> Self {
        Self {
            inner: Mutex::new(ObserverInner {
                subscriptions: BTreeMap::new(),
                tools: None,
                resources: None,
                pending_tools_changed: false,
                pending_resources_changed: false,
                pending_resource_updates: BTreeMap::new(),
                next_resource_marker_epoch: 0,
                next_subscription_generation: 0,
                dirty_catalog: false,
                due: false,
                cancelled: false,
                notification_mailbox: None,
            }),
            telemetry,
        }
    }

    /// Retains one canonical URI only when its v1 kind is subscribable.
    pub fn subscribe(&self, uri: &str) -> Result<McpSubscribeResult, McpObserverError> {
        self.subscribe_inner(uri, None)
    }

    /// Retains one authorized subscription with the exact fingerprint read at
    /// its successful subscription linearization point.
    pub fn subscribe_with_baseline(
        &self,
        uri: &str,
        baseline: McpVisibleFingerprint,
    ) -> Result<McpSubscribeResult, McpObserverError> {
        self.subscribe_inner(uri, Some(baseline))
    }

    fn subscribe_inner(
        &self,
        uri: &str,
        baseline: Option<McpVisibleFingerprint>,
    ) -> Result<McpSubscribeResult, McpObserverError> {
        let locator = parse_resource_locator(uri).map_err(|_| McpObserverError::Unsupported)?;
        if !subscribable(&locator) {
            return Err(McpObserverError::Unsupported);
        }
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        if inner.subscriptions.contains_key(uri) {
            return Ok(McpSubscribeResult::AlreadyPresent);
        }
        if inner.subscriptions.len() >= MAX_MCP_SUBSCRIPTIONS {
            return Err(McpObserverError::LimitExceeded);
        }
        let generation = next_subscription_generation(&mut inner)?;
        inner.subscriptions.insert(
            uri.to_owned(),
            SubscriptionObservation {
                locator,
                fingerprint: baseline,
                generation,
            },
        );
        Ok(McpSubscribeResult::Added)
    }

    /// Removes one exact canonical URI without treating absence as failure.
    pub fn unsubscribe(&self, uri: &str) -> Result<McpUnsubscribeResult, McpObserverError> {
        parse_resource_locator(uri).map_err(|_| McpObserverError::Unsupported)?;
        let result = {
            let mut inner = self.lock();
            if inner.cancelled {
                return Err(McpObserverError::Cancelled);
            }
            inner.pending_resource_updates.remove(uri);
            if inner.subscriptions.remove(uri).is_some() {
                McpUnsubscribeResult::Removed
            } else {
                McpUnsubscribeResult::Absent
            }
        };
        if result == McpUnsubscribeResult::Removed {
            self.remove_queued_resource(uri);
        }
        Ok(result)
    }

    /// Replaces both complete visible inventories and coalesces actual changes.
    ///
    /// The first complete observation establishes the baseline and emits no
    /// list-change marker.
    pub fn replace_visible_catalogs(
        &self,
        tools: Vec<McpVisibleFingerprint>,
        resources: Vec<McpVisibleFingerprint>,
    ) -> Result<(), McpObserverError> {
        validate_inventory(&tools)?;
        validate_inventory(&resources)?;
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        if inner
            .tools
            .as_ref()
            .is_some_and(|current| current != &tools)
        {
            inner.pending_tools_changed = true;
        }
        if inner
            .resources
            .as_ref()
            .is_some_and(|current| current != &resources)
        {
            inner.pending_resources_changed = true;
        }
        inner.tools = Some(tools);
        inner.resources = Some(resources);
        inner.dirty_catalog = false;
        inner.due = false;
        Ok(())
    }

    /// Coalesces one post-durability catalog-dirty signal without doing work.
    pub fn mark_catalog_dirty(&self) -> Result<(), McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        inner.dirty_catalog = true;
        Ok(())
    }

    /// Coalesces one due observation tick without queuing or spawning.
    pub fn mark_due(&self) -> Result<(), McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        inner.due = true;
        Ok(())
    }

    /// Reports and clears one due marker after a permit is obtained.
    pub fn take_due(&self) -> Result<bool, McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        let due = std::mem::take(&mut inner.due);
        let dirty_catalog = std::mem::take(&mut inner.dirty_catalog);
        Ok(due || dirty_catalog)
    }

    /// Coalesces the latest update marker only for an exact live subscription.
    pub fn mark_resource_updated(&self, uri: &str) -> Result<(), McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        if !inner.subscriptions.contains_key(uri) {
            return Err(McpObserverError::Unsupported);
        }
        let epoch = next_resource_marker_epoch(&mut inner)?;
        inner.pending_resource_updates.insert(uri.to_owned(), epoch);
        debug_assert!(
            marker_count(&inner) <= MAX_MCP_PENDING_NOTIFICATION_MARKERS,
            "subscription and marker bounds must compose"
        );
        Ok(())
    }

    /// Removes all current markers for fresh authentication and authorized reads.
    pub fn take_pending(&self) -> Result<McpPendingNotifications, McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        let notifications = McpPendingNotifications {
            tools_list_changed: std::mem::take(&mut inner.pending_tools_changed),
            resources_list_changed: std::mem::take(&mut inner.pending_resources_changed),
            resource_updates: std::mem::take(&mut inner.pending_resource_updates)
                .into_keys()
                .collect(),
        };
        if notifications.marker_count() > MAX_MCP_PENDING_NOTIFICATION_MARKERS {
            return Err(McpObserverError::LimitExceeded);
        }
        Ok(notifications)
    }

    fn take_pending_after_reads(
        &self,
        freshly_read_updates: &[ReadyResourceUpdate],
    ) -> Result<McpPendingEmission, McpObserverError> {
        if freshly_read_updates.len() > MAX_MCP_SUBSCRIPTIONS {
            return Err(McpObserverError::LimitExceeded);
        }
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        let mut resource_updates: Vec<_> = freshly_read_updates
            .iter()
            .filter(|update| {
                inner
                    .subscriptions
                    .get(&update.uri)
                    .is_some_and(|subscription| {
                        subscription.generation == update.subscription_generation
                    })
            })
            .map(|update| ReadyResourceUpdate {
                uri: update.uri.clone(),
                subscription_generation: update.subscription_generation,
            })
            .collect();
        resource_updates.sort_by(|left, right| left.uri.cmp(&right.uri));
        resource_updates.dedup_by(|left, right| left.uri == right.uri);
        let notifications = McpPendingEmission {
            tools_list_changed: std::mem::take(&mut inner.pending_tools_changed),
            resources_list_changed: std::mem::take(&mut inner.pending_resources_changed),
            resource_updates,
        };
        if notifications.marker_count() > MAX_MCP_PENDING_NOTIFICATION_MARKERS {
            return Err(McpObserverError::LimitExceeded);
        }
        Ok(notifications)
    }

    /// Coalesces markers that could not be emitted because output was unavailable.
    ///
    /// Resource markers are restored only while the exact URI remains
    /// subscribed. No resource body, authorization result, or failure reason is
    /// retained.
    pub fn restore_pending(
        &self,
        notifications: McpPendingNotifications,
    ) -> Result<(), McpObserverError> {
        if notifications.marker_count() > MAX_MCP_PENDING_NOTIFICATION_MARKERS {
            return Err(McpObserverError::LimitExceeded);
        }
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        inner.pending_tools_changed |= notifications.tools_list_changed;
        inner.pending_resources_changed |= notifications.resources_list_changed;
        for uri in notifications.resource_updates {
            if inner.subscriptions.contains_key(&uri)
                && !inner.pending_resource_updates.contains_key(&uri)
            {
                let epoch = next_resource_marker_epoch(&mut inner)?;
                inner.pending_resource_updates.insert(uri, epoch);
            }
        }
        if marker_count(&inner) > MAX_MCP_PENDING_NOTIFICATION_MARKERS {
            return Err(McpObserverError::LimitExceeded);
        }
        Ok(())
    }

    /// Cancels and releases every nondurable observer value exactly once.
    pub fn cancel(&self) {
        let mailbox = {
            let mut inner = self.lock();
            if inner.cancelled {
                return;
            }
            let mailbox = inner
                .notification_mailbox
                .take()
                .and_then(|mailbox| mailbox.upgrade());
            *inner = ObserverInner {
                cancelled: true,
                ..ObserverInner::default()
            };
            mailbox
        };
        if let Some(mailbox) = mailbox {
            mailbox.cancel();
        }
    }

    /// Returns exact canonical subscribed URIs for a bounded sequential poll.
    pub fn subscribed_uris(&self) -> Result<Vec<String>, McpObserverError> {
        let inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        Ok(inner.subscriptions.keys().cloned().collect())
    }

    fn subscription_snapshot(&self) -> Result<Vec<SubscriptionSnapshot>, McpObserverError> {
        let inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        Ok(inner
            .subscriptions
            .iter()
            .map(|(uri, observation)| SubscriptionSnapshot {
                uri: uri.clone(),
                locator: observation.locator.clone(),
                pending_epoch: inner.pending_resource_updates.get(uri).copied(),
                needs_baseline: observation.fingerprint.is_none(),
                generation: observation.generation,
            })
            .collect())
    }

    fn apply_successful_pass(
        &self,
        catalogs: Option<(Vec<McpVisibleFingerprint>, Vec<McpVisibleFingerprint>)>,
        resource_observations: Vec<(String, McpSubscribedResourceObservation, Option<u64>, u64)>,
    ) -> Result<Vec<ReadyResourceUpdate>, McpObserverError> {
        if let Some((tools, resources)) = &catalogs {
            validate_inventory(tools)?;
            validate_inventory(resources)?;
        }
        if resource_observations.len() > MAX_MCP_SUBSCRIPTIONS {
            return Err(McpObserverError::LimitExceeded);
        }

        let (ready_resource_updates, removed_resources) = {
            let mut inner = self.lock();
            if inner.cancelled {
                return Err(McpObserverError::Cancelled);
            }

            if let Some((tools, resources)) = catalogs {
                if inner
                    .tools
                    .as_ref()
                    .is_some_and(|current| current != &tools)
                {
                    inner.pending_tools_changed = true;
                }
                if inner
                    .resources
                    .as_ref()
                    .is_some_and(|current| current != &resources)
                {
                    inner.pending_resources_changed = true;
                }
                inner.tools = Some(tools);
                inner.resources = Some(resources);
            }

            let mut ready_resource_updates = Vec::new();
            let mut removed_resources = Vec::new();
            for (uri, observation, acknowledged_epoch, observed_generation) in resource_observations
            {
                let Some(current_generation) = inner
                    .subscriptions
                    .get(&uri)
                    .map(|subscription| subscription.generation)
                else {
                    continue;
                };
                if current_generation != observed_generation {
                    continue;
                }
                let acknowledged_pending_marker = acknowledged_epoch.is_some_and(|epoch| {
                    inner.pending_resource_updates.get(&uri).copied() == Some(epoch)
                });
                match observation {
                    McpSubscribedResourceObservation::Hidden => {
                        inner.subscriptions.remove(&uri);
                        inner.pending_resource_updates.remove(&uri);
                        removed_resources.push(uri);
                    }
                    McpSubscribedResourceObservation::Visible(fingerprint) => {
                        let changed = inner
                            .subscriptions
                            .get(&uri)
                            .and_then(|subscription| subscription.fingerprint.as_ref())
                            .is_some_and(|current| current != &fingerprint);
                        let Some(subscription) = inner.subscriptions.get_mut(&uri) else {
                            continue;
                        };
                        subscription.fingerprint = Some(fingerprint);
                        if acknowledged_pending_marker {
                            inner.pending_resource_updates.remove(&uri);
                        }
                        if changed || acknowledged_pending_marker {
                            ready_resource_updates.push(ReadyResourceUpdate {
                                uri,
                                subscription_generation: observed_generation,
                            });
                        }
                    }
                }
            }

            if marker_count(&inner) > MAX_MCP_PENDING_NOTIFICATION_MARKERS {
                return Err(McpObserverError::LimitExceeded);
            }
            (ready_resource_updates, removed_resources)
        };
        for uri in removed_resources {
            self.remove_queued_resource(&uri);
        }
        Ok(ready_resource_updates)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ObserverInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn attach_notification_mailbox(
        self: &Arc<Self>,
        mailbox: &Arc<McpObserverNotificationMailbox>,
    ) -> Result<(), McpObserverError> {
        let mut inner = self.lock();
        if inner.cancelled {
            return Err(McpObserverError::Cancelled);
        }
        if inner
            .notification_mailbox
            .as_ref()
            .and_then(Weak::upgrade)
            .is_some()
        {
            return Err(McpObserverError::LimitExceeded);
        }
        inner.notification_mailbox = Some(Arc::downgrade(mailbox));
        Ok(())
    }

    fn notification_mailbox(&self) -> Option<Arc<McpObserverNotificationMailbox>> {
        self.lock()
            .notification_mailbox
            .as_ref()
            .and_then(Weak::upgrade)
    }

    fn remove_queued_resource(&self, uri: &str) {
        if let Some(mailbox) = self.notification_mailbox() {
            mailbox.remove_resource(uri);
        }
    }
}

impl Default for McpObserverState {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for McpObserverState {
    fn drop(&mut self) {
        let mailbox = self
            .inner
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .notification_mailbox
            .take()
            .and_then(|mailbox| mailbox.upgrade());
        if let Some(mailbox) = mailbox {
            mailbox.close_from_observer_drop();
        }
    }
}

impl fmt::Debug for McpObserverState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverState([REDACTED])")
    }
}

/// Result of one injected monotonic scheduler sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObserverTickResult {
    /// The next five-second observation boundary has not arrived.
    NotDue,
    /// The shared 32-permit boundary was full; one due marker remains.
    Deferred,
    /// Both compact inventories were unchanged and resource polls completed.
    Unchanged,
    /// Both complete compact inventories were accepted under a new fence.
    Refreshed,
    /// A closed pass failure discarded all provisional state until the next tick.
    RetryNextTick,
    /// The 900-second or 180-tick absolute observation budget ended.
    Expired,
}

/// Closed session-observer failure without auth, cursor, URI, or policy detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObserverLoopError {
    /// Injected monotonic time regressed or overflowed.
    ClockUnavailable,
    /// Fresh authentication failed and the session must close.
    AuthenticationLost,
    /// The observer was permanently cancelled.
    Cancelled,
}

impl fmt::Display for McpObserverLoopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP session observation is unavailable")
    }
}

impl Error for McpObserverLoopError {}

/// One transport-neutral, sequential, bounded session-observation loop.
///
/// Transport composition injects monotonic samples and owns the actual timer.
/// This type never sleeps, refreshes an idle deadline, or retains an in-flight
/// backend call between samples.
pub struct McpObserverLoop<Backend>
where
    Backend: McpObserverBackend + ?Sized,
{
    backend: Arc<Backend>,
    state: Arc<McpObserverState>,
    semaphore: Arc<McpObserverSemaphore>,
    notification_sink: Arc<dyn McpObserverNotificationSink>,
    started_at: Duration,
    next_tick_at: Duration,
    last_sample_at: Duration,
    prior_fence: Option<Backend::Fence>,
    ticks: u16,
    logical_operations: u32,
    terminal: bool,
}

impl<Backend> McpObserverLoop<Backend>
where
    Backend: McpObserverBackend + ?Sized,
{
    /// Creates one observer whose first due boundary is five seconds after start.
    pub fn new(
        backend: Arc<Backend>,
        state: Arc<McpObserverState>,
        semaphore: Arc<McpObserverSemaphore>,
        notification_sink: Arc<dyn McpObserverNotificationSink>,
        started_at: Duration,
    ) -> Result<Self, McpObserverLoopError> {
        let next_tick_at = started_at
            .checked_add(MCP_OBSERVER_TICK_INTERVAL)
            .ok_or(McpObserverLoopError::ClockUnavailable)?;
        started_at
            .checked_add(MAX_MCP_OBSERVER_LIFETIME)
            .ok_or(McpObserverLoopError::ClockUnavailable)?;
        Ok(Self {
            backend,
            state,
            semaphore,
            notification_sink,
            started_at,
            next_tick_at,
            last_sample_at: started_at,
            prior_fence: None,
            ticks: 0,
            logical_operations: 0,
            terminal: false,
        })
    }

    /// Runs at most one observation pass for one injected monotonic sample.
    ///
    /// A delayed sample does not replay missed ticks. Every accepted pass is
    /// sequential and holds one observer permit only until this future returns
    /// or is cancelled.
    pub async fn observe_tick(
        &mut self,
        now: Duration,
    ) -> Result<McpObserverTickResult, McpObserverLoopError> {
        if self.terminal {
            return Err(McpObserverLoopError::Cancelled);
        }
        if now < self.last_sample_at {
            self.terminate();
            return Err(McpObserverLoopError::ClockUnavailable);
        }
        self.last_sample_at = now;

        let lifetime_deadline = self
            .started_at
            .checked_add(MAX_MCP_OBSERVER_LIFETIME)
            .ok_or_else(|| {
                self.terminate();
                McpObserverLoopError::ClockUnavailable
            })?;
        if now > lifetime_deadline || self.ticks >= MAX_MCP_OBSERVER_TICKS {
            self.terminate();
            return Ok(McpObserverTickResult::Expired);
        }
        if now < self.next_tick_at {
            return Ok(McpObserverTickResult::NotDue);
        }

        self.ticks = self.ticks.checked_add(1).ok_or_else(|| {
            self.terminate();
            McpObserverLoopError::ClockUnavailable
        })?;
        self.next_tick_at = now.checked_add(MCP_OBSERVER_TICK_INTERVAL).ok_or_else(|| {
            self.terminate();
            McpObserverLoopError::ClockUnavailable
        })?;
        self.state.mark_due().map_err(|_| {
            self.terminal = true;
            McpObserverLoopError::Cancelled
        })?;

        let permit = match self.semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(McpAdmissionError::CapacityExhausted) => {
                return Ok(McpObserverTickResult::Deferred);
            }
            Err(McpAdmissionError::InvalidSession) => {
                self.terminate();
                return Err(McpObserverLoopError::Cancelled);
            }
        };
        if !self.state.take_due().map_err(|_| {
            self.terminal = true;
            McpObserverLoopError::Cancelled
        })? {
            drop(permit);
            return Ok(McpObserverTickResult::NotDue);
        }

        let logical_operations_before = self.logical_operations;
        let pass = self.perform_pass().await;
        drop(permit);
        if self
            .logical_operations
            .saturating_sub(logical_operations_before)
            > MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_TICK
            || self.logical_operations > MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_SESSION
        {
            self.terminate();
            return Err(McpObserverLoopError::Cancelled);
        }

        match pass {
            Ok(changed) => Ok(if changed {
                McpObserverTickResult::Refreshed
            } else {
                McpObserverTickResult::Unchanged
            }),
            Err(McpObserverBackendError::RetryNextTick) => Ok(McpObserverTickResult::RetryNextTick),
            Err(McpObserverBackendError::AuthenticationLost) => {
                self.terminate();
                Err(McpObserverLoopError::AuthenticationLost)
            }
            Err(McpObserverBackendError::Cancelled) => {
                self.terminate();
                Err(McpObserverLoopError::Cancelled)
            }
        }
    }

    /// Permanently releases every nondurable value owned by this loop.
    pub fn cancel(&mut self) {
        self.terminate();
    }

    /// Returns the number of admitted five-second ticks.
    #[must_use]
    pub const fn ticks(&self) -> u16 {
        self.ticks
    }

    /// Returns fresh watcher-generated logical observer operations.
    #[must_use]
    pub const fn logical_operations(&self) -> u32 {
        self.logical_operations
    }

    async fn perform_pass(&mut self) -> Result<bool, McpObserverBackendError> {
        let prior_fence = self.prior_fence.clone();
        let tools = self
            .discover(
                McpObservedInventory::Tools,
                McpCompactObservationRequest::initial(prior_fence.as_ref()),
            )
            .await?;
        let resources = self
            .discover(
                McpObservedInventory::Resources,
                McpCompactObservationRequest::initial(prior_fence.as_ref()),
            )
            .await?;

        let (changed, next_fence, catalogs) = match (tools, resources) {
            (
                McpCompactObservationResult::CatalogUnchanged(tools_fence),
                McpCompactObservationResult::CatalogUnchanged(resources_fence),
            ) => {
                let Some(prior_fence) = prior_fence.as_ref() else {
                    return Err(McpObserverBackendError::RetryNextTick);
                };
                if &tools_fence != prior_fence
                    || &resources_fence != prior_fence
                    || tools_fence != resources_fence
                {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                (false, None, None)
            }
            (
                McpCompactObservationResult::Page(tools_page),
                McpCompactObservationResult::Page(resources_page),
            ) => {
                if tools_page.observed_fence != resources_page.observed_fence
                    || prior_fence
                        .as_ref()
                        .is_some_and(|prior| prior == &tools_page.observed_fence)
                {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                let next_fence = tools_page.observed_fence.clone();
                let tools = self
                    .finish_inventory(McpObservedInventory::Tools, tools_page)
                    .await?;
                let resources = self
                    .finish_inventory(McpObservedInventory::Resources, resources_page)
                    .await?;
                (true, Some(next_fence), Some((tools, resources)))
            }
            _ => return Err(McpObserverBackendError::RetryNextTick),
        };

        let subscriptions = self
            .state
            .subscription_snapshot()
            .map_err(|_| McpObserverBackendError::Cancelled)?;
        let mut resource_observations = Vec::with_capacity(subscriptions.len());
        for subscription in subscriptions {
            if changed
                || subscription.pending_epoch.is_some()
                || subscription.needs_baseline
                || polls_every_tick(&subscription.locator)
            {
                let observation = self.observe_resource(&subscription.uri).await?;
                resource_observations.push((
                    subscription.uri,
                    observation,
                    subscription.pending_epoch,
                    subscription.generation,
                ));
            }
        }

        let freshly_read_updates = self
            .state
            .apply_successful_pass(catalogs, resource_observations)
            .map_err(|error| match error {
                McpObserverError::Cancelled => McpObserverBackendError::Cancelled,
                McpObserverError::Unsupported | McpObserverError::LimitExceeded => {
                    McpObserverBackendError::RetryNextTick
                }
            })?;
        if let Some(next_fence) = next_fence {
            self.prior_fence = Some(next_fence);
        }
        self.emit_pending_notifications(&freshly_read_updates)?;
        Ok(changed)
    }

    fn emit_pending_notifications(
        &self,
        freshly_read_updates: &[ReadyResourceUpdate],
    ) -> Result<(), McpObserverBackendError> {
        let pending = self
            .state
            .take_pending_after_reads(freshly_read_updates)
            .map_err(observer_state_backend_error)?;
        let mut notifications = Vec::with_capacity(pending.marker_count());
        if pending.tools_list_changed {
            notifications.push(McpObserverNotification::ToolsListChanged);
        }
        if pending.resources_list_changed {
            notifications.push(McpObserverNotification::ResourcesListChanged);
        }
        notifications.extend(pending.resource_updates.into_iter().map(|update| {
            McpObserverNotification::ResourceUpdated(McpObserverResourceUpdate::new(
                update.uri,
                update.subscription_generation,
            ))
        }));

        for index in 0..notifications.len() {
            match self
                .notification_sink
                .try_emit(notifications[index].clone())
            {
                Ok(()) => match &notifications[index] {
                    McpObserverNotification::ToolsListChanged => {
                        self.state
                            .telemetry
                            .record(McpTelemetryEvent::ListChangeNotification {
                                kind: McpListChangeKind::Tools,
                            });
                    }
                    McpObserverNotification::ResourcesListChanged => {
                        self.state
                            .telemetry
                            .record(McpTelemetryEvent::ListChangeNotification {
                                kind: McpListChangeKind::Resources,
                            });
                    }
                    McpObserverNotification::ResourceUpdated(_) => {}
                },
                Err(McpObserverNotificationError::Backpressured) => {
                    self.state
                        .restore_pending(pending_from_notifications(&notifications[index..]))
                        .map_err(observer_state_backend_error)?;
                    return Ok(());
                }
                Err(McpObserverNotificationError::Closed) => {
                    return Err(McpObserverBackendError::Cancelled);
                }
            }
        }
        Ok(())
    }

    async fn finish_inventory(
        &mut self,
        inventory: McpObservedInventory,
        mut page: McpCompactObservationPage<Backend::Fence>,
    ) -> Result<Vec<McpVisibleFingerprint>, McpObserverBackendError> {
        let observed_fence = page.observed_fence.clone();
        let mut retained = Vec::new();
        let mut returned = 0_usize;
        let mut calls = 1_u8;

        loop {
            returned = returned
                .checked_add(page.fingerprints.len())
                .ok_or(McpObserverBackendError::RetryNextTick)?;
            if returned > MAX_MCP_OBSERVER_RETURNED_ITEMS {
                return Err(McpObserverBackendError::RetryNextTick);
            }
            for fingerprint in page.fingerprints {
                if retained.len() >= MAX_MCP_VISIBLE_FINGERPRINTS {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
                retained.push(fingerprint);
            }

            let Some(cursor) = page.next_cursor else {
                return Ok(retained);
            };
            if retained.len() >= MAX_MCP_VISIBLE_FINGERPRINTS
                || calls >= MAX_MCP_OBSERVER_DISCOVERY_CALLS
            {
                return Err(McpObserverBackendError::RetryNextTick);
            }
            calls = calls
                .checked_add(1)
                .ok_or(McpObserverBackendError::RetryNextTick)?;
            page = match self
                .discover(
                    inventory,
                    McpCompactObservationRequest::continuation(cursor),
                )
                .await?
            {
                McpCompactObservationResult::Page(page)
                    if page.observed_fence == observed_fence =>
                {
                    page
                }
                McpCompactObservationResult::Page(_)
                | McpCompactObservationResult::CatalogUnchanged(_) => {
                    return Err(McpObserverBackendError::RetryNextTick);
                }
            };
        }
    }

    async fn discover(
        &mut self,
        inventory: McpObservedInventory,
        request: McpCompactObservationRequest<'_, Backend::Fence>,
    ) -> Result<McpCompactObservationResult<Backend::Fence>, McpObserverBackendError> {
        self.logical_operations = self
            .logical_operations
            .checked_add(1)
            .ok_or(McpObserverBackendError::RetryNextTick)?;
        self.backend.discover_compact(inventory, request).await
    }

    async fn observe_resource(
        &mut self,
        uri: &str,
    ) -> Result<McpSubscribedResourceObservation, McpObserverBackendError> {
        self.logical_operations = self
            .logical_operations
            .checked_add(1)
            .ok_or(McpObserverBackendError::RetryNextTick)?;
        self.backend.observe_subscribed_resource(uri).await
    }

    fn terminate(&mut self) {
        if self.terminal {
            return;
        }
        self.terminal = true;
        self.prior_fence = None;
        self.state.cancel();
    }
}

impl<Backend> fmt::Debug for McpObserverLoop<Backend>
where
    Backend: McpObserverBackend + ?Sized,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverLoop([REDACTED])")
    }
}

/// Drives one observer with injected monotonic samples until its fixed budget ends.
#[cfg(any(feature = "stdio", feature = "streamable-http"))]
pub(crate) async fn drive_mcp_observer<Backend, Scheduler>(
    observer: &mut McpObserverLoop<Backend>,
    scheduler: &mut Scheduler,
) -> Result<McpObserverTickResult, McpObserverLoopError>
where
    Backend: McpObserverBackend + ?Sized,
    Scheduler: McpObserverScheduler,
{
    loop {
        let now = scheduler.next_tick().await?;
        let result = observer.observe_tick(now).await?;
        if result == McpObserverTickResult::Expired || observer.ticks() >= MAX_MCP_OBSERVER_TICKS {
            observer.cancel();
            return Ok(McpObserverTickResult::Expired);
        }
    }
}

/// Closed observer-state failure without URI, cursor, or policy data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpObserverError {
    /// The requested URI kind is not a subscribable v1 resource.
    Unsupported,
    /// A subscription, item, byte, or marker bound was exceeded.
    LimitExceeded,
    /// The observer has been permanently cancelled.
    Cancelled,
}

impl fmt::Display for McpObserverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP observation is unavailable")
    }
}

impl Error for McpObserverError {}

fn subscribable(locator: &McpResourceLocator) -> bool {
    matches!(
        locator,
        McpResourceLocator::ActiveContract
            | McpResourceLocator::CommandPlan { .. }
            | McpResourceLocator::ProjectionStatus { .. }
            | McpResourceLocator::ServerHealth
            | McpResourceLocator::ReactiveWakeup
    )
}

fn polls_every_tick(locator: &McpResourceLocator) -> bool {
    matches!(
        locator,
        McpResourceLocator::ProjectionStatus { .. }
            | McpResourceLocator::ServerHealth
            | McpResourceLocator::ReactiveWakeup
    )
}

fn validate_inventory(inventory: &[McpVisibleFingerprint]) -> Result<(), McpObserverError> {
    if inventory.len() > MAX_MCP_VISIBLE_FINGERPRINTS {
        return Err(McpObserverError::LimitExceeded);
    }
    Ok(())
}

fn marker_count(inner: &ObserverInner) -> usize {
    usize::from(inner.pending_tools_changed)
        + usize::from(inner.pending_resources_changed)
        + inner.pending_resource_updates.len()
}

fn next_resource_marker_epoch(inner: &mut ObserverInner) -> Result<u64, McpObserverError> {
    let epoch = inner
        .next_resource_marker_epoch
        .checked_add(1)
        .ok_or(McpObserverError::LimitExceeded)?;
    inner.next_resource_marker_epoch = epoch;
    Ok(epoch)
}

fn next_subscription_generation(inner: &mut ObserverInner) -> Result<u64, McpObserverError> {
    let generation = inner
        .next_subscription_generation
        .checked_add(1)
        .ok_or(McpObserverError::LimitExceeded)?;
    inner.next_subscription_generation = generation;
    Ok(generation)
}

fn pending_from_notifications(
    notifications: &[McpObserverNotification],
) -> McpPendingNotifications {
    let mut pending = McpPendingNotifications {
        tools_list_changed: false,
        resources_list_changed: false,
        resource_updates: Vec::new(),
    };
    for notification in notifications {
        match notification {
            McpObserverNotification::ToolsListChanged => pending.tools_list_changed = true,
            McpObserverNotification::ResourcesListChanged => {
                pending.resources_list_changed = true;
            }
            McpObserverNotification::ResourceUpdated(update) => {
                pending.resource_updates.push(update.uri.clone());
            }
        }
    }
    pending
}

const fn observer_state_backend_error(error: McpObserverError) -> McpObserverBackendError {
    match error {
        McpObserverError::Cancelled => McpObserverBackendError::Cancelled,
        McpObserverError::Unsupported | McpObserverError::LimitExceeded => {
            McpObserverBackendError::RetryNextTick
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::atomic::{AtomicU32, Ordering},
    };

    use super::*;

    #[derive(Default)]
    struct RecordingTelemetry(Mutex<Vec<McpTelemetryEvent>>);

    impl McpTelemetry for RecordingTelemetry {
        fn record(&self, event: McpTelemetryEvent) {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(event);
        }
    }

    impl RecordingTelemetry {
        fn snapshot(&self) -> Vec<McpTelemetryEvent> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    fn fingerprint(value: u16) -> McpVisibleFingerprint {
        McpVisibleFingerprint::new(value.to_be_bytes().to_vec()).expect("bounded fingerprint")
    }

    fn fingerprints(start: u16, count: usize) -> Vec<McpVisibleFingerprint> {
        (0..count)
            .map(|offset| fingerprint(start + u16::try_from(offset).expect("fixture offset")))
            .collect()
    }

    fn cursor(value: u8) -> [u8; 16] {
        [value; 16]
    }

    fn page(
        start: u16,
        count: usize,
        next_cursor: Option<[u8; 16]>,
        fence: u8,
    ) -> McpCompactObservationResult<u8> {
        McpCompactObservationResult::Page(
            McpCompactObservationPage::new(fingerprints(start, count), next_cursor, fence)
                .expect("bounded test page"),
        )
    }

    fn block_on<T>(future: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(future)
    }

    #[cfg(feature = "stdio")]
    struct DeterministicObserverScheduler {
        samples: VecDeque<Result<Duration, McpObserverLoopError>>,
    }

    #[cfg(feature = "stdio")]
    impl McpObserverScheduler for DeterministicObserverScheduler {
        fn next_tick(&mut self) -> McpObserverSchedulerFuture<'_> {
            let sample = self
                .samples
                .pop_front()
                .unwrap_or(Err(McpObserverLoopError::ClockUnavailable));
            Box::pin(async move { sample })
        }
    }

    #[test]
    fn common_visible_fingerprint_framing_is_exact_and_transport_neutral() {
        let fixed = McpVisibleFingerprint::fixed_tool(1).expect("fixed tool");
        assert_eq!(
            fixed.as_bytes(),
            b"riffdb.mcp-visible-fingerprint/v1\0\x01\0\0\0\x01\x01"
        );
        assert_eq!(
            McpVisibleFingerprint::fixed_tool(0),
            Err(McpObserverError::Unsupported)
        );

        let command = McpVisibleFingerprint::command_tool(
            "riffdb_cmd_orders_place",
            "PlaceOrder",
            "orders",
            1,
            7,
            &[1; 32],
            &[2; 32],
        )
        .expect("command tool");
        let changed = McpVisibleFingerprint::command_tool(
            "riffdb_cmd_orders_place",
            "PlaceOrder",
            "orders",
            2,
            7,
            &[1; 32],
            &[2; 32],
        )
        .expect("changed command tool");
        assert_ne!(command, changed);
        assert_eq!(
            command,
            McpVisibleFingerprint::command_tool(
                "riffdb_cmd_orders_place",
                "PlaceOrder",
                "orders",
                1,
                7,
                &[1; 32],
                &[2; 32],
            )
            .expect("same command tool")
        );

        let descriptor =
            crate::McpResourceDescriptor::new("active_contract", "riffdb://contract/active")
                .expect("resource descriptor");
        let resource =
            McpVisibleFingerprint::resource_descriptor(&descriptor).expect("resource identity");
        assert_ne!(fixed, resource);
        assert_ne!(command, resource);

        let plan = command_plan_fingerprint(McpCommandPlanFingerprintInput {
            uri: "riffdb://command/orders/7/plan",
            contract_lineage: "orders",
            contract_version: 1,
            command_id: 7,
            source_command: "PlaceOrder",
            plan_hash: &[3; 32],
            input_schema_hash: &[1; 32],
            outcome_schema_hash: &[2; 32],
        })
        .expect("command plan");
        assert!(plan.as_bytes().len() < MAX_MCP_COMPACT_FINGERPRINT_BYTES);
        assert_ne!(
            plan,
            command_plan_fingerprint(McpCommandPlanFingerprintInput {
                uri: "riffdb://command/orders/7/plan",
                contract_lineage: "orders",
                contract_version: 1,
                command_id: 7,
                source_command: "PlaceOrder",
                plan_hash: &[4; 32],
                input_schema_hash: &[1; 32],
                outcome_schema_hash: &[2; 32],
            })
            .expect("changed plan")
        );
        assert_eq!(
            command_plan_fingerprint(McpCommandPlanFingerprintInput {
                uri: "riffdb://command/orders/8/plan",
                contract_lineage: "orders",
                contract_version: 1,
                command_id: 7,
                source_command: "PlaceOrder",
                plan_hash: &[3; 32],
                input_schema_hash: &[1; 32],
                outcome_schema_hash: &[2; 32],
            }),
            Err(McpObserverError::Unsupported)
        );
    }

    #[test]
    fn notification_mailbox_has_exact_keyed_capacity_and_coalesces_duplicates() {
        let state = Arc::new(McpObserverState::new());
        let uris = (1..=MAX_MCP_SUBSCRIPTIONS)
            .map(|id| format!("riffdb://projection/orders/{id}/status"))
            .collect::<Vec<_>>();
        for uri in &uris {
            state.subscribe(uri).expect("bounded subscription");
        }
        let (sender, mut receiver) =
            mcp_observer_notification_channel(&state).expect("one mailbox");
        for _ in 0..MAX_MCP_PENDING_NOTIFICATION_MARKERS {
            sender
                .try_emit(McpObserverNotification::ToolsListChanged)
                .expect("duplicate list marker coalesces");
        }
        sender
            .try_emit(McpObserverNotification::ResourcesListChanged)
            .expect("second list key");
        for (index, uri) in uris.iter().enumerate() {
            let update = McpObserverResourceUpdate::new(
                uri.clone(),
                u64::try_from(index + 1).expect("bounded generation"),
            );
            sender
                .try_emit(McpObserverNotification::ResourceUpdated(update.clone()))
                .expect("one exact URI key");
            sender
                .try_emit(McpObserverNotification::ResourceUpdated(update))
                .expect("duplicate URI replaces the same key");
        }
        drop(sender);
        let mut accepted = Vec::new();
        while let Some(notification) = block_on(receiver.recv()) {
            accepted.push(notification);
        }
        assert_eq!(accepted.len(), MAX_MCP_PENDING_NOTIFICATION_MARKERS);
        assert_eq!(accepted[0], McpObserverNotification::ToolsListChanged);
        assert_eq!(accepted[1], McpObserverNotification::ResourcesListChanged);
        assert_eq!(
            accepted[2..]
                .iter()
                .map(|notification| match notification {
                    McpObserverNotification::ResourceUpdated(update) => update.uri(),
                    McpObserverNotification::ToolsListChanged
                    | McpObserverNotification::ResourcesListChanged => {
                        panic!("resource marker order")
                    }
                })
                .collect::<Vec<_>>(),
            uris.iter().map(String::as_str).collect::<Vec<_>>()
        );

        let state = Arc::new(McpObserverState::new());
        let (sender, receiver) = mcp_observer_notification_channel(&state).expect("one mailbox");
        drop(receiver);
        assert_eq!(
            sender.try_emit(McpObserverNotification::ToolsListChanged),
            Err(McpObserverNotificationError::Closed)
        );
        assert_eq!(
            format!("{sender:?}"),
            "McpObserverNotificationSender([REDACTED])"
        );
    }

    #[test]
    fn notification_sink_uses_nonblocking_lock_attempts() {
        let state = Arc::new(McpObserverState::new());
        let (sender, receiver) = mcp_observer_notification_channel(&state).expect("one mailbox");

        let observer_guard = state.lock();
        assert_eq!(
            sender.try_emit(McpObserverNotification::ToolsListChanged),
            Err(McpObserverNotificationError::Backpressured)
        );
        drop(observer_guard);

        let mailbox_guard = sender.lease.mailbox.lock();
        assert_eq!(
            sender.try_emit(McpObserverNotification::ResourcesListChanged),
            Err(McpObserverNotificationError::Backpressured)
        );
        drop(mailbox_guard);
        drop(receiver);
    }

    #[test]
    fn authoritative_cancellation_closes_markers_before_mailbox_cleanup() {
        let state = Arc::new(McpObserverState::new());
        let (sender, receiver) = mcp_observer_notification_channel(&state).expect("one mailbox");

        // Models the interval after observer cancellation linearizes under the
        // authoritative lock but before the mailbox cleanup phase obtains its lock.
        state.lock().cancelled = true;
        assert_eq!(
            sender.try_emit(McpObserverNotification::ToolsListChanged),
            Err(McpObserverNotificationError::Closed)
        );
        drop(receiver);

        let queued_state = Arc::new(McpObserverState::new());
        let (queued_sender, mut queued_receiver) =
            mcp_observer_notification_channel(&queued_state).expect("one mailbox");
        queued_sender
            .try_emit(McpObserverNotification::ResourcesListChanged)
            .expect("marker before authoritative cancellation");
        queued_state.lock().cancelled = true;
        assert_eq!(
            block_on(queued_receiver.recv()),
            None,
            "a queued catalog marker cannot cross authoritative cancellation"
        );

        let cancelled = Arc::new(McpObserverState::new());
        cancelled.cancel();
        assert!(matches!(
            mcp_observer_notification_channel(&cancelled),
            Err(McpObserverError::Cancelled)
        ));
    }

    #[test]
    fn observer_drop_closes_and_wakes_a_pending_mailbox_receiver() {
        struct WakeCounter(AtomicU32);

        impl std::task::Wake for WakeCounter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let state = Arc::new(McpObserverState::new());
        let (sender, mut receiver) =
            mcp_observer_notification_channel(&state).expect("one mailbox");
        let wake_counter = Arc::new(WakeCounter(AtomicU32::new(0)));
        let waker = std::task::Waker::from(Arc::clone(&wake_counter));
        let mut context = Context::from_waker(&waker);
        assert!(matches!(receiver.poll_recv(&mut context), Poll::Pending));

        drop(state);
        assert!(wake_counter.0.load(Ordering::Relaxed) > 0);
        assert_eq!(
            sender.try_emit(McpObserverNotification::ToolsListChanged),
            Err(McpObserverNotificationError::Closed)
        );
        assert_eq!(block_on(receiver.recv()), None);
    }

    #[test]
    fn notification_dequeue_is_the_irrevocable_emission_linearization_point() {
        let uri = "riffdb://contract/active";
        let state = Arc::new(McpObserverState::new());
        state.subscribe(uri).expect("subscription");
        let generation = state.subscription_snapshot().expect("snapshot")[0].generation;
        let (sender, mut receiver) =
            mcp_observer_notification_channel(&state).expect("one mailbox");
        sender
            .try_emit(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), generation),
            ))
            .expect("current marker");

        let emitted = block_on(receiver.recv()).expect("emitted marker");
        state.unsubscribe(uri).expect("later unsubscribe");
        assert_eq!(
            emitted,
            McpObserverNotification::ResourceUpdated(McpObserverResourceUpdate::new(
                uri.to_owned(),
                generation,
            )),
            "a later invalidation cannot revoke an already dequeued marker"
        );
        drop(sender);
        assert_eq!(block_on(receiver.recv()), None);
    }

    #[test]
    fn notification_mailbox_removes_stale_subscription_generations_and_cancellation() {
        let uri = "riffdb://contract/active";
        let state = Arc::new(McpObserverState::new());
        state.subscribe(uri).expect("first generation");
        let first_generation = state.subscription_snapshot().expect("snapshot")[0].generation;
        let (sender, mut receiver) =
            mcp_observer_notification_channel(&state).expect("one mailbox");
        sender
            .try_emit(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), first_generation),
            ))
            .expect("first generation marker");
        state.unsubscribe(uri).expect("unsubscribe");
        state.subscribe(uri).expect("second generation");
        let second_generation = state.subscription_snapshot().expect("snapshot")[0].generation;
        assert_ne!(first_generation, second_generation);
        assert!(
            state
                .apply_successful_pass(
                    None,
                    vec![(
                        uri.to_owned(),
                        McpSubscribedResourceObservation::Visible(fingerprint(9)),
                        None,
                        first_generation,
                    )],
                )
                .expect("obsolete read is ignored")
                .is_empty()
        );
        assert!(
            state.subscription_snapshot().expect("new generation")[0].needs_baseline,
            "an obsolete read cannot establish the replacement subscription baseline"
        );
        sender
            .try_emit(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), first_generation),
            ))
            .expect("stale generation is discarded existence-blind");
        sender
            .try_emit(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), second_generation),
            ))
            .expect("current generation marker");
        assert_eq!(
            block_on(receiver.recv()),
            Some(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), second_generation)
            ))
        );

        sender
            .try_emit(McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(uri.to_owned(), second_generation),
            ))
            .expect("marker before hidden observation");
        state
            .apply_successful_pass(
                None,
                vec![(
                    uri.to_owned(),
                    McpSubscribedResourceObservation::Hidden,
                    None,
                    second_generation,
                )],
            )
            .expect("hidden observation");
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(receiver.poll_recv(&mut context), Poll::Pending));

        sender
            .try_emit(McpObserverNotification::ToolsListChanged)
            .expect("queued before cancellation");
        state.cancel();
        assert_eq!(block_on(receiver.recv()), None);
        assert_eq!(
            sender.try_emit(McpObserverNotification::ResourcesListChanged),
            Err(McpObserverNotificationError::Closed)
        );
    }

    #[test]
    fn delayed_transport_coalesces_repeated_catalog_markers_to_one_per_key() {
        let backend = Arc::new(ScriptedBackend::default());
        for fence in 1_u8..=4 {
            backend.push_discovery(
                McpObservedInventory::Tools,
                Ok(page(u16::from(fence), 1, None, fence)),
            );
            backend.push_discovery(
                McpObservedInventory::Resources,
                Ok(page(u16::from(fence), 1, None, fence)),
            );
        }
        let state = Arc::new(McpObserverState::new());
        let (sender, mut receiver) =
            mcp_observer_notification_channel(&state).expect("one mailbox");
        let notification_sink: Arc<dyn McpObserverNotificationSink> = Arc::new(sender);
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            notification_sink,
            Duration::ZERO,
        )
        .expect("observer");

        for tick in 1..=4 {
            assert_eq!(
                block_on(observer.observe_tick(Duration::from_secs(tick * 5))),
                Ok(McpObserverTickResult::Refreshed)
            );
        }
        assert_eq!(
            block_on(receiver.recv()),
            Some(McpObserverNotification::ToolsListChanged)
        );
        assert_eq!(
            block_on(receiver.recv()),
            Some(McpObserverNotification::ResourcesListChanged)
        );
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(receiver.poll_recv(&mut context), Poll::Pending));
    }

    struct BackpressuredSink;

    impl McpObserverNotificationSink for BackpressuredSink {
        fn try_emit(&self, _: McpObserverNotification) -> Result<(), McpObserverNotificationError> {
            Err(McpObserverNotificationError::Backpressured)
        }
    }

    fn backpressured_sink() -> Arc<dyn McpObserverNotificationSink> {
        Arc::new(BackpressuredSink)
    }

    #[derive(Default)]
    struct RecordingSinkState {
        results: VecDeque<Result<(), McpObserverNotificationError>>,
        accepted: Vec<McpObserverNotification>,
    }

    #[derive(Default)]
    struct RecordingSink {
        state: Mutex<RecordingSinkState>,
    }

    impl RecordingSink {
        fn push_result(&self, result: Result<(), McpObserverNotificationError>) {
            self.state
                .lock()
                .expect("notification sink")
                .results
                .push_back(result);
        }

        fn accepted(&self) -> Vec<McpObserverNotification> {
            self.state
                .lock()
                .expect("notification sink")
                .accepted
                .clone()
        }
    }

    impl McpObserverNotificationSink for RecordingSink {
        fn try_emit(
            &self,
            notification: McpObserverNotification,
        ) -> Result<(), McpObserverNotificationError> {
            let mut state = self.state.lock().expect("notification sink");
            let result = state.results.pop_front().unwrap_or(Ok(()));
            if result.is_ok() {
                state.accepted.push(notification);
            }
            result
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct DiscoveryCall {
        inventory: McpObservedInventory,
        cursor: Option<[u8; 16]>,
        prior_fence: Option<u8>,
        limit: u16,
    }

    #[derive(Default)]
    struct ScriptedBackendState {
        tools: VecDeque<Result<McpCompactObservationResult<u8>, McpObserverBackendError>>,
        resources: VecDeque<Result<McpCompactObservationResult<u8>, McpObserverBackendError>>,
        resource_observations: BTreeMap<
            String,
            VecDeque<Result<McpSubscribedResourceObservation, McpObserverBackendError>>,
        >,
        discovery_calls: Vec<DiscoveryCall>,
        resource_calls: Vec<String>,
    }

    #[derive(Default)]
    struct ScriptedBackend {
        state: Mutex<ScriptedBackendState>,
    }

    impl ScriptedBackend {
        fn push_discovery(
            &self,
            inventory: McpObservedInventory,
            result: Result<McpCompactObservationResult<u8>, McpObserverBackendError>,
        ) {
            let mut state = self.state.lock().expect("script state");
            match inventory {
                McpObservedInventory::Tools => state.tools.push_back(result),
                McpObservedInventory::Resources => state.resources.push_back(result),
            }
        }

        fn push_resource(
            &self,
            uri: &str,
            result: Result<McpSubscribedResourceObservation, McpObserverBackendError>,
        ) {
            self.state
                .lock()
                .expect("script state")
                .resource_observations
                .entry(uri.to_owned())
                .or_default()
                .push_back(result);
        }

        fn discovery_calls(&self) -> Vec<DiscoveryCall> {
            self.state
                .lock()
                .expect("script state")
                .discovery_calls
                .clone()
        }

        fn resource_calls(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("script state")
                .resource_calls
                .clone()
        }
    }

    impl McpObserverBackend for ScriptedBackend {
        type Fence = u8;

        fn discover_compact<'a>(
            &'a self,
            inventory: McpObservedInventory,
            request: McpCompactObservationRequest<'a, Self::Fence>,
        ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
            let result = {
                let mut state = self.state.lock().expect("script state");
                state.discovery_calls.push(DiscoveryCall {
                    inventory,
                    cursor: request.cursor(),
                    prior_fence: request.prior_fence().copied(),
                    limit: request.limit(),
                });
                match inventory {
                    McpObservedInventory::Tools => state.tools.pop_front(),
                    McpObservedInventory::Resources => state.resources.pop_front(),
                }
                .unwrap_or(Err(McpObserverBackendError::RetryNextTick))
            };
            Box::pin(async move { result })
        }

        fn observe_subscribed_resource<'a>(
            &'a self,
            uri: &'a str,
        ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
            let result = {
                let mut state = self.state.lock().expect("script state");
                state.resource_calls.push(uri.to_owned());
                state
                    .resource_observations
                    .get_mut(uri)
                    .and_then(VecDeque::pop_front)
                    .unwrap_or(Err(McpObserverBackendError::RetryNextTick))
            };
            Box::pin(async move { result })
        }
    }

    #[derive(Default)]
    struct StableBackend {
        calls: AtomicU32,
    }

    impl McpObserverBackend for StableBackend {
        type Fence = u8;

        fn discover_compact<'a>(
            &'a self,
            _: McpObservedInventory,
            request: McpCompactObservationRequest<'a, Self::Fence>,
        ) -> McpObserverBackendFuture<'a, McpCompactObservationResult<Self::Fence>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let result = if request.prior_fence().is_some() {
                McpCompactObservationResult::CatalogUnchanged(1)
            } else {
                page(0, 0, None, 1)
            };
            Box::pin(async move { Ok(result) })
        }

        fn observe_subscribed_resource<'a>(
            &'a self,
            _: &'a str,
        ) -> McpObserverBackendFuture<'a, McpSubscribedResourceObservation> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))) })
        }
    }

    #[test]
    fn exact_subscribable_set_and_eight_uri_limit_are_enforced() {
        let state = McpObserverState::new();
        assert_eq!(
            state.subscribe("riffdb://contract/active"),
            Ok(McpSubscribeResult::Added)
        );
        assert_eq!(
            state.subscribe("riffdb://contract/active"),
            Ok(McpSubscribeResult::AlreadyPresent)
        );
        assert_eq!(
            state.subscribe("riffdb://commit/1"),
            Err(McpObserverError::Unsupported)
        );
        assert_eq!(
            state.subscribe("riffdb://reactive/wakeup"),
            Ok(McpSubscribeResult::Added)
        );

        for projection in 1..=6 {
            assert_eq!(
                state.subscribe(&format!(
                    "riffdb://projection/LegalSpend/{projection}/status"
                )),
                Ok(McpSubscribeResult::Added)
            );
        }
        assert_eq!(
            state.subscribe("riffdb://server/health"),
            Err(McpObserverError::LimitExceeded)
        );
    }

    #[test]
    fn authorized_subscription_baseline_cannot_swallow_a_later_change() {
        let uri = "riffdb://contract/active";
        let state = McpObserverState::new();
        state
            .subscribe_with_baseline(uri, fingerprint(1))
            .expect("subscribe with authorized baseline");
        let snapshot = state
            .subscription_snapshot()
            .expect("subscription snapshot");
        assert!(!snapshot[0].needs_baseline);

        let updates = state
            .apply_successful_pass(
                None,
                vec![(
                    uri.to_owned(),
                    McpSubscribedResourceObservation::Visible(fingerprint(2)),
                    None,
                    snapshot[0].generation,
                )],
            )
            .expect("changed resource observation");
        assert_eq!(updates.len(), 1);
        assert_eq!(updates[0].uri, uri);
    }

    #[test]
    fn first_catalog_is_baseline_and_real_changes_coalesce() {
        let state = McpObserverState::new();
        state
            .replace_visible_catalogs(vec![fingerprint(1)], vec![fingerprint(2)])
            .expect("baseline");
        assert_eq!(
            state.take_pending().expect("empty baseline markers"),
            McpPendingNotifications {
                tools_list_changed: false,
                resources_list_changed: false,
                resource_updates: Vec::new(),
            }
        );

        state
            .replace_visible_catalogs(vec![fingerprint(3)], vec![fingerprint(4)])
            .expect("changed catalog");
        state
            .replace_visible_catalogs(vec![fingerprint(5)], vec![fingerprint(6)])
            .expect("coalesced change");
        let pending = state.take_pending().expect("changed markers");
        assert!(pending.tools_list_changed);
        assert!(pending.resources_list_changed);
        assert_eq!(pending.marker_count(), 2);
    }

    #[test]
    fn resource_markers_are_latest_only_and_unsubscribe_removes_them() {
        let state = McpObserverState::new();
        state
            .subscribe("riffdb://contract/active")
            .expect("subscribe");
        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("first marker");
        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("coalesced marker");
        assert_eq!(
            state.take_pending().expect("one marker").resource_updates(),
            ["riffdb://contract/active"]
        );
        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("new marker");
        assert_eq!(
            state.unsubscribe("riffdb://contract/active"),
            Ok(McpUnsubscribeResult::Removed)
        );
        assert!(
            state
                .take_pending()
                .expect("removed marker")
                .resource_updates()
                .is_empty()
        );
    }

    #[test]
    fn subscription_baselines_and_newer_markers_are_not_consumed_by_older_reads() {
        let uri = "riffdb://contract/active";
        let state = McpObserverState::new();
        state.subscribe(uri).expect("subscribe");

        let initial = state.subscription_snapshot().expect("initial snapshot");
        assert!(
            initial[0].needs_baseline,
            "new subscriptions require one baseline read"
        );
        assert!(
            state
                .apply_successful_pass(
                    None,
                    vec![(
                        uri.to_owned(),
                        McpSubscribedResourceObservation::Visible(fingerprint(1)),
                        None,
                        initial[0].generation,
                    )],
                )
                .expect("baseline")
                .is_empty()
        );
        assert!(
            !state.subscription_snapshot().expect("baseline snapshot")[0].needs_baseline,
            "a completed baseline is not polled continuously"
        );

        state.mark_resource_updated(uri).expect("first marker");
        let acknowledged_epoch =
            state.subscription_snapshot().expect("dirty snapshot")[0].pending_epoch;
        let generation = state.subscription_snapshot().expect("dirty snapshot")[0].generation;
        state
            .mark_resource_updated(uri)
            .expect("newer concurrent marker");
        assert!(
            state
                .apply_successful_pass(
                    None,
                    vec![(
                        uri.to_owned(),
                        McpSubscribedResourceObservation::Visible(fingerprint(1)),
                        acknowledged_epoch,
                        generation,
                    )],
                )
                .expect("older read")
                .is_empty(),
            "an older unchanged read cannot acknowledge a newer marker"
        );
        assert_eq!(
            state
                .take_pending()
                .expect("newer marker")
                .resource_updates(),
            [uri]
        );
    }

    #[test]
    fn output_backpressure_restores_only_live_coalesced_markers() {
        let state = McpObserverState::new();
        state
            .subscribe("riffdb://contract/active")
            .expect("subscribe");
        state
            .replace_visible_catalogs(vec![fingerprint(1)], vec![fingerprint(2)])
            .expect("baseline");
        state
            .replace_visible_catalogs(vec![fingerprint(3)], vec![fingerprint(4)])
            .expect("changed");
        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("update");
        let pending = state.take_pending().expect("pending");
        assert_eq!(pending.marker_count(), 3);
        state.restore_pending(pending).expect("restore");
        state
            .restore_pending(
                state
                    .take_pending()
                    .expect("restored markers remain bounded"),
            )
            .expect("coalesced restore");
        assert_eq!(state.take_pending().expect("one of each").marker_count(), 3);

        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("update");
        let pending = state.take_pending().expect("resource marker");
        state
            .unsubscribe("riffdb://contract/active")
            .expect("unsubscribe");
        state
            .restore_pending(pending)
            .expect("existence-blind drop");
        assert_eq!(state.take_pending().expect("removed").marker_count(), 0);
    }

    #[test]
    fn due_and_dirty_signals_coalesce_and_newer_signals_survive_a_pass() {
        let state = McpObserverState::new();
        state.mark_due().expect("first due");
        state.mark_due().expect("coalesced due");
        state.mark_catalog_dirty().expect("dirty catalog");
        assert_eq!(state.take_due(), Ok(true));
        assert_eq!(state.take_due(), Ok(false));

        state.mark_due().expect("new due during pass");
        state
            .mark_catalog_dirty()
            .expect("new catalog signal during pass");
        assert!(
            state
                .apply_successful_pass(None, Vec::new())
                .expect("older successful pass")
                .is_empty()
        );
        assert_eq!(
            state.take_due(),
            Ok(true),
            "an older pass cannot erase a newer coalesced signal"
        );
    }

    #[test]
    fn cancellation_releases_every_nondurable_value_and_is_idempotent() {
        let state = McpObserverState::new();
        state
            .subscribe("riffdb://contract/active")
            .expect("subscribe");
        state.mark_due().expect("due");
        state
            .mark_resource_updated("riffdb://contract/active")
            .expect("update");
        state.cancel();
        state.cancel();
        assert_eq!(state.subscribed_uris(), Err(McpObserverError::Cancelled));
        assert_eq!(state.take_pending(), Err(McpObserverError::Cancelled));
        assert_eq!(format!("{state:?}"), "McpObserverState([REDACTED])");
    }

    #[test]
    fn compact_inventory_and_fingerprint_bounds_fail_closed() {
        assert_eq!(
            McpVisibleFingerprint::new(Vec::new()),
            Err(McpObserverError::LimitExceeded)
        );
        assert_eq!(
            McpVisibleFingerprint::new(vec![0; MAX_MCP_COMPACT_FINGERPRINT_BYTES + 1]),
            Err(McpObserverError::LimitExceeded)
        );
        let state = McpObserverState::new();
        assert_eq!(
            state.replace_visible_catalogs(
                vec![fingerprint(1); MAX_MCP_VISIBLE_FINGERPRINTS + 1],
                Vec::new(),
            ),
            Err(McpObserverError::LimitExceeded)
        );
    }

    // req: MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043, MCP-045
    #[test]
    fn changed_inventory_accepts_exact_500_500_24_sequence() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        for inventory in [McpObservedInventory::Tools, McpObservedInventory::Resources] {
            backend.push_discovery(inventory, Ok(page(0, 500, Some(cursor(1)), 2)));
            backend.push_discovery(inventory, Ok(page(500, 500, Some(cursor(2)), 2)));
            backend.push_discovery(inventory, Ok(page(1_000, 24, None, 2)));
        }

        let state = Arc::new(McpObserverState::new());
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(state.take_pending().expect("baseline").marker_count(), 0);
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Refreshed)
        );
        let pending = state.take_pending().expect("changed markers");
        assert!(pending.tools_list_changed);
        assert!(pending.resources_list_changed);

        let calls = backend.discovery_calls();
        assert_eq!(calls.len(), 8);
        assert!(
            calls
                .iter()
                .all(|call| call.limit == MCP_OBSERVER_DISCOVERY_PAGE_LIMIT)
        );
        assert_eq!(calls[2].prior_fence, Some(1));
        assert_eq!(calls[3].prior_fence, Some(1));
        assert_eq!(calls[4].cursor, Some(cursor(1)));
        assert_eq!(calls[5].cursor, Some(cursor(2)));
        assert_eq!(calls[6].cursor, Some(cursor(1)));
        assert_eq!(calls[7].cursor, Some(cursor(2)));
        assert_eq!(observer.logical_operations(), 8);
    }

    #[test]
    fn logical_operation_budget_matches_worst_case_changed_tick_composition() {
        let compact_operations = u32::from(MAX_MCP_OBSERVER_DISCOVERY_CALLS).checked_mul(2);
        let subscription_operations = u32::try_from(MAX_MCP_SUBSCRIPTIONS);
        assert_eq!(compact_operations, Some(6));
        assert_eq!(subscription_operations, Ok(8));
        assert_eq!(
            compact_operations.expect("bounded compact operations")
                + subscription_operations.expect("bounded subscriptions"),
            MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_TICK
        );
        assert_eq!(
            MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_TICK
                .checked_mul(u32::from(MAX_MCP_OBSERVER_TICKS)),
            Some(MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_SESSION)
        );

        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        for (inventory, start) in [
            (McpObservedInventory::Tools, 10),
            (McpObservedInventory::Resources, 20),
        ] {
            backend.push_discovery(inventory, Ok(page(start, 1, Some(cursor(1)), 2)));
            backend.push_discovery(inventory, Ok(page(start + 1, 1, Some(cursor(2)), 2)));
            backend.push_discovery(inventory, Ok(page(start + 2, 1, None, 2)));
        }

        let state = Arc::new(McpObserverState::new());
        let uris = (1..=MAX_MCP_SUBSCRIPTIONS)
            .map(|id| format!("riffdb://projection/orders/{id}/status"))
            .collect::<Vec<_>>();
        for uri in &uris {
            state.subscribe(uri).expect("bounded subscription");
            backend.push_resource(
                uri,
                Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))),
            );
            backend.push_resource(
                uri,
                Ok(McpSubscribedResourceObservation::Visible(fingerprint(2))),
            );
        }
        let mut observer = McpObserverLoop::new(
            backend,
            state,
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        let before_changed_tick = observer.logical_operations();
        assert_eq!(before_changed_tick, 10);
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(
            observer
                .logical_operations()
                .saturating_sub(before_changed_tick),
            MAX_MCP_OBSERVER_LOGICAL_OPERATIONS_PER_TICK
        );
    }

    // req: MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043, MCP-045
    #[test]
    fn item_1025_and_cursor_after_1024_abort_without_partial_refresh() {
        for terminal_count in [25, 24] {
            let backend = Arc::new(ScriptedBackend::default());
            backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
            backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
            backend.push_discovery(
                McpObservedInventory::Tools,
                Ok(page(0, 500, Some(cursor(1)), 2)),
            );
            backend.push_discovery(McpObservedInventory::Resources, Ok(page(0, 0, None, 2)));
            backend.push_discovery(
                McpObservedInventory::Tools,
                Ok(page(500, 500, Some(cursor(2)), 2)),
            );
            backend.push_discovery(
                McpObservedInventory::Tools,
                Ok(page(
                    1_000,
                    terminal_count,
                    (terminal_count == 24).then(|| cursor(3)),
                    2,
                )),
            );

            let state = Arc::new(McpObserverState::new());
            let mut observer = McpObserverLoop::new(
                Arc::clone(&backend),
                Arc::clone(&state),
                Arc::new(McpObserverSemaphore::new()),
                backpressured_sink(),
                Duration::ZERO,
            )
            .expect("observer");
            assert_eq!(
                block_on(observer.observe_tick(Duration::from_secs(5))),
                Ok(McpObserverTickResult::Refreshed)
            );
            assert_eq!(
                block_on(observer.observe_tick(Duration::from_secs(10))),
                Ok(McpObserverTickResult::RetryNextTick)
            );
            assert_eq!(
                state
                    .take_pending()
                    .expect("no partial markers")
                    .marker_count(),
                0
            );
            assert_eq!(
                backend
                    .discovery_calls()
                    .iter()
                    .filter(|call| {
                        call.inventory == McpObservedInventory::Tools && call.prior_fence == Some(1)
                            || call.inventory == McpObservedInventory::Tools
                                && call.cursor.is_some()
                    })
                    .count(),
                3
            );
        }
    }

    #[test]
    fn a_fourth_compact_call_is_never_attempted() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        backend.push_discovery(
            McpObservedInventory::Tools,
            Ok(page(1, 1, Some(cursor(1)), 2)),
        );
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 2)));
        backend.push_discovery(
            McpObservedInventory::Tools,
            Ok(page(2, 1, Some(cursor(2)), 2)),
        );
        backend.push_discovery(
            McpObservedInventory::Tools,
            Ok(page(3, 1, Some(cursor(3)), 2)),
        );

        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::new(McpObserverState::new()),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::RetryNextTick)
        );
        assert_eq!(backend.discovery_calls().len(), 6);
    }

    #[test]
    fn exhausted_observer_permits_coalesce_until_the_next_tick() {
        let semaphore = Arc::new(McpObserverSemaphore::new());
        let permits: Vec<_> = (0..crate::MAX_MCP_OBSERVERS_IN_FLIGHT)
            .map(|_| semaphore.try_acquire().expect("within observer bound"))
            .collect();
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(0, 0, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(0, 0, None, 1)));
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::new(McpObserverState::new()),
            Arc::clone(&semaphore),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Deferred)
        );
        assert!(backend.discovery_calls().is_empty());
        drop(permits);
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(backend.discovery_calls().len(), 2);
    }

    #[test]
    fn subscription_polling_and_markers_follow_exact_resource_kinds() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        backend.push_discovery(
            McpObservedInventory::Tools,
            Ok(McpCompactObservationResult::CatalogUnchanged(1)),
        );
        backend.push_discovery(
            McpObservedInventory::Resources,
            Ok(McpCompactObservationResult::CatalogUnchanged(1)),
        );
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 2)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 2)));

        let active = "riffdb://contract/active";
        let projection = "riffdb://projection/LegalSpend/1/status";
        let health = "riffdb://server/health";
        let wakeup = "riffdb://reactive/wakeup";
        for uri in [active, projection, health, wakeup] {
            backend.push_resource(
                uri,
                Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))),
            );
        }
        for uri in [projection, health, wakeup] {
            backend.push_resource(
                uri,
                Ok(McpSubscribedResourceObservation::Visible(fingerprint(2))),
            );
        }
        for uri in [active, projection, health, wakeup] {
            backend.push_resource(
                uri,
                Ok(McpSubscribedResourceObservation::Visible(fingerprint(3))),
            );
        }

        let state = Arc::new(McpObserverState::new());
        for uri in [active, projection, health, wakeup] {
            state.subscribe(uri).expect("supported subscription");
        }
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(state.take_pending().expect("baseline").marker_count(), 0);
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Unchanged)
        );
        assert_eq!(
            state
                .take_pending()
                .expect("continuous markers")
                .resource_updates(),
            [projection, wakeup, health]
        );
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(15))),
            Ok(McpObserverTickResult::Refreshed)
        );
        let pending = state.take_pending().expect("catalog-driven reads");
        assert!(!pending.tools_list_changed);
        assert!(!pending.resources_list_changed);
        assert_eq!(
            pending.resource_updates(),
            [active, projection, wakeup, health]
        );
        assert_eq!(
            backend.resource_calls(),
            [
                active.to_owned(),
                projection.to_owned(),
                wakeup.to_owned(),
                health.to_owned(),
                projection.to_owned(),
                wakeup.to_owned(),
                health.to_owned(),
                active.to_owned(),
                projection.to_owned(),
                wakeup.to_owned(),
                health.to_owned(),
            ]
        );
    }

    #[test]
    fn successful_pass_emits_coalesced_notifications_in_deterministic_order() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(3, 1, None, 2)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(4, 1, None, 2)));
        let active = "riffdb://contract/active";
        backend.push_resource(
            active,
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))),
        );
        backend.push_resource(
            active,
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(2))),
        );

        let telemetry = Arc::new(RecordingTelemetry::default());
        let state = Arc::new(McpObserverState::with_telemetry(telemetry.clone()));
        state.subscribe(active).expect("supported subscription");
        let sink = Arc::new(RecordingSink::default());
        let notification_sink: Arc<dyn McpObserverNotificationSink> = sink.clone();
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            notification_sink,
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert!(sink.accepted().is_empty(), "baseline emits no notification");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(
            sink.accepted(),
            [
                McpObserverNotification::ToolsListChanged,
                McpObserverNotification::ResourcesListChanged,
                McpObserverNotification::ResourceUpdated(McpObserverResourceUpdate::new(
                    active.to_owned(),
                    1,
                )),
            ]
        );
        assert_eq!(
            state
                .take_pending()
                .expect("all markers accepted")
                .marker_count(),
            0
        );
        assert_eq!(
            telemetry.snapshot(),
            [
                McpTelemetryEvent::ListChangeNotification {
                    kind: McpListChangeKind::Tools,
                },
                McpTelemetryEvent::ListChangeNotification {
                    kind: McpListChangeKind::Resources,
                },
            ]
        );
    }

    #[test]
    fn backpressured_resource_marker_is_reread_before_retry_emission() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        for inventory in [McpObservedInventory::Tools, McpObservedInventory::Resources] {
            backend.push_discovery(
                inventory,
                Ok(McpCompactObservationResult::CatalogUnchanged(1)),
            );
            backend.push_discovery(
                inventory,
                Ok(McpCompactObservationResult::CatalogUnchanged(1)),
            );
        }
        let active = "riffdb://contract/active";
        backend.push_resource(
            active,
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))),
        );
        backend.push_resource(
            active,
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(2))),
        );
        backend.push_resource(
            active,
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(2))),
        );

        let state = Arc::new(McpObserverState::new());
        state.subscribe(active).expect("supported subscription");
        let sink = Arc::new(RecordingSink::default());
        sink.push_result(Err(McpObserverNotificationError::Backpressured));
        sink.push_result(Ok(()));
        let notification_sink: Arc<dyn McpObserverNotificationSink> = sink.clone();
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            notification_sink,
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        state
            .mark_resource_updated(active)
            .expect("coalesced external marker");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Ok(McpObserverTickResult::Unchanged)
        );
        assert!(sink.accepted().is_empty(), "backpressure accepts nothing");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(15))),
            Ok(McpObserverTickResult::Unchanged)
        );
        assert_eq!(
            sink.accepted(),
            [McpObserverNotification::ResourceUpdated(
                McpObserverResourceUpdate::new(active.to_owned(), 1)
            )]
        );
        assert_eq!(
            backend.resource_calls(),
            [active.to_owned(), active.to_owned(), active.to_owned()]
        );
        assert_eq!(
            state
                .take_pending()
                .expect("retry was accepted")
                .marker_count(),
            0
        );
    }

    #[test]
    fn closed_notification_sink_terminates_the_session() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(3, 1, None, 2)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(4, 1, None, 2)));
        let sink = Arc::new(RecordingSink::default());
        sink.push_result(Err(McpObserverNotificationError::Closed));
        let notification_sink: Arc<dyn McpObserverNotificationSink> = sink;
        let state = Arc::new(McpObserverState::new());
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            notification_sink,
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Err(McpObserverLoopError::Cancelled)
        );
        assert_eq!(state.take_pending(), Err(McpObserverError::Cancelled));
    }

    #[test]
    fn hidden_subscription_is_removed_without_an_update_marker() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(0, 0, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(0, 0, None, 1)));
        backend.push_resource(
            "riffdb://contract/active",
            Ok(McpSubscribedResourceObservation::Hidden),
        );
        let state = Arc::new(McpObserverState::new());
        state
            .subscribe("riffdb://contract/active")
            .expect("subscribe");
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");

        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert!(state.subscribed_uris().expect("live observer").is_empty());
        assert_eq!(state.take_pending().expect("no leak").marker_count(), 0);
    }

    #[test]
    fn schedule_has_no_catch_up_and_stops_after_180_ticks() {
        let backend = Arc::new(StableBackend::default());
        let state = Arc::new(McpObserverState::new());
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(4))),
            Ok(McpObserverTickResult::NotDue)
        );
        for tick in 1..=MAX_MCP_OBSERVER_TICKS {
            let now = Duration::from_secs(u64::from(tick) * 5);
            let expected = if tick == 1 {
                McpObserverTickResult::Refreshed
            } else {
                McpObserverTickResult::Unchanged
            };
            assert_eq!(block_on(observer.observe_tick(now)), Ok(expected));
        }
        assert_eq!(observer.ticks(), MAX_MCP_OBSERVER_TICKS);
        assert_eq!(
            observer.logical_operations(),
            u32::from(MAX_MCP_OBSERVER_TICKS) * 2
        );
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(905))),
            Ok(McpObserverTickResult::Expired)
        );
        assert_eq!(state.subscribed_uris(), Err(McpObserverError::Cancelled));
    }

    #[test]
    #[cfg(feature = "stdio")]
    fn injected_scheduler_closes_at_the_exact_900_second_boundary() {
        let backend = Arc::new(StableBackend::default());
        let state = Arc::new(McpObserverState::new());
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");
        let mut scheduler = DeterministicObserverScheduler {
            samples: (1..=MAX_MCP_OBSERVER_TICKS)
                .map(|tick| Ok(Duration::from_secs(u64::from(tick) * 5)))
                .collect(),
        };

        assert_eq!(
            block_on(drive_mcp_observer(&mut observer, &mut scheduler)),
            Ok(McpObserverTickResult::Expired)
        );
        assert_eq!(observer.ticks(), MAX_MCP_OBSERVER_TICKS);
        assert_eq!(
            observer.logical_operations(),
            u32::from(MAX_MCP_OBSERVER_TICKS) * 2
        );
        assert!(scheduler.samples.is_empty());
        assert_eq!(state.subscribed_uris(), Err(McpObserverError::Cancelled));
    }

    #[test]
    fn monotonic_regression_and_auth_loss_cancel_without_partial_state() {
        let backend = Arc::new(ScriptedBackend::default());
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(1, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(2, 1, None, 1)));
        backend.push_discovery(McpObservedInventory::Tools, Ok(page(3, 1, None, 2)));
        backend.push_discovery(McpObservedInventory::Resources, Ok(page(4, 1, None, 2)));
        backend.push_resource(
            "riffdb://server/health",
            Ok(McpSubscribedResourceObservation::Visible(fingerprint(1))),
        );
        backend.push_resource(
            "riffdb://server/health",
            Err(McpObserverBackendError::AuthenticationLost),
        );
        let state = Arc::new(McpObserverState::new());
        state
            .subscribe("riffdb://server/health")
            .expect("subscription");
        let mut observer = McpObserverLoop::new(
            Arc::clone(&backend),
            Arc::clone(&state),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::ZERO,
        )
        .expect("observer");
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(5))),
            Ok(McpObserverTickResult::Refreshed)
        );
        assert_eq!(
            block_on(observer.observe_tick(Duration::from_secs(10))),
            Err(McpObserverLoopError::AuthenticationLost)
        );
        assert_eq!(state.take_pending(), Err(McpObserverError::Cancelled));

        let stable = Arc::new(StableBackend::default());
        let mut regressing = McpObserverLoop::new(
            stable,
            Arc::new(McpObserverState::new()),
            Arc::new(McpObserverSemaphore::new()),
            backpressured_sink(),
            Duration::from_secs(10),
        )
        .expect("observer");
        assert_eq!(
            block_on(regressing.observe_tick(Duration::from_secs(9))),
            Err(McpObserverLoopError::ClockUnavailable)
        );
    }
}
