//! Bounded MCP progress, cancellation, and request-poll state.

use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use rmcp::model::{ProgressNotificationParam, ProgressToken, RequestId};
use tokio_util::sync::CancellationToken;

/// Maximum progress notifications emitted for one request.
pub const MAX_MCP_PROGRESS_NOTIFICATIONS: u8 = 32;
/// Maximum retained serialized bytes in one progress token.
pub const MAX_MCP_PROGRESS_TOKEN_BYTES: usize = 1_024;
/// Largest integer exactly representable by JSON's interoperable number range.
pub const MAX_MCP_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
/// Maximum active cancellation entries, matching server in-flight admission.
pub const MAX_MCP_CANCELLATION_ENTRIES: usize = 256;
/// Maximum retained serialized bytes in one MCP JSON-RPC request identifier.
pub const MAX_MCP_REQUEST_ID_BYTES: usize = 1_024;
/// Maximum request-scoped service observations.
pub const MAX_MCP_POLL_OBSERVATIONS: u8 = 32;
/// Minimum time between request-scoped observations.
pub const MIN_MCP_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Maximum request-scoped polling lifetime.
pub const MAX_MCP_POLL_LIFETIME: Duration = Duration::from_secs(30);

/// One request's bounded progress state.
pub struct McpProgressTracker {
    token: Option<ProgressToken>,
    emitted: u8,
    last_progress: Option<u64>,
    total: Option<u64>,
    terminal: bool,
}

impl McpProgressTracker {
    /// Creates progress state only when capability negotiation and token input agree.
    pub fn new(negotiated: bool, token: Option<ProgressToken>) -> Result<Self, McpProgressError> {
        let token = if negotiated {
            match token {
                Some(token) => {
                    let bytes = serde_json::to_vec(&token)
                        .map_err(|_| McpProgressError::InvalidProgress)?;
                    if bytes.is_empty() || bytes.len() > MAX_MCP_PROGRESS_TOKEN_BYTES {
                        return Err(McpProgressError::InvalidProgress);
                    }
                    Some(token)
                }
                None => None,
            }
        } else {
            None
        };
        Ok(Self {
            token,
            emitted: 0,
            last_progress: None,
            total: None,
            terminal: false,
        })
    }

    /// Produces one checked notification for a real completed-work milestone.
    pub fn advance(
        &mut self,
        progress: u64,
        total: Option<u64>,
    ) -> Result<Option<ProgressNotificationParam>, McpProgressError> {
        let Some(token) = self.token.clone() else {
            return Ok(None);
        };
        if self.terminal
            || self.emitted >= MAX_MCP_PROGRESS_NOTIFICATIONS
            || progress > MAX_MCP_SAFE_INTEGER
            || total.is_some_and(|total| total > MAX_MCP_SAFE_INTEGER || progress > total)
            || self
                .last_progress
                .is_some_and(|last_progress| progress < last_progress)
            || self.total.is_some() && total != self.total
        {
            return Err(McpProgressError::InvalidProgress);
        }
        if self.total.is_none() {
            self.total = total;
        }
        self.last_progress = Some(progress);
        self.emitted = self
            .emitted
            .checked_add(1)
            .ok_or(McpProgressError::InvalidProgress)?;
        let notification = match total {
            Some(total) => {
                ProgressNotificationParam::new(token, progress as f64).with_total(total as f64)
            }
            None => ProgressNotificationParam::new(token, progress as f64),
        };
        Ok(Some(notification))
    }

    /// Permanently ends progress before the terminal result is released.
    pub fn finish(&mut self) {
        self.terminal = true;
        self.token = None;
    }

    /// Reports whether progress is enabled for this request.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.token.is_some() && !self.terminal
    }
}

impl fmt::Debug for McpProgressTracker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpProgressTracker([REDACTED])")
    }
}

/// Closed progress-state failure without token or request data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpProgressError {
    /// Token, number, order, total, or notification count was invalid.
    InvalidProgress,
}

impl fmt::Display for McpProgressError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP progress is unavailable")
    }
}

impl Error for McpProgressError {}

struct CancellationState {
    cancellation: CancellationToken,
}

/// Cloneable read-only cancellation signal passed to backend work.
#[derive(Clone)]
pub struct McpCancellationSignal(Arc<CancellationState>);

impl McpCancellationSignal {
    /// Reports whether the matching MCP request was cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.cancellation.is_cancelled()
    }

    /// Resolves promptly and race-safely once the matching request is cancelled.
    ///
    /// Backends select this future against pending transport work so dropping
    /// the selected-out operation releases nondurable client and RPC state
    /// without polling or sleeps.
    pub async fn cancelled(&self) {
        self.0.cancellation.cancelled().await;
    }
}

impl fmt::Debug for McpCancellationSignal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCancellationSignal([REDACTED])")
    }
}

#[derive(Default)]
struct CancellationInner {
    entries: HashMap<RequestId, Weak<CancellationState>>,
}

/// Bounded registry connecting MCP cancellation notifications to live work.
#[derive(Default)]
pub struct McpCancellationRegistry {
    inner: Mutex<CancellationInner>,
}

impl McpCancellationRegistry {
    /// Creates an empty cancellation registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CancellationInner {
                entries: HashMap::new(),
            }),
        }
    }

    /// Registers one exact live JSON-RPC request identity.
    pub fn register(
        self: &Arc<Self>,
        request_id: RequestId,
    ) -> Result<(McpCancellationSignal, McpCancellationGuard), McpCancellationError> {
        self.register_with_token(request_id, CancellationToken::new())
    }

    pub(crate) fn register_with_token(
        self: &Arc<Self>,
        request_id: RequestId,
        cancellation: CancellationToken,
    ) -> Result<(McpCancellationSignal, McpCancellationGuard), McpCancellationError> {
        validate_request_id(&request_id)?;
        let mut inner = self.lock();
        inner.entries.retain(|_, state| state.strong_count() != 0);
        if inner.entries.contains_key(&request_id) {
            return Err(McpCancellationError::DuplicateRequest);
        }
        if inner.entries.len() >= MAX_MCP_CANCELLATION_ENTRIES {
            return Err(McpCancellationError::CapacityExhausted);
        }
        let state = Arc::new(CancellationState { cancellation });
        inner
            .entries
            .insert(request_id.clone(), Arc::downgrade(&state));
        Ok((
            McpCancellationSignal(Arc::clone(&state)),
            McpCancellationGuard {
                registry: Arc::clone(self),
                request_id: Some(request_id),
                state,
            },
        ))
    }

    /// Requests cancellation when the exact live request is still registered.
    pub fn cancel(&self, request_id: &RequestId) -> Result<bool, McpCancellationError> {
        validate_request_id(request_id)?;
        let state = {
            let mut inner = self.lock();
            let Some(state) = inner.entries.get(request_id).and_then(Weak::upgrade) else {
                inner.entries.remove(request_id);
                return Ok(false);
            };
            state
        };
        state.cancellation.cancel();
        Ok(true)
    }

    #[cfg(any(feature = "stdio", test))]
    pub(crate) fn cancel_all(&self) {
        let states: Vec<_> = {
            let mut inner = self.lock();
            inner.entries.retain(|_, state| state.strong_count() != 0);
            inner.entries.values().filter_map(Weak::upgrade).collect()
        };
        for state in states {
            state.cancellation.cancel();
        }
    }

    fn remove(&self, request_id: &RequestId, state: &Arc<CancellationState>) {
        let mut inner = self.lock();
        let matches = inner
            .entries
            .get(request_id)
            .and_then(Weak::upgrade)
            .is_some_and(|registered| Arc::ptr_eq(&registered, state));
        if matches {
            inner.entries.remove(request_id);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CancellationInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.lock().entries.len()
    }
}

impl fmt::Debug for McpCancellationRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCancellationRegistry([REDACTED])")
    }
}

/// Non-cloneable live registration removed exactly once on drop.
pub struct McpCancellationGuard {
    registry: Arc<McpCancellationRegistry>,
    request_id: Option<RequestId>,
    state: Arc<CancellationState>,
}

impl fmt::Debug for McpCancellationGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpCancellationGuard([REDACTED])")
    }
}

impl Drop for McpCancellationGuard {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id.take() {
            self.registry.remove(&request_id, &self.state);
        }
    }
}

/// Closed cancellation-registry failure without request identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpCancellationError {
    /// The JSON-RPC request identity was not bounded.
    InvalidRequest,
    /// Another live request already owns the exact identity.
    DuplicateRequest,
    /// The bounded registry has no free entry.
    CapacityExhausted,
}

impl fmt::Display for McpCancellationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP cancellation is unavailable")
    }
}

impl Error for McpCancellationError {}

fn validate_request_id(request_id: &RequestId) -> Result<(), McpCancellationError> {
    let bytes = serde_json::to_vec(request_id).map_err(|_| McpCancellationError::InvalidRequest)?;
    if bytes.is_empty() || bytes.len() > MAX_MCP_REQUEST_ID_BYTES {
        return Err(McpCancellationError::InvalidRequest);
    }
    Ok(())
}

/// Deterministic request-scoped polling budget over injected clock samples.
#[derive(Clone, Copy, Debug)]
pub struct McpPollBudget {
    started_at: Duration,
    last_observation_at: Option<Duration>,
    observations: u8,
}

impl McpPollBudget {
    /// Starts one request-local polling budget.
    #[must_use]
    pub const fn new(started_at: Duration) -> Self {
        Self {
            started_at,
            last_observation_at: None,
            observations: 0,
        }
    }

    /// Admits one observation only within every count, spacing, and lifetime bound.
    pub fn observe(&mut self, now: Duration) -> Result<(), McpPollError> {
        let elapsed = now
            .checked_sub(self.started_at)
            .ok_or(McpPollError::ClockUnavailable)?;
        if elapsed > MAX_MCP_POLL_LIFETIME
            || self.observations >= MAX_MCP_POLL_OBSERVATIONS
            || self.last_observation_at.is_some_and(|previous| {
                now.checked_sub(previous)
                    .is_none_or(|spacing| spacing < MIN_MCP_POLL_INTERVAL)
            })
        {
            return Err(McpPollError::BudgetExhausted);
        }
        self.last_observation_at = Some(now);
        self.observations = self
            .observations
            .checked_add(1)
            .ok_or(McpPollError::BudgetExhausted)?;
        Ok(())
    }

    /// Returns the number of successfully admitted observations.
    #[must_use]
    pub const fn observations(&self) -> u8 {
        self.observations
    }
}

/// Closed request-poll failure without timing or request data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpPollError {
    /// A clock sample preceded the request start.
    ClockUnavailable,
    /// Count, spacing, or lifetime no longer permits an observation.
    BudgetExhausted,
}

impl fmt::Display for McpPollError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP polling is unavailable")
    }
}

impl Error for McpPollError {}

#[cfg(test)]
mod tests {
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicBool, Ordering},
        task::{Context, Poll},
    };

    use futures::future::{Either, select};
    use rmcp::model::NumberOrString;

    use super::*;

    fn token(value: &str) -> ProgressToken {
        ProgressToken(NumberOrString::String(value.into()))
    }

    fn request(value: i64) -> RequestId {
        NumberOrString::Number(value)
    }

    struct PendingBackend {
        polled: Arc<AtomicBool>,
        dropped: Arc<AtomicBool>,
    }

    impl Future for PendingBackend {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.polled.store(true, Ordering::Release);
            Poll::Pending
        }
    }

    impl Drop for PendingBackend {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    #[test]
    fn progress_is_negotiated_token_bound_monotonic_and_terminal() {
        let mut disabled =
            McpProgressTracker::new(false, Some(token("ignored"))).expect("disabled tracker");
        assert_eq!(disabled.advance(0, None), Ok(None));

        let mut tracker =
            McpProgressTracker::new(true, Some(token("progress"))).expect("enabled tracker");
        let first = tracker
            .advance(0, Some(2))
            .expect("first progress")
            .expect("enabled");
        assert_eq!(first.progress, 0.0);
        assert_eq!(first.total, Some(2.0));
        assert!(tracker.advance(1, Some(2)).expect("second").is_some());
        assert_eq!(
            tracker.advance(0, Some(2)),
            Err(McpProgressError::InvalidProgress)
        );
        tracker.finish();
        assert_eq!(tracker.advance(2, Some(2)), Ok(None));
        assert!(!tracker.is_enabled());
    }

    #[test]
    fn exact_progress_count_and_safe_integer_bounds_fail_closed() {
        let mut tracker =
            McpProgressTracker::new(true, Some(token("bounded"))).expect("enabled tracker");
        for value in 0..MAX_MCP_PROGRESS_NOTIFICATIONS {
            assert!(
                tracker
                    .advance(u64::from(value), None)
                    .expect("within bound")
                    .is_some()
            );
        }
        assert_eq!(
            tracker.advance(u64::from(MAX_MCP_PROGRESS_NOTIFICATIONS), None),
            Err(McpProgressError::InvalidProgress)
        );
        let mut unsafe_integer =
            McpProgressTracker::new(true, Some(token("unsafe"))).expect("enabled tracker");
        assert_eq!(
            unsafe_integer.advance(MAX_MCP_SAFE_INTEGER + 1, None),
            Err(McpProgressError::InvalidProgress)
        );
    }

    #[test]
    fn cancellation_registration_signal_and_drop_are_exact() {
        let registry = Arc::new(McpCancellationRegistry::new());
        let (signal, guard) = registry.register(request(1)).expect("register");
        assert!(!signal.is_cancelled());
        assert_eq!(
            registry.register(request(1)).unwrap_err(),
            McpCancellationError::DuplicateRequest
        );
        assert_eq!(registry.cancel(&request(1)), Ok(true));
        assert!(signal.is_cancelled());
        drop(guard);
        assert_eq!(registry.len(), 0);
        assert_eq!(registry.cancel(&request(1)), Ok(false));
    }

    #[test]
    fn awaitable_cancellation_drops_pending_backend_work_without_polling() {
        let registry = Arc::new(McpCancellationRegistry::new());
        let (signal, _guard) = registry.register(request(7)).expect("register");
        let polled = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let backend = Box::pin(PendingBackend {
            polled: Arc::clone(&polled),
            dropped: Arc::clone(&dropped),
        });
        let cancellation = Box::pin(signal.cancelled());

        assert_eq!(registry.cancel(&request(7)), Ok(true));
        let selected = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime")
            .block_on(select(backend, cancellation));
        let Either::Right(((), pending_backend)) = selected else {
            panic!("pending backend cannot complete");
        };
        assert!(polled.load(Ordering::Acquire));
        assert!(!dropped.load(Ordering::Acquire));
        drop(pending_backend);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn linked_session_and_registry_cancellation_reach_every_live_request() {
        let registry = Arc::new(McpCancellationRegistry::new());
        let session = CancellationToken::new();
        let (first, first_guard) = registry
            .register_with_token(request(8), session.child_token())
            .expect("first linked registration");
        let (second, second_guard) = registry
            .register_with_token(request(9), session.child_token())
            .expect("second linked registration");

        registry.cancel_all();
        assert!(first.is_cancelled());
        assert!(second.is_cancelled());

        drop((first_guard, second_guard));
        assert_eq!(registry.len(), 0);

        let (third, _third_guard) = registry
            .register_with_token(request(10), session.child_token())
            .expect("third linked registration");
        assert!(!third.is_cancelled());
        session.cancel();
        assert!(third.is_cancelled());
    }

    #[test]
    fn cancellation_registry_capacity_is_exact_and_recoverable() {
        let registry = Arc::new(McpCancellationRegistry::new());
        let guards: Vec<_> = (0..MAX_MCP_CANCELLATION_ENTRIES)
            .map(|value| {
                registry
                    .register(request(value as i64))
                    .expect("within registry bound")
                    .1
            })
            .collect();
        assert_eq!(
            registry
                .register(request(MAX_MCP_CANCELLATION_ENTRIES as i64))
                .unwrap_err(),
            McpCancellationError::CapacityExhausted
        );
        drop(guards);
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn polling_count_spacing_lifetime_and_regression_are_deterministic() {
        let mut budget = McpPollBudget::new(Duration::from_secs(10));
        assert_eq!(budget.observe(Duration::from_secs(10)), Ok(()));
        assert_eq!(
            budget.observe(Duration::from_millis(10_099)),
            Err(McpPollError::BudgetExhausted)
        );
        assert_eq!(budget.observe(Duration::from_millis(10_100)), Ok(()));

        let mut count = McpPollBudget::new(Duration::ZERO);
        for observation in 0..MAX_MCP_POLL_OBSERVATIONS {
            assert_eq!(
                count.observe(
                    MIN_MCP_POLL_INTERVAL
                        .checked_mul(u32::from(observation))
                        .expect("bounded schedule")
                ),
                Ok(())
            );
        }
        assert_eq!(
            count.observe(Duration::from_secs(4)),
            Err(McpPollError::BudgetExhausted)
        );
        assert_eq!(
            McpPollBudget::new(Duration::from_secs(1)).observe(Duration::ZERO),
            Err(McpPollError::ClockUnavailable)
        );
        assert_eq!(
            McpPollBudget::new(Duration::ZERO)
                .observe(MAX_MCP_POLL_LIFETIME + Duration::from_nanos(1)),
            Err(McpPollError::BudgetExhausted)
        );
    }

    #[test]
    fn lifecycle_debug_and_errors_do_not_expose_transport_identifiers() {
        let tracker =
            McpProgressTracker::new(true, Some(token("secret"))).expect("enabled tracker");
        assert_eq!(format!("{tracker:?}"), "McpProgressTracker([REDACTED])");
        let registry = McpCancellationRegistry::new();
        assert_eq!(
            format!("{registry:?}"),
            "McpCancellationRegistry([REDACTED])"
        );
        assert_eq!(
            McpCancellationError::CapacityExhausted.to_string(),
            "MCP cancellation is unavailable"
        );
    }
}
