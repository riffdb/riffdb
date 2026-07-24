//! Bounded in-flight admission for MCP request and observer work.

use std::{collections::BTreeMap, error::Error, fmt, sync::Arc, sync::Mutex};

/// Maximum in-flight MCP requests across one server process.
pub const MAX_MCP_SERVER_IN_FLIGHT: usize = 256;
/// Maximum in-flight MCP requests attributed to one session.
pub const MAX_MCP_SESSION_IN_FLIGHT: usize = 8;
/// Maximum visible-ASCII bytes in an internal transport session key.
pub const MAX_MCP_ADMISSION_SESSION_KEY_BYTES: usize = 128;
/// Maximum simultaneously executing session observation passes.
pub const MAX_MCP_OBSERVERS_IN_FLIGHT: usize = 32;

/// Internal transport session identity used only for bounded accounting.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct McpAdmissionSessionKey(String);

impl McpAdmissionSessionKey {
    pub(crate) fn stdio() -> Self {
        Self("stdio".to_owned())
    }

    /// Checks one exact transport session spelling without normalization.
    pub fn new(value: impl Into<String>) -> Result<Self, McpAdmissionError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_MCP_ADMISSION_SESSION_KEY_BYTES
            || !value.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(McpAdmissionError::InvalidSession);
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for McpAdmissionSessionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpAdmissionSessionKey([REDACTED])")
    }
}

#[derive(Default)]
struct RequestCounts {
    total: usize,
    per_session: BTreeMap<McpAdmissionSessionKey, usize>,
}

/// Shared nonblocking request admission limiter.
#[derive(Default)]
pub struct McpInflightLimiter {
    counts: Mutex<RequestCounts>,
}

impl McpInflightLimiter {
    /// Creates one empty server-wide admission registry.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            counts: Mutex::new(RequestCounts {
                total: 0,
                per_session: BTreeMap::new(),
            }),
        }
    }

    /// Acquires one request count or rejects without queuing.
    pub fn try_acquire(
        self: &Arc<Self>,
        session: McpAdmissionSessionKey,
    ) -> Result<McpInflightPermit, McpAdmissionError> {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if counts.total >= MAX_MCP_SERVER_IN_FLIGHT
            || counts
                .per_session
                .get(&session)
                .is_some_and(|count| *count >= MAX_MCP_SESSION_IN_FLIGHT)
        {
            return Err(McpAdmissionError::CapacityExhausted);
        }
        counts.total = counts
            .total
            .checked_add(1)
            .ok_or(McpAdmissionError::CapacityExhausted)?;
        let count = counts.per_session.entry(session.clone()).or_default();
        *count = count
            .checked_add(1)
            .ok_or(McpAdmissionError::CapacityExhausted)?;
        Ok(McpInflightPermit {
            limiter: Arc::clone(self),
            session: Some(session),
        })
    }

    #[cfg(test)]
    fn counts(&self, session: &McpAdmissionSessionKey) -> (usize, usize) {
        let counts = self
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            counts.total,
            counts.per_session.get(session).copied().unwrap_or(0),
        )
    }
}

impl fmt::Debug for McpInflightLimiter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpInflightLimiter([REDACTED])")
    }
}

/// Non-cloneable authority for exactly one admitted request.
pub struct McpInflightPermit {
    limiter: Arc<McpInflightLimiter>,
    session: Option<McpAdmissionSessionKey>,
}

impl fmt::Debug for McpInflightPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpInflightPermit([REDACTED])")
    }
}

impl Drop for McpInflightPermit {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let mut counts = self
            .limiter
            .counts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counts.total = counts.total.saturating_sub(1);
        let remove = if let Some(count) = counts.per_session.get_mut(&session) {
            *count = count.saturating_sub(1);
            *count == 0
        } else {
            false
        };
        if remove {
            counts.per_session.remove(&session);
        }
    }
}

/// Shared nonblocking observer-pass semaphore.
#[derive(Default)]
pub struct McpObserverSemaphore {
    in_flight: Mutex<usize>,
}

impl McpObserverSemaphore {
    /// Creates an empty 32-permit observer boundary.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            in_flight: Mutex::new(0),
        }
    }

    /// Acquires one observer permit without waiting or queuing.
    pub fn try_acquire(self: &Arc<Self>) -> Result<McpObserverPermit, McpAdmissionError> {
        let mut in_flight = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *in_flight >= MAX_MCP_OBSERVERS_IN_FLIGHT {
            return Err(McpAdmissionError::CapacityExhausted);
        }
        *in_flight = in_flight
            .checked_add(1)
            .ok_or(McpAdmissionError::CapacityExhausted)?;
        Ok(McpObserverPermit {
            semaphore: Arc::clone(self),
            active: true,
        })
    }
}

impl fmt::Debug for McpObserverSemaphore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverSemaphore")
    }
}

/// Non-cloneable authority for exactly one observation pass.
pub struct McpObserverPermit {
    semaphore: Arc<McpObserverSemaphore>,
    active: bool,
}

impl fmt::Debug for McpObserverPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpObserverPermit")
    }
}

impl Drop for McpObserverPermit {
    fn drop(&mut self) {
        if !std::mem::take(&mut self.active) {
            return;
        }
        let mut in_flight = self
            .semaphore
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *in_flight = in_flight.saturating_sub(1);
    }
}

/// Closed in-flight admission failure without session or request data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpAdmissionError {
    /// A transport session key was malformed or over bound.
    InvalidSession,
    /// The applicable server, session, or observer count is exhausted.
    CapacityExhausted,
}

impl fmt::Display for McpAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP request admission was denied")
    }
}

impl Error for McpAdmissionError {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn key(value: &str) -> McpAdmissionSessionKey {
        McpAdmissionSessionKey::new(value).expect("bounded session")
    }

    #[test]
    fn per_session_limit_is_exact_and_drop_releases_capacity() {
        let limiter = Arc::new(McpInflightLimiter::new());
        let session = key("session");
        let permits: Vec<_> = (0..MAX_MCP_SESSION_IN_FLIGHT)
            .map(|_| {
                limiter
                    .try_acquire(session.clone())
                    .expect("within per-session limit")
            })
            .collect();
        assert_eq!(
            limiter.try_acquire(session.clone()).unwrap_err(),
            McpAdmissionError::CapacityExhausted
        );
        assert_eq!(
            limiter.counts(&session),
            (MAX_MCP_SESSION_IN_FLIGHT, MAX_MCP_SESSION_IN_FLIGHT)
        );
        drop(permits);
        assert_eq!(limiter.counts(&session), (0, 0));
        assert!(limiter.try_acquire(session).is_ok());
    }

    #[test]
    fn server_limit_is_exact_across_distinct_sessions() {
        let limiter = Arc::new(McpInflightLimiter::new());
        let permits: Vec<_> = (0..MAX_MCP_SERVER_IN_FLIGHT)
            .map(|index| {
                limiter
                    .try_acquire(key(&format!("session-{index}")))
                    .expect("within server limit")
            })
            .collect();
        assert_eq!(
            limiter.try_acquire(key("overflow")).unwrap_err(),
            McpAdmissionError::CapacityExhausted
        );
        drop(permits);
        assert!(limiter.try_acquire(key("after-drop")).is_ok());
    }

    #[test]
    fn concurrent_drop_and_acquire_preserve_exact_counts() {
        let limiter = Arc::new(McpInflightLimiter::new());
        let session = key("scheduled");
        let permit = limiter
            .try_acquire(session.clone())
            .expect("initial permit");
        let barrier = Arc::new(Barrier::new(2));
        let thread_barrier = Arc::clone(&barrier);
        let thread_limiter = Arc::clone(&limiter);
        let thread_session = session.clone();
        let handle = thread::spawn(move || {
            thread_barrier.wait();
            let acquired = thread_limiter
                .try_acquire(thread_session)
                .expect("second permit");
            drop(acquired);
        });
        drop(permit);
        barrier.wait();
        handle.join().expect("thread completes");
        assert_eq!(limiter.counts(&session), (0, 0));
    }

    #[test]
    fn observer_semaphore_is_nonblocking_and_releases_on_drop() {
        let semaphore = Arc::new(McpObserverSemaphore::new());
        let permits: Vec<_> = (0..MAX_MCP_OBSERVERS_IN_FLIGHT)
            .map(|_| semaphore.try_acquire().expect("within observer bound"))
            .collect();
        assert_eq!(
            semaphore.try_acquire().unwrap_err(),
            McpAdmissionError::CapacityExhausted
        );
        drop(permits);
        assert!(semaphore.try_acquire().is_ok());
    }

    #[test]
    fn session_keys_and_errors_are_bounded_and_redacted() {
        assert_eq!(
            format!("{:?}", key("secret")),
            "McpAdmissionSessionKey([REDACTED])"
        );
        for value in ["", "contains space", "\n", &"x".repeat(129)] {
            assert_eq!(
                McpAdmissionSessionKey::new(value),
                Err(McpAdmissionError::InvalidSession)
            );
        }
        assert_eq!(
            McpAdmissionError::CapacityExhausted.to_string(),
            "MCP request admission was denied"
        );
    }
}
