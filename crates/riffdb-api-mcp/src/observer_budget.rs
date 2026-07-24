//! Exact physical service-call accounting for one MCP session observer.

use std::{
    error::Error,
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU32, Ordering},
    },
};

/// Maximum physical service calls admitted for one changed observation tick.
pub const MAX_MCP_OBSERVER_PHYSICAL_CALLS_PER_TICK: u32 = 46;
/// Maximum physical service calls admitted over one observer session.
pub const MAX_MCP_OBSERVER_PHYSICAL_CALLS: u32 = 8_280;

const MAX_COMPACT_DISCOVERY_PHYSICAL_CALLS: u8 = 1;
const MAX_SUBSCRIBED_RESOURCE_PHYSICAL_CALLS: u8 = 5;

/// One session's monotonic watcher-generated physical service-call meter.
#[derive(Default)]
pub struct McpObserverPhysicalCallBudget {
    calls: AtomicU32,
}

impl McpObserverPhysicalCallBudget {
    /// Creates an unspent session meter.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            calls: AtomicU32::new(0),
        }
    }

    /// Starts one compact discovery operation, which may dispatch once.
    #[must_use]
    pub fn compact_discovery(self: &Arc<Self>) -> McpObserverPhysicalCallHandle {
        McpObserverPhysicalCallHandle::new(Arc::clone(self), MAX_COMPACT_DISCOVERY_PHYSICAL_CALLS)
    }

    /// Starts one subscribed-resource operation, which may dispatch five times.
    #[must_use]
    pub fn subscribed_resource(self: &Arc<Self>) -> McpObserverPhysicalCallHandle {
        McpObserverPhysicalCallHandle::new(Arc::clone(self), MAX_SUBSCRIBED_RESOURCE_PHYSICAL_CALLS)
    }

    /// Returns physical calls charged before attempted dispatch.
    #[must_use]
    pub fn calls(&self) -> u32 {
        self.calls.load(Ordering::Acquire)
    }

    fn charge(&self) -> Result<(), McpObserverPhysicalCallError> {
        self.calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |calls| {
                (calls < MAX_MCP_OBSERVER_PHYSICAL_CALLS).then_some(calls + 1)
            })
            .map(|_| ())
            .map_err(|_| McpObserverPhysicalCallError)
    }
}

impl fmt::Debug for McpObserverPhysicalCallBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpObserverPhysicalCallBudget")
            .field("calls", &self.calls())
            .finish()
    }
}

/// One logical observer operation's non-refundable dispatch allowance.
#[derive(Clone)]
pub struct McpObserverPhysicalCallHandle {
    budget: Arc<McpObserverPhysicalCallBudget>,
    operation_calls: Arc<AtomicU8>,
    maximum: u8,
}

impl McpObserverPhysicalCallHandle {
    fn new(budget: Arc<McpObserverPhysicalCallBudget>, maximum: u8) -> Self {
        Self {
            budget,
            operation_calls: Arc::new(AtomicU8::new(0)),
            maximum,
        }
    }

    /// Charges immediately before one physical service dispatch.
    ///
    /// A successful charge is never refunded, even when dispatch or response
    /// handling later fails.
    pub fn charge(&self) -> Result<(), McpObserverPhysicalCallError> {
        self.operation_calls
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |calls| {
                (calls < self.maximum).then_some(calls + 1)
            })
            .map_err(|_| McpObserverPhysicalCallError)?;
        self.budget.charge()
    }

    /// Returns calls charged to this logical operation.
    #[must_use]
    pub fn operation_calls(&self) -> u8 {
        self.operation_calls.load(Ordering::Acquire)
    }
}

impl fmt::Debug for McpObserverPhysicalCallHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("McpObserverPhysicalCallHandle")
            .field("operation_calls", &self.operation_calls())
            .field("maximum", &self.maximum)
            .finish_non_exhaustive()
    }
}

/// Closed observer-budget exhaustion without resource or policy detail.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpObserverPhysicalCallError;

impl fmt::Display for McpObserverPhysicalCallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP observer physical service-call budget is exhausted")
    }
}

impl Error for McpObserverPhysicalCallError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_handles_enforce_one_and_five_without_refund() {
        let budget = Arc::new(McpObserverPhysicalCallBudget::new());
        let compact = budget.compact_discovery();
        compact.charge().expect("one compact call");
        assert_eq!(compact.charge(), Err(McpObserverPhysicalCallError));

        let resource = budget.subscribed_resource();
        for _ in 0..5 {
            resource.charge().expect("one resource call");
        }
        assert_eq!(resource.charge(), Err(McpObserverPhysicalCallError));
        drop(compact);
        drop(resource);
        assert_eq!(budget.calls(), 6);
    }

    #[test]
    fn session_accepts_exactly_eight_thousand_two_hundred_eighty_calls() {
        let budget = Arc::new(McpObserverPhysicalCallBudget::new());
        for _ in 0..(MAX_MCP_OBSERVER_PHYSICAL_CALLS / 5) {
            let operation = budget.subscribed_resource();
            for _ in 0..5 {
                operation.charge().expect("within session budget");
            }
        }
        assert_eq!(budget.calls(), MAX_MCP_OBSERVER_PHYSICAL_CALLS);
        assert_eq!(
            budget.compact_discovery().charge(),
            Err(McpObserverPhysicalCallError)
        );
        assert_eq!(budget.calls(), MAX_MCP_OBSERVER_PHYSICAL_CALLS);
    }

    #[test]
    fn physical_bounds_match_the_accepted_worst_case_schedule() {
        assert_eq!(
            MAX_MCP_OBSERVER_PHYSICAL_CALLS_PER_TICK,
            6 * u32::from(MAX_COMPACT_DISCOVERY_PHYSICAL_CALLS)
                + 8 * u32::from(MAX_SUBSCRIBED_RESOURCE_PHYSICAL_CALLS)
        );
        assert_eq!(
            MAX_MCP_OBSERVER_PHYSICAL_CALLS,
            180 * MAX_MCP_OBSERVER_PHYSICAL_CALLS_PER_TICK
        );
    }
}
