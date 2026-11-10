//! Process-wide upper bound shared by transport and API-neutral request spans.
use std::sync::atomic::{AtomicUsize, Ordering};

/// Maximum concurrently live first-party diagnostic spans per process.
pub const MAX_ACTIVE_DIAGNOSTIC_SPANS: usize = 4096;
struct SpanBudget {
    active: AtomicUsize,
}
static PROCESS_SPANS: SpanBudget = SpanBudget {
    active: AtomicUsize::new(0),
};
/// Opaque admission ownership; cancellation and ordinary drop release it.
pub struct SpanPermit<'a> {
    budget: &'a SpanBudget,
}
impl std::fmt::Debug for SpanPermit<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpanPermit([BOUNDED])")
    }
}
impl Drop for SpanPermit<'_> {
    fn drop(&mut self) {
        self.budget.active.fetch_sub(1, Ordering::AcqRel);
    }
}
impl SpanBudget {
    fn acquire(&self) -> Option<SpanPermit<'_>> {
        self.active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                if value < MAX_ACTIVE_DIAGNOSTIC_SPANS {
                    value.checked_add(1)
                } else {
                    None
                }
            })
            .ok()?;
        Some(SpanPermit { budget: self })
    }
}
/// Admits one span without waiting. A refusal loses diagnostic evidence only.
#[must_use]
pub fn try_admit_diagnostic_span() -> Option<SpanPermit<'static>> {
    let permit = PROCESS_SPANS.acquire();
    if permit.is_none() {
        crate::process_telemetry_delivery_metrics().record(
            crate::TelemetrySignal::Traces,
            crate::TelemetryDisposition::SpanLimit,
            1,
        );
    }
    permit
}

thread_local! {
    static CONSTRUCTING:std::cell::RefCell<Option<std::sync::Arc<SpanPermit<'static>>>>=const {std::cell::RefCell::new(None)};
}
struct ConstructionScope(Option<std::sync::Arc<SpanPermit<'static>>>);
impl Drop for ConstructionScope {
    fn drop(&mut self) {
        let previous = self.0.take();
        let _ = CONSTRUCTING.try_with(|slot| {
            if let Ok(mut slot) = slot.try_borrow_mut() {
                *slot = previous;
            }
        });
    }
}
/// Runs trusted span construction with an admitted slot available to the server
/// subscriber. No borrow is held while invoking the constructor or subscriber.
/// A missing TLS scope fails closed for diagnostic construction only.
pub fn with_diagnostic_span_admission<T>(
    permit: std::sync::Arc<SpanPermit<'static>>,
    construct: impl FnOnce() -> T,
) -> Option<T> {
    let previous = CONSTRUCTING
        .try_with(|slot| {
            slot.try_borrow_mut()
                .map(|mut slot| slot.replace(permit))
                .ok()
        })
        .ok()??;
    let _scope = ConstructionScope(previous);
    Some(construct())
}
/// Composition hook: retain this ownership in the newly registered span's
/// extensions, so every cloned/parent reference shares the same bounded slot.
#[must_use]
pub fn take_constructing_diagnostic_span_admission() -> Option<std::sync::Arc<SpanPermit<'static>>>
{
    CONSTRUCTING
        .try_with(|slot| slot.try_borrow_mut().ok().and_then(|mut slot| slot.take()))
        .ok()
        .flatten()
}
/// Reports whether exactly one admitted span may still be constructed in this scope.
#[must_use]
pub fn has_constructing_diagnostic_span_admission() -> bool {
    CONSTRUCTING
        .try_with(|slot| slot.try_borrow().ok().is_some_and(|slot| slot.is_some()))
        .unwrap_or(false)
}
/// Opaque process-local tracing parent for transport executor handoff. This
/// carries neither request authority nor wire metadata and exposes no raw span.
#[derive(Clone)]
pub struct RequestSpanParent(tracing::Span);
impl RequestSpanParent {
    /// Captures only the currently admitted tracing parent.
    #[must_use]
    pub fn capture() -> Self {
        Self(tracing::Span::current())
    }
    /// Constructs child observations under the retained process-local parent.
    pub fn in_scope<T>(&self, operation: impl FnOnce() -> T) -> T {
        self.0.in_scope(operation)
    }
}
impl std::fmt::Debug for RequestSpanParent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RequestSpanParent([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // req: END-007, END-008, PERF-001
    #[test]
    fn span_permits_enforce_the_process_bound_and_release_on_cancellation() {
        let budget = SpanBudget {
            active: AtomicUsize::new(0),
        };
        let mut held = (0..MAX_ACTIVE_DIAGNOSTIC_SPANS)
            .map(|_| budget.acquire().unwrap())
            .collect::<Vec<_>>();
        assert!(budget.acquire().is_none());
        drop(held.pop());
        let next = budget.acquire().unwrap();
        drop(next);
        drop(held);
        assert_eq!(budget.active.load(Ordering::Acquire), 0);
        let start = std::sync::Barrier::new(4);
        let acquired = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let budget = &budget;
                let start = &start;
                let acquired = &acquired;
                scope.spawn(move || {
                    start.wait();
                    let held = (0..1024)
                        .filter_map(|_| budget.acquire())
                        .collect::<Vec<_>>();
                    acquired.wait();
                    let valid = held.len() == 1024 && budget.acquire().is_none();
                    acquired.wait();
                    drop(held);
                    assert!(valid);
                });
            }
        });
        assert_eq!(budget.active.load(Ordering::Acquire), 0);
    }
}
