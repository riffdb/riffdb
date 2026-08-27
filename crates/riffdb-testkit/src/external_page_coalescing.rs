//! Framework-neutral acceptance model for ADR-0159's external page translation.

use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Cursor(usize);

#[derive(Clone, Debug, Eq, PartialEq)]
struct Page {
    rows: Vec<u32>,
    continuation: Option<Cursor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    Cancelled,
    Cursor,
    Authorization,
    Provider,
    Transport,
}

#[derive(Debug)]
struct Provider {
    rows: Vec<u32>,
    calls: usize,
    fail_at: Option<(usize, Failure)>,
}

impl Provider {
    fn page(&mut self, limit: NonZeroUsize, continuation: Option<Cursor>) -> Result<Page, Failure> {
        self.calls = self.calls.checked_add(1).expect("bounded test calls");
        if let Some((call, failure)) = self.fail_at
            && call == self.calls
        {
            return Err(failure);
        }

        let start = continuation.map_or(0, |cursor| cursor.0);
        let end = start
            .checked_add(limit.get())
            .expect("bounded requested page")
            .min(self.rows.len());
        let continuation = (end < self.rows.len()).then_some(Cursor(end));
        Ok(Page {
            rows: self.rows[start..end].to_vec(),
            continuation,
        })
    }
}

fn exact_external_page(
    provider: &mut Provider,
    requested: NonZeroUsize,
    compiled_maximum: NonZeroUsize,
    initial: Option<Cursor>,
    cancelled_before_call: Option<usize>,
) -> Result<Page, Failure> {
    let mut rows = Vec::new();
    let mut continuation = initial;

    while rows.len() < requested.get() {
        let next_call = provider
            .calls
            .checked_add(1)
            .expect("bounded provider calls");
        if cancelled_before_call == Some(next_call) {
            return Err(Failure::Cancelled);
        }
        let remaining = requested
            .get()
            .checked_sub(rows.len())
            .expect("rows never exceed request");
        let limit = NonZeroUsize::new(remaining.min(compiled_maximum.get()))
            .expect("positive remaining page");
        let page = provider.page(limit, continuation)?;
        rows.try_reserve(page.rows.len())
            .expect("bounded returned rows");
        rows.extend(page.rows);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }

    Ok(Page { rows, continuation })
}

#[test]
fn exact_external_page_handles_smaller_equal_larger_and_huge_finite_requests() {
    for (requested, expected_rows, expected_calls, expected_cursor) in [
        (1, vec![1], 1, Some(Cursor(1))),
        (2, vec![1, 2], 1, Some(Cursor(2))),
        (5, vec![1, 2, 3, 4, 5], 3, Some(Cursor(5))),
        (25_000_000, vec![1, 2, 3, 4, 5, 6], 3, None),
    ] {
        let mut provider = Provider {
            rows: vec![1, 2, 3, 4, 5, 6],
            calls: 0,
            fail_at: None,
        };
        let result = exact_external_page(
            &mut provider,
            NonZeroUsize::new(requested).expect("positive request"),
            NonZeroUsize::new(2).expect("positive maximum"),
            None,
            None,
        )
        .expect("exact external page");
        assert_eq!(result.rows, expected_rows);
        assert_eq!(result.continuation, expected_cursor);
        assert_eq!(provider.calls, expected_calls);
        assert!(result.rows.capacity() < 25_000_000);
    }
}

#[test]
fn exact_external_page_returns_final_cursor_unchanged_and_resumes_without_loss() {
    let mut provider = Provider {
        rows: vec![1, 2, 3, 4, 5, 6],
        calls: 0,
        fail_at: None,
    };
    let first = exact_external_page(
        &mut provider,
        NonZeroUsize::new(5).expect("positive request"),
        NonZeroUsize::new(2).expect("positive maximum"),
        None,
        None,
    )
    .expect("first external page");
    assert_eq!(first.rows, [1, 2, 3, 4, 5]);
    assert_eq!(first.continuation, Some(Cursor(5)));

    let second = exact_external_page(
        &mut provider,
        NonZeroUsize::new(5).expect("positive request"),
        NonZeroUsize::new(2).expect("positive maximum"),
        first.continuation,
        None,
    )
    .expect("resumed external page");
    assert_eq!(second.rows, [6]);
    assert_eq!(second.continuation, None);
}

#[test]
fn cancellation_and_every_provider_failure_release_no_partial_success() {
    for failure in [
        Failure::Cursor,
        Failure::Authorization,
        Failure::Provider,
        Failure::Transport,
    ] {
        for boundary in 1..=3 {
            let mut provider = Provider {
                rows: vec![1, 2, 3, 4, 5, 6],
                calls: 0,
                fail_at: Some((boundary, failure)),
            };
            assert_eq!(
                exact_external_page(
                    &mut provider,
                    NonZeroUsize::new(5).expect("positive request"),
                    NonZeroUsize::new(2).expect("positive maximum"),
                    None,
                    None,
                ),
                Err(failure)
            );
            assert_eq!(provider.calls, boundary);
        }
    }

    for boundary in 1..=3 {
        let mut provider = Provider {
            rows: vec![1, 2, 3, 4, 5, 6],
            calls: 0,
            fail_at: None,
        };
        assert_eq!(
            exact_external_page(
                &mut provider,
                NonZeroUsize::new(5).expect("positive request"),
                NonZeroUsize::new(2).expect("positive maximum"),
                None,
                Some(boundary),
            ),
            Err(Failure::Cancelled)
        );
        assert_eq!(provider.calls, boundary - 1);
    }
}
