//! Candidate evaluation and visible-prefix selection share one bounded walk.

use std::num::NonZeroU16;

pub(super) fn collect_admitted_prefix<C, T, E>(
    candidates: &[C],
    admitted_limit: Option<NonZeroU16>,
    mut evaluate: impl FnMut(&C) -> Result<(T, bool), E>,
) -> Result<Vec<T>, E> {
    let mut observations = Vec::with_capacity(candidates.len());
    let mut admitted = 0usize;
    for candidate in candidates {
        let (observation, allowed) = evaluate(candidate)?;
        observations.push(observation);
        admitted += usize::from(allowed);
        if admitted_limit.is_some_and(|limit| admitted == usize::from(limit.get())) {
            break;
        }
    }
    Ok(observations)
}

#[cfg(test)]
mod tests {
    use super::*;

    // req: END-003, END-007
    #[test]
    fn a_full_visible_page_never_evaluates_the_unused_candidate_suffix() {
        let mut evaluated = Vec::new();
        let result = collect_admitted_prefix(
            &(0..1024).collect::<Vec<_>>(),
            NonZeroU16::new(32),
            |candidate| {
                evaluated.push(*candidate);
                Ok::<_, ()>((*candidate, true))
            },
        )
        .unwrap();
        assert_eq!(result, (0..32).collect::<Vec<_>>());
        assert_eq!(
            evaluated, result,
            "authorization must stop at the same admitted prefix retained by release"
        );
    }

    // req: END-003, END-007
    #[test]
    fn hidden_candidates_consume_work_and_remain_in_the_selected_fence() {
        let mut evaluated = Vec::new();
        let result = collect_admitted_prefix(
            &(0..100).collect::<Vec<_>>(),
            NonZeroU16::new(3),
            |candidate| {
                evaluated.push(*candidate);
                Ok::<_, ()>((*candidate, *candidate >= 4 && candidate % 2 == 0))
            },
        )
        .unwrap();
        assert_eq!(result, (0..9).collect::<Vec<_>>());
        assert_eq!(evaluated, result);
    }

    // req: END-003, END-007
    #[test]
    fn insufficient_visibility_retains_the_entire_bounded_candidate_prefix() {
        let mut evaluated = Vec::new();
        let candidates = (0..1024).collect::<Vec<_>>();
        let result = collect_admitted_prefix(&candidates, NonZeroU16::new(32), |candidate| {
            evaluated.push(*candidate);
            Ok::<_, ()>((*candidate, *candidate == 0))
        })
        .unwrap();
        assert_eq!(result, candidates);
        assert_eq!(evaluated, candidates);
    }

    // req: END-003, END-007
    #[test]
    fn an_error_in_the_required_prefix_refuses_and_evaluates_no_later_candidate() {
        let mut evaluated = Vec::new();
        let result = collect_admitted_prefix(&[0, 1, 2, 3], NonZeroU16::new(3), |candidate| {
            evaluated.push(*candidate);
            if *candidate == 1 {
                Err("required observation unavailable")
            } else {
                Ok((*candidate, true))
            }
        });
        assert_eq!(result, Err("required observation unavailable"));
        assert_eq!(evaluated, [0, 1]);
    }

    // req: END-003, END-007
    #[test]
    fn all_selected_lease_candidates_are_evaluated_when_no_visible_limit_applies() {
        let candidates = [0, 1, 2, 3];
        let mut evaluated = Vec::new();
        let result = collect_admitted_prefix(&candidates, None, |candidate| {
            evaluated.push(*candidate);
            Ok::<_, ()>((*candidate, candidate % 2 == 0))
        })
        .unwrap();
        assert_eq!(result, candidates);
        assert_eq!(evaluated, candidates);
    }

    // req: END-003, END-007
    #[test]
    fn a_completed_prefix_opens_no_observation_from_the_unused_suffix() {
        let result = collect_admitted_prefix(
            &(0..1024).collect::<Vec<_>>(),
            NonZeroU16::new(32),
            |candidate| {
                if *candidate >= 32 {
                    Err("unused observation opened")
                } else {
                    Ok((*candidate, true))
                }
            },
        );
        assert_eq!(result, Ok((0..32).collect::<Vec<_>>()));
    }

    // req: END-003, END-007
    #[test]
    fn an_empty_candidate_window_evaluates_nothing() {
        let result = collect_admitted_prefix::<u64, u64, ()>(&[], NonZeroU16::new(32), |_| {
            panic!("empty window evaluated")
        });
        assert_eq!(result, Ok(Vec::new()));
    }
}
