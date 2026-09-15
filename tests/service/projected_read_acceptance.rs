#![forbid(unsafe_code)]

//! CP2b acceptance spine (ADR-0086 §2/§3/§5/§7).
//!
//! B8 — the live cross-path equivalence gate: the compiled board_page-shaped
//! symbolic query (real executor over live redb storage) and the projected
//! columnar query (real engine applied by a real worker thread from the same
//! storage) must return BYTE-IDENTICAL row content at the same frontier.
//!
//! B9 — the freshness matrix: Causal satisfied-immediately / waits-then-serves
//! under real apply advance / times out typed; Bounded within and beyond;
//! Available always serves and reports its frontier; stale-incarnation tokens
//! never serve; revocation mid-wait (REAL `AuthorizationFixture::revoke_current`)
//! denies at the post-wake reauthorize safe point.
//!
//! Falsifiability anchors (verified as temporary code edits, see the round
//! report): (a) deleting the post-wake reauthorize from the causal loop makes
//! `revocation_mid_wait_denies_at_the_post_wake_safe_point` fail its prompt-
//! denial bound; (e) passing an empty select when fields were requested makes
//! the B8 gate fail on the served-select assertion and on cell order.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_columnar::QueryBudget;
use riffdb_errors::PublicErrorKind;
use riffdb_service::{
    CommandApplication, ExecuteCommandRequest, ExecuteCommandResult, ExecuteProjectedQueryRequest,
    ExecuteProjectedQueryResult, ExecuteSymbolicQueryResult, ProjectedAggregateOp,
    ProjectedAggregateValue as AggregateValue, ProjectedColumnPredicate, ProjectedGroupBySpec,
    ProjectedQueryApplication, ProjectedQueryBody, QueryResult, SymbolicContractSelector,
    SymbolicQueryApplication, SymbolicResultField,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, CommitToken, FreshnessPolicy, FrontierPosition,
    ProjectionFrontier, encode_canonical_value,
};

use support::{
    BOARD_HISTORY_INCARNATION, BOARD_PROJECTION_NAME, BOARD_SELECT, ServiceHarness,
    run_async_threads,
};

/// Comparison field order for one board row (ticket_id via primary-key return).
const BOARD_ROW_FIELDS: [&str; 6] = [
    "ticket_id",
    "project_id",
    "title",
    "status",
    "reporter_id",
    "assignee_id",
];

fn uuid(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

fn encode(value: &CanonicalValue) -> Vec<u8> {
    encode_canonical_value(value).expect("canonical encoding")
}

/// Executes one real journaled command and returns its commit sequence.
async fn journaled(
    harness: &ServiceHarness,
    seed: u8,
    request: ExecuteCommandRequest,
) -> CommitSequence {
    let (context, _cancellation) = harness.context(seed);
    let result = harness
        .service
        .execute_command(context, request)
        .await
        .expect("board command commits through the real coordinator");
    let ExecuteCommandResult::Journaled(journaled) = result else {
        panic!("board commands are journaled mutations");
    };
    journaled.commit_sequence()
}

/// Extracts byte-encoded board rows `[ticket_id, project_id, title, status,
/// reporter_id, assignee_id]` from a projected `Ready` outcome.
///
/// Deliberately positional over the served select (never name-lookup), so a
/// service that widens or reorders the selection diverges byte-for-byte.
fn projected_board_rows(result: &ExecuteProjectedQueryResult) -> Vec<Vec<Vec<u8>>> {
    let ExecuteProjectedQueryResult::Ready {
        fields,
        primary_key_fields,
        rows,
        result: non_row,
        ..
    } = result
    else {
        panic!("projected board query must be Ready, got {result:?}");
    };
    assert!(non_row.is_none(), "board queries return rows");
    let expected_select: Vec<String> = BOARD_SELECT.iter().map(|name| (*name).to_owned()).collect();
    assert_eq!(
        fields, &expected_select,
        "served select must be exactly the requested select"
    );
    assert_eq!(
        primary_key_fields,
        &["organization_id".to_owned(), "ticket_id".to_owned()],
        "board primary-key return exposes (organization_id, ticket_id)"
    );
    rows.iter()
        .map(|row| {
            assert_eq!(
                row.cells.len(),
                BOARD_SELECT.len(),
                "row must carry exactly the selected cells"
            );
            let mut encoded = Vec::with_capacity(BOARD_ROW_FIELDS.len());
            // ticket_id from the primary-key return.
            encoded.push(encode(&row.primary_key[1]));
            // Select order is project_id, title, status, reporter_id, assignee_id
            // which continues the comparison order exactly.
            encoded.extend(row.cells.iter().map(encode));
            encoded
        })
        .collect()
}

/// Extracts byte-encoded board rows in the same field order from the compiled
/// symbolic board_page result.
fn symbolic_board_rows(result: &ExecuteSymbolicQueryResult) -> Vec<Vec<Vec<u8>>> {
    assert_eq!(result.outcome(), "Found", "board page resolves Found");
    let Some(SymbolicResultField::Many(records)) = result.fields().get("tickets") else {
        panic!("board page returns many tickets");
    };
    records
        .iter()
        .map(|record| {
            assert_eq!(record.entity(), "Ticket");
            BOARD_ROW_FIELDS
                .iter()
                .map(|name| {
                    let value = record
                        .fields()
                        .get(*name)
                        .unwrap_or_else(|| panic!("missing board field {name}"));
                    encode(value)
                })
                .collect()
        })
        .collect()
}

fn causal(token: CommitToken, max_wait: Duration) -> FreshnessPolicy {
    FreshnessPolicy::Causal { token, max_wait }
}

fn frontier_at(sequence: CommitSequence) -> ProjectionFrontier {
    ProjectionFrontier::new_scoped(
        support::database_id(),
        BOARD_HISTORY_INCARNATION,
        FrontierPosition::AppliedThrough(sequence),
    )
}

/// B8 — the live cross-path equivalence gate.
///
/// Real commands create tickets across two orgs with interleaved irrelevant
/// Note entities; the real columnar worker applies them from the same live
/// storage; both read paths execute for three (org, project, status) shapes
/// and every returned row must be byte-identical at the same frontier.
/// Run-aborting on divergence; never comparing a path to itself (the symbolic
/// path scans authoritative index/entity storage, the projected path scans
/// the columnar engine's published snapshot).
#[test]
fn board_projected_rows_are_byte_identical_to_the_compiled_board_page() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org_a = uuid(0x41);
        let org_b = uuid(0x42);
        let project_one = uuid(0x51);
        let project_two = uuid(0x54);
        let reporter = uuid(0x52);
        let assignee = uuid(0x53);

        let mut seed: u8 = 0x60;
        let mut next_seed = || {
            seed = seed.checked_add(1).expect("bounded request seed");
            seed
        };
        let mut sequences = Vec::new();

        // Org A / project one / Open: the primary board (deliberately
        // non-monotonic ticket-id creation order so `order by ticket_id asc`
        // is exercised for real).
        for ticket_seed in [0x8fu8, 0x81, 0x8b, 0x83, 0x8d, 0x85, 0x89, 0x87] {
            let request = harness.create_ticket_request(
                &format!("b8-a-open-{ticket_seed:02x}"),
                org_a,
                uuid(ticket_seed),
                project_one,
                reporter,
                assignee,
                "Open",
                &format!("a-open-{ticket_seed:02x}"),
            );
            sequences.push(journaled(&harness, next_seed(), request).await);
            // Interleave an irrelevant Note entity in the same org.
            let note = harness.create_note_request(
                &format!("b8-note-{ticket_seed:02x}"),
                org_a,
                uuid(ticket_seed.wrapping_add(0x40)),
                "irrelevant",
            );
            sequences.push(journaled(&harness, next_seed(), note).await);
        }
        // Org A / project one / Closed: excluded by the status predicate.
        for ticket_seed in [0x92u8, 0x94, 0x96] {
            let request = harness.create_ticket_request(
                &format!("b8-a-closed-{ticket_seed:02x}"),
                org_a,
                uuid(ticket_seed),
                project_one,
                reporter,
                assignee,
                "Closed",
                &format!("a-closed-{ticket_seed:02x}"),
            );
            sequences.push(journaled(&harness, next_seed(), request).await);
        }
        // Org A / project two / Open: excluded by the project predicate.
        for ticket_seed in [0x98u8, 0x9a] {
            let request = harness.create_ticket_request(
                &format!("b8-a-p2-{ticket_seed:02x}"),
                org_a,
                uuid(ticket_seed),
                project_two,
                reporter,
                assignee,
                "Open",
                &format!("a-p2-{ticket_seed:02x}"),
            );
            sequences.push(journaled(&harness, next_seed(), request).await);
        }
        // Org B / project one / Open: the cross-tenant board.
        for ticket_seed in [0xa6u8, 0xa2, 0xa4, 0xa8] {
            let request = harness.create_ticket_request(
                &format!("b8-b-open-{ticket_seed:02x}"),
                org_b,
                uuid(ticket_seed),
                project_one,
                reporter,
                assignee,
                "Open",
                &format!("b-open-{ticket_seed:02x}"),
            );
            sequences.push(journaled(&harness, next_seed(), request).await);
            let note = harness.create_note_request(
                &format!("b8-b-note-{ticket_seed:02x}"),
                org_b,
                uuid(ticket_seed.wrapping_add(0x30)),
                "irrelevant",
            );
            sequences.push(journaled(&harness, next_seed(), note).await);
        }

        let last_sequence = *sequences.last().expect("at least one committed write");
        let token = CommitToken::new_scoped(
            support::database_id(),
            BOARD_HISTORY_INCARNATION,
            last_sequence,
        );

        // Both paths for each shape, at the same frontier (Causal to the last
        // write; no further writes happen during comparison).
        let shapes: [([u8; 16], [u8; 16], &str, usize); 3] = [
            (org_a, project_one, "Open", 8),
            (org_a, project_one, "Closed", 3),
            (org_b, project_one, "Open", 4),
        ];
        let mut boards = Vec::new();
        for (organization_id, project_id, status, expected_rows) in shapes {
            let (context, _cancellation) = harness.context(next_seed());
            let projected = harness
                .service
                .execute_projected_query(
                    context,
                    harness.board_projected_request(
                        organization_id,
                        project_id,
                        status,
                        causal(token.clone(), Duration::from_secs(20)),
                    ),
                )
                .await
                .expect("projected board query serves");
            let ExecuteProjectedQueryResult::Ready { frontier, .. } = &projected else {
                panic!("projected board query must be Ready, got {projected:?}");
            };
            assert!(
                frontier.satisfies(&token),
                "served frontier must cover the causal token"
            );

            let (context, _cancellation) = harness.context(next_seed());
            let symbolic = harness
                .service
                .execute_symbolic_query(
                    context,
                    harness.board_symbolic_request(organization_id, project_id, status),
                )
                .await
                .expect("compiled board page executes over live storage");
            assert!(
                symbolic.application_head() >= last_sequence.get(),
                "symbolic snapshot must be at the same frontier as the writes"
            );

            let projected_rows = projected_board_rows(&projected);
            let symbolic_rows = symbolic_board_rows(&symbolic);
            assert_eq!(
                projected_rows.len(),
                expected_rows,
                "the equivalence gate must compare a non-trivial board"
            );
            assert_eq!(
                projected_rows, symbolic_rows,
                "CROSS-PATH DIVERGENCE: projected columnar rows differ from the \
                 compiled board_page rows for org {organization_id:02x?} status {status}"
            );
            boards.push(projected_rows);
        }
        // Tenant isolation is visible in the gate itself: the org A and org B
        // boards must not share a single row.
        let org_a_board = &boards[0];
        let org_b_board = &boards[2];
        assert!(
            org_a_board.iter().all(|row| !org_b_board.contains(row)),
            "cross-tenant rows leaked between org boards"
        );

        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}

/// B9 — Causal satisfied immediately: with the projection already at the
/// token, the query serves with a zero wait budget (no parking required).
#[test]
fn causal_token_satisfied_immediately_serves_without_waiting() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let request = harness.create_ticket_request(
            "causal-now",
            org,
            uuid(0x81),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "causal-now",
        );
        let sequence = journaled(&harness, 0x21, request).await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let token =
            CommitToken::new_scoped(support::database_id(), BOARD_HISTORY_INCARNATION, sequence);
        let (context, _cancellation) = harness.context(0x22);
        let result = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(
                    org,
                    project,
                    "Open",
                    causal(token.clone(), Duration::ZERO),
                ),
            )
            .await
            .expect("satisfied causal read serves");
        let rows = projected_board_rows(&result);
        assert_eq!(rows.len(), 1, "the committed ticket is served");
        let ExecuteProjectedQueryResult::Ready { frontier, .. } = &result else {
            unreachable!("checked Ready above");
        };
        assert!(frontier.satisfies(&token));
        harness.stop_coordinator();
    });
}

/// B9 — Causal waits then serves: the wait parks while apply is paused, a
/// command committed from another task advances the head, real apply resumes
/// mid-wait, the notifier wakes the wait, and the query serves at/after the
/// token.
#[test]
fn causal_wait_parks_until_real_apply_advances_then_serves_at_the_token() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);

        // Publish once (real apply of a real commit), then park the
        // projection strictly behind the next write.
        let seeded = harness.create_note_request("causal-wait-seed", org, uuid(0xc1), "seed");
        let seeded_sequence = journaled(&harness, 0x30, seeded).await;
        harness
            .board_columnar()
            .wait_until_applied(seeded_sequence, Duration::from_secs(10));
        harness.board_columnar().set_apply_enabled(false);
        let request = harness.create_ticket_request(
            "causal-wait",
            org,
            uuid(0x83),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "causal-wait",
        );
        let sequence = journaled(&harness, 0x31, request).await;
        assert!(
            harness.board_columnar().published_position()
                < FrontierPosition::AppliedThrough(sequence),
            "paused apply must be strictly behind the write"
        );

        let token =
            CommitToken::new_scoped(support::database_id(), BOARD_HISTORY_INCARNATION, sequence);
        let service = harness.service.clone();
        let projected_request = harness.board_projected_request(
            org,
            project,
            "Open",
            causal(token.clone(), Duration::from_secs(20)),
        );
        let (context, _cancellation) = harness.context(0x32);
        let parked = tokio::spawn(async move {
            service
                .execute_projected_query(context, projected_request)
                .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !parked.is_finished(),
            "the causal wait must park while apply is paused"
        );

        // Drive a real command from another task while the wait is parked,
        // then resume real apply.
        let note = harness.create_note_request("causal-wait-note", org, uuid(0xc3), "mid-wait");
        let note_sequence = journaled(&harness, 0x33, note).await;
        assert!(note_sequence > sequence);
        let columnar = Arc::clone(harness.board_columnar());
        tokio::spawn(async move {
            columnar.set_apply_enabled(true);
        })
        .await
        .expect("resume task");

        let result = tokio::time::timeout(Duration::from_secs(15), parked)
            .await
            .expect("the wait must wake once real apply advances")
            .expect("join")
            .expect("woken causal read serves");
        let rows = projected_board_rows(&result);
        assert_eq!(rows.len(), 1, "the awaited ticket is served");
        let ExecuteProjectedQueryResult::Ready { frontier, .. } = &result else {
            unreachable!("checked Ready above");
        };
        assert!(
            frontier.satisfies(&token),
            "the woken read serves at/after the causal token"
        );
        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}

/// B9 — Causal times out: with apply parked below the token the wait exhausts
/// its budget and returns typed `Lagging` carrying required/current/head/lag;
/// `retry_after` is `None` because the wait budget is fully exhausted.
#[test]
fn causal_wait_times_out_with_typed_lagging() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        // Publish once so the wait parks as a lag, not as Building.
        let seeded = harness.create_note_request("causal-timeout-seed", org, uuid(0xc2), "seed");
        let seeded_sequence = journaled(&harness, 0x40, seeded).await;
        harness
            .board_columnar()
            .wait_until_applied(seeded_sequence, Duration::from_secs(10));
        harness.board_columnar().set_apply_enabled(false);
        let request = harness.create_ticket_request(
            "causal-timeout",
            org,
            uuid(0x85),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "causal-timeout",
        );
        let sequence = journaled(&harness, 0x41, request).await;

        let token =
            CommitToken::new_scoped(support::database_id(), BOARD_HISTORY_INCARNATION, sequence);
        let (context, _cancellation) = harness.context(0x42);
        let result = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(
                    org,
                    project,
                    "Open",
                    causal(token, Duration::from_millis(300)),
                ),
            )
            .await
            .expect("timed-out causal read returns a typed outcome");
        let ExecuteProjectedQueryResult::Lagging {
            required,
            current,
            head,
            lag_sequences,
            retry_after,
        } = &result
        else {
            panic!("timed-out causal read must be Lagging, got {result:?}");
        };
        assert_eq!(
            required,
            &frontier_at(sequence),
            "required = the token fence"
        );
        assert!(
            current.position() < FrontierPosition::AppliedThrough(sequence),
            "current stays strictly behind the required fence"
        );
        assert!(
            head.position() >= FrontierPosition::AppliedThrough(sequence),
            "head covers the durably committed write"
        );
        assert!(
            lag_sequences.is_some_and(|lag| lag >= 1),
            "sequence-distance lag is populated"
        );
        assert!(
            retry_after.is_none(),
            "an exhausted wait budget reports no residual retry hint"
        );
        harness.board_columnar().set_apply_enabled(true);
        harness.stop_coordinator();
    });
}

/// B9 — Bounded within and beyond `max_lag_sequences`, deterministically:
/// apply is paused at a known frontier, three further commits build the lag.
#[test]
fn bounded_freshness_serves_within_and_reports_lagging_beyond_max_lag() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let request = harness.create_ticket_request(
            "bounded",
            org,
            uuid(0x87),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "bounded",
        );
        let applied_sequence = journaled(&harness, 0x51, request).await;
        harness
            .board_columnar()
            .wait_until_applied(applied_sequence, Duration::from_secs(10));
        harness.board_columnar().set_apply_enabled(false);

        let mut head_sequence = applied_sequence;
        for (ordinal, seed) in [0xc5u8, 0xc7, 0xc9].into_iter().enumerate() {
            let note = harness.create_note_request(
                &format!("bounded-note-{ordinal}"),
                org,
                uuid(seed),
                "lag",
            );
            head_sequence = journaled(&harness, 0x52 + ordinal as u8, note).await;
        }
        let lag = head_sequence.get() - applied_sequence.get();
        assert_eq!(lag, 3, "three commits build the deterministic lag");

        // Beyond: max_lag below the actual lag must report typed Lagging with
        // the head as the required fence.
        let (context, _cancellation) = harness.context(0x59);
        let beyond = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(
                    org,
                    project,
                    "Open",
                    FreshnessPolicy::Bounded {
                        max_lag_sequences: lag - 1,
                    },
                ),
            )
            .await
            .expect("beyond-bound read returns a typed outcome");
        let ExecuteProjectedQueryResult::Lagging {
            required,
            current,
            lag_sequences,
            ..
        } = &beyond
        else {
            panic!("beyond-bound read must be Lagging, got {beyond:?}");
        };
        assert_eq!(required, &frontier_at(head_sequence), "required = head");
        assert_eq!(current, &frontier_at(applied_sequence));
        assert_eq!(*lag_sequences, Some(lag));

        // Within: max_lag at the actual lag serves at the stale frontier.
        let (context, _cancellation) = harness.context(0x5a);
        let within = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(
                    org,
                    project,
                    "Open",
                    FreshnessPolicy::Bounded {
                        max_lag_sequences: lag,
                    },
                ),
            )
            .await
            .expect("within-bound read serves");
        let rows = projected_board_rows(&within);
        assert_eq!(rows.len(), 1);
        let ExecuteProjectedQueryResult::Ready { frontier, head, .. } = &within else {
            unreachable!("checked Ready above");
        };
        assert_eq!(
            frontier,
            &frontier_at(applied_sequence),
            "served frontier is reported"
        );
        assert_eq!(
            head,
            &frontier_at(head_sequence),
            "head is reported alongside"
        );

        harness.board_columnar().set_apply_enabled(true);
        harness.stop_coordinator();
    });
}

/// B9 — Available always serves and reports its (possibly stale) frontier:
/// with apply paused, a newer ticket is invisible and the reported frontier
/// stays at the applied sequence; after real apply catches up it appears.
#[test]
fn available_always_serves_and_reports_the_stale_frontier() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let first = harness.create_ticket_request(
            "available-a",
            org,
            uuid(0x89),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "available-a",
        );
        let applied_sequence = journaled(&harness, 0x61, first).await;
        harness
            .board_columnar()
            .wait_until_applied(applied_sequence, Duration::from_secs(10));
        harness.board_columnar().set_apply_enabled(false);
        let second = harness.create_ticket_request(
            "available-b",
            org,
            uuid(0x8b),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "available-b",
        );
        let head_sequence = journaled(&harness, 0x62, second).await;

        let (context, _cancellation) = harness.context(0x63);
        let stale = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(org, project, "Open", FreshnessPolicy::Available),
            )
            .await
            .expect("Available always serves");
        let rows = projected_board_rows(&stale);
        assert_eq!(rows.len(), 1, "the unapplied ticket must not be visible");
        let ExecuteProjectedQueryResult::Ready { frontier, head, .. } = &stale else {
            unreachable!("checked Ready above");
        };
        assert_eq!(
            frontier,
            &frontier_at(applied_sequence),
            "stale frontier reported"
        );
        assert_eq!(head, &frontier_at(head_sequence), "true head reported");

        harness.board_columnar().set_apply_enabled(true);
        harness
            .board_columnar()
            .wait_until_applied(head_sequence, Duration::from_secs(10));
        let (context, _cancellation) = harness.context(0x64);
        let fresh = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(org, project, "Open", FreshnessPolicy::Available),
            )
            .await
            .expect("Available serves after catch-up");
        assert_eq!(
            projected_board_rows(&fresh).len(),
            2,
            "both tickets visible"
        );
        harness.stop_coordinator();
    });
}

/// B9 — a token from another history incarnation (a restore fence) is never
/// served: the read returns typed Lagging carrying the foreign incarnation in
/// its required fence even though the projection is fully caught up.
#[test]
fn stale_incarnation_token_is_never_served() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let request = harness.create_ticket_request(
            "stale-incarnation",
            org,
            uuid(0x8d),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "stale-incarnation",
        );
        let sequence = journaled(&harness, 0x71, request).await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let foreign = CommitToken::new_scoped(
            support::database_id(),
            BOARD_HISTORY_INCARNATION + 1,
            sequence,
        );
        let (context, _cancellation) = harness.context(0x72);
        let result = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(
                    org,
                    project,
                    "Open",
                    causal(foreign, Duration::from_millis(300)),
                ),
            )
            .await
            .expect("stale-incarnation read returns a typed outcome");
        let ExecuteProjectedQueryResult::Lagging {
            required, current, ..
        } = &result
        else {
            panic!("stale-incarnation token must never serve, got {result:?}");
        };
        assert_eq!(
            required.history_incarnation(),
            BOARD_HISTORY_INCARNATION + 1,
            "the required fence carries the foreign incarnation"
        );
        assert_eq!(
            current.history_incarnation(),
            BOARD_HISTORY_INCARNATION,
            "the current frontier stays incarnation-bound"
        );
        harness.stop_coordinator();
    });
}

/// B9 — revocation mid-wait takes effect at the post-wake safe point.
///
/// A causal wait parks on a token BEYOND anything apply will reach; the
/// capability is revoked FOR REAL (`AuthorizationFixture::revoke_current`,
/// the same active-to-revoked record transition production resolves); real
/// apply then advances (waking the parked registration WITHOUT satisfying the
/// token); the mandatory post-wake reauthorize must deny promptly — well
/// before the 90-second wait budget — and no rows are served.
///
/// Falsifiability (transcript (a)): delete the post-wake
/// `begun.reauthorize_read` from the causal loop and this test fails — the
/// revoked principal keeps camping on the wait until the budget expires,
/// breaking the prompt-denial bound below.
#[test]
fn revocation_mid_wait_denies_at_the_post_wake_safe_point() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let request = harness.create_ticket_request(
            "revoke-mid-wait",
            org,
            uuid(0x8f),
            project,
            uuid(0x52),
            uuid(0x53),
            "Open",
            "revoke-mid-wait",
        );
        let sequence = journaled(&harness, 0x81, request).await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));
        harness.board_columnar().set_apply_enabled(false);
        // Head will advance past `sequence` but never reach the fence below.
        let note = harness.create_note_request("revoke-mid-wait-note", org, uuid(0xd1), "wake");
        let note_sequence = journaled(&harness, 0x82, note).await;
        let unreachable = CommitToken::new_scoped(
            support::database_id(),
            BOARD_HISTORY_INCARNATION,
            CommitSequence::new(note_sequence.get() + 8).expect("nonzero fence"),
        );

        let service = harness.service.clone();
        let projected_request = harness.board_projected_request(
            org,
            project,
            "Open",
            causal(unreachable, Duration::from_secs(90)),
        );
        let (context, _cancellation) =
            harness.context_with_deadline(0x83, Instant::now() + Duration::from_secs(120));
        let parked = tokio::spawn(async move {
            service
                .execute_projected_query(context, projected_request)
                .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!parked.is_finished(), "the causal wait must be parked");

        // REAL revocation while the wait is parked.
        harness.revoke_policy();
        // Real apply advance wakes the registration without satisfying the token.
        harness.board_columnar().set_apply_enabled(true);

        let denial_deadline = Duration::from_secs(10);
        let joined = tokio::time::timeout(denial_deadline, parked).await.expect(
            "the post-wake safe point must deny promptly at the wake, \
             not at the 90s wait deadline",
        );
        let failure = joined
            .expect("join")
            .expect_err("a capability revoked mid-wait must never be served rows");
        assert_eq!(
            failure.public_error().map(|error| error.kind()),
            Some(PublicErrorKind::AuthorizationDenied),
            "post-wake reauthorize must deny, got {failure:?}"
        );
        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}

/// The composed board harness itself keeps the two paths honest: symbolic and
/// projected reads answer from different machinery, so an empty write set
/// yields an empty board on both (guards against accidentally wiring one path
/// into the other in the harness).
#[test]
fn empty_board_is_empty_on_both_paths() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        // One commit so the head is sequenced and the projection publishes.
        let note = harness.create_note_request("empty-board-note", org, uuid(0xd5), "seed");
        let sequence = journaled(&harness, 0x91, note).await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let (context, _cancellation) = harness.context(0x92);
        let projected = harness
            .service
            .execute_projected_query(
                context,
                harness.board_projected_request(org, project, "Open", FreshnessPolicy::Available),
            )
            .await
            .expect("empty board serves");
        assert!(projected_board_rows(&projected).is_empty());

        let (context, _cancellation) = harness.context(0x93);
        let symbolic = harness
            .service
            .execute_symbolic_query(
                context,
                harness.board_symbolic_request(org, project, "Open"),
            )
            .await
            .expect("empty compiled board executes");
        assert!(symbolic_board_rows(&symbolic).is_empty());
        harness.stop_coordinator();
    });
}

// ---------------------------------------------------------------------------
// Aggregate / group-by carriage acceptance.
//
// The spine is DIFFERENTIAL TRUTH: every aggregate the engine folds must equal
// the same value computed client-side from the row results of the same query,
// over the same real storage, real columnar apply, and real authorization.
// A fold that quietly disagreed with the rows it summarized would be invisible
// to any test that only checked the fold against a hand-written constant.
// ---------------------------------------------------------------------------

/// Seeded aggregate fixture: `(story_points, cost_minor_units, status, title)`.
const AGGREGATE_SEED: [(i64, i64, &str, &str); 6] = [
    (3, 500, "Open", "alpha"),
    (5, 250, "Open", "bravo"),
    (-2, 125, "Closed", "charlie"),
    (13, 900, "InProgress", "delta"),
    (0, 0, "Closed", "echo"),
    (8, 75, "Open", "foxtrot"),
];

/// The neighbour organization's fixture, deliberately unlike [`AGGREGATE_SEED`]
/// in every dimension an aggregate can report: a different row count, sums that
/// share no digits, and titles at the opposite end of the alphabet. Seeding
/// both orgs from one constant would separate "folded both orgs" from "folded
/// one", but not "folded mine" from "folded the neighbour's".
const NEIGHBOUR_SEED: [(i64, i64, &str, &str); 3] = [
    (1000, 10, "Open", "zulu"),
    (2000, 20, "Closed", "yankee"),
    (4000, 40, "Open", "xray"),
];

/// Seeds [`AGGREGATE_SEED`] into `org` and returns the last commit sequence.
async fn seed_aggregate_board(
    harness: &ServiceHarness,
    org: [u8; 16],
    project: [u8; 16],
    ticket_seed_base: u8,
    caller_prefix: &str,
) -> CommitSequence {
    seed_fixture(
        harness,
        org,
        project,
        ticket_seed_base,
        caller_prefix,
        &AGGREGATE_SEED,
    )
    .await
}

/// Seeds an explicit fixture into `org` and returns the last commit sequence.
async fn seed_fixture(
    harness: &ServiceHarness,
    org: [u8; 16],
    project: [u8; 16],
    ticket_seed_base: u8,
    caller_prefix: &str,
    fixture: &[(i64, i64, &str, &str)],
) -> CommitSequence {
    // Request seeds derive from the ticket base so two seeding passes in one
    // test never reuse a request identity.
    let reporter = uuid(0x52);
    let assignee = uuid(0x53);
    let mut sequence = None;
    for (index, (points, cost, status, title)) in fixture.iter().enumerate() {
        let request = harness.create_ticket_request_with_metrics(
            &format!("{caller_prefix}-{index}"),
            org,
            uuid(ticket_seed_base.wrapping_add(index as u8)),
            project,
            reporter,
            assignee,
            status,
            title,
            *points,
            *cost,
        );
        sequence =
            Some(journaled(harness, ticket_seed_base.wrapping_add(index as u8), request).await);
    }
    sequence.expect("aggregate fixture seeds at least one ticket")
}

/// The row-shaped twin of an aggregate request: same org, same predicates, all
/// aggregate input columns selected so the client can fold them itself.
fn row_twin_request(
    org: [u8; 16],
    predicates: Vec<ProjectedColumnPredicate>,
    select: &[&str],
) -> ExecuteProjectedQueryRequest {
    let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
        .with_select(select.iter().map(|name| (*name).to_owned()).collect())
        .with_predicates(predicates)
        // Authorization bounds every projected read by declared rows; the
        // engine ignores `limit` on the aggregate path, so the aggregate twin
        // below folds the whole matching set under the same declared bound.
        .with_limit(Some(50));
    ExecuteProjectedQueryRequest::new(
        SymbolicContractSelector::active(),
        BOARD_PROJECTION_NAME,
        body,
        FreshnessPolicy::Available,
    )
}

fn aggregate_request(
    org: [u8; 16],
    predicates: Vec<ProjectedColumnPredicate>,
    aggregate: ProjectedAggregateOp,
) -> ExecuteProjectedQueryRequest {
    let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
        .with_predicates(predicates)
        .with_limit(Some(50))
        .with_aggregate(Some(aggregate));
    ExecuteProjectedQueryRequest::new(
        SymbolicContractSelector::active(),
        BOARD_PROJECTION_NAME,
        body,
        FreshnessPolicy::Available,
    )
}

fn grouped_request(
    org: [u8; 16],
    keys: Vec<&str>,
    aggregates: Vec<ProjectedAggregateOp>,
) -> ExecuteProjectedQueryRequest {
    let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
        .with_limit(Some(50))
        .with_group_by(Some(ProjectedGroupBySpec {
            keys: keys.into_iter().map(str::to_owned).collect(),
            aggregates,
        }));
    ExecuteProjectedQueryRequest::new(
        SymbolicContractSelector::active(),
        BOARD_PROJECTION_NAME,
        body,
        FreshnessPolicy::Available,
    )
}

/// Extracts the single whole-set aggregate value from a Ready outcome, proving
/// on the way that the row metadata is empty for this shape.
fn whole_set_value(result: &ExecuteProjectedQueryResult) -> AggregateValue {
    let ExecuteProjectedQueryResult::Ready {
        fields,
        primary_key_fields,
        rows,
        result: Some(QueryResult::Aggregate(value)),
        ..
    } = result
    else {
        panic!("expected a whole-set aggregate, got {result:?}");
    };
    assert!(
        fields.is_empty() && primary_key_fields.is_empty() && rows.is_empty(),
        "a whole-set aggregate carries no row metadata"
    );
    value.clone()
}

/// One engine group: key cells then aggregate cells.
type EngineGroup = (Vec<CanonicalValue>, Vec<AggregateValue>);

/// Extracts group key names and groups, checking positional alignment.
fn grouped_values(result: &ExecuteProjectedQueryResult) -> (Vec<String>, Vec<EngineGroup>) {
    let ExecuteProjectedQueryResult::Ready {
        fields,
        primary_key_fields,
        rows,
        result: Some(QueryResult::Groups { key_fields, groups }),
        ..
    } = result
    else {
        panic!("expected grouped aggregates, got {result:?}");
    };
    assert!(
        primary_key_fields.is_empty() && rows.is_empty(),
        "a grouped result carries no row metadata"
    );
    assert_eq!(
        fields.len(),
        key_fields.len(),
        "group-key names must cover every engine key field"
    );
    for (keys, _) in groups {
        assert_eq!(keys.len(), key_fields.len(), "group key arity must match");
    }
    (fields.clone(), groups.clone())
}

/// Returns the served rows' cells for a row query, keyed by select position.
fn row_cells(result: &ExecuteProjectedQueryResult, select: &[&str]) -> Vec<Vec<CanonicalValue>> {
    let ExecuteProjectedQueryResult::Ready {
        fields,
        rows,
        result: non_row,
        ..
    } = result
    else {
        panic!("expected rows, got {result:?}");
    };
    assert!(non_row.is_none(), "a row query returns no folded result");
    let expected: Vec<String> = select.iter().map(|name| (*name).to_owned()).collect();
    assert_eq!(fields, &expected, "served select must be the requested one");
    rows.iter().map(|row| row.cells.clone()).collect()
}

fn as_i64(value: &CanonicalValue) -> i64 {
    match value {
        CanonicalValue::I64(value) => *value,
        other => panic!("expected an i64 cell, got {other:?}"),
    }
}

/// THE DIFFERENTIAL TRUTH GATE.
///
/// Every fold the engine performs is recomputed client-side from the row
/// results of the same predicate over the same snapshot, and the two must
/// agree exactly — count, sum, min, max, and a grouped fold with its keys.
/// Run-aborting on divergence; the client-side fold never consults the engine
/// result, and the engine fold never sees the rows.
#[test]
fn engine_aggregates_equal_client_side_folds_of_the_same_rows() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-truth").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let mut seed: u8 = 0xc0;
        let mut next_context = || {
            seed = seed.wrapping_add(1);
            harness.context(seed)
        };

        // ---- the row twin: one query, every aggregate input column ----
        let select = ["story_points", "status", "title"];
        let (context, _cancellation) = next_context();
        let rows = harness
            .service
            .execute_projected_query(context, row_twin_request(org, Vec::new(), &select))
            .await
            .expect("row twin serves");
        let cells = row_cells(&rows, &select);
        assert_eq!(
            cells.len(),
            AGGREGATE_SEED.len(),
            "the row twin must see the whole seeded set"
        );

        // ---- client-side folds, computed only from the rows above ----
        let expected_count = cells.len() as u64;
        let expected_sum: i128 = cells.iter().map(|row| i128::from(as_i64(&row[0]))).sum();
        let expected_min_points = cells
            .iter()
            .map(|row| as_i64(&row[0]))
            .min()
            .expect("non-empty");
        let expected_max_points = cells
            .iter()
            .map(|row| as_i64(&row[0]))
            .max()
            .expect("non-empty");
        // MIN/MAX use typed value order — for strings, lexicographic on the
        // string itself. That is deliberately NOT the length-prefixed encoded
        // byte order that governs group emission, and one result can carry
        // both: see `min_max_use_typed_order_not_group_key_byte_order`.
        let expected_min_title = cells
            .iter()
            .map(|row| row[2].clone())
            .min_by(|left, right| match (left, right) {
                (CanonicalValue::String(left), CanonicalValue::String(right)) => {
                    left.as_str().cmp(right.as_str())
                }
                other => panic!("expected string title cells, got {other:?}"),
            })
            .expect("non-empty");

        // ---- the engine folds ----
        let (context, _cancellation) = next_context();
        let count = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(org, Vec::new(), ProjectedAggregateOp::Count),
            )
            .await
            .expect("count serves");
        assert_eq!(
            whole_set_value(&count),
            AggregateValue::Count(expected_count),
            "engine count must equal the client-side row count"
        );

        let (context, _cancellation) = next_context();
        let sum = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Sum {
                        field: "story_points".to_owned(),
                    },
                ),
            )
            .await
            .expect("sum serves");
        assert_eq!(
            whole_set_value(&sum),
            AggregateValue::Sum(expected_sum),
            "engine sum must equal the client-side row sum"
        );

        let (context, _cancellation) = next_context();
        let min = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Min {
                        field: "story_points".to_owned(),
                    },
                ),
            )
            .await
            .expect("min serves");
        assert_eq!(
            whole_set_value(&min),
            AggregateValue::Scalar(Some(CanonicalValue::I64(expected_min_points))),
            "engine min must equal the client-side row minimum"
        );

        let (context, _cancellation) = next_context();
        let max = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Max {
                        field: "story_points".to_owned(),
                    },
                ),
            )
            .await
            .expect("max serves");
        assert_eq!(
            whole_set_value(&max),
            AggregateValue::Scalar(Some(CanonicalValue::I64(expected_max_points))),
            "engine max must equal the client-side row maximum"
        );

        // A string column exercises the non-numeric comparison path.
        let (context, _cancellation) = next_context();
        let min_title = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Min {
                        field: "title".to_owned(),
                    },
                ),
            )
            .await
            .expect("min title serves");
        assert_eq!(
            whole_set_value(&min_title),
            AggregateValue::Scalar(Some(expected_min_title)),
            "engine string min must equal the client-side row minimum"
        );

        // ---- grouped fold versus the client-side grouping of the same rows ----
        let (context, _cancellation) = next_context();
        let grouped = harness
            .service
            .execute_projected_query(
                context,
                grouped_request(
                    org,
                    vec!["status"],
                    vec![
                        ProjectedAggregateOp::Count,
                        ProjectedAggregateOp::Sum {
                            field: "story_points".to_owned(),
                        },
                        ProjectedAggregateOp::Max {
                            field: "story_points".to_owned(),
                        },
                    ],
                ),
            )
            .await
            .expect("grouped serves");
        let (key_names, groups) = grouped_values(&grouped);
        assert_eq!(key_names, vec!["status".to_owned()]);

        // Client-side grouping keyed by the encoded status cell.
        let mut expected_groups: BTreeMap<Vec<u8>, (CanonicalValue, u64, i128, i64)> =
            BTreeMap::new();
        for row in &cells {
            let entry =
                expected_groups
                    .entry(encode(&row[1]))
                    .or_insert((row[1].clone(), 0, 0, i64::MIN));
            entry.1 += 1;
            entry.2 += i128::from(as_i64(&row[0]));
            entry.3 = entry.3.max(as_i64(&row[0]));
        }
        assert_eq!(
            groups.len(),
            expected_groups.len(),
            "engine group count must equal the client-side group count"
        );
        for ((keys, values), (_, (expected_key, count, sum, max))) in
            groups.iter().zip(expected_groups.iter())
        {
            assert_eq!(
                encode(&keys[0]),
                encode(expected_key),
                "grouped keys must match the client-side grouping, byte for byte"
            );
            assert_eq!(values.len(), 3, "three functions, three cells");
            assert_eq!(values[0], AggregateValue::Count(*count));
            assert_eq!(values[1], AggregateValue::Sum(*sum));
            assert_eq!(
                values[2],
                AggregateValue::Scalar(Some(CanonicalValue::I64(*max)))
            );
        }

        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}

/// One aggregate result can carry two different orders, and conflating them
/// silently reports the wrong extreme. MIN/MAX compare typed values; group
/// emission compares length-prefixed encoded keys. The seeded titles separate
/// the two: lexicographic min is "alpha", encoded-byte min is the shortest.
#[test]
fn min_max_use_typed_order_not_group_key_byte_order() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-orders").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let titles: Vec<CanonicalValue> = AGGREGATE_SEED
            .iter()
            .map(|(_, _, _, title)| CanonicalValue::string(*title).expect("bounded title"))
            .collect();
        let typed_min = titles
            .iter()
            .min_by(|left, right| match (left, right) {
                (CanonicalValue::String(left), CanonicalValue::String(right)) => {
                    left.as_str().cmp(right.as_str())
                }
                other => panic!("string titles only, got {other:?}"),
            })
            .expect("non-empty")
            .clone();
        let encoded_min = titles
            .iter()
            .min_by(|left, right| encode(left).cmp(&encode(right)))
            .expect("non-empty")
            .clone();
        assert_ne!(
            typed_min, encoded_min,
            "the fixture must separate the two orders, or this test proves nothing"
        );

        let (context, _cancellation) = harness.context(0xd8);
        let min = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Min {
                        field: "title".to_owned(),
                    },
                ),
            )
            .await
            .expect("min title serves");
        assert_eq!(
            whole_set_value(&min),
            AggregateValue::Scalar(Some(typed_min)),
            "MIN must use typed value order, not encoded-key byte order"
        );

        // The same query grouped by title emits in encoded-key byte order.
        let (context, _cancellation) = harness.context(0xd9);
        let grouped = harness
            .service
            .execute_projected_query(
                context,
                grouped_request(org, vec!["title"], vec![ProjectedAggregateOp::Count]),
            )
            .await
            .expect("grouped serves");
        let (_, groups) = grouped_values(&grouped);
        assert_eq!(
            encode(&groups[0].0[0]),
            encode(&encoded_min),
            "the first emitted group is the encoded-byte minimum, not the typed one"
        );
        harness.stop_coordinator();
    });
}

/// Group emission order is the engine's encoded-key byte order, and the
/// service reports it untouched. Sorting anywhere downstream would make the
/// documented ordering contract a lie.
#[test]
fn grouped_results_arrive_in_encoded_group_key_byte_order() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-order").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let (context, _cancellation) = harness.context(0xd1);
        let grouped = harness
            .service
            .execute_projected_query(
                context,
                grouped_request(org, vec!["title"], vec![ProjectedAggregateOp::Count]),
            )
            .await
            .expect("grouped serves");
        let (_, groups) = grouped_values(&grouped);
        let observed: Vec<Vec<u8>> = groups.iter().map(|(keys, _)| encode(&keys[0])).collect();
        let mut sorted = observed.clone();
        sorted.sort();
        assert_eq!(
            observed, sorted,
            "groups must arrive in ascending encoded-key byte order"
        );
        assert_eq!(groups.len(), AGGREGATE_SEED.len(), "titles are distinct");
        harness.stop_coordinator();
    });
}

/// An aggregate is an org-scoped fold. A count that crossed the partition would
/// disclose another tenant's row cardinality with no field ever named.
#[test]
fn aggregates_never_fold_another_organizations_rows() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org_a = uuid(0x41);
        let org_b = uuid(0x42);
        let project = uuid(0x51);
        seed_fixture(&harness, org_a, project, 0x60, "agg-iso-a", &AGGREGATE_SEED).await;
        let sequence =
            seed_fixture(&harness, org_b, project, 0x80, "agg-iso-b", &NEIGHBOUR_SEED).await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let expected = |fixture: &[(i64, i64, &str, &str)]| {
            let count = fixture.len() as u64;
            let sum: i128 = fixture
                .iter()
                .map(|(points, _, _, _)| i128::from(*points))
                .sum();
            let min_title = fixture
                .iter()
                .map(|(_, _, _, title)| (*title).to_owned())
                .min()
                .expect("non-empty fixture");
            (count, sum, min_title)
        };
        let (count_a, sum_a, title_a) = expected(&AGGREGATE_SEED);
        let (count_b, sum_b, title_b) = expected(&NEIGHBOUR_SEED);
        // The fixtures must actually be distinguishable, or this test proves
        // only that both orgs were not folded together.
        assert_ne!(count_a, count_b);
        assert_ne!(sum_a, sum_b);
        assert_ne!(title_a, title_b);
        // Neither org's answer is the answer for both orgs combined.
        let combined_count = count_a + count_b;
        let combined_sum = sum_a + sum_b;

        for (org, label, count, sum, min_title) in [
            (org_a, "org_a", count_a, sum_a, title_a),
            (org_b, "org_b", count_b, sum_b, title_b),
        ] {
            let (context, _cancellation) = harness.context(0xe1);
            let observed_count = harness
                .service
                .execute_projected_query(
                    context,
                    aggregate_request(org, Vec::new(), ProjectedAggregateOp::Count),
                )
                .await
                .expect("count serves");
            assert_eq!(
                whole_set_value(&observed_count),
                AggregateValue::Count(count),
                "{label} must count exactly its own rows"
            );
            assert_ne!(
                whole_set_value(&observed_count),
                AggregateValue::Count(combined_count),
                "{label} must not count both partitions"
            );

            let (context, _cancellation) = harness.context(0xe2);
            let observed_sum = harness
                .service
                .execute_projected_query(
                    context,
                    aggregate_request(
                        org,
                        Vec::new(),
                        ProjectedAggregateOp::Sum {
                            field: "story_points".to_owned(),
                        },
                    ),
                )
                .await
                .expect("sum serves");
            assert_eq!(
                whole_set_value(&observed_sum),
                AggregateValue::Sum(sum),
                "{label} must sum only its own column values"
            );
            assert_ne!(
                whole_set_value(&observed_sum),
                AggregateValue::Sum(combined_sum),
                "{label} must not sum across partitions"
            );

            // An extreme is the sharpest probe: it names one row, so folding
            // the wrong partition changes the answer to a value that exists
            // only over there.
            let (context, _cancellation) = harness.context(0xe3);
            let observed_min = harness
                .service
                .execute_projected_query(
                    context,
                    aggregate_request(
                        org,
                        Vec::new(),
                        ProjectedAggregateOp::Min {
                            field: "title".to_owned(),
                        },
                    ),
                )
                .await
                .expect("min serves");
            assert_eq!(
                whole_set_value(&observed_min),
                AggregateValue::Scalar(Some(
                    CanonicalValue::string(&min_title).expect("bounded title")
                )),
                "{label} must take its extreme from its own partition only"
            );

            // Grouping is the other disclosure surface: the neighbour's key
            // values must never appear among this org's group keys.
            let (context, _cancellation) = harness.context(0xe4);
            let grouped = harness
                .service
                .execute_projected_query(
                    context,
                    grouped_request(org, vec!["title"], vec![ProjectedAggregateOp::Count]),
                )
                .await
                .expect("grouped serves");
            let (_, groups) = grouped_values(&grouped);
            let observed_titles: Vec<CanonicalValue> =
                groups.iter().map(|(keys, _)| keys[0].clone()).collect();
            let foreign = if org == org_a {
                &NEIGHBOUR_SEED[..]
            } else {
                &AGGREGATE_SEED[..]
            };
            for (_, _, _, title) in foreign {
                assert!(
                    !observed_titles
                        .contains(&CanonicalValue::string(*title).expect("bounded title")),
                    "{label} leaked the neighbour's group key {title}"
                );
            }
            assert_eq!(observed_titles.len(), count as usize);
        }
        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}

/// Empty matching set: a whole-set fold still answers, with identity values and
/// an absent extreme. Reporting "no groups" here would be indistinguishable
/// from a query that had never run.
#[test]
fn empty_matching_set_answers_with_identity_values_and_an_absent_extreme() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-empty").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        // A project no ticket belongs to: the predicate matches nothing.
        let empty = vec![ProjectedColumnPredicate::Eq {
            field: "project_id".to_owned(),
            value: CanonicalValue::Uuid(uuid(0x5f)),
        }];

        let (context, _cancellation) = harness.context(0xf1);
        let count = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(org, empty.clone(), ProjectedAggregateOp::Count),
            )
            .await
            .expect("count over an empty set still serves");
        assert_eq!(
            whole_set_value(&count),
            AggregateValue::Count(0),
            "an empty matching set counts zero, it does not vanish"
        );

        let (context, _cancellation) = harness.context(0xf2);
        let sum = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    empty.clone(),
                    ProjectedAggregateOp::Sum {
                        field: "story_points".to_owned(),
                    },
                ),
            )
            .await
            .expect("sum over an empty set still serves");
        assert_eq!(whole_set_value(&sum), AggregateValue::Sum(0));

        for op in [
            ProjectedAggregateOp::Min {
                field: "story_points".to_owned(),
            },
            ProjectedAggregateOp::Max {
                field: "story_points".to_owned(),
            },
        ] {
            let (context, _cancellation) = harness.context(0xf3);
            let extreme = harness
                .service
                .execute_projected_query(context, aggregate_request(org, empty.clone(), op))
                .await
                .expect("extreme over an empty set still serves");
            assert_eq!(
                whole_set_value(&extreme),
                AggregateValue::Scalar(None),
                "an empty matching set has no extreme; absence is not NULL"
            );
        }

        // Grouped: no matching rows means no groups, which is the correct
        // grouped answer and is why the whole-set path is used for globals.
        let (context, _cancellation) = harness.context(0xf4);
        let grouped = harness
            .service
            .execute_projected_query(context, {
                let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
                    .with_predicates(empty)
                    .with_limit(Some(50))
                    .with_group_by(Some(ProjectedGroupBySpec {
                        keys: vec!["status".to_owned()],
                        aggregates: vec![ProjectedAggregateOp::Count],
                    }));
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    FreshnessPolicy::Available,
                )
            })
            .await
            .expect("grouped over an empty set still serves");
        let (_, groups) = grouped_values(&grouped);
        assert!(groups.is_empty(), "no matching rows means no groups");
        harness.stop_coordinator();
    });
}

/// A budget the fold exceeds must return a typed rejection, never a truncated
/// or partial answer (ADR-0087 governance).
#[test]
fn exceeded_group_cardinality_budget_is_a_typed_rejection_not_a_partial_answer() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-budget").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        // Six distinct titles against a two-group ceiling.
        let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
            .with_limit(Some(50))
            .with_group_by(Some(ProjectedGroupBySpec {
                keys: vec!["title".to_owned()],
                aggregates: vec![ProjectedAggregateOp::Count],
            }))
            .with_budget(QueryBudget {
                max_scanned_rows: 100_000,
                max_group_cardinality: 2,
            });
        let (context, _cancellation) = harness.context(0xa1);
        let failure = harness
            .service
            .execute_projected_query(
                context,
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    FreshnessPolicy::Available,
                ),
            )
            .await
            .expect_err("an exceeded grouping budget must never serve a partial fold");
        assert_eq!(
            failure.public_error().map(riffdb_errors::PublicError::kind),
            Some(PublicErrorKind::Validation),
            "budget rejection stays a typed public failure: {failure:?}"
        );

        // The scan budget is the same contract on the other axis.
        let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
            .with_limit(Some(50))
            .with_aggregate(Some(ProjectedAggregateOp::Count))
            .with_budget(QueryBudget {
                max_scanned_rows: 1,
                max_group_cardinality: 10_000,
            });
        let (context, _cancellation) = harness.context(0xa2);
        let failure = harness
            .service
            .execute_projected_query(
                context,
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    FreshnessPolicy::Available,
                ),
            )
            .await
            .expect_err("an exceeded scan budget must never serve a partial fold");
        assert_eq!(
            failure.public_error().map(riffdb_errors::PublicError::kind),
            Some(PublicErrorKind::Validation)
        );

        // The same query inside its budget still answers, so the rejections
        // above are about the budget and not about the shape.
        let (context, _cancellation) = harness.context(0xa3);
        let ok = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(org, Vec::new(), ProjectedAggregateOp::Count),
            )
            .await
            .expect("the default budget still serves");
        assert_eq!(
            whole_set_value(&ok),
            AggregateValue::Count(AGGREGATE_SEED.len() as u64)
        );
        harness.stop_coordinator();
    });
}

/// F3 — the declared row limit must bound grouped output volume, not only the
/// authorization decision.
///
/// The engine ignores `limit` on the grouped path, so without the service-side
/// clamp a caller declaring `limit = 1` — a declaration every grant this system
/// can issue admits — could receive up to the whole server grouping budget in
/// group rows of raw key-column values. The clamp makes the authorized row
/// count the grouping ceiling, and an overrun stays a typed rejection rather
/// than a truncated answer.
#[test]
fn the_declared_row_limit_bounds_grouped_output_volume() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-clamp").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        // Six distinct titles, one declared row. The default server grouping
        // budget (10,000) would happily return all six.
        let grouped_with_limit = |limit: u32| {
            let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
                .with_limit(Some(limit as usize))
                .with_group_by(Some(ProjectedGroupBySpec {
                    keys: vec!["title".to_owned()],
                    aggregates: vec![ProjectedAggregateOp::Count],
                }));
            ExecuteProjectedQueryRequest::new(
                SymbolicContractSelector::active(),
                BOARD_PROJECTION_NAME,
                body,
                FreshnessPolicy::Available,
            )
        };

        let (context, _cancellation) = harness.context(0xaa);
        let failure = harness
            .service
            .execute_projected_query(context, grouped_with_limit(1))
            .await
            .expect_err("one declared row must not admit six group rows");
        assert_eq!(
            failure.public_error().map(riffdb_errors::PublicError::kind),
            Some(PublicErrorKind::Validation),
            "the overrun must be typed, not truncated: {failure:?}"
        );

        // Exactly at the boundary the same query is served in full, so the
        // clamp is the declared bound and not an unconditional refusal.
        let (context, _cancellation) = harness.context(0xab);
        let served = harness
            .service
            .execute_projected_query(context, grouped_with_limit(AGGREGATE_SEED.len() as u32))
            .await
            .expect("a limit that covers the group count still serves");
        let (_, groups) = grouped_values(&served);
        assert_eq!(groups.len(), AGGREGATE_SEED.len());

        // One below the boundary is refused: the ceiling is exact.
        let (context, _cancellation) = harness.context(0xac);
        assert!(
            harness
                .service
                .execute_projected_query(
                    context,
                    grouped_with_limit(AGGREGATE_SEED.len() as u32 - 1)
                )
                .await
                .is_err(),
            "the clamp must be exact, not approximate"
        );

        // A whole-set aggregate is unaffected: its result cardinality is fixed
        // by the descriptor count, not by the matching set.
        let (context, _cancellation) = harness.context(0xad);
        let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
            .with_limit(Some(1))
            .with_aggregate(Some(ProjectedAggregateOp::Count));
        let whole_set = harness
            .service
            .execute_projected_query(
                context,
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    FreshnessPolicy::Available,
                ),
            )
            .await
            .expect("a whole-set fold returns one value regardless of the limit");
        assert_eq!(
            whole_set_value(&whole_set),
            AggregateValue::Count(AGGREGATE_SEED.len() as u64)
        );

        // Row reads keep their own behaviour: limit truncates, never rejects.
        let (context, _cancellation) = harness.context(0xae);
        let rows = harness
            .service
            .execute_projected_query(context, {
                let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
                    .with_select(vec!["title".to_owned()])
                    .with_limit(Some(1));
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    FreshnessPolicy::Available,
                )
            })
            .await
            .expect("row reads still truncate rather than reject");
        assert_eq!(row_cells(&rows, &["title"]).len(), 1);
        harness.stop_coordinator();
    });
}

/// Summing a money column is a real engine limitation. It must surface as a
/// typed, actionable rejection naming a wrong *type* — not the generic invalid
/// value code every other projected rejection uses.
#[test]
fn summing_a_money_column_is_a_typed_actionable_rejection() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-money").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));

        let (context, _cancellation) = harness.context(0xa8);
        let failure = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Sum {
                        field: "cost".to_owned(),
                    },
                ),
            )
            .await
            .expect_err("money columns cannot be summed by the engine");
        let error = failure
            .public_error()
            .expect("the rejection must be public and typed");
        assert_eq!(error.kind(), PublicErrorKind::Validation);
        let riffdb_errors::PublicErrorDetails::Validation(issues) = error.details() else {
            panic!("a validation failure carries its issues, got {error:?}");
        };
        assert!(
            issues
                .as_slice()
                .iter()
                .any(|issue| issue.code() == riffdb_errors::ValidationCode::TypeMismatch),
            "the caller must be told the column type is wrong, not that a value is: {issues:?}"
        );

        // Min and max over the same money column still work: the rejection is
        // specific to the fold that needs integer arithmetic.
        let (context, _cancellation) = harness.context(0xa9);
        let min = harness
            .service
            .execute_projected_query(
                context,
                aggregate_request(
                    org,
                    Vec::new(),
                    ProjectedAggregateOp::Min {
                        field: "cost".to_owned(),
                    },
                ),
            )
            .await
            .expect("money columns still compare");
        assert!(matches!(
            whole_set_value(&min),
            AggregateValue::Scalar(Some(CanonicalValue::Money(_)))
        ));
        harness.stop_coordinator();
    });
}

/// Freshness is orthogonal to result shape: a causal aggregate parks, wakes on
/// real apply, and reports the same frontier and chaining token a row read at
/// that frontier reports.
#[test]
fn causal_aggregates_carry_the_same_frontier_and_token_as_rows() {
    run_async_threads(4, async move {
        let mut harness = ServiceHarness::columnar_board();
        let org = uuid(0x41);
        let project = uuid(0x51);
        let sequence = seed_aggregate_board(&harness, org, project, 0x60, "agg-causal").await;
        harness
            .board_columnar()
            .wait_until_applied(sequence, Duration::from_secs(10));
        let token =
            CommitToken::new_scoped(support::database_id(), BOARD_HISTORY_INCARNATION, sequence);

        let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
            .with_limit(Some(50))
            .with_aggregate(Some(ProjectedAggregateOp::Count));
        let (context, _cancellation) = harness.context(0xb8);
        let aggregate = harness
            .service
            .execute_projected_query(
                context,
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    causal(token.clone(), Duration::from_secs(10)),
                ),
            )
            .await
            .expect("a causal aggregate serves at the token");

        let (context, _cancellation) = harness.context(0xb9);
        let rows = harness
            .service
            .execute_projected_query(context, {
                let body = ProjectedQueryBody::new(CanonicalValue::Uuid(org))
                    .with_select(vec!["story_points".to_owned()])
                    .with_limit(Some(50));
                ExecuteProjectedQueryRequest::new(
                    SymbolicContractSelector::active(),
                    BOARD_PROJECTION_NAME,
                    body,
                    causal(token, Duration::from_secs(10)),
                )
            })
            .await
            .expect("a causal row read serves at the token");

        let ExecuteProjectedQueryResult::Ready {
            frontier: aggregate_frontier,
            head: aggregate_head,
            commit_token: aggregate_token,
            ..
        } = &aggregate
        else {
            panic!("expected Ready, got {aggregate:?}");
        };
        let ExecuteProjectedQueryResult::Ready {
            frontier: row_frontier,
            head: row_head,
            commit_token: row_token,
            ..
        } = &rows
        else {
            panic!("expected Ready, got {rows:?}");
        };
        assert_eq!(
            aggregate_frontier.as_bytes(),
            row_frontier.as_bytes(),
            "both shapes report the same served frontier"
        );
        assert_eq!(aggregate_head.as_bytes(), row_head.as_bytes());
        assert_eq!(
            aggregate_token.as_ref().map(CommitToken::as_bytes),
            row_token.as_ref().map(CommitToken::as_bytes),
            "the chaining token must not depend on the result shape"
        );
        assert!(
            aggregate_token.is_some(),
            "a sequenced frontier yields a chaining token"
        );
        assert_eq!(
            whole_set_value(&aggregate),
            AggregateValue::Count(AGGREGATE_SEED.len() as u64)
        );
        harness.board_columnar().assert_no_apply_error();
        harness.stop_coordinator();
    });
}
