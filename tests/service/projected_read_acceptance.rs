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

use std::sync::Arc;
use std::time::{Duration, Instant};

use riffdb_errors::PublicErrorKind;
use riffdb_service::{
    CommandApplication, ExecuteCommandRequest, ExecuteCommandResult, ExecuteProjectedQueryResult,
    ExecuteSymbolicQueryResult, ProjectedQueryApplication, SymbolicQueryApplication,
    SymbolicResultField,
};
use riffdb_types::{
    CanonicalValue, CommitSequence, CommitToken, FreshnessPolicy, FrontierPosition,
    ProjectionFrontier, encode_canonical_value,
};

use support::{BOARD_HISTORY_INCARNATION, BOARD_SELECT, ServiceHarness, run_async_threads};

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
    ProjectionFrontier::new(
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
        let token = CommitToken::new(BOARD_HISTORY_INCARNATION, last_sequence);

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

        let token = CommitToken::new(BOARD_HISTORY_INCARNATION, sequence);
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

        let token = CommitToken::new(BOARD_HISTORY_INCARNATION, sequence);
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

        let token = CommitToken::new(BOARD_HISTORY_INCARNATION, sequence);
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

        let foreign = CommitToken::new(BOARD_HISTORY_INCARNATION + 1, sequence);
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
        let unreachable = CommitToken::new(
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
