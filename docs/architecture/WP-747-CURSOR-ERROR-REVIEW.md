# WP-747 cursor error classification

Status: accepted by the maintainer in session on 2026-09-15: "Approve exact text".
Standalone acceptance commit: `3fe8826b`.

## Conflict

WP-747 previously asked for foreign or pre-promotion tokens, cursors and fences
to map to `RDB-HISTORY-0101`. Accepted ADR-0070 instead requires stale,
superseded and evicted continuation handles to return `RDB-CURSOR-0101`.
The accepted cursor is a 16-byte process-local registry handle, with no
decodable database or incarnation claim. After restart, promotion or arrival
at another process, a missing handle cannot distinguish those causes from
expiry or eviction. Remapping every missing handle would change ADR-0070.

REP-004 and ADR-0178 require fail-closed refusal, without assigning a different
error code to opaque process-local handles. The accepted follower-columnar
amendment also says obsolete process-bound cursors fail closed.

The TLS test `follower_exact_providers_match_primary_after_tail_and_restart`
now proves valid follower continuation matches the primary, a source-process
handle cannot continue on the follower, and a live follower handle cannot
continue after follower restart. Both refusals preserve `CursorInvalid` and
release no page; the complete authoritative namespace remains a source prefix.

## Exact accepted replacement for the WP-747 deliverable

> Fail-closed refusal of tokens, cursors and fences from another lineage or a
> pre-promotion incarnation. Detectable database/history mismatches in scoped
> tokens and history-bearing fences use the existing `RDB-HISTORY-0101` class.
> Opaque process-local continuation handles that cannot resolve in the current
> registry retain `RDB-CURSOR-0101`, including foreign-process and obsolete
> pre-promotion handles, as required by ADR-0070. No refusal releases rows or
> internally retries from the first page. This changes no cursor bytes,
> authorization, retention, freshness guarantee, SPEC requirement or ADR.

The standalone acceptance commit replaced only that work-package deliverable
and recorded this clarification in its open closure decisions. The installed
deliverable was compared verbatim with the block above. Acceptance authorizes
no cursor format change or weaker foreign-history refusal.
