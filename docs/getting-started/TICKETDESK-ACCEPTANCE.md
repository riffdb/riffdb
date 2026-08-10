# TicketDesk application acceptance

## Outcome

The post-WP-200 symbolic rebuild rates **8.3/10** as an application platform,
up from the independent-agent baseline of 4/10. Systems/protocol rigor remains
8/10 or better.

This score is tied to machine-checked behavior:

| Area | Score | Evidence |
|---|---:|---|
| Domain modeling | 8 | Contract and RiffQL sources use domain symbols |
| Mutations | 8 | One symbolic compiled-command invocation per mutation |
| Reads | 9 | One named RiffQL RPC and one snapshot per list/detail page |
| Authorization | 8 | Compiler-derived fields, indexes, rows, and partition route |
| Day-one loop | 8 | `riffdb dev --seed` performs bounded bootstrap and setup |
| Debuggability | 8 | Source-spanned diagnostics and name-only explain plans |
| Type safety/codegen | 8 | Reproducible Rust, Go, TypeScript, Python, and MCP artifacts |
| Application docs | 8 | CRUD workflow, language, planning, and module guides |

The original feedback’s six concrete failure modes are closed:

1. Application code uses symbols and generated identity constants, never
   compiler IDs.
2. List and detail pages are one RiffQL request, not scan plus N point reads.
3. The query compiler derives capability requirements; callers do not build
   masks.
4. Compiler failures contain stable codes, symbols, spans, and remediation.
5. `riffdb dev` performs local bootstrap, deploy, role grant, watch, and seed.
6. Stable operations generate parameter, result, command, and MCP artifacts.

## Reproducible transcript

```text
$ ./scripts/riffdb-dev-acceptance
ticketdesk-seed: execute ListTickets
ticketdesk-seed: execute TicketPage
riffdb-dev-seed-v1        276     1944435382ns
riffdb-dev-query-p50-v1   400901ns       414151ns
TicketDesk symbolic boundary is clean.
riffdb dev bootstrap, role preset, contract, and query module acceptance passed.

$ ./scripts/benchmark-application-path --assert-wp270
point_get_ticket                         205081ns <=    5000000ns
point_get_user                           167591ns <=    5000000ns
ticket_detail_page                      1512004ns <=   15000000ns
create_comment                          8616727ns <=   20000000ns
seed_276_rows                        1741288103ns <= 3000000000ns
unary_transport_complete_request_upper_bound
                                          167591ns <=    1000000ns
Application performance gates pass using a reused public HTTP/2 connection.
```

Exact checked reports live at:

- `benchmarks/application-path/latest-v1.json`
- `benchmarks/application-path/wp270-riffql-v1.json`

## gRPC decision

Unary gRPC remains the application transport. The earlier 43–477 ms results
were dominated by repeated policy/current-view work and client-side structural
RPC multiplication. They did not establish that HTTP/2 framing was the
bottleneck.

The optimized complete point request is 0.168–0.205 ms, which is itself an
upper bound on framing and adaptation. Named RiffQL collapses the list and
detail shapes to one public RPC and one authoritative read transaction; the
measured named list/detail p50 values are about 0.4 ms. Replacing gRPC would
add a second protocol without addressing the original structural cause.

## Boundary enforcement

`scripts/check-ticketdesk-symbolic-boundary` rejects:

- numeric entity, field, index, and command-input IDs;
- protobuf construction;
- encoded-key parsing;
- field-visibility masks;
- `GetEntity` or `ScanIndex`; and
- read-side request loops or N+1 calls.

The acceptance application contains four named pages and all eight TicketDesk
mutations. The live seed creates exactly 276 rows through commands and then
executes both a representative list and composite detail query.
