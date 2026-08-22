# WP-662 bounded framed transport closure

WP-662 is complete without production activation. The bounded framed
application transport reduced the Tonic/HTTP2 residual, but it did not pass
ADR-0137's reject-first mechanics gate on every required profile and process
generation. The production frame protocol, ingress demultiplexer, client and
driver-host selectors, public benchmark selector, fixtures, and documentation
were removed. Unary gRPC remains the generated-application default and
`PERF-018` is unchanged.

## Measurement identity

All measurements exercised candidate revision `a52ccbe3`, the smoke-scale
TicketDesk seed, the interactive closed-loop profile, one persistent client
connection per worker, one exact generated `GetTicket` point read, and the
generated `CreateComment` command. Each cell used a fresh database and daemon
process. Unary and framed cells alternated as unary/framed, framed/unary,
unary/framed across three process generations. No PostgreSQL qualification was
run because the mechanics gate failed first.

The workstation used an AMD Ryzen 9 7950X and five measured seconds after one
warmup second. N1 (`Intel Xeon`, eight vCPUs) and E2 (`AMD EPYC 7B12`, eight
vCPUs) used ten measured seconds after two warmup seconds. The two cloud hosts
built and ran byte-identical artifacts:

- `riffdbd`: `e1a55eeaacfdc31f0494a261f7bc765a9990be966cee8060bf0cc34472691297`
- `riffdb-app-baseline`: `b54f5368fcaf5c540543337ca71e0b967fcc99151a87b58b79f99d27e3a08774`

These short cells are reject-first mechanics evidence, not `PERF-018` release
evidence.

## Mechanics result

The table gives the range across the three counterbalanced generations. A
positive change is faster. The point-read column is the most favorable valid
interpretation of ADR-0137's generated-operation threshold; the candidate
still fails. Command means are included because the ADR requires the same
ledger for a representative command.

| Host | c1 point-read change | c8 point-read change | c1 command change | c8 command change | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Workstation | 32.97% to 34.26% | 36.47% to 37.64% | -1.45% to 2.20% | 1.50% to 1.98% | c1 below 35% in all generations |
| N1 | 34.61% to 37.51% | 36.35% to 36.89% | 7.81% to 11.35% | -0.65% to 3.68% | one c1 generation below 35% |
| E2 | 21.13% to 30.53% | 31.46% to 36.49% | 2.04% to 8.91% | -1.56% to 5.28% | c1 below 35% in all generations |

The caller-throughput observations agree with the latency result. Workstation
c1 improved only 7.07% to 9.59%; N1 improved 26.34% to 31.40%; E2 improved
13.45% to 25.03%. At c8, E2's third generation improved only 16.52%, below the
required 20%.

## Available stage ledger

Each report closes caller point-read time into the sum of the existing fixed
server named-query stages and an outside-service residual. This is a
conservative two-bucket attribution because the fixed server stage population
covers all named reads in the interactive mix, not a correlated per-call
trace. It must not be represented as the finer client-encode/socket/client-
decode ledger required for activation.

| Host | Unary server-stage sum | Framed server-stage sum | Unary outside residual | Framed outside residual | Outside reduction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Workstation | 32.65-35.23 us | 33.66-33.96 us | 89.06-89.61 us | 47.99-48.31 us | 45.76-46.31% |
| N1 | 133.95-136.52 us | 134.44-141.32 us | 377.71-391.53 us | 193.43-203.23 us | 47.94-50.30% |
| E2 | 154.41-159.84 us | 160.34-188.32 us | 500.94-517.61 us | 294.91-336.01 us | 33.63-41.13% |

E2 misses the required 40% outside-service reduction in two generations and
shows greater-than-five-percent server-stage dilation in those same
generations. The required fine-grained ledger was therefore not pursued: a
missing activation artifact is itself a reject-first failure and cannot cure
the already-failed c1 threshold.

The framed path's smaller command gain is expected: it removes transport work
but not the command's durability fence. This candidate cannot meet a 35%
command-mean threshold without changing a different mechanism.

## Receipts

Raw value-free JSON receipts remain outside the repository:

- workstation: `/home/kevin/tmp/wp662-ws-c{1,8}-{u,f}-r{0,1,2}.json`; manifest
  SHA-256 `9066071199b8c616c85eba69abc2779692a1c95a69345ef3f05baa2b32187d01`
- N1: `/home/kevin/tmp/wp662-cloud-results/n1/`; manifest SHA-256
  `721e628e36abd2d94f90e1b19647ec0962a29c6fcc083d7cdc3b0c566c816866`
- E2: `/home/kevin/tmp/wp662-cloud-results/e2/`; manifest SHA-256
  `de3167620528fee60f9c2578fce7385398481a13d9ee74a7be12b2f66ca6cd15`

Each manifest digest is the SHA-256 of the sorted twelve-line
`sha256sum` manifest after replacing the absolute directory with the receipt
basename.

## Decision

ADR-0137 says any missed mechanics threshold removes production selection and
stops before expensive qualification. The implementation commits were
reverted in full. No frame listener, ALPN identifier, client API, generated
binding selector, driver-host selector, benchmark selector, public protocol
fixture, or customer documentation remains. The accepted ADR remains the
record of the falsified design; it does not activate a compatibility surface.

WP-623 remains open. A future transport candidate requires a new accepted ADR
and new reject-first evidence rather than silently reviving this protocol.
