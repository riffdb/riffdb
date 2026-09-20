---
adr: "0249"
title: Tail Latency Is Disclosure With A Named Mechanism
status: accepted
tier: guarantee
date: 2026-09-20
accepted: 2026-09-20
acceptance: 'maintainer, in session, 2026-09-20: "ADR-0249 is approved"
  (ADR-0249 as written)'
requires: [ADR-0171, ADR-0239, ADR-0146]
amends:
  - ADR-0171 by stating what p99 is, which its stability rule leaves unsaid
    while binding p50 and p95.
supersedes: []
requirements: [PERF-018]
packages: [WP-799]
obligations: []
review_triggers:
  - p99 would be made a pass/fail gate without its run-to-run spread having been
    measured on the host the gate binds.
  - A tail regression would be published without a named mechanism.
  - A campaign would be blocked on a tail figure that no accepted record gates.
---
# ADR-0249: Tail Latency Is Disclosure With A Named Mechanism

## Context

WP-749 has been holding its own closure against a p99 pass. **No accepted record
gates p99.** PERF-018 gates throughput at 0.90 and p95 at 1.25; ADR-0171 binds
"RiffDB's qualified p50 and p95 ... on every scenario and profile". SPEC.md
mentions p50/p95/p99 only among the things measured. A package has therefore
been blocked for days by a standard the project never set, which is worse than
either gating it or not.

p95 was the right statistic to gate, and the reason is structural rather than
conservative. ADR-0146 fixed generations at exactly 1,000 measurements, which
makes p95 the fiftieth worst sample and p99 the tenth. The p99 estimator has
materially higher variance by construction. **It is a different statistic, not a
stricter one**, and a p99 gate set at p95-like tightness fails on noise while
teaching nothing.

The input a fair threshold needs does not exist. ADR-0239 records this host's
run-to-run variance at 2 to 4 percent for throughput; **nobody has measured the
run-to-run spread of p99.** ADR-0171's own 1.20 was derived from observed
behaviour rather than chosen, and choosing a p99 number now without the same
grounding would repeat the error this programme has made repeatedly: acting on
an assumed figure and discovering later it was measured wrong.

## Decision

1. **p99 is disclosure, not a pass/fail gate.** It is measured and published on
   every campaign that publishes p50 and p95. It does not by itself invalidate
   evidence or block a closure. This follows ADR-0171's own move, where a
   comparator spread above 1.20 became mandatory disclosure rather than
   invalidating evidence.
2. **A published tail regression carries a named mechanism** — one extra fsync
   per N commits, a lock held across a flush, an allocation spike at a bound.
   The mechanism is the disclosure; a number alone is not.
3. **An unexplained tail movement blocks, at any size.** A regression nobody can
   attribute is a correctness signal rather than a performance one, and the
   cheapest moment to investigate it is when it appears. This is deliberately
   stricter about an unexplained 2 percent than about an explained 15 percent.
4. **p99 becomes a gate only after its spread is measured** on the host the gate
   would bind, by five generations of identical code, and the threshold is set
   at what that host actually does. Until then no campaign is blocked on it and
   no closure is refused for it.

## Options considered

**Set a p99 ceiling now, by analogy to p95's 1.25.** Rejected. The two
statistics have different variance at the same sample count, so the analogy does
not hold, and the number would be a guess presented as a gate.

**Leave p99 ungated and unmentioned**, which is the literal status quo.
Rejected because it is what produced the current situation: a package blocking
itself on an unwritten standard, with no record to point at either way.

**Gate p99 strictly, accepting that durability features will fail it.** Rejected.
Archive and replication add real durable work and some tail cost is the honest
price. A gate that the correct implementation cannot pass produces pressure to
relax the gate, which is the pattern this programme has already run twice.

## Consequences

A tail regression can now ship, and that is the intended trade: it ships
*explained*, in public, with the mechanism recorded. The risk is that explained
regressions accumulate until the tail is bad for reasons each of which was
individually justified. Decision 4 is the answer — once the spread is measured
the gate becomes available, and the accumulated disclosures are the evidence for
where to set it.

WP-749 is unblocked immediately. Its archive campaign publishes p99 alongside
p50 and p95, attributes any regression, and is not refused closure on the tail
figure alone.
