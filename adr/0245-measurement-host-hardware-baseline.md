---
adr: "0245"
title: Measurement Host Hardware Baseline
status: proposed
tier: guarantee
date: 2026-09-19
accepted: null
acceptance: null
requires: [ADR-0171, ADR-0239]
amends:
  - ADR-0239 by retiring N1 from the release profile set its decision 2 names.
  - ADR-0171 by requiring that a profile bound by the stability rule carry
    hardware SHA-2 acceleration.
supersedes: []
requirements: [PERF-018]
packages: [WP-797]
obligations:
  - id: OBL-0245-1
    package: WP-797
    proof: every_measurement_profile_reports_hardware_sha2_acceleration
    says: Every host admitted as a measurement profile reports hardware SHA-2
      acceleration, checked from the host rather than assumed from its name.
review_triggers:
  - A cost attribution would be drawn from a host whose accelerator profile
    differs from the deployment target.
  - The release profile set would fall to one host, or a second profile would be
    added without hardware SHA-2 acceleration.
  - A hardware capability other than SHA-2 would be found to divide the profiles
    non-uniformly, such as AES-NI, AVX-512 or a storage class.
---
# ADR-0245: Measurement Host Hardware Baseline

## Context

On 2026-09-19 a capture-cost attribution measured on N1 reported hashing at
491 µs per group, 70% of all capture work, and identified it as the target worth
a durable-format redesign. Re-measured on C3D with the same binaries, hashing
was 63 µs — **7.8× smaller** — and 1.41% of writer wall. The conclusion did not
survive the host change, and the work it would have justified was nearly
started.

The cause is that N1 is the only measurement host without hardware SHA-2
acceleration:

| host | CPU | hardware SHA-2 |
|---|---|---|
| C3D, development standard | AMD EPYC 9B14 | yes |
| E2, release profile | AMD EPYC 7B12 | yes |
| N1, release profile | Intel Xeon @ 2.30 GHz | **no** |
| developer workstation | — | yes |

The problem is not that N1 is slow. A uniformly slow host is a useful
magnifying glass: it exaggerates absolute cost while preserving proportion, so
an attribution taken there still ports. N1 is slow **non-uniformly** — it
penalises hash-bound work by roughly 7.8× while other work is slower by closer
to 1.5 to 2×. Attribution is proportion, so a host that distorts proportion
inverts the ranking of what is worth fixing, which is exactly what happened.

This asymmetry was already recorded before the incident. Recording it was not
enough to prevent it, which is why this record makes it a gate rather than a
note.

## Decision

1. A host admitted as a measurement profile must carry hardware SHA-2
   acceleration, verified from the host rather than assumed from its instance
   name. The requirement is stated as hardware SHA-2, not as SHA-NI, because
   the ARMv8 cryptographic extensions are the same capability on the ARM
   hosts RiffDB expects to support.
2. N1 is retired from the release profile set ADR-0239 decision 2 names.
3. The release profile set stays at two hosts. It is not reduced to one:
   two profiles exist to separate a real regression from a single host's
   artifact, and one host cannot do that. N1's replacement may be
   resource-constrained — small, shared-tenant, contended — because a low
   resource ceiling is a useful profile. It may not lack the accelerators of
   the deployment target.
4. **Cost is never attributed on a host whose accelerator profile differs from
   the deployment target.** Such a host may still serve as an absolute floor or
   a regression gate, where only its own history is compared. It may not be used
   to decide what to optimise.
5. Requiring modern hardware for measurement is not by itself a runtime
   requirement. RiffDB does not refuse to start without hardware SHA-2. The
   baseline is documented as the supported measurement and deployment target;
   any change to what the runtime *requires* is a separate decision.

## Options considered

**Keep N1 as a floor-only profile** was rejected. The floor/attribution
distinction is real, and decision 4 records it, but leaving N1 in the release
set makes the distinction something every future reader must remember under
time pressure. It was already written down once and still cost days.

**Reduce to a single release profile** was rejected. It is the cheapest option
and it removes the ability to tell a regression from a host artifact, which is
the reason a second profile exists.

**Require AES-NI and AVX-512 as well** was not taken. Only SHA-2 has been shown
to divide these hosts non-uniformly for this workload. Extending the
requirement on evidence is a review trigger; extending it on speculation would
constrain host selection for no measured reason.

## Consequences

PERF-018 binds the stability rule to every profile, so the profile set is a
SPEC-level fact and retiring N1 amends it. Until a replacement is provisioned
the release gate runs on one profile, which decision 3 says is not acceptable
as a resting state; WP-797 owns closing that gap.

Historical evidence measured on N1 remains valid as what it was: a measurement
on that host. It is not restated or recomputed. Any *attribution* drawn from it
is suspect wherever hardware SHA-2 would change the proportion, and the capture
attribution that prompted this record is the known instance.

The cost of the requirement is that RiffDB is no longer measured on hardware
older than roughly 2017 on AMD or 2019 on Intel. For a database whose write
path hashes every mutation, that is the hardware its users will have.
