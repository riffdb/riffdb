# Public-source-only authoring evaluation

WP-722 evaluates contract and RiffQL authoring, independently of generated SDK
or runtime-driver evaluation. Each run starts in a fresh context with an unseen
domain brief and exactly three product inputs:

1. the public handbook copied by `scripts/wp722-authoring-kit`;
2. a normal `riffdb new` scaffold, including its repository-local
   `AUTHORING.md`; and
3. an opaque `riffdb` executable that returns ordinary authoring diagnostics.

The kit contains no repository metadata, specification, work-package file,
compiler source, or runtime source. Network access is disabled. An evaluator
may read only its private copy of the kit and its domain brief. It records every
failed `riffdb --output json application check --source-only` invocation before
making another edit; a diagnostic counts as sufficient only when its `help`
field itself states a working source repair. Fix codes, handbook searches, and
prior product knowledge do not rescue missing help.

The checked evidence lives in three run directories:

- `library`: two roots, a same-partition cross-aggregate command, fine conflict
  keys, and a bounded index query;
- `clinic`: a required relationship proof, bounded cascade delete policy, and
  a bounded index query; and
- `fleet`: aggregate/locality choice, an indexed restrict policy, and a bounded
  index query.

Every run is produced by a distinct fresh agent identity. The repository gate
recompiles each final contract and query through `application check
--source-only`, validates the closed report schema, rejects any implementation
source read or network use, checks that every observed diagnostic retained
nonempty sufficient help, requires every selected run to name the same
content-addressed final kit inventory, and verifies the required source
markers. Failed diagnostic-discovery attempts and a successful run against a
pre-final kit are retained separately without being reclassified or selected.
The score
is ten points: two each for the final contract and query, two for source
isolation, two for a complete diagnostic transcript whose help was sufficient,
one for the brief's required modeling shape, and one for complete evidence. A
run passes only at 10/10; scores are not averaged.

Reproduce the evidence gate with:

```bash
./scripts/wp722-authoring-acceptance
```

The final sources are also the worked-example corpus. Because the gate compiles
them from their public manifests, examples of aggregate choice, delete policy,
bounded reads, required relationship proof, and partition-local
cross-aggregate writes cannot drift into illustrative-only syntax.
