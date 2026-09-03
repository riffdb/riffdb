# Superseded diagnostic-discovery runs

These three fresh runs used the first source-pruned kit, whose
`checksums.sha256` digest was
`8b5cded7c8278f134ef4fc398dc7a9f15b11f8c83ae39a9454f7734d50afb131`.
All three eventually compiled, but none is exit-gate evidence: their immutable
reports rated at least one diagnostic's help insufficient. They are retained
rather than rewritten because those failures caused the WP-722 diagnostic
repairs.

- Library-01 found that missing-comma `RDB-S004` named only the whole grammar.
- Clinic-01 found ordinary collection-delete source reaching help-less
  `RDB-C023`.
- Fleet-01 reproduced `RDB-C023`, found `RDB-QP001` too general to identify an
  unknown symbolic type, and found help-less `RDB-QM004` for manifest/source
  query-name drift.

The passing campaign selects only the `runs/` evidence and does not average or
reinterpret these reports.
