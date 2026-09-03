# Superseded public-authoring runs

These completed runs are retained exactly as observed but are not release
acceptance evidence. `library-02` compiled successfully against kit inventory
`5439d40c08bb12640c2bb76ebb3f4209509275449c6981674c3efa260ea30eee`.
It was superseded because the Clinic and Fleet evaluations exposed additional
diagnostic repairs, producing the final kit inventory used by the selected
campaign. No result from this directory is selected or scored by the gate.

`candidate-final-kit-v3` retains the complete Library-03, Clinic-02, and
Fleet-02 candidate campaign after the workspace battery found that its binary
also changed accepted non-comma `RDB-S004` help. The original campaign manifest
is preserved byte-for-byte with SHA-256
`f5a1a25fb855addf9f8819f62cfe5967dbc431c22f544b4423fbd4bc51902282`,
and its complete content-addressed `runs/` inventory is copied beside it. The
reports' diagnostic sufficiency judgments are unchanged. This candidate is
also non-evidentiary because it did not exercise the final narrowed binary.

`candidate-final-kit-v4` retains the complete Library-04, Clinic-03, and
Fleet-03 campaign against narrowed kit inventory `60d5d2a5cf08be5d7ef4e7925c4a14ff018d4599bb5abd431b6c5cdbfa3dbe49`.
Its original campaign manifest is preserved byte-for-byte with SHA-256
`dbfcb56c8e82e868c967f8d3825ed4c67938345d5eada60597748d5999d7de8e`.
The package clippy gate then required a source-form-only collapse of the
missing-comma condition. Debug information changed the opaque binary's bytes,
so exact-kit qualification superseded this otherwise passing campaign. Its
source, raw diagnostic, report inventory, and judgments remain unchanged.
