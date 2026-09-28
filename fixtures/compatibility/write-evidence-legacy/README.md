# Historical write classifications

These canonical contract bundles were preserved from the pre-ADR-0262 fixture
inventory before V25 regeneration. They are durable protocol fixtures, not
compiled application binaries. Do not recompile them with the current compiler.

Compatibility tests decode and retain their canonical bytes and legacy proof
status. The initialized-state fixture also reproduces serialized payload
replacement despite revision increments, proving that historical artifacts
are not silently given new semantics.
