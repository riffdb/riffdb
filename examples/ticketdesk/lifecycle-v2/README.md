# Exact-parent lifecycle successor fixture

This compiler-owned application closure supports the sustainable TicketDesk
campaign's additive deployment. It retains the V1 entity and command identities
and adds `LifecycleVersion`. Its expected parent is the exact lifecycle V1
bundle. The public CLI generates the closure against that active parent in an
owned TLS preparation database; standalone genesis compilation is insufficient.

Run `scripts/generate-ticketdesk-lifecycle-successor` from the repository root,
or add `--check` for offline closure and independent parent-identity checks.
Generated files must never be edited manually. Source changes originate in
`fixtures/ticketdesk_lifecycle/additive-v2.riff`; the generator copies it into
this application before asking the public compiler to write the lock.

The preparation's `--additive-deploy` mode proves the V1-to-V2 deployment and
current Rust/TypeScript operations. It does not claim continuous offered load,
observer handover, fault recovery or campaign qualification.
