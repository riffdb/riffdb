# TicketDesk symbolic acceptance application

This client is the post-WP-200 rebuild. Every page calls one named RiffQL
operation through `execute_named_application_query`; every mutation calls one
name-addressed command through `execute_application_command`.

The checked generated module supplies parameter/result/input types and pins the
exact query-module hash. Application source contains no numeric schema IDs,
protobuf construction, encoded-key parsing, field masks, `GetEntity`,
`ScanIndex`, or public N+1 loops.

`TicketDeskClient` accepts `StableApplicationClient`, whose public surface has
no kernel entity/index or administrative operations. The live acceptance uses
the default `ticketdesk-application` credential containing only the eight exact
commands and eight exact immutable named queries.

Run the machine boundary:

```bash
./scripts/check-ticketdesk-symbolic-boundary
```

The underlying transport remains public gRPC over a reused HTTP/2 connection.
RiffQL removes the structural RPC multiplication: the server plans batch reads
and joins and executes the complete page against one snapshot.
