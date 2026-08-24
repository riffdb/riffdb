# TypeScript applications

RiffDB TypeScript applications use two compiler- and product-owned layers:

```text
generated named operations
        |
        v
@riffdb/application
        |
        v
private driver socket
        |
        v
riffdb-driverd -> verified public RiffDB application surface
```

Server-side application code opens one retained `DriverApplicationTransport`,
wraps it in `DriverGeneratedApplicationTransport`, and passes that transport
to the generated contract client. It then calls only generated methods:

```ts
const driver = await DriverApplicationTransport.connect({
  socketPath,
  identity: publicDriverIdentity,
});
const transport = new DriverGeneratedApplicationTransport(driver);
const db = new AgentAlphaClient(transport, 3);

const created = await db.createItem({
  idempotency_key: requestId,
  item_id: itemId,
  title,
});
const page = await db.itemPage(
  { item_id: itemId },
  created.commitSequence === undefined
    ? {}
    : { readAfterCommit: created.commitSequence },
);

await driver.shutdown();
```

The generated request carries the exact contract/module/plan identities and
compiler-owned schemas. The runtime rejects a response before returning it if
its identity, cardinality, outcome, symbolic fields, wire field identities, or
value types differ.

For a named page fully proven by an explicit covering index, the generated
request negotiates driver V2 compact carriage. Its operation-specific decoder
checks the exact compiler-owned entity and field order, then constructs typed
rows directly from bounded positional values. Application code cannot select
the index, schema, or ordinal and receives the same result type when a legacy
named-record response is used.

Exact decimals and money never pass through a JavaScript `number`. Use the
first-party constructors instead of manually encoding coefficient bytes:

```ts
import { exactDecimal, exactMoney } from "@riffdb/application";

const unitPrice = exactMoney("USD", "25.00");       // money<USD>
const taxRate = exactDecimal("0.0825", 8, 4);       // decimal<8,4>
```

The constructors reject exponents, excess scale or precision, invalid
currencies, and floating-point inputs. The current CLI preserves the compiled
precision in tagged results. Generated decoding also restores it from the
exact result schema when a compatible older tagged value omits the redundant
field, while still rejecting a conflicting precision, scale, or currency.

Generated clients are valid under standard strict TypeScript projects with
both `noUnusedLocals` and `noUnusedParameters` enabled. Compact-carriage decoder
helpers are emitted by reachability from the compiled result layouts: a client
contains only the string, integer, timestamp, and shared payload helpers its
generated decoders actually call. Do not copy unused decoder support into the
generated file or disable an unused-symbol check to accommodate generated
output.

Generated command batch methods accept `concurrency` from 1 through 384 and at
most 4,096 inputs. They use a bounded worker pool over ordinary generated
commands; every item keeps its own identity and result, and the collection is
not atomic.

The protected application-role credential belongs only to `riffdb-driverd`.
The TypeScript process receives a private socket path and public exact-handshake
identity, never the credential, remote endpoint, TLS configuration, or a gRPC
client. Do not expose the driver socket or generated server client to browser
JavaScript. `CliApplicationTransport` remains a compatibility/debug adapter;
new generated repositories use the retained host path.

`AbortSignal` cancels query, command, batch, stream, contextual, and live-query
waits through the local protocol. Stream batch, in-flight, lease, wait, and
negative-acknowledgement delay bounds are transmitted exactly rather than
being advisory client values. Async iterators retain cursors and stop on typed
terminal results. Call `shutdown()` during server drain.

## Generated offline web repository

`riffdb new my-app --language typescript` emits a complete server-side HTTP
starter:

```text
package.json
package-lock.json
tsconfig.json
vendor/riffdb-application/
node_modules/              # materialized by the sealed installation
generated/typescript/client.ts
src/main.ts
```

The sealed installation copies its exact TypeScript 7 compiler, Node type
definitions, and product runtime into the staged repository. No registry access
or manual package edit is required:

```text
npm_config_offline=true npm run check
npm_config_offline=true npm run build
npm start -- <socket> <manifest-hash> <catalog-hash> <database> <role> \
  <role-hash> <remote-identity-hash> <lineage> <version> <bundle-hash>
```

The starter serves `/item`, executes one generated idempotent command, performs
one generated page-shaped read with the returned read-after-commit fence, and
returns only the typed observation. Browser code never receives the credential.

For the canonical local loop, run `riffdb dev --seed --run`. RiffDB starts a
verified loopback-TLS daemon and the first-party `riffdb-driverd`, provisions
the exact application role, and supplies the starter only the protected socket
plus its public handshake identity. The sealed alpha bundle carries a static
development-only loopback certificate and key for this workflow; they are not
production credentials and the daemon never binds them to a non-loopback
address. Remote and production deployments must supply their own TLS identity
through the documented driver-host configuration.

The command stays in the foreground for a web application. Once it prints
`riffdb-app-ready-v1<TAB>PORT`, leave that terminal running and exercise the
generated route from another terminal:

```text
curl http://127.0.0.1:PORT/item
```

The ready marker is an invitation to send requests, not command completion.
Interrupting the foreground command closes the application, scoped driver, and
development daemon together.

Application Source V4 additionally generates typed event async iterators, live
query update unions, a framework-neutral live store, and an application-server
SSE relay. The relay retains the RiffDB credential server-side and requires an
application-owned authorization callback. See [Reactive Application
Clients](../reactive/CLIENTS.md).
