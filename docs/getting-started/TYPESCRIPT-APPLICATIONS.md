# TypeScript applications

RiffDB TypeScript applications use two compiler- and product-owned layers:

```text
generated named operations
        |
        v
@riffdb/application
        |
        v
public RiffDB application surface
```

Application code constructs `CliApplicationTransport` once and passes it to
the generated contract client. It then calls only generated methods:

```ts
const transport = new CliApplicationTransport({
  riffdbPath,
  endpoint,
  credentialFile,
});
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
```

The generated request carries the exact contract/module/plan identities and
compiler-owned schemas. The runtime rejects a response before returning it if
its identity, cardinality, outcome, symbolic fields, wire field identities, or
value types differ.

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

The credential must be the protected application-role credential produced by
`riffdb dev`. Do not pass the bootstrap or administrative credential to a web
application. Do not expose the credential to browser JavaScript; the generated
client is intended for the server side of a TypeScript web application.

The POC runtime executes the public CLI as a bounded child process. This keeps
the TypeScript package free of a second independently implemented protocol
stack while the generated safety contract stabilizes. It is not the final
low-latency transport; production TypeScript should use a first-party
long-lived channel implementing the same `ApplicationTransport` contract.

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
npm start -- <endpoint> <credential-file> <riffdb-path>
```

The starter serves `/item`, executes one generated idempotent command, performs
one generated page-shaped read with the returned read-after-commit fence, and
returns only the typed observation. Browser code never receives the credential.

Application Source V4 additionally generates typed event async iterators, live
query update unions, a framework-neutral live store, and an application-server
SSE relay. The relay retains the RiffDB credential server-side and requires an
application-owned authorization callback. See [Reactive Application
Clients](../reactive/CLIENTS.md).
