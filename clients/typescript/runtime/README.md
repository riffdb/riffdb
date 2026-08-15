# RiffDB Typescript application package

RiffDB is a contract-first operational database. Application writes remain
compiled commands; this package never exposes SQL, raw transactions, numeric
compiler IDs, storage keys, or an authorization bypass.

## Install

```bash
npm install --save-dev @riffdb/cli@0.1.0
npm install @riffdb/client@0.1.0
```

## Four-step application story

1. **schema** — Declare the selected generated client and author the symbolic schema and bounded named operations.

   ```bash
   riffdb init inventory --generator typescript
   ```
2. **install** — Compile, lock, and install through the exact change-review ceremony only when compiler-owned identity changes.

   ```bash
   riffdb push
   ```
3. **generate** — Materialize the selected typed client from the exact lock.

   ```bash
   riffdb generate
   ```
4. **query** — Invoke only generated named operations through the first-party runtime.

   ```bash
   npm start
   ```

The CLI requires an already running RiffDB service and an ordinary protected
application credential. Acknowledged commands are durable; an uncertain
response must be resolved with the same idempotency key.

## Alpha compatibility

This is a pre-1.0 alpha package. It carries the exact durable-format statement
from `release/durable-format-manifest-v1.json`; downgrade is unsupported and a
breaking epoch requires symbolic export/reimport. Sealed and explicitly offline bundles retain vendored @riffdb/application, riffdb-application, riffdb.dev/application, and the source-contained Rust SDK.
