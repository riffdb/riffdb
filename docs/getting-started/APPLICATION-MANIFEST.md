# Application manifest

An application manifest is the canonical, reviewed input to RiffDB application
generation. It binds source paths to exact compiled identities, names the
operations available to each symbolic role, and declares all generated and
seed artifacts. It is not a discovery hint: a mismatch fails closed.

The current schema identifier is:

```text
riffdb.application-manifest/v1
```

The v1 document is closed JSON with exactly these top-level members:

```json
{
  "application": "ticketdesk",
  "contract": {
    "bundle_hash": "<64 lowercase hexadecimal characters>",
    "lineage": "TicketDesk",
    "source": "riffdb/contract.riff",
    "version": 1
  },
  "generation": {
    "mcp": "generated/ticketdesk.mcp.json",
    "rust": "src/generated/riffdb.rs",
    "typescript": "src/generated/riffdb.ts"
  },
  "query_modules": [
    {
      "module_hash": "<64 lowercase hexadecimal characters>",
      "name": "ticketdesk",
      "queries": [
        {
          "name": "TicketPage",
          "source": "riffdb/queries/ticket_page.riffq"
        }
      ],
      "version": 1
    }
  ],
  "roles": [
    {
      "commands": ["CreateTicket"],
      "name": "TicketDeskAgent",
      "queries": ["TicketPage"]
    }
  ],
  "schema": "riffdb.application-manifest/v1",
  "seed_inputs": ["riffdb/seed/dev.jsonl"]
}
```

All paths are normalized, workspace-relative paths. Absolute paths, parent
components, empty components, duplicate paths, duplicate names, undeclared
query references, and unknown members are rejected. Versions are positive.
Names and collections are bounded; the source document may not exceed one MiB.
Hash text is exact lowercase hexadecimal.

RiffDB sorts every set-like collection, serializes the validated value as
compact canonical JSON with a final line feed, and hashes those bytes under the
`riffdb.application-manifest/v1` domain. The compiler exposes byte spans for
manifest-defined symbols and paths so diagnostics can point to the reviewed
input. Reformatting or reordering set-like members does not change identity;
changing a semantic value does.

Generation checks the manifest's contract lineage, version, bundle hash, query
module name, version, and module hash against the compiled inputs. It never
silently selects an ambient active version or regenerates against a different
identity. The checked-in TicketDesk example is
[`fixtures/application-manifests/ticketdesk-v1.json`](../../fixtures/application-manifests/ticketdesk-v1.json).

Run generation and drift checks with:

```bash
./scripts/generate-query-clients
./scripts/generate-query-clients --check
./scripts/check-application-bindings
```

Generated Rust and TypeScript facades own parameter serialization, response
decoding, exact identity checks, opaque cursors, read-after-commit fences,
typed command outcomes, and retry-safe uncertainty recovery. Generated MCP
schemas come from the same operation registry. Application code uses these
facades or RiffQL text; numeric IDs, field masks, protobuf records, and raw RPC
wrappers remain generated or internal implementation details.
