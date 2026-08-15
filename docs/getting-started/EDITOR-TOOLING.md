# Contract and RiffQL editor tooling

The signed RiffDB CLI includes a compiler-backed Language Server Protocol
service:

```bash
riffdb lsp
```

The command speaks LSP over standard input and output. Editors should launch
it directly with no endpoint, credential, or database configuration. It
supports full-document synchronization for `.riff` and `.riffq` files,
compiler diagnostics, hover types, go-to-definition, and schema-fed
completion.

Diagnostics are not an editor approximation. Contract diagnostics pass
through the same compiler and authoring-diagnostic conversion used by
`riffdb push`; RiffQL diagnostics use the authoritative RiffQL parser and,
when the local contract is available, the query-module compiler. The primary
code, safe message, and UTF-8 source span therefore agree with command-line
checks. LSP positions are translated to UTF-16 only at the protocol boundary.

The service is deliberately local and authority-free. It does not load a
credential, connect to a daemon, mutate files, deploy contracts, or run an
application operation. It accepts at most 1 MiB per frame or source document,
32 open documents, 32 compiler diagnostics, and 256 schema symbols. Malformed
frames fail closed; malformed JSON receives the standard bounded JSON-RPC
parse error. Full-document changes are the only accepted synchronization
shape.

## Highlighting grammars

CLI packages also carry `tree-sitter-riff` and `tree-sitter-riffql`. These
grammars provide highlighting for `.riff` and `.riffq` in Tree-sitter-capable
editors and source forges. They are intentionally non-authoritative: a
highlighter accepting a token sequence never makes that sequence a valid
RiffDB contract or query.

Their keyword inventories are generated from the first-party lexer/parser.
The checked-in highlight snapshot corpus covers the bounded public contract
and RiffQL handbook examples. Run:

```bash
./scripts/generate-editor-grammars --check
```

to verify both the inventories and snapshots. The canonical package copies
live under `crates/riffdb-cli/assets/editor/`.

Installed asset locations are deterministic:

| CLI package | Grammar asset root |
|---|---|
| Native installer | `<prefix>/share/riffdb/editor` |
| npm `@riffdb/cli` | `node_modules/@riffdb/cli/editor` |
| PyPI `riffdb-cli` | `site-packages/riffdb_cli/editor` |
| Rust `riffdb-cli` crate | `assets/editor` in the crate archive |

## VS Code and other editors

`clients/editor/vscode-riffdb` is a thin extension scaffold. It registers the
two file types and launches `riffdb lsp`; it contains no parser, compiler,
authorization logic, credentials, or application transport.

Other editors should use the same launch contract. Configure the language
server command as `riffdb lsp`, associate `.riff` and `.riffq`, and install the
corresponding Tree-sitter grammar if the editor supports Tree-sitter-based
highlighting.

The alpha grammar packages include JavaScript grammar sources, highlight
queries, exact keyword inventories, and Tree-sitter metadata. Generated native
parser bindings are editor-distribution concerns and are not linked into the
RiffDB runtime.
