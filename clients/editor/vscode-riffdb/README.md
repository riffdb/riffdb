# RiffDB editor client

This is a thin VS Code client for `riffdb lsp`. It contains no parser,
compiler, credentials, authorization logic, or RiffDB application transport.
Install the signed RiffDB CLI first and ensure `riffdb` is on `PATH`.

The separately published `tree-sitter-riff` and `tree-sitter-riffql` assets
provide non-authoritative highlighting for Tree-sitter-capable editors and
forges. Compiler diagnostics from `riffdb lsp` remain authoritative.
