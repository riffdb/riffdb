(function () {
    "use strict";

    var main = document.querySelector("main");
    var sidebarToggle = document.querySelector("#mdbook-sidebar-toggle");
    if (sidebarToggle) {
        sidebarToggle.setAttribute("role", "button");
    }

    if (main && !main.querySelector(".riffdb-status")) {
        var status = document.createElement("aside");
        status.className = "riffdb-status";
        status.setAttribute("role", "note");
        status.textContent = "Proof of concept: pre-alpha compatibility. Review known limitations before production evaluation.";
        main.insertBefore(status, main.firstChild);
    }

    // Syntax highlighting for Riff (.riff), RiffQL (.riffq / .riffql), and RiffR (.riffr)
    // synced with clients/editor/tree-sitter-riff and clients/editor/tree-sitter-riffql.
    var RIFF_TYPES = new Set([
        "bool", "bytes", "date", "decimal", "i64", "list", "money", "optional",
        "string", "timestamp", "u64", "uuid", "vector",
        "Cursor", "Limit", "Set"
    ]);

    var RIFF_LITERALS = new Set([
        "true", "false", "null"
    ]);

    var RIFF_DECLARATION_KEYWORDS = new Set([
        "aggregate", "child", "columnar_projection", "command", "contract",
        "entity", "enum", "event", "field", "index", "module", "policy",
        "projection", "query", "relationship", "role", "root", "service",
        "state_machine", "unique", "vector_field", "workflow"
    ]);

    var RIFF_KEYWORDS = new Set([
        "active", "after", "aggregate", "aggregate_bytes", "all", "allow",
        "any", "apply", "approval", "as", "asc", "attempts", "available",
        "binary_utf8_v1", "bounded", "bulk", "by", "candidates", "capability",
        "cascade", "causal", "checked_by", "child", "claim", "collect",
        "collect_distinct", "columnar_projection", "command", "conflict_key",
        "conjunction", "contains", "contract", "count", "count_distinct",
        "count_distinct_present", "count_present", "cover", "create", "day",
        "decide", "default", "delete", "delete_policy", "desc", "difference",
        "disjunction", "distinct", "duration_seconds", "each", "else", "embed",
        "emit", "ends_with", "entity", "enum", "event", "exact_count",
        "exhausted", "exists", "expire", "expired", "expires_at", "fact",
        "fence", "fencing_token", "field", "first", "for", "freshness", "from",
        "frontier", "group", "having", "hour", "idempotency_key", "if",
        "ilike", "illegal", "import", "in", "include", "index",
        "inherit_session_commit", "init_or_mutate", "initial", "initialize",
        "input", "insert", "intersect", "invalid", "invariant", "is", "key",
        "last", "lease", "like", "many", "matching", "max", "max_lag_ms",
        "max_wait_ms", "maximum", "maybe", "mean", "measure", "median", "min",
        "module", "month", "mutate", "nearest", "no_effect", "no_inbound",
        "not", "not_ilike", "not_in", "not_like", "nulls",
        "observe_or_initialize", "offset", "on", "one", "order", "outcomes",
        "outbox", "overwrite", "owner", "p50", "p90", "p95", "p99",
        "partition_by", "pattern_index", "per", "phrase", "policy",
        "policy_anchor", "prefix", "presence", "principal", "projected",
        "projection", "proximity", "query", "read", "reconstitute",
        "reference", "reimport", "reject", "relationship", "release", "renew",
        "require", "restrict", "return", "reveals", "revision", "riff_bm25_v1",
        "role", "root", "row", "run", "select", "service", "set", "source",
        "stale", "starts_with", "state", "state_machine", "stddev", "sum",
        "take", "text_index", "text_key", "to", "transaction_time",
        "transactionally_ordered", "transition", "unavailable",
        "unicode_fold_v1", "union", "unique", "update", "using", "uuid_v7",
        "variance", "vector_field", "version", "week", "when", "where", "with",
        "within", "workflow", "year"
    ]);

    function escapeHtml(text) {
        return text
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function wrapToken(cls, text) {
        return '<span class="' + cls + '">' + escapeHtml(text) + "</span>";
    }

    var TOKEN_RE = new RegExp(
        [
            "(\\/\\*[\\s\\S]*?\\*\\/|\\/\\/[^\\n]*)",            // 1: block or line comment
            '("(?:[^"\\\\\\n]|\\\\.)*")',                        // 2: string literal
            "(\\$[A-Za-z_][A-Za-z0-9_]*)",                       // 3: RiffQL $parameter
            "(\\b[0-9]+(?:\\.[0-9]+|\\.\\.[0-9]+)?\\b)",         // 4: number or range
            "(\\b[A-Za-z_][A-Za-z0-9_]*\\b)",                    // 5: identifier / keyword / type
            "(->|<=|>=|==|!=|&&|\\|\\|)"                         // 6: multi-char operator
        ].join("|"),
        "g"
    );

    function highlightRiffSource(source) {
        var out = "";
        var lastIndex = 0;
        var expectTitle = false;
        TOKEN_RE.lastIndex = 0;
        var match;

        while ((match = TOKEN_RE.exec(source)) !== null) {
            if (match.index > lastIndex) {
                var between = source.slice(lastIndex, match.index);
                out += escapeHtml(between);
                if (/[^\s]/.test(between)) {
                    expectTitle = false;
                }
            }

            var token = match[0];
            if (match[1] !== undefined) {
                out += wrapToken("hljs-comment", token);
            } else if (match[2] !== undefined) {
                out += wrapToken("hljs-string", token);
                expectTitle = false;
            } else if (match[3] !== undefined) {
                out += wrapToken("hljs-variable", token);
                expectTitle = false;
            } else if (match[4] !== undefined) {
                out += wrapToken("hljs-number", token);
                expectTitle = false;
            } else if (match[5] !== undefined) {
                if (RIFF_LITERALS.has(token)) {
                    out += wrapToken("hljs-literal", token);
                    expectTitle = false;
                } else if (RIFF_TYPES.has(token)) {
                    out += wrapToken("hljs-type", token);
                    expectTitle = false;
                } else if (RIFF_KEYWORDS.has(token)) {
                    out += wrapToken("hljs-keyword", token);
                    expectTitle = RIFF_DECLARATION_KEYWORDS.has(token);
                } else if (expectTitle) {
                    out += wrapToken("hljs-title", token);
                    expectTitle = false;
                } else if (/^[A-Z]/.test(token)) {
                    out += wrapToken("hljs-type", token);
                } else {
                    out += escapeHtml(token);
                }
            } else if (match[6] !== undefined) {
                out += wrapToken("hljs-built_in", token);
                expectTitle = false;
            } else {
                out += escapeHtml(token);
            }

            lastIndex = TOKEN_RE.lastIndex;
        }

        if (lastIndex < source.length) {
            out += escapeHtml(source.slice(lastIndex));
        }

        return out;
    }

    var selector = [
        "pre code.language-riff",
        "pre code.language-riffq",
        "pre code.language-riffql",
        "pre code.language-riffr"
    ].join(", ");

    var blocks = document.querySelectorAll(selector);
    for (var i = 0; i < blocks.length; i++) {
        var block = blocks[i];
        var raw = block.textContent || "";
        block.innerHTML = highlightRiffSource(raw);
        block.classList.add("hljs");
    }
}());
