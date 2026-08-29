# Vector Search

RiffDB vector search is a named, bounded RiffQL read over authoritative
application-supplied embeddings. The application computes embeddings; RiffDB
validates and stores them through compiled commands, tracks when they become
stale, and maintains a derived per-organization search projection.

The application never selects an index implementation. A contract may declare
an approximate-search threshold and recall floor; RiffDB then uses exact search
for small admitted partitions and first-party HNSW only when the admitted row
count is above the threshold.

## Declare the vector field

```riff
entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<256>
    field body: string<4096>
    vector_field embedding(1536, cosine, (title, body), staleness_slo 60,
        model "text-embedding-v1", current_version "2026-08-21",
        replay_age_seconds 86400, replay_bytes 1073741824,
        replay_backlog 100000,
        ann_threshold 256, recall_target_bps 9500)
}
```

`ann_threshold` and `recall_target_bps` are an atomic pair. In this example,
search is exact at 256 or fewer admitted rows in the requested organization and
uses approximate search above 256. The declared 9,500 basis points means
recall@K must be at least 0.95 against exact search at the same frontier. Query
text and parameters cannot lower that target or force a tier.

The source fields `(title, body)` declare what makes the embedding stale. The
model identity and current version are also contract facts; a caller cannot
write a differently identified embedding and claim it is current.

## Write embeddings through a command

```riff
command SetDocumentEmbedding {
    input request_id: string<128>
    input organization_id: uuid
    input document_id: uuid
    input embedding: vector<1536>
    input submitted_model: string<256>
    input submitted_version: string<256>

    idempotency_key request_id
    mutate Document(organization_id, document_id) as document else Missing {}
    embed document.embedding = embedding
        from (submitted_model, submitted_version)
    return Embedded { document: document }
}
```

The `embed` effect commits the vector and its model/version/write-sequence
evidence atomically with the command outcome, provenance, and idempotency
record. Generated clients provide a field-specific constructor using the
contract-sealed model identity and version. Generic `set` cannot mutate a
vector field.

Changing a declared source field without a later embedding command makes that
row stale. Generated clients expose bounded symbolic stale-row and model-version
inspection when the application role declares the corresponding authority.

## Read through a named RiffQL operation

```riffql
query SimilarDocuments(
    $organization_id: Document.organization_id,
    $query_vector: Document.embedding,
    $k: Limit<499>,
) {
    source projected Document.embedding
    freshness causal inherit_session_commit true max_wait_ms 500

    many documents from Document
        where organization_id == $organization_id
        nearest(embedding, $query_vector, $k)

    return Found {
        documents: documents {
            document_id
            title
        }
    }
    outcomes Found
}
```

The organization equality is mandatory, K is positive and compiler-bounded,
and the source must name the same vector field used by `nearest`. Row policy is
applied to the complete candidate set before vector validation, graph
construction, scoring, ranking, or K selection. Another organization and a
policy-denied row cannot shape graph statistics or results.

`freshness causal` is the normal read-after-write choice. `available` permits a
currently published frontier, while duration-bounded freshness uses trusted
commit timestamps. Every successful projected response reports its frontier.

## Correct typed failures

- `RDB-QP002` means the query lacks a provable single partition. Add the
  declared organization equality; do not fetch broadly and filter in the app.
- `RDB-PROJECTION-0104` means the named query lacks the exact compiler-owned
  `source projected Entity.vector_field` identity. Refresh and deploy the
  query module rather than selecting a storage index manually.
- `RDB-PROJECTION-0103` means no published generation satisfies the declared
  freshness floor. Retry according to the typed outcome; do not silently
  weaken freshness.
- A vector dimension or model/version mismatch is an input error naming the
  symbolic field or input path. Regenerate or use the field-specific generated
  constructor; do not encode vectors as bytes.
- Building, rebuilding, or detached projection state is unavailable until a
  complete checkpoint is published. RiffDB never serves a partial graph or an
  old generation as if it were current.

## POC bounds

Production nearest reads admit at most 500 rows from one organization. Because
the approximate tier engages only above the declared threshold, a threshold of
500 or more remains exact under this POC ceiling. The HNSW graph is derived and
rebuilt ephemerally per bounded query; it is not part of backup identity.
Persistent incremental graphs are deliberately deferred.

For the full declarations and query grammar, see
[Contract Authoring](../contracts/AUTHORING.md) and
[RiffQL](../riffql/LANGUAGE.md#nearest-neighbor-bindings-alpha).
