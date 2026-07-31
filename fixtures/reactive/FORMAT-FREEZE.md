# P8 reactive format freeze

WP-414 freezes the owners and successor versions before production artifacts
exist. Exact binary and JSON golden bytes are added by their owning package.

| Artifact | Owner | Successor introduced by |
|---|---|---|
| Event partition syntax and IR | contract syntax / contract IR | WP-415 |
| Event route key and record | storage API | WP-415 |
| Safe symbolic event envelope | service | WP-415 |
| Reactive module and plan | query module / query IR | WP-416 |
| Application Source V4 / Lock V5 | query module | WP-416 |
| Reactive role permissions | types / policy | WP-416 |
| Consumer checkpoint, lease, ack, dead letter | storage API | WP-417 |
| Consumer public protocol | service / public Proto | WP-417 |
| Live cursor and update protocol | service / public Proto | WP-418 |
| Generated reactive bindings and MCP schemas | query module / MCP | WP-419 |
| Causation provenance successor | storage API | WP-420 |
| Contextual work and causation token | service | WP-420 |

Existing `StoredDurableEventV1`, `EventId`, event hashes, commits, projection and
outbox records, application source V1-V3, locks V1-V4, and kernel commit
subscription messages are compatibility inputs and are not repurposed.
