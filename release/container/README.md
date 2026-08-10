# Remote container alpha

This directory is a release-derived direct-TLS deployment example. Build the
two release binaries first, build `Dockerfile`, and place protected runtime
material in a private directory selected by `RIFFDB_CONTAINER_ROOT`. Set
`RIFFDB_CONTAINER_UID` and `RIFFDB_CONTAINER_GID` to the owner of that private
directory (the acceptance script uses the invoking user). The
certificate must contain `riffdbd` as a DNS SAN. Copy the three TOML templates
into that directory and keep credentials and private keys mode `0600`.

`riffdbd` alone mounts the database, backup volume, digest keys, and TLS private
key. `application-probe` mounts only its application credential, client
configuration, and CA. `operator-probe` mounts a distinct operator credential.
The HAProxy sibling performs TCP pass-through and cannot assert a RiffDB
principal, database, or tenant.

Bring up the database and proxy, perform the one-time bootstrap from the
database container's own loopback namespace into the protected `bootstrap/`
handoff directory, and then use that operator credential through the TLS proxy
to create the application credential. Remote bootstrap remains forbidden; the
application and every post-bootstrap operator operation use the network path.
Enable the two one-shot proof profiles after provisioning:

```bash
docker compose -f release/container/compose.yaml up -d riffdbd riffdb-proxy
docker compose -f release/container/compose.yaml --profile operator run --rm operator-probe
docker compose -f release/container/compose.yaml --profile application run --rm application-probe
```

The database healthcheck is deliberately unauthenticated liveness. Routing
readiness is the authenticated, selected-database probe executed by the
operator and application containers. Replace certificate/key and client CA
files atomically; do not overwrite them in place. Stop with the configured
grace period so the server can complete its bounded drain.

This alpha example is for controlled design-partner networks. It does not add
per-principal rate limits, tenant storage quotas, mTLS, or public certificate
automation.
