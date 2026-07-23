# Client Configuration v1

The effective configuration has exactly four fields. Per-field precedence is:
explicit flag, named environment variable, explicit TOML, built-in default.
A present empty or invalid higher-precedence value rejects and never falls
through.

| Field | Flag | Environment | TOML | Default |
|---|---|---|---|---|
| Endpoint | `--endpoint` | `RIFFDB_ENDPOINT` | `client.endpoint` | `http://127.0.0.1:7443` |
| Output | `--output` | `RIFFDB_OUTPUT` | `client.output` | `human` |
| Attempts | `--max-attempts` | `RIFFDB_MAX_ATTEMPTS` | `client.max_attempts` | `3` |
| Credential file | `--credential-file` | `RIFFDB_CREDENTIAL_FILE` | `client.credential_file` | absent |

The TOML selector is only `--config`, then `RIFFDB_CONFIG`, then absent. It is
not an effective field and has no discovery default. `--config` and
`RIFFDB_CONFIG` are paths, not inline TOML.

Closed bounds and validation:

- TOML is complete UTF-8 at most 65,536 bytes, parsed through EOF.
- The sole optional top-level item is `[client]`; its only optional keys are
  `endpoint`, `output`, `max_attempts`, and `credential_file`.
- Unknown/duplicate tables or keys, wrong types, empty selected values, raw
  credentials, and bootstrap documents reject.
- Endpoint is at most 512 ASCII bytes and exactly lowercase
  `http://<literal-loopback-IP>:<port-1..65535>` with no normalization.
- Output is exactly `human` or `json`.
- Attempts is canonical decimal `1..=10`; TOML stores it as an integer.
- Every path is nonempty, NUL-free, and at most 4,096 platform bytes. TOML paths
  are UTF-8; argv/environment paths may use platform encoding.
- Exactly one normal credential source may resolve: the protected credential
  file or `RIFFDB_CAPABILITY_TOKEN`. The environment token is exactly 43 bytes.
- Only `RIFFDB_CONFIG`, `RIFFDB_ENDPOINT`, `RIFFDB_OUTPUT`,
  `RIFFDB_MAX_ATTEMPTS`, `RIFFDB_CREDENTIAL_FILE`, and
  `RIFFDB_CAPABILITY_TOKEN` are read. Other `RIFFDB_*` names are ignored.

`client-complete.toml` is the complete representative valid document,
`client-minimal.toml` selects all defaults, and `client-ipv6-json.toml` freezes
an explicit loopback IPv6/machine-output configuration. An empty document is
also valid and selects all defaults.
