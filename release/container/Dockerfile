FROM ubuntu:24.04@sha256:561618e2c15bf2397621dd04f96926663a3b5616c189cf7e38db7e82f5c538ea

ARG RIFFDB_PROFILE=release

RUN groupadd --gid 65532 riffdb \
    && useradd --uid 65532 --gid 65532 --no-create-home --shell /usr/sbin/nologin riffdb

COPY target/${RIFFDB_PROFILE}/riffdb /usr/local/bin/riffdb
COPY target/${RIFFDB_PROFILE}/riffdbd /usr/local/bin/riffdbd
COPY release/container/copy-protected /usr/local/bin/copy-protected

# Cargo preserves the creating process's umask in target artifacts. Normalize
# the two copied release binaries so a restrictive host umask cannot produce
# an image that the fixed non-root runtime identity is unable to execute.
RUN chmod 0555 /usr/local/bin/riffdb /usr/local/bin/riffdbd

USER 65532:65532
WORKDIR /var/lib/riffdb
ENTRYPOINT []
CMD ["riffdbd", "--config", "/etc/riffdb/riffdbd.toml"]
