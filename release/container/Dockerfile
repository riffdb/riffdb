FROM ubuntu:24.10@sha256:cdf755952ed117f6126ff4e65810bf93767d4c38f5c7185b50ec1f1078b464cc

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
