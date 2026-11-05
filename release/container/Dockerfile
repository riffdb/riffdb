FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c

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
