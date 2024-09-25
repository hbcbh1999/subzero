# build with support for all backends
# docker build -t subzero .

# build with support for specific backends
# docker build --build-arg FEATURES="postgresql,sqlite" -t subzero .

ARG FEATURES="all"

FROM rustlang/rust:nightly AS builder
ARG FEATURES
WORKDIR /usr/src/subzero
COPY . .
RUN cargo build --package subzero-rocket --no-default-features --features ${FEATURES} --release

FROM debian:bookworm-slim
ARG FEATURES
ENV SUBZERO_ADDRESS="0.0.0.0"
RUN apt-get update && \
    apt-get install -y openssl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/subzero/target/release/subzero /usr/local/bin/subzero
COPY --from=builder /usr/src/subzero/introspection/postgresql_introspection_query.sql /postgresql_introspection_query.sql
COPY --from=builder /usr/src/subzero/introspection/sqlite_introspection_query.sql /sqlite_introspection_query.sql
COPY --from=builder /usr/src/subzero/introspection/clickhouse_introspection_query.sql /sqlite_introspection_query.sql
EXPOSE 8000
CMD ["subzero"]