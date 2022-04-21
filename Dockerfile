# build with support for all backends
# docker build -t subzero .

# build with support for specific backends
# docker build --build-arg FEATURES="postgresql,sqlite" -t subzero .

ARG FEATURES="all"

FROM rustlang/rust:nightly as builder
ARG FEATURES
WORKDIR /usr/src/subzero
COPY . .
RUN cargo build --features ${FEATURES} --release

FROM debian:buster-slim
ARG FEATURES
RUN apt-get update && \
    apt-get install -y openssl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/subzero/target/release/subzero /usr/local/bin/subzero
COPY --from=builder /usr/src/subzero/postgresql_structure_query.sql /postgresql_structure_query.sql
COPY --from=builder /usr/src/subzero/sqlite_structure_query.sql /sqlite_structure_query.sql
EXPOSE 8000
CMD ["subzero"]