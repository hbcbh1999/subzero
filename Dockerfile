# docker build --build-arg BACKEND=postgresql -t subzero-postgresql .
# docker build --build-arg BACKEND=sqlite -t subzero-sqlite .

ARG BACKEND="postgresql"

# FROM rust:1.59 as builder
FROM rustlang/rust:nightly as builder
ARG BACKEND
WORKDIR /usr/src/subzero
COPY . .
RUN cargo build --features ${BACKEND} --release

FROM debian:buster-slim
ARG BACKEND
RUN apt-get update && \
    apt-get install -y openssl && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/subzero/target/release/subzero /usr/local/bin/subzero
COPY --from=builder /usr/src/subzero/postgresql_structure_query.sql /postgresql_structure_query.sql
COPY --from=builder /usr/src/subzero/sqlite_structure_query.sql /sqlite_structure_query.sql
EXPOSE 8000
CMD ["subzero"]