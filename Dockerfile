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
    # apt-get install -y extra-runtime-dependencies && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/subzero/target/release/subzero-${BACKEND} /usr/local/bin/subzero
COPY --from=builder /usr/src/subzero/${BACKEND}_structure_query.sql /structure_query.sql
EXPOSE 8000
CMD ["subzero"]