# docker build --build-arg BACKEND=postgresql -t subzero-postgresql .
# docker build --build-arg BACKEND=sqlite -t subzero-sqlite .

ARG BACKEND="postgresql"

# FROM rust:1.59 as builder
FROM rustlang/rust:nightly as builder
ARG BACKEND
WORKDIR /usr/src/subzero
# RUN rustup show
# COPY dummy.rs .
# COPY Cargo.toml .
# RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
# RUN cargo build --features ${BACKEND} --release
# RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
# COPY . .
# RUN cargo install --features ${BACKEND} --path .
COPY . .
RUN cargo build --features postgresql --release

FROM debian:buster-slim
ARG BACKEND
RUN apt-get update && \
    # apt-get install -y extra-runtime-dependencies && \
    rm -rf /var/lib/apt/lists/*
# COPY --from=builder /usr/local/cargo/bin/subzero-${BACKEND} /usr/local/bin/subzero
COPY --from=builder /usr/src/subzero/target/release/subzero-${BACKEND} /usr/local/bin/subzero
CMD ["subzero"]