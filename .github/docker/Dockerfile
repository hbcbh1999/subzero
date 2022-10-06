FROM gcr.io/distroless/cc:latest

ARG TARGETARCH

ADD $TARGETARCH/subzero /
ADD introspection/postgresql_introspection_query.sql /postgresql_introspection_query.sql
ADD introspection/sqlite_introspection_query.sql /sqlite_introspection_query.sql
ADD introspection/clickhouse_introspection_query.sql /sqlite_introspection_query.sql

ENTRYPOINT ["/subzero"]
