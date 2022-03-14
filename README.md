load database
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 3600) && psql -f tests/postgrest/fixtures/load.sql $url
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 3600) && psql -f demo/db/pg_init.sql $url


SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS="[test]" \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE="postgrest_test_anonymous" \
cargo run

SUBZERO_LOG_LEVEL=debug cargo test --features postgresql -- --test-threads=1

cargo build --features sqlite --release --target=x86_64-unknown-linux-musl
cargo build --features postgresql --release --target=x86_64-unknown-linux-musl

SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE={sql_file=postgresql_structure_query.sql} \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE=postgrest_test_authenticator \
SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
cargo run --features=postgresql --release --bin subzero-postgresql

docker run -p 3000:3000 \
-env PGRST_DB_URI=$url \
-env PGRST_DB_SCHEMA=public \
-env PGRST_DB_ANON_ROLE=postgrest_test_authenticator \
postgrest/postgrest

docker run --rm -p 3000:3000 \
-e PGRST_DB_URI=postgresql://postgrest_test_authenticator@host.docker.internal:54215/test \
-e PGRST_DB_SCHEMA="public" \
-e PGRST_DB_ANON_ROLE="postgrest_test_authenticator" \
postgrest/postgrest


PGRST_DB_SCHEMAS="public" \
PGRST_DB_ANON_ROLE="postgrest_test_authenticator" \
PGRST_DB_URI=$url \
./postgrest
