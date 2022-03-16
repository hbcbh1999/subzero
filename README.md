run tests
cargo test --features postgresql -- --test-threads=1


build
cargo build --features sqlite --release
cargo build --features postgresql --release


load database local temporary databse
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 3600) && psql -f tests/postgrest/fixtures/load.sql $url
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 3600) && psql -f demo/db/pg_init.sql $url


run agains a local database
SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE={sql_file=postgresql_structure_query.sql} \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE=postgrest_test_authenticator \
SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
cargo run --features=postgresql --bin subzero-postgresql  --release


export SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE={sql_file=postgresql_structure_query.sql} \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE=postgrest_test_authenticator \
SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
&& cargo flamegraph --features=postgresql --root --bin subzero-postgresql