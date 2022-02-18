load database
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 3600) && psql -f tests/postgrest/fixtures/load.sql $url


SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS="[test]" \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE="postgrest_test_anonymous" \
cargo run

cargo test --features postgresql -- --test-threads=1