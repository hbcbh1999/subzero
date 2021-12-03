load database
export url=$(tests/bin/pg_tmp.sh -t -u postgrest_test_authenticator -w 300) && psql -f tests/postgrest/fixtures/load.sql $url


SUBZERO_DB_URI=$url SUBZERO_DB_SCHEMAS="[test]" cargo run --release

cargo test -- --test-threads=1