#WARNING! outdated

## update version for all cargo packages
needs `cargo install cargo-edit`
```
cargo set-version 0.1.1
```

```
npm version 0.1.1
```

## Start a demo in docker
This will also build the images locally
```
docker-compose up -d
```

## Build native binary
For production add `--release` flag at the end
```
cargo build
```

## Run tests
```
cargo test -- --test-threads=1
```

## Build docker images
```
docker build -t subzero .
```

## Create temporary database

```
export url=$(rocket/tests/bin/pg_tmp.sh -t -u authenticator -w 3600) && psql -f demo/db/pg_init.sql $url
```
```
export url=$(rocket/tests/bin/pg_tmp.sh -k -t -u postgrest_test_authenticator -w 3600) && psql -f rocket/tests/postgresql/fixtures/load.sql $url
```


## Run agains a local database

```
SUBZERO_DB_URI=$url cargo run
```

