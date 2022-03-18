## Start a demo in docker
This will also build the images locally
```
docker-compose up -d
```

## Run tests
```
cargo test --features postgresql -- --test-threads=1
```

```
cargo test --features sqlite -- --test-threads=1
```


## Build native binary
For production add `--release` flag at the end
```
cargo build --features sqlite
```

```
cargo build --features postgresql
```

## Build docker images
```
docker build --build-arg BACKEND=postgresql -t subzero-postgresql .
```

```
docker build --build-arg BACKEND=sqlite -t subzero-sqlite .
```



## Create temporary database

```
export url=$(tests/bin/pg_tmp.sh -t -u authenticator -w 3600) && psql -f demo/db/pg_init.sql $url
```


## Run agains a local database

```
SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE={sql_file=postgresql_structure_query.sql} \
SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE=authenticator \
SUBZERO_VHOSTS__DEFAULT__DB_URI=$url \
cargo run --features=postgresql --bin subzero-postgresql
```

