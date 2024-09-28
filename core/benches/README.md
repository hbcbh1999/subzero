## benchmark the core functionality of the library
```
cargo bench --features=postgresql
```

## bench results on a mac air m1
```
parse request           time:   [17.457 µs 17.474 µs 17.489 µs]
                        change: [-0.4172% -0.2562% -0.0880%] (p = 0.00 < 0.05)
                        Change within noise threshold.

generate query & prepare statement
                        time:   [21.485 µs 21.520 µs 21.564 µs]
                        change: [+0.3433% +0.6913% +1.0429%] (p = 0.00 < 0.05)
                        Change within noise threshold.
```

build the docker image for benchmarking (in the root directory)
```
docker build --build-arg FEATURES="postgresql" -t subzero .
```

bring up the docker containers
```
docker compose up -d
```

docker stats after the containers are up
```
CONTAINER ID   NAME                  CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           BLOCK I/O        PIDS
7b3508427347   benches-subzero-1     0.00%     2.504MiB / 5.788GiB   0.04%     3.98kB / 36.4kB   0B / 0B          7
47fb25db5fc8   benches-postgrest-1   0.09%     21.14MiB / 5.788GiB   0.36%     44.5kB / 33.7kB   0B / 0B          17
64c31f17579e   benches-db-1          0.05%     46.89MiB / 5.788GiB   0.79%     71.7kB / 45.3kB   4.1kB / 52.6MB   9
```

## stats under load for postgrest
```
hey -z 1m  -T "application/json" "http://localhost:3000/tasks?select=id&id=eq.1"
```
```
Summary:
  Total:	60.0092 secs
  Slowest:	0.1945 secs
  Fastest:	0.0008 secs
  Average:	0.0103 secs
  Requests/sec:	4871.7360
Status code distribution:
  [200]	292349 responses
```
```
CONTAINER ID   NAME                  CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           BLOCK I/O        PIDS
47fb25db5fc8   benches-postgrest-1   326.40%   85.39MiB / 5.788GiB   1.44%     243MB / 178MB     0B / 0B          40
64c31f17579e   benches-db-1          130.28%   59.18MiB / 5.788GiB   1.00%     118MB / 192MB     4.1kB / 53.5MB   18
```

## stats under load for subzero
```
hey -z 1m  -T "application/json" "http://localhost:8000/tasks?select=id&id=eq.1"
```
```
Summary:
  Total:	60.0082 secs
  Slowest:	0.1067 secs
  Fastest:	0.0019 secs
  Average:	0.0079 secs
  Requests/sec:	6324.8174
Status code distribution:
  [200]	379541 responses
```

```
CONTAINER ID   NAME                  CPU %     MEM USAGE / LIMIT     MEM %     NET I/O         BLOCK I/O        PIDS
7b3508427347   benches-subzero-1     249.87%   10.87MiB / 5.788GiB   0.18%     167MB / 249MB   0B / 0B          7
64c31f17579e   benches-db-1          211.42%   75.39MiB / 5.788GiB   1.27%     380MB / 421MB   4.1kB / 54.4MB   27
```

## takeaways
while the test is not rigurous (db is on the same machine), and is irelevant for tipical deployments,
it still provides some insights.

subzero uses less CPU under load and still has a better throughput (+30%).
memory usage is the interesting part, a 10X reduction in memory usage:
 - 2MB vs 20MB at startup
 - 10MB vs 85MB under load (with the same request) 

