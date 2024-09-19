This folder contains the code to create an executable that exposes a web server based on [Rocket](https://rocket.rs/) framework.

The result is functionaly equivalent to the PostgREST executable. This executable is built and packaged as a Docker image in the [Dockerfile](../Dockerfile).

Differences from PostgREST:
- 6-10x faster with lower resource usage
- Supports multiple databases (PostgreSQL, MySQL, SQLite, Clickhouse)
- You can add your custom middleware, endpoints, etc. (for now you'd need to fork the repo and modify the in this folder. In the future we plan to have a higher level lib/crate to be used in your own Rocket/Actix/Axum application)
- You can add your custom authentication/authorization/validation logic
- A more advanced `select` parameter that allows for [function calls](https://docs.subzero.cloud/reference/data/read/#calling-functions)
- Support for [analytical queries](https://docs.subzero.cloud/reference/data/aggregate/) (Aggregates, Group By, Window Functions, etc.)
- The introspection query is external which allows for more flexibility in the introspection process.
- spread operator (`...`) not yet supported

