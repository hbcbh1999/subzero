*subZero* is a fast, Rust-powered library designed to simplify building REST APIs and backend services. It generalizes the concepts found in tools like PostgREST, PostGraphile, and Hasura, with the aim to support multiple REST/GraphQL flavors like PostgREST, OData, Hasura and various databases on the backend (PostgreSQL, SQLite, MySQL, Clickhouse).

In contrast to PostgREST/Hasura, subZero is designed as a library, not just an executable. This allows you to integrate it into your backend (Rust, Node, Workers, C, Java), offering the fast turnaround time of PostgREST without sacrificing the flexibility of a custom backend.

The directory structure is as follows:

- [core](core/README.md): The core library that contains the AST, parser, and query builder.
- [rocket](rocket/README.md): The code to create an executable that exposes a web server based on the Rocket framework.
- [java-bindings](java-bindings/README.md): The Java bindings for subZero.
- [ffi](ffi/README.md): The code to build and expose a C shared library that exposes the low-level subZero functionality.
- [js-bindings](js-bindings/README.md): The JavaScript/TypeScript bindings for subZero.


The compiled library is licensed under [LGPLv3 license](http://www.gnu.org/licenses/lgpl-3.0.html).

The source code is licensed under [AGPLv3 license](http://www.gnu.org/licenses/agpl-3.0.html)