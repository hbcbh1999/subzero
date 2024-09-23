# WIP: pg_rest

This extension allows converting http requests to queries and executing them from within PostgreSQL.
This might be desirable when you want to have a very simple http proxie that forward the execution of the request to the database.

## Usage (once the extension is installed)

```sql
create extension pg_rest;

select
    body,
    status,
    headers,
    page_total,
    total_result_set
from
    rest.handle(
        row(
            'GET',                            -- method
            '/api/users',                     -- path
            'select=id,name&id=eq.2',         -- query_string
            null::bytea,                      -- body
            array[
                row('Content-Type', 'application/json')::rest.http_header,
                row('Authorization', 'Bearer your_token')::rest.http_header
            ]::rest.http_header[]             -- headers
        )::rest.http_request,
        '
        {
            "schema": "public",
            "env": {
                "role": "alice",
                "user_id": 2
            },
            "path_prefix": "/api/",
            "max_rows": 100,
            "schemas": "public,api",
            "allow_login_roles": false,
            "custom_relations": null,
            "custom_permissions": null
        }
        '
    );
```