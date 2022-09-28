import { expect, test } from '@jest/globals';
import { Subzero, Statement } from '../index';

const schema = {
    "schemas":[
        {
            "name":"public",
            "objects":[
                {
                    "kind":"function",
                    "name":"myfunction",
                    "volatile":"v",
                    "composite":false,
                    "setof":true,
                    "return_type":"int4",
                    "return_type_schema":"pg_catalog",
                    "parameters":[
                        {
                            "name":"a",
                            "type":"integer",
                            "required":true,
                            "variadic":false
                        }
                    ]
                },
                {
                    "kind":"view",
                    "name":"tasks",
                    "columns":[
                        {
                            "name":"id",
                            "data_type":"int",
                            "primary_key":true
                        },
                        {
                            "name":"name",
                            "data_type":"text"
                        }
                    ],
                    "foreign_keys":[
                        {
                            "name":"project_id_fk",
                            "table":["api","tasks"],
                            "columns": ["project_id"],
                            "referenced_table":["api","projects"],
                            "referenced_columns": ["id"]
                        }
                    ]
                },
                {
                    "kind":"table",
                    "name":"projects",
                    "columns":[
                        {
                            "name":"id",
                            "data_type":"int",
                            "primary_key":true
                        }
                    ],
                    "foreign_keys":[],
                    "column_level_permissions":{
                        "role": {
                            "get": ["id","name"]
                        }
                    },
                    "row_level_permissions": {
                        "role": {
                            "get": [
                                {"single":{"field":{"name":"id"},"filter":{"op":["eq",["10","int"]]}}}
                            ]
                        }
                    }
                }
            ]
        }
    ]
};

function normalize_statement(s : Statement) {
    return {
        query: s.query.replace(/\s+/g, ' ').trim(),
        parameters: s.parameters
    };
}

const subzero = new Subzero('postgresql', schema);
test('main query', () => {
    expect(
        normalize_statement(
            subzero.get_main_query(
                "GET", // method
                "public", // schema
                "tasks", // entity
                "/tasks", // path
                [["id", "eq.1"]], // get query parameters
                undefined, // body
                [["accept", "application/json"]], // headers
                [["acookie","cookieval"]], // cookies
                [["role", "admin"],["request", '{"method":"GET"}']] // env
            )
        )
    )
    .toStrictEqual(
        normalize_statement({
            query: `
            with 
            env as materialized (select set_config($1, $2, true), $3 as "request",set_config($4, $5, true), $6 as "role")
            , _subzero_query as (
                select "public"."tasks"."id", "public"."tasks"."name" from "public"."tasks", env where "public"."tasks"."id" = $7
            )
            , _subzero_count_query AS (select 1)
            select
                pg_catalog.count(_subzero_t) as page_total,
                null::bigint as total_result_set,
                coalesce(json_agg(_subzero_t), '[]')::character varying as body,
                true as constraints_satisfied,
                nullif(current_setting('response.headers', true), '') as response_headers,
                nullif(current_setting('response.status', true), '') as response_status
            from ( select * from _subzero_query ) _subzero_t
            `,
            parameters: ["request"
                , '{"method":"GET"}'
                , '{"method":"GET"}'
                , 'role'
                , 'admin'
                , 'admin'
                , "1"
            ]
        })
    );
});

test('core query', () => {
    expect(
        normalize_statement(
            subzero.get_core_query(
                "GET", // method
                "public", // schema
                "tasks", // entity
                "/tasks", // path
                [["id", "eq.1"]], // get query parameters
                undefined, // body
                [["accept", "application/json"]] // headers
            )
        )
    )
        .toStrictEqual(
            normalize_statement({
            query: `select "public"."tasks"."id", "public"."tasks"."name" from "public"."tasks" where "public"."tasks"."id" = $1`,
            parameters: ["1"]
            })
    );
});

