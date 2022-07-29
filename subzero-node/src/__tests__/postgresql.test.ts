import { expect, test } from '@jest/globals';
import { Subzero } from '../index';

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

function normalize_statement(p: [query: string, parameters: any]) {
    return [p[0].replace(/\s+/g, ' ').trim(), p[1]];
}

const subzero = new Subzero('postgresql', schema);
test('basic', () => {
    expect(
        normalize_statement(subzero.get_main_query("GET", "public", "tasks", "/tasks", [["id", "eq.1"]], undefined, new Map(), new Map()))
    )
    .toStrictEqual(
        normalize_statement([
            `
            with _subzero_query as (
                select "public"."tasks".* from "public"."tasks"   where "public"."tasks"."id" = $1
            )
            , _subzero_count_query AS (select 1)
            select
                pg_catalog.count(_subzero_t) as page_total,
                null::bigint as total_result_set,
                coalesce(json_agg(_subzero_t), '[]')::character varying as body,
                nullif(current_setting('response.headers', true), '') as response_headers,
                nullif(current_setting('response.status', true), '') as response_status
            from ( select * from _subzero_query ) _subzero_t
            `,
            ["1"]
        ])
    );
});

