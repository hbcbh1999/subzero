// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[macro_use]
extern crate lazy_static;
use std::collections::HashMap;
// use subzero_core::api::*;
//use subzero_core::dynamic_statement::generate;
use subzero_core::formatter::postgresql::{generate, fmt_main_query};
use subzero_core::parser::postgrest::parse;
use subzero_core::schema::*;

pub static JSON_SCHEMA: &str = r#"
                    {
                        "schemas":[
                            {
                                "name":"api",
                                "objects":[
                                    {
                                        "kind":"view",
                                        "name":"addresses",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"location", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"users",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" },
                                            { "name":"billing_address_id", "data_type":"int" },
                                            { "name":"shipping_address_id", "data_type":"int" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"billing_address_id_fk",
                                                "table":["api","users"],
                                                "columns": ["billing_address_id"],
                                                "referenced_table":["api","addresses"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"shipping_address_id_fk",
                                                "table":["api","users"],
                                                "columns": ["shipping_address_id"],
                                                "referenced_table":["api","addresses"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"clients",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"projects",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"client_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"client_id_fk",
                                                "table":["api","projects"],
                                                "columns": ["client_id"],
                                                "referenced_table":["api","clients"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"tasks",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"project_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
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
                                        "kind":"view",
                                        "name":"users_tasks",
                                        "columns":[
                                            { "name":"task_id", "data_type":"int", "primary_key":true },
                                            { "name":"user_id", "data_type":"int", "primary_key":true }
                                            
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"task_id_fk",
                                                "table":["api","users_tasks"],
                                                "columns": ["task_id"],
                                                "referenced_table":["api","tasks"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"user_id_fk",
                                                "table":["api","users_tasks"],
                                                "columns": ["user_id"],
                                                "referenced_table":["api","users"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                "#;

lazy_static! {
    static ref PARAMETERS: Vec<(&'static str, &'static str)> = {
        vec![
            ("select", "id,name,clients(id),tasks(id)"),
            ("id", "not.gt.10"),
            ("tasks.id", "lt.500"),
            ("not.or", "(id.eq.11,id.eq.12)"),
            ("tasks.or", "(id.eq.11,id.eq.12)"),
        ]
    };
    static ref DB_SCHEMA: DbSchema<'static> = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let emtpy_hashmap0 = HashMap::new();
    let emtpy_hashmap1 = HashMap::new();
    let emtpy_hashmap2 = HashMap::new();

    let request = parse("api", "projects", &DB_SCHEMA, "GET", "/projects", PARAMETERS.to_vec(), None, emtpy_hashmap1, emtpy_hashmap2, None).unwrap();
    c.bench_function("parse request", |b| {
        b.iter(|| {
            parse(
                black_box("api"),
                black_box("projects"),
                black_box(&DB_SCHEMA),
                black_box("GET"),
                black_box("/projects"),
                black_box(PARAMETERS.to_vec()),
                black_box(None),
                HashMap::new(),
                HashMap::new(),
                None,
            )
        })
    });

    c.bench_function("generate query & prepare statement", |b| {
        b.iter(|| {
            let q = fmt_main_query(black_box(&DB_SCHEMA), black_box("api"), black_box(&request), &emtpy_hashmap0).unwrap();
            generate(q)
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
