use criterion::{black_box, criterion_group, criterion_main, Criterion};
#[macro_use] extern crate lazy_static;
use subzero::parser::postgrest::parse;
use subzero::formatter::postgresql::format;
use subzero::dynamic_statement::{generate};
use subzero::api::*;
use subzero::schema::*;

pub static JSON_SCHEMA:&str = 
                r#"
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

    lazy_static!{
        
        static ref PARAMETERS: Vec<(&'static str, &'static str)> = { vec![
            ("select", "id,name,clients(id),tasks(id)"),
            ("id","not.gt.10"),
            ("tasks.id","lt.500"),
            ("not.or", "(id.eq.11,id.eq.12)"),
            ("tasks.or", "(id.eq.11,id.eq.12)"),
    
        ]};

        static ref DB_SCHEMA:DbSchema = {
            serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap()
        };

        static ref REQUEST:ApiRequest<'static> = {
            parse(&s("api"), &s("projects"), &DB_SCHEMA, &Method::GET, PARAMETERS.to_vec(), None).unwrap()
        };
    }
fn s(s:&str) -> String {
    s.to_string()
}

fn criterion_benchmark(c: &mut Criterion) {
    

    c.bench_function("parse request", |b| b.iter(|| 
        parse(black_box(&s("api")), black_box(&s("projects")), black_box(&DB_SCHEMA), black_box(&Method::GET), black_box(PARAMETERS.to_vec()), black_box(None))
    ));

    c.bench_function("generate query & prepare statement", |b| b.iter(|| 
        generate(format(black_box(&s("api")), black_box(&REQUEST.query)))
    ));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);