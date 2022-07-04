//use core::fmt;

use super::base::{
    cast_select_item_format, fmt_as, fmt_body, fmt_condition, fmt_condition_tree, fmt_count_query, fmt_field, fmt_field_format, fmt_filter,
    fmt_identity, fmt_in_filter, fmt_json_operand, fmt_json_operation, fmt_json_path, fmt_limit, fmt_logic_operator, fmt_main_query, fmt_offset,
    fmt_operator, fmt_order, fmt_order_term, fmt_groupby, fmt_groupby_term, fmt_qi, fmt_query, fmt_select_item, fmt_select_name, fmt_sub_select_item, return_representation,
    simple_select_item_format, star_select_item_format, fmt_function_param, fmt_select_item_function,
};
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet};
use crate::error::Result;
//use clickhouse::{Client,sql::Bind};
//use bytes::{BufMut, BytesMut};
//use postgres_types::{to_sql_checked, Format, IsNull, ToSql, Type};
// use postgres_types::{ToSql};
//use std::error::Error;
#[derive(Debug)]
pub enum Param<'a> {
    LV(&'a ListVal),
    SV(&'a SingleVal),
    PL(&'a Payload),
}
// helper type aliases
pub trait ToSql {
    fn to_param(&self) -> Param;
    fn to_data_type(&self) -> &Option<String>;
}

pub type SqlParam<'a> = (dyn ToSql + Sync + 'a);
pub type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;


impl ToSql for ListVal {
    fn to_param(&self) -> Param {Param::LV(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl ToSql for SingleVal {
    fn to_param(&self) -> Param {Param::SV(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl ToSql for Payload {
    fn to_param(&self) -> Param {Param::PL(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}




fmt_main_query!();
fmt_query!();
fmt_count_query!();
fmt_body!();
fmt_condition_tree!();
fmt_condition!();
fmt_filter!();
fmt_select_name!();
fmt_select_item!();
fmt_select_item_function!();
fmt_function_param!();
fmt_sub_select_item!();
fmt_operator!();
fmt_logic_operator!();
fmt_identity!();
fmt_qi!();
fmt_field!();
fmt_order!();
fmt_order_term!();
fmt_groupby!();
fmt_groupby_term!();
fmt_as!();
fmt_limit!();
fmt_offset!();
fmt_json_path!();
fmt_json_operation!();
fmt_json_operand!();


#[cfg(test)]
mod tests {
    use crate::dynamic_statement::{generate_fn, SqlSnippet, SqlSnippetChunk, param_placeholder_format};
    use pretty_assertions::assert_eq;
    use regex::Regex;
    //use crate::api::{SelectItem::*};
    //use crate::api::LogicOperator::*;
    //use crate::api::{Condition::*, Filter::*};
    // use combine::stream::PointerOffset;
    // use combine::easy::{Error, Errors};
    // //use combine::error::StringStreamError;
    // use crate::error::Error as AppError;
    // use combine::EasyParser;
    use super::*;
    //use crate::parser::subzero::tests::{JSON_SCHEMA};
    generate_fn!();
    fn s(s: &str) -> String { s.to_string() }

    // #[test]
    // fn test_fmt_function_query() {
    //     let payload = r#"{"id":"10"}"#.to_string();
    //     let q = Query {
    //         node: FunctionCall {
    //             fn_name: Qi(s("api"), s("myfunction")),
    //             parameters: CallParams::KeyParams(vec![ProcParam {
    //                 name: s("id"),
    //                 type_: s("integer"),
    //                 required: true,
    //                 variadic: false,
    //             }]),
    //             payload: Payload(payload.clone(),None),
    //             is_scalar: true,
    //             returns_single: false,
    //             is_multiple_call: false,
    //             returning: vec![s("*")],
    //             select: vec![Star],
    //             where_: ConditionTree {
    //                 operator: And,
    //                 conditions: vec![],
    //             },
    //             return_table_type: None,
    //             limit: None,
    //             offset: None,
    //             order: vec![],
    //         },
    //         sub_selects: vec![],
    //     };

    //     let (query_str, parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None).unwrap());
    //     let p = Payload(payload,None);
    //     let pp: Vec<&(dyn ToSql + Sync)> = vec![&p];
    //     assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
    //     let re = Regex::new(r"\s+").unwrap();
    //     assert_eq!(
    //         re.replace_all(query_str.as_str(), " "),
    //         re.replace_all(
    //             r#"
    //             with
    //                 subzero_payload as ( select $1::json as json_data ),
    //                 subzero_body as (
    //                     select 
    //                         case when json_typeof(json_data) = 'array' 
    //                         then json_data 
    //                         else json_build_array(json_data) 
    //                         end as val 
    //                     from subzero_payload
    //                 ),
    //                 subzero_args as (
    //                     select * 
    //                     from json_to_recordset((select val from subzero_body)) as _("id" integer)
    //                 ),
    //                 subzero_source as (
    //                         select "api"."myfunction"( "id" := (select "id" from subzero_args limit 1)) as subzero_scalar
    //                 )
    //             select "subzero_source".* from "subzero_source"
    //             "#,
    //             " "
    //         )
    //     );
    // }

    // #[test]
    // fn test_fmt_insert_query() {
    //     let payload = r#"[{"id":10, "a":"a field"}]"#.to_string();
    //     let q = Query {
    //         node: Insert {
    //             on_conflict: None,
    //             select: vec![
    //                 Simple {
    //                     field: Field {
    //                         name: s("a"),
    //                         json_path: None,
    //                     },
    //                     alias: None,
    //                     cast: None,
    //                 },
    //                 Simple {
    //                     field: Field {
    //                         name: s("b"),
    //                         json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))]),
    //                     },
    //                     alias: None,
    //                     cast: None,
    //                 },
    //             ],
    //             into: s("projects"),
    //             where_: ConditionTree {
    //                 operator: And,
    //                 conditions: vec![
    //                     // Single {filter: Op(s(">="),s("5")), field: Field {name: s("id"), json_path: None}, negate: false},
    //                     // Single {filter: Op(s("<"),s("10")), field: Field {name: s("id"), json_path: None}, negate: true}
    //                 ],
    //             },
    //             columns: vec![s("id"), s("a")],
    //             payload: Payload(payload.clone(),None),
    //             returning: vec![s("id"), s("a")],
    //         },
    //         sub_selects: vec![
    //             SubSelect {
    //                 query: Query {
    //                     node: Select {
    //                         order: vec![],
    //                         limit: None,
    //                         offset: None,
    //                         select: vec![Simple {
    //                             field: Field {
    //                                 name: s("id"),
    //                                 json_path: None,
    //                             },
    //                             alias: None,
    //                             cast: None,
    //                         }],
    //                         groupby: vec![],
    //                         from: (s("clients"), None),
    //                         join_tables: vec![],
    //                         where_: ConditionTree {
    //                             operator: And,
    //                             conditions: vec![Single {
    //                                 field: Field {
    //                                     name: s("id"),
    //                                     json_path: None,
    //                                 },
    //                                 filter: Filter::Col(
    //                                     Qi(s(""), s("subzero_source")),
    //                                     Field {
    //                                         name: s("client_id"),
    //                                         json_path: None,
    //                                     },
    //                                 ),
    //                                 negate: false,
    //                             }],
    //                         },
    //                     },
    //                     sub_selects: vec![],
    //                 },
    //                 alias: None,
    //                 hint: None,
    //                 join: Some(Parent(ForeignKey {
    //                     name: s("client_id_fk"),
    //                     table: Qi(s("api"), s("projects")),
    //                     columns: vec![s("client_id")],
    //                     referenced_table: Qi(s("api"), s("clients")),
    //                     referenced_columns: vec![s("id")],
    //                 })),
    //             },
    //             SubSelect {
    //                 query: Query {
    //                     node: Select {
    //                         order: vec![],
    //                         groupby: vec![],
    //                         limit: None,
    //                         offset: None,
    //                         select: vec![Simple {
    //                             field: Field {
    //                                 name: s("id"),
    //                                 json_path: None,
    //                             },
    //                             alias: None,
    //                             cast: None,
    //                         }],
    //                         from: (s("tasks"), None),
    //                         join_tables: vec![],
    //                         where_: ConditionTree {
    //                             operator: And,
    //                             conditions: vec![
    //                                 Single {
    //                                     field: Field {
    //                                         name: s("project_id"),
    //                                         json_path: None,
    //                                     },
    //                                     filter: Filter::Col(
    //                                         Qi(s(""), s("subzero_source")),
    //                                         Field {
    //                                             name: s("id"),
    //                                             json_path: None,
    //                                         },
    //                                     ),
    //                                     negate: false,
    //                                 },
    //                                 Single {
    //                                     filter: Op(s(">"), SingleVal(s("50"),None)),
    //                                     field: Field {
    //                                         name: s("id"),
    //                                         json_path: None,
    //                                     },
    //                                     negate: false,
    //                                 },
    //                                 Single {
    //                                     filter: In(ListVal(vec![s("51"), s("52")],None)),
    //                                     field: Field {
    //                                         name: s("id"),
    //                                         json_path: None,
    //                                     },
    //                                     negate: false,
    //                                 },
    //                             ],
    //                         },
    //                     },
    //                     sub_selects: vec![],
    //                 },
    //                 hint: None,
    //                 alias: None,
    //                 join: Some(Child(ForeignKey {
    //                     name: s("project_id_fk"),
    //                     table: Qi(s("api"), s("tasks")),
    //                     columns: vec![s("project_id")],
    //                     referenced_table: Qi(s("api"), s("projects")),
    //                     referenced_columns: vec![s("id")],
    //                 })),
    //             },
    //         ],
    //     };

    //     let (query_str, parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None).unwrap());
    //     let p0: &(dyn ToSql + Sync) = &ListVal(vec![s("51"), s("52")],None);
    //     let p1: &(dyn ToSql + Sync) = &SingleVal(s("50"),None);
    //     let p = Payload(payload,None);
    //     let pp: Vec<&(dyn ToSql + Sync)> = vec![&p, p1, p0];
    //     assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
    //     let re = Regex::new(r"\s+").unwrap();
    //     assert_eq!(
    //         re.replace_all(query_str.as_str(), " "),
    //         re.replace_all(
    //             r#"
    //     with 
    //     subzero_payload as ( select $1::json as json_data ),
    //     subzero_body as (
    //         select
    //             case when json_typeof(json_data) = 'array'
    //             then json_data
    //             else json_build_array(json_data)
    //             end as val
    //         from
    //             subzero_payload
    //     ),
    //     subzero_source as (
    //         insert into "api"."projects" ("id","a")
    //         select "id","a"
    //         from json_populate_recordset(null::"api"."projects", (select val from subzero_body)) _
    //         returning "id","a"
    //     )
    //     select
    //         "subzero_source"."a",
    //         to_jsonb("subzero_source"."b")->1->>'key' as "key",
    //         row_to_json("subzero_source_clients".*) as "clients",
    //         coalesce((select json_agg("tasks".*) from (
    //             select
    //                 "api"."tasks"."id"
    //             from "api"."tasks"
    //             where
            
    //                 "api"."tasks"."project_id" = "subzero_source"."id"
    //                 and
    //                 "api"."tasks"."id" > $2
    //                 and
    //                 "api"."tasks"."id" = any ($3)
    //         ) as "tasks"), '[]') as "tasks"
    //     from "subzero_source"
    //     left join lateral (
    //         select
    //             "api"."clients"."id"
    //         from "api"."clients"
    //         where
            
    //             "api"."clients"."id" = "subzero_source"."client_id"
    //     ) as "subzero_source_clients" on true
    //     "#,
    //             " "
    //         )
    //     );
    // }

    // #[bench]
    // fn bench_fmt_generate_query(b: &mut Bencher){

    // }

    #[test]
    fn test_fmt_select_query() {
        let q = Query {
            node: Select {
                order: vec![],
                groupby: vec![],
                limit: None,
                offset: None,
                select: vec![
                    Simple {
                        field: Field {
                            name: s("a"),
                            json_path: None,
                        },
                        alias: None,
                        cast: None,
                    },
                    Simple {
                        field: Field {
                            name: s("b"),
                            json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))]),
                        },
                        alias: None,
                        cast: None,
                    },
                ],
                from: (s("projects"), None),
                join_tables: vec![],
                where_: ConditionTree {
                    operator: And,
                    conditions: vec![
                        Single {
                            filter: Op(s(">="), SingleVal(s("5"),None)),
                            field: Field {
                                name: s("id"),
                                json_path: None,
                            },
                            negate: false,
                        },
                        Single {
                            filter: Op(s("<"), SingleVal(s("10"),None)),
                            field: Field {
                                name: s("id"),
                                json_path: None,
                            },
                            negate: true,
                        },
                    ],
                },
            },
            sub_selects: vec![
                SubSelect {
                    query: Query {
                        node: Select {
                            order: vec![],
                            groupby: vec![],
                            limit: None,
                            offset: None,
                            select: vec![Simple {
                                field: Field {
                                    name: s("id"),
                                    json_path: None,
                                },
                                alias: None,
                                cast: None,
                            }],
                            from: (s("clients"), None),
                            join_tables: vec![],
                            where_: ConditionTree {
                                operator: And,
                                conditions: vec![Single {
                                    field: Field {
                                        name: s("id"),
                                        json_path: None,
                                    },
                                    filter: Filter::Col(
                                        Qi(s("api"), s("projects")),
                                        Field {
                                            name: s("client_id"),
                                            json_path: None,
                                        },
                                    ),
                                    negate: false,
                                }],
                            },
                        },
                        sub_selects: vec![],
                    },
                    alias: None,
                    hint: None,
                    join: Some(Parent(ForeignKey {
                        name: s("client_id_fk"),
                        table: Qi(s("api"), s("projects")),
                        columns: vec![s("client_id")],
                        referenced_table: Qi(s("api"), s("clients")),
                        referenced_columns: vec![s("id")],
                    })),
                },
                SubSelect {
                    query: Query {
                        node: Select {
                            order: vec![],
                            groupby: vec![],
                            limit: None,
                            offset: None,
                            select: vec![Simple {
                                field: Field {
                                    name: s("id"),
                                    json_path: None,
                                },
                                alias: None,
                                cast: None,
                            }],
                            from: (s("tasks"), None),
                            join_tables: vec![],
                            where_: ConditionTree {
                                operator: And,
                                conditions: vec![
                                    Single {
                                        field: Field {
                                            name: s("project_id"),
                                            json_path: None,
                                        },
                                        filter: Filter::Col(
                                            Qi(s("api"), s("projects")),
                                            Field {
                                                name: s("id"),
                                                json_path: None,
                                            },
                                        ),
                                        negate: false,
                                    },
                                    Single {
                                        filter: Op(s(">"), SingleVal(s("50"),None)),
                                        field: Field {
                                            name: s("id"),
                                            json_path: None,
                                        },
                                        negate: false,
                                    },
                                    Single {
                                        filter: In(ListVal(vec![s("51"), s("52")],None)),
                                        field: Field {
                                            name: s("id"),
                                            json_path: None,
                                        },
                                        negate: false,
                                    },
                                ],
                            },
                        },
                        sub_selects: vec![],
                    },
                    hint: None,
                    alias: None,
                    join: Some(Child(ForeignKey {
                        name: s("project_id_fk"),
                        table: Qi(s("api"), s("tasks")),
                        columns: vec![s("project_id")],
                        referenced_table: Qi(s("api"), s("projects")),
                        referenced_columns: vec![s("id")],
                    })),
                },
            ],
        };

        let (query_str, _parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None).unwrap());
        // assert_eq!(
        //     format!("{:?}", parameters),
        //     "[SingleVal(\"50\"), ListVal([\"51\", \"52\"]), SingleVal(\"5\"), SingleVal(\"10\")]"
        // );
        let re = Regex::new(r"\s+").unwrap();
        assert_eq!(
            re.replace_all(query_str.as_str(), " "),
            re.replace_all(
                r#"
        select
            "apis"."projects"."a",
            to_jsonb("api"."projects"."b")->1->>'key' as "key",
            row_to_json("projects_clients".*) as "clients",
            coalesce((select json_agg("tasks".*) from (
                select
                    "api"."tasks"."id"
                from "api"."tasks"
                where
                    "api"."tasks"."project_id" = "api"."projects"."id"
                    and
                    "api"."tasks"."id" > $1
                    and
                    "api"."tasks"."id" = any ($2)
            ) as "tasks"), '[]') as "tasks"
        from "api"."projects"
        left join lateral (
            select
                "api"."clients"."id"
            from "api"."clients"
            where
                "api"."clients"."id" = "api"."projects"."client_id"
        ) as "projects_clients" on true
        where
            "api"."projects"."id" >= $3
            and
            not("api"."projects"."id" < $4)
        "#,
                " "
            )
        );
    }

    
}
