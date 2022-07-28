use super::base::{
    fmt_as,
    //fmt_body,
    fmt_condition,
    fmt_condition_tree,
    //fmt_main_query,
    //fmt_query,
    fmt_count_query,
    fmt_field,
    fmt_filter,
    fmt_identity,
    fmt_json_path,
    //fmt_as_name,
    fmt_limit,
    fmt_logic_operator,
    fmt_offset,
    //fmt_sub_select_item,
    fmt_operator,
    fmt_order,
    fmt_order_term,
    fmt_groupby,
    fmt_groupby_term,
    //fmt_qi,
    fmt_select_item,
    fmt_function_param,
    fmt_select_name,
    //fmt_json_operation,
    //fmt_json_operand,
    //return_representation,

    star_select_item_format,
    //fmt_select_item_function,
    fmt_function_call,
};
pub use super::base::return_representation;
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, 
    SqlSnippet,SqlSnippetChunk,generate_fn, param_placeholder_format,};
use crate::error::{Result,Error};
use super::{ToParam, Snippet, SqlParam};
generate_fn!();
macro_rules! simple_select_item_format {
    () => {
        "'{select_name}', {field}{as:.0}"
    };
}
macro_rules! cast_select_item_format {
    () => {
        "'{select_name}', cast({field} as {cast}){as:.0}"
    };
}

//fmt_main_query!();
pub fn fmt_main_query<'a>(schema_str: &'a str, request: &'a ApiRequest) -> Result<Snippet<'a>> {
    let schema = String::from(schema_str);
    let count = matches!(&request.preferences, Some(Preferences {count: Some(Count::ExactCount),..}));

    let return_representation = return_representation(request);
    let body_snippet = match (return_representation, &request.accept_content_type, &request.query.node) {
        (false, _, _) => "''",
        (true, SingularJSON, FunctionCall { is_scalar: true, .. })
        | (
            true,
            ApplicationJSON,
            FunctionCall {
                returns_single: true,
                is_multiple_call: false,
                is_scalar: true,
                ..
            },
        ) => "coalesce((json_agg(_subzero_t.subzero_scalar)->0)::text, 'null')",
        (
            true,
            ApplicationJSON,
            FunctionCall {
                returns_single: false,
                is_multiple_call: false,
                is_scalar: true,
                ..
            },
        ) => "coalesce((json_agg(_subzero_t.subzero_scalar))::text, '[]')",
        (true, SingularJSON, FunctionCall { is_scalar: false, .. })
        | (
            true,
            ApplicationJSON,
            FunctionCall {
                returns_single: true,
                is_multiple_call: false,
                is_scalar: false,
                ..
            },
        ) => "coalesce((json_agg(_subzero_t)->0)::text, 'null')",

        (true, ApplicationJSON, _) => "json_group_array(_subzero_t.row)",
        (true, SingularJSON, _) => "coalesce((json_agg(_subzero_t)->0)::text, 'null')",
        (true, TextCSV, _) => {
            r#"
            (SELECT coalesce(string_agg(a.k, ','), '')
              FROM (
                SELECT json_object_keys(r)::text as k
                FROM ( 
                  SELECT row_to_json(hh) as r from _subzero_query as hh limit 1
                ) s
              ) a
            )
            || chr(10) ||
            coalesce(string_agg(substring(_subzero_t::text, 2, length(_subzero_t::text) - 2), chr(10)), '')
        "#
        }
    };

    let run_unwrapped_query = matches!(request.query.node, Insert {..} | Update {..} | Delete {..});
    let wrap_cte_name = if run_unwrapped_query {None} else {Some("_subzero_query")};
    let source_query = fmt_query(&schema, return_representation, wrap_cte_name, &request.query, &None)?;
    let main_query = if run_unwrapped_query {
        source_query
    }
    else {
        source_query
        + " , "
        + if count {
            fmt_count_query(&schema, Some("_subzero_count_query"), &request.query)?
        } else {
            sql("_subzero_count_query as (select 1)")
        }
        + " select"
        + " count(_subzero_t.row) AS page_total, "
        + if count { "(SELECT count(*) FROM _subzero_count_query)" } else { "null" }
        + " as total_result_set, "
        + body_snippet
        + " as body, "
        + " null as response_headers, "
        + " null as response_status "
        + " from ( select * from _subzero_query ) _subzero_t"
    };
    
    Ok(main_query)
}
//fmt_query!();
fn fmt_query<'a>(
    schema: &String, _return_representation: bool, wrapin_cte: Option<&'static str>, q: &'a Query, _join: &Option<Join>,
) -> Result<Snippet<'a>> {
    let (cte_snippet, query_snippet) = match &q.node {
        FunctionCall {..} => {
            return Err(Error::UnsupportedFeature { message: "function calls in sqlite not supported".to_string()})
        }
        Select {
            select,
            from: (table, table_alias),
            join_tables,
            where_,
            limit,
            offset,
            order,
            groupby,
        } => {
            let (qi, from_snippet) = match table_alias {
                Some(a) => (
                    Qi("".to_string(), a.clone()),
                    format!("{} as {}", fmt_qi(&Qi(schema.clone(), table.clone())), fmt_identity(a)),
                ),
                None => (Qi(schema.clone(), table.clone()), fmt_qi(&Qi(schema.clone(), table.clone()))),
            };
            if select.iter().any(|s| matches!( s, Star)) {
                return Err(Error::UnsupportedFeature {message: "'select *' not supported, use explicit select parameters".to_string()})
            }
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(&qi, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, &qi, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());

            (
                None,
                sql(" select ")
                    + " json_object("
                    + select.join(", ")
                    + ") as row"
                    + " from "
                    + from_snippet
                    + " "
                    + if !join_tables.is_empty() {
                        format!(
                            ", {}",
                            join_tables
                                .iter()
                                .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        String::new()
                    }
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if !where_.conditions.is_empty() {
                        "where " + fmt_condition_tree(&qi, where_)?
                    } else {
                        sql("")
                    }
                    + " "
                    + fmt_groupby(&qi, groupby)?
                    + " "
                    + fmt_order(&qi, order)?
                    + " "
                    + fmt_limit(limit)
                    + " "
                    + fmt_offset(offset),
            )
        }
        Insert {
            into,
            columns,
            payload,
            where_,
            returning,
            on_conflict,
            .. //select
        } => {
            let qi = &Qi(schema.clone(), into.clone());
            // let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            // let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| if r.as_str() == "*" { "*".to_string() } else { fmt_identity(r) })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let into_columns = if !columns.is_empty() {
                format!("({})", columns.iter().map(fmt_identity).collect::<Vec<_>>().join(","))
            } else {
                String::new()
            };
            let select_columns = columns.iter().map(fmt_identity).collect::<Vec<_>>().join(",");
            (
                None,
                "with " +
                fmt_body(payload, columns) +
                " insert into " + fmt_qi(qi) + " " +into_columns +
                " select " + select_columns +
                " from subzero_body " +
                " " + if !where_.conditions.is_empty() { "where " + fmt_condition_tree(&Qi("".to_string(), "_".to_string()), where_)? } else { sql(" where true ") } + // this line is only relevant for upsert
                match on_conflict {
                    Some((r,cols)) if !cols.is_empty() => {
                        let on_c = format!("on conflict({})",cols.iter().map(fmt_identity).collect::<Vec<_>>().join(", "));
                        let on_do = match (r, columns.len()) {
                            (Resolution::IgnoreDuplicates, _) |
                            (_, 0) => "do nothing".to_string(),
                            _ => format!(
                                "do update set {}",
                                columns.iter().map(|c|
                                    format!("{} = excluded.{}", fmt_identity(c), fmt_identity(c))
                                ).collect::<Vec<_>>().join(", ")
                            )
                        };
                        format!("{} {}", on_c, on_do)
                    },
                    _ => String::new()
                } +
                " returning " + returned_columns
                
                // Some(
                //     fmt_body(payload, columns)+
                //     ", subzero_source as ( " + 
                //     " insert into " + fmt_qi(qi) + " " +into_columns +
                //     " select " + select_columns +
                //     " from subzero_body _ " +
                //     " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&Qi("".to_string(), "_".to_string()), where_)? } else { sql("") } + // this line is only relevant for upsert
                //     match on_conflict {
                //         Some((r,cols)) if cols.len()>0 => {
                //             let on_c = format!("on conflict({})",cols.iter().map(fmt_identity).collect::<Vec<_>>().join(", "));
                //             let on_do = match (r, columns.len()) {
                //                 (Resolution::IgnoreDuplicates, _) |
                //                 (_, 0) => format!("do nothing"),
                //                 _ => format!(
                //                     "do update set {}",
                //                     columns.iter().map(|c|
                //                         format!("{} = excluded.{}", fmt_identity(c), fmt_identity(c))
                //                     ).collect::<Vec<_>>().join(", ")
                //                 )
                //             };
                //             format!("{} {}", on_c, on_do)
                //         },
                //         _ => format!("")
                //     } +
                //     " returning " + returned_columns +
                //     " )",
                // ),
                // if return_representation {
                //     " select "
                //         + select.join(", ")
                //         + " from "
                //         + fmt_identity(&"subzero_source".to_string())
                //         + " "
                //         + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                //         + " "
                //         + if where_.conditions.len() > 0 {
                //             "where " + fmt_condition_tree(qi_subzero_source, where_)?
                //         } else {
                //             sql("")
                //         }
                // } else {
                //     sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
                // },
            )
        }
        Delete {
            from,
            where_,
            returning,
            .. //select,
        } => {
            let qi = &Qi(schema.clone(), from.clone());
            // let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            // let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| if r.as_str() == "*" { "*".to_string() } else { fmt_identity(r) })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            (
                None,
                sql(" delete from ")
                + fmt_qi(qi)
                + " "
                + if !where_.conditions.is_empty() {
                    "where " + fmt_condition_tree(qi, where_)?
                } else {
                    sql("")
                }
                + " returning "
                + returned_columns
            )

            // (
            //     Some(
            //         sql(" subzero_source as ( ")
            //             + " delete from "
            //             + fmt_qi(qi)
            //             + " "
            //             + if where_.conditions.len() > 0 {
            //                 "where " + fmt_condition_tree(&qi, where_)?
            //             } else {
            //                 sql("")
            //             }
            //             + " returning "
            //             + returned_columns
            //             + " )",
            //     ),
            //     if return_representation {
            //         " select "
            //             + select.join(", ")
            //             + " from "
            //             + fmt_identity(&"subzero_source".to_string())
            //             + " "
            //             + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
            //             + " "
            //             + if where_.conditions.len() > 0 {
            //                 "where " + fmt_condition_tree(qi_subzero_source, where_)?
            //             } else {
            //                 sql("")
            //             }
            //     } else {
            //         sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
            //     },
            // )
        }
        Update {
            table,
            columns,
            payload,
            where_,
            returning,
            ..//select,
        } => {
            let qi = &Qi(schema.clone(), table.clone());
            //let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            // let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            format!("{}.*", fmt_qi(qi))
                        } else {
                            format!("{}.{}", fmt_qi(qi), fmt_identity(r))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let set_columns = columns
                .iter()
                .map(|c| format!("{} = _.{}", fmt_identity(c), fmt_identity(c)))
                .collect::<Vec<_>>()
                .join(",");
            (
                None,
                if columns.is_empty() {
                    let sel = if returning.is_empty() {
                        "null".to_string()
                    } else {
                        returning
                            .iter()
                            .map(|r| {
                                if r.as_str() == "*" {
                                    format!("{}.*", table)
                                } else {
                                    format!("{}.{}", table, r)
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    sql(format!(" select {} from {} where false ", sel, fmt_qi(qi)))
                } else {
                        "with "
                        + fmt_body(payload, columns)
                        + " update "
                        + fmt_qi(qi)
                        + " set "
                        + set_columns
                        + " from (select * from subzero_body) _ "
                        + " "
                        + if !where_.conditions.is_empty() {
                            "where " + fmt_condition_tree(qi, where_)?
                        } else {
                            sql("")
                        }
                        + " returning "
                        + returned_columns
                }
            // (
            //     if columns.len() == 0 {
            //         let sel = if returning.len() == 0 {
            //             "null".to_string()
            //         } else {
            //             returning
            //                 .iter()
            //                 .map(|r| {
            //                     if r.as_str() == "*" {
            //                         format!("{}.*", table)
            //                     } else {
            //                         format!("{}.{}", table, r)
            //                     }
            //                 })
            //                 .collect::<Vec<_>>()
            //                 .join(",")
            //         };
            //         Some(sql(format!(" subzero_source as (select {} from {} where false )", sel, fmt_qi(qi))))
            //     } else {
            //         Some(
            //             fmt_body(payload, columns)
            //                 + ", subzero_source as ( "
            //                 + " update "
            //                 + fmt_qi(qi)
            //                 + " set "
            //                 + set_columns
            //                 + " from (select * from json_populate_recordset (null::"
            //                 + fmt_qi(qi)
            //                 + " , (select val from subzero_body) )) _ "
            //                 + " "
            //                 + if where_.conditions.len() > 0 {
            //                     "where " + fmt_condition_tree(&qi, where_)?
            //                 } else {
            //                     sql("")
            //                 }
            //                 + " returning "
            //                 + returned_columns
            //                 + " )",
            //         )
            //     },
            //     if return_representation {
            //         " select "
            //             + select.join(", ")
            //             + " from "
            //             + fmt_identity(&"subzero_source".to_string())
            //             + " "
            //             + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
            //         //" " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
            //     } else {
            //         sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
            //     },
            )
        }
    };

    Ok(match wrapin_cte {
        Some(cte_name) => match cte_snippet {
            Some(cte) => " with " + cte + " , " + format!("{} as ( ", cte_name) + query_snippet + " )",
            None => format!(" with {} as ( ", cte_name) + query_snippet + " )",
        },
        None => match cte_snippet {
            Some(cte) => " with " + cte + query_snippet,
            None => query_snippet,
        },
    })
}
fmt_count_query!();
//fmt_body!();
#[rustfmt::skip]
fn fmt_body<'a>(payload: &'a Payload, columns: &'a [String]) -> Snippet<'a> {
    let payload_param: &SqlParam = payload;
    " subzero_payload as ( select " + param(payload_param) + " as json_data ),"
    + " subzero_body as ("
    + " select "
    + columns.iter().map(|c| format!("json_extract(value, '$.{}') as {}", c, fmt_identity(c))).collect::<Vec<_>>().join(",")
    + " from (select value from json_each(("
        + " select"
        + " case when json_type(json_data) = 'array'"
        + " then json_data"
        + " else json_array(json_data)"
        + " end as val"
        + " from subzero_payload"
    + " )))"
    + " )"
}
fmt_condition_tree!();
fmt_condition!();
macro_rules! fmt_in_filter {
    ($p:ident) => {
        fmt_operator(&"in".to_string())? + ("( select * from rarray(" + param($p) + ") )")
    };
}
fmt_filter!();
fmt_select_name!();
fmt_function_call!();
//fmt_select_item_function!();
fn fmt_select_item_function<'a>(qi: &Qi, fn_name: &String,
    parameters: &'a [FunctionParam],
    partitions: &'a Vec<Field>,
    orders: &'a Vec<OrderTerm>,
    alias: &'a Option<String>,) -> Result<Snippet<'a>>
{

    Ok(
        format!("'{}', ", fmt_select_name(fn_name, &None, alias).unwrap_or_default()) +
        sql(fmt_identity(fn_name)) + 
        "(" +
            parameters
            .iter()
            .map(|p| fmt_function_param(qi, p))
            .collect::<Result<Vec<_>>>()?
            .join(",") +
        ")" + 
        if partitions.is_empty() && orders.is_empty() {
            sql("")
        } else {
            sql(" over( ") +
                if partitions.is_empty() {
                    sql("")
                } else {
                    sql("partition by ") +
                    partitions
                        .iter()
                        .map(|p| fmt_field(qi, p))
                        .collect::<Result<Vec<_>>>()?
                        .join(",")
                } +
                " " +
                if orders.is_empty() {
                    "".to_string()
                } else {
                    fmt_order(qi, orders)?
                } +
            " )"
        }
        
    )
}
fmt_select_item!();
fmt_function_param!();
//fmt_sub_select_item!();
fn fmt_sub_select_item<'a>(schema: &String, _qi: &Qi, i: &'a SubSelect) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
    let SubSelect { query, alias, join, .. } = i;
    match join {
        Some(j) => match j {
            Parent(fk) => {
                let alias_or_name = format!("'{}'", alias.as_ref().unwrap_or(&fk.referenced_table.1));
                //let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                let subquery = fmt_query(schema, true, None, query, join)?;

                Ok(((sql(alias_or_name) + ", " + "(" + subquery + ")"), vec![]))
            }
            Child(fk) => {
                let alias_or_name = format!("'{}'", alias.as_ref().unwrap_or(&fk.table.1));
                let local_table_name = fmt_identity(&fk.table.1);
                let subquery = fmt_query(schema, true, None, query, join)?;
                Ok((
                    (sql(alias_or_name)
                        + ", "
                        + "("
                        + " select json_group_array("
                        + local_table_name.clone()
                        + ".row)"
                        + " from ("
                        + subquery
                        + " ) as "
                        + local_table_name
                        + ")"),
                    vec![],
                ))
            }
            Many(_table, _fk1, fk2) => {
                let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk2.referenced_table.1));
                let local_table_name = fmt_identity(&fk2.referenced_table.1);
                let subquery = fmt_query(schema, true, None, query, join)?;
                Ok((
                    (sql(alias_or_name)
                        + ", "
                        + "("
                        + " select json_group_array("
                        + local_table_name.clone()
                        + ".row)"
                        + " from ("
                        + subquery
                        + " ) as "
                        + local_table_name
                        + ")"),
                    vec![],
                ))
            }
        },
        None => panic!("unable to format join query without matching relation"),
    }
}

fmt_operator!();
fmt_logic_operator!();
fmt_identity!();
//fmt_qi!();
fn fmt_qi(qi: &Qi) -> String {
    match (qi.0.as_str(), qi.1.as_str()) {
        ("", "")  => format!(""),
        _ => fmt_identity(&qi.1),
    }
}
macro_rules! fmt_field_format {
    () => {
        "json_extract({}{}{}, '${}')"
    };
}
fmt_field!();
fmt_order!();
fmt_order_term!();
fmt_groupby!();
fmt_groupby_term!();
fmt_as!();
fmt_limit!();
fmt_offset!();
fmt_json_path!();
//fmt_json_operation!();
fn fmt_json_operation(j: &JsonOperation) -> String {
    match j {
        JArrow(o) => format!(".{}", fmt_json_operand(o)),
        J2Arrow(o) => format!(".{}", fmt_json_operand(o)),
    }
}
//fmt_json_operand!();
fn fmt_json_operand(o: &JsonOperand) -> String {
    match o {
        JKey(k) => k.clone(),
        JIdx(i) => format!("[{}]", i),
    }
}
