use super::base::{
    fmt_as, fmt_condition, fmt_condition_tree, fmt_count_query, fmt_field, fmt_filter, fmt_in_filter, fmt_json_path, fmt_limit, fmt_logic_operator,
    fmt_main_query, fmt_offset, fmt_operator, fmt_order, fmt_order_term, fmt_groupby, fmt_groupby_term, fmt_qi, fmt_select_item, fmt_select_name,
    star_select_item_format, fmt_function_param, fmt_env_query, get_body_snippet, fmt_function_call,
};
pub use super::base::return_representation;
use crate::schema::DbSchema;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet, SqlSnippetChunk, generate_fn,};
use crate::error::{Result, Error};

use super::{ToParam, Snippet, SqlParam};
lazy_static! {
    pub static ref SUPPORTED_OPERATORS: HashSet<&'static str> = ["eq", "gte", "gt", "lte", "lt", "neq", "like", "ilike", "in", "is"]
        .iter()
        .copied()
        .collect();
}

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
macro_rules! fmt_field_format {
    () => {
        //"to_jsonb({}{}{}){}"
        "json_extract({}{}{}, '${}')"
    };
}

macro_rules! param_placeholder_format {
    () => {
        "?{pos:.0}{data_type:.0}"
    };
}

macro_rules! body_snippet {
    (json_array) => {
        "coalesce(json_arrayagg(_subzero_t.row_), '[]')"
    };
    (json_object) => {
        "coalesce(json_extract(json_arrayagg(_subzero_t.row_),'$[0]'), 'null')"
    };
    (csv) => {
        "''"
    }; // TODO!! unimplemented
    (function_scalar) => {
        "''"
    }; //TODO!! unimplemented
    (function_scalar_array) => {
        "''"
    }; //TODO!! unimplemented
    (function_any) => {
        "''"
    }; //TODO!! unimplemented
}

generate_fn!();
fmt_main_query!();
pub fn fmt_main_query_internal<'a>(
    db_schema: &'a DbSchema<'_>, schema: &'a str, method: &'a str, accept_content_type: &ContentType, query: &'a Query,
    preferences: &'a Option<Preferences>, env: &'a HashMap<&'a str, &'a str>,
) -> Result<Snippet<'a>> {
    let count = matches!(
        preferences,
        Some(Preferences {
            count: Some(Count::ExactCount),
            ..
        })
    );
    let check_constraints = matches!(query.node, Insert { .. } | Update { .. } | Select { check: Some(_), .. });
    let return_representation = return_representation(method, query, preferences);
    let body_snippet = get_body_snippet!(return_representation, accept_content_type, query)?;
    let run_unwrapped_query = matches!(query.node, Insert { .. } | Update { .. } | Delete { .. });
    //let has_payload_cte = matches!(query.node, Insert { .. } | Update { .. });

    let main_query = if run_unwrapped_query {
        fmt_query(db_schema, schema, return_representation, None, query, &None, Some("with env as (" + fmt_env_query(env) + ") "))?
    } else {
        let source_query = fmt_query(db_schema, schema, return_representation, Some("_subzero_query"), query, &None, None)?;
        sql("with")
            + " env as ("
            + fmt_env_query(env)
            + ")"
            + " , "
            + source_query
            + " , "
            + if count {
                fmt_count_query(db_schema, schema, Some("_subzero_count_query"), query)?
            } else {
                sql("_subzero_count_query AS (select 1)")
            }
            + " select"
            + " count(*) as page_total, "
            + if count { "(SELECT count(*) FROM _subzero_count_query)" } else { "null" }
            + " as total_result_set, "
            + body_snippet
            + " as body, "
            + if check_constraints {
                //"(select coalesce(bool_and(_subzero_check__constraint),true) from subzero_source) as constraints_satisfied, "
                "( select coalesce(min(case when _subzero_check__constraint then 1 else 0 end) = 1,true)  from _subzero_query) as constraints_satisfied, "
            } else {
                "true as constraints_satisfied, "
            }
            + " nullif(@response.headers, '') as response_headers, "
            + " nullif(@response.status, '') as response_status "
            + " from ( select * from _subzero_query ) _subzero_t"
    };
    Ok(main_query)
}
//fmt_query!();
pub fn fmt_query<'a>(
    db_schema: &DbSchema<'a>, schema: &'a str, _return_representation: bool, wrapin_cte: Option<&'static str>, q: &'a Query, _join: &Option<Join>,
    extra_cte: Option<Snippet<'a>>,
) -> Result<Snippet<'a>> {
    let add_env_tbl_to_from = wrapin_cte.is_some();
    let is_insert = matches!(q.node, Insert { .. });
    let (cte_snippet, query_snippet) = match &q.node {
        FunctionCall {
            fn_name,
            parameters,
            payload,
            is_scalar,
            is_multiple_call,
            returning,
            select,
            where_,
            limit,
            offset,
            order,
            ..
        } => {
            let (params_cte, arg_frag): (Snippet<'a>, Snippet<'a>) = match &parameters {
                CallParams::OnePosParam(_p) => {
                    let bb: &SqlParam = payload;
                    (sql(" "), param(bb))
                }
                CallParams::KeyParams(p) if p.is_empty() => (sql(" "), sql("")),
                CallParams::KeyParams(prms) => (
                    fmt_body(payload, &prms.iter().map(|p| p.name).collect::<Vec<_>>())
                        + ", subzero_args as ( "
                        + "select * from json_to_recordset((select val from subzero_body)) as _("
                        + prms
                            .iter()
                            //.map(|p| format!("{} {}", fmt_identity(&p.name), p.type_))
                            .map(|p| [fmt_identity(p.name), p.type_.to_string()].join(" "))
                            .collect::<Vec<_>>()
                            .join(", ")
                        + ")"
                        + " ), ",
                    sql(prms
                        .iter()
                        .map(|p| {
                            let variadic = if p.variadic { "variadic" } else { "" };
                            let ident = fmt_identity(p.name);
                            if *is_multiple_call {
                                //format!("{} {} := subzero_args.{}", variadic, ident, ident)
                                [variadic, ident.as_str(), ":= subzero_args.", ident.as_str()].join(" ")
                            } else {
                                // format!(
                                //     "{} {}  := (select {} from subzero_args limit 1)",
                                //     variadic, ident, ident
                                // )
                                [variadic, ident.as_str(), ":= (select", ident.as_str(), "from subzero_args limit 1)"].join(" ")
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")),
                ),
            };
            let call_it = fmt_qi(fn_name) + "(" + arg_frag + ") ";
            let returned_columns = if returning.is_empty() {
                "*".to_string()
            } else {
                returning
                    .iter()
                    .map(|&r| {
                        if r == "*" {
                            //format!("{}.*", fmt_identity(&fn_name.1))
                            [fmt_identity(fn_name.1).as_str(), ".*"].join("")
                        } else {
                            //format!("{}.{}", fmt_identity(&fn_name.1), fmt_identity(r))
                            [fmt_identity(fn_name.1), fmt_identity(r)].join(".")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let args_body = if *is_multiple_call {
                if *is_scalar {
                    "select " + call_it + " subzero_scalar from subzero_args, env"
                } else {
                    format!("select subzero_lat_args.* from subzero_args, lateral ( select {returned_columns} from ")
                        + call_it
                        + ", env"
                        + " ) subzero_lat_args"
                }
            } else if *is_scalar {
                "select " + call_it + " as subzero_scalar from env"
            } else {
                format!("select {returned_columns} from ") + call_it + " , env"
            };

            let qi_subzero_source = &Qi("", "subzero_source");
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(db_schema, schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects);
            (
                Some(params_cte + " subzero_source as ( " + args_body + " )"),
                " select "
                    + select.join(", ")
                    + " from "
                    + fmt_identity("subzero_source")
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if !where_.conditions.is_empty() {
                        "where " + fmt_condition_tree(qi_subzero_source, where_)?
                    } else {
                        sql("")
                    }
                    + " "
                    + fmt_order(qi_subzero_source, order)?
                    + " "
                    + fmt_limit(limit)
                    + " "
                    + fmt_offset(offset),
            )
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
            check,
        } => {
            //let table_alias = table_alias_suffix.map(|s| format!("{}{}", table, s)).unwrap_or_default();
            let (_qi, from_snippet) = match table_alias {
                None => (Qi(schema, table), fmt_qi(&Qi(schema, table))),
                Some(a) => (
                    Qi("", a),
                    // format!(
                    //     "{} as {}",
                    //     fmt_qi(&Qi(schema.clone(), table.clone())),
                    //     fmt_identity(&a)
                    // ),
                    [fmt_qi(&Qi(schema, table)), fmt_identity(a)].join(" as "),
                ),
            };
            let qi = &_qi;
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(db_schema, schema, qi, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects);

            // this check field is relevant in second stage select (after a mutation)
            // for databases that do not support returning clause
            let check_condition = match check {
                Some(c) if !c.conditions.is_empty() => ", " + fmt_condition_tree(qi, c)? + " as _subzero_check__constraint",
                _ => sql(""),
            };

            (
                None,
                " select json_object("
                    + select.join(", ")
                    + ") as row_"
                    + check_condition
                    + " from "
                    + from_snippet
                    + if add_env_tbl_to_from { ", env " } else { "" }
                    + if !join_tables.is_empty() {
                        // format!(
                        //     ", {}",
                        //     join_tables
                        //         .iter()
                        //         .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                        //         .collect::<Vec<_>>()
                        //         .join(", ")
                        // )
                        String::from(", ") + join_tables.iter().map(|f| fmt_qi(&Qi(schema, f))).collect::<Vec<_>>().join(", ").as_str()
                    } else {
                        //String::new()
                        String::new()
                    }
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if !where_.conditions.is_empty() {
                        "where " + fmt_condition_tree(qi, where_)?
                    } else {
                        sql("")
                    }
                    + " "
                    + (fmt_groupby(qi, groupby)?)
                    + (fmt_order(qi, order)?)
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
            //where_,
            //check,
            //returning,
            //select,
            on_conflict,
            ..
        } => {
            let schema_obj = db_schema.get_object(schema, into)?;
            let primary_key = schema_obj.columns.iter().find(|&(_, c)| c.primary_key).map(|(_, c)| c.name).unwrap_or("");
            let qi = &Qi(schema, into);
            //let qi_subzero_source = &Qi("", "subzero_source");
            // let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, _joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(db_schema, schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            // let returned_columns = if returning.is_empty() {
            //     "1".to_string()
            // } else {
            //     returning
            //         .iter()
            //         .map(|&r| {
            //             if r == "*" {
            //                 //format!("*")
            //                 String::from("*")
            //             } else {
            //                 fmt_identity(r)
            //             }
            //         })
            //         .collect::<Vec<_>>()
            //         .join(",")
            // };

            let into_columns = if !columns.is_empty() {
                columns.iter().map(|&c| fmt_identity(c)).collect::<Vec<_>>().join(",")
            } else {
                String::new()
            };
            (
                Some(
                    fmt_body(payload, columns) +
                " select " +
                columns.iter().map(|&c|
                    if c == primary_key {
                        format!("if( ( (@subzero_ids := json_array_append(@subzero_ids, '$', `{c}`)) <> null ), `{c}`, `{c}`) as `{c}`")
                    } else {
                        fmt_identity(c)
                    }
                ).collect::<Vec<_>>().join(",") +
                " from subzero_body" +
                //" on duplicate key update " + update_columns +
                // " " + if !where_.conditions.is_empty() { "where " + fmt_condition_tree(&Qi("", "_"), where_)? } else { sql("") } + // this line is only relevant for upsert
                match on_conflict {
                    Some((r,cols)) if !cols.is_empty() => {
                        //let on_c = format!("on conflict({})",cols.iter().map(|&c| fmt_identity(c)).collect::<Vec<_>>().join(", "));
                        //let on_c = "on duplicate key update ";
                        //let update_columns = columns.iter().map(|&c| format!("`{c}` = values(`{c}`)")).collect::<Vec<_>>().join(",");
                        //and ( (@ids := concat_ws(',', id, @ids)) <> null )
                        let on_do = match (r, columns.len()) {
                            (Resolution::IgnoreDuplicates, _) |
                            (_, 0) => //cols.iter().map(|&c| format!("`{c}` = values(`{c}`)")).collect::<Vec<_>>().join(","),    
                                    format!("`{primary_key}` = if( ( (@subzero_ignored_ids := json_array_append(@subzero_ignored_ids, '$', values(`{primary_key}`))) <> null ), values(`{primary_key}`), values(`{primary_key}`))"),
                            _ => columns.iter().map(|&c| format!("`{c}` = values(`{c}`)")).collect::<Vec<_>>().join(",")
                        };
                        format!(" on duplicate key update  {on_do}")
                    },
                    _ => String::new()
                }, // + " returning " + returned_columns +

                                       // // for each row add a column if it passes the internal permissions check defined for the schema
                                       // if !check.conditions.is_empty() {
                                       //     ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "
                                       // }
                                       // else {
                                       //     sql(", true  as _subzero_check__constraint ")
                                       // } +
                                       // " )"
                ),
                sql("insert into ") + fmt_qi(qi) + " (" + into_columns + ") ",
            )
        }
        Delete {
            from,
            where_,
            // returning,
            // select,
            ..
        } => {
            let schema_obj = db_schema.get_object(schema, from)?;
            let primary_key = schema_obj.columns.iter().find(|&(_, c)| c.primary_key).map(|(_, c)| c.name).unwrap_or("");
            let qi = &Qi(schema, from);
            let fmt_qi = fmt_qi(qi);
            // let qi_subzero_source = &Qi("", "subzero_source");
            // let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(db_schema, schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            // let returned_columns = if returning.is_empty() {
            //     "1".to_string()
            // } else {
            //     returning
            //         .iter()
            //         .map(|&r| if r == "*" { "*".to_string() } else { fmt_identity(r) })
            //         .collect::<Vec<_>>()
            //         .join(",")
            // };
            let collect_ids_condition = format!("(@subzero_ids := json_array_append(@subzero_ids, '$', {fmt_qi}.`{primary_key}`)) <> '[]'");
            (
                None,
                sql(" delete from ")
                    + fmt_qi
                    + if !where_.conditions.is_empty() {
                        " where " + fmt_condition_tree(qi, where_)? + " and " + collect_ids_condition
                    } else {
                        sql(" where ") + collect_ids_condition
                    },
            )

            // (
            //     Some(
            //         sql(" subzero_source as ( ")
            //             + " delete from "
            //             + fmt_qi(qi)
            //             + " "
            //             + if !where_.conditions.is_empty() {
            //                 "where " + fmt_condition_tree(qi, where_)?
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
            //             + fmt_identity("subzero_source")
            //             + " "
            //             + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
            //             + " "
            //             + if !where_.conditions.is_empty() {
            //                 "where " + fmt_condition_tree(qi_subzero_source, where_)?
            //             } else {
            //                 sql("")
            //             }
            //     } else {
            //         sql(format!(" select * from {}", fmt_identity("subzero_source")))
            //     },
            // )
        }
        Update {
            table,
            columns,
            payload,
            where_,
            // check,
            // returning,
            // select,
            ..
        } => {
            let schema_obj = db_schema.get_object(schema, table)?;
            let primary_key = schema_obj.columns.iter().find(|&(_, c)| c.primary_key).map(|(_, c)| c.name).unwrap_or("");
            let qi = &Qi(schema, table);
            //let qi_subzero_source = &Qi("", "subzero_source");
            //let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            // let (sub_selects, joins): (Vec<_>, Vec<_>) = q
            //     .sub_selects
            //     .iter()
            //     .map(|s| fmt_sub_select_item(db_schema, schema, qi_subzero_source, s))
            //     .collect::<Result<Vec<_>>>()?
            //     .into_iter()
            //     .unzip();
            // select.extend(sub_selects.into_iter());
            // let returned_columns = if returning.is_empty() {
            //     "1".to_string()
            // } else {
            //     returning
            //         .iter()
            //         .map(|&r| {
            //             if r == "*" {
            //                 format!("{}.*", fmt_qi(qi))
            //             } else {
            //                 format!("{}.{}", fmt_qi(qi), fmt_identity(r))
            //             }
            //         })
            //         .collect::<Vec<_>>()
            //         .join(",")
            // };

            let qi_fmt = fmt_qi(qi);
            let set_columns = columns
                .iter()
                .map(|c| format!("{qi_fmt}.{} = subzero_body.{}", fmt_identity(c), fmt_identity(c)))
                .collect::<Vec<_>>()
                .join(",");
            let collect_ids_condition = format!("(@subzero_ids := json_array_append(@subzero_ids, '$', {qi_fmt}.`{primary_key}`)) <> '[]'");
            (
                Some(fmt_body(payload, columns)),
                // update clients _, subzero_body
                // set _.name = subzero_body.name
                // where id > 0
                // and ( (@ids := json_array_append(@ids, '$', id)) <> '[]' )
                // ;
                sql(" update ")
                    + qi_fmt
                    + ", subzero_body"
                    + " set "
                    + set_columns
                    + if !where_.conditions.is_empty() {
                        " where " + fmt_condition_tree(qi, where_)? + " and " + collect_ids_condition
                    } else {
                        sql(" where ") + collect_ids_condition
                    },
            )

            // (
            //     if columns.is_empty() {
            //         let sel = if returning.is_empty() {
            //             "null".to_string()
            //         } else {
            //             returning
            //                 .iter()
            //                 .map(|&r| if r == "*" { format!("{table}.*") } else { format!("{table}.{r}") })
            //                 .collect::<Vec<_>>()
            //                 .join(",")
            //         };
            //         Some(sql(format!(
            //             " subzero_source as (select {}, true as _subzero_check__constraint from {} where false )",
            //             sel,
            //             fmt_qi(qi)
            //         )))
            //     } else {
            //         Some(
            //             fmt_body(payload, columns)
            //                 + ", subzero_source as ( "
            //                 + " update "
            //                 + fmt_qi(qi)
            //                 + " set "
            //                 + set_columns
            //                 + " from (select * from json_populate_recordset (null::"+ fmt_qi(qi)+ " , (select val from subzero_body) )) _ "
            //                 + " "
            //                 + if !where_.conditions.is_empty() {
            //                     "where " + fmt_condition_tree(qi, where_)?
            //                 } else {
            //                     sql("")
            //                 }
            //                 + " returning "
            //                 + returned_columns
            //                 // for each row add a column if it passes the internal permissions check defined for the schema
            //                 + if !check.conditions.is_empty() { ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "} else { sql(", true as _subzero_check__constraint ") }
            //                 + " )",
            //         )
            //     },
            //     if return_representation {
            //         " select "
            //             + select.join(", ")
            //             + " from "
            //             + fmt_identity("subzero_source")
            //             + " "
            //             + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
            //         //" " + if !where_.conditions.is_empty() { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
            //     } else {
            //         sql(format!(" select * from {}", fmt_identity("subzero_source")))
            //     },
            // )
        }
    };

    let cte_snippet = match (cte_snippet, extra_cte) {
        (Some(cte), Some(extra_cte)) => Some(extra_cte + " , " + cte),
        (Some(cte), None) => Some(cte),
        (None, Some(extra_cte)) => Some(extra_cte),
        (None, None) => None,
    };

    Ok(match wrapin_cte {
        Some(cte_name) => match cte_snippet {
            Some(cte) => " " + cte + " , " + format!("{cte_name} as ( ") + query_snippet + " )",
            None => format!(" {cte_name} as ( ") + query_snippet + " )",
        },
        None => match cte_snippet {
            Some(cte) if is_insert => query_snippet + " " + cte,
            Some(cte) => cte + " " + query_snippet,
            None => query_snippet,
        },
    })
}
fmt_env_query!();
fmt_count_query!();
//fmt_body!();
fn fmt_body<'a>(payload: &'a Payload, columns: &[&'a str]) -> Snippet<'a> {
    let payload_param: &SqlParam = payload;
    let payload_columns = columns.iter().map(|&c| format!("`{c}` text path '$.{c}'")).collect::<Vec<_>>().join(",");
    " subzero_payload as ( select "
        + param(payload_param)
        + " as val ),"
        + " subzero_body as ("
        + "   select t.*"
        + "   from subzero_payload p,"
        + "   json_table("
        + "     case when json_type(p.val) = 'ARRAY' then p.val else concat('[',p.val,']') end,"
        + "     '$[*]'"
        + "     columns("
        + payload_columns
        + ")"
        + " ) t"
        + ")"
}
fmt_condition_tree!();
fmt_condition!();
//fmt_env_var!();
fn fmt_env_var(e: &EnvVar) -> String {
    match e {
        EnvVar { var, part: None } => format!("(select {} from env)", fmt_identity(var)),
        EnvVar { var, part: Some(part) } => format!("(select {}->>'$.{}' from env)", fmt_identity(var), part),
    }
}
macro_rules! fmt_in_filter {
    ($p:ident) => {
        "=any(select * from json_table(" + param($p) + ", '$[*]' columns (val text path '$')) as t)"
    };
}
fmt_filter!();
fmt_select_name!();
fmt_function_call!();
//fmt_select_item_function!();
fn fmt_select_item_function<'a, 'b>(
    qi: &'b Qi<'b>, fn_name: &'a str, parameters: &'a [FunctionParam<'a>], partitions: &'a [Field<'a>], orders: &'a [OrderTerm],
    alias: &'a Option<&str>,
) -> Result<Snippet<'a>> {
    Ok(format!("'{}',", fmt_select_name(fn_name, &None, alias).unwrap_or_default().as_str())
        + fmt_function_call(qi, fn_name, parameters)?
        + if partitions.is_empty() && orders.is_empty() {
            sql("")
        } else {
            sql(" over( ")
                + if partitions.is_empty() {
                    sql("")
                } else {
                    sql("partition by ") + partitions.iter().map(|p| fmt_field(qi, p)).collect::<Result<Vec<_>>>()?.join(",")
                }
                + " "
                + if orders.is_empty() { "".to_string() } else { fmt_order(qi, orders)? }
                + " )"
        })
}
fmt_select_item!();
fmt_function_param!();
//fmt_sub_select_item!();
fn fmt_sub_select_item<'a, 'b>(
    db_schema: &DbSchema<'a>, schema: &'a str, qi: &'b Qi<'b>, i: &'a SubSelect,
) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
    let SubSelect { query, alias, join, .. } = i;
    match join {
        Some(j) => match j {
            Parent(fk) => {
                let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                let subquery = fmt_query(db_schema, schema, true, None, query, join, None)?;

                Ok((
                    sql(format!("'{}', {}.row_", alias_or_name, fmt_identity(&local_table_name))),
                    vec!["left join lateral (" + subquery + ") as " + sql(fmt_identity(&local_table_name)) + " on true"],
                ))
            }
            Child(fk) => {
                let alias_or_name = alias.as_ref().unwrap_or(&fk.table.1);
                let local_table_name = fmt_identity(fk.table.1);
                let subquery = fmt_query(db_schema, schema, true, None, query, join, None)?;
                Ok((
                    (format!("'{alias_or_name}', coalesce((select json_arrayagg(")
                        + sql(local_table_name.clone())
                        + ".row_) from ("
                        + subquery
                        + ") as "
                        + sql(local_table_name)
                        + "), json_array())"),
                    vec![],
                ))
            }
            Many(_table, _fk1, fk2) => {
                let alias_or_name = alias.as_ref().unwrap_or(&fk2.referenced_table.1);
                let local_table_name = fmt_identity(fk2.referenced_table.1);
                let subquery = fmt_query(db_schema, schema, true, None, query, join, None)?;
                Ok((
                    (format!("'{alias_or_name}', coalesce((select json_arrayagg(")
                        + sql(local_table_name.clone())
                        + ".row_) from ("
                        + subquery
                        + ") as "
                        + sql(local_table_name)
                        + "), json_array())"),
                    vec![],
                ))
            }
        },
        None => panic!("unable to format join query without matching relation"),
    }
}
fmt_operator!();
fmt_logic_operator!();
//fmt_identity!();
#[allow(clippy::ptr_arg)]
fn fmt_identity(i: &str) -> String {
    String::from("`") + i.chars().take_while(|x| x != &'\0').collect::<String>().replace('`', "``").as_str() + "`"
}
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
        JKey(k) => k.to_string(),
        JIdx(i) => format!("[{i}]"),
    }
}
