use super::base::{
    fmt_as, fmt_body, fmt_condition, fmt_condition_tree, fmt_count_query, fmt_field, fmt_field_format, fmt_filter,
    fmt_env_var, fmt_in_filter, fmt_json_operand, fmt_json_operation, fmt_json_path, fmt_limit, fmt_logic_operator, fmt_main_query, fmt_offset,
    fmt_operator, fmt_order, fmt_order_term, fmt_groupby, fmt_groupby_term, fmt_qi, fmt_select_item, fmt_select_name, return_representation,
    star_select_item_format, fmt_function_param, fmt_select_item_function, fmt_function_call, fmt_env_query, get_body_snippet,
};
use std::borrow::Cow;
use std::collections::HashMap;
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet, SqlSnippetChunk, generate_fn, param_placeholder_format};
use crate::error::{Result, Error};

use super::{ToParam, Snippet, SqlParam};

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

macro_rules! param_placeholder_format {
    () => {
        "?{pos:.0}{data_type:.0}"
    };
}

macro_rules! body_snippet {
    (json_array) => {
        "json_arrayagg(_subzero_t.row_)"
    };
    (json_object) => {
        "coalesce((json_arrayagg(_subzero_t.row_)->0), 'null')"
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
    schema: &'a str, method: &'a str, accept_content_type: &ContentType, query: &'a Query, preferences: &'a Option<Preferences>,
    env: &'a HashMap<&'a str, &'a str>,
) -> Result<Snippet<'a>> {
    let count = matches!(
        preferences,
        Some(Preferences {
            count: Some(Count::ExactCount),
            ..
        })
    );
    let check_constraints = matches!(query.node, Insert { .. } | Update { .. });
    let return_representation = return_representation(method, query, preferences);
    let body_snippet = get_body_snippet!(return_representation, accept_content_type, query)?;
    Ok(sql("with")
        + " env as ("
        + fmt_env_query(env)
        + ")"
        + " , "
        + fmt_query(schema, return_representation, Some("_subzero_query"), query, &None)?
        + " , "
        + if count {
            fmt_count_query(schema, Some("_subzero_count_query"), query)?
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
            "(select coalesce(bool_and(_subzero_check__constraint),true) from subzero_source) as constraints_satisfied, "
        } else {
            "true as constraints_satisfied, "
        }
        + " nullif(@response.headers, '') as response_headers, "
        + " nullif(@response.status, '') as response_status "
        + " from ( select * from _subzero_query ) _subzero_t")
}
//fmt_query!();
pub fn fmt_query<'a>(
    schema: &'a str, return_representation: bool, wrapin_cte: Option<&'static str>, q: &'a Query, _join: &Option<Join>,
) -> Result<Snippet<'a>> {
    let add_env_tbl_to_from = wrapin_cte.is_some();
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
                    fmt_body(payload)
                        + ", subzero_args as ( "
                        + "select * from json_to_recordset((select val from subzero_body)) as _("
                        + prms
                            .iter()
                            //.map(|p| format!("{} {}", fmt_identity(&p.name), p.type_))
                            .map(|p| vec![fmt_identity(p.name), p.type_.to_string()].join(" "))
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
                                vec![variadic, ident.as_str(), ":= subzero_args.", ident.as_str()].join(" ")
                            } else {
                                // format!(
                                //     "{} {}  := (select {} from subzero_args limit 1)",
                                //     variadic, ident, ident
                                // )
                                vec![variadic, ident.as_str(), ":= (select", ident.as_str(), "from subzero_args limit 1)"].join(" ")
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
                            vec![fmt_identity(fn_name.1).as_str(), ".*"].join("")
                        } else {
                            //format!("{}.{}", fmt_identity(&fn_name.1), fmt_identity(r))
                            vec![fmt_identity(fn_name.1), fmt_identity(r)].join(".")
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
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());
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
                    vec![fmt_qi(&Qi(schema, table)), fmt_identity(a)].join(" as "),
                ),
            };
            let qi = &_qi;
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());

            (
                None,
                " select json_object("
                    + select.join(", ")
                    + ") as row_"
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
            where_,
            check,
            returning,
            select,
            on_conflict,
        } => {
            let qi = &Qi(schema, into);
            let qi_subzero_source = &Qi("", "subzero_source");
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|&r| {
                        if r == "*" {
                            //format!("*")
                            String::from("*")
                        } else {
                            fmt_identity(r)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let into_columns = if !columns.is_empty() {
                // format!(
                //     "({})",
                //     columns
                //         .iter()
                //         .map(fmt_identity)
                //         .collect::<Vec<_>>()
                //         .join(",")
                // )
                String::from("(") + columns.iter().map(|&c| fmt_identity(c)).collect::<Vec<_>>().join(",").as_str() + ")"
            } else {
                //String::new()
                String::new()
            };
            let select_columns = columns.iter().map(|&c| fmt_identity(c)).collect::<Vec<_>>().join(",");
            (
                Some(
                    fmt_body(payload)+
                    ", subzero_source as ( " +
                    " insert into " + fmt_qi(qi) + " " +into_columns +
                    " select " + select_columns +
                    " from json_populate_recordset(null::" + fmt_qi(qi) + ", (select val from subzero_body)) _ " +
                    " " + if !where_.conditions.is_empty() { "where " + fmt_condition_tree(&Qi("", "_"), where_)? } else { sql("") } + // this line is only relevant for upsert
                    match on_conflict {
                        Some((r,cols)) if !cols.is_empty() => {
                            let on_c = format!("on conflict({})",cols.iter().map(|&c| fmt_identity(c)).collect::<Vec<_>>().join(", "));
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
                            format!("{on_c} {on_do}")
                        },
                        _ => String::new()
                    } +
                    " returning " + returned_columns +
                    // for each row add a column if it passes the internal permissions check defined for the schema
                    if !check.conditions.is_empty() { ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "} else { sql(", true  as _subzero_check__constraint ") } +
                    " )",
                ),
                if return_representation {
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
                } else {
                    sql(format!(" select * from {}", fmt_identity("subzero_source")))
                },
            )
        }
        Delete {
            from,
            where_,
            returning,
            select,
        } => {
            let qi = &Qi(schema, from);
            let qi_subzero_source = &Qi("", "subzero_source");
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|&r| if r == "*" { "*".to_string() } else { fmt_identity(r) })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            (
                Some(
                    sql(" subzero_source as ( ")
                        + " delete from "
                        + fmt_qi(qi)
                        + " "
                        + if !where_.conditions.is_empty() {
                            "where " + fmt_condition_tree(qi, where_)?
                        } else {
                            sql("")
                        }
                        + " returning "
                        + returned_columns
                        + " )",
                ),
                if return_representation {
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
                } else {
                    sql(format!(" select * from {}", fmt_identity("subzero_source")))
                },
            )
        }
        Update {
            table,
            columns,
            payload,
            where_,
            check,
            returning,
            select,
        } => {
            let qi = &Qi(schema, table);
            let qi_subzero_source = &Qi("", "subzero_source");
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|&r| {
                        if r == "*" {
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
                if columns.is_empty() {
                    let sel = if returning.is_empty() {
                        "null".to_string()
                    } else {
                        returning
                            .iter()
                            .map(|&r| if r == "*" { format!("{table}.*") } else { format!("{table}.{r}") })
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    Some(sql(format!(
                        " subzero_source as (select {}, true as _subzero_check__constraint from {} where false )",
                        sel,
                        fmt_qi(qi)
                    )))
                } else {
                    Some(
                        fmt_body(payload)
                            + ", subzero_source as ( "
                            + " update "
                            + fmt_qi(qi)
                            + " set "
                            + set_columns
                            + " from (select * from json_populate_recordset (null::"+ fmt_qi(qi)+ " , (select val from subzero_body) )) _ "
                            + " "
                            + if !where_.conditions.is_empty() {
                                "where " + fmt_condition_tree(qi, where_)?
                            } else {
                                sql("")
                            }
                            + " returning "
                            + returned_columns
                            // for each row add a column if it passes the internal permissions check defined for the schema
                            + if !check.conditions.is_empty() { ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "} else { sql(", true as _subzero_check__constraint ") }
                            + " )",
                    )
                },
                if return_representation {
                    " select "
                        + select.join(", ")
                        + " from "
                        + fmt_identity("subzero_source")
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    //" " + if !where_.conditions.is_empty() { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
                } else {
                    sql(format!(" select * from {}", fmt_identity("subzero_source")))
                },
            )
        }
    };

    Ok(match wrapin_cte {
        Some(cte_name) => match cte_snippet {
            Some(cte) => " " + cte + " , " + format!("{cte_name} as ( ") + query_snippet + " )",
            None => format!(" {cte_name} as ( ") + query_snippet + " )",
        },
        None => match cte_snippet {
            Some(cte) => " " + cte + query_snippet,
            None => query_snippet,
        },
    })
}
fmt_env_query!();
fmt_count_query!();
fmt_body!();
fmt_condition_tree!();
fmt_condition!();
fmt_env_var!();
macro_rules! fmt_in_filter {
    ($p:ident) => {
        fmt_operator(&"= any")? // + ("( select value from json_each(" + param($p) + ") )")
        + ("(select * from json_table(" + param($p) + ", '$[*]' columns (val text path '$')) as t)")
    };
}
fmt_filter!();
fmt_select_name!();
fmt_function_call!();
fmt_select_item_function!();
fmt_select_item!();
fmt_function_param!();
//fmt_sub_select_item!();
fn fmt_sub_select_item<'a, 'b>(schema: &'a str, qi: &'b Qi<'b>, i: &'a SubSelect) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
    let SubSelect { query, alias, join, .. } = i;
    match join {
        Some(j) => match j {
            Parent(fk) => {
                let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                let subquery = fmt_query(schema, true, None, query, join)?;

                Ok((
                    sql(format!("'{}', {}.row_", alias_or_name, fmt_identity(&local_table_name))),
                    vec!["left join lateral (" + subquery + ") as " + sql(fmt_identity(&local_table_name)) + " on true"],
                ))
            }
            Child(fk) => {
                let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk.table.1));
                let local_table_name = fmt_identity(fk.table.1);
                let subquery = fmt_query(schema, true, None, query, join)?;
                Ok((
                    ("coalesce((select json_agg("
                        + sql(local_table_name.clone())
                        + ".*) from ("
                        + subquery
                        + ") as "
                        + sql(local_table_name)
                        + "), '[]') as "
                        + sql(alias_or_name)),
                    vec![],
                ))
            }
            Many(_table, _fk1, fk2) => {
                let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk2.referenced_table.1));
                let local_table_name = fmt_identity(fk2.referenced_table.1);
                let subquery = fmt_query(schema, true, None, query, join)?;
                Ok((
                    ("coalesce((select json_agg("
                        + sql(local_table_name.clone())
                        + ".*) from ("
                        + subquery
                        + ") as "
                        + sql(local_table_name)
                        + "), '[]') as "
                        + sql(alias_or_name)),
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
fn fmt_identity(i: &str) -> String { String::from("`") + i.chars().take_while(|x| x != &'\0').collect::<String>().replace('`', "``").as_str() + "`" }
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
