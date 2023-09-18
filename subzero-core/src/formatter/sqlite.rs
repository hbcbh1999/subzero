use super::base::{
    fmt_as,
    //fmt_body,
    fmt_condition,
    fmt_condition_tree,
    //fmt_main_query_internal,
    fmt_main_query,
    //fmt_query,
    fmt_count_query,
    fmt_field,
    //fmt_env_var,
    fmt_filter,
    fmt_identity,
    fmt_json_path,
    //fmt_as_name,
    fmt_limit,
    fmt_logic_operator,
    fmt_offset,
    //fmt_sub_select_item,
    //fmt_operator,
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
    get_body_snippet,
};
use crate::schema::DbSchema;
use std::collections::HashMap;
pub use super::base::return_representation;
use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet, SqlSnippetChunk, generate_fn};
use crate::error::{Result, Error};
use super::{ToParam, Snippet, SqlParam};
use std::borrow::Cow;
macro_rules! param_placeholder_format {
    () => {
        "?{pos:.0}{data_type:.0}"
    };
}
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

macro_rules! body_snippet {
    (json_array) => {
        "json_group_array(json(_subzero_t.row))"
    };
    (json_object) => {
        "coalesce(json_group_array(json(_subzero_t.row))->0, 'null')"
    };
    (csv) => {
        "''"
    }; // TODO!! unimplemented
    (function_scalar) => {
        "''"
    }; // unreachable
    (function_scalar_array) => {
        "''"
    }; // unreachable
    (function_any) => {
        "''"
    }; // unreachable
}

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

    let return_representation = return_representation(method, query, preferences);
    let body_snippet = get_body_snippet!(return_representation, accept_content_type, query)?;
    let run_unwrapped_query = matches!(query.node, Insert { .. } | Update { .. } | Delete { .. });
    let has_payload_cte = matches!(query.node, Insert { .. } | Update { .. });
    let wrap_cte_name = if run_unwrapped_query { None } else { Some("_subzero_query") };
    let source_query = fmt_query(db_schema, schema, return_representation, wrap_cte_name, query, &None)?;
    let main_query = if run_unwrapped_query {
        "with env as materialized (" + fmt_env_query(env) + ") " + if has_payload_cte { ", " } else { "" } + source_query
    } else {
        "with env as materialized ("
            + fmt_env_query(env)
            + "), "
            + source_query
            + " , "
            + if count {
                fmt_count_query(db_schema, schema, Some("_subzero_count_query"), query)?
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

pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select "
        + if env.is_empty() {
            sql("null")
        } else {
            env.iter()
                .map(|(k, v)| param(v as &SqlParam) + " as " + fmt_identity(&String::from(*k)))
                .join(",")
        }
}

//fmt_query!();
pub fn fmt_query<'a>(
    db_schema: &'a DbSchema<'_>, schema: &'a str, _return_representation: bool, wrapin_cte: Option<&'static str>, q: &'a Query, _join: &Option<Join>,
) -> Result<Snippet<'a>> {
    let add_env_tbl_to_from = wrapin_cte.is_some();

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
            ..
        } => {
            //let table_alias = table_alias_suffix.map(|s| format!("{}{}", table, s)).unwrap_or_default();
            let (qi, from_snippet) = match table_alias {
                None => (Qi(schema, table), fmt_qi(&Qi(schema, table))),
                Some(a) => (
                    Qi("", a),
                    format!("{} as {}", fmt_qi(&Qi(schema, table)), fmt_identity(a)),
                ),
            };
            if select.iter().any(|s| matches!( s, Star)) {
                return Err(Error::UnsupportedFeature {message: "'select *' not supported, use explicit select parameters".to_string()})
            }
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(&qi, s)).collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(db_schema, schema, &qi, s))
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
                    + if add_env_tbl_to_from { ", env " } else { "" }
                    + " "
                    + if !join_tables.is_empty() {
                        format!(
                            ", {}",
                            join_tables
                                .iter()
                                .map(|f| fmt_qi(&Qi(schema, f)))
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
            check,
            where_,
            returning,
            on_conflict,
            .. //select
        } => {
            let qi = &Qi(schema, into);
            let returned_columns = if returning.is_empty() {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|&r| if r == "*" { "*".to_string() } else { fmt_identity(r) })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let into_columns = if !columns.is_empty() {
                format!("({})", columns.iter().map(|c| fmt_identity(c)).collect::<Vec<_>>().join(","))
            } else {
                String::new()
            };
            let select_columns = columns.iter().map(|c| fmt_identity(c)).collect::<Vec<_>>().join(",");
            (
                None,
                fmt_body(payload, columns) +
                " insert into " + fmt_qi(qi) + " " +into_columns +
                " select " + select_columns +
                " from subzero_body _ " +
                // where is only relevant for upsert
                if !where_.conditions.is_empty(){"where " + fmt_condition_tree(&Qi("", "_"), where_)?} else { sql(" where true ") } + 
                match on_conflict {
                    Some((r,cols)) if !cols.is_empty() => {
                        let on_c = format!("on conflict({})",cols.iter().map(|c| fmt_identity(c)).collect::<Vec<_>>().join(", "));
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
                if !check.conditions.is_empty() { ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "} else { sql(", 1  as _subzero_check__constraint ") }
            )
        }
        Delete {
            from,
            where_,
            returning,
            .. //select,
        } => {
            let qi = &Qi(schema, from);
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


        }
        Update {
            table,
            columns,
            payload,
            check,
            where_,
            returning,
            ..//select,
        } => {
            let qi = &Qi(schema, table);
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
                None,
                if columns.is_empty() {
                    let sel = if returning.is_empty() {
                        "null".to_string()
                    } else {
                        returning
                            .iter()
                            .map(|&r| {
                                if r == "*" {
                                    format!("{table}.*")
                                } else {
                                    format!("{table}.{r}")
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    };

                    sql(format!(" select {} from {} where false ", sel, fmt_qi(qi)))
                } else {
                        fmt_body(payload, columns)
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
                        + returned_columns +
                        // for each row add a column if it passes the internal permissions check defined for the schema
                        if !check.conditions.is_empty() { ", " + fmt_condition_tree(qi, check)? + " as _subzero_check__constraint "} else { sql(", 1 as _subzero_check__constraint ") }
                }
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
fmt_count_query!();
//fmt_body!();
#[rustfmt::skip]
fn fmt_body<'a>(payload: &'a Payload, columns: &'a [&'a str]) -> Snippet<'a> {
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
        fmt_operator(&"in")? + ("( select value from json_each(" + param($p) + ") )")
    };
}
//fmt_env_var!();
fn fmt_env_var(e: &'_ EnvVar) -> String {
    match e {
        EnvVar { var, part: None } => format!("(select {} from env)", fmt_identity(var)),
        EnvVar { var, part: Some(part) } => format!("(select json({})->>'{}' from env)", fmt_identity(var), part),
    }
}
fmt_filter!();
fmt_select_name!();
fmt_function_call!();
//fmt_select_item_function!();
fn fmt_select_item_function<'a>(
    qi: &Qi, fn_name: &str, parameters: &'a [FunctionParam], partitions: &'a Vec<Field>, orders: &'a Vec<OrderTerm>, alias: &'a Option<&str>,
) -> Result<Snippet<'a>> {
    Ok(format!("'{}', ", fmt_select_name(fn_name, &None, alias).unwrap_or_default())
        + sql(fmt_identity(fn_name))
        + "("
        + parameters
            .iter()
            .map(|p| fmt_function_param(qi, p))
            .collect::<Result<Vec<_>>>()?
            .join(",")
        + ")"
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
fn fmt_sub_select_item<'a>(db_schema: &'a DbSchema<'_>, schema: &'a str, _qi: &Qi, i: &'a SubSelect) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
    let SubSelect { query, alias, join, .. } = i;
    match join {
        Some(j) => match j {
            Parent(fk) => {
                let alias_or_name = format!("'{}'", alias.as_ref().unwrap_or(&fk.referenced_table.1));
                //let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                let subquery = fmt_query(db_schema, schema, true, None, query, join)?;

                Ok(((sql(alias_or_name) + ", " + "(" + subquery + ")"), vec![]))
            }
            Child(fk) => {
                let alias_or_name = format!("'{}'", alias.as_ref().unwrap_or(&fk.table.1));
                let local_table_name = fmt_identity(fk.table.1);
                let subquery = fmt_query(db_schema, schema, true, None, query, join)?;
                Ok((
                    (sql(alias_or_name)
                        + ", "
                        + "("
                        + " select json_group_array(json("
                        + local_table_name.clone()
                        + ".row))"
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
                let local_table_name = fmt_identity(fk2.referenced_table.1);
                let subquery = fmt_query(db_schema, schema, true, None, query, join)?;
                Ok((
                    (sql(alias_or_name)
                        + ", "
                        + "("
                        + " select json_group_array(json("
                        + local_table_name.clone()
                        + ".row))"
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

//fmt_operator!();
fn fmt_operator<'a>(o: &'a Operator<'a>) -> Result<String> {
    // match on the operator and return the sqlite equivalent

    Ok(String::from(match *o {
        "ilike" => "like",
        _ => o,
    }) + " ")
}
fmt_logic_operator!();
fmt_identity!();
//fmt_qi!();
fn fmt_qi(qi: &Qi) -> String {
    match (qi.0, qi.1) {
        ("", "") => String::new(),
        _ => fmt_identity(qi.1),
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
        JKey(k) => k.to_string(),
        JIdx(i) => format!("[{i}]"),
    }
}
