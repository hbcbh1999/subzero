use crate::api::{
    *,
    Condition::*, ContentType::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*,
    LogicOperator::*, QueryNode::*, SelectItem::*,
};
use crate::dynamic_statement::{param, sql, JoinIterator, SqlSnippet};
use rusqlite::{ToSql, types::{ToSqlOutput, Value, Value::*, ValueRef}, Result,};
use std::rc::Rc;
use super::base::{
    //fmt_main_query,
    //fmt_query,
    fmt_count_query,
    fmt_body,
    fmt_condition_tree,
    fmt_condition,
    //fmt_filter,
    //fmt_select_item,
    //fmt_sub_select_item,
    fmt_operator,
    fmt_logic_operator,
    fmt_identity,
    //fmt_qi,
    fmt_field,
    fmt_order,
    fmt_order_term,
    fmt_as,
    //fmt_as_name,
    fmt_limit,
    fmt_offset,
    fmt_json_path,
    fmt_json_operation,
    fmt_json_operand,
    return_representation,
};

impl ToSql for ListVal {
    fn to_sql(&self) ->Result<ToSqlOutput<'_>> {
        match self {
            ListVal(v) => {
                Ok(ToSqlOutput::Array(
                    Rc::new(v.into_iter().map(|v| Value::from(v.clone())).collect())
                ))
            }
        }
    }
}

impl ToSql for SingleVal {
    fn to_sql(&self) ->Result<ToSqlOutput<'_>> {
        match self {
            SingleVal(v) => {
                Ok(ToSqlOutput::Borrowed(ValueRef::Text(v.as_bytes())))
            }
        }
    }
}

impl ToSql for Payload {
    fn to_sql(&self) ->Result<ToSqlOutput<'_>> {
        match self {
            Payload(v) => {
                Ok(ToSqlOutput::Owned(Text(v.clone())))
            }
        }
    }
}

// helper type aliases
type SqlParam<'a> = (dyn ToSql + Sync + 'a);
type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;


//fmt_main_query!();
pub fn fmt_main_query<'a>(schema: &String, request: &'a ApiRequest) -> Snippet<'a> {
    let count = match &request.preferences {
        Some(Preferences {
            count: Some(Count::ExactCount),
            ..
        }) => true,
        _ => false,
    };

    let return_representation = return_representation(request);
    let body_snippet = match (
        return_representation,
        &request.accept_content_type,
        &request.query.node,
    ) {
        (false, _, _) => "''",
        (
            true,
            SingularJSON,
            FunctionCall {
                is_scalar: true, ..
            },
        )
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
        (
            true,
            SingularJSON,
            FunctionCall {
                is_scalar: false, ..
            },
        )
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

    fmt_query(
        schema,
        return_representation,
        Some("_subzero_query"),
        &request.query,
        &None,
    ) + " , "
        + if count {
            fmt_count_query(schema, Some("_subzero_count_query"), &request.query)
        } else {
            sql("_subzero_count_query as (select 1)")
        }
        + " select"
        + " count(_subzero_t.row) AS page_total, "
        + if count {
            "(SELECT count(*) FROM _subzero_count_query)"
        } else {
            "null"
        }
        + " as total_result_set, "
        + body_snippet
        + " as body, "
        + " null as response_headers, "
        + " null as response_status "
        + " from ( select * from _subzero_query ) _subzero_t"
}
//fmt_query!();
fn fmt_query<'a>(
    schema: &String,
    return_representation: bool,
    wrapin_cte: Option<&'static str>,
    q: &'a Query,
    _join: &Option<Join>,
) -> Snippet<'a> {
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
            let bb: &(dyn ToSql + Sync + 'a) = payload;
            let (params_cte, arg_frag): (Snippet<'a>, Snippet<'a>) = match &parameters {
                CallParams::OnePosParam(_p) => (sql(" "), param(bb)),
                CallParams::KeyParams(p) if p.len() == 0 => (sql(" "), sql("")),
                CallParams::KeyParams(prms) => (
                    fmt_body(payload)
                        + ", subzero_args as ( "
                        + "select * from json_to_recordset((select val from subzero_body)) as _("
                        + prms
                            .iter()
                            .map(|p| format!("{} {}", fmt_identity(&p.name), p.type_))
                            .collect::<Vec<_>>()
                            .join(", ")
                        + ")"
                        + " ), ",
                    sql(prms
                        .iter()
                        .map(|p| {
                            let variadic = if p.variadic { "variadic" } else { "" };
                            let ident = fmt_identity(&p.name);
                            if *is_multiple_call {
                                format!("{} {} := subzero_args.{}", variadic, ident, ident)
                            } else {
                                format!(
                                    "{} {}  := (select {} from subzero_args limit 1)",
                                    variadic, ident, ident
                                )
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ")),
                ),
            };
            let call_it = fmt_qi(fn_name) + "(" + arg_frag + ")";
            let returned_columns = if returning.len() == 0 {
                "*".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            format!("*")
                        } else {
                            format!("{}.{}", fmt_identity(&fn_name.1), fmt_identity(r))
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let args_body = if *is_multiple_call {
                if *is_scalar {
                    "select " + call_it + "subzero_scalar from subzero_args"
                } else {
                    format!(
                        "select subzero_lat_args.* from subzero_args, lateral ( select {} from ",
                        returned_columns
                    ) + call_it
                        + " ) subzero_lat_args"
                }
            } else {
                if *is_scalar {
                    "select " + call_it + " as subzero_scalar"
                } else {
                    format!("select {} from ", returned_columns) + call_it
                }
            };

            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .unzip();
            select.extend(sub_selects.into_iter());
            (
                Some(params_cte + " subzero_source as ( " + args_body + " )"),
                " select "
                    + select.join(", ")
                    + " from "
                    + fmt_identity(&"subzero_source".to_string())
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if where_.conditions.len() > 0 {
                        "where " + fmt_condition_tree(qi_subzero_source, where_)
                    } else {
                        sql("")
                    }
                    + " "
                    + fmt_order(qi_subzero_source, order)
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
        } => {
            let (qi, from_snippet) = match table_alias {
                Some(a) => (
                    Qi("".to_string(), a.clone()),
                    format!(
                        "{} as {}",
                        fmt_qi(&Qi(schema.clone(), table.clone())),
                        fmt_identity(&a)
                    ),
                ),
                None => (
                    Qi(schema.clone(), table.clone()),
                    fmt_qi(&Qi(schema.clone(), table.clone())),
                ),
            };
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(&qi, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, &qi, s))
                .unzip();
            select.extend(sub_selects.into_iter());

            (
                None,
                sql(" select ")
                    + " json_object(" + select.join(", ") + ") as row"
                    + " from "
                    + from_snippet
                    + " "
                    + if join_tables.len() > 0 {
                        format!(
                            ", {}",
                            join_tables
                                .iter()
                                .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    } else {
                        format!("")
                    }
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if where_.conditions.len() > 0 {
                        "where " + fmt_condition_tree(&qi, where_)
                    } else {
                        sql("")
                    }
                    + " "
                    + fmt_order(&qi, order)
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
            select,
            on_conflict,
        } => {
            let qi = &Qi(schema.clone(), into.clone());
            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            format!("*")
                        } else {
                            fmt_identity(r)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let into_columns = if columns.len() > 0 {
                format!(
                    "({})",
                    columns
                        .iter()
                        .map(fmt_identity)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            } else {
                format!("")
            };
            let select_columns = columns
                .iter()
                .map(fmt_identity)
                .collect::<Vec<_>>()
                .join(",");
            (
                Some(
                    fmt_body(payload)+
                    ", subzero_source as ( " + 
                    " insert into " + fmt_qi(qi) + " " +into_columns +
                    " select " + select_columns +
                    " from json_populate_recordset(null::" + fmt_qi(qi) + ", (select val from subzero_body)) _ " +
                    " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&Qi("".to_string(), "_".to_string()), where_) } else { sql("") } + // this line is only relevant for upsert
                    match on_conflict {
                        Some((r,cols)) if cols.len()>0 => {
                            let on_c = format!("on conflict({})",cols.iter().map(fmt_identity).collect::<Vec<_>>().join(", "));
                            let on_do = match (r, columns.len()) {
                                (Resolution::IgnoreDuplicates, _) |
                                (_, 0) => format!("do nothing"),
                                _ => format!(
                                    "do update set {}",
                                    columns.iter().map(|c|
                                        format!("{} = excluded.{}", fmt_identity(c), fmt_identity(c))
                                    ).collect::<Vec<_>>().join(", ")
                                )
                            };
                            format!("{} {}", on_c, on_do)
                        },
                        _ => format!("")
                    } +
                    " returning " + returned_columns +
                    " )",
                ),
                if return_representation {
                    " select "
                        + select.join(", ")
                        + " from "
                        + fmt_identity(&"subzero_source".to_string())
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                        + " "
                        + if where_.conditions.len() > 0 {
                            "where " + fmt_condition_tree(qi_subzero_source, where_)
                        } else {
                            sql("")
                        }
                } else {
                    sql(format!(
                        " select * from {}",
                        fmt_identity(&"subzero_source".to_string())
                    ))
                },
            )
        }
        Delete {
            from,
            where_,
            returning,
            select,
        } => {
            let qi = &Qi(schema.clone(), from.clone());
            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            format!("*")
                        } else {
                            fmt_identity(r)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            (
                Some(
                    sql(" subzero_source as ( ")
                        + " delete from "
                        + fmt_qi(qi)
                        + " "
                        + if where_.conditions.len() > 0 {
                            "where " + fmt_condition_tree(&qi, where_)
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
                        + fmt_identity(&"subzero_source".to_string())
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                        + " "
                        + if where_.conditions.len() > 0 {
                            "where " + fmt_condition_tree(qi_subzero_source, where_)
                        } else {
                            sql("")
                        }
                } else {
                    sql(format!(
                        " select * from {}",
                        fmt_identity(&"subzero_source".to_string())
                    ))
                },
            )
        }
        Update {
            table,
            columns,
            payload,
            where_,
            returning,
            select,
        } => {
            let qi = &Qi(schema.clone(), table.clone());
            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
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
                if columns.len() == 0 {
                    let sel = if returning.len() == 0 {
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
                    Some(sql(format!(
                        " subzero_source as (select {} from {} where false )",
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
                            + " from (select * from json_populate_recordset (null::"
                            + fmt_qi(qi)
                            + " , (select val from subzero_body) )) _ "
                            + " "
                            + if where_.conditions.len() > 0 {
                                "where " + fmt_condition_tree(&qi, where_)
                            } else {
                                sql("")
                            }
                            + " returning "
                            + returned_columns
                            + " )",
                    )
                },
                if return_representation {
                    " select "
                        + select.join(", ")
                        + " from "
                        + fmt_identity(&"subzero_source".to_string())
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    //" " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
                } else {
                    sql(format!(
                        " select * from {}",
                        fmt_identity(&"subzero_source".to_string())
                    ))
                },
            )
        }
    };

    match wrapin_cte {
        Some(cte_name) => match cte_snippet {
            Some(cte) => {
                " with " + cte + " , " + format!("{} as ( ", cte_name) + query_snippet + " )"
            }
            None => format!(" with {} as ( ", cte_name) + query_snippet + " )",
        },
        None => match cte_snippet {
            Some(cte) => " with " + cte + query_snippet,
            None => query_snippet,
        },
    }
}
fmt_count_query!();
fmt_body!();
fmt_condition_tree!();
fmt_condition!();
//fmt_filter!();
fn fmt_filter<'a>(f: &'a Filter) -> Snippet<'a> {
    match f {
        Op(o, v) => {
            let vv: &(dyn ToSql + Sync) = v;
            fmt_operator(o) + param(vv)
        }
        In(l) => {
            let ll: &(dyn ToSql + Sync) = l;
            fmt_operator(&"in".to_string()) + ("( select * from rarray(" + param(ll) + ") )")
        }
        Is(v) => {
            let vv = match v {
                TrileanVal::TriTrue => "true",
                TrileanVal::TriFalse => "false",
                TrileanVal::TriNull => "null",
                TrileanVal::TriUnknown => "unknown",
            };
            sql(format!("is {}", vv))
        }
        Fts(o, lng, v) => {
            let vv: &(dyn ToSql + Sync) = v;
            match lng {
                Some(l) => {
                    let ll: &(dyn ToSql + Sync) = l;
                    //println!("==formating {:?} {:?} {:?}", o, ll, vv);
                    fmt_operator(o) + ("(" + param(ll) + "," + param(vv) + ")")
                }
                None => fmt_operator(o) + ("(" + param(vv) + ")"),
            }
        }
        Col(qi, fld) => sql(format!("= {}", fmt_field(qi, fld))),
    }
}
//fmt_select_item!();
fn fmt_select_name(name: &String, json_path: &Option<Vec<JsonOperation>>, alias: &Option<String>) -> String {
    match (name, json_path, alias) {
        (n, Some(jp), None) => match jp.last() {
            Some(JArrow(JKey(k))) | Some(J2Arrow(JKey(k))) => format!("{}",&k),
            Some(JArrow(JIdx(_))) | Some(J2Arrow(JIdx(_))) => format!("{}",
                jp.iter()
                    .rev()
                    .find_map(|i| match i {
                        J2Arrow(JKey(k)) | JArrow(JKey(k)) => Some(k),
                        _ => None,
                    })
                    .unwrap_or(n)
                
            ),
            None => format!("{}",n),
        },
        (_, _, Some(aa)) => format!("{}",aa),
        (n, None, None) => format!("{}",n),
    }
}
fn fmt_select_item<'a>(qi: &Qi, i: &'a SelectItem) -> Snippet<'a> {
    match i {
        Star => sql(format!("{}.*", fmt_qi(qi))),
        Simple {
            field: field @ Field { name, json_path },
            alias,
            cast: None,
        } => sql(format!(
            "'{}', {}",
            fmt_select_name(name, json_path, alias),
            fmt_field(qi, field),
        )),
        Simple {
            field: field @ Field { name, json_path },
            alias,
            cast: Some(cast),
        } => sql(format!(
            "'{}', cast({} as {})",
            fmt_select_name(name, json_path, alias),
            fmt_field(qi, field),
            cast,
        )),
    }
}
//fmt_sub_select_item!();
fn fmt_sub_select_item<'a>(
    schema: &String,
    qi: &Qi,
    i: &'a SubSelect,
) -> (Snippet<'a>, Vec<Snippet<'a>>) {
    match i {
        SubSelect {
            query, alias, join, ..
        } => match join {
            Some(j) => match j {
                Parent(fk) => {
                    let alias_or_name =  format!("'{}'",alias.as_ref().unwrap_or(&fk.referenced_table.1));
                    let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                    let subquery = fmt_query(schema, true, None, query, join);

                    (
                        (
                            sql(alias_or_name) + ", "
                            + "("
                            + subquery
                            + ")"
                        ),
                        vec![],
                    )
                }
                Child(fk) => {
                    let alias_or_name = format!("'{}'",alias.as_ref().unwrap_or(&fk.table.1));
                    let local_table_name = fmt_identity(&fk.table.1);
                    let subquery = fmt_query(schema, true, None, query, join);
                    (
                        (
                            sql(alias_or_name) + ", "
                            + "("
                            + " select json_group_array(" + local_table_name.clone() +".row)"
                            + " from ("
                            + subquery
                            + " ) as " + local_table_name
                            + ")"
                        ),
                        vec![],
                    )
                    // (
                    //     ("coalesce((select json_agg("
                    //         + sql(local_table_name.clone())
                    //         + ".*) from ("
                    //         + subquery
                    //         + ") as "
                    //         + sql(local_table_name.clone())
                    //         + "), '[]') as "
                    //         + sql(alias_or_name)),
                    //     vec![],
                    // )
                }
                Many(_table, _fk1, fk2) => {
                    let alias_or_name =
                        fmt_identity(alias.as_ref().unwrap_or(&fk2.referenced_table.1));
                    let local_table_name = fmt_identity(&fk2.referenced_table.1);
                    let subquery = fmt_query(schema, true, None, query, join);
                    (
                        ("coalesce((select json_agg("
                            + sql(local_table_name.clone())
                            + ".*) from ("
                            + subquery
                            + ") as "
                            + sql(local_table_name.clone())
                            + "), '[]') as "
                            + sql(alias_or_name)),
                        vec![],
                    )
                }
            },
            None => panic!("unable to format join query without matching relation"),
        },
    }
}

fmt_operator!();
// fn fmt_operator(o: &Operator) -> String {
//     match o.as_str() {
//         "= any" => format!(" in "),
//         oo => format!("{} ", oo),
//     }
    
// }
fmt_logic_operator!();
fmt_identity!();
//fmt_qi!();
fn fmt_qi(qi: &Qi) -> String {
    match (qi.0.as_str(), qi.1.as_str()) {
        ("", _) => format!("{}", fmt_identity(&qi.1)),
        ("_sqlite_public_", _) => format!("{}", fmt_identity(&qi.1)),
        _ => panic!("there is no support for 'schema' as a namespace in sqlite"),
    }
}
fmt_field!();
fmt_order!();
fmt_order_term!();
fmt_as!();
fmt_limit!();
fmt_offset!();
fmt_json_path!();
fmt_json_operation!();
fmt_json_operand!();
