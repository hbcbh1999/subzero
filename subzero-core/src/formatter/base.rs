use crate::api::{Preferences, QueryNode::*, Representation, Query};

#[allow(unused_macros)]
macro_rules! fmt_field_format {
    () => {
        "to_jsonb({}{}{}){}"
    };
}
#[allow(unused_imports)]
pub(super) use fmt_field_format;

#[allow(unused_macros)]
macro_rules! star_select_item_format {
    () => {
        "{}.*"
    };
}
#[allow(unused_imports)]
pub(super) use star_select_item_format;

#[allow(unused_macros)]
macro_rules! simple_select_item_format {
    () => {
        "{field}{as}{select_name:.0}"
    };
}
#[allow(unused_imports)]
pub(super) use simple_select_item_format;

#[allow(unused_macros)]
macro_rules! cast_select_item_format {
    () => {
        "cast({field} as {cast}){as}{select_name:.0}"
    };
}
#[allow(unused_imports)]
pub(super) use cast_select_item_format;

#[allow(unused_macros)]
macro_rules! fmt_main_query_internal { () => {
pub fn fmt_main_query_internal<'a>(schema_str: &'a str, method: &'a str, accept_content_type: &ContentType, query: &'a Query, preferences: &'a Option<Preferences>, env: &'a HashMap<&'a str, &'a str>) -> Result<Snippet<'a>> {
    let schema = String::from(schema_str);
    let count = match preferences {
        Some(Preferences {
            count: Some(Count::ExactCount),
            ..
        }) => true,
        _ => false,
    };

    let check_constraints = matches!(query.node, Insert{..} | Update{..} );
    let return_representation = return_representation(method, query, preferences);
    let body_snippet = match (
        return_representation,
        accept_content_type,
        &query.node,
    ) {
        (false, _, _) => Ok("''"),
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
        ) => Ok("coalesce((json_agg(_subzero_t.subzero_scalar)->0)::text, 'null')"),
        (
            true,
            ApplicationJSON,
            FunctionCall {
                returns_single: false,
                is_multiple_call: false,
                is_scalar: true,
                ..
            },
        ) => Ok("coalesce((json_agg(_subzero_t.subzero_scalar))::text, '[]')"),
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
        ) => Ok("coalesce((json_agg(_subzero_t)->0)::text, 'null')"),

        (true, ApplicationJSON, _) => Ok("coalesce(json_agg(_subzero_t), '[]')::character varying"),
        (true, SingularJSON, _) => Ok("coalesce((json_agg(_subzero_t)->0)::text, 'null')"),
        (true, TextCSV, _) => {
            Ok(r#"
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
        "#)
        },
        (_,Other(t),_) => Err(Error::ContentTypeError {
            message: format!("None of these Content-Types are available: {}", t),
        }),
    }?;

    //let env = env_map.iter().map(|(k,v)| (k.to_string(), v.to_string())).collect::<HashMap<_,_>>();
    Ok(
        sql("with")
        + " env as materialized (" + fmt_env_query(&env)+ ")"
        + " , "
        + fmt_query(
            &schema,
            return_representation,
            Some("_subzero_query"),
            query,
            &None,
        )?  + " , "

        + if count {
            fmt_count_query(&schema, Some("_subzero_count_query"), query)?
        } else {
            sql("_subzero_count_query AS (select 1)")
        }
        + " select"
        + " pg_catalog.count(_subzero_t) as page_total, "
        + if count {
            "(SELECT pg_catalog.count(*) FROM _subzero_count_query)"
        } else {
            "null::bigint"
        }
        + " as total_result_set, "
        + body_snippet + " as body, "
        + if check_constraints {"(select coalesce(bool_and(_subzero_check__constraint),true) from subzero_source) as constraints_satisfied, "
        } else {"true as constraints_satisfied, "}
        + " nullif(current_setting('response.headers', true), '') as response_headers, "
        + " nullif(current_setting('response.status', true), '') as response_status "

        + " from ( select * from _subzero_query ) _subzero_t"
    )
}
}}
#[allow(unused_imports)]
pub(super) use fmt_main_query_internal;

#[allow(unused_macros)]
macro_rules! fmt_main_query {
    () => {
        pub fn fmt_main_query<'a>(schema_str: &'a str, request: &'a ApiRequest, env: &'a HashMap<&'a str, &'a str>) -> Result<Snippet<'a>> {
            fmt_main_query_internal(schema_str, &request.method, &request.accept_content_type, &request.query, &request.preferences, env)
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_main_query;

#[allow(unused_macros)]
macro_rules! fmt_env_query {
    () => {
        pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
            "select "
                + if env.is_empty() {
                    sql("null")
                } else {
                    env.iter()
                        .map(|(k, v)| {
                            // "set_config("
                            //     + param(k as &SqlParam)
                            //     + ", "
                            //     + param(v as &SqlParam)
                            //     + ", true), "
                            param(v as &SqlParam) + " as " + fmt_identity(&String::from(*k))
                        })
                        .join(",")
                }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_env_query;

#[allow(unused_macros)]
macro_rules! fmt_query { () => {
pub fn fmt_query<'a>(
    schema: &String,
    return_representation: bool,
    wrapin_cte: Option<&'static str>,
    q: &'a Query,
    _join: &Option<Join>,
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
            let bb: &SqlParam = payload;
            let (params_cte, arg_frag): (Snippet<'a>, Snippet<'a>) = match &parameters {
                CallParams::OnePosParam(_p) => (sql(" "), param(bb)),
                CallParams::KeyParams(p) if p.len() == 0 => (sql(" "), sql("")),
                CallParams::KeyParams(prms) => (
                    fmt_body(payload)
                        + ", subzero_args as ( "
                        + "select * from json_to_recordset((select val from subzero_body)) as _("
                        + prms
                            .iter()
                            //.map(|p| format!("{} {}", fmt_identity(&p.name), p.type_))
                            .map(|p| vec![fmt_identity(&p.name), p.type_.clone()].join(" "))
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
            let returned_columns = if returning.len() == 0 {
                "*".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            //format!("{}.*", fmt_identity(&fn_name.1))
                            vec![fmt_identity(&fn_name.1).as_str(), ".*"].join("")
                        } else {
                            //format!("{}.{}", fmt_identity(&fn_name.1), fmt_identity(r))
                            vec![fmt_identity(&fn_name.1), fmt_identity(r)].join(".")
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let args_body = if *is_multiple_call {
                if *is_scalar {
                    "select " + call_it + " subzero_scalar from subzero_args, env"
                } else {
                    format!(
                        "select subzero_lat_args.* from subzero_args, lateral ( select {} from ",
                        returned_columns
                    ) + call_it + ", env"
                        + " ) subzero_lat_args"
                }
            } else {
                if *is_scalar {
                    "select " + call_it + " as subzero_scalar from env"
                } else {
                    format!("select {} from ", returned_columns) + call_it + " , env"
                }
            };

            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?;
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
                    + fmt_identity(&"subzero_source".to_string())
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if where_.conditions.len() > 0 {
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
            let (qi, from_snippet) = match table_alias {
                Some(a) => (
                    Qi("".to_string(), a.clone()),
                    // format!(
                    //     "{} as {}",
                    //     fmt_qi(&Qi(schema.clone(), table.clone())),
                    //     fmt_identity(&a)
                    // ),
                    vec![fmt_qi(&Qi(schema.clone(), table.clone())), fmt_identity(&a)].join(" as "),
                ),
                None => (
                    Qi(schema.clone(), table.clone()),
                    fmt_qi(&Qi(schema.clone(), table.clone())),
                ),
            };
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
                " select "
                    + select.join(", ")
                    + " from "
                    + from_snippet
                    + if add_env_tbl_to_from { ", env " } else { "" }
                    + if join_tables.len() > 0 {
                        // format!(
                        //     ", {}",
                        //     join_tables
                        //         .iter()
                        //         .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                        //         .collect::<Vec<_>>()
                        //         .join(", ")
                        // )
                        String::from(", ") +
                        join_tables
                                .iter()
                                .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                                .collect::<Vec<_>>()
                                .join(", ").as_str()
                    } else {
                        //String::new()
                        String::new()
                    }
                    + " "
                    + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    + " "
                    + if where_.conditions.len() > 0 {
                        "where " + fmt_condition_tree(&qi, where_)?
                    } else {
                        sql("")
                    }
                    + " "
                    + (fmt_groupby(&qi, groupby)?)
                    + (fmt_order(&qi, order)?)
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
            let qi = &Qi(schema.clone(), into.clone());
            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            } else {
                returning
                    .iter()
                    .map(|r| {
                        if r.as_str() == "*" {
                            //format!("*")
                            String::from("*")
                        } else {
                            fmt_identity(r)
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            };

            let into_columns = if columns.len() > 0 {
                // format!(
                //     "({})",
                //     columns
                //         .iter()
                //         .map(fmt_identity)
                //         .collect::<Vec<_>>()
                //         .join(",")
                // )
                String::from("(")
                + columns
                        .iter()
                        .map(fmt_identity)
                        .collect::<Vec<_>>()
                        .join(",").as_str()
                + ")"
            } else {
                //String::new()
                String::new()
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
                    " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&Qi("".to_string(), "_".to_string()), where_)? } else { sql("") } + // this line is only relevant for upsert
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
                        + fmt_identity(&"subzero_source".to_string())
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                        + " "
                        + if where_.conditions.len() > 0 {
                            "where " + fmt_condition_tree(qi_subzero_source, where_)?
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
                .collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
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
                            "where " + fmt_condition_tree(&qi, where_)?
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
                            "where " + fmt_condition_tree(qi_subzero_source, where_)?
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
            check,
            returning,
            select,
        } => {
            let qi = &Qi(schema.clone(), table.clone());
            let qi_subzero_source = &Qi("".to_string(), "subzero_source".to_string());
            let mut select: Vec<_> = select
                .iter()
                .map(|s| fmt_select_item(qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?;
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, qi_subzero_source, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
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
                            + if where_.conditions.len() > 0 {
                                "where " + fmt_condition_tree(&qi, where_)?
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

    Ok(
    match wrapin_cte {
        Some(cte_name) => match cte_snippet {
            Some(cte) => {
                " " + cte + " , " + format!("{} as ( ", cte_name) + query_snippet + " )"
            }
            None => format!(" {} as ( ", cte_name) + query_snippet + " )",
        },
        None => match cte_snippet {
            Some(cte) => " " + cte + query_snippet,
            None => query_snippet,
        },
    }
    )
}
}}
#[allow(unused_imports)]
pub(super) use fmt_query;

#[allow(unused_macros)]
macro_rules! fmt_count_query {
    () => {
        fn fmt_count_query<'a>(schema: &String, wrapin_cte: Option<&'static str>, q: &'a Query) -> Result<Snippet<'a>> {
            let query_snippet = match &q.node {
                FunctionCall { .. } => sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string()))),
                Select {
                    from: (table, _),
                    join_tables,
                    where_,
                    ..
                } => {
                    let qi = &Qi(schema.clone(), table.clone());
                    //let (_, joins): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(&schema, qi, s)).unzip();
                    //let select: Vec<_> = select.iter().map(|s| fmt_select_item(&schema, qi, s)).collect();
                    let (_, joins): (Vec<_>, Vec<_>) = q
                        .sub_selects
                        .iter()
                        .map(|s| fmt_sub_select_item(&schema, qi, s))
                        .collect::<Result<Vec<_>>>()?
                        .into_iter()
                        .unzip();
                    //select.extend(sub_selects.into_iter());
                    sql(" select 1 from ")
                        + vec![table.clone()]
                            .iter()
                            .chain(join_tables.iter())
                            .map(|f| fmt_qi(&Qi(schema.clone(), f.clone())))
                            .collect::<Vec<_>>()
                            .join(", ")
                        + " "
                        + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                        + " "
                        + if where_.conditions.len() > 0 {
                            "where " + fmt_condition_tree(qi, where_)?
                        } else {
                            sql("")
                        }
                }
                Insert { .. } => sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string()))),
                Update { .. } => sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string()))),
                Delete { .. } => sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string()))),
            };

            Ok(match wrapin_cte {
                Some(cte_name) => format!(" {} as ( ", cte_name) + query_snippet + " )",
                None => query_snippet,
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_count_query;

#[allow(unused_macros)]
macro_rules! fmt_body {
    () => {
#[rustfmt::skip]
        fn fmt_body<'a>(payload: &'a Payload) -> Snippet<'a> {
            let payload_param: &SqlParam = payload;
            " subzero_payload as ( select " + param(payload_param) + "::json as json_data ),"
            + " subzero_body as ("
                + " select"
                + " case when json_typeof(json_data) = 'array'"
                + " then json_data"
                + " else json_build_array(json_data)"
                + " end as val"
                + " from subzero_payload"
            + " )"
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_body;

#[allow(unused_macros)]
macro_rules! fmt_condition_tree {
    () => {
        fn fmt_condition_tree<'a>(qi: &Qi, t: &'a ConditionTree) -> Result<Snippet<'a>> {
            match t {
                ConditionTree { operator, conditions } => {
                    //let sep = format!(" {} ", fmt_logic_operator(operator));
                    //let sep = String::from(" ") + fmt_logic_operator(operator) + " ";
                    Ok(conditions
                        .iter()
                        .map(|c| fmt_condition(qi, c))
                        .collect::<Result<Vec<_>>>()?
                        .join(fmt_logic_operator(operator)))
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_condition_tree;

#[allow(unused_macros)]
macro_rules! fmt_condition {
    () => {
        fn fmt_condition<'a>(qi: &Qi, c: &'a Condition) -> Result<Snippet<'a>> {
            Ok(match c {
                Single { field, filter, negate } => {
                    //let fld = sql(format!("{} ", fmt_field(qi, field)?));
                    let fld = sql(fmt_field(qi, field)? + " ");

                    if *negate {
                        "not(" + fld + fmt_filter(filter)? + ")"
                    } else {
                        fld + fmt_filter(filter)?
                    }
                }
                Foreign {
                    left: (l_qi, l_fld),
                    right: (r_qi, r_fld),
                } =>
                //sql(format!("{} = {}", fmt_field(l_qi, l_fld)?, fmt_field(r_qi, r_fld)?)),
                {
                    sql(fmt_field(l_qi, l_fld)? + " = " + fmt_field(r_qi, r_fld)?.as_str())
                }

                Group { negate, tree } => {
                    if *negate {
                        "not(" + fmt_condition_tree(qi, tree)? + ")"
                    } else {
                        "(" + fmt_condition_tree(qi, tree)? + ")"
                    }
                }

                Raw { sql: s } => sql(s.as_str()),
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_condition;

#[allow(unused_macros)]
macro_rules! fmt_in_filter {
    ($p:ident) => {
        fmt_operator(&"= any".to_string())? + ("(" + param($p) + ")")
    };
}
#[allow(unused_imports)]
pub(super) use fmt_in_filter;

#[allow(unused_macros)]
macro_rules! fmt_filter {
    () => {
        fn fmt_filter<'a>(f: &'a Filter) -> Result<Snippet<'a>> {
            Ok(match f {
                Op(o, v) => {
                    let vv: &SqlParam = v;
                    fmt_operator(o)? + param(vv)
                }
                In(l) => {
                    let ll: &SqlParam = l;
                    fmt_in_filter!(ll)
                }
                Is(v) => {
                    let vv = match v {
                        TrileanVal::TriTrue => "true",
                        TrileanVal::TriFalse => "false",
                        TrileanVal::TriNull => "null",
                        TrileanVal::TriUnknown => "unknown",
                    };
                    //sql(format!("is {}", vv))
                    sql(String::from("is ") + vv)
                }
                Fts(o, lng, v) => {
                    let vv: &SqlParam = v;
                    match lng {
                        Some(l) => {
                            let ll: &SqlParam = l;
                            fmt_operator(o)? + ("(" + param(ll) + "," + param(vv) + ")")
                        }
                        None => fmt_operator(o)? + ("(" + param(vv) + ")"),
                    }
                }
                Col(qi, fld) => sql(format!("= {}", fmt_field(qi, fld)?)),
                Env(o, e) => sql(format!("{} {}", fmt_operator(o)?, fmt_env_var(e))),
                // Env(o, EnvVar{var, part:None}) => sql(format!("{} (select \"{}\" from env)",fmt_operator(o)?, var)),
                // Env(o, EnvVar{var, part:Some(part)}) => sql(format!("{} (select \"{}\"::json->>'{}' from env)",fmt_operator(o)?, var, part)),
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_filter;

#[allow(unused_macros)]
macro_rules! fmt_env_var {
    () => {
        fn fmt_env_var<'a>(e: &'a EnvVar) -> String {
            match e {
                EnvVar { var, part: None } => format!("(select {} from env)", fmt_identity(var)),
                EnvVar { var, part: Some(part) } => format!("(select {}::json->>'{}' from env)", fmt_identity(var), part),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_env_var;

#[allow(unused_macros)]
macro_rules! fmt_function_call {
    () => {
        fn fmt_function_call<'a>(qi: &Qi, fn_name: &String, parameters: &'a [FunctionParam]) -> Result<Snippet<'a>> {
            Ok(sql(fmt_identity(fn_name))
                + "("
                + parameters
                    .iter()
                    .map(|p| fmt_function_param(qi, p))
                    .collect::<Result<Vec<_>>>()?
                    .join(",")
                + ")")
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_function_call;

#[allow(unused_macros)]
macro_rules! fmt_select_item_function {
    () => {
        fn fmt_select_item_function<'a>(
            qi: &Qi, fn_name: &String, parameters: &'a [FunctionParam], partitions: &'a Vec<Field>, orders: &'a Vec<OrderTerm>,
            alias: &'a Option<String>,
        ) -> Result<Snippet<'a>> {
            Ok(fmt_function_call(qi, fn_name, parameters)?
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
                        + if orders.is_empty() {
                            "".to_string()
                        } else {
                            fmt_order(qi, orders)?
                        }
                        + " )"
                }
                + fmt_as(fn_name, &None, alias))
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_select_item_function;

#[allow(unused_macros)]
macro_rules! fmt_select_item {
    () => {
        fn fmt_select_item<'a>(qi: &Qi, i: &'a SelectItem) -> Result<Snippet<'a>> {
            match i {
                Star => Ok(sql(format!(star_select_item_format!(), fmt_qi(qi)))),
                Simple {
                    field: field @ Field { name, json_path },
                    alias,
                    cast: None,
                } => Ok(sql(format!(
                    simple_select_item_format!(),
                    field=fmt_field(qi, field)?,
                    as=fmt_as(name, json_path, alias),
                    select_name=fmt_select_name(name, json_path, alias).unwrap_or("".to_string())
                ))),
                Simple {
                    field: field @ Field { name, json_path },
                    alias,
                    cast: Some(cast),
                } => Ok(sql(format!(
                    cast_select_item_format!(),
                    field=fmt_field(qi, field)?,
                    cast=cast,
                    as=fmt_as(name, json_path, alias),
                    select_name=fmt_select_name(name, json_path, alias).unwrap_or("".to_string())
                ))),
                Func {
                    alias,
                    fn_name,
                    parameters,
                    partitions,
                    orders,
                } => fmt_select_item_function(qi, fn_name, parameters, partitions, orders, alias),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_select_item;

#[allow(unused_macros)]
macro_rules! fmt_sub_select_item {
    () => {
        fn fmt_sub_select_item<'a>(schema: &String, qi: &Qi, i: &'a SubSelect) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
            match i {
                SubSelect { query, alias, join, .. } => match join {
                    Some(j) => match j {
                        Parent(fk) => {
                            let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                            let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                            let subquery = fmt_query(schema, true, None, query, join)?;

                            Ok((
                                sql(format!("row_to_json({}.*) as {}", fmt_identity(&local_table_name), fmt_identity(alias_or_name))),
                                vec!["left join lateral (" + subquery + ") as " + sql(fmt_identity(&local_table_name)) + " on true"],
                            ))
                        }
                        Child(fk) => {
                            let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk.table.1));
                            let local_table_name = fmt_identity(&fk.table.1);
                            let subquery = fmt_query(schema, true, None, query, join)?;
                            Ok((
                                ("coalesce((select json_agg("
                                    + sql(local_table_name.clone())
                                    + ".*) from ("
                                    + subquery
                                    + ") as "
                                    + sql(local_table_name.clone())
                                    + "), '[]') as "
                                    + sql(alias_or_name)),
                                vec![],
                            ))
                        }
                        Many(_table, _fk1, fk2) => {
                            let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk2.referenced_table.1));
                            let local_table_name = fmt_identity(&fk2.referenced_table.1);
                            let subquery = fmt_query(schema, true, None, query, join)?;
                            Ok((
                                ("coalesce((select json_agg("
                                    + sql(local_table_name.clone())
                                    + ".*) from ("
                                    + subquery
                                    + ") as "
                                    + sql(local_table_name.clone())
                                    + "), '[]') as "
                                    + sql(alias_or_name)),
                                vec![],
                            ))
                        }
                    },
                    None => panic!("unable to format join query without matching relation"),
                },
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_sub_select_item;

#[allow(unused_macros)]
macro_rules! fmt_operator {
    () => {
        //fn fmt_operator(o: &Operator) -> Result<String> { Ok(format!("{} ", o)) }
        fn fmt_operator(o: &Operator) -> Result<String> { Ok(String::new() + o.as_str() + " ") }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_operator;

#[allow(unused_macros)]
macro_rules! fmt_logic_operator {
    () => {
        fn fmt_logic_operator(o: &LogicOperator) -> &str {
            match o {
                And => " and ",
                Or => " or ",
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_logic_operator;

#[allow(unused_macros)]
macro_rules! fmt_identity {
    () => {
        #[allow(clippy::ptr_arg)]
        fn fmt_identity(i: &String) -> String {
            String::from("\"")
                + i.chars()
                    .take_while(|x| x != &'\0')
                    .collect::<String>()
                    .replace("\"", "\"\"")
                    .as_str()
                + "\""
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_identity;

#[allow(unused_macros)]
macro_rules! fmt_qi {
    () => {
        fn fmt_qi(qi: &Qi) -> String {
            match (qi.0.as_str(), qi.1.as_str()) {
                // (_,"subzero_source") |
                // (_,"subzero_fn_call") |
                ("", "") => String::new(),
                ("", _) | ("_sqlite_public_", _) => fmt_identity(&qi.1),
                //_ => format!("{}.{}", fmt_identity(&qi.0), fmt_identity(&qi.1)),
                _ => vec![fmt_identity(&qi.0), fmt_identity(&qi.1)].join("."),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_qi;

#[allow(unused_macros)]
macro_rules! fmt_field {
    () => {
        fn fmt_field(qi: &Qi, f: &Field) -> Result<String> {
            let sep = match (qi.0.as_str(), qi.1.as_str()) {
                ("", "") => "",
                _ => ".",
            };

            Ok(match f {
                Field {
                    name,
                    json_path: json_path @ Some(_),
                } => format!(fmt_field_format!(), fmt_qi(qi), sep, fmt_identity(&name), fmt_json_path(&json_path)),
                Field { name, json_path: None } => format!("{}{}{}", fmt_qi(qi), sep, fmt_identity(&name)),
            })
            //format!("{}{}", fmt_identity(&f.name), fmt_json_path(&f.json_path))
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_field;

#[allow(unused_macros)]
macro_rules! fmt_function_param {
    () => {
        fn fmt_function_param<'a>(qi: &Qi, p: &'a FunctionParam) -> Result<Snippet<'a>> {
            Ok(match p {
                FunctionParam::Val(v, c) => {
                    let vv: &SqlParam = v;
                    match c {
                        Some(c) => "cast(" + param(vv) + format!(" as {}", c) + ")",
                        None => param(vv),
                    }
                }
                FunctionParam::Fld(f) => sql(fmt_field(qi, f)?),
                FunctionParam::Func { fn_name, parameters } => fmt_function_call(qi, fn_name, parameters)?,
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_function_param;

#[allow(unused_macros)]
macro_rules! fmt_order {
    () => {
        fn fmt_order(qi: &Qi, o: &Vec<OrderTerm>) -> Result<String> {
            Ok(if o.len() > 0 {
                //format!("order by {}", o.iter().map(|t| fmt_order_term(qi, t)).collect::<Result<Vec<_>>>()?.join(", "))
                String::from("order by ")
                    + o.iter()
                        .map(|t| fmt_order_term(qi, t))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ")
                        .as_str()
            } else {
                String::new()
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_order;

#[allow(unused_macros)]
macro_rules! fmt_order_term {
    () => {
        fn fmt_order_term(_qi: &Qi, t: &OrderTerm) -> Result<String> {
            let direction = match &t.direction {
                None => "",
                Some(d) => match d {
                    OrderDirection::Asc => "asc",
                    OrderDirection::Desc => "desc",
                },
            };
            let nulls = match &t.null_order {
                None => "",
                Some(n) => match n {
                    OrderNulls::NullsFirst => "nulls first",
                    OrderNulls::NullsLast => "nulls last",
                },
            };
            //Ok(format!("{} {} {}", fmt_field(&Qi("".to_string(),"".to_string()), &t.term)?, direction, nulls))
            Ok(vec![fmt_field(&Qi("".to_string(), "".to_string()), &t.term)?.as_str(), direction, nulls].join(" "))
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_order_term;

#[allow(unused_macros)]
macro_rules! fmt_groupby {
    () => {
        fn fmt_groupby(qi: &Qi, o: &Vec<GroupByTerm>) -> Result<String> {
            Ok(if o.len() > 0 {
                format!("group by {}", o.iter().map(|t| fmt_groupby_term(qi, t)).collect::<Result<Vec<_>>>()?.join(", "))
            } else {
                //String::new()
                String::new()
            })
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_groupby;

#[allow(unused_macros)]
macro_rules! fmt_groupby_term {
    () => {
        fn fmt_groupby_term(_qi: &Qi, t: &GroupByTerm) -> Result<String> {
            fmt_field(&Qi("".to_string(), "".to_string()), &t.0)
            //Ok(fmt_identity(&t.0.name))
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_groupby_term;

#[allow(unused_macros)]
macro_rules! fmt_select_name {
    () => {
        fn fmt_select_name(name: &String, json_path: &Option<Vec<JsonOperation>>, alias: &Option<String>) -> Option<String> {
            match (name, json_path, alias) {
                (n, Some(jp), None) => match jp.last() {
                    Some(JArrow(JKey(k))) | Some(J2Arrow(JKey(k))) => Some(k.clone()),
                    Some(JArrow(JIdx(_))) | Some(J2Arrow(JIdx(_))) => Some(
                        jp.iter()
                            .rev()
                            .find_map(|i| match i {
                                J2Arrow(JKey(k)) | JArrow(JKey(k)) => Some(k),
                                _ => None,
                            })
                            .unwrap_or(n)
                            .clone(),
                    ),
                    None => None,
                },
                (_, _, Some(aa)) => Some(aa.clone()),
                (n, None, None) => Some(n.clone()),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_select_name;

#[allow(unused_macros)]
macro_rules! fmt_as {
    () => {
        fn fmt_as(name: &String, json_path: &Option<Vec<JsonOperation>>, alias: &Option<String>) -> String {
            match (name, json_path, alias) {
                (_, Some(_), None) => match fmt_select_name(name, json_path, alias) {
                    Some(nn) => String::from(" as ") + fmt_identity(&nn).as_str(), //format!(" as {}", fmt_identity(&nn)),
                    None => String::new(),
                },
                (_, _, Some(aa)) => String::from(" as ") + fmt_identity(aa).as_str(), //format!(" as {}", fmt_identity(aa)),
                _ => String::new(),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_as;

#[allow(unused_macros)]
macro_rules! fmt_limit {
    () => {
        fn fmt_limit<'a>(l: &'a Option<SingleVal>) -> Snippet<'a> {
            match l {
                Some(ll) => {
                    let vv: &SqlParam = ll;
                    "limit " + param(vv)
                }
                None => sql(""),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_limit;

#[allow(unused_macros)]
macro_rules! fmt_offset {
    () => {
        fn fmt_offset<'a>(o: &'a Option<SingleVal>) -> Snippet<'a> {
            match o {
                Some(oo) => {
                    let vv: &SqlParam = oo;
                    "offset " + param(vv)
                }
                None => sql(""),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_offset;

#[allow(unused_macros)]
macro_rules! fmt_json_path {
    () => {
        fn fmt_json_path(p: &Option<Vec<JsonOperation>>) -> String {
            match p {
                Some(j) => j.iter().map(fmt_json_operation).collect::<Vec<_>>().join(""),
                None => String::new(),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_json_path;

#[allow(unused_macros)]
macro_rules! fmt_json_operation {
    () => {
        fn fmt_json_operation(j: &JsonOperation) -> String {
            match j {
                JArrow(o) => String::from("->") + fmt_json_operand(o).as_str(),
                J2Arrow(o) => String::from("->>") + fmt_json_operand(o).as_str(),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_json_operation;

#[allow(unused_macros)]
macro_rules! fmt_json_operand {
    () => {
        fn fmt_json_operand(o: &JsonOperand) -> String {
            match o {
                JKey(k) => String::from("'") + k.as_str() + "'", //format!("'{}'", k),
                JIdx(i) => i.clone(),
            }
        }
    };
}
#[allow(unused_imports)]
pub(super) use fmt_json_operand;

#[allow(unused)]
pub fn return_representation<'a>(method: &str, query: &'a Query, preferences: &Option<Preferences>) -> bool {
    !matches!(
        (method, &query.node, preferences),
        ("POST", Insert { .. }, None)
            | (
                "POST",
                Insert { .. },
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                })
            )
            | (
                "POST",
                Insert { .. },
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                })
            )
            | ("PATCH", Update { .. }, None)
            | (
                "PATCH",
                Update { .. },
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                })
            )
            | (
                "PATCH",
                Update { .. },
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                })
            )
            | ("PUT", Insert { .. }, None)
            | (
                "PUT",
                Insert { .. },
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                })
            )
            | (
                "PUT",
                Insert { .. },
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                })
            )
            | ("DELETE", Delete { .. }, None)
            | (
                "DELETE",
                Delete { .. },
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                })
            )
    )
}

// #[allow(unused_imports)]
// pub(super) use cast_select_item_format;
// #[allow(unused_imports)]
// pub(super) use fmt_as;
// #[allow(unused_imports)]
// pub(super) use fmt_body;
// #[allow(unused_imports)]
// pub(super) use fmt_condition;
// #[allow(unused_imports)]
// pub(super) use fmt_condition_tree;
// #[allow(unused_imports)]
// pub(super) use fmt_count_query;
// #[allow(unused_imports)]
// pub(super) use fmt_field;
// #[allow(unused_imports)]
// pub(super) use fmt_field_format;
// #[allow(unused_imports)]
// pub(super) use fmt_filter;
// #[allow(unused_imports)]
// pub(super) use fmt_env_var;
// #[allow(unused_imports)]
// pub(super) use fmt_identity;
// #[allow(unused_imports)]
// pub(super) use fmt_in_filter;
// #[allow(unused_imports)]
// pub(super) use fmt_json_operand;
// #[allow(unused_imports)]
// pub(super) use fmt_json_operation;
// #[allow(unused_imports)]
// pub(super) use fmt_json_path;
// #[allow(unused_imports)]
// pub(super) use fmt_limit;
// #[allow(unused_imports)]
// pub(super) use fmt_logic_operator;
// #[allow(unused_imports)]
// pub(super) use fmt_main_query;
// #[allow(unused_imports)]
// pub(super) use fmt_offset;
// #[allow(unused_imports)]
// pub(super) use fmt_operator;
// #[allow(unused_imports)]
// pub(super) use fmt_order;
// #[allow(unused_imports)]
// pub(super) use fmt_order_term;
// #[allow(unused_imports)]
// pub(super) use fmt_groupby;
// #[allow(unused_imports)]
// pub(super) use fmt_groupby_term;
// #[allow(unused_imports)]
// pub(super) use fmt_qi;
// #[allow(unused_imports)]
// pub(super) use fmt_query;
// #[allow(unused_imports)]
// pub(super) use fmt_function_call;
// #[allow(unused_imports)]
// pub(super) use fmt_select_item_function;
// #[allow(unused_imports)]
// pub(super) use fmt_select_item;
// #[allow(unused_imports)]
// pub(super) use fmt_select_name;
// #[allow(unused_imports)]
// pub(super) use fmt_sub_select_item;
// #[allow(unused_imports)]
// pub(super) use simple_select_item_format;
// #[allow(unused_imports)]
// pub(super) use star_select_item_format;
// #[allow(unused_imports)]
// pub(super) use fmt_function_param;
