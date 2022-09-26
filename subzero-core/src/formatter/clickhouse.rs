//use core::fmt;

use std::collections::HashSet;
use std::collections::HashMap;

use super::base::{
    cast_select_item_format, fmt_condition, fmt_field, fmt_filter,
    fmt_identity, fmt_json_path, fmt_limit, fmt_logic_operator, fmt_offset,
    fmt_operator, fmt_order, fmt_order_term, fmt_groupby, fmt_groupby_term, fmt_qi, fmt_select_item, fmt_select_name, return_representation,
    simple_select_item_format, star_select_item_format, fmt_select_item_function,fmt_function_call,
};
use crate::api::{Condition::*, Filter::*, Join::*, JsonOperand::*, JsonOperation::*, LogicOperator::*, QueryNode::*, SelectItem::*, *, ContentType::SingularJSON};
use crate::dynamic_statement::{
    param, sql, JoinIterator, SqlSnippet,  SqlSnippetChunk,
    generate_fn,
};
use crate::error::{Result, *};
use super::{ToParam, Snippet, SqlParam};

macro_rules! fmt_field_format {
    () => {
        "JSON_VALUE({}{}{}, '${}')"
    };
}

macro_rules! param_placeholder_format {() => {"{{p{pos}:{data_type}}}"};}
generate_fn!(true, "String");

//fmt_main_query!();
pub fn fmt_main_query<'a>(schema_str: &'a str, request: &'a ApiRequest, env: &'a HashMap<&'a str, &'a str>) -> Result<Snippet<'a>> {
    let schema = String::from(schema_str);
    let _count = match &request.preferences {
        Some(Preferences {
            count: Some(Count::ExactCount),
            ..
        }) => Err(Error::InternalError { message: "not implemented yet for clickhouse".to_string() })?, //true,
        _ => false,
    };

    let return_representation = return_representation(request);
    let has_payload_cte = matches!(request.query.node, Insert {..} | Update {..});
    Ok(
        "with env as (" + fmt_env_query(&env)+ ") " +
        if has_payload_cte {", "} else {""} +
        fmt_query(
            &schema,
            return_representation,
            None, //Some("_subzero_query"),
            &request.query,
            &None,
        )? + 
        "
        format JSONEachRow
        settings 
        "
        +
        match &request.accept_content_type {
            SingularJSON => "",
            _ => "output_format_json_array_of_rows=1,",
        }
        +
        "
        join_use_nulls=1,
        output_format_json_named_tuples_as_objects=1
        "
    )
}
pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select " +
    if env.is_empty() {sql("null")} 
    else {
        env
        .iter()
        .map(|(k, v)| param(v as &SqlParam) + " as " + fmt_identity(&String::from(*k)))
        .join(",")
    }
}
//fmt_query!();
pub fn fmt_query<'a>(
    schema: &String,
    _return_representation: bool,
    wrapin_cte: Option<&'static str>,
    q: &'a Query,
    join: &Option<Join>,
) -> Result<Snippet<'a>> {

    let add_env_tbl_to_from = wrapin_cte.is_some();

    let (cte_snippet, query_snippet) = match &q.node {
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

            // get the columns needed for joins
            let empty = vec![];
            let join_cols = match join {
                Some(Child(ForeignKey { columns, ..})) => columns,
                Some(Parent(ForeignKey { referenced_columns, ..})) => referenced_columns,
                _ => &empty
            };
            // add simple items to the select snippets
            let mut select_snippets = select.iter()
                // do not add the columns needed for joins to the select snippets, we'll do that later
                .filter(|s| match s {
                    Simple {alias: None, cast: None, field: Field { name, json_path: None, .. }} => {
                        !join_cols.contains(&name)
                    }
                    _ => true,
                })
                .map(|s| fmt_select_item(&qi, s)).collect::<Result<Vec<_>>>()?;

            // add subselects
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q
                .sub_selects
                .iter()
                .map(|s| fmt_sub_select_item(schema, &qi, s))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .unzip();
            select_snippets.extend(sub_selects.into_iter());

            // add columns needed for joins
            let join_cols_snippets = join_cols
            .into_iter()
            .map(|name| 
                Ok(sql(format!(
                    simple_select_item_format!(),
                    field=fmt_field(&qi, &Field { name: name.clone(), json_path: None })?,
                    as=fmt_as(name, &None, &None),
                    select_name=fmt_select_name(name, &None, &None).unwrap_or("".to_string())
                )))
            )
            .collect::<Result<Vec<_>>>()?;
            select_snippets.extend(join_cols_snippets.into_iter());
            
            let groupby_for_join = q.sub_selects.iter()
                // get the table primary keys from subselect joins
                .map(|s| match s {
                    SubSelect {join: Some(Child(ForeignKey{referenced_columns, ..})), ..} => Some(referenced_columns),
                    SubSelect {join: Some(Parent(ForeignKey{referenced_columns, ..})), ..} => Some(referenced_columns),
                    _ => None,
                })
                .flatten()
                // use only the first join (is this ok?)
                .take(1)
                .next()
                .map_or(vec![], |pks| {
                    let mut uniques = HashSet::new();
                    let mut terms = pks.iter().map(|c| GroupByTerm(Field {name: c.clone(), json_path: None}))
                    // append the other selected fields to the groupby
                    .chain(
                        select.iter().map(|s| 
                            match s {
                                Simple {field, ..} => Some(GroupByTerm(field.clone())),
                                _ => None
                            }
                        ).flatten()
                    )
                    .collect::<Vec<_>>();
                    //filter only qunique
                    terms.retain(|GroupByTerm(Field { name, .. })| uniques.insert(name.clone()));
                    terms
            });
            
            (
                None,
                " select "
                    + select_snippets.join(", ")
                    + " from "
                    + from_snippet
                    + if add_env_tbl_to_from { ", env " } else { "" }
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
                    + match fmt_condition_tree(&qi, where_)? {
                        SqlSnippet(s) => match s[..] {
                            [] => sql(""),
                            _ => "where " + SqlSnippet(s),
                        }
                    }
                    + " "
                    + (fmt_groupby(&qi, if groupby.len() > 0 { groupby } else { &groupby_for_join }  )?)
                    + " "
                    + (fmt_order(&qi, order)?)
                    + " "
                    + fmt_limit(limit)
                    + " "
                    + fmt_offset(offset),
            )
        }
        _ => {
            Err(Error::InternalError { message: "not implemented yet for clickhouse".to_string() })?
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
//fmt_count_query!();
//fmt_body!();
//fmt_condition_tree!();
fn fmt_condition_tree<'a>(qi: &Qi, t: &'a ConditionTree) -> Result<Snippet<'a>> {
    match t {
        ConditionTree { operator, conditions } => {
            let sep = format!(" {} ", fmt_logic_operator(operator));
            Ok(conditions
                .iter()
                .filter(|c| match c {
                    Single {filter: Col (_, _), ..} => false,
                    Foreign { .. } => false,
                    _ => true,
                })
                .map(|c| fmt_condition(qi, c))
                .collect::<Result<Vec<_>>>()?
                .join(sep.as_str()))
        }
    }
}
fmt_condition!();
macro_rules! fmt_in_filter {
    ($p:ident) => {
        fmt_operator(&"in ".to_string())? + param($p)
    };
}
fn fmt_env_var<'a>(e: &'a EnvVar) -> String {
    match e {
        EnvVar{var, part:None} => format!("(select {} from env)",fmt_identity(var)),
        EnvVar{var, part:Some(part)} => format!("(select JSON_VALUE({},'$.{}') from env)",fmt_identity(var), part),
    }
}
fmt_filter!();
fmt_select_name!();
fmt_select_item!();
fmt_function_call!();
fmt_select_item_function!();
//fmt_function_param!();
fn fmt_function_param<'a>(qi: &Qi, p: &'a FunctionParam) -> Result<Snippet<'a>> {
    Ok(match p {
        FunctionParam::Val(v,_c) => {
            let vv: &SqlParam = v;
            param(vv)
            // match c {
            //     Some(c) => "cast(" + param(vv) + format!(" as {}", c) + ")",
            //     None => param(vv),
            // }
        },
        FunctionParam::Fld(f) => sql(fmt_field(qi, f)?),
        FunctionParam::Func {fn_name,parameters,} => fmt_function_call(qi, fn_name, parameters)?,
    })
}
//fmt_sub_select_item!();
fn fmt_sub_select_item<'a>(schema: &String, qi: &Qi, i: &'a SubSelect) -> Result<(Snippet<'a>, Vec<Snippet<'a>>)> {
    match i {
        SubSelect { query, alias, join, .. } => match join {
            Some(j) => {
                let subselect_columns = query.node.select().iter()
                            .map(|i| 
                                match i {
                                    Star => format!(star_select_item_format!(), fmt_qi(qi)),
                                    Simple {
                                        field: Field { name, json_path },
                                        alias,
                                        ..
                                    } => fmt_select_name(name, json_path, alias).unwrap_or("".to_string()),
                                    Func {
                                        alias,
                                        fn_name,
                                        ..
                                    } => fmt_select_name(fn_name, &None, alias).unwrap_or("".to_string()),
                                }
                            ).collect::<Vec<_>>();
                // extract back join conditions that were inserted at parse time
                let (join_conditions, join_separator) = match query.node.where_() {
                    ConditionTree { operator, conditions } => {
                        (conditions
                            .iter()
                            .filter(|c| match c {
                                Single {filter: Col (_, _), ..} => true,
                                Foreign { .. } => true,
                                _ => false,
                            })
                            .collect::<Vec<_>>(),
                        format!(" {} ", fmt_logic_operator(operator)))
                    }
                };

                match j {
                    Parent(fk) => {
                        let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                        let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                        let subquery = fmt_query(schema, true, None, query, join)?;
                        Ok((
                            //sql(format!("row_to_json({}.*) as {}", fmt_identity(&local_table_name), fmt_identity(alias_or_name))),
                            sql("any(") +
                                "cast(" +
                                    "tuple("+
                                        subselect_columns.iter().map(|i| format!("\"{}\".\"{}\"", local_table_name, i)).collect::<Vec<_>>().join(", ") +
                                    "), " +
                                    "concat(" +
                                        "'Tuple(', " +
                                        subselect_columns.iter().map(|i| format!("'{} ', toTypeName(\"{}\".\"{}\")", i, local_table_name, i)).collect::<Vec<_>>().join(", ',', ") +
                                        ", ')'" +
                                    ")" +
                                ")" +
                            ") as " + fmt_identity(alias_or_name),
                            //vec!["left join lateral (" + subquery + ") as " + sql(fmt_identity(&local_table_name)) + " on true"],
                            vec![
                                "left join (" + subquery + ") as " + sql(fmt_identity(&local_table_name)) + 
                                " on " + join_conditions.iter().map(|c| fmt_condition(&Qi("".to_string(), local_table_name.clone()), c)).collect::<Result<Vec<_>>>()?.join(join_separator.as_str())
                            ],
                        ))
                    }
                    Child(fk) => {
                        let alias_or_name = alias.as_ref().unwrap_or(&fk.table.1);
                        let local_table_name = &fk.table.1;
                        let subquery = fmt_query(schema, true, None, query, join)?;
                        Ok((
                            sql("groupArray(") +
                                "cast(" +
                                    "tuple("+
                                        subselect_columns.iter().map(|i| format!("\"{}\".\"{}\"", local_table_name, i)).collect::<Vec<_>>().join(", ") +
                                    "), " +
                                    "concat(" +
                                        "'Tuple(', " +
                                        subselect_columns.iter().map(|i| format!("'{} ', toTypeName(\"{}\".\"{}\")", i, local_table_name, i)).collect::<Vec<_>>().join(", ',', ") +
                                        ", ')'" +
                                    ")" +
                                ")" +
                            ") as " + fmt_identity(&alias_or_name),
                            vec![
                                "left join (" + subquery + ") as " + sql(fmt_identity(local_table_name)) + 
                                " on " + join_conditions.iter().map(|c| fmt_condition(&Qi("".to_string(), local_table_name.clone()), c)).collect::<Result<Vec<_>>>()?.join(join_separator.as_str())
                            ],
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
                
                }
            },
            None => panic!("unable to format join query without matching relation"),
        },
    }
}
fmt_operator!();
fmt_logic_operator!();
fmt_identity!();
fmt_qi!();
fmt_field!();
fmt_order!();
fmt_order_term!();
fmt_groupby!();
fmt_groupby_term!();
//fmt_as!();
fn fmt_as(name: &String, json_path: &Option<Vec<JsonOperation>>, alias: &Option<String>) -> String {
    match (name, json_path, alias) {
        (_, Some(_), None) =>
            match fmt_select_name(name, json_path, alias) {
                Some(nn) => format!(" as {}", fmt_identity(&nn)),
                None => format!(" as {}", fmt_identity(&name)),
            },
        (_, _, Some(aa)) => format!(" as {}", fmt_identity(aa)),
        _ => format!(" as {}", fmt_identity(&name)),
    }
}
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


#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::dynamic_statement::{generate_fn, SqlSnippet, SqlSnippetChunk, param_placeholder_format};
    use pretty_assertions::assert_eq;
    use regex::Regex;
    use crate::api::{ContentType::*};
    use super::*;
    macro_rules! param_placeholder_format {() => {"{{p{pos}:{data_type}}}"};}
    generate_fn!(true);
    fn s(s: &str) -> String { s.to_string() }
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
                            name: s("id"),
                            json_path: None,
                        },
                        alias: None,
                        cast: None,
                    },
                    Simple {
                        field: Field {
                            name: s("name"),
                            json_path: None,
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
                            filter: Op(s(">="), SingleVal(s("5"),Some(s("Int32")))),
                            field: Field {
                                name: s("id"),
                                json_path: None,
                            },
                            negate: false,
                        }
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
                            },
                            Simple {
                                field: Field {
                                    name: s("name"),
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
                                        Qi(s("default"), s("projects")),
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
                    alias: Some(s("client")),
                    hint: None,
                    join: Some(Parent(ForeignKey {
                        name: s("client_id_fk"),
                        table: Qi(s("default"), s("projects")),
                        columns: vec![s("client_id")],
                        referenced_table: Qi(s("default"), s("clients")),
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
                            },Simple {
                                field: Field {
                                    name: s("name"),
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
                                            Qi(s("default"), s("projects")),
                                            Field {
                                                name: s("id"),
                                                json_path: None,
                                            },
                                        ),
                                        negate: false,
                                    },
                                    Single {
                                        filter: Op(s(">"), SingleVal(s("50"),Some(s("Int32")))),
                                        field: Field {
                                            name: s("id"),
                                            json_path: None,
                                        },
                                        negate: false,
                                    },
                                    Single {
                                        filter: In(ListVal(vec![s("51"), s("52")],Some(s("Array(Int32)")))),
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
                        table: Qi(s("default"), s("tasks")),
                        columns: vec![s("project_id")],
                        referenced_table: Qi(s("default"), s("projects")),
                        referenced_columns: vec![s("id")],
                    })),
                },
            ],
        };
        let emtpy_hashmap = HashMap::new();
        

        let (query_str, _parameters, _) = generate(fmt_query(&s("default"), true, None, &q, &None).unwrap());
        // assert_eq!(
        //     format!("{:?}", parameters),
        //     "[SingleVal(\"50\"), ListVal([\"51\", \"52\"]), SingleVal(\"5\"), SingleVal(\"10\")]"
        // );
        let expected_query_str = r#"
            select 
                "default"."projects"."id" as "id",
                "default"."projects"."name" as "name", 
                any(cast(tuple("projects_client"."id", "projects_client"."name"), concat('Tuple(', 'id ', toTypeName("projects_client"."id"), ',', 'name ', toTypeName("projects_client"."name"), ')'))) as "client",
                groupArray(cast(tuple("tasks"."id", "tasks"."name"), concat('Tuple(', 'id ', toTypeName("tasks"."id"), ',', 'name ', toTypeName("tasks"."name"), ')'))) as "tasks"
            from "default"."projects"
            left join (
                select 
                    "default"."clients"."name" as "name",
                    "default"."clients"."id" as "id"
                    from "default"."clients"
            ) as "projects_client" on "projects_client"."id" = "default"."projects"."client_id"
            left join (
                select 
                    "default"."tasks"."id" as "id",
                    "default"."tasks"."name" as "name",
                    "default"."tasks"."project_id" as "project_id"
                from "default"."tasks"
                where 
                    "default"."tasks"."id" > {p1:Int32} 
                    and 
                    "default"."tasks"."id" in {p2:Array(Int32)}
            ) as "tasks" on "tasks"."project_id" = "default"."projects"."id"
            where "default"."projects"."id" >= {p3:Int32}
            group by "id", "name"
        "#;


        let re = Regex::new(r"\s+").unwrap();
        assert_eq!(
            re.replace_all(query_str.as_str(), " "),
            re.replace_all(expected_query_str," ")
        );

        //dummy api struct with valid query
        let api_request = ApiRequest {
            schema_name: "default",
            get: vec![],
            preferences: None,
            path: "dummy",
            method: "GET",
            read_only: true,
            accept_content_type: ApplicationJSON,
            headers: emtpy_hashmap.clone(),
            cookies: emtpy_hashmap.clone(),
            query: q
        }; 

        let expected_main_query_str = format!(r#"with env as (select null)
        {}
        format JSONEachRow
        settings 
        
        output_format_json_array_of_rows=1,
        join_use_nulls=1,
        output_format_json_named_tuples_as_objects=1
        
        "#, expected_query_str);

        let (main_query_str, _parameters, _) = generate(fmt_main_query(&s("default"), &api_request, &HashMap::new()).unwrap());
        assert_eq!(
            re.replace_all(main_query_str.as_str(), " "),
            re.replace_all(expected_main_query_str.as_str()," ")
        );


    }

    
}
