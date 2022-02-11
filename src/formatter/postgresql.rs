use crate::api::{
    *,
    JsonOperation::*, SelectItem::*, JsonOperand::*, LogicOperator:: *,
    Condition::*, Filter::*, Join::*, QueryNode::*, ContentType::*,
};
use crate::dynamic_statement::{
    sql, param, SqlSnippet, JoinIterator,
};
use postgres_types::{ToSql, Type, to_sql_checked, IsNull, Format};
use std::error::Error;
use bytes::{BufMut, BytesMut};

impl ToSql for ListVal {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            ListVal(v) => {
                if v.len() > 0 {
                    out.put_slice(
                        format!(
                            "{{\"{}\"}}",
                            v.iter()
                            .map(|e| e.replace("\\","\\\\").replace("\"", "\\\"") )
                            .collect::<Vec<_>>()
                            .join("\",\"")
                        )
                        .as_str().as_bytes()
                    );
                }
                else {
                    out.put_slice(format!("{{}}").as_str().as_bytes());
                }
                
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool { true }

    fn encode_format(&self) -> Format { Format::Text }

    to_sql_checked!();
}

impl ToSql for SingleVal {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            SingleVal(v) => {
                out.put_slice(v.as_str().as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool { true }

    fn encode_format(&self) -> Format { Format::Text }

    to_sql_checked!();
}

impl ToSql for Payload {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            Payload(v) => {
                out.put_slice(v.as_str().as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool { true }

    fn encode_format(&self) -> Format { Format::Text }

    to_sql_checked!();
}

// helper type aliases
type SqlParam<'a> = (dyn ToSql + Sync + 'a);
type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;

fn return_representation<'a>(request: &'a ApiRequest) -> bool {
    match (&request.method, &request.query.node, &request.preferences) {
        (&Method::POST, Insert {..}, None) |
        (&Method::POST, Insert {..}, Some(Preferences { representation: Some(Representation::None), ..})) |
        (&Method::POST, Insert {..}, Some(Preferences { representation: Some(Representation::HeadersOnly), ..})) |
        (&Method::PATCH, Update {..}, None) |
        (&Method::PATCH, Update {..}, Some(Preferences { representation: Some(Representation::None), ..})) |
        (&Method::PATCH, Update {..}, Some(Preferences { representation: Some(Representation::HeadersOnly), ..})) |
        (&Method::DELETE, Delete {..}, None) |
        (&Method::DELETE, Delete {..}, Some(Preferences { representation: Some(Representation::None), ..}))
            => false,
        _ => true,
    }
}

// fn is_self_join( join: &Option<Join> ) -> bool {
//     match join {
//         None => false,
//         Some(Parent(_)) => false,
//         Some(Many(_,_,_)) => false,
//         Some(Child(fk)) => fk.table == fk.referenced_table,
//     }
// }

pub fn main_query<'a>(schema: &String, request: &'a ApiRequest) -> Snippet<'a>{
    let count = match &request.preferences {
        Some(Preferences { count: Some(Count::ExactCount), ..}) => true,
        _ => false,
    };

    let return_representation = return_representation(request);
    let body_snippet = match (return_representation, &request.accept_content_type, &request.query.node) {
        (false, _, _) => "''",
        (true, SingularJSON, FunctionCall {is_scalar:true, ..} ) |
        (true, ApplicationJSON, FunctionCall {returns_single:true, is_multiple_call: false, is_scalar: true, ..} )
            => "coalesce((json_agg(_subzero_t.subzero_scalar)->0)::text, 'null')",
        (true, ApplicationJSON, FunctionCall {returns_single:false, is_multiple_call: false, is_scalar: true, ..} )
            => "coalesce((json_agg(_subzero_t.subzero_scalar))::text, '[]')",
        (true, SingularJSON, FunctionCall {is_scalar:false, ..} ) |
        (true, ApplicationJSON, FunctionCall {returns_single:true, is_multiple_call: false, is_scalar: false, ..} )
            => "coalesce((json_agg(_subzero_t)->0)::text, 'null')",
        
        (true, ApplicationJSON,_) => "coalesce(json_agg(_subzero_t), '[]')::character varying",
        (true, SingularJSON, _) => "coalesce((json_agg(_subzero_t)->0)::text, 'null')",
        (true, TextCSV, _) => r#"
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
        "#,
    };

    fmt_query(schema, return_representation, Some("_subzero_query"), &request.query, &None) +
    " , " + if count { fmt_count_query(schema, Some("_subzero_count_query"), &request.query)} else {sql("_subzero_count_query AS (select 1)")} +
    " select" +
    " pg_catalog.count(_subzero_t) AS page_total, "+
    if count {"(SELECT pg_catalog.count(*) FROM _subzero_count_query)"} else { "null::bigint" } + " as total_result_set, " +
    body_snippet + " as body, " +
    " nullif(current_setting('response.headers', true), '') as response_headers, " +
    " nullif(current_setting('response.status', true), '') AS response_status " +
    " from ( select * from _subzero_query ) _subzero_t"
}

fn fmt_query<'a>(schema: &String, return_representation: bool, wrapin_cte: Option<&'static str>, q: &'a Query, _join: &Option<Join>) -> Snippet<'a>{
    let (cte_snippet, query_snippet) = match &q.node {
        FunctionCall { fn_name, parameters, payload, is_scalar, is_multiple_call, returning, select, where_, limit, offset, order, .. } => {
            let bb:&(dyn ToSql + Sync + 'a) = payload;
            let (params_cte, arg_frag):(Snippet<'a>,Snippet<'a>) = match &parameters {
                CallParams::OnePosParam(_p) => (sql(" "), param(bb)),
                CallParams::KeyParams(p) if p.len() == 0 => (sql(" "),sql("")),
                CallParams::KeyParams(prms) => (
                        fmt_body(payload) +
                        ", subzero_args as ( " +
                            "select * from json_to_recordset((select val from subzero_body)) as _(" +
                                prms.iter().map(|p| format!("{} {}", fmt_identity(&p.name), p.type_ )).collect::<Vec<_>>().join(", ") +
                            ")" +
                        " ), ",
                        sql(prms.iter().map(|p|{
                            let variadic = if p.variadic {"variadic"} else {""};
                            let ident = fmt_identity(&p.name);
                            if *is_multiple_call {
                                format!("{} {} := subzero_args.{}", variadic, ident, ident)
                            }
                            else {
                                format!("{} {}  := (select {} from subzero_args limit 1)", variadic, ident, ident )
                            }
                        }).collect::<Vec<_>>().join(", "))
                )
            };
            let call_it = fmt_qi(fn_name) + "(" + arg_frag + ")";
            let returned_columns = if returning.len() == 0 {
                    "*".to_string()
                }
                else {
                    returning.iter()
                    .map(|r| if r.as_str() == "*" {format!("*")} else{format!("{}.{}", fmt_identity(&fn_name.1),fmt_identity(r))})
                    .collect::<Vec<_>>().join(",")
                };

            let args_body = if *is_multiple_call {
                if *is_scalar {
                    "select " + call_it + "subzero_scalar from subzero_args"
                }
                else {
                    format!("select subzero_lat_args.* from subzero_args, lateral ( select {} from ", returned_columns) + 
                    call_it + " ) subzero_lat_args"
                }
            }
            else {
                if *is_scalar {
                    "select " + call_it + " as subzero_scalar"
                }
                else {
                    format!("select {} from ",returned_columns) + call_it
                }
            };

            let qi_subzero_source = &Qi("".to_string(),"subzero_source".to_string());
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(schema, qi_subzero_source, s)).unzip();
            select.extend(sub_selects.into_iter());
            (
                Some (
                    params_cte +
                    " subzero_source as ( " + args_body + " )"
                )
            ,
            " select " + select.join(", ")  +
            " from " + fmt_identity(&"subzero_source".to_string()) +
            " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ") +
            " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") } +
            " " + fmt_order(qi_subzero_source, order) +
            " " + fmt_limit(limit) +
            " " + fmt_offset(offset)
            )

        },
        Select {select, from: (table, table_alias), join_tables, where_, limit, offset, order} => {
            
            let (qi, from_snippet) = match table_alias {
                Some(a) => (
                    Qi("".to_string(),a.clone()), 
                    format!("{} as {}", fmt_qi(&Qi(schema.clone(),table.clone())), fmt_identity(&a))
                ),
                None => (
                    Qi(schema.clone(),table.clone()),
                    fmt_qi(&Qi(schema.clone(),table.clone()))
                ),
            };
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(&qi, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(schema, &qi, s)).unzip();
            select.extend(sub_selects.into_iter());

            (
                None,
                " select " + select.join(", ") +
                " from " + from_snippet +
                " " + if join_tables.len() > 0 { format!(", {}", join_tables.iter().map(|f| fmt_qi(&Qi(schema.clone(), f.clone()))).collect::<Vec<_>>().join(", ")) } else { format!("") } +
                " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ") +
                " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&qi, where_) } else { sql("") } +
                " " + fmt_order(&qi, order) +
                " " + fmt_limit(limit) +
                " " + fmt_offset(offset)
            )
        },
        Insert {into, columns, payload, where_, returning, select} => {
            let qi = &Qi(schema.clone(),into.clone());
            let qi_subzero_source = &Qi("".to_string(),"subzero_source".to_string());
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(schema, qi_subzero_source, s)).unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            }
            else {
                returning.iter()
                .map(|r| if r.as_str() == "*" {format!("*")} else{fmt_identity(r)})
                .collect::<Vec<_>>().join(",")
            };

            let into_columns = if columns.len() > 0 { format!("({})", columns.iter().map(fmt_identity).collect::<Vec<_>>().join(","))} else { format!("") };
            let select_columns = columns.iter().map(fmt_identity).collect::<Vec<_>>().join(",");
            (
                Some(
                    fmt_body(payload)+
                    ", subzero_source as ( " + 
                        format!( " insert into {} {} select {} from json_populate_recordset(null::{}, (select val from subzero_body)) _ returning {}",
                            fmt_qi(qi), 
                            into_columns,
                            select_columns,
                            fmt_qi(qi),
                            returned_columns
                        ) +
                    " )"
                ),
                if return_representation {
                    " select " + select.join(", ")  +
                    " from " + fmt_identity(&"subzero_source".to_string()) +
                    " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ") +
                    " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
                }
                else {
                    sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
                }
            )
        },
        Delete {from, where_, returning, select} => {
            let qi = &Qi(schema.clone(),from.clone());
            let qi_subzero_source = &Qi("".to_string(),"subzero_source".to_string());
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(schema, qi_subzero_source, s)).unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            }
            else {
                returning.iter()
                .map(|r| if r.as_str() == "*" {format!("*")} else{fmt_identity(r)})
                .collect::<Vec<_>>().join(",")
            };

            (
                Some(
                    sql(" subzero_source as ( ") +
                    " delete from " + fmt_qi(qi) +
                    " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&qi, where_) } else { sql("") } +
                    " returning " + returned_columns + 
                    " )"
                ),
                if return_representation {
                    " select " + select.join(", ")  +
                    " from " + fmt_identity(&"subzero_source".to_string()) +
                    " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ") +
                    " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
                }
                else {
                    sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
                }
            )
        },
        Update {table, columns, payload, where_, returning, select} => {
            let qi = &Qi(schema.clone(),table.clone());
            let qi_subzero_source = &Qi("".to_string(),"subzero_source".to_string());
            let mut select: Vec<_> = select.iter().map(|s| fmt_select_item(qi_subzero_source, s)).collect();
            let (sub_selects, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(schema, qi_subzero_source, s)).unzip();
            select.extend(sub_selects.into_iter());
            let returned_columns = if returning.len() == 0 {
                "1".to_string()
            }
            else {
                returning.iter()
                .map(|r| if r.as_str() == "*" {format!("{}.*", fmt_qi(qi))} else{format!("{}.{}", fmt_qi(qi), fmt_identity(r))})
                .collect::<Vec<_>>().join(",")
            };

            let set_columns = columns.iter().map(|c| format!("{} = _.{}", fmt_identity(c), fmt_identity(c)) ).collect::<Vec<_>>().join(",");
            (
                if columns.len() == 0 {
                    let sel = if returning.len() == 0 {
                        "null".to_string()
                    }
                    else {
                        returning.iter()
                        .map(|r| if r.as_str() == "*" {format!("{}.*", table)} else{format!("{}.{}", table, r)})
                        .collect::<Vec<_>>().join(",")
                    };
                    Some(sql(format!(" subzero_source as (select {} from {} where false )", sel, fmt_qi(qi))))
                }
                else {
                    Some(
                        fmt_body(payload)+
                        ", subzero_source as ( " +
                        " update " + fmt_qi(qi) +
                        " set " + set_columns +
                        " from (select * from json_populate_recordset (null::" + fmt_qi(qi) + " , (select val from subzero_body) )) _ " +
                        " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(&qi, where_) } else { sql("") } +
                        " returning " + returned_columns + 
                        " )"
                    )
                },
                if return_representation {
                    " select " + select.join(", ")  +
                    " from " + fmt_identity(&"subzero_source".to_string()) +
                    " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ")
                    //" " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi_subzero_source, where_) } else { sql("") }
                }
                else {
                    sql(format!(" select * from {}", fmt_identity(&"subzero_source".to_string())))
                }
            )
        },
    };

    match wrapin_cte {
        Some(cte_name) => {
            match cte_snippet {
                Some(cte) => " with " + cte + " , " + format!("{} as ( ", cte_name) + query_snippet + " )",
                None => format!(" with {} as ( ", cte_name) + query_snippet + " )",
            }
        },
        None => {
            match cte_snippet {
                Some(cte) => " with " + cte + query_snippet,
                None => query_snippet
            }
        }
    }
}

fn fmt_count_query<'a>(schema: &String, wrapin_cte: Option<&'static str>, q: &'a Query) -> Snippet<'a>{
    let query_snippet = match &q.node {
        FunctionCall { .. } => {
            
            sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string())))
        },
        Select {from: (table, _), join_tables, where_, ..} => {
            let qi = &Qi(schema.clone(),table.clone());
            //let (_, joins): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(&schema, qi, s)).unzip();
            //let select: Vec<_> = select.iter().map(|s| fmt_select_item(&schema, qi, s)).collect();
            let (_, joins): (Vec<_>, Vec<_>) = q.sub_selects.iter().map(|s| fmt_sub_select_item(&schema, qi, s)).unzip();
            //select.extend(sub_selects.into_iter());
            sql(" select 1 from ") + 
            vec![table.clone()].iter().chain(join_tables.iter()).map(|f| fmt_qi(&Qi(schema.clone(), f.clone()))).collect::<Vec<_>>().join(", ") +
            " " + joins.into_iter().flatten().collect::<Vec<_>>().join(" ") +
            " " + if where_.conditions.len() > 0 { "where " + fmt_condition_tree(qi, where_) } else { sql("") }
        },
        Insert {..} => {
            sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string())))
        },
        Update {..} => {
            sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string())))
        },
        Delete {..} => {
            sql(format!(" select 1 from {}", fmt_identity(&"subzero_source".to_string())))
        }
    };

    match wrapin_cte {
        Some(cte_name) => {
            format!(" {} as ( ", cte_name) + query_snippet + " )"
        },
        None => {
            query_snippet
        }
    }
}

fn fmt_body<'a>(payload: &'a Payload) -> Snippet<'a> {
    let payload_param:&(dyn ToSql + Sync) = payload;
    " subzero_payload as ( select " + param(payload_param) + "::json as json_data ),"+
    " subzero_body as ("+
        " select"+
            " case when json_typeof(json_data) = 'array'"+
            " then json_data"+
            " else json_build_array(json_data)"+
            " end as val"+
        " from subzero_payload"+
    " )"
}

fn fmt_condition_tree<'a>(qi: &Qi, t: &'a ConditionTree) -> Snippet<'a> {
    
    match t {
        ConditionTree {operator, conditions} => {
            let sep = format!(" {} ",fmt_logic_operator(operator));
            conditions.iter().map(|c| fmt_condition(qi, c)).collect::<Vec<_>>().join(sep.as_str())
        }
    }
}

fn fmt_condition<'a>(qi: &Qi, c: &'a Condition) -> Snippet<'a> {
    match c {
        Single {field, filter, negate} => {
            let fld = sql(format!("{} ", fmt_field(qi, field)));

            if *negate {
                "not(" + fld + fmt_filter(filter) + ")"
            }
            else{
                fld + fmt_filter(filter)
            }
        },
        Foreign {left: (l_qi, l_fld), right: (r_qi, r_fld)} => {
            sql( format!("{} = {}", fmt_field(l_qi, l_fld), fmt_field(r_qi, r_fld)) )
        },

        Group (negate, tree) => {
            if *negate {
                "not("+ fmt_condition_tree(qi, tree) + ")"
            }
            else{
                "("+ fmt_condition_tree(qi, tree) + ")"
            }
        }
        
    }
}

fn fmt_filter<'a>(f: &'a Filter) -> Snippet<'a>{

    match f {
        Op (o, v) => {
            let vv:&(dyn ToSql + Sync) = v;
            fmt_operator(o) + param(vv)
        }
        In (l) => {
            let ll:&(dyn ToSql + Sync) = l;
            fmt_operator(&"= any".to_string()) + ("(" + param(ll) + ")")
        },
        Is (v) => {
            let vv = match v {
                TrileanVal::TriTrue=>"true", 
                TrileanVal::TriFalse=>"false",
                TrileanVal::TriNull=>"null",
                TrileanVal::TriUnknown=>"unknown",
            };
            sql(format!("is {}", vv))
        }
        Fts (o, lng, v) => {
            let vv:&(dyn ToSql + Sync) = v;
            match lng {
                Some(l) => {
                    let ll:&(dyn ToSql + Sync) = l;
                    println!("==formating {:?} {:?} {:?}", o, ll, vv);
                    fmt_operator(o) + ("(" + param(ll) + "," + param(vv) + ")")
                }
                None => fmt_operator(o) + ("(" + param(vv) + ")")
            }
        },
        Col (qi, fld) => sql( format!("= {}", fmt_field(qi, fld)) )
    }
}

fn fmt_select_item<'a >(qi: &Qi, i: &'a SelectItem) -> Snippet<'a> {
    match i {
        Star => sql(format!("{}.*", fmt_qi(qi))),
        Simple {field: field@Field { name, json_path }, alias, cast: None} => sql(format!("{}{}", fmt_field(qi, field), fmt_as(name, json_path, alias ))),
        Simple {field: field@Field { name, json_path }, alias, cast: Some(cast)} => sql(format!("cast({} as {}){}", fmt_field(qi, field), cast, fmt_as(name, json_path, alias ))),
    }
}

fn fmt_sub_select_item<'a >(schema: &String, qi: &Qi, i: &'a SubSelect) -> (Snippet<'a>, Vec<Snippet<'a>>) {
    match i {
        SubSelect {query,alias,join,..} => match join {
            Some(j) => match j {
                Parent (fk) => {
                    let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                    let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                    let subquery = fmt_query(schema, true, None, query, join);
                    
                    (
                        sql(format!("row_to_json({}.*) as {}",fmt_identity(&local_table_name), fmt_identity(alias_or_name))),
                        vec!["left join lateral ("+subquery+") as " +sql(fmt_identity(&local_table_name))+ " on true"]
                    )
                },
                Child (fk) => {
                    let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk.table.1));
                    let local_table_name = fmt_identity(&fk.table.1);
                    let subquery = fmt_query(schema, true, None, query, join);
                    (
                        ("coalesce((select json_agg("+sql(local_table_name.clone())+".*) from ("+subquery+") as "+sql(local_table_name.clone())+"), '[]') as " + sql(alias_or_name)),
                        vec![]
                    )
                },
                Many (_table, _fk1, fk2) => {
                    let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk2.referenced_table.1));
                    let local_table_name = fmt_identity(&fk2.referenced_table.1);
                    let subquery = fmt_query(schema, true, None, query, join);
                    (
                        ("coalesce((select json_agg("+sql(local_table_name.clone())+".*) from ("+subquery+") as "+sql(local_table_name.clone())+"), '[]') as " + sql(alias_or_name)),
                        vec![]
                    )
                }
            },
            None => panic!("unable to format join query without matching relation")
        }
    }
}

fn fmt_operator(o: &Operator) -> String{
    format!("{} ", o)
}

fn fmt_logic_operator( o: &LogicOperator ) -> String {
    match o {
        And => format!("and"),
        Or => format!("or")
    }
}

fn fmt_identity(i: &String) -> String{
    format!("\"{}\"", i)
}

fn fmt_qi(qi: &Qi) -> String{
    match (qi.0.as_str(),qi.1.as_str()) {
        // (_,"subzero_source") |
        // (_,"subzero_fn_call") |
        ("", _) => format!("{}", fmt_identity(&qi.1)),
        _ => format!("{}.{}", fmt_identity(&qi.0), fmt_identity(&qi.1))
    }
}

fn fmt_field(qi: &Qi, f: &Field) -> String {
    match f {
        Field { name, json_path: json_path@Some(_) } => format!("to_jsonb({}.{}){}", fmt_qi(qi), fmt_identity(&name), fmt_json_path(&json_path)),
        Field { name, json_path: None } => format!("{}.{}", fmt_qi(qi), fmt_identity(&name)),
    }
    //format!("{}{}", fmt_identity(&f.name), fmt_json_path(&f.json_path))
}

fn fmt_order(qi: &Qi, o: &Vec<OrderTerm>) -> String {
    if o.len() > 0 {
        format!("order by {}", o.iter().map(|t| fmt_order_term(qi, t)).collect::<Vec<_>>().join(", "))
    }
    else {
        format!("")
    }
}

fn fmt_order_term(qi: &Qi, t: &OrderTerm) -> String {
    let direction = match &t.direction {
        None => "",
        Some(d) => match d { OrderDirection::Asc => "asc", OrderDirection::Desc => "desc" }
    };
    let nulls = match &t.null_order {
        None => "",
        Some(n) => match n { OrderNulls::NullsFirst => "nulls first", OrderNulls::NullsLast => "nulls last" }
    };
    format!("{} {} {}", fmt_field(qi, &t.term), direction, nulls)
}

// fn fmt_cast(cast: &Option<String>) -> String {
//     match cast {
//         Some(c) => format!("::{}", fmt_identity(c)),
//         None => format!("")
//     }
// }

fn fmt_as( name: &String, json_path: &Option<Vec<JsonOperation>>, alias: &Option<String>) -> String {
    match (name, json_path, alias) {
        (n, Some(jp), None) =>
            match jp.last() {
                Some(JArrow(JKey(k))) | Some(J2Arrow(JKey(k)))  => format!(" as {}", fmt_identity(&k)),
                Some(JArrow(JIdx(_))) | Some(J2Arrow(JIdx(_)))   => format!(" as {}", fmt_identity(
                    jp.iter().rev().find_map(|i| 
                        match i {
                            J2Arrow(JKey(k)) | JArrow(JKey(k)) => Some(k),
                            _ => None
                        }
                    ).unwrap_or(n)
                )),
                _  => format!("")
            }
        ,
        (_,_,Some(aa)) => format!(" as {}", fmt_identity(aa)),
        _ => format!("")
    }
}
// fn fmt_alias(a: &Option<String>) -> String {
//     match a {
//         Some(aa) => format!(" as \"{}\"", aa),
//         None => format!("")
//     }
// }

fn fmt_limit<'a>(l: &'a Option<SingleVal>) -> Snippet<'a> {
    match l {
        Some(ll) => {
            let vv:&(dyn ToSql + Sync) = ll;
            "limit " + param(vv)
        },
        None => sql("")
    }
}

fn fmt_offset<'a>(o: &'a Option<SingleVal>) -> Snippet<'a> {
    match o {
        Some(oo) => {
            let vv:&(dyn ToSql + Sync) = oo;
            "offset " + param(vv)
        },
        None => sql("")
    }
}

fn fmt_json_path(p: &Option<Vec<JsonOperation>>) -> String {
    match p {
        Some(j) => format!("{}", j.iter().map(fmt_json_operation).collect::<Vec<_>>().join("")),
        None => format!("")
    }
}

fn fmt_json_operation(j: &JsonOperation) -> String {
    match j {
        JArrow (o) => format!("->{}", fmt_json_operand(o)),
        J2Arrow (o) => format!("->>{}", fmt_json_operand(o)),
    }
}

fn fmt_json_operand(o: &JsonOperand) -> String{
    match o {
        JKey (k) => format!("'{}'", k),
        JIdx (i) => format!("{}", i),
    }
}


#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq};
    use crate::dynamic_statement::generate;
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
    fn s(s:&str) -> String {
        s.to_string()
    }
    
    #[test]
    fn test_fmt_function_query(){
        let payload = r#"{"id":"10"}"#.to_string();
        let q = Query{ node: FunctionCall {
            fn_name: Qi(s("api"), s("myfunction")),
            parameters: CallParams::KeyParams(vec![
                ProcParam {
                    name: s("id"),
                    type_: s("integer"),
                    required: true,
                    variadic: false,
                }
            ]),
            payload: Payload(payload.clone()),
            is_scalar: true,
            returns_single: false,
            is_multiple_call: false,
            returning: vec![s("*")],
            select: vec![Star],
            where_: ConditionTree { operator: And, conditions: vec![] },
            return_table_type: None,
            limit: None, offset: None, order: vec![],
            },
            sub_selects: vec![]
        };

        let (query_str, parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None));
        let p = Payload(payload);
        let pp: Vec<&(dyn ToSql + Sync)> = vec![&p];
        assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
        let re = Regex::new(r"\s+").unwrap();
        assert_eq!(
            re.replace_all(query_str.as_str(), " "), 
            re.replace_all(
                r#"
                with
                    subzero_payload as ( select $1::json as json_data ),
                    subzero_body as (
                        select 
                            case when json_typeof(json_data) = 'array' 
                            then json_data 
                            else json_build_array(json_data) 
                            end as val 
                        from subzero_payload
                    ),
                    subzero_args as (
                        select * 
                        from json_to_recordset((select val from subzero_body)) as _("id" integer)
                    ),
                    subzero_source as (
                            select "api"."myfunction"( "id" := (select "id" from subzero_args limit 1)) as subzero_scalar
                    )
                select "subzero_source".* from "subzero_source"
                "#
                , " "
            )
        );
    }

    #[test]
    fn test_fmt_insert_query(){
        let payload = r#"[{"id":10, "a":"a field"}]"#.to_string();
        let q = Query{ node: Insert {
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None, cast: None},
                Simple {field: Field {name: s("b"), json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))])}, alias: None, cast: None},
                ],
            into: s("projects"),
            where_: ConditionTree { operator: And, conditions: vec![
                // Single {filter: Op(s(">="),s("5")), field: Field {name: s("id"), json_path: None}, negate: false},
                // Single {filter: Op(s("<"),s("10")), field: Field {name: s("id"), json_path: None}, negate: true}
            ]},
            columns: vec![s("id"), s("a")],
            payload: Payload(payload.clone()),
            returning: vec![s("id"), s("a")],
            },
            sub_selects:vec![
                SubSelect{
                    query: Query{ node: Select {order: vec![], limit: None, offset: None, 
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
                        ],
                        from: (s("clients"), None),
                        join_tables: vec![],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"),json_path: None},
                                filter: Filter::Col(Qi(s(""),s("subzero_source")),Field {name: s("client_id"),json_path: None}),
                                negate: false,
                           }
                        ]}
                    }, sub_selects: vec![]},
                    alias: None,
                    hint: None,
                    join: Some(
                        Parent(ForeignKey {
                                name: s("client_id_fk"),
                                table: Qi(s("api"),s("projects")),
                                columns: vec![s("client_id")],
                                referenced_table: Qi(s("api"),s("clients")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                },
                SubSelect{
                    query:Query{ node:  Select {order: vec![], limit: None, offset: None, 
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
                        ],
                        from: (s("tasks"), None),
                        join_tables: vec![],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("project_id"),json_path: None},
                                filter: Filter::Col(Qi(s(""),s("subzero_source")),Field {name: s("id"),json_path: None}),
                                negate: false,
                            },
                            Single {filter: Op(s(">"),SingleVal(s("50"))), field: Field {name: s("id"), json_path: None}, negate: false},
                            Single {filter: In(ListVal(vec![s("51"), s("52")])), field: Field {name: s("id"), json_path: None}, negate: false}
                        ]}
                    }, sub_selects: vec![]},
                    hint: None,
                    alias: None,
                    join: Some(
                        Child(ForeignKey {
                                name: s("project_id_fk"),
                                table: Qi(s("api"),s("tasks")),
                                columns: vec![s("project_id")],
                                referenced_table: Qi(s("api"),s("projects")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                }
            
            ]
        };

        let (query_str, parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None));
        let p0:&(dyn ToSql + Sync) = &ListVal(vec![s("51"), s("52")]);
        let p1:&(dyn ToSql + Sync) = &SingleVal(s("50"));
        let p = Payload(payload);
        let pp: Vec<&(dyn ToSql + Sync)> = vec![&p, p1, p0];
        assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
        let re = Regex::new(r"\s+").unwrap();
        assert_eq!(re.replace_all(query_str.as_str(), " "), re.replace_all(
        r#"
        with 
        subzero_payload as ( select $1::json as json_data ),
        subzero_body as (
            select
                case when json_typeof(json_data) = 'array'
                then json_data
                else json_build_array(json_data)
                end as val
            from
                subzero_payload
        ),
        subzero_source as (
            insert into "api"."projects" ("id","a")
            select "id","a"
            from json_populate_recordset(null::"api"."projects", (select val from subzero_body)) _
            returning "id","a"
        )
        select
            "subzero_source"."a",
            to_jsonb("subzero_source"."b")->1->>'key' as "key",
            row_to_json("subzero_source_clients".*) as "clients",
            coalesce((select json_agg("tasks".*) from (
                select
                    "api"."tasks"."id"
                from "api"."tasks"
                where
            
                    "api"."tasks"."project_id" = "subzero_source"."id"
                    and
                    "api"."tasks"."id" > $2
                    and
                    "api"."tasks"."id" = any ($3)
            ) as "tasks"), '[]') as "tasks"
        from "subzero_source"
        left join lateral (
            select
                "api"."clients"."id"
            from "api"."clients"
            where
            
                "api"."clients"."id" = "subzero_source"."client_id"
        ) as "subzero_source_clients" on true
        "#
        , " "));
    }

    // #[bench]
    // fn bench_fmt_generate_query(b: &mut Bencher){

    // }

    #[test]
    fn test_fmt_select_query(){
        
        let q = Query{ node: Select {order: vec![], limit: None, offset: None, 
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None, cast: None},
                Simple {field: Field {name: s("b"), json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))])}, alias: None, cast: None},
                ],
            from: (s("projects"), None),
            join_tables: vec![],
            where_: ConditionTree { operator: And, conditions: vec![
                Single {filter: Op(s(">="),SingleVal(s("5"))), field: Field {name: s("id"), json_path: None}, negate: false},
                Single {filter: Op(s("<"),SingleVal(s("10"))), field: Field {name: s("id"), json_path: None}, negate: true}
            ]}
        }, sub_selects: vec![
            SubSelect{
                query: Query{ node: Select {order: vec![], limit: None, offset: None, 
                    select: vec![
                        Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
                    ],
                    from: (s("clients"), None),
                    join_tables: vec![],
                    where_: ConditionTree { operator: And, conditions: vec![
                        Single {
                            field: Field {name: s("id"),json_path: None},
                            filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("client_id"),json_path: None}),
                            negate: false,
                       }
                    ]}
                }, sub_selects: vec![]},
                alias: None,
                hint: None,
                join: Some(
                    Parent(ForeignKey {
                            name: s("client_id_fk"),
                            table: Qi(s("api"),s("projects")),
                            columns: vec![s("client_id")],
                            referenced_table: Qi(s("api"),s("clients")),
                            referenced_columns: vec![s("id")],
                        }),
                )
            },
            SubSelect{
                query: Query{ node: Select {order: vec![], limit: None, offset: None, 
                    select: vec![
                        Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
                    ],
                    from: (s("tasks"), None),
                    join_tables: vec![],
                    where_: ConditionTree { operator: And, conditions: vec![
                        Single {
                            field: Field {name: s("project_id"),json_path: None},
                            filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("id"),json_path: None}),
                            negate: false,
                        },
                        Single {filter: Op(s(">"),SingleVal(s("50"))), field: Field {name: s("id"), json_path: None}, negate: false},
                        Single {filter: In(ListVal(vec![s("51"), s("52")])), field: Field {name: s("id"), json_path: None}, negate: false}
                    ]}
                }, sub_selects: vec![]},
                hint: None,
                alias: None,
                join: Some(
                    Child(ForeignKey {
                            name: s("project_id_fk"),
                            table: Qi(s("api"),s("tasks")),
                            columns: vec![s("project_id")],
                            referenced_table: Qi(s("api"),s("projects")),
                            referenced_columns: vec![s("id")],
                        }),
                )
            }
        
        ]};

        let (query_str, parameters, _) = generate(fmt_query(&s("api"), true, None, &q, &None));
        assert_eq!(format!("{:?}", parameters), "[SingleVal(\"50\"), ListVal([\"51\", \"52\"]), SingleVal(\"5\"), SingleVal(\"10\")]");
        let re = Regex::new(r"\s+").unwrap();
        assert_eq!(re.replace_all(query_str.as_str(), " "), re.replace_all(
        r#"
        select
            "api"."projects"."a",
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
        " "));
    }
    
    
    #[test]
    fn test_fmt_condition_tree(){
        assert_eq!(
            format!("{:?}",generate(fmt_condition_tree(
                &Qi(s("schema"),s("table")),
                &ConditionTree {
                    operator: And,
                    conditions: vec![
                        Single {
                            field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                            filter: Op (s(">"), SingleVal(s("2"))),
                            negate: false
                        },
                        Group (false, ConditionTree {
                            operator: And,
                            conditions: vec![
                                Single {
                                    field: Field {name:s("name"), json_path:None},
                                    filter: Op (s(">"), SingleVal(s("2"))),
                                    negate: false
                                },
                                Single {
                                    field: Field {name:s("name"), json_path:None},
                                    filter: Op (s("<"), SingleVal(s("5"))),
                                    negate: false
                                }
                            ]
                        })
                    ]
                }
            ))),
            format!("{:?}",(s("to_jsonb(\"schema\".\"table\".\"name\")->'key'->>21 > $1 and (\"schema\".\"table\".\"name\" > $2 and \"schema\".\"table\".\"name\" < $3)"), vec![SingleVal(s("2")), SingleVal(s("2")), SingleVal(s("5"))], 4))
        );
    }
    
    #[test]
    fn test_fmt_condition(){
        assert_eq!(
            format!("{:?}",generate(fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                    filter: Op (s(">"), SingleVal(s("2"))),
                    negate: false
                }
            ))),
            format!("{:?}",(s("to_jsonb(\"schema\".\"table\".\"name\")->'key'->>21 > $1"), vec![&SingleVal(s("2"))], 2))
        );

        assert_eq!(
            format!("{:?}",generate(fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:None},
                    filter: In (ListVal(vec![s("5"), s("6")])),
                    negate: true
                }
            ))),
            format!("{:?}",(s("not(\"schema\".\"table\".\"name\" = any ($1))"), vec![ListVal(vec![s("5"), s("6")])],2))
        );
    }
    
    #[test]
    fn test_fmt_filter(){
        assert_eq!(format!("{:?}",generate(fmt_filter(&Op (s(">"), SingleVal(s("2")))))), format!("{:?}",(&s("> $1"), vec![SingleVal(s("2"))], 2)));
        assert_eq!(format!("{:?}",generate(fmt_filter(&In (ListVal(vec![s("5"), s("6")]))))), format!("{:?}",(&s("= any ($1)"), vec![ListVal(vec![s("5"), s("6")])],2)));
        assert_eq!(format!("{:?}",generate(fmt_filter(&Fts (s("@@ to_tsquery"), Some(SingleVal(s("eng"))), SingleVal(s("2")))))), r#"("@@ to_tsquery ($1,$2)", [SingleVal("eng"), SingleVal("2")], 3)"#.to_string());
        let p :Vec<&(dyn ToSql + Sync)> = vec![];
        assert_eq!(format!("{:?}",generate(fmt_filter(&Col (Qi(s("api"),s("projects")), Field {name: s("id"), json_path: None})))), format!("{:?}",(&s("= \"api\".\"projects\".\"id\""), p, 1)));
    }
    
    #[test]
    fn test_fmt_operator(){
        assert_eq!(fmt_operator(&s(">")), s("> "));
    }
    
    #[test]
    fn test_fmt_logic_operator(){
        assert_eq!(fmt_logic_operator(&And), s("and"));
        assert_eq!(fmt_logic_operator(&Or), s("or"));
    }
    
    #[test]
    fn test_fmt_select_item(){
        let select = Simple {
            field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
            alias: Some(s("alias")),
            cast: None
        };
        let select_item = fmt_select_item(
            &Qi(s("schema"),s("table")), 
            &select
        );
        let (query_str,_,_) = generate(select_item);
        assert_eq!(query_str,s("to_jsonb(\"schema\".\"table\".\"name\")->'key'->>21 as \"alias\""));
    }
    
    #[test]
    fn test_fmt_qi(){
        assert_eq!(fmt_qi(&Qi(s("schema"),s("table"))), s("\"schema\".\"table\""));
    }
    
    #[test]
    fn test_fmt_field(){
        assert_eq!(
            fmt_field(&Qi(s("a"),s("b")), &Field {name:s("name"), json_path:None}),
            s(r#""a"."b"."name""#)
        );
        assert_eq!(
            fmt_field(&Qi(s("a"),s("b")), &Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])}),
            s(r#"to_jsonb("a"."b"."name")->'key'->>21"#)
        );
    }

    // #[test]
    // fn test_fmt_alias(){
    //     assert_eq!(fmt_alias(&Some(s("alias"))), s(" as \"alias\""));
    // }

    #[test]
    fn test_fmt_json_path(){
        assert_eq!(
            fmt_json_path(&Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])), 
            s("->'key'->>21")
        );
    }

    #[test]
    fn test_fmt_json_operation(){
        assert_eq!(fmt_json_operation(&JArrow(JKey(s("key")))), s("->'key'"));
        assert_eq!(fmt_json_operation(&J2Arrow(JIdx(s("21")))), s("->>21"));
    }

    #[test]
    fn test_fmt_json_operand(){
        assert_eq!(fmt_json_operand(&JKey(s("key"))),s("'key'"));
        assert_eq!(fmt_json_operand(&JIdx(s("23"))),s("23"));
    }
}