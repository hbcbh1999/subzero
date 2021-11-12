
use crate::api::*;
use crate::api::{
    LogicOperator::*,Join::*, Condition::*, Filter::*, Query::*,
    
};
use crate::schema::*;
use crate::error::*;
use snafu::{OptionExt};
use std::collections::HashMap;
use std::collections::BTreeSet;
use serde_json::Value as JsonValue;

use combine::{
    //error::{ParseError},
    easy::{ParseError},
    //easy::Error as ParserError,
    //easy::Error::fmt_errors,
    //easy::{Errors,Error},
    parser::{
        char::{char, digit, letter, spaces, string},
        choice::{choice, optional},
        repeat::{many1, sep_by, sep_by1},
        sequence::{between},
        token::{one_of, none_of},
        
    },
    stream::StreamErrorFor,
    not_followed_by,
    attempt, any, many, eof,
    Parser, Stream, EasyParser
};
use std::collections::HashSet;
use std::iter::FromIterator;


use combine::error::{StreamError};

// #[derive(Debug)]
// pub struct PostgrestRequest<'r> ( pub ApiRequest );

// #[rocket::async_trait]
// impl<'r> FromRequest<'r> for PostgrestRequest<'r> {
//     type Error = std::convert::Infallible;

//     async fn from_request(request: &'r Request<'_>) -> Outcome<Self, Self::Error> {
//         println!("postgrest parser used");
//         Outcome::Success(
//             PostgrestRequest ( ApiRequest{
//                 // root: request.uri().to_string(),
//                 // method: request.method(),
//                 // headers: &request.headers(),
//                 queries: vec![Query {
//                     select: vec![],
//                     from: "".to_string(),
//                     from_alias: None,
//                     where_: ConditionTree { operator: And, conditions: vec![]}
//                 }],
//             })
//         )
//     }
// }

lazy_static!{
    static ref OPERATORS: HashMap<&'static str, &'static str> = [
         ("eq", "=")
        ,("gte", ">=")
        ,("gt", ">")
        ,("lte", "<=")
        ,("lt", "<")
        ,("neq", "<>")
        ,("like", "like")
        ,("ilike", "ilike")
        //,("in", "in")
        ,("is", "is")
        ,("cs", "@>")
        ,("cd", "<@")
        ,("ov", "&&")
        ,("sl", "<<")
        ,("sr", ">>")
        ,("nxr", "&<")
        ,("nxl", "&>")
        ,("adj", "-|-")
    ].iter().copied().collect();
    static ref FTS_OPERATORS: HashMap<&'static str, &'static str> = [
         ("fts", "@@ to_tsquery")
        ,("plfts", "@@ plainto_tsquery")
        ,("phfts", "@@ phraseto_tsquery")
        ,("wfts", "@@ websearch_to_tsquery")

    ].iter().copied().collect();
}

fn lex<Input, P>(p: P) -> impl Parser<Input, Output = P::Output>
where
    P: Parser<Input>,
    Input: Stream<Token = char>,
    // <Input as StreamOnce>::Error: ParseError<
    //     <Input as StreamOnce>::Token,
    //     <Input as StreamOnce>::Range,
    //     <Input as StreamOnce>::Position,
    // >,
{
    p.skip(spaces())
}

fn field_name<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    let dash = attempt(char('-').skip(not_followed_by(char('>'))));
    lex(choice(( 
        quoted_value(), 
        sep_by1(
            many1::<String, _, _>(choice((letter(),digit(),one_of("_".chars())))),
            dash
        ).map(|words: Vec<String>| words.join("-"))
    )))
    .expected("field name (* or [a..z0..9_])")
}

fn quoted_value<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    between(
        char('"'),
        char('"'),
        many1(none_of("\"".chars()))
    )
}

fn field<Input>() -> impl Parser<Input, Output = Field>
where Input: Stream<Token = char>
{
    field_name()
    .and(optional(json_path()))
    .map(|(name, json_path)| 
        Field {
            name: name,
            json_path: json_path,
        }
    )
}

fn json_path<Input>() -> impl Parser<Input, Output = Vec<JsonOperation>>
where Input: Stream<Token = char>
{

    //let end = look_ahead( string("->").or(string("::")).map(|_| ()).or(eof()) );
    let arrow = attempt(string("->>")).or(string("->")).map(|v|
        match v {
            "->>" => JsonOperation::J2Arrow,
            "->" => JsonOperation::JArrow,
            &_ => panic!("error parsing json path")
        }
    );
    let signed_number = 
        optional(string("-")).and(many1(digit()))
        .map(|v:(Option<&str>, String)|{
            let (s,n) = v;
            return format!("{}{}", s.unwrap_or(""), n);
        });
    let operand = choice((
        signed_number.map(|n| JsonOperand::JIdx(n)),
        field_name().map(|k| JsonOperand::JKey(k))
    ));
    //many1(arrow.and(operand.and(end)).map(|((arrow,(operand,_)))| arrow(operand)))
    many1(arrow.and(operand).map(|(arrow,operand)| arrow(operand)))
}

fn alias_separator<Input>() -> impl Parser<Input, Output = char>
where Input: Stream<Token = char>
{
    attempt(char(':').skip(not_followed_by(char(':'))))
}

fn alias<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    choice((
        many1(choice((letter(),digit(),one_of("@._".chars())))),
        quoted_value()
    ))
    .and(alias_separator())
    .map(|(a,_) | a)
}

fn dot<Input>() -> impl Parser<Input, Output = char>
where Input: Stream<Token = char>
{
    char('.')
}

fn tree_path<Input>() -> impl Parser<Input, Output = (Vec<String>, Field)>
where Input: Stream<Token = char>
{
    sep_by1(field_name(), dot()).and(optional(json_path()))
    .map(|a:(Vec<String>,Option<Vec<JsonOperation>>)|{
        let (names, json_path) = a;
        match names.split_last() {
            Some((name, path)) =>  (path.to_vec(), Field {name: name.clone(), json_path}),
            None => panic!("failed to parse tree path")
        }
        
    })
}

fn logic_tree_path<Input>() -> impl Parser<Input, Output = (Vec<String>, bool, LogicOperator)>
where Input: Stream<Token = char>
{
    sep_by1(field_name(), dot())
    .map(|names:Vec<String>|{
        match names.split_last() {
            Some((name, path)) => {
                let op = match name.as_str() {
                    "and" => And,
                    "or" => Or,
                    &_ => panic!("unknown logic operator")
                };
                match path.split_last() {
                    Some((negate, path1)) => {
                        if negate == "not" {(path1.to_vec(), true, op)}
                        else {(path.to_vec(), false, op)}
                    }
                    None => (path.to_vec(), false, op)
                }
            },
            None => panic!("failed to parse logic tree path")
        }
        
    })
}

fn select<'r, Input>() -> impl Parser<Input, Output = Vec<SelectItem<'r>>>
where Input: Stream<Token = char>
{
    sep_by1(select_item(), lex(char(','))).skip(eof())
}

fn columns<Input>() -> impl Parser<Input, Output = Vec<String>>
where Input: Stream<Token = char>
{
    sep_by1(field_name(), lex(char(','))).skip(eof())
}

// We need to use `parser!` to break the recursive use of `select_item` to prevent the returned parser
// from containing itself
#[inline]
fn select_item<'r, Input>() -> impl Parser<Input, Output = SelectItem<'r>>
where Input: Stream<Token = char>
{
    select_item_()
}

parser! {
    #[inline]
    fn select_item_['r, Input]()(Input) -> SelectItem<'r>
    where [ Input: Stream<Token = char> ]
    {
       let column = 
            optional(attempt(alias()))
            .and(field())
            .map(|(alias, field)| SelectItem::Simple {field: field, alias: alias});
        let sub_select = (
            optional(attempt(alias())),
            lex(field_name()),
            optional(char('!').or(char('.')).and(field_name()).map(|(_,hint)| hint)),
            between(lex(char('(')), lex(char(')')),  sep_by(select_item(), lex(char(','))))
        )
        .map(|(alias, from, join_hint, select)| 
            SelectItem::SubSelect {
                query: Select {
                    select: select,
                    from: from,
                    //from_alias: alias,
                    where_: ConditionTree { operator: And, conditions: vec![]}
                },
                alias: alias,
                hint: join_hint,
                join: None
            }
        );

        attempt(sub_select).or(column)
    }
}

fn single_value<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    many(any())
}

fn logic_single_value<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    choice((
        attempt(
            quoted_value().skip(
                not_followed_by(none_of(",)".chars()))
            )
        ),
        between(char('{'), char('}'), many(none_of("{}".chars()))).map(|v:String| format!("{{{}}}",v) ),
        many(none_of(",)".chars())),
    ))
}

fn list_value<Input>() -> impl Parser<Input, Output = Vec<String>>
where Input: Stream<Token = char>
{
    lex(
        between(
            lex(char('(')),
            lex(char(')')),
            sep_by1(list_element(), lex(char(',')))
        )
    )
}

fn list_element<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    attempt(quoted_value().skip(not_followed_by(none_of(",)".chars())))).or(many(none_of(",)".chars())))
}

fn operator<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    many1(letter()).and_then(|o: String| {
        match OPERATORS.get(o.as_str()) {
            Some(oo) => Ok(oo.to_string()),
            None => {
                //println!("unknown operator {}", o);
                Err(StreamErrorFor::<Input>::message_static_message("unknown operator"))
            }
        }
    })
}

fn fts_operator<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    many1(letter()).and_then(|o: String| {
        match FTS_OPERATORS.get(o.as_str()) {
            Some(oo) => Ok(oo.to_string()),
            None => Err(StreamErrorFor::<Input>::message_static_message("unknown fts operator"))
        }
    })
}
fn negatable_filter<Input>() -> impl Parser<Input, Output = (bool,Filter)>
where Input: Stream<Token = char>
{
    optional(attempt(string("not").skip(dot()))).and(filter()).map(|(n,f)| (n.is_some(),f))
}
//TODO! filter and logic_filter parsers should be combined, they differ only in single_value parser type
fn filter<Input>() -> impl Parser<Input, Output = Filter>
where Input: Stream<Token = char>
{
    //let value = if use_logical_value { opaque!(logic_single_value()) } else { opaque!(single_value()) };

    choice((
        attempt(operator().skip(dot()).and(single_value()).map(|(o,v)| Filter::Op(o, SingleVal(v)))),
        attempt(string("in").skip(dot()).and(list_value()).map(|(_,v)| Filter::In(ListVal(v)))),
        fts_operator()
            .and(optional(
                between(
                    char('('),
                    char(')'),
                    many1(choice(
                        (letter(),digit(),char('_'))
                    ))
                )
            ))
            .skip(dot())
            .and(single_value())
            .map(|((o,l),v)| Filter::Fts (o,l,SingleVal(v))),

    ))
}

fn logic_filter<Input>() -> impl Parser<Input, Output = Filter>
where Input: Stream<Token = char>
{
    //let value = if use_logical_value { opaque!(logic_single_value()) } else { opaque!(single_value()) };

    choice((
        attempt(operator().skip(dot()).and(logic_single_value()).map(|(o,v)| Filter::Op(o, SingleVal(v)))),
        attempt(string("in").skip(dot()).and(list_value()).map(|(_,v)| Filter::In(ListVal(v)))),
        fts_operator()
            .and(optional(
                between(
                    char('('),
                    char(')'),
                    many1(choice(
                        (letter(),digit(),char('_'))
                    ))
                )
            ))
            .skip(dot())
            .and(logic_single_value())
            .map(|((o,l),v)| Filter::Fts (o,l,SingleVal(v))),

    ))
}

fn logic_condition<Input>() -> impl Parser<Input, Output = Condition>
where Input: Stream<Token = char>
{
    logic_condition_()
}

parser! {
    #[inline]
    fn logic_condition_[Input]()(Input) -> Condition
    where [ Input: Stream<Token = char> ]
    {
        let single = field().skip(dot())
            .and(optional(attempt(string("not").skip(dot()))))
            .and(logic_filter())
            .map(|((field,negate),filter)|
                Condition::Single {
                    field: field, 
                    filter: filter, 
                    negate: negate.is_some()
                }
            );

        let group = optional(attempt(string("not").skip(dot())))
            .and(
                choice((string("and"),string("or"))).map(|l|
                    match l {
                        "and" => And,
                        "or" => Or,
                        &_ => panic!("unknown logic operator")
                    }
                )
                .and(between(lex(char('(')),lex(char(')')),sep_by1(logic_condition(), lex(char(',')))))
            )
            .map(|(negate, (operator, conditions))|{
                Condition::Group(negate.is_some(), ConditionTree {
                    operator: operator,
                    conditions: conditions
                })
            });
        
        attempt(group).or(single)
    }
}

fn is_logical(s: &str)->bool{ s.ends_with("or") || s.ends_with("and") }

fn to_app_error<'a>(s: &'a str, p: String) -> impl Fn(ParseError<&'a str>) -> Error {
    move |e| {
        let details = format!("{}", e.map_position(|p| p.translate_position(s)));
        let message = format!("Failed to parse {} parameter ({})", p, s);
        Error::ParseRequestError {message, details, parameter:p.clone()}
    }
}

pub fn get_parameter<'a >(name: &str, parameters: &'a Vec<(&'a str, &'a str)>)->Option<&'a (&'a str, &'a str)> {
    parameters.iter().filter(|v| v.0 == name).next()
}

pub fn parse<'r>(
    schema: &String, 
    root: &String, 
    db_schema: &DbSchema, 
    method: &Method, 
    parameters: Vec<(&str, &str)>, 
    body: Option<&'r String>, 
    headers: HashMap<&'r str, &'r str>,
    cookies: HashMap<&'r str, &'r str>,
) -> Result<ApiRequest<'r>> {
    // extract and parse select item
    let &(_, select_param) = get_parameter("select", &parameters).unwrap_or(&("select","*"));
    let (select_items, _) = select().easy_parse(select_param).map_err(to_app_error(select_param, "select".to_string()))?;
    let mut query = match *method {
        Method::GET => {
            let mut q = Select {
                select: select_items,
                from: root.clone(),
                where_: ConditionTree { operator: And, conditions: vec![] }
            };
            add_join_conditions(&mut q, &schema, db_schema)?;
            Ok(q)
        },
        Method::POST => {
            let payload = body.context(InvalidBody)?;
            let columns = match get_parameter("columns", &parameters){
                Some(&(_, columns_param)) => 
                    columns().easy_parse(columns_param)
                    .map(|v| v.0)
                    .map_err(to_app_error(columns_param, "columns".to_string())),
                None => {
                    let json_payload: Result<JsonValue,serde_json::Error> = serde_json::from_str(payload);
                    match json_payload {
                        Ok(j) => {
                            match j {
                                JsonValue::Object(m) => Ok(m.keys().cloned().collect()),
                                JsonValue::Array(v) => {
                                    match v.get(0) {
                                        Some(JsonValue::Object(m)) => {
                                            let canonical_set:HashSet<&String> = HashSet::from_iter(m.keys());
                                            let all_keys_match = v.iter().all(|vv|
                                                match vv {
                                                    JsonValue::Object(mm) => canonical_set == HashSet::from_iter(mm.keys()),
                                                    _ => false
                                                }
                                            );
                                            if all_keys_match {
                                                Ok(m.keys().cloned().collect())
                                            }
                                            else {
                                                let details = format!("All object keys must match");
                                                let message = format!("Failed to parse json body");
                                                Err(Error::ParseRequestError {message, details, parameter: payload.to_string()})
                                            }
                                            
                                        },
                                        _ => Ok(vec![])
                                    }
                                },
                                _ => Ok(vec![])
                            }
                        },
                        Err(_) => {
                            let details = format!("");
                            let message = format!("Failed to parse json body");
                            Err(Error::ParseRequestError {message, details, parameter: payload.to_string()})
                        }
                    }
                }
            }?;

            let mut q = Insert {
                into: root.clone(),
                columns: columns,
                payload,
                where_: ConditionTree { operator: And, conditions: vec![] },
                returning: vec![],
                select: select_items,
                //, onConflict :: Maybe (PreferResolution, [FieldName])
            };
            
            add_join_conditions(&mut q, &schema, db_schema)?;
            let (select, table, returning) = match &mut q {
                Insert {select,into,returning,..}=>(select,into,returning),
                _ => panic!("q can not be of other types")
            };
            
            let new_returning = select.iter().map(|s|{
                match s {
                    SelectItem::Simple {field, ..} => Ok(vec![&field.name]),
                    SelectItem::SubSelect {join:Some(j), ..} => {
                        match j {
                            Child(fk) => Ok(fk.referenced_columns.iter().collect()),
                            Parent(fk) => Ok(fk.columns.iter().collect()),
                            Many(_,fk1,fk2) => {
                                let mut f = vec![];
                                f.extend(fk1.referenced_columns.iter());
                                f.extend(fk2.referenced_columns.iter());
                                Ok(f)
                            },
                        }
                    },
                    _ => Err(Error::NoRelBetween {origin: table.clone(), target: "subselect".to_string()})
                }
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter().flatten().cloned().collect::<BTreeSet<_>>();

            returning.extend(new_returning);
            Ok(q)
        },
        // Method::PATCH => Ok(Update),
        // Method::PUT => Ok(Upsert),
        // Method::DELETE => Ok(Delete),
        _ => Err(Error::UnsupportedVerb)
    }?;

    
    
    //extract and parse logical filters "and=(...)"
    let logical_filters_str:Vec<&(&str, &str)> = parameters.iter().filter(|(k,_)|is_logical(k)).collect();
    let mut logical_conditions = logical_filters_str.into_iter().map(|&(p, f)|{
        let ((tp, n, lo), _) = logic_tree_path().easy_parse(p).map_err(to_app_error(p, p.to_string()))?;
        let ns = if n { "not." } else { "" };
        let los = if lo == And {  "and" } else { "or" };
        let s = format!("{}{}{}", ns,los,f);
        let (c, _) = logic_condition().easy_parse(s.as_str()).map_err(to_app_error(&s, p.to_string()))?;
        Ok((tp, c))
    }).collect::<Vec<Result<_,Error>>>().into_iter().collect::<Result<Vec<_>,_>>()?;
    
    //extract and parse simple filters "id=gt.10"
    let filters_str:Vec<&(&str, &str)> = parameters.iter().filter(|(k,_)|
        !(is_logical(k) || ["select","columns"].contains(&k))
    ).collect();
    let filters = filters_str.into_iter().map(|&(p, f)|{
        let (tp, _)= tree_path().easy_parse(p).map_err(to_app_error(p, p.to_string()))?;
        let ((n,ff), _) = negatable_filter().easy_parse(f).map_err(to_app_error(f, p.to_string()))?;
        Ok((tp, n, ff))
    }).collect::<Vec<Result<_,Error>>>().into_iter().collect::<Result<Vec<_>,_>>()?;
    let mut single_conditions = filters.into_iter().map(|((p,fld), negate, flt)|{
        (p, Condition::Single {field:fld, filter:flt, negate: negate})
    }).collect::<Vec<(_,_)>>();


    let mut conditions = vec![];
    conditions.append(&mut single_conditions);
    conditions.append(&mut logical_conditions);

    insert_conditions(&mut query, conditions);
    

    Ok(ApiRequest {
        method: method.clone(),
        query,
        headers,
        cookies,
    })
}

fn get_join(current_schema: &String, db_schema: &DbSchema, origin: &String, target: &String, hint: &mut Option<String>) -> Result<Join>{
    let schema = db_schema.schemas.get(current_schema).context(UnacceptableSchema {schemas: vec![current_schema.to_owned()]})?;
    let origin_table = schema.objects.get(origin).context(UnknownRelation {relation: origin.to_owned()})?;
    match schema.objects.get(target) {
        // the target is an existing table
        Some(target_table) => {
            match hint {
                Some(h) => {
                    // projects?select=clients!projects_client_id_fkey(*)
                    if let Some(fk) = target_table.foreign_keys.get(h) {
                        return Ok(Parent(fk.clone()));
                    }
                    if let Some(fk) = target_table.foreign_keys.get(h) {
                        return Ok(Child(fk.clone()));
                    }

                    // users?select=tasks!users_tasks(*)
                    // TODO!!! handle
                    if let Some(join_table) = schema.objects.get(h) {
                        let ofk1 = join_table.foreign_keys.values().find_map(|fk| {
                            if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin {
                                Some(fk)
                            }
                            else { None }
                        });
                        let ofk2 = join_table.foreign_keys.values().find_map(|fk| {
                            if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target {
                                Some(fk)
                            }
                            else { None }
                        });
                        if let (Some(fk1), Some(fk2)) = (ofk1, ofk2){
                            return Ok( Many(join_table.name.clone(), fk1.clone(), fk2.clone()) )
                        }
                        else {
                            return Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()})
                        }
                        
                    }

                    // projects?select=clients!client_id(*)
                    // projects?select=clients!id(*)
                    let mut joins = vec![];
                    
                    joins.extend(origin_table.foreign_keys.iter()
                        .filter(|&(_, fk)|
                               &fk.referenced_table.0 == current_schema 
                            && &fk.referenced_table.1 == target
                            && fk.columns.len() == 1
                            && ( fk.columns.contains(h) || fk.referenced_columns.contains(h) )
                        )
                        .map(|(_, fk)| Parent(fk.clone()))
                        .collect::<Vec<_>>()
                    );
                    joins.extend(target_table.foreign_keys.iter()
                    .filter(|&(_, fk)|
                           &fk.referenced_table.0 == current_schema 
                        && &fk.referenced_table.1 == origin
                        && fk.columns.len() == 1
                        && ( fk.columns.contains(h) || fk.referenced_columns.contains(h) )
                    )
                    .map(|(_, fk)| Parent(fk.clone()))
                    .collect::<Vec<_>>()
                    );
                    
                    if joins.len() == 1 {
                        Ok(joins[0].clone())
                    }
                    else if joins.len() == 0 {
                        Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()})
                    }
                    else{
                        Err(Error::AmbiguousRelBetween {origin: origin.to_owned(), target: target.to_owned(), relations: joins})
                    }
                    
                    //Ok(joins)
                }, 
                // there is no hint, look for foreign keys between the two tables
                None => {
                    // check child relations
                    // projects?select=tasks(*)
                    let child_joins = target_table.foreign_keys.iter()
                    .filter(|&(_, fk)| &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin )
                    .map(|(_, fk)| Child(fk.clone()))
                    .collect::<Vec<_>>();
                    
                    // check parent relations
                    // projects?select=clients(*)
                    let parent_joins = origin_table.foreign_keys.iter()
                    .filter(|&(_, fk)| &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target )
                    .map(|(_, fk)| Parent(fk.clone()))
                    .collect::<Vec<_>>();

                    let mut joins = vec![];
                    joins.extend(child_joins);
                    joins.extend(parent_joins);
                    
                    if joins.len() == 1 {
                        Ok(joins[0].clone())
                    }
                    else if joins.len() == 0 {
                        // check many to many relations
                        // users?select=tasks(*)
                        let many_joins = schema.objects.values().filter_map(|join_table|{
                            let fk1 = join_table.foreign_keys.values().find_map(|fk| {
                                if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin {
                                    Some(fk)
                                }
                                else { None }
                            })?;
                            let fk2 = join_table.foreign_keys.values().find_map(|fk| {
                                if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target {
                                    Some(fk)
                                }
                                else { None }
                            })?;
                            Some( Many(join_table.name.clone(), fk1.clone(), fk2.clone()) )
                        }).collect::<Vec<_>>();
                        if many_joins.len() == 1 {
                            Ok(many_joins[0].clone())
                        }
                        else if many_joins.len() == 0 {
                            Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()})
                        }
                        else{
                            Err(Error::AmbiguousRelBetween {origin: origin.to_owned(), target: target.to_owned(), relations: many_joins})
                        }
                    }
                    else{
                        Err(Error::AmbiguousRelBetween {origin: origin.to_owned(), target: target.to_owned(), relations: joins})
                    }
                }
            }
        },
        // the target is not a table
        None => {
            match origin_table.foreign_keys.get(target) {
                // the target is a foreign key name
                // projects?select=projects_client_id_fkey(*)
                Some (fk) => if &fk.referenced_table.0 == current_schema { Ok(Child(fk.clone())) }
                             else { Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()}) }
                // the target is a foreign key column
                // projects?select=client_id(*)
                None => {
                    let joins = origin_table.foreign_keys.iter()
                        .filter(|&(_, fk)| &fk.referenced_table.0 == current_schema && fk.columns.len() == 1 && fk.columns.contains(target) )
                        .map(|(_, fk)| Child(fk.clone()))
                        .collect::<Vec<_>>();
                    //Ok(joins)
                    if joins.len() == 1 {
                        Ok(joins[0].clone())
                    }
                    else if joins.len() == 0 {
                        Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()})
                    }
                    else{
                        Err(Error::AmbiguousRelBetween {origin: origin.to_owned(), target: target.to_owned(), relations: joins})
                    }
                }
            }
        }
    }
}

fn add_join_conditions( query: &mut Query, schema: &String, db_schema: &DbSchema )->Result<()>{
    let subzero_source = &"subzero_source".to_string();
    let (select, parent_table, parent_alias) : (&mut Vec<SelectItem>, &String, &String) = match query {
        Select {select, from, ..} => (select.as_mut(), from, from),
        Insert {select, into, ..} => (select.as_mut(), into, subzero_source),
    };
    
    for s in select.iter_mut() {
        match s {
            SelectItem::SubSelect{query: q, join, hint, ..} => {
                let child_table = match q {
                    Select {from, ..} => from,
                    _ => panic!("there should not be any Insert queries as subselects"),
                };
                let new_join = get_join(schema, db_schema, parent_table, child_table, hint)?;
                match &new_join {
                    Parent (fk) => insert_conditions(q, vec![(vec![],Single { //clients
                        field: Field {name: fk.referenced_columns[0].clone(), json_path: None},
                        filter: Col (Qi (schema.clone(), parent_alias.clone()), Field {name: fk.columns[0].clone(), json_path: None}),
                        negate: false
                    })]),
                    Child (fk) => insert_conditions(q, vec![(vec![],Single { //tasks
                        field: Field {name: fk.columns[0].clone(), json_path: None},
                        filter: Col (Qi (schema.clone(), parent_alias.clone()), Field {name: fk.referenced_columns[0].clone(), json_path: None}),
                        negate: false
                    })]),
                    Many (_tbl, _fk1, _fk2) => insert_conditions(q, vec![])
                }
                std::mem::swap(join, &mut Some(new_join));
                add_join_conditions( q, schema, db_schema)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn insert_conditions( query: &mut Query, mut conditions: Vec<(Vec<String>,Condition)>){
    let (select, query_conditions) : (&mut Vec<SelectItem>, &mut Vec<Condition>) = match query {
        Select {select, where_, ..} => (select.as_mut(), where_.conditions.as_mut()),
        Insert {select, where_, ..} => (select.as_mut(), where_.conditions.as_mut()),
    };
    let node_conditions = conditions.drain_filter(|(path, _)| path.len() == 0).map(|(_,c)| c).collect::<Vec<_>>();
    node_conditions.into_iter().for_each(|c| query_conditions.push(c));
    
    for s in select.iter_mut() {
        match s {
            SelectItem::SubSelect{query: q, ..} => {
                let from : &String = match q {
                    Select {from, ..} => from,
                    _ => panic!("there should not be any Insert queries as subselects"),
                };
                let node_conditions = conditions.drain_filter(|(path, _)|
                    if path.get(0) == Some(from) { path.remove(0); true }
                    else {false}
                ).collect::<Vec<_>>();
                insert_conditions(q, node_conditions);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::matches;
    use pretty_assertions::{assert_eq, assert_ne};
    use crate::api::{JsonOperand::*, JsonOperation::*, SelectItem::*, Condition::{Group, Single}};
    use combine::{stream::PointerOffset};
    use combine::easy::{Error, Errors};
    use combine::stream::position;
    use combine::stream::position::SourcePosition;
    //use combine::error::StringStreamError;
    use crate::error::Error as AppError;
    use combine::EasyParser;
    use super::*;
    
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
    
    fn s(s:&str) -> String {
        s.to_string()
    }
    
    #[test]
    fn test_insert_conditions(){
       
        let mut query = Select {
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                        ],
                        from: s("child"),
                        where_: ConditionTree { operator: And, conditions: vec![]}
                    },
                    alias: None,
                    hint: None,
                    join: None
                }
            ],
            from: s("parent"),
            //from_alias: None,
            where_: ConditionTree { operator: And, conditions: vec![] }
        };
        let condition = Single {
            field: Field {name: s("a"), json_path: None},
            filter: Filter::Op(s(">="),SingleVal(s("5"))),
            negate: false,
        };
        insert_conditions( &mut query, vec![
            (vec![],condition.clone()),
            (vec![s("child")],condition.clone()),
        ]);
        assert_eq!(query,
            Select {
                select: vec![
                    Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                    SubSelect{
                        query: Select {
                            select: vec![
                                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                            ],
                            from: s("child"),
                            //from_alias: None,
                            where_: ConditionTree { operator: And, conditions: vec![condition.clone()] }
                        },
                        alias: None,
                        hint: None,
                        join: None
                    }
                ],
                from: s("parent"),
                where_: ConditionTree { operator: And, conditions: vec![condition.clone()] }
            }
        );
    }
    
    #[test]
    fn test_parse_get(){
        
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
       
        let a = parse(&s("api"), &s("projects"), &db_schema, &Method::GET, vec![
            ("select", "id,name,clients(id),tasks(id)"),
            ("id","not.gt.10"),
            ("tasks.id","lt.500"),
            ("not.or", "(id.eq.11,id.eq.12)"),
            ("tasks.or", "(id.eq.11,id.eq.12)"),
            ], None, HashMap::new(), HashMap::new());

        assert_eq!(
            a.unwrap()
            ,
            ApiRequest {
                method: Method::GET,
                headers: HashMap::new(),
                cookies: HashMap::new(),
                query: 
                    Select {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                            Simple {field: Field {name: s("name"), json_path: None}, alias: None},
                            SubSelect{
                                query: Select {
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: s("clients"),
                                    //from_alias: None,
                                    where_: ConditionTree { operator: And, conditions: vec![
                                        Single {
                                            field: Field {name: s("id"),json_path: None},
                                            filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("client_id"),json_path: None}),
                                            negate: false,
                                       }
                                    ]}
                                },
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
                                query: Select {
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: s("tasks"),
                                    //from_alias: None,
                                    where_: ConditionTree { operator: And, conditions: vec![
                                        Single {
                                            field: Field {name: s("project_id"),json_path: None},
                                            filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("id"),json_path: None}),
                                            negate: false,
                                       },
                                        Single {
                                            field: Field {name: s("id"), json_path: None},
                                            filter: Filter::Op(s("<"),SingleVal(s("500"))),
                                            negate: false,
                                        },
                                        Group(
                                            false,
                                            ConditionTree {
                                                operator: Or,
                                                conditions: vec![
                                                    Single {filter: Filter::Op(s("="),SingleVal(s("11"))), field: Field {name: s("id"), json_path: None}, negate: false },
                                                    Single {filter: Filter::Op(s("="),SingleVal(s("12"))), field: Field {name: s("id"), json_path: None}, negate: false }
                                                ]
                                            }
                                        )
                                    ]}
                                },
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
                        ],
                        from: s("projects"),
                        //from_alias: None,
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"), json_path: None},
                                filter: Filter::Op(s(">"),SingleVal(s("10"))),
                                negate: true,
                            },
                            Group(
                                true,
                                ConditionTree {
                                    operator: Or,
                                    conditions: vec![
                                        Single {filter: Filter::Op(s("="),SingleVal(s("11"))), field: Field {name: s("id"), json_path: None}, negate: false },
                                        Single {filter: Filter::Op(s("="),SingleVal(s("12"))), field: Field {name: s("id"), json_path: None}, negate: false }
                                    ]
                                }
                            )
                        ] }
                    }
                
            }
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, vec![
                ("select", "id,name,unknown(id)")
            ], None, HashMap::new(), HashMap::new()),
            Err(AppError::NoRelBetween{origin:s("projects"), target:s("unknown")})
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, vec![
                ("select", "id-,na$me")
            ], None, HashMap::new(), HashMap::new()),
            Err(AppError::ParseRequestError{
                parameter:s("select"),
                message: s("Failed to parse select parameter (id-,na$me)"),
                details: s("Parse error at 3\nUnexpected `,`\nExpected `letter`, `digit` or `_`\n")
            })
        );
    }

    #[test]
    fn test_parse_post(){
        
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
       
        let payload = s(r#"{"id":10, "name":"john"}"#);
        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&payload), HashMap::new(), HashMap::new())
            ,
            Ok(ApiRequest {
                method: Method::POST,
                headers: HashMap::new(),
                cookies: HashMap::new(),
                query: 
                    Insert {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        payload: &payload,
                        into: s("projects"),
                        columns: vec![s("id"), s("name")],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"), json_path: None},
                                filter: Filter::Op(s(">"),SingleVal(s("10"))),
                                negate: false,
                            }
                        ] },
                        returning: vec![s("id")]
                    }
                
            })
        );
        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id,name"),
                ("id","gt.10"),
                ("columns","id,name"),
            ], Some(&payload), HashMap::new(), HashMap::new())
            ,
            Ok(ApiRequest {
                method: Method::POST,
                headers: HashMap::new(),
                cookies: HashMap::new(),
                query: 
                    Insert {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                            Simple {field: Field {name: s("name"), json_path: None}, alias: None},
                        ],
                        payload: &payload,
                        into: s("projects"),
                        columns: vec![s("id"), s("name")],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"), json_path: None},
                                filter: Filter::Op(s(">"),SingleVal(s("10"))),
                                negate: false,
                            }
                        ] },
                        returning: vec![ s("id"), s("name"), ]
                    }
                
            })
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id"),
                ("id","gt.10"),
                ("columns","id,1 name"),
            ], Some(&s(r#"{"id":10, "name":"john", "phone":"123"}"#)), HashMap::new(), HashMap::new())
            ,
            Err(AppError::ParseRequestError {
                message: s("Failed to parse columns parameter (id,1 name)"),
                details: s("Parse error at 5\nUnexpected `n`\nExpected `,`, `whitespaces` or `end of input`\n"),
                parameter: s("columns"),
            })
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"{"id":10, "name""#)), HashMap::new(), HashMap::new())
            ,
            Err(AppError::ParseRequestError {
                message: s("Failed to parse json body"),
                details: s(""),
                parameter: s("{\"id\":10, \"name\""),
            })
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "phone":"123"}]"#)), HashMap::new(), HashMap::new())
            ,
            Err(AppError::ParseRequestError {
                message: s("Failed to parse json body"),
                details: s("All object keys must match"),
                parameter: s(r#"[{"id":10, "name":"john"},{"id":10, "phone":"123"}]"#),
            })
        );

        

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, vec![
                ("select", "id,name,unknown(id)")
            ], None, HashMap::new(), HashMap::new()),
            Err(AppError::NoRelBetween{origin:s("projects"), target:s("unknown")})
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, vec![
                ("select", "id-,na$me")
            ], None, HashMap::new(), HashMap::new()),
            Err(AppError::ParseRequestError{
                parameter:s("select"),
                message: s("Failed to parse select parameter (id-,na$me)"),
                details: s("Parse error at 3\nUnexpected `,`\nExpected `letter`, `digit` or `_`\n")
            })
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#)), HashMap::new(), HashMap::new())
            ,
            Ok(ApiRequest {
                method: Method::POST,
                headers: HashMap::new(),
                cookies: HashMap::new(),
                query: 
                    Insert {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        payload: &s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#),
                        into: s("projects"),
                        columns: vec![s("id"), s("name")],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"), json_path: None},
                                filter: Filter::Op(s(">"),SingleVal(s("10"))),
                                negate: false,
                            }
                        ] },
                        returning: vec![s("id")]
                    }
                
            })
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, vec![
                ("select", "id,name,tasks(id),clients(id)"),
                ("id","gt.10"),
                ("tasks.id","gt.20"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#)), HashMap::new(), HashMap::new())
            ,
            Ok(ApiRequest {
                method: Method::POST,
                headers: HashMap::new(),
                cookies: HashMap::new(),
                query: 
                    Insert {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                            Simple {field: Field {name: s("name"), json_path: None}, alias: None},
                            SubSelect{
                                query: Select {
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: s("tasks"),
                                    //from_alias: None,
                                    where_: ConditionTree { operator: And, conditions: vec![
                                        Single {
                                            field: Field {name: s("project_id"),json_path: None},
                                            filter: Filter::Col(Qi(s("api"),s("subzero_source")),Field {name: s("id"),json_path: None}),
                                            negate: false,
                                       },
                                        Single {
                                            field: Field {name: s("id"), json_path: None},
                                            filter: Filter::Op(s(">"),SingleVal(s("20"))),
                                            negate: false,
                                        }
                                    ]}
                                },
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
                            },
                            SubSelect{
                                query: Select {
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: s("clients"),
                                    //from_alias: None,
                                    where_: ConditionTree { operator: And, conditions: vec![
                                        Single {
                                            field: Field {name: s("id"),json_path: None},
                                            filter: Filter::Col(Qi(s("api"),s("subzero_source")),Field {name: s("client_id"),json_path: None}),
                                            negate: false,
                                       }
                                    ]}
                                },
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
                        ],
                        payload: &s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#),
                        into: s("projects"),
                        columns: vec![s("id"), s("name")],
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"), json_path: None},
                                filter: Filter::Op(s(">"),SingleVal(s("10"))),
                                negate: false,
                            }
                        ] },
                        returning: vec![s("client_id"), s("id"), s("name")]
                    }
                
            })
        );
    }

    #[test]
    fn test_get_join_conditions(){
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        assert_eq!( get_join(&s("api"), &db_schema, &s("projects"), &s("tasks"), &mut None),
            Ok(
                
                    Child(ForeignKey {
                        name: s("project_id_fk"),
                        table: Qi(s("api"),s("tasks")),
                        columns: vec![s("project_id")],
                        referenced_table: Qi(s("api"),s("projects")),
                        referenced_columns: vec![s("id")],
                    })
                
            )
        );
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("projects"), &mut None),
            Ok(
                
                    Parent(ForeignKey {
                        name: s("project_id_fk"),
                        table: Qi(s("api"),s("tasks")),
                        columns: vec![s("project_id")],
                        referenced_table: Qi(s("api"),s("projects")),
                        referenced_columns: vec![s("id")],
                    })
                
            )
        );
        assert_eq!( get_join(&s("api"), &db_schema, &s("clients"), &s("projects"), &mut None),
            Ok(
                
                    Child(ForeignKey {
                        name: s("client_id_fk"),
                        table: Qi(s("api"),s("projects")),
                        columns: vec![s("client_id")],
                        referenced_table: Qi(s("api"),s("clients")),
                        referenced_columns: vec![s("id")],
                    })
                
            )
        );
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("users"), &mut None),
            Ok(
               
                    Many(
                        s("users_tasks"),
                        ForeignKey {
                            name: s("task_id_fk"),
                            table: Qi(s("api"),s("users_tasks")),
                            columns: vec![s("task_id")],
                            referenced_table: Qi(s("api"),s("tasks")),
                            referenced_columns: vec![s("id")],
                        },
                        ForeignKey {
                            name: s("user_id_fk"),
                            table: Qi(s("api"),s("users_tasks")),
                            columns: vec![s("user_id")],
                            referenced_table: Qi(s("api"),s("users")),
                            referenced_columns: vec![s("id")],
                        },
                    )
               
            )
        );
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("users"), &mut Some(s("users_tasks"))),
            Ok(
               
                    Many(
                        s("users_tasks"),
                        ForeignKey {
                            name: s("task_id_fk"),
                            table: Qi(s("api"),s("users_tasks")),
                            columns: vec![s("task_id")],
                            referenced_table: Qi(s("api"),s("tasks")),
                            referenced_columns: vec![s("id")],
                        },
                        ForeignKey {
                            name: s("user_id_fk"),
                            table: Qi(s("api"),s("users_tasks")),
                            columns: vec![s("user_id")],
                            referenced_table: Qi(s("api"),s("users")),
                            referenced_columns: vec![s("id")],
                        },
                    )
               
            )
        );

        // let result = get_join(&s("api"), &db_schema, &s("users"), &s("addresses"), &mut None);
        // let expected = AppError::AmbiguousRelBetween {
        //     origin: s("users"), target: s("addresses"),
        //     relations: vec![
        //         Parent(
        //             ForeignKey {
        //                 name: s("billing_address_id_fk"),
        //                 table: Qi(s("api"),s("users")),
        //                 columns: vec![
        //                     s("billing_address_id"),
        //                 ],
        //                 referenced_table: Qi(s("api"),s("addresses")),
        //                 referenced_columns: vec![
        //                     s("id"),
        //                 ],
        //             },
        //         ),
        //         Parent(
        //             ForeignKey {
        //                 name: s("shipping_address_id_fk"),
        //                 table: Qi(s("api"),s("users")),
        //                 columns: vec![
        //                     s("shipping_address_id"),
        //                 ],
        //                 referenced_table: Qi(s("api"),s("addresses")),
        //                 referenced_columns: vec![
        //                     s("id"),
        //                 ],
        //             },
        //         ),
        //     ]
        // };
        // assert!(result.is_err());
        // let error = result.unwrap();

        // assert!(matches!(
        //     get_join(&s("api"), &db_schema, &s("users"), &s("addresses"), &mut None),
        //     1
        // );
        assert!(matches!(
            get_join(&s("api"), &db_schema, &s("users"), &s("addresses"), &mut None),
            Err(AppError::AmbiguousRelBetween {..})
        ));

    }

    #[test]
    fn parse_filter() {
        assert_eq!(
            filter().easy_parse("gte.5"), 
            Ok((Filter::Op(s(">="),SingleVal(s("5"))),""))
        );
        assert_eq!(
            filter().easy_parse("in.(1,2,3)"), 
            Ok((Filter::In(ListVal(["1","2","3"].map(str::to_string).to_vec())),""))
        );
        assert_eq!(
            filter().easy_parse("fts.word"), 
            Ok((Filter::Fts(s("@@ to_tsquery"), None, SingleVal(s("word"))),""))
        );
    }

    #[test]
    fn parse_logic_condition() {
        let field = Field {name: s("id"), json_path: None};
        assert_eq!(
            logic_condition().easy_parse("id.gte.5"), 
            Ok((
                Single {filter: Filter::Op(s(">="),SingleVal(s("5"))), field: field.clone(), negate: false}
            ,""))
        );
        assert_eq!(
            logic_condition().easy_parse("id.not.in.(1,2,3)"), 
            Ok((
                Single {filter: Filter::In(ListVal(vec![s("1"),s("2"),s("3")])), field: field.clone(), negate: true}
            ,""))
        );
        assert_eq!(
            logic_condition().easy_parse("id.fts.word"), 
            Ok((
                Single {filter: Filter::Fts(s("@@ to_tsquery"), None, SingleVal(s("word"))), field: field.clone(), negate: false}
            ,""))
        );
        assert_eq!(
            logic_condition().easy_parse("not.or(id.gte.5, id.lte.10)"), 
            Ok((
                Condition::Group(
                    true,
                    ConditionTree {
                        operator: Or,
                        conditions: vec![
                            Single {filter: Filter::Op(s(">="),SingleVal(s("5"))), field: field.clone(), negate: false },
                            Single {filter: Filter::Op(s("<="),SingleVal(s("10"))), field: field.clone(), negate: false }
                        ]
                    }
                )
            ,""))
        );
        assert_eq!(
            logic_condition().easy_parse("not.or(id.gte.5, id.lte.10, and(id.gte.2, id.lte.4))"), 
            Ok((
                Condition::Group(
                    true,
                    ConditionTree {
                        operator: Or,
                        conditions: vec![
                            Single {filter: Filter::Op(s(">="),SingleVal(s("5"))), field: field.clone(), negate: false },
                            Single {filter: Filter::Op(s("<="),SingleVal(s("10"))), field: field.clone(), negate: false },
                            Condition::Group(
                                false,
                                ConditionTree {
                                    operator: And,
                                    conditions: vec![
                                        Single {filter: Filter::Op(s(">="),SingleVal(s("2"))), field: field.clone(), negate: false },
                                        Single {filter: Filter::Op(s("<="),SingleVal(s("4"))), field: field.clone(), negate: false }
                                    ]
                                }
                            )
                        ]
                    }
                )
            ,""))
        );
    }

    #[test]
    fn parse_operator() {
        assert_eq!(operator().easy_parse("gte."), Ok((s(">="),".")));
        assert_eq!(
            operator().easy_parse("gtv."),
            Err(Errors {
                position: PointerOffset::new("gtv.".as_ptr() as usize),
                errors: vec![Error::Message("unknown operator".into())]
            })
        );
    }

    #[test]
    fn parse_fts_operator() {
        assert_eq!(fts_operator().easy_parse("plfts."), Ok((s("@@ plainto_tsquery"),".")));
        assert_eq!(
            fts_operator().easy_parse("xfts."),
            Err(Errors {
                position: PointerOffset::new("xfts.".as_ptr() as usize),
                errors: vec![Error::Message("unknown fts operator".into())]
            })
        );
    }

    #[test]
    fn parse_single_value() {
        assert_eq!(single_value().easy_parse("any 123 value"), Ok((s("any 123 value"),"")));
        assert_eq!(single_value().easy_parse("any123value,another"), Ok((s("any123value,another"),"")));
    }

    #[test]
    fn parse_logic_single_value() {
        assert_eq!(logic_single_value().easy_parse("any 123 value"), Ok((s("any 123 value"),"")));
        assert_eq!(logic_single_value().easy_parse("any123value,another"), Ok((s("any123value"),",another")));
        assert_eq!(logic_single_value().easy_parse("\"any 123 value,)\""), Ok((s("any 123 value,)"),"")));
        assert_eq!(logic_single_value().easy_parse("{a, b, c}"), Ok((s("{a, b, c}"),"")));
    }

    #[test]
    fn parse_list_element() {
        assert_eq!(list_element().easy_parse("any 123 value"), Ok((s("any 123 value"),"")));
        assert_eq!(list_element().easy_parse("any123value,another"), Ok((s("any123value"),",another")));
        assert_eq!(list_element().easy_parse("any123value)"), Ok((s("any123value"),")")));
        assert_eq!(list_element().easy_parse("\"any123value,)\",another"), Ok((s("any123value,)"),",another")));
    }

    #[test]
    fn parse_list_value() {
        assert_eq!(list_value().easy_parse("(any 123 value)"), Ok((vec![s("any 123 value")],"")));
        assert_eq!(list_value().easy_parse("(any123value,another)"), Ok((vec![s("any123value"),s("another")],"")));
        assert_eq!(list_value().easy_parse("(\"any123 value\", another)"), Ok((vec![s("any123 value"),s("another")],"")));
        assert_eq!(list_value().easy_parse("(\"any123 value\", 123)"), Ok((vec![s("any123 value"),s("123")],"")));
    }

    #[test]
    fn parse_alias_separator(){
        assert_eq!(alias_separator().easy_parse(":abc"), Ok((':',"abc")));
        assert_eq!(alias_separator().easy_parse("::abc").is_err(), true);
    }

    #[test]
    fn parse_json_path() {
        assert_eq!(
            json_path().easy_parse("->key"), 
            Ok((vec![JArrow(JKey(s("key")))],""))
        );

        assert_eq!(
            json_path().easy_parse("->>51"), 
            Ok((vec![J2Arrow(JIdx(s("51")))],""))
        );

        assert_eq!(
            json_path().easy_parse("->key1->>key2"), 
            Ok((vec![JArrow(JKey(s("key1"))), J2Arrow(JKey(s("key2")))],""))
        );

        assert_eq!(
            json_path().easy_parse("->key1->>key2,rest"), 
            Ok((vec![JArrow(JKey(s("key1"))), J2Arrow(JKey(s("key2")))],",rest"))
        );

    }

    #[test]
    fn parse_field_name() {
        assert_eq!(field_name().easy_parse("field rest"), Ok((s("field"),"rest")));
        assert_eq!(field_name().easy_parse("field12"), Ok((s("field12"),"")));
        assert_ne!(field_name().easy_parse("field,invalid"), Ok((s("field,invalid"),"")));
        assert_eq!(field_name().easy_parse("field-name"), Ok((s("field-name"),"")));
        assert_eq!(field_name().easy_parse("field-name->"), Ok((s("field-name"),"->")));
        assert_eq!(quoted_value().easy_parse("\"field name\""), Ok((s("field name"),"")));
    }

    #[test]
    fn parse_columns() {
        assert_eq!(columns().easy_parse("col1, col2 "), Ok((vec![s("col1"), s("col2")],"")));
        
        assert_eq!(columns().easy_parse(position::Stream::new("id,# name")), Err(Errors {
            position: SourcePosition { line: 1, column: 4 },
            errors: vec![
                Error::Unexpected('#'.into()),
                Error::Expected("whitespace".into()),
                Error::Expected("field name (* or [a..z0..9_])".into())
            ]
        }));

        assert_eq!(columns().easy_parse(position::Stream::new("col1, col2, ")), Err(Errors {
            position: SourcePosition { line: 1, column: 13 },
            errors: vec![
                Error::Unexpected("end of input".into()),
                Error::Expected("whitespace".into()),
                Error::Expected("field name (* or [a..z0..9_])".into())
            ]
        }));

        assert_eq!(columns().easy_parse(position::Stream::new("col1, col2 col3")), Err(Errors {
            position: SourcePosition { line: 1, column: 12 },
            errors: vec![
                Error::Unexpected('c'.into()),
                Error::Expected(','.into()),
                Error::Expected("whitespaces".into()),
                Error::Expected("end of input".into())
            ]
        }));
    }

    #[test]
    fn parse_field() {
        let result = Field {
            name: s("field"),
            json_path: None
        };
        assert_eq!(field().easy_parse("field"), Ok((result,"")));
        let result = Field {
            name: s("field"),
            json_path: Some(vec![JArrow(JKey(s("key")))])
        };
        assert_eq!(field().easy_parse("field->key"), Ok((result,"")));
    }

    #[test]
    fn parse_tree_path() {
        let result = (
            vec![s("sub"), s("path")],
            Field {
                name: s("field"),
                json_path: Some(vec![JArrow(JKey(s("key")))])
            }
        );
        assert_eq!(tree_path().easy_parse("sub.path.field->key"), Ok((result,"")));
    }

    #[test]
    fn parse_logic_tree_path() {
        assert_eq!(logic_tree_path().easy_parse("and"), Ok(((vec![], false, And),"")));
        assert_eq!(logic_tree_path().easy_parse("not.or"), Ok(((vec![], true, Or),"")));
        assert_eq!(logic_tree_path().easy_parse("sub.path.and"), Ok(((vec![s("sub"), s("path")], false, And),"")));
        assert_eq!(logic_tree_path().easy_parse("sub.path.not.or"), Ok(((vec![s("sub"), s("path")], true, Or),"")));
    }


    #[test]
    fn parse_select_item(){
        assert_eq!(
            select_item().easy_parse("alias:column"), 
            Ok((
                Simple {field: Field {name:s("column"), json_path: None}, alias:  Some(s("alias"))}
                ,""
            ))
        );

        assert_eq!(
            select_item().easy_parse("column"), 
            Ok((
                Simple {field: Field {name:s("column"), json_path: None}, alias:  None}
                ,""
            ))
        );

        assert_eq!(
            select_item().easy_parse("table!hint ( column0->key, column1 ,  alias2:column2 )"), 
            Ok((
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name:s("column0"), json_path: Some(vec![JArrow(JKey(s("key")))])}, alias:  None},
                            Simple {field: Field {name:s("column1"), json_path: None}, alias:  None},
                            Simple {field: Field {name:s("column2"), json_path: None}, alias:  Some(s("alias2"))},
                        ],
                        from: s("table"),
                        //from_alias: None,
                        where_: ConditionTree { operator: And, conditions: vec![]}
                    },
                    alias: None,
                    hint: Some(s("hint")),
                    join: None
                }
                ,""
            ))
        );
    }

}
