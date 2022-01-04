

use std::collections::{HashMap,HashSet,BTreeSet};
use std::iter::FromIterator;
use crate::api::{
    *,
    LogicOperator::*,Join::*, Condition::*, Filter::*, Query::*,
    ResponseContentType::*,
};
use crate::schema::{*, ObjectType::*, ProcReturnType::*,PgType::*};
use crate::error::*;
use snafu::{OptionExt, ResultExt};
use serde_json::{Value as JsonValue};
use combine::{
    error::{StreamError},
    easy::{ParseError,Error as ParserError, Info},
    stream::StreamErrorFor,
    Parser, Stream, EasyParser,
    parser::{
        char::{char, digit, letter, spaces, string},
        choice::{choice, optional},
        repeat::{many1, sep_by, sep_by1},
        sequence::{between},
        token::{one_of, none_of, any, eof,},
        repeat::{many},
        combinator::{attempt,not_followed_by,},
    },
};


lazy_static!{
    //static ref STAR: String = "*".to_string();
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

    static ref OPERATORS_START: Vec<String> = {
        OPERATORS.keys().chain(["not","in"].iter()).chain(FTS_OPERATORS.keys()).map(|&op| format!("{}.", op) )
        .chain(FTS_OPERATORS.keys().map(|&op| format!("{}(", op) ))
        .collect()
    };
}

pub fn parse<'r>(
    schema: &String, 
    root: &String, 
    db_schema: &DbSchema, 
    method: &Method, 
    parameters: &Vec<(&str, &str)>, 
    body: Option<&'r String>, 
    headers: &'r HashMap<&'r str, &'r str>,
    cookies: &'r HashMap<&'r str, &'r str>,
) -> Result<ApiRequest<'r>> {

    let schema_obj = db_schema.schemas.get(schema).context(UnacceptableSchema {schemas: vec![schema.to_owned()]})?;
    // println!("--------------looking for {} {}->{}.{:?}", current_schema, origin, target, hint);
    // println!("--------------got schema");
    let root_obj = schema_obj.objects.get(root).context(NotFound)?;

    //println!("root_obj {:#?}", root_obj);
    let mut select_items = vec![SelectItem::Star];
    let mut limits = vec![];
    let mut offsets = vec![];
    let mut orders = vec![];
    let mut conditions = vec![];
    let mut columns_ = None;
    let mut fn_arguments = vec![];

    let accept_content_type = match headers.get("Accept") {
        Some(accept_header) => {
            let (act, _) = content_type()
            .message("failed to parse accept header")
            .easy_parse(*accept_header)
            .map_err(to_app_error(accept_header))?;
            Ok(act)
        }
        None => Ok(ApplicationJSON)
    }?;
    
    

    // iterate over parameters, parse and collect them in the relevant vectors
    for &(k,v) in parameters.iter() {
        match k {
            "select" => {
                let (parsed_value, _) = select()
                    .message("failed to parse select parameter")
                    .easy_parse(v).map_err(to_app_error(v))?;
                select_items = parsed_value;
            }

            "columns" => {
                let (parsed_value, _) = columns()
                    .message("failed to parse columns parameter")
                    .easy_parse(v).map_err(to_app_error(v))?;
                columns_ = Some(parsed_value);
            }

            kk if is_logical(kk) => {
                let ((tp, n, lo), _) = logic_tree_path()
                    .message("failed to parser logic tree path")
                    .easy_parse(k).map_err(to_app_error(k))?;
                let ns = if n { "not." } else { "" };
                let los = if lo == And {  "and" } else { "or" };
                let s = format!("{}{}{}", ns,los,v);
                let (c, _) = logic_condition()
                    .message("failed to parse logic tree")
                    .easy_parse(s.as_str()).map_err(to_app_error(&s))?;
                conditions.push((tp, c));
            }

            kk if is_limit(kk) => {
                let ((tp,_), _)= tree_path()
                    .message("failed to parser limit tree path")
                    .easy_parse(k).map_err(to_app_error(k))?;
                let (parsed_value,_) = limit()
                    .message("failed to parse limit parameter")
                    .easy_parse(v).map_err(to_app_error(v))?;
                limits.push((tp, parsed_value));
            }

            kk if is_offset(kk) => {
                let ((tp,_), _)= tree_path()
                    .message("failed to parser offset tree path")
                    .easy_parse(k).map_err(to_app_error(k))?;
                let (parsed_value,_) = offset()
                    .message("failed to parse limit parameter")
                    .easy_parse(v).map_err(to_app_error(v))?;
                offsets.push((tp, parsed_value));
            }

            kk if is_order(kk) => {
                let ((tp,_), _)= tree_path()
                    .message("failed to parser order tree path")
                    .easy_parse(k).map_err(to_app_error(k))?;
                let (parsed_value,_) = order()
                    .message("failed to parse order")
                    .easy_parse(v).map_err(to_app_error(v))?;
                orders.push((tp, parsed_value));
            }

            //is filter or function parameter
            _ => {
                let ((tp,field), _)= tree_path()
                    .message("failed to parser filter tree path")
                    .easy_parse(k).map_err(to_app_error(k))?;

                match root_obj.kind {
                    Function {..} => {
                        if tp.len() > 0 || has_operator(v) {
                            // this is a filter
                            let ((negate,filter), _) = negatable_filter()
                                .message("failed to parse filter")
                                .easy_parse(v).map_err(to_app_error(v))?;
                            conditions.push((tp, Condition::Single {field, filter, negate}));
                        }
                        else {
                            //this is a function parameter
                            fn_arguments.push((k,v));
                        }
                    }
                    _ => {
                        let ((negate,filter), _) = negatable_filter()
                        .message("failed to parse filter")
                        .easy_parse(v).map_err(to_app_error(v))?;
                        conditions.push((tp, Condition::Single {field, filter, negate}));

                    }
                };
            }
        }
    }

    let mut query = match (method, root_obj.kind.clone()) {
        (method, Function {return_type, parameters, ..}) => {
            let arguments = match *method {
                Method::GET => {

                    let a:HashSet<&str> = HashSet::from_iter(fn_arguments.iter().map(|(k,_)|*k));
                    let p:HashSet<&str> = HashSet::from_iter(parameters.iter().map(|p| p.name.as_str()));
                    // check if all parameters are supplied
                    if a != p {
                        return Err(
                            Error::NoRpc {
                                schema: schema.clone(),
                                proc_name: root.clone(),
                                argument_keys: fn_arguments.iter().map(|(k,_)| format!("{}",k)).collect(),
                                has_prefer_single_object: false,
                                content_type: accept_content_type,
                                is_inv_post: false
                            }
                        )
                    }

                    let mut args:HashMap<String, JsonValue> = HashMap::new();
                    for p in &parameters {
                        let v:Vec<&str> = fn_arguments.iter().zip(std::iter::repeat(p.name.clone())).filter_map(|(&(k,v),n)| if k == n { Some(v) } else {None} ).collect();
                        let vv = match (p.variadic, v.len()) {
                            (_,0) => None,
                            (true,_) => Some(JsonValue::Array(
                                v.iter()
                                .map(|s| JsonValue::String(s.to_string()))
                                .collect()
                            )),
                            (false,1) => Some(JsonValue::String(v[0].to_string())),
                            (false,_) => None,
                        };
                        if vv.is_some() {
                            args.insert(p.name.clone(), vv.unwrap());
                        }
                    }
                    Ok(args)
                },
                Method::POST => {
                    let payload = body.context(InvalidBody {message: "body not available".to_string()})?;
                    let json_payload = serde_json::from_str(payload).context(JsonDeserialize)?;
                    Ok(json_payload)
                },
                _ => Err(Error::UnsupportedVerb)
            }?;

            let params = match (parameters.len(), parameters.get(0)) {
                (1, Some(p)) if p.name == "" => CallParams::OnePosParam(p.clone()),
                _ => {
                    let specified_parameters = arguments.keys().collect::<Vec<_>>();
                    CallParams::KeyParams(
                        parameters.into_iter().filter(|p| specified_parameters.contains(&&p.name) ).collect::<Vec<_>>()
                    )
                },
            };
            
            //TODO!!! check all the required arguments are available

            let payload: String = serde_json::to_string(&arguments).context(JsonSerialize)?;
            
            let mut q = FunctionCall {
                fn_name: Qi(schema.clone(), root.clone()),
                parameters: params,
                
                //CallParams::KeyParams(vec![]),
                payload: Some(payload),
                
                is_scalar: match return_type {
                    One(Scalar) => true,
                    SetOf(Scalar) => true,
                    _ => false,
                },
                returns_single: match return_type {
                    One(_) => true,
                    SetOf(_) => false,
                },
                is_multiple_call: false,

                returning: vec![],//get_returning(&select_items)?,
                select: select_items,
                where_: ConditionTree { operator: And, conditions: vec![] },
                return_table_type: match return_type {
                    SetOf(Composite(q)) => Some(q),
                    One(Composite(q)) => Some(q),
                    _ => None,
                },
            };
            add_join_conditions(&mut q, &schema, db_schema)?;
            
            //we populate the returing becasue it relies on the "join" information
            if let FunctionCall { ref mut returning, ref select, ..} = q {
                returning.extend(get_returning(select)?);
            }
            Ok(q)
        },
        (&Method::GET, _) => {
            let mut q = Select {
                select: select_items,
                from: vec![root.clone()],
                where_: ConditionTree { operator: And, conditions: vec![] },
                limit: None, offset: None, order: vec![],
            };
            add_join_conditions(&mut q, &schema, db_schema)?;
            Ok(q)
        },
        (&Method::POST,_) => {
            let payload = body.context(InvalidBody {message: "body not available".to_string()})?;
            let columns = match columns_ {
                Some(c) => Ok(c),
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
                                                Err(Error::InvalidBody {message: format!("All object keys must match")})
                                            }
                                            
                                        },
                                        _ => Ok(vec![])
                                    }
                                },
                                _ => Ok(vec![])
                            }
                        },
                        Err(e) => {
                            Err(Error::InvalidBody {message: format!("Failed to parse json body {}", e)})
                        }
                    }
                }
            }?;

            let mut q = Insert {
                into: root.clone(),
                columns: columns,
                payload,
                where_: ConditionTree { operator: And, conditions: vec![] },
                returning: vec![], //get_returning(&select_items)?,
                select: select_items,
                //, onConflict :: Maybe (PreferResolution, [FieldName])
            };
            add_join_conditions(&mut q, &schema, db_schema)?;
            //we populate the returing becasue it relies on the "join" information
            if let Insert { ref mut returning, ref select, ..} = q {
                returning.extend(get_returning(select)?);
            }
            Ok(q)
        },
        // Method::PATCH => Ok(Update),
        // Method::PUT => Ok(Upsert),
        // Method::DELETE => Ok(Delete),
        _ => Err(Error::UnsupportedVerb)
    }?;

    //add_join_conditions(&mut query, &schema, db_schema)?;
    insert_conditions(&mut query, conditions);
    //insert_limits(&mut query, limits);
    //insert_offsets(&mut query, offsets);
    //insert_orders(&mut query, orders);

    // insert_properties(&mut query, conditions, |q, p|{
    //     let query_conditions: &mut Vec<Condition> = match q {
    //         Select {where_, ..} => where_.conditions.as_mut(),
    //         Insert {where_, ..} => where_.conditions.as_mut(),
    //     };
    //     p.into_iter().for_each(|c| query_conditions.push(c));
    // });

    insert_properties(&mut query, limits, |q, p|{
        let limit = match q {
            Select {limit, ..} => limit,
            Insert {..} => todo!(),
            FunctionCall { .. } => todo!(),
        };
        for v in p {
            std::mem::swap(limit, &mut Some(v));
        }
    });

    insert_properties(&mut query, offsets, |q, p|{
        let offset = match q {
            Select {offset, ..} => offset,
            Insert {..} => todo!(),
            FunctionCall { .. } => todo!(),
        };
        for v in p {
            std::mem::swap(offset, &mut Some(v));
        }
    });

    insert_properties(&mut query, orders, |q, p|{
        let order = match q {
            Select {order, ..} => order,
            Insert {..} => todo!(),
            FunctionCall { .. } => todo!(),
        };
        for mut o in p {
            std::mem::swap(order, &mut o);
        }
    });
    

    Ok(ApiRequest {
        method: method.clone(),
        query,
        accept_content_type,
        headers,
        cookies,
    })
}

// parser functions
fn lex<Input, P>(p: P) -> impl Parser<Input, Output = P::Output>
where P: Parser<Input>, Input: Stream<Token = char>,
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
            many1::<String, _, _>(choice((letter(),digit(),one_of("_ ".chars())))).map(|s| s.trim().to_owned()),
            dash
        ).map(|words: Vec<String>| words.join("-"))
    )))
    
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
                    x => panic!("unknown logic operator {}", x)
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
        let star = char('*').map(|_| SelectItem::Star);
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
                    from: vec![from],
                    //from_alias: alias,
                    where_: ConditionTree { operator: And, conditions: vec![]},
                    limit: None, offset: None, order: vec![],
                },
                alias: alias,
                hint: join_hint,
                join: None
            }
        );

        attempt(sub_select).or(column).or(star)
    }
}

fn single_value<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    many(any())
}

fn integer<Input>() -> impl Parser<Input, Output = SingleVal>
where Input: Stream<Token = char>
{
    many1(digit()).map(|v| SingleVal(v))
}

fn limit<Input>() -> impl Parser<Input, Output = SingleVal>
where Input: Stream<Token = char>
{
    integer()
}

fn offset<Input>() -> impl Parser<Input, Output = SingleVal>
where Input: Stream<Token = char>
{
    integer()
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
            sep_by(list_element(), lex(char(',')))
        )
    )
}

fn list_element<Input>() -> impl Parser<Input, Output = String>
where Input: Stream<Token = char>
{
    attempt(quoted_value().skip(not_followed_by(none_of(",)".chars())))).or(many1(none_of(",)".chars())))
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
        attempt(operator().skip(dot()).and(single_value()).map(|(o,v)|
            match &*o {
                "like" | "ilike" => {
                    Ok(Filter::Op(o, SingleVal(v.replace("*","%"))))
                }
                "is" => {
                    match &*v {
                        "null" => Ok(Filter::Is(TrileanVal::TriNull)),
                        "unknown" => Ok(Filter::Is(TrileanVal::TriUnknown)),
                        "true" => Ok(Filter::Is(TrileanVal::TriTrue)),
                        "false" => Ok(Filter::Is(TrileanVal::TriFalse)),
                        _ => Err(StreamErrorFor::<Input>::message_static_message("unknown value for is operator, use null, unknown, true, false"))
                    }
                }
                _ => {
                    Ok(Filter::Op(o, SingleVal(v)))
                }
            }
            
        )),
        attempt(string("in").skip(dot()).and(list_value()).map(|(_,v)| Ok(Filter::In(ListVal(v))))),
        fts_operator()
            .and(optional(
                between(
                    char('('),
                    char(')'),
                    many1(choice(
                        (letter(),digit(),char('_'))
                    ))
                ).map(|v| SingleVal(v))
            ))
            .skip(dot())
            .and(single_value())
            .map(|((o,l),v)| Ok(Filter::Fts (o,l,SingleVal(v)))),

    )).and_then(|r| r)
}

fn logic_filter<Input>() -> impl Parser<Input, Output = Filter>
where Input: Stream<Token = char>
{
    //let value = if use_logical_value { opaque!(logic_single_value()) } else { opaque!(single_value()) };

    choice((
        attempt(operator().skip(dot()).and(logic_single_value()).map(|(o,v)|
            match &*o {
                "like" | "ilike" => {
                    Ok(Filter::Op(o, SingleVal(v.replace("*","%"))))
                }
                "is" => {
                    match &*v {
                        "null" => Ok(Filter::Is(TrileanVal::TriNull)),
                        "unknown" => Ok(Filter::Is(TrileanVal::TriUnknown)),
                        "true" => Ok(Filter::Is(TrileanVal::TriTrue)),
                        "false" => Ok(Filter::Is(TrileanVal::TriFalse)),
                        _ => Err(StreamErrorFor::<Input>::message_static_message("unknown value for is operator, use null, unknown, true, false"))
                    }
                }
                _ => {
                    Ok(Filter::Op(o, SingleVal(v)))
                }
            }
            
        )),
        attempt(string("in").skip(dot()).and(list_value()).map(|(_,v)| Ok(Filter::In(ListVal(v))))),
        fts_operator()
            .and(optional(
                between(
                    char('('),
                    char(')'),
                    many1(choice(
                        (letter(),digit(),char('_'))
                    ))
                ).map(|v| SingleVal(v))
            ))
            .skip(dot())
            .and(logic_single_value())
            .map(|((o,l),v)| Ok(Filter::Fts (o,l,SingleVal(v)))),
    )).and_then(|v| v)
}

fn order<Input>() -> impl Parser<Input, Output = Vec<OrderTerm>>
where Input: Stream<Token = char>
{
    sep_by1(order_term(), lex(char(','))).skip(eof())
}

fn order_term<Input>() -> impl Parser<Input, Output = OrderTerm>
where Input: Stream<Token = char>
{
    let direction = attempt(dot().and(string("asc").map(|_| OrderDirection::Asc).or(string("desc").map(|_| OrderDirection::Desc))).map(|(_,v)| v));
    let nulls = dot().and(
        attempt(string("nullsfirst").map(|_| OrderNulls::NullsFirst)).or(string("nullslast").map(|_| OrderNulls::NullsLast))
    ).map(|(_,v)| v);
    field().and(optional(direction).and(optional(nulls))).map(|(term, (direction, null_order))| OrderTerm{term, direction, null_order})
}

fn content_type<Input>() -> impl Parser<Input, Output = ResponseContentType>
where Input: Stream<Token = char>
{
    choice((
    string("*/*").map(|_| ApplicationJSON),
    string("application/json").map(|_| ApplicationJSON),
    string("application/vnd.pgrst.object").map(|_| SingularJSON),
    string("application/vnd.pgrst.object+json").map(|_| SingularJSON),
    string("text/csv").map(|_| TextCSV),
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
                        x => panic!("unknown logic operator {}", x)
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

// helper functions

fn get_join(current_schema: &String, db_schema: &DbSchema, origin: &String, target: &String, hint: &mut Option<String>) -> Result<Join>{
    // let match_fk = | s: &String, t:&String, n:&String | {
    //     | fk: &&ForeignKey |{
    //         &fk.name == n && &fk.referenced_table.0 == s
    //     }
    // };
    let schema = db_schema.schemas.get(current_schema).context(UnacceptableSchema {schemas: vec![current_schema.to_owned()]})?;
    //println!("--------------looking for {} {}->{}.{:?}", current_schema, origin, target, hint);
    // println!("--------------got schema");
    let origin_table = schema.objects.get(origin).context(UnknownRelation {relation: origin.to_owned()})?;
    // println!("--------------got table");
    match schema.objects.get(target) {
        // the target is an existing table
        Some(target_table) => {
            // println!("--------------got target table");
            match hint {
                Some(h) => {
                    // projects?select=clients!projects_client_id_fkey(*)
                    if let Some(fk) = origin_table.foreign_keys.iter().find(|&fk|
                        &fk.name == h && &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target
                    ) {
                        return Ok(Parent(fk.clone()));
                    }
                    if let Some(fk) = target_table.foreign_keys.iter().find(|&fk|
                        &fk.name == h && &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin
                    ) {
                        return Ok(Child(fk.clone()));
                    }

                    // users?select=tasks!users_tasks(*)
                    // TODO!!! handle
                    if let Some(join_table) = schema.objects.get(h) {
                        let ofk1 = join_table.foreign_keys.iter().find_map(|fk| {
                            if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin {
                                Some(fk)
                            }
                            else { None }
                        });
                        let ofk2 = join_table.foreign_keys.iter().find_map(|fk| {
                            if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target {
                                Some(fk)
                            }
                            else { None }
                        });
                        if let (Some(fk1), Some(fk2)) = (ofk1, ofk2){
                            return Ok( Many(Qi(current_schema.clone(),join_table.name.clone()), fk1.clone(), fk2.clone()) )
                        }
                        else {
                            return Err(Error::NoRelBetween {origin: origin.to_owned(), target: target.to_owned()})
                        }
                        
                    }

                    // projects?select=clients!client_id(*)
                    // projects?select=clients!id(*)
                    let mut joins = vec![];
                    
                    joins.extend(origin_table.foreign_keys.iter()
                        .filter(|&fk|
                               &fk.referenced_table.0 == current_schema 
                            && &fk.referenced_table.1 == target
                            && fk.columns.len() == 1
                            && ( fk.columns.contains(h) || fk.referenced_columns.contains(h) )
                        )
                        .map(|fk| Parent(fk.clone()))
                        .collect::<Vec<_>>()
                    );
                    joins.extend(target_table.foreign_keys.iter()
                    .filter(|&fk|
                           &fk.referenced_table.0 == current_schema 
                        && &fk.referenced_table.1 == origin
                        && fk.columns.len() == 1
                        && ( fk.columns.contains(h) || fk.referenced_columns.contains(h) )
                    )
                    .map(|fk| Parent(fk.clone()))
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
                    .filter(|&fk| &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin )
                    .map(|fk| Child(fk.clone()))
                    .collect::<Vec<_>>();
                    // println!("target tbl fks: {:#?}", target_table);
                    
                    // check parent relations
                    // projects?select=clients(*)
                    let parent_joins = origin_table.foreign_keys.iter()
                    .filter(|&fk| &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target )
                    .map(|fk| Parent(fk.clone()))
                    .collect::<Vec<_>>();

                    let mut joins = vec![];
                    joins.extend(child_joins);
                    joins.extend(parent_joins);
                    
                    if joins.len() == 1 {
                        // println!("--------------found 1 {:?}", joins[0].clone());
                        Ok(joins[0].clone())
                    }
                    else if joins.len() == 0 {
                        // check many to many relations
                        // users?select=tasks(*)
                        let many_joins = schema.objects.values().filter_map(|join_table|{
                            let fk1 = join_table.foreign_keys.iter().find_map(|fk| {
                                if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin {
                                    Some(fk)
                                }
                                else { None }
                            })?;
                            let fk2 = join_table.foreign_keys.iter().find_map(|fk| {
                                if &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target {
                                    Some(fk)
                                }
                                else { None }
                            })?;
                            Some( Many(Qi(current_schema.clone(),join_table.name.clone()), fk1.clone(), fk2.clone()) )
                        }).collect::<Vec<_>>();
                        if many_joins.len() == 1 {
                            // println!("--------------found many join");
                            Ok(many_joins[0].clone())
                        }
                        else if many_joins.len() == 0 {
                            // println!("--------------nothing found");
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
            // println!("--------------no target table");
            match origin_table.foreign_keys.iter().find(|&fk| &fk.name == target && &fk.referenced_table.0 == current_schema) {
                // the target is a foreign key name
                // projects?select=projects_client_id_fkey(*)
                // TODO! when views are involved there may be multiple fks with the same name
                Some (fk) => Ok(Child(fk.clone())),
                // the target is a foreign key column
                // projects?select=client_id(*)
                None => {
                    let joins = origin_table.foreign_keys.iter()
                        .filter(|&fk| &fk.referenced_table.0 == current_schema && fk.columns.len() == 1 && fk.columns.contains(target) )
                        .map(|fk| Child(fk.clone()))
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
    let dummy_source = &"subzero_source".to_string();
    let (select, parent_table, parent_alias) : (&mut Vec<SelectItem>, &String, &String) = match query {
        Select {select, from, ..} => (select.as_mut(), from.get(0).unwrap(), from.get(0).unwrap()),
        Insert {select, into, ..} => (select.as_mut(), into, subzero_source),
        FunctionCall { select, return_table_type, .. } => {
            let table = match return_table_type {
                Some(q) => &q.1,
                None => dummy_source,
            };
            (select.as_mut(), table, subzero_source)
        },
    };
    
    for s in select.iter_mut() {
        match s {
            SelectItem::SubSelect{query: q, join, hint, ..} => {
                let from = match q {
                    Select {from, ..} => from,
                    _ => panic!("there should not be any Insert queries as subselects"),
                };
                let child_table = from.get(0).unwrap();
                let new_join = get_join(schema, db_schema, parent_table, child_table, hint)?;
                match &new_join {
                    Parent (fk) => {
                        let mut conditions = vec![];
                        for i in 0..fk.columns.len() {
                            conditions.push((
                                vec![],
                                Single { //tasks
                                    field: Field {name: fk.referenced_columns[i].clone(), json_path: None},
                                    filter: Col (Qi (schema.clone(), parent_alias.clone()), Field {name: fk.columns[i].clone(), json_path: None}),
                                    negate: false
                                }
                            ));
                        }
                        insert_conditions(q, conditions);
                    },
                    Child (fk) => {
                        let mut conditions = vec![];
                        for i in 0..fk.columns.len() {
                            conditions.push((
                                vec![],
                                Single { //tasks
                                    field: Field {name: fk.columns[i].clone(), json_path: None},
                                    filter: Col (Qi (schema.clone(), parent_alias.clone()), Field {name: fk.referenced_columns[i].clone(), json_path: None}),
                                    negate: false
                                }
                            ));
                        }
                        insert_conditions(q, conditions);
                    },
                    //TODO!!! insert many to many conditions
                    Many (join_table, fk1, fk2) => {
                        let mut conditions = vec![];
                        //fk1 is for origin table
                        for i in 0..fk1.columns.len() {
                            conditions.push((
                                vec![],
                                Foreign {
                                    left: (Qi (schema.clone(), parent_alias.clone()), Field {name: fk1.referenced_columns[i].clone(), json_path: None}),
                                    right: (Qi (join_table.0.clone(), join_table.1.clone()), Field {name: fk1.columns[i].clone(), json_path: None})
                                }
                            ));
                        }
                        //fk2 is for target table
                        for i in 0..fk2.columns.len() {
                            conditions.push((
                                vec![],
                                Single {
                                    field: Field {name: fk2.referenced_columns[i].clone(), json_path: None},
                                    filter: Col (Qi (join_table.0.clone(), join_table.1.clone()), Field {name: fk2.columns[i].clone(), json_path: None}),
                                    negate: false
                                }
                            ));
                        }
                        from.push(join_table.1.clone());
                        insert_conditions(q, conditions);
                        
                    }
                }
                std::mem::swap(join, &mut Some(new_join));
                add_join_conditions( q, schema, db_schema)?
            }
            _ => {}
        }
    }
    Ok(())
}

fn insert_properties<T>(query: &mut Query, mut properties: Vec<(Vec<String>,T)>, f: fn(&mut Query, Vec<T>),  ) {
    let node_properties = properties.drain_filter(|(path, _)| path.len() == 0).map(|(_,c)| c).collect::<Vec<_>>();
    if node_properties.len() > 0 {
         f(query, node_properties) 
    };
    

    let select = match query {
        Select {select,..} => select,
        Insert {select,..} => select,
        FunctionCall {select, .. } => select,
    };

    for s in select.iter_mut() {
        match s {
            SelectItem::SubSelect{query: q, alias, ..} => {
                let from : &String = match q {
                    Select {from, ..} => from.get(0).unwrap(),
                    _ => panic!("there should not be any Insert queries as subselects"),
                };
                let node_properties = properties.drain_filter(|(path, _)|
                    match path.get(0) {
                        Some(p) => {
                            if p == from || Some(p) == alias.as_ref()  { path.remove(0); true }
                            else {false}
                        }
                        None => false
                    }
                ).collect::<Vec<_>>();
                insert_properties(q, node_properties, f);
            }
            _ => {}
        }
    }
}

fn insert_conditions( query: &mut Query, conditions: Vec<(Vec<String>,Condition)>){
    insert_properties(query, conditions, |q, p|{
        let query_conditions: &mut Vec<Condition> = match q {
            Select {where_, ..} => where_.conditions.as_mut(),
            Insert {where_, ..} => where_.conditions.as_mut(),
            FunctionCall {where_, .. } => where_.conditions.as_mut(),
        };
        p.into_iter().for_each(|c| query_conditions.push(c));
    });
}

fn is_logical(s: &str)->bool{ s == "and" || s == "or" || s.ends_with(".or") || s.ends_with(".and") }

fn is_limit(s: &str)->bool{ s == "limit" || s.ends_with(".limit") }

fn is_offset(s: &str)->bool{ s == "offset" || s.ends_with(".offset") }

fn is_order(s: &str)->bool{ s == "order" || s.ends_with(".order") }

fn has_operator(s: &str)->bool {
    OPERATORS_START.iter().map(|op| s.starts_with(op) )
    .any(|b| b)
}

fn to_app_error<'a>(s: &'a str) -> impl Fn(ParseError<&'a str>) -> Error {
    move |mut e| {
        let m = e.errors.drain_filter(|v| 
            match v {
                ParserError::Message(_) => true,
                _ => false
            }
        ).collect::<Vec<_>>();
        let position = e.position.translate_position(s);
        let message = match m.as_slice() {
            [ParserError::Message(Info::Static(s))] => s,
            _ => ""
        };
        let message = format!("\"{} ({})\" (line 1, column {})", message, s, position + 1);
        let details = format!("{}", e)
            .replace(format!("Parse error at {}", e.position).as_str(), "")
            .replace("\n", " ")
            .trim()
            .to_string();
        Error::ParseRequestError {message, details}
    }
}

fn get_returning(select: &Vec<SelectItem>) -> Result<Vec<String>> {
    Ok(select.iter().map(|s|{
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
            SelectItem::Star => Ok(vec![]),
            //TODO!! error here is wrong
            x => Err(Error::NoRelBetween {origin: "table".to_string(), target: format!("x {:?}",x)}) 
            
        }
    })
    .collect::<Result<Vec<_>, _>>()?
    .into_iter().flatten().cloned().collect::<BTreeSet<_>>().into_iter().collect())
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
                                        "kind":"function",
                                        "name":"myfunction",
                                        "volatile":"v",
                                        "composite":false,
                                        "setof":false,
                                        "return_type":"int4",
                                        "return_type_schema":"pg_catalog",
                                        "parameters":[
                                            {
                                                "name":"id",
                                                "type":"integer",
                                                "required":true,
                                                "variadic":false
                                            }
                                        ]
                                    },
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
    fn test_parse_get_function(){
        let emtpy_hashmap = HashMap::new();
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        let mut api_request = ApiRequest {
            method: Method::GET,
            headers: &emtpy_hashmap,
            accept_content_type: ApplicationJSON,
            cookies: &emtpy_hashmap,
            query: FunctionCall {
                fn_name: Qi(s("api"), s("myfunction")),
                parameters: CallParams::KeyParams(vec![
                    ProcParam {
                        name: s("id"),
                        type_: s("integer"),
                        required: true,
                        variadic: false,
                    }
                ]),
                payload: Some(s(r#"{"id":"10"}"#)),
                is_scalar: true,
                returns_single: true,
                is_multiple_call: false,
                returning: vec![],
                select: vec![Star],
                where_: ConditionTree { operator: And, conditions: vec![] },
                return_table_type: None,
            }
            
        };
        let a = parse(&s("api"), &s("myfunction"), &db_schema, &Method::GET, &vec![
            ("id","10"),
            ], None, &emtpy_hashmap, &emtpy_hashmap);

        assert_eq!(a.unwrap(),api_request);

        api_request.method = Method::POST;

        let body = s(r#"{"id":"10"}"#);
        let b = parse(&s("api"), &s("myfunction"), &db_schema, &Method::POST, &vec![], Some(&body), &emtpy_hashmap, &emtpy_hashmap);
        assert_eq!(b.unwrap(),api_request);
    }

    #[test]
    fn test_insert_conditions(){
       
        let mut query = Select { order: vec![], limit: None, offset: None,
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                SubSelect{
                    query: Select { order: vec![], limit: None, offset: None,
                        select: vec![
                            Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                        ],
                        from: vec![s("child")],
                        where_: ConditionTree { operator: And, conditions: vec![]}
                    },
                    alias: None,
                    hint: None,
                    join: None
                }
            ],
            from: vec![s("parent")],
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
            Select { order: vec![], limit: None, offset: None,
                select: vec![
                    Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                    SubSelect{
                        query: Select { order: vec![], limit: None, offset: None,
                            select: vec![
                                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                            ],
                            from: vec![s("child")],
                            //from_alias: None,
                            where_: ConditionTree { operator: And, conditions: vec![condition.clone()] }
                        },
                        alias: None,
                        hint: None,
                        join: None
                    }
                ],
                from: vec![s("parent")],
                where_: ConditionTree { operator: And, conditions: vec![condition.clone()] }
            }
        );
    }

    #[test]
    fn test_parse_get(){
        let emtpy_hashmap = HashMap::new();
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
       
        let a = parse(&s("api"), &s("projects"), &db_schema, &Method::GET, &vec![
            ("select", "id,name,clients(id),tasks(id)"),
            ("id","not.gt.10"),
            ("tasks.id","lt.500"),
            ("not.or", "(id.eq.11,id.eq.12)"),
            ("tasks.or", "(id.eq.11,id.eq.12)"),
            ], None, &emtpy_hashmap, &emtpy_hashmap);

        assert_eq!(
            a.unwrap()
            ,
            ApiRequest {
                method: Method::GET,
                accept_content_type: ApplicationJSON,
                headers: &emtpy_hashmap,
                cookies: &emtpy_hashmap,
                query: 
                    Select { order: vec![], limit: None, offset: None,
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                            Simple {field: Field {name: s("name"), json_path: None}, alias: None},
                            SubSelect{
                                query: Select { order: vec![], limit: None, offset: None,
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: vec![s("clients")],
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
                                query: Select { order: vec![], limit: None, offset: None,
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: vec![s("tasks")],
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
                        from: vec![s("projects")],
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
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, &vec![
                ("select", "id,name,unknown(id)")
            ], None, &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e)),
            Err(AppError::NoRelBetween{origin:s("projects"), target:s("unknown")}).map_err(|e| format!("{}",e))
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, &vec![
                ("select", "id-,na$me")
            ], None, &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e)),
            Err(AppError::ParseRequestError{
                message: s("\"failed to parse select parameter (id-,na$me)\" (line 1, column 4)"),
                details: s("Unexpected `,` Expected `letter`, `digit`, `_` or ` `")
            }).map_err(|e| format!("{}",e))
        );
    }

    #[test]
    fn test_parse_post(){
        let emtpy_hashmap = HashMap::new();
        let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
       
        let payload = s(r#"{"id":10, "name":"john"}"#);
        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&payload), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Ok(ApiRequest {
                method: Method::POST,
                accept_content_type: ApplicationJSON,
                headers: &emtpy_hashmap,
                cookies: &emtpy_hashmap,
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
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id,name"),
                ("id","gt.10"),
                ("columns","id,name"),
            ], Some(&payload), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Ok(ApiRequest {
                method: Method::POST,
                accept_content_type: ApplicationJSON,
                headers: &emtpy_hashmap,
                cookies: &emtpy_hashmap,
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
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id"),
                ("id","gt.10"),
                ("columns","id,1$name"),
            ], Some(&s(r#"{"id":10, "name":"john", "phone":"123"}"#)), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Err(AppError::ParseRequestError {
                message: s("\"failed to parse columns parameter (id,1$name)\" (line 1, column 5)"),
                details: s("Unexpected `$` Expected `,`, `whitespaces` or `end of input`"),
            }).map_err(|e| format!("{}",e))
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"{"id":10, "name""#)), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Err(AppError::InvalidBody {
                message: s("Failed to parse json body EOF while parsing an object at line 1 column 16")
            }).map_err(|e| format!("{}",e))
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "phone":"123"}]"#)), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Err(AppError::InvalidBody {
                message: s("All object keys must match"),
            }).map_err(|e| format!("{}",e))
        );

        

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, &vec![
                ("select", "id,name,unknown(id)")
            ], None, &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e)),
            Err(AppError::NoRelBetween{origin:s("projects"), target:s("unknown")}).map_err(|e| format!("{}",e))
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::GET, &vec![
                ("select", "id-,na$me")
            ], None, &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e)),
            Err(AppError::ParseRequestError{
                message: s("\"failed to parse select parameter (id-,na$me)\" (line 1, column 4)"),
                details: s("Unexpected `,` Expected `letter`, `digit`, `_` or ` `")
            }).map_err(|e| format!("{}",e))
        );

        assert_eq!(
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id"),
                ("id","gt.10"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#)), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Ok(ApiRequest {
                method: Method::POST,
                accept_content_type: ApplicationJSON,
                headers: &emtpy_hashmap,
                cookies: &emtpy_hashmap,
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
            parse(&s("api"), &s("projects"), &db_schema, &Method::POST, &vec![
                ("select", "id,name,tasks(id),clients(id)"),
                ("id","gt.10"),
                ("tasks.id","gt.20"),
            ], Some(&s(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#)), &emtpy_hashmap, &emtpy_hashmap).map_err(|e| format!("{}",e))
            ,
            Ok(ApiRequest {
                method: Method::POST,
                accept_content_type: ApplicationJSON,
                headers: &emtpy_hashmap,
                cookies: &emtpy_hashmap,
                query: 
                    Insert {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                            Simple {field: Field {name: s("name"), json_path: None}, alias: None},
                            SubSelect{
                                query: Select { order: vec![], limit: None, offset: None,
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: vec![s("tasks")],
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
                                query: Select { order: vec![], limit: None, offset: None,
                                    select: vec![
                                        Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                                    ],
                                    from: vec![s("clients")],
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
        assert_eq!( get_join(&s("api"), &db_schema, &s("projects"), &s("tasks"), &mut None).map_err(|e| format!("{}",e)),
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
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("projects"), &mut None).map_err(|e| format!("{}",e)),
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
        assert_eq!( get_join(&s("api"), &db_schema, &s("clients"), &s("projects"), &mut None).map_err(|e| format!("{}",e)),
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
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("users"), &mut None).map_err(|e| format!("{}",e)),
            Ok(
               
                    Many(
                        Qi(s("api"), s("users_tasks")),
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
        assert_eq!( get_join(&s("api"), &db_schema, &s("tasks"), &s("users"), &mut Some(s("users_tasks"))).map_err(|e| format!("{}",e)),
            Ok(
               
                    Many(
                        Qi(s("api"), s("users_tasks")),
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
        assert_eq!(list_value().easy_parse("()"), Ok((vec![],"")));
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
        assert_eq!(field_name().easy_parse("field with space "), Ok((s("field with space"),"")));
        assert_eq!(field_name().easy_parse("field12"), Ok((s("field12"),"")));
        assert_ne!(field_name().easy_parse("field,invalid"), Ok((s("field,invalid"),"")));
        assert_eq!(field_name().easy_parse("field-name"), Ok((s("field-name"),"")));
        assert_eq!(field_name().easy_parse("field-name->"), Ok((s("field-name"),"->")));
        assert_eq!(quoted_value().easy_parse("\"field name\""), Ok((s("field name"),"")));
    }

    #[test]
    fn parse_order(){
        
        assert_eq!(order_term().easy_parse("field"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: None,null_order: None},"")));
        assert_eq!(order_term().easy_parse("field.asc"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Asc),null_order: None},"")));
        assert_eq!(order_term().easy_parse("field.desc"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Desc),null_order: None},"")));
        assert_eq!(order_term().easy_parse("field.desc.nullsfirst"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Desc),null_order: Some(OrderNulls::NullsFirst)},"")));
        assert_eq!(order_term().easy_parse("field.desc.nullslast"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Desc),null_order: Some(OrderNulls::NullsLast)},"")));
        assert_eq!(order_term().easy_parse("field.nullslast"), Ok((OrderTerm{term:Field{name:s("field"), json_path:None},direction: None,null_order: Some(OrderNulls::NullsLast)},"")));
        assert_eq!(
            order().easy_parse("field,field.asc,field.desc.nullslast"),
            Ok((vec![
                OrderTerm{term:Field{name:s("field"), json_path:None},direction: None,null_order: None},
                OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Asc),null_order: None},
                OrderTerm{term:Field{name:s("field"), json_path:None},direction: Some(OrderDirection::Desc),null_order: Some(OrderNulls::NullsLast)},
            ]
            ,""))
        );
    }

    #[test]
    fn parse_columns() {
        assert_eq!(columns().easy_parse("col1, col2 "), Ok((vec![s("col1"), s("col2")],"")));
        
        assert_eq!(columns().easy_parse(position::Stream::new("id,# name")), Err(Errors {
            position: SourcePosition { line: 1, column: 4 },
            errors: vec![
                Error::Unexpected('#'.into()),
                Error::Expected("whitespace".into()),
                Error::Expected('"'.into()),
                Error::Expected("letter".into()),
                Error::Expected("digit".into()),
                Error::Expected('_'.into()),
                Error::Expected(' '.into()),
            ]
        }));

        assert_eq!(columns().easy_parse(position::Stream::new("col1, col2, ")), Err(Errors {
            position: SourcePosition { line: 1, column: 13 },
            errors: vec![
                Error::Unexpected("end of input".into()),
                Error::Expected("whitespace".into()),
                Error::Expected('"'.into()),
                Error::Expected("letter".into()),
                Error::Expected("digit".into()),
                Error::Expected('_'.into()),
                Error::Expected(' '.into()),
            ]
        }));

        // assert_eq!(columns().easy_parse(position::Stream::new("col1, col2 col3")), Err(Errors {
        //     position: SourcePosition { line: 1, column: 12 },
        //     errors: vec![
        //         Error::Unexpected('c'.into()),
        //         Error::Expected(','.into()),
        //         Error::Expected("whitespaces".into()),
        //         Error::Expected("end of input".into())
        //     ]
        // }));
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
        //println!("{:#?}", tree_path().easy_parse("stores.zone_type_id"));
        //assert!(false);
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
                    query: Select { order: vec![], limit: None, offset: None,
                        select: vec![
                            Simple {field: Field {name:s("column0"), json_path: Some(vec![JArrow(JKey(s("key")))])}, alias:  None},
                            Simple {field: Field {name:s("column1"), json_path: None}, alias:  None},
                            Simple {field: Field {name:s("column2"), json_path: None}, alias:  Some(s("alias2"))},
                        ],
                        from: vec![s("table")],
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
