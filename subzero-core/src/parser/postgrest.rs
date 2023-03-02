//use core::slice::SlicePattern;
use std::collections::{BTreeSet, HashMap, HashSet, BTreeMap};
use std::iter::{zip, FromIterator};

use std::borrow::Cow;
//use std::borrow::Cow::Borrowed;
//use std::str::EncodeUtf16;

use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, LogicOperator::*, QueryNode::*, SelectItem::*, SelectKind::*, *};
use crate::error::*;
use crate::schema::{ObjectType::*, PgType::*, ProcReturnType::*, *};

use csv::{Reader, ByteRecord};
use serde_json::{
    value::{RawValue as JsonRawValue, Value as JsonValue},
};
use snafu::{OptionExt, ResultExt};

use nom::{
    Err,
    error::{ParseError, context, ErrorKind, VerboseErrorKind, VerboseError},
    //error::convert_error,
    combinator::{peek, recognize, eof, map, map_res, map_opt, opt, value},
    sequence::{delimited, terminated, preceded, tuple},
    bytes::complete::{tag, is_not, is_a, take},
    character::complete::{multispace0, char, alpha1, digit1, one_of},
    multi::{many1, many0, separated_list1, separated_list0},
    branch::{alt},
};

/// Useful functions to calculate the offset between slices and show a hexdump of a slice
pub trait Offset {
    /// Offset between the first byte of self and the first byte of the argument
    fn offset(&self, second: &Self) -> usize;
}
// impl<'a> Offset for &'a str {
//     fn offset(&self, second: &Self) -> usize {
//       let fst = self.as_ptr();
//       let snd = second.as_ptr();

//       snd as usize - fst as usize
//     }
// }
impl Offset for str {
    fn offset(&self, second: &Self) -> usize {
        let fst = self.as_ptr();
        let snd = second.as_ptr();

        snd as usize - fst as usize
    }
}

pub fn convert_error<I: core::ops::Deref<Target = str>>(input: I, e: VerboseError<I>) -> (Vec<usize>, nom::lib::std::string::String) {
    use nom::lib::std::fmt::Write;
    //use nom::traits::Offset;

    let mut result = nom::lib::std::string::String::new();
    let mut offsets = vec![];

    for (i, (substring, kind)) in e.errors.iter().enumerate() {
        let offset = input.offset(substring);

        if input.is_empty() {
            offsets.push(offset);
            match kind {
                VerboseErrorKind::Char(c) => {
                    write!(&mut result, "{i}: expected '{c}', got empty input\n\n")
                }

                VerboseErrorKind::Nom(e) => write!(&mut result, "{i}: in {e:?}, got empty input\n\n"),
                VerboseErrorKind::Context(s) => write!(&mut result, "{i}: in {s}, got empty input\n\n"),
            }
        } else {
            let prefix = &input.as_bytes()[..offset];

            // Count the number of newlines in the first `offset` bytes of input
            let line_number = prefix.iter().filter(|&&b| b == b'\n').count() + 1;

            // Find the line that includes the subslice:
            // Find the *last* newline before the substring starts
            let line_begin = prefix.iter().rev().position(|&b| b == b'\n').map(|pos| offset - pos).unwrap_or(0);

            // Find the full line after that newline
            let line = input[line_begin..].lines().next().unwrap_or(&input[line_begin..]).trim_end();

            // The (1-indexed) column number is the offset of our substring into that line
            let column_number = line.offset(substring) + 1;
            offsets.push(column_number);
            match kind {
                VerboseErrorKind::Char(c) => {
                    if let Some(actual) = substring.chars().next() {
                        write!(
                            &mut result,
                            "{i}: at line {line_number}:\n\
                            {line}\n\
                            {caret:>column$}\n\
                            expected '{expected}', found {actual}\n\n",
                            i = i,
                            line_number = line_number,
                            line = line,
                            caret = '^',
                            column = column_number,
                            expected = c,
                            actual = actual,
                        )
                    } else {
                        write!(
                            &mut result,
                            "{i}: at line {line_number}:\n\
                            {line}\n\
                            {caret:>column$}\n\
                            expected '{expected}', got end of input\n\n",
                            i = i,
                            line_number = line_number,
                            line = line,
                            caret = '^',
                            column = column_number,
                            expected = c,
                        )
                    }
                }
                VerboseErrorKind::Context(s) => write!(
                    &mut result,
                    "{i}: at line {line_number}, in {context}:\n\
                    {line}\n\
                    {caret:>column$}\n\n",
                    i = i,
                    line_number = line_number,
                    context = s,
                    line = line,
                    caret = '^',
                    column = column_number,
                ),
                VerboseErrorKind::Nom(e) => write!(
                    &mut result,
                    "{i}: at line {line_number}, in {nom_err:?}:\n\
                    {line}\n\
                    {caret:>column$}\n\n",
                    i = i,
                    line_number = line_number,
                    nom_err = e,
                    line = line,
                    caret = '^',
                    column = column_number,
                ),
            }
        }
        // Because `write!` to a `String` is infallible, this `unwrap` is fine.
        .unwrap();
    }

    (offsets, result)
}

// use nom::IResult;
type IResult<I, O, E = nom::error::VerboseError<I>> = Result<(I, O), Err<E>>;
type Parsed<'a, T> = IResult<&'a str, T>;

const STAR: &str = "*";
const ALIAS_SUFIXES: [&str; 10] = ["_0", "_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9"];
lazy_static! {
    // static ref STAR: String = "*".to_string();
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
        OPERATORS.keys().chain(["not","in"].iter()).chain(FTS_OPERATORS.keys()).map(|&op| format!("{op}.") )
        .chain(FTS_OPERATORS.keys().map(|&op| format!("{op}(") ))
        .collect()
    };
}

fn get_payload<'a>(content_type: ContentType, _body: &'a str, columns_param: Option<Vec<&'a str>>) -> Result<(Vec<&'a str>, Cow<'a, str>)> {
    let (columns, body) = match (content_type, columns_param) {
        (ApplicationJSON, Some(c)) | (SingularJSON, Some(c)) => Ok((c, Cow::Borrowed(_body))),
        (ApplicationJSON, None) | (SingularJSON, None) => {
            // first nonempty char in body
            let c = _body.chars().find(|c| !c.is_whitespace());
            let columns = match c {
                Some('{') => {
                    let json = serde_json::from_str::<BTreeMap<&str, &JsonRawValue>>(_body).context(JsonDeserializeSnafu)?;
                    Ok(json.keys().copied().collect::<Vec<_>>())
                }
                Some('[') => {
                    let json = serde_json::from_str::<Vec<BTreeMap<&str, &JsonRawValue>>>(_body).context(JsonDeserializeSnafu)?;
                    let columns = match json.get(0) {
                        Some(row) => row.keys().copied().collect::<Vec<_>>(),
                        None => vec![],
                    };
                    let canonical_set: HashSet<_> = columns.iter().copied().collect();
                    let all_keys_match = json.iter().all(|vv| canonical_set == HashSet::from_iter(vv.keys().copied()));
                    if all_keys_match {
                        Ok(columns)
                    } else {
                        Err(Error::InvalidBody {
                            message: "All object keys must match".to_string(),
                        })
                    }
                }
                _ => Err(Error::InvalidBody {
                    message: "Failed to parse json body".to_string(),
                }),
            }?;
            Ok((columns, Cow::Borrowed(_body)))
        }
        (TextCSV, cols) => {
            let mut rdr = Reader::from_reader(_body.as_bytes());
            let mut rows = vec![];
            let headers = match cols {
                Some(c) => {
                    rdr.set_byte_headers(ByteRecord::from(c.clone()));
                    c
                }
                None => {
                    // parse the first row as headers manually
                    // we do this because of lifetime issues with the csv crate
                    // get the first row directly from the _body
                    let first_row = match _body.lines().next() {
                        Some(row) => Ok(row),
                        None => Err(Error::InvalidBody {
                            message: "Failed to parse csv body".to_string(),
                        }),
                    }?;
                    // parse line as csv header row
                    let columns: Vec<&'a str> = first_row.split(',').map(str::trim).map(|s| s.trim_matches('"')).collect();

                    columns
                }
            };

            for record in rdr.byte_records() {
                rows.push(record.context(CsvDeserializeSnafu)?);
            }

            //manually create the json body
            let mut body = String::from("[");
            for row in rows {
                body.push('{');
                for (i, v) in row.iter().enumerate() {
                    body.push('"');
                    body.push_str(headers[i]);
                    body.push_str("\":");
                    match std::str::from_utf8(v).context(Utf8DeserializeSnafu)? {
                        "NULL" => body.push_str("null"),
                        vv => {
                            body.push('"');
                            body.push_str(vv.replace('"', "\\\"").as_str());
                            body.push('"');
                        }
                    }
                    body.push(',');
                }
                body.pop();
                body.push_str("},");
            }
            body.pop();
            body.push(']');
            Ok((headers, Cow::Owned(body)))
        }
        (Other(t), _) => Err(Error::ContentTypeError {
            message: format!("None of these Content-Types are available: {t}"),
        }),
    }?;
    Ok((columns, body))
}

#[allow(clippy::too_many_arguments)]
pub fn parse<'a>(
    schema: &'a str, root: &'a str, db_schema: &'a DbSchema<'a>, method: &'a str, path: &'a str, get: Vec<(&'a str, &'a str)>, body: Option<&'a str>,
    headers: HashMap<&'a str, &'a str>, cookies: HashMap<&'a str, &'a str>, max_rows: Option<&'a str>,
) -> Result<ApiRequest<'a>> {
    //let body = body.map(|b| b.to_string());
    let schema_obj = db_schema.schemas.get(schema).context(UnacceptableSchemaSnafu {
        schemas: vec![schema.to_owned()],
    })?;
    let root_obj = schema_obj.objects.get(root).context(NotFoundSnafu { target: root })?;

    //let mut select_items = vec![SelectItem::Star];
    let mut limits = vec![];
    let mut offsets = vec![];
    let mut orders = vec![];
    let mut groupbys = vec![];
    let mut conditions = vec![];
    let mut columns_: Option<Vec<&str>> = None;
    let mut on_conflict_: Option<Vec<&str>> = None;
    //let mut
    let mut fn_arguments = vec![];
    let accept_content_type = match headers.get("accept") {
        //TODO!!! accept header can have multiple content types
        Some(&accept_header) => {
            // let (act, _) = content_type()
            //     .message("failed to parse accept header")
            //     .easy_parse(accept_header)
            //     .map_err(|_| Error::ContentTypeError {
            //         message: format!("None of these Content-Types are available: {}", accept_header),
            //     })?;
            let (_, act) = context("failed to parse accept header", content_type)(accept_header).map_err(|_| Error::ContentTypeError {
                message: format!("None of these Content-Types are available: {accept_header}"),
            })?;
            Ok(act)
        }
        None => Ok(ApplicationJSON),
    }?;
    let content_type = match headers.get("content-type") {
        Some(&t) => {
            // let (act, _) = content_type()
            //     .message("failed to parse content-type header")
            //     .easy_parse(t)
            //     .map_err(|_| Error::ContentTypeError {
            //         message: format!("None of these Content-Types are available: {}", t),
            //     })?;
            let (_, act) = context("failed to parse content-type header", content_type)(t).map_err(|_| Error::ContentTypeError {
                message: format!("None of these Content-Types are available: {t}"),
            })?;
            Ok(act)
        }
        None => Ok(ApplicationJSON),
    }?;
    let preferences = match headers.get("prefer") {
        Some(&pref) => {
            // let (p, _) = preferences()
            //     .message("failed to parse Prefer header ")
            //     .easy_parse(pref)
            //     .map_err(to_app_error(pref))?;
            let (_, p) = context("failed to parse Prefer header", preferences)(pref).map_err(|e| to_app_error(pref, e))?;
            Ok(Some(p))
        }
        None => Ok(None),
    }?;

    let mut select_items = vec![Item(Star)];
    // iterate over parameters, parse and collect them in the relevant vectors
    for (k, v) in get.iter() {
        match *k {
            "select" => {
                // let (parsed_value, _) = select()
                //     .message("failed to parse select parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                // select_items = parsed_value;
                let (_, parsed_value) = context("failed to parse select parameter", select)(v).map_err(|e| to_app_error(v, e))?;
                select_items = parsed_value
            }

            "columns" => {
                // let (parsed_value, _) = columns()
                //     .message("failed to parse columns parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                // columns_ = Some(parsed_value);
                let (_, parsed_value) = context("failed to parse columns parameter", columns)(v).map_err(|e| to_app_error(v, e))?;
                columns_ = Some(parsed_value);
            }
            "groupby" => {
                // let (parsed_value, _) = groupby()
                //     .message("failed to parse groupby parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                // groupbys = parsed_value;
                let (_, parsed_value) = context("failed to parse groupby parameter", groupby)(v).map_err(|e| to_app_error(v, e))?;
                groupbys = parsed_value;
            }
            "on_conflict" => {
                // let (parsed_value, _) = on_conflict()
                //     .message("failed to parse on_conflict parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                // on_conflict_ = Some(parsed_value);
                let (_, parsed_value) = context("failed to parse on_conflict parameter", on_conflict)(v).map_err(|e| to_app_error(v, e))?;
                on_conflict_ = Some(parsed_value);
            }

            kk if is_logical(kk) => {
                // let ((tp, n, lo), _) = logic_tree_path()
                //     .message("failed to parser logic tree path")
                //     .easy_parse(*k)
                //     .map_err(to_app_error(k))?;
                //let (tp, n, lo): (Vec<&str>, bool, LogicOperator) = todo!();
                let (_, (tp, n, lo)) = context("failed to parser logic tree path", logic_tree_path)(k).map_err(|e| to_app_error(k, e))?;

                // let ns = if n { "not." } else { "" };
                // let los = if lo == And { "and" } else { "or" };
                // let s = format!("{}{}{}", ns, los, v);

                // let (c, _) = logic_condition()
                //     .message("failed to parse logic tree")
                //     .easy_parse(s.as_str())
                //     .map_err(to_app_error(&s))?;
                //let (_, c) = logic_condition(Some(n), Some(lo), v).map_err(|e| to_app_error("failed to parse logic tree", e))?;
                let (_, c) =
                    context("failed to parse logic tree", |ii| logic_condition(Some(&n), Some(&lo), ii))(v).map_err(|e| to_app_error(v, e))?;
                conditions.push((tp, c));
            }

            kk if is_limit(kk) => {
                // let ((tp, _), _) = tree_path()
                //     .message("failed to parser limit tree path")
                //     .easy_parse(*k)
                //     .map_err(to_app_error(k))?;
                let (_, (tp, _)) = context("failed to parser limit tree path", tree_path)(k).map_err(|e| to_app_error(k, e))?;
                // let (parsed_value, _) = limit()
                //     .message("failed to parse limit parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                let (_, parsed_value) = context("failed to parse limit parameter", limit)(v).map_err(|e| to_app_error(v, e))?;
                limits.push((tp, parsed_value));
            }

            kk if is_offset(kk) => {
                // let ((tp, _), _) = tree_path()
                //     .message("failed to parser offset tree path")
                //     .easy_parse(*k)
                //     .map_err(to_app_error(k))?;
                let (_, (tp, _)) = context("failed to parser offset tree path", tree_path)(k).map_err(|e| to_app_error(k, e))?;
                // let (parsed_value, _) = offset()
                //     .message("failed to parse limit parameter")
                //     .easy_parse(*v)
                //     .map_err(to_app_error(v))?;
                let (_, parsed_value) = context("failed to parse limit parameter", offset)(v).map_err(|e| to_app_error(v, e))?;
                offsets.push((tp, parsed_value));
            }

            kk if is_order(kk) => {
                // let ((tp, _), _) = tree_path()
                //     .message("failed to parser order tree path")
                //     .easy_parse(*k)
                //     .map_err(to_app_error(k))?;
                let (_, (tp, _)) = context("failed to parser order tree path", tree_path)(k).map_err(|e| to_app_error(k, e))?;
                // let (parsed_value, _) = order().message("failed to parse order"*v).map_err(to_app_error(v))?;
                let (_, parsed_value) = context("failed to parse order", order)(v).map_err(|e| to_app_error(v, e))?;
                orders.push((tp, parsed_value));
            }

            //is filter or function parameter
            _ => {
                // let ((tp, field), _) = tree_path()
                //     .message("failed to parser filter tree path")
                //     .easy_parse(*k)
                //     .map_err(to_app_error(k))?;
                //let (tp, field): (Vec<&str>, Field) = todo!();
                let (_, (tp, field)) = context("failed to parser filter tree path", tree_path)(k).map_err(|e| to_app_error(k, e))?;

                let data_type = root_obj.columns.get(field.name).map(|c| c.data_type);
                match root_obj.kind {
                    Function { .. } => {
                        if !tp.is_empty() || has_operator(v) {
                            // this is a filter
                            // let ((negate, filter), _) = negatable_filter(&data_type)
                            //     .message("failed to parse filter")
                            //     .easy_parse(*v)
                            //     .map_err(to_app_error(v))?;
                            //let (negate, filter) = todo!();
                            //let (_, (negate, filter)) = negatable_filter(&data_type, v).map_err(|e| to_app_error("failed to parse filter", e))?;
                            let (_, (negate, filter)) =
                                context("failed to parse filter", |ii| negatable_filter(&data_type, ii))(v).map_err(|e| to_app_error(v, e))?;
                            conditions.push((tp, Condition::Single { field, filter, negate }));
                        } else {
                            //this is a function parameter
                            fn_arguments.push((*k, *v));
                        }
                    }
                    _ => {
                        // let ((negate, filter), _) = negatable_filter(&data_type)
                        //     .message("failed to parse filter")
                        //     .easy_parse(*v)
                        //     .map_err(to_app_error(v))?;
                        //let (negate, filter) = todo!();
                        //let (_, (negate, filter)) = negatable_filter(&data_type, v).map_err(|e| to_app_error("failed to parse filter", e))?;
                        let (_, (negate, filter)) =
                            context("failed to parse filter", |ii| negatable_filter(&data_type, ii))(v).map_err(|e| to_app_error(v, e))?;
                        conditions.push((tp, Condition::Single { field, filter, negate }));
                    }
                };
            }
        }
    }

    // in some cases we don't want to select anything back, event when select parameter is specified,
    // so in order to not trigger any permissions errors, we select nothing back
    let is_function_call = matches!(&root_obj.kind, Function { .. });
    if !is_function_call {
        match (method, &preferences) {
            ("POST", None)
            | (
                "POST",
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                }),
            )
            | (
                "POST",
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                }),
            )
            | ("PATCH", None)
            | (
                "PATCH",
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                }),
            )
            | (
                "PATCH",
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                }),
            )
            | ("PUT", None)
            | (
                "PUT",
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                }),
            )
            | (
                "PUT",
                Some(Preferences {
                    representation: Some(Representation::HeadersOnly),
                    ..
                }),
            )
            | ("DELETE", None)
            | (
                "DELETE",
                Some(Preferences {
                    representation: Some(Representation::None),
                    ..
                }),
            ) => select_items = vec![],
            _ => {}
        }
    };

    let (node_select, sub_selects) = split_select(select_items);
    let mut query = match (method, &root_obj.kind, body) {
        (method, Function { return_type, parameters, .. }, _body) => {
            let parameters_map = parameters.iter().map(|p| (p.name, p)).collect::<HashMap<_, _>>();
            let required_params: HashSet<&str> = HashSet::from_iter(parameters.iter().filter(|p| p.required).map(|p| p.name));
            let all_params: HashSet<&str> = HashSet::from_iter(parameters.iter().map(|p| p.name));
            let (parameter_values, params) = match (method, _body) {
                ("GET", None) => {
                    let mut args: BTreeMap<&str, JsonValue> = BTreeMap::new();
                    for (n, v) in fn_arguments {
                        if let Some(&p) = parameters_map.get(n) {
                            if p.variadic {
                                if let Some(e) = args.get_mut(n) {
                                    if let JsonValue::Array(a) = e {
                                        a.push(v.to_string().into());
                                    }
                                } else {
                                    args.insert(n, JsonValue::Array(vec![v.to_string().into()]));
                                }
                            } else {
                                args.insert(n, v.to_string().into());
                            }
                        } else {
                            //this is an unknown param, we still add it but bellow we'll return an error because of it
                            args.insert(n, v.to_string().into());
                        }
                    }
                    //let payload = serde_json::to_string(&args).context(JsonSerializeSnafu)?;
                    let params = match (parameters.len(), parameters.get(0)) {
                        (1, Some(p)) if p.name.is_empty() => CallParams::OnePosParam(p.clone()),
                        _ => {
                            //let specified_parameters = args.keys().collect::<Vec<_>>();
                            let specified_parameters: HashSet<&str> = HashSet::from_iter(args.keys().copied());
                            if !specified_parameters.is_superset(&required_params) || !specified_parameters.is_subset(&all_params) {
                                let mut argument_keys = args.keys().map(|k| k.to_string()).collect::<Vec<_>>();
                                argument_keys.sort();
                                return Err(Error::NoRpc {
                                    schema: schema.to_string(),
                                    proc_name: root.to_string(),
                                    argument_keys,
                                    has_prefer_single_object: false,
                                    content_type: accept_content_type,
                                    is_inv_post: false,
                                });
                            }
                            CallParams::KeyParams(
                                parameters
                                    .iter()
                                    .filter(|p| specified_parameters.contains(&p.name))
                                    .map(
                                        |&ProcParam {
                                             name,
                                             type_,
                                             required,
                                             variadic,
                                         }| ProcParam {
                                            name,
                                            type_,
                                            required,
                                            variadic,
                                        },
                                    )
                                    .collect::<Vec<_>>(),
                            )
                        }
                    };

                    Ok((ParamValues::Parsed(args), params))
                }
                ("POST", Some(payload)) => {
                    let params = match (parameters.len(), parameters.get(0)) {
                        (1, Some(p)) if p.name.is_empty() && (p.type_ == "json" || p.type_ == "jsonb") => CallParams::OnePosParam(p.clone()),
                        _ => {
                            let json_payload: BTreeMap<&str, &JsonRawValue> = match (payload.len(), content_type) {
                                (0, _) => serde_json::from_str("{}").context(JsonDeserializeSnafu),
                                (_, _) => serde_json::from_str(payload).context(JsonDeserializeSnafu),
                            }?;
                            let argument_keys: Vec<&str> = match columns_ {
                                None => json_payload.keys().copied().collect(),
                                Some(c) => json_payload.keys().copied().filter(|k| c.contains(k)).collect(),
                            };
                            let specified_parameters: HashSet<&str> = HashSet::from_iter(argument_keys.iter().copied());

                            if !specified_parameters.is_superset(&required_params) || !specified_parameters.is_subset(&all_params) {
                                return Err(Error::NoRpc {
                                    schema: schema.to_string(),
                                    proc_name: root.to_string(),
                                    argument_keys: argument_keys.iter().map(|k| k.to_string()).collect(),
                                    has_prefer_single_object: false,
                                    content_type: accept_content_type,
                                    is_inv_post: true,
                                });
                            }

                            CallParams::KeyParams(
                                parameters
                                    .iter()
                                    .filter(|p| specified_parameters.contains(&p.name))
                                    .map(
                                        |&ProcParam {
                                             name,
                                             type_,
                                             required,
                                             variadic,
                                         }| ProcParam {
                                            name,
                                            type_,
                                            required,
                                            variadic,
                                        },
                                    )
                                    .collect::<Vec<_>>(),
                            )
                        }
                    };

                    Ok((ParamValues::Raw(payload), params))
                }
                _ => Err(Error::UnsupportedVerb),
            }?;

            let payload = match parameter_values {
                ParamValues::Parsed(args) => Payload(Cow::Owned(serde_json::to_string(&args).context(JsonSerializeSnafu)?), None),
                ParamValues::Raw(r) => Payload(Cow::Borrowed(r), None),
            };
            let mut q = Query {
                node: FunctionCall {
                    fn_name: Qi(schema, root),
                    parameters: params,

                    //CallParams::KeyParams(vec![]),
                    payload,
                    //parameter_values,
                    is_scalar: matches!(return_type, One(Scalar) | SetOf(Scalar)),
                    returns_single: match return_type {
                        One(_) => true,
                        SetOf(_) => false,
                    },
                    is_multiple_call: false,

                    returning: vec![], //get_returning(&select_items)?,
                    select: node_select,
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    return_table_type: match return_type {
                        SetOf(Composite(Qi(a, b))) => Some(Qi(a, b)),
                        One(Composite(Qi(a, b))) => Some(Qi(a, b)),
                        _ => None,
                    },
                    limit: None,
                    offset: None,
                    order: vec![],
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;

            //we populate the returing because it relies on the "join" information
            // if let Query {
            //     node: FunctionCall {
            //         ref mut returning,
            //         ref select,
            //         ..
            //     },
            //     ref sub_selects,
            // } = q
            // {
            //     returning.extend(get_returning(select, sub_selects)?);
            // }
            Ok(q)
        }
        ("GET", _, None) => {
            let mut q = Query {
                node: Select {
                    select: node_select,
                    from: (root, None),
                    join_tables: vec![],
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    limit: None,
                    offset: None,
                    order: vec![],
                    groupby: groupbys,
                    check: None,
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;
            Ok(q)
        }
        ("POST", _, Some(_body)) => {
            let _body = body.context(InvalidBodySnafu {
                message: "body not available".to_string(),
            })?;

            let (columns, payload) = get_payload(content_type, _body, columns_)?;
            //let columns = _columns.iter().map(|c| c.as_str()).collect();

            let on_conflict = match &preferences {
                Some(Preferences { resolution: Some(r), .. }) => {
                    let on_conflict_cols = match on_conflict_ {
                        Some(cols) => cols,
                        None => root_obj
                            .columns
                            .iter()
                            .filter_map(|(n, c)| if c.primary_key { Some(*n) } else { None })
                            .collect::<Vec<_>>(),
                    };
                    Some((r.clone(), on_conflict_cols))
                }
                _ => None,
            };
            let mut q = Query {
                node: Insert {
                    into: root,
                    columns,
                    payload: Payload(payload, None),
                    check: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    returning: vec![], //get_returning(&select_items)?,
                    select: node_select,
                    on_conflict,
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;

            Ok(q)
        }
        ("DELETE", _, _) => {
            let mut q = Query {
                node: Delete {
                    from: root,
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    returning: vec![],
                    select: node_select,
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;

            Ok(q)
        }
        ("PATCH", _, Some(_body)) => {
            let _body = body.context(InvalidBodySnafu {
                message: "body not available".to_string(),
            })?;

            let (columns, payload) = get_payload(content_type, _body, columns_)?;
            //let columns = _columns.iter().map(|c| c.as_str()).collect();
            let mut q = Query {
                node: Update {
                    table: root,
                    columns,
                    payload: Payload(payload, None),
                    check: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    returning: vec![],
                    select: node_select,
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;

            Ok(q)
        }
        ("PUT", _, Some(_body)) => {
            let _body = body.context(InvalidBodySnafu {
                message: "body not available".to_string(),
            })?;

            let (columns, payload) = get_payload(content_type, _body, columns_)?;
            //let columns = _columns.iter().map(|c| c.as_str()).collect();

            // check all the required filters are there for the PUT request to be valid
            let eq = &"=".to_string();
            let root_conditions = conditions
                .iter()
                .filter_map(|(p, c)| if p.is_empty() { Some(c) } else { None })
                .collect::<Vec<_>>();
            let pk_cols = root_obj
                .columns
                .iter()
                .filter_map(|(n, c)| if c.primary_key { Some(*n) } else { None })
                .collect::<BTreeSet<_>>();
            let conditions_on_fields = root_conditions
                .iter()
                .filter_map(|&c| match c {
                    Single {
                        field,
                        filter: Op(o, _),
                        negate: false,
                    } if o == eq => Some(field.name),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();

            if !(!pk_cols.is_empty() && conditions_on_fields == pk_cols && root_conditions.len() == conditions_on_fields.len()) {
                return Err(Error::InvalidFilters);
            }

            let mut q = Query {
                node: Insert {
                    into: root,
                    columns,
                    payload: Payload(payload, None),
                    check: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    returning: vec![], //get_returning(&select_items)?,
                    select: node_select,
                    on_conflict: Some((Resolution::MergeDuplicates, pk_cols.into_iter().collect())),
                },
                sub_selects,
            };
            add_join_info(&mut q, schema, db_schema, 0)?;

            Ok(q)
        }
        _ => Err(Error::UnsupportedVerb),
    }?;

    insert_join_conditions(&mut query, schema)?;

    query.insert_conditions(conditions)?;

    query.insert_properties(limits, |q, p| {
        let limit = match &mut q.node {
            Select { limit, .. } => Ok(limit),
            Insert { .. } => Err(Error::LimitOffsetNotAllowedError),
            Delete { .. } => Err(Error::LimitOffsetNotAllowedError),
            Update { .. } => Err(Error::LimitOffsetNotAllowedError),
            FunctionCall { limit, .. } => Ok(limit),
        }?;
        for v in p {
            *limit = Some(v);
        }
        Ok(())
    })?;

    query.insert_properties(offsets, |q, p| {
        let offset = match &mut q.node {
            Select { offset, .. } => Ok(offset),
            Insert { .. } => Err(Error::LimitOffsetNotAllowedError),
            Delete { .. } => Err(Error::LimitOffsetNotAllowedError),
            Update { .. } => Err(Error::LimitOffsetNotAllowedError),
            FunctionCall { offset, .. } => Ok(offset),
        }?;
        for v in p {
            *offset = Some(v);
        }
        Ok(())
    })?;

    query.insert_properties(orders, |q, p| {
        let order = match &mut q.node {
            Select { order, .. } => Ok(order),
            Insert { .. } => Err(Error::OrderNotAllowedError),
            Delete { .. } => Err(Error::OrderNotAllowedError),
            Update { .. } => Err(Error::OrderNotAllowedError),
            FunctionCall { order, .. } => Ok(order),
        }?;
        for o in p {
            *order = o;
        }
        Ok(())
    })?;

    // enforce max rows limit for each node
    enforce_max_rows(&mut query, max_rows);
    // replace select * with all the columns
    replace_start(&mut query, schema_obj)?;

    Ok(ApiRequest {
        schema_name: schema,
        read_only: matches!(method, "GET"),
        preferences,
        method,
        path,
        query,
        accept_content_type,
        headers,
        cookies,
        get,
    })
}

// enforce max rows
fn enforce_max_rows<'a>(query: &mut Query<'a>, max_rows: Option<&'a str>) {
    if let Some(max_str) = max_rows {
        let max = max_str.parse::<u32>().unwrap_or(1000);
        for (_, node) in query {
            let none = &mut None;
            let limit = match node {
                FunctionCall { limit, .. } => limit,
                Select { limit, .. } => limit,
                Insert { .. } => none,
                Delete { .. } => none,
                Update { .. } => none,
            };
            match limit {
                Some(SingleVal(l, ..)) => match l.parse::<u32>() {
                    Ok(ll) if ll > max => *limit = Some(SingleVal(Cow::Borrowed(max_str), None)),
                    _ => *limit = Some(SingleVal(Cow::Borrowed(max_str), None)),
                },
                None => *limit = Some(SingleVal(Cow::Borrowed(max_str), None)),
            }
        }
    }
}

// replace star with all columns
fn replace_start<'a>(query: &mut Query<'a>, schema_obj: &Schema<'a>) -> Result<()> {
    for (_, node) in query {
        let (select, o_table_name) = match node {
            Select {
                select, from: (table, _), ..
            } => (select, Some(table)),
            Insert { select, into, .. } => (select, Some(into)),
            Delete { select, from, .. } => (select, Some(from)),
            Update { select, table, .. } => (select, Some(table)),
            // for function calls we don't know the table name always so we don't do anything
            FunctionCall { select, .. } => (select, None),
        };
        if let Some(table_name) = o_table_name {
            let mut star_removed = false;
            select.retain(|s| {
                if let SelectItem::Star = s {
                    star_removed = true;
                    false
                } else {
                    true
                }
            });
            if star_removed {
                let table_obj = schema_obj.objects.get(table_name).context(NotFoundSnafu {
                    target: table_name.to_string(),
                })?;
                for col in table_obj.columns.keys() {
                    select.push(SelectItem::Simple {
                        field: Field { name: col, json_path: None },
                        alias: None,
                        cast: None,
                    });
                }
            }
        }
    }
    Ok(())
}

// // parser functions
// fn lex<Input, P>(p: P) -> impl Parser<Input, Output = P::Output>
// where
//     P: Parser<Input>,
//     Input: Stream<Token = char>,
// {
//     p.skip(spaces())
// }

/// A combinator that takes a parser `inner` and produces a parser that also consumes both leading and
/// trailing whitespace, returning the output of `inner`.
fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where
    F: FnMut(&'a str) -> IResult<&'a str, O, E>,
{
    delimited(multispace0, inner, multispace0)
}

//done
// fn field_name<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     let dash = attempt(char('-').skip(not_followed_by(char('>'))));
//     lex(choice((
//         quoted_value(),
//         sep_by1(
//             many1::<String, _, _>(choice((letter(), digit(), one_of("_ ".chars())))).map(|s| s.trim().to_owned()),
//             dash,
//         )
//         .map(|words: Vec<String>| words.join("-")),
//     )))
// }
fn dash(i: &str) -> Parsed<&str> {
    terminated(tag("-"), peek(is_not(">")))(i)
}

fn field_name(i: &str) -> Parsed<&str> {
    alt((quoted_value, map(recognize(separated_list1(dash, many1(alt((alpha1, digit1, is_a("_ ")))))), |s| s.trim())))(i)
}

//done
// fn function_name<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     let dash = attempt(char('-').skip(not_followed_by(char('>'))));
//     lex(choice((
//         quoted_value(),
//         sep_by1(
//             many1::<String, _, _>(choice((letter(), digit(), one_of("_ ".chars())))).map(|s| s.trim().to_owned()),
//             dash,
//         )
//         .map(|words: Vec<String>| words.join("-")),
//     )))
// }
fn function_name(i: &str) -> Parsed<&str> {
    alt((quoted_value, map(recognize(separated_list1(dash, many1(alt((alpha1, digit1, is_a("_ ")))))), |s| s.trim())))(i)
}

//done
// fn quoted_value<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
// between(
//     char('"'),
//     char('"'),
//     many(
//         choice(
//             (
//                 none_of("\\\"".chars()),
//                 char('\\').and(any()).map(|(_, c)| c)
//             )
//         )
//     )
// )
// }

fn quoted_value_escaped(i: &str) -> Parsed<Cow<str>> {
    // map(
    //     preceded(
    //         char('\"'),
    //         cut(terminated(
    //             escaped_transform(
    //                 alphanumeric1,
    //                 '\\',
    //                 alt((
    //                     value("\\", tag("\\")),
    //                     value("\"", tag("\"")),
    //                     value("\n", tag("n")),
    //                 ))
    //             ),
    //             char('\"')
    //         ))
    //     ),
    //     Cow::Owned
    // )(i)
    map(
        delimited(
            char('"'),
            many0(alt((
                is_not("\\\""),
                //map(tag("\\\""),|_| "\""),
                preceded(char('\\'), take(1usize)),
            ))),
            char('"'),
        ),
        |v| Cow::Owned(v.join("")),
    )(i)
}

fn quoted_value(i: &str) -> Parsed<&str> {
    delimited(char('"'), is_not("\""), char('"'))(i)
}

//done
// fn field<Input>() -> impl Parser<Input, Output = Field>
// where
//     Input: Stream<Token = char>,
// {
//     field_name().and(optional(json_path())).map(|(name, json_path)| Field { name, json_path })
// }
fn field(i: &str) -> Parsed<Field> {
    map(tuple((field_name, opt(json_path))), |(name, json_path)| Field { name, json_path })(i)
}

//done
// fn json_path<Input>() -> impl Parser<Input, Output = Vec<JsonOperation>>
// where
//     Input: Stream<Token = char>,
// {
//     //let end = look_ahead( string("->").or(string("::")).map(|_| ()).or(eof()) );
//     let arrow = attempt(string("->>")).or(string("->")).map(|v| match v {
//         "->>" => JsonOperation::J2Arrow,
//         "->" => JsonOperation::JArrow,
//         &_ => panic!("error parsing json path"),
//     });
//     let signed_number = optional(string("-"))
//         .and(many1(digit()).and(look_ahead(choice((string("->"), string("::"), string("."), string(","))).or(eof().map(|_| "")))))
//         .map(|v: (Option<&str>, (String, &str))| {
//             let (s, (n, _)) = v;
//             format!("{}{}", s.unwrap_or(""), n)
//         });
//     let operand = choice((attempt(signed_number.map(JsonOperand::JIdx)), field_name().map(JsonOperand::JKey)));
//     //many1(arrow.and(operand.and(end)).map(|((arrow,(operand,_)))| arrow(operand)))
//     many1(arrow.and(operand).map(|(arrow, operand)| arrow(operand)))
// }
fn arrow(i: &str) -> Parsed<&str> {
    alt((tag("->>"), tag("->")))(i)
}
fn json_path(i: &str) -> Parsed<Vec<JsonOperation>> {
    many1(map(tuple((arrow, json_operand)), |(a, o)| match a {
        "->>" => JsonOperation::J2Arrow(o),
        "->" => JsonOperation::JArrow(o),
        &_ => unreachable!("error parsing json path"),
    }))(i)
}
fn signed_number(i: &str) -> Parsed<&str> {
    recognize(preceded(opt(char('-')), terminated(digit1, peek(alt((tag("->"), tag("::"), tag("."), tag(","), eof))))))(i)
}
fn json_operand(i: &str) -> Parsed<JsonOperand> {
    alt((map(signed_number, JsonOperand::JIdx), map(field_name, JsonOperand::JKey)))(i)
}
//done
// fn alias_separator<Input>() -> impl Parser<Input, Output = char>
// where
//     Input: Stream<Token = char>,
// {
//     attempt(char(':').skip(not_followed_by(char(':'))))
// }
fn alias_separator(i: &str) -> Parsed<&str> {
    terminated(tag(":"), peek(is_not(":")))(i)
    //tag(":")(i)
}

//done
// fn alias<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     choice((many1(choice((letter(), digit(), one_of("@._".chars())))), quoted_value()))
//         .and(alias_separator())
//         .map(|(a, _)| a)
// }
fn alias(i: &str) -> Parsed<&str> {
    terminated(recognize(many1(alt((alpha1, digit1, recognize(one_of("@._")))))), alias_separator)(i)
}

//done
// fn cast<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     string("::").and(many1(choice((letter(), digit())))).map(|(_, c)| c)
// }
fn cast(i: &str) -> Parsed<&str> {
    preceded(tag("::"), recognize(many1(alt((alpha1, digit1)))))(i)
}

//done
// fn dot<Input>() -> impl Parser<Input, Output = char>
// where
//     Input: Stream<Token = char>,
// {
//     char('.')
// }
fn dot(i: &str) -> Parsed<&str> {
    tag(".")(i)
}

//done
// fn tree_path<Input>() -> impl Parser<Input, Output = (Vec<String>, Field)>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(field_name(), dot())
//         .and(optional(json_path()))
//         .map(|a: (Vec<String>, Option<Vec<JsonOperation>>)| {
//             let (names, json_path) = a;
//             match names.split_last() {
//                 Some((name, path)) => (
//                     path.to_vec(),
//                     Field {
//                         name: name.clone(),
//                         json_path,
//                     },
//                 ),
//                 None => panic!("failed to parse tree path"),
//             }
//         })
// }
fn tree_path(i: &str) -> Parsed<(Vec<&str>, Field)> {
    map(tuple((separated_list1(dot, field_name), opt(json_path))), |(names, json_path)| match names.split_last() {
        Some((name, path)) => (path.to_vec(), Field { name, json_path }),
        None => unreachable!("failed to parse tree path"),
    })(i)
}

//done
// fn logic_tree_path<Input>() -> impl Parser<Input, Output = (Vec<String>, bool, LogicOperator)>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(field_name(), dot()).map(|names: Vec<String>| match names.split_last() {
//         Some((name, path)) => {
//             let op = match name.as_str() {
//                 "and" => And,
//                 "or" => Or,
//                 x => panic!("unknown logic operator {}", x),
//             };
//             match path.split_last() {
//                 Some((negate, path1)) => {
//                     if negate == "not" {
//                         (path1.to_vec(), true, op)
//                     } else {
//                         (path.to_vec(), false, op)
//                     }
//                 }
//                 None => (path.to_vec(), false, op),
//             }
//         }
//         None => panic!("failed to parse logic tree path"),
//     })
// }
fn logic_tree_path(i: &str) -> Parsed<(Vec<&str>, bool, LogicOperator)> {
    map(separated_list1(dot, field_name), |names| match names.split_last() {
        Some((&name, path)) => {
            let op = match name {
                "and" => LogicOperator::And,
                "or" => LogicOperator::Or,
                x => unreachable!("unknown logic operator {}", x),
            };
            match path.split_last() {
                Some((&negate, path1)) => {
                    if negate == "not" {
                        (path1.to_vec(), true, op)
                    } else {
                        (path.to_vec(), false, op)
                    }
                }
                None => (path.to_vec(), false, op),
            }
        }
        None => unreachable!("failed to parse logic tree path"),
    })(i)
}

//done
// fn select<Input>() -> impl Parser<Input, Output = Vec<SelectKind>>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(select_item(), lex(char(','))).skip(eof())
// }
fn select(i: &str) -> Parsed<Vec<SelectKind>> {
    terminated(separated_list1(ws(char(',')), select_item), eof)(i)
}

//done
// fn columns<Input>() -> impl Parser<Input, Output = Vec<String>>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(field_name(), lex(char(','))).skip(eof())
// }
fn columns(i: &str) -> Parsed<Vec<&str>> {
    terminated(separated_list1(tag(","), ws(field_name)), eof)(i)
}

//done
// fn on_conflict<Input>() -> impl Parser<Input, Output = Vec<String>>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(field_name(), lex(char(','))).skip(eof())
// }
fn on_conflict(i: &str) -> Parsed<Vec<&str>> {
    terminated(separated_list1(tag(","), ws(field_name)), eof)(i)
}

// done
// fn function_param<Input>() -> impl Parser<Input, Output = FunctionParam>
// where
//     Input: Stream<Token = char>,
// {
//     choice!(
//         function_call().map(|(fn_name, parameters)| FunctionParam::Func { fn_name, parameters }),
//         field().map(FunctionParam::Fld),
//         between(char('\''), char('\''), many::<String, _, _>(none_of("'".chars())),)
//             .and(optional(cast()))
//             .map(|(v, c)| FunctionParam::Val(SingleVal(v, c.clone()), c))
//     )
// }
fn function_param(i: &str) -> Parsed<FunctionParam> {
    alt((
        map(function_call, |(fn_name, parameters)| FunctionParam::Func { fn_name, parameters }),
        map(field, FunctionParam::Fld),
        map(tuple((delimited(char('\''), is_not("'"), char('\'')), opt(cast))), |(v, c)| {
            FunctionParam::Val(SingleVal(Cow::Borrowed(v), c.map(Cow::Borrowed)), c)
        }),
    ))(i)
}

//done
// We need to use `parser!` to break the recursive use of `function_call` to prevent the returned parser from containing itself
// fn function_call<Input>() -> impl Parser<Input, Output = (String, Vec<FunctionParam>)>
// where
//     Input: Stream<Token = char>,
// {
//     function_call_()
// }
// parser! {
//     #[inline]
//     fn function_call_[Input]()(Input) -> (String,Vec<FunctionParam>)
//     where [Input: Stream<Token = char>]
//     {
//         (
//             char('$'),
//             function_name(),
//             between(lex(char('(')), lex(char(')')),  sep_by(function_param(), lex(char(',')))),
//         )
//         .map(|(_, fn_name, parameters)| (fn_name,parameters))
//     }
// }
fn function_call(i: &str) -> Parsed<(&str, Vec<FunctionParam>)> {
    map(
        tuple((char('$'), function_name, delimited(ws(char('(')), separated_list0(ws(char(',')), function_param), ws(char(')'))))),
        |(_, fn_name, parameters)| (fn_name, parameters),
    )(i)
}

//done
// We need to use `parser!` to break the recursive use of `select_item` to prevent the returned parser from containing itself
// #[inline]
// fn select_item<Input>() -> impl Parser<Input, Output = SelectKind>
// where
//     Input: Stream<Token = char>,
// {
//     select_item_()
// }

//done
// parser! {
//     #[inline]
//     fn select_item_[Input]()(Input) -> SelectKind
//     where [ Input: Stream<Token = char> ]
//     {
//         let star = char('*').map(|_| Item(Star));
//         let column =
//             optional(attempt(alias()))
//             .and(field())
//             .and(optional(cast()))
//             .map(|((alias, field), cast)| Item(Simple {field, alias, cast}));
//         let function = (
//             optional(attempt(alias())),
//             function_call(),
//             optional(
//                 attempt(string("-p")
//                 .and(between(lex(char('(')), lex(char(')')),  sep_by(field(), lex(char(','))))))
//             ),
//             optional(
//                 attempt(string("-o")
//                 .and(between(lex(char('(')), lex(char(')')),  sep_by(order_term(), lex(char(','))))))
//             )
//         )
//         .map(|(alias, (fn_name, parameters), partitions, orders)|
//             Item(Func{
//                 alias, fn_name, parameters,
//                 partitions: match partitions { None => vec![], Some((_,p))=>p},
//                 orders: match orders {None => vec![], Some((_,o))=>o},
//
//             })
//         );
//
//         let sub_select = (
//             optional(attempt(alias())),
//             lex(field_name()),
//             optional(char('!').or(char('.')).and(field_name()).map(|(_,hint)| hint)),
//             between(lex(char('(')), lex(char(')')),  sep_by(select_item(), lex(char(','))))
//         )
//         .map(|(alias, from, hint, select)| {
//             let (sel, sub_sel) = split_select(select);
//             Sub(Box::new(SubSelect {
//                 query: Query{
//                     node: Select {
//                         select: sel,//select,
//                         from: (from, None),
//                         join_tables: vec![],
//                         //from_alias: alias,
//                         where_: ConditionTree { operator: And, conditions: vec![]},
//                         limit: None, offset: None, order: vec![],
//                         groupby: vec![],
//                     },
//                     sub_selects: sub_sel
//                 },
//                 alias,
//                 hint,
//                 join: None
//             }))
//         });
//
//         choice!(
//             attempt(function),
//             attempt(sub_select),
//             attempt(column),
//             star
//         )
//     }
// }
fn select_item(i: &str) -> Parsed<SelectKind> {
    let star = map(char('*'), |_| Item(Star));
    let column = map(tuple((opt(alias), field, opt(cast))), |(alias, field, cast)| Item(Simple { field, alias, cast }));
    let function = map(
        tuple((
            opt(alias),
            function_call,
            opt(tuple((tag("-p"), delimited(char('('), separated_list1(ws(char(',')), field), char(')'))))),
            opt(tuple((tag("-o"), delimited(char('('), separated_list1(ws(char(',')), order_term), char(')'))))),
        )),
        |(alias, (fn_name, parameters), partitions, orders)| {
            Item(Func {
                alias,
                fn_name,
                parameters,
                partitions: partitions.map(|(_, p)| p).unwrap_or_default(),
                orders: orders.map(|(_, o)| o).unwrap_or_default(),
            })
        },
    );
    let sub_select = map(
        tuple((
            opt(alias),
            ws(field_name),
            opt(map(tuple((one_of("!."), ws(field_name))), |(_, hint)| hint)),
            delimited(char('('), separated_list1(ws(char(',')), select_item), char(')')),
        )),
        |(alias, from, hint, select)| {
            let (sel, sub_sel) = split_select(select);
            Sub(Box::new(SubSelect {
                query: Query {
                    node: Select {
                        select: sel, //select,
                        from: (from, None),
                        join_tables: vec![],
                        //from_alias: alias,
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![],
                        },
                        limit: None,
                        offset: None,
                        order: vec![],
                        groupby: vec![],
                        check: None,
                    },
                    sub_selects: sub_sel,
                },
                alias,
                hint,
                join: None,
            }))
        },
    );
    alt((function, sub_select, column, star))(i)
}

//done
// fn single_value<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = (String, Option<String>)>
// where
//     Input: Stream<Token = char>,
// {
//     let dt = data_type.clone();
//     many(any()).map(move |v| (v, dt.clone()))
// }

fn single_value<'a>(data_type: &Option<&'a str>, i: &'a str) -> Parsed<'a, SingleVal<'a>> {
    let v = match data_type {
        Some(dt) => SingleVal(Cow::Borrowed(i), Some(Cow::Borrowed(*dt))),
        None => SingleVal(Cow::Borrowed(i), None),
    };
    Ok(("", v))
}

fn apply<'a, 'b, A: 'a, B: 'a>(a: &'b A, p: impl Fn(&'b A, &'a str) -> Parsed<'a, B> + 'b) -> impl Fn(&'a str) -> Parsed<'a, B> + 'b {
    move |i| p(a, i)
}

//done
// fn integer<Input>() -> impl Parser<Input, Output = SingleVal>
// where
//     Input: Stream<Token = char>,
// {
//     many1(digit()).map(|v| SingleVal(v, Some("integer".to_string())))
// }
fn integer(i: &str) -> Parsed<SingleVal> {
    let (input, integer) = recognize(many1(digit1))(i)?;
    Ok((input, SingleVal(Cow::Borrowed(integer), Some(Cow::Borrowed("integer")))))
}

//done
// fn limit<Input>() -> impl Parser<Input, Output = SingleVal>
// where
//     Input: Stream<Token = char>,
// {
//     integer()
// }
fn limit(i: &str) -> Parsed<SingleVal> {
    integer(i)
}

//done
// fn offset<Input>() -> impl Parser<Input, Output = SingleVal>
// where
//     Input: Stream<Token = char>,
// {
//     integer()
//}
fn offset(i: &str) -> Parsed<SingleVal> {
    integer(i)
}

//done
// fn logic_single_value<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = (String, Option<String>)>
// where
//     Input: Stream<Token = char>,
// {
//     let dt = data_type.clone();
//     choice((
//         attempt(quoted_value().skip(not_followed_by(none_of(",)".chars())))),
//         between(char('{'), char('}'), many(none_of("{}".chars()))).map(|v: String| format!("{{{}}}", v)),
//         many(none_of(",)".chars())),
//     ))
//     .map(move |v| (v, dt.clone()))
// }
fn logic_single_value<'a>(data_type: &'a Option<&'a str>, i: &'a str) -> Parsed<'a, SingleVal<'a>> {
    let (input, v) = alt((
        quoted_value_escaped,
        map(recognize(delimited(char('{'), is_not("{}"), char('}'))), Cow::Borrowed),
        map(is_not(",)"), Cow::Borrowed),
    ))(i)?;
    let v = match data_type {
        Some(dt) => SingleVal(v, Some(Cow::Borrowed(*dt))),
        None => SingleVal(v, None),
    };
    Ok((input, v))
}

//done
// fn list_value<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = (Vec<String>, Option<String>)>
// where
//     Input: Stream<Token = char>,
// {
//     let dt = data_type.as_ref().map(|v| format!("Array({})", v)); //TODO!!! this is hardcoded for clickhouse
//     lex(between(lex(char('(')), lex(char(')')), sep_by(list_element(), lex(char(','))))).map(move |v| (v, dt.clone()))
// }
fn list_value<'a>(data_type: &Option<&'a str>, i: &'a str) -> Parsed<'a, ListVal<'a>> {
    let dt = data_type.map(|v| Cow::Owned(format!("Array({v})"))); //TODO!!! this is hardcoded for clickhouse
    let (input, list) = delimited(ws(char('(')), separated_list0(ws(char(',')), list_element), ws(char(')')))(i)?;
    Ok((input, ListVal(list, dt)))
}

//done
// fn list_element<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     attempt(quoted_value().skip(not_followed_by(none_of(",)".chars())))).or(many1(none_of(",)".chars())))
// }
fn list_element(i: &str) -> Parsed<Cow<str>> {
    alt((
        //terminated(quoted_value, peek(none_of(",)"))),
        quoted_value_escaped,
        map(is_not(",)"), Cow::Borrowed),
    ))(i)
}

//done
// fn operator<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     many1(letter()).and_then(|o: String| match OPERATORS.get(o.as_str()) {
//         Some(oo) => Ok(oo.to_string()),
//         None => Err(StreamErrorFor::<Input>::message_static_message("unknown operator")),
//     })
// }
fn operator(i: &str) -> Parsed<&str> {
    map_res(alpha1, |o: &str| match OPERATORS.get(o) {
        Some(&op) => Ok(op),
        None => Err(Err::Error(("unknown operator", ErrorKind::Fail))),
    })(i)
}

//done
// fn fts_operator<Input>() -> impl Parser<Input, Output = String>
// where
//     Input: Stream<Token = char>,
// {
//     many1(letter()).and_then(|o: String| match FTS_OPERATORS.get(o.as_str()) {
//         Some(oo) => Ok(oo.to_string()),
//         None => Err(StreamErrorFor::<Input>::message_static_message("unknown fts operator")),
//     })
// }
fn fts_operator(i: &str) -> Parsed<&str> {
    map_res(alpha1, |o: &str| match FTS_OPERATORS.get(o) {
        Some(&op) => Ok(op),
        None => Err(Err::Error(("unknown fts operator", ErrorKind::Fail))),
    })(i)
}

// fn negatable_filter<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = (bool, Filter)>
// where
//     Input: Stream<Token = char>,
// {
//     optional(attempt(string("not").skip(dot())))
//         .and(filter(data_type))
//         .map(|(n, f)| (n.is_some(), f))
// }
fn negatable_filter<'a>(data_type: &Option<&'a str>, i: &'a str) -> Parsed<'a, (bool, Filter<'a>)> {
    map(tuple((opt(tag("not.")), apply(data_type, filter))), |(n, f)| (n.is_some(), f))(i)
}

//done
//TODO! filter and logic_filter parsers should be combined, they differ only in single_value parser type
// fn filter<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = Filter>
// where
//     Input: Stream<Token = char>,
// {
//     //let value = if use_logical_value { opaque!(logic_single_value()) } else { opaque!(single_value()) };
//
//     choice((
//         attempt(operator().skip(dot()).and(single_value(data_type)).map(move |(o, (v, dt))| match &*o {
//             "like" | "ilike" => Ok(Filter::Op(o, SingleVal(v.replace('*', "%"), dt))),
//             "is" => match &*v {
//                 "null" => Ok(Filter::Is(TrileanVal::TriNull)),
//                 "unknown" => Ok(Filter::Is(TrileanVal::TriUnknown)),
//                 "true" => Ok(Filter::Is(TrileanVal::TriTrue)),
//                 "false" => Ok(Filter::Is(TrileanVal::TriFalse)),
//                 _ => Err(StreamErrorFor::<Input>::message_static_message(
//                     "unknown value for is operator, use null, unknown, true, false",
//                 )),
//             },
//             _ => Ok(Filter::Op(o, SingleVal(v, dt))),
//         })),
//         attempt(
//             string("in")
//                 .skip(dot())
//                 .and(list_value(data_type))
//                 .map(move |(_, (v, dt))| Ok(Filter::In(ListVal(v, dt)))),
//         ),
//         fts_operator()
//             .and(optional(
//                 between(char('('), char(')'), many1(choice((letter(), digit(), char('_'))))).map(|v| SingleVal(v, None)),
//             ))
//             .skip(dot())
//             .and(single_value(data_type))
//             .map(move |((o, l), (v, dt))| Ok(Filter::Fts(o, l, SingleVal(v, dt)))),
//     ))
//     .and_then(|r| r)
// }
// fn filter_common<'a, 'b, P>(p: P, data_type: &'b Option<&'a str>, i: &'a str) -> Parsed<'a, Filter<'a>>
// where P: fn(&'b Option<&'a str>, &'a str) -> Parsed<'a, SingleVal<'a>> +'b
fn filter_common<'a, 'b>(
    p: fn(&'b Option<&'a str>, &'a str) -> Parsed<'a, SingleVal<'a>>, data_type: &'b Option<&'a str>, i: &'a str,
) -> Parsed<'a, Filter<'a>> {
    alt((
        map(tuple((operator, dot, apply(data_type, p))), |(o, _, SingleVal(v, dt))| match o {
            "like" | "ilike" => Filter::Op(o, SingleVal(Cow::Owned(v.replace('*', "%")), dt)),
            "is" => match &*v {
                "null" => Filter::Is(TrileanVal::TriNull),
                "unknown" => Filter::Is(TrileanVal::TriUnknown),
                "true" => Filter::Is(TrileanVal::TriTrue),
                "false" => Filter::Is(TrileanVal::TriFalse),
                _ => panic!("unknown value for is operator, use null, unknown, true, false"),
            },
            _ => Filter::Op(o, SingleVal(v, dt)),
        }),
        map(tuple((tag("in"), char('.'), apply(data_type, list_value))), |(_, _, ListVal(v, dt))| Filter::In(ListVal(v, dt))),
        map(
            tuple((
                fts_operator,
                opt(delimited(ws(char('(')), recognize(many1(alt((alpha1, digit1, tag("_"))))), ws(char(')')))),
                char('.'),
                apply(data_type, p),
            )),
            |(o, l, _, SingleVal(v, dt))| Filter::Fts(o, l.map(|v| SingleVal(Cow::Borrowed(v), None)), SingleVal(v, dt)),
        ),
    ))(i)
}

fn filter<'a>(data_type: &Option<&'a str>, i: &'a str) -> Parsed<'a, Filter<'a>> {
    filter_common(single_value, data_type, i)
}

//done
// fn logic_filter<Input>(data_type: &Option<String>) -> impl Parser<Input, Output = Filter>
// where
//     Input: Stream<Token = char>,
// {
//     //let value = if use_logical_value { opaque!(logic_single_value()) } else { opaque!(single_value()) };
//
//     choice((
//         attempt(operator().skip(dot()).and(logic_single_value(data_type)).map(|(o, (v, dt))| match &*o {
//             "like" | "ilike" => Ok(Filter::Op(o, SingleVal(v.replace('*', "%"), dt))),
//             "is" => match &*v {
//                 "null" => Ok(Filter::Is(TrileanVal::TriNull)),
//                 "unknown" => Ok(Filter::Is(TrileanVal::TriUnknown)),
//                 "true" => Ok(Filter::Is(TrileanVal::TriTrue)),
//                 "false" => Ok(Filter::Is(TrileanVal::TriFalse)),
//                 _ => Err(StreamErrorFor::<Input>::message_static_message(
//                     "unknown value for is operator, use null, unknown, true, false",
//                 )),
//             },
//             _ => Ok(Filter::Op(o, SingleVal(v, dt))),
//         })),
//         attempt(
//             string("in")
//                 .skip(dot())
//                 .and(list_value(data_type))
//                 .map(|(_, (v, dt))| Ok(Filter::In(ListVal(v, dt)))),
//         ),
//         fts_operator()
//             .and(optional(
//                 between(char('('), char(')'), many1(choice((letter(), digit(), char('_'))))).map(|v| SingleVal(v, None)),
//             ))
//             .skip(dot())
//             .and(logic_single_value(data_type))
//             .map(|((o, l), (v, dt))| Ok(Filter::Fts(o, l, SingleVal(v, dt)))),
//     ))
//     .and_then(|v| v)
// }
fn logic_filter<'a>(data_type: &'a Option<&'a str>, i: &'a str) -> Parsed<'a, Filter<'a>> {
    filter_common(logic_single_value, data_type, i)
}

//done
// fn order<Input>() -> impl Parser<Input, Output = Vec<OrderTerm>>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(order_term(), lex(char(','))).skip(eof())
// }
fn order(i: &str) -> Parsed<Vec<OrderTerm>> {
    terminated(
        separated_list1(
            tag(","),
            // ws(map(
            //     tuple((
            //         field,
            //         opt(preceded(
            //             dot,
            //             alt((value(OrderDirection::Asc, tag("asc")), value(OrderDirection::Desc, tag("desc")))),
            //         )),
            //         opt(preceded(
            //             dot,
            //             alt((value(OrderNulls::NullsFirst, tag("nullsfirst")), value(OrderNulls::NullsLast, tag("nullslast")))),
            //         )),
            //     )),
            //     |(term, direction, null_order)| OrderTerm { term, direction, null_order },
            // )),
            ws(order_term),
        ),
        eof,
    )(i)
}

//done
// fn order_term<Input>() -> impl Parser<Input, Output = OrderTerm>
// where
//     Input: Stream<Token = char>,
// {
//     let direction = attempt(
//         dot()
//             .and(
//                 string("asc")
//                     .map(|_| OrderDirection::Asc)
//                     .or(string("desc").map(|_| OrderDirection::Desc)),
//             )
//             .map(|(_, v)| v),
//     );
//     let nulls = dot()
//         .and(attempt(string("nullsfirst").map(|_| OrderNulls::NullsFirst)).or(string("nullslast").map(|_| OrderNulls::NullsLast)))
//         .map(|(_, v)| v);
//     field()
//         .and(optional(direction).and(optional(nulls)))
//         .map(|(term, (direction, null_order))| OrderTerm { term, direction, null_order })
// }
fn order_term(i: &str) -> Parsed<OrderTerm> {
    map(
        tuple((
            field,
            opt(preceded(dot, alt((value(OrderDirection::Asc, tag("asc")), value(OrderDirection::Desc, tag("desc")))))),
            opt(preceded(dot, alt((value(OrderNulls::NullsFirst, tag("nullsfirst")), value(OrderNulls::NullsLast, tag("nullslast")))))),
        )),
        |(term, direction, null_order)| OrderTerm { term, direction, null_order },
    )(i)
}

//done
// fn groupby<Input>() -> impl Parser<Input, Output = Vec<GroupByTerm>>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(groupby_term(), lex(char(','))).skip(eof())
// }
fn groupby(i: &str) -> Parsed<Vec<GroupByTerm>> {
    terminated(separated_list1(tag(","), map(ws(field), GroupByTerm)), eof)(i)
}

//done
// fn groupby_term<Input>() -> impl Parser<Input, Output = GroupByTerm>
// where
//     Input: Stream<Token = char>,
// {
//     field().map(GroupByTerm)
// }

//done
// fn content_type<Input>() -> impl Parser<Input, Output = ContentType>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(many1(none_of(",".chars())), char(',')).and_then(|v: Vec<String>| {
//         let vv = v
//             .iter()
//             .map(|t| {
//                 let tt = t.trim().split(';').collect::<Vec<_>>();
//                 match tt.first() {
//                     Some(&"*/*") => ApplicationJSON,
//                     Some(&"application/json") => ApplicationJSON,
//                     Some(&"application/vnd.pgrst.object") => SingularJSON,
//                     Some(&"application/vnd.pgrst.object+json") => SingularJSON,
//                     Some(&"text/csv") => TextCSV,
//                     Some(o) => Other(o.to_string()),
//                     None => Other(t.to_string()),
//                 }
//             })
//             // remove unknown content types
//             .filter(|t| !matches!(t, Other(_)))
//             .collect::<Vec<_>>();
//         match vv.first() {
//             Some(ct) => Ok(ct.clone()),
//             None => Err(StreamErrorFor::<Input>::message_static_message("unknown operator")),
//         }
//     })
//     // choice((
//     //     string("*/*").map(|_| ApplicationJSON),
//     //     attempt(string("application/json")).map(|_| ApplicationJSON),
//     //     attempt(string("application/vnd.pgrst.object")).map(|_| SingularJSON),
//     //     attempt(string("application/vnd.pgrst.object+json")).map(|_| SingularJSON),
//     //     string("text/csv").map(|_| TextCSV),
//     // ))
// }

fn content_type(i: &str) -> Parsed<ContentType> {
    map_res(
        separated_list1(
            tag(","),
            map(is_not(","), |t: &str| {
                let tt = t.trim().split(';').collect::<Vec<_>>();
                match tt.first() {
                    Some(&"*/*") => ApplicationJSON,
                    Some(&"application/json") => ApplicationJSON,
                    Some(&"application/vnd.pgrst.object") => SingularJSON,
                    Some(&"application/vnd.pgrst.object+json") => SingularJSON,
                    Some(&"text/csv") => TextCSV,
                    Some(o) => Other(o.to_string()),
                    None => Other(t.to_string()),
                }
            }),
        ),
        |v: Vec<ContentType>| {
            let vv = v
                // remove unknown content types
                .into_iter()
                .filter(|t| !matches!(t, Other(_)))
                .collect::<Vec<_>>();
            match vv.first() {
                Some(ct) => Ok(ct.clone()),
                None => Err("unknown content type"),
            }
        },
    )(i)
}

//done
// fn preferences<Input>() -> impl Parser<Input, Output = Preferences>
// where
//     Input: Stream<Token = char>,
// {
//     sep_by1(
//         choice((
//             attempt(string("return=").and(choice((string("representation"), string("minimal"), string("headers-only"))))),
//             attempt(string("count=").and(choice((string("exact"), string("planned"), string("estimated"))))),
//             attempt(string("resolution=").and(choice((string("merge-duplicates("), string(")ignore-duplicates"))))),
//         )),
//         lex(char(',')),
//     )
//     .map(|v: Vec<(&str, &str)>| {
//         let m = v.into_iter().collect::<HashMap<_, _>>();
//         Preferences {
//             resolution: match m.get("resolution=") {
//                 Some(r) => match *r {
//                     "merge-duplicates" => Some(Resolution::MergeDuplicates),
//                     "ignore-duplicates" => Some(Resolution::IgnoreDuplicates),
//                     _ => None,
//                 },
//                 None => None,
//             },
//             representation: match m.get("return=") {
//                 Some(r) => match *r {
//                     "representation" => Some(Representation::Full),
//                     "minimal" => Some(Representation::None),
//                     "headers-only" => Some(Representation::HeadersOnly),
//                     _ => None,
//                 },
//                 None => None,
//             },
//             count: match m.get("count=") {
//                 Some(r) => match *r {
//                     "exact" => Some(Count::ExactCount),
//                     "planned" => Some(Count::PlannedCount),
//                     "estimated" => Some(Count::EstimatedCount),
//                     _ => None,
//                 },
//                 None => None,
//             },
//         }
//     })
// }

fn preferences(i: &str) -> Parsed<Preferences> {
    map_opt(
        separated_list1(
            tag(","),
            map_res(is_not(","), |t: &str| {
                let tt = t.trim().split('=').map(|s| s.trim()).collect::<Vec<_>>();
                match tt.as_slice() {
                    ["resolution", s] => Ok(("resolution", *s)),
                    ["return", s] => Ok(("return", *s)),
                    ["count", s] => Ok(("count", *s)),
                    _ => Err("unknown preference"),
                }
            }),
        ),
        |v: Vec<(&str, _)>| {
            let m = v.into_iter().collect::<HashMap<_, _>>();
            Some(Preferences {
                resolution: match m.get("resolution") {
                    Some(r) => match *r {
                        "merge-duplicates" => Some(Resolution::MergeDuplicates),
                        "ignore-duplicates" => Some(Resolution::IgnoreDuplicates),
                        _ => None,
                    },
                    None => None,
                },
                representation: match m.get("return") {
                    Some(r) => match *r {
                        "representation" => Some(Representation::Full),
                        "minimal" => Some(Representation::None),
                        "headers-only" => Some(Representation::HeadersOnly),
                        _ => None,
                    },
                    None => None,
                },
                count: match m.get("count") {
                    Some(r) => match *r {
                        "exact" => Some(Count::ExactCount),
                        "planned" => Some(Count::PlannedCount),
                        "estimated" => Some(Count::EstimatedCount),
                        _ => None,
                    },
                    None => None,
                },
            })
        },
    )(i)
}

//done
// fn logic_condition<Input>() -> impl Parser<Input, Output = Condition>
// where
//     Input: Stream<Token = char>,
// {
//     logic_condition_()
// }
// parser! {
//     #[inline]
//     fn logic_condition_[Input]()(Input) -> Condition
//     where [ Input: Stream<Token = char> ]
//     {
//         let single = field().skip(dot())
//             .and(optional(attempt(string("not").skip(dot()))))
//             .and(logic_filter(&None))
//             .map(|((field,negate),filter)|
//                 Condition::Single {field,filter,negate: negate.is_some()}
//             );
//
//         let group = optional(attempt(string("not").skip(dot())))
//             .and(
//                 lex(choice((string("and"),string("or")))).map(|l|
//                     match l {
//                         "and" => And,
//                         "or" => Or,
//                         x => panic!("unknown logic operator {}", x)
//                     }
//                 )
//                 .and(between(lex(char('(')),lex(char(')')),sep_by1(logic_condition(), lex(char(',')))))
//             )
//             .map(|(negate, (operator, conditions))|{
//                 Condition::Group{ negate: negate.is_some(), tree: ConditionTree { operator, conditions,}}
//             });
//
//         attempt(single).or(group)
//     }
// }

fn logic_condition<'a, 'b>(n: Option<&'b bool>, lo: Option<&'b LogicOperator>, i: &'a str) -> Parsed<'a, Condition<'a>> {
    match (n, lo) {
        (Some(negate), Some(operator)) => {
            let (i, conditions) = delimited(ws(char('(')), separated_list1(ws(char(',')), |ii| logic_condition(None, None, ii)), ws(char(')')))(i)?;
            Ok((
                i,
                Condition::Group {
                    negate: *negate,
                    tree: ConditionTree {
                        operator: operator.clone(),
                        conditions,
                    },
                },
            ))
        }
        _ => alt((
            //single
            ws(map(tuple((field, char('.'), opt(tag("not.")), |ii| logic_filter(&None, ii))), |(field, _, negate, filter)| Condition::Single {
                field,
                filter,
                negate: negate.is_some(),
            })),
            //group
            map(
                tuple((
                    opt(tag("not.")),
                    alt((tag("and"), tag("or"))),
                    delimited(ws(char('(')), separated_list1(ws(char(',')), |ii| logic_condition(None, None, ii)), ws(char(')'))),
                )),
                |(negate, operator, conditions)| Condition::Group {
                    negate: negate.is_some(),
                    tree: ConditionTree {
                        operator: match operator {
                            "and" => LogicOperator::And,
                            "or" => LogicOperator::Or,
                            _ => unreachable!("unknown logic operator {}", operator),
                        },
                        conditions,
                    },
                },
            ),
        ))(i),
    }
}

// helper functions
fn split_select(select: Vec<SelectKind>) -> (Vec<SelectItem>, Vec<SubSelect>) {
    let mut sel = vec![];
    let mut sub_sel = vec![];
    for i in select {
        match i {
            Item(s) => sel.push(s),
            Sub(s) => sub_sel.push(*s),
        }
    }
    (sel, sub_sel)
}

fn is_self_join(join: &Join) -> bool {
    match join {
        Parent(fk) => fk.table == fk.referenced_table,
        Many(_, _, _) => false,
        Child(fk) => fk.table == fk.referenced_table,
    }
}

fn add_join_info<'a, 'b>(query: &'b mut Query<'a>, schema: &'a str, db_schema: &'a DbSchema<'a>, depth: u16) -> Result<()> {
    let dummy_source = "subzero_source";
    let (parent_table, returning, select): (&'a str, Option<&'b mut Vec<&'a str>>, &'b mut Vec<SelectItem<'a>>) = match &mut query.node {
        Select {
            from: (table, _), select, ..
        } => (*table, None, select),
        Insert { into, returning, select, .. } => (*into, Some(returning), select),
        Delete { from, returning, select, .. } => (*from, Some(returning), select),
        Update {
            table, returning, select, ..
        } => (*table, Some(returning), select),
        FunctionCall {
            return_table_type,
            returning,
            select,
            ..
        } => match return_table_type {
            Some(q) => (q.1, Some(returning), select),
            None => (dummy_source, Some(returning), select),
        },
    };

    for SubSelect {
        query: q, join, hint, alias, ..
    } in query.sub_selects.iter_mut()
    {
        if let Select {
            from: (child_table, table_alias),
            ..
        } = &mut q.node
        {
            //let al = format!("{}_{}", child_table, depth);
            if depth > 9 {
                return Err(Error::ParseRequestError {
                    message: "Maximum depth of 10 exceeded. Please check your query for circular references.".to_string(),
                    details: String::new(),
                });
            }
            let new_join: Join<'a> = db_schema.get_join(schema, parent_table, child_table, hint)?;
            if is_self_join(&new_join) {
                *table_alias = Some(ALIAS_SUFIXES[depth as usize]);
            }
            match &new_join {
                Parent(fk) if &fk.referenced_table.1 != child_table => {
                    if alias.is_none() {
                        *alias = Some(child_table);
                    }
                    *child_table = fk.referenced_table.1;
                }
                _ => {}
            }
            *join = Some(new_join);
            add_join_info(q, schema, db_schema, depth + 1)?;
        }
    }

    if let Some(r) = returning {
        r.extend(get_returning(select, &query.sub_selects)?);
    }

    Ok(())
}

fn insert_join_conditions<'a, 'b>(query: &'b mut Query<'a>, schema: &'a str) -> Result<()> {
    let subzero_source = "subzero_source";

    let (parent_qi_1, parent_qi_2): (&'a str, &'a str) = match &query.node {
        Select {
            from: (table, table_alias), ..
        } => match table_alias {
            Some(a) => ("", a),
            None => (schema, *table),
        },
        Insert { .. } => ("", subzero_source),
        Update { .. } => ("", subzero_source),
        Delete { .. } => ("", subzero_source),
        FunctionCall { .. } => ("", subzero_source),
    };
    for SubSelect { query: q, join, .. } in query.sub_selects.iter_mut() {
        if let (Select { join_tables, .. }, Some(join)) = (&mut q.node, join) {
            if let Many(join_table, _, _) = &join {
                join_tables.push(join_table.1);
            }

            let conditions = match join {
                Parent(fk) => zip(&fk.columns, &fk.referenced_columns)
                    .map(|(col, ref_col)| {
                        (
                            vec![],
                            Single {
                                field: Field {
                                    name: ref_col,
                                    json_path: None,
                                },
                                filter: Col(Qi(parent_qi_1, parent_qi_2), Field { name: col, json_path: None }),
                                negate: false,
                            },
                        )
                    })
                    .collect(),
                Child(fk) => zip(&fk.columns, &fk.referenced_columns)
                    .map(|(col, ref_col)| {
                        (
                            vec![],
                            Single {
                                field: Field { name: col, json_path: None },
                                filter: Col(
                                    Qi(parent_qi_1, parent_qi_2),
                                    Field {
                                        name: ref_col,
                                        json_path: None,
                                    },
                                ),
                                negate: false,
                            },
                        )
                    })
                    .collect(),
                Many(join_table, fk1, fk2) => {
                    //fk1 is for origin table
                    zip(&fk1.columns, &fk1.referenced_columns)
                        .map(|(col, ref_col)| {
                            (
                                vec![],
                                Foreign {
                                    left: (
                                        Qi(parent_qi_1, parent_qi_2),
                                        Field {
                                            name: ref_col,
                                            json_path: None,
                                        },
                                    ),
                                    right: (Qi(join_table.0, join_table.1), Field { name: col, json_path: None }),
                                },
                            )
                        })
                        .chain(
                            //fk2 is for target table
                            zip(&fk2.columns, &fk2.referenced_columns).map(|(col, ref_col)| {
                                (
                                    vec![],
                                    Single {
                                        field: Field {
                                            name: ref_col,
                                            json_path: None,
                                        },
                                        filter: Col(Qi(join_table.0, join_table.1), Field { name: col, json_path: None }),
                                        negate: false,
                                    },
                                )
                            }),
                        )
                        .collect()
                }
            };
            q.insert_conditions(conditions)?;
            insert_join_conditions(q, schema)?;
        }
    }
    Ok(())
}

fn is_logical(s: &str) -> bool {
    s == "and" || s == "or" || s.ends_with(".or") || s.ends_with(".and")
}

fn is_limit(s: &str) -> bool {
    s == "limit" || s.ends_with(".limit")
}

fn is_offset(s: &str) -> bool {
    s == "offset" || s.ends_with(".offset")
}

fn is_order(s: &str) -> bool {
    s == "order" || s.ends_with(".order")
}

fn has_operator(s: &str) -> bool {
    OPERATORS_START.iter().map(|op| s.starts_with(op)).any(|b| b)
}

// fn to_app_error<'a>(s: &'a str) -> impl Fn(ParseError<&'a str>) -> Error {
//     move |mut e| {
//         let m = e.errors.drain_filter(|v| matches!(v, ParserError::Message(_))).collect::<Vec<_>>();
//         let position = e.position.translate_position(s);
//         let message = match m.as_slice() {
//             [ParserError::Message(Info::Static(s))] => s,
//             _ => "",
//         };
//         let message = format!("\"{} ({})\" (line 1, column {})", message, s, position + 1);
//         let details = format!("{}", e)
//             .replace(format!("Parse error at {}", e.position).as_str(), "")
//             .replace('\n', " ")
//             .trim()
//             .to_string();
//         Error::ParseRequestError { message, details }
//     }
// }

fn to_app_error(s: &str, e: nom::Err<nom::error::VerboseError<&str>>) -> Error {
    // let m = e.errors.drain_filter(|v| matches!(v, ParserError::Message(_))).collect::<Vec<_>>();
    // let position = e.position.translate_position(s);
    // let message = match m.as_slice() {
    //     [ParserError::Message(Info::Static(s))] => s,
    //     _ => "",
    // };
    // let message = format!("\"{} ({})\" (line 1, column {})", message, s, position + 1);
    // let details = format!("{}", e)
    //     .replace(format!("Parse error at {}", e.position).as_str(), "")
    //     .replace('\n', " ")
    //     .trim()
    //     .to_string();
    match e {
        nom::Err::Error(_e) | nom::Err::Failure(_e) => {
            //println!("Raw error:\n{:?}", &_e);
            let m = _e
                .errors
                .iter()
                .filter(|(_, v)| matches!(v, VerboseErrorKind::Context(_)))
                .collect::<Vec<_>>();
            let message = match m.as_slice() {
                [(_, VerboseErrorKind::Context(s))] => s,
                _ => "",
            };
            let (offsets, details) = convert_error(s, _e);
            let position = offsets.first().unwrap_or(&0usize);
            let message = format!("\"{message} ({s})\" (line 1, column {position})");
            //let details = details.replace('\n', " ").trim().to_string();
            //println!("Parse error:\n{}", details);

            Error::ParseRequestError { message, details }
        }
        nom::Err::Incomplete(_e) => {
            let message = "parse error".to_string();
            let details = format!("{_e:?}");
            Error::ParseRequestError { message, details }
        }
    }
}

//fn get_returning(select: &Vec<SelectKind>) -> Result<Vec<String>> {
fn get_returning<'a>(selects: &[SelectItem<'a>], sub_selects: &[SubSelect<'a>]) -> Result<Vec<&'a str>> {
    let returning = selects
        .iter()
        .map(|s| match s {
            Simple { field, .. } => Ok(vec![field.name]),
            Star => Ok(vec![STAR]),
            Func { .. } => Ok(vec![]),
        })
        .chain(sub_selects.iter().map(|s| match s {
            SubSelect { join: Some(j), .. } => match j {
                Child(fk) => Ok(fk.referenced_columns.clone()),
                Parent(fk) => Ok(fk.columns.clone()),
                Many(_, fk1, fk2) => {
                    let mut f = vec![];
                    f.extend(fk1.referenced_columns.iter());
                    f.extend(fk2.referenced_columns.iter());
                    Ok(f)
                }
            },
            x => Err(Error::NoRelBetween {
                origin: "table".to_string(),
                target: format!("{x:?}"),
            }),
        }))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(returning)
}

#[cfg(test)]
pub mod tests {
    //use std::matches;
    use crate::api::{
        Condition::{Group, Single},
        JsonOperand::*,
        JsonOperation::*,
    };
    //use combine::easy::{Error, Errors};
    //use combine::stream::PointerOffset;
    use pretty_assertions::{assert_eq, assert_ne};

    //use combine::stream::position;
    //use combine::stream::position::SourcePosition;
    //use combine::error::StringStreamError;
    use super::*;
    use crate::error::Error as AppError;
    //use combine::EasyParser;

    pub static JSON_SCHEMA: &str = r#"
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

    fn s(s: &str) -> String {
        s.to_string()
    }
    fn cow(s: &str) -> Cow<str> {
        Cow::Borrowed(s)
    }
    fn sv(s: &str) -> SingleVal {
        SingleVal(cow(s), None)
    }
    // fn vs(v: Vec<(&str, &str)>) -> Vec<(String, String)> {
    //     v.into_iter().map(|(s, s2)| (s.to_string(), s2.to_string())).collect()
    // }
    #[test]
    fn test_parse_get_function() {
        let emtpy_hashmap: HashMap<&str, &str> = HashMap::new();
        let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        let mut api_request = ApiRequest {
            schema_name: "api",
            get: vec![("id", "10")],
            preferences: None,
            path: "dummy",
            method: "GET",
            read_only: true,
            headers: emtpy_hashmap.clone(),
            accept_content_type: ApplicationJSON,
            cookies: emtpy_hashmap.clone(),
            query: Query {
                node: FunctionCall {
                    fn_name: Qi("api", "myfunction"),
                    parameters: CallParams::KeyParams(vec![ProcParam {
                        name: "id",
                        type_: "integer",
                        required: true,
                        variadic: false,
                    }]),
                    payload: Payload(cow(r#"{"id":"10"}"#), None),
                    is_scalar: true,
                    returns_single: true,
                    is_multiple_call: false,
                    returning: vec!["*"],
                    select: vec![Star],
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![],
                    },
                    return_table_type: None,
                    limit: None,
                    offset: None,
                    order: vec![],
                },
                sub_selects: vec![],
            },
        };
        let a = parse("api", "myfunction", &db_schema, "GET", "dummy", vec![("id", "10")], None, emtpy_hashmap.clone(), emtpy_hashmap.clone(), None);

        assert_eq!(a.unwrap(), api_request);

        api_request.method = "POST";
        api_request.get = vec![];
        api_request.read_only = false;

        let body = r#"{"id":"10"}"#;
        let b = parse("api", "myfunction", &db_schema, "POST", "dummy", vec![], Some(body), emtpy_hashmap.clone(), emtpy_hashmap.clone(), None);
        assert_eq!(b.unwrap(), api_request);
    }

    #[test]
    fn test_insert_conditions() {
        let mut query = Query {
            node: Select {
                groupby: vec![],
                order: vec![],
                limit: None,
                offset: None,
                select: vec![Simple {
                    field: Field { name: "a", json_path: None },
                    alias: None,
                    cast: None,
                }],
                from: ("parent", None),
                join_tables: vec![],
                //from_alias: None,
                where_: ConditionTree {
                    operator: And,
                    conditions: vec![],
                },
                check: None,
            },
            sub_selects: vec![SubSelect {
                query: Query {
                    node: Select {
                        order: vec![],
                        groupby: vec![],
                        limit: None,
                        offset: None,
                        select: vec![Simple {
                            field: Field { name: "a", json_path: None },
                            alias: None,
                            cast: None,
                        }],
                        from: ("child", None),
                        join_tables: vec![],
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![],
                        },
                        check: None,
                    },
                    sub_selects: vec![],
                },
                alias: None,
                hint: None,
                join: None,
            }],
        };
        let condition = Single {
            field: Field { name: "a", json_path: None },
            filter: Filter::Op(">=", SingleVal(cow("5"), None)),
            negate: false,
        };
        let _ = query.insert_conditions(vec![(vec![], condition.clone()), (vec!["child"], condition.clone())]);
        assert_eq!(
            query,
            Query {
                node: Select {
                    order: vec![],
                    groupby: vec![],
                    limit: None,
                    offset: None,
                    select: vec![Simple {
                        field: Field { name: "a", json_path: None },
                        alias: None,
                        cast: None
                    },],
                    from: ("parent", None),
                    join_tables: vec![],
                    where_: ConditionTree {
                        operator: And,
                        conditions: vec![condition.clone()]
                    },
                    check: None
                },
                sub_selects: vec![SubSelect {
                    query: Query {
                        node: Select {
                            order: vec![],
                            groupby: vec![],
                            limit: None,
                            offset: None,
                            select: vec![Simple {
                                field: Field { name: "a", json_path: None },
                                alias: None,
                                cast: None
                            },],
                            from: ("child", None),
                            join_tables: vec![],
                            //from_alias: None,
                            where_: ConditionTree {
                                operator: And,
                                conditions: vec![condition]
                            },
                            check: None
                        },
                        sub_selects: vec![]
                    },
                    alias: None,
                    hint: None,
                    join: None
                }]
            }
        );
    }

    #[test]
    fn test_parse_get() {
        let emtpy_hashmap: HashMap<&str, &str> = HashMap::new();
        let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        let a = parse(
            "api",
            "projects",
            &db_schema,
            "GET",
            "dummy",
            vec![
                ("select", "id,name,clients(id),tasks(id)"),
                ("id", "not.gt.10"),
                ("tasks.id", "lt.500"),
                ("not.or", "(id.eq.11,id.eq.12)"),
                ("tasks.or", "(id.eq.11,id.eq.12)"),
            ],
            None,
            emtpy_hashmap.clone(),
            emtpy_hashmap.clone(),
            None,
        );

        assert_eq!(
            a.unwrap(),
            ApiRequest {
                schema_name: "api",
                get: vec![
                    ("select", "id,name,clients(id),tasks(id)"),
                    ("id", "not.gt.10"),
                    ("tasks.id", "lt.500"),
                    ("not.or", "(id.eq.11,id.eq.12)"),
                    ("tasks.or", "(id.eq.11,id.eq.12)"),
                ],
                preferences: None,
                path: "dummy",
                method: "GET",
                read_only: true,
                accept_content_type: ApplicationJSON,
                headers: emtpy_hashmap.clone(),
                cookies: emtpy_hashmap.clone(),
                query: Query {
                    node: Select {
                        order: vec![],
                        groupby: vec![],
                        limit: None,
                        offset: None,
                        select: vec![
                            Simple {
                                field: Field { name: "id", json_path: None },
                                alias: None,
                                cast: None
                            },
                            Simple {
                                field: Field {
                                    name: "name",
                                    json_path: None
                                },
                                alias: None,
                                cast: None
                            },
                        ],
                        from: ("projects", None),
                        join_tables: vec![],
                        //from_alias: None,
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![
                                Single {
                                    field: Field { name: "id", json_path: None },
                                    filter: Filter::Op(">", SingleVal(cow("10"), Some(cow("int")))),
                                    negate: true,
                                },
                                Group {
                                    negate: true,
                                    tree: ConditionTree {
                                        operator: Or,
                                        conditions: vec![
                                            Single {
                                                filter: Filter::Op("=", SingleVal(cow("11"), None)),
                                                field: Field { name: "id", json_path: None },
                                                negate: false
                                            },
                                            Single {
                                                filter: Filter::Op("=", SingleVal(cow("12"), None)),
                                                field: Field { name: "id", json_path: None },
                                                negate: false
                                            }
                                        ]
                                    }
                                }
                            ]
                        },
                        check: None
                    },
                    sub_selects: vec![
                        SubSelect {
                            query: Query {
                                sub_selects: vec![],
                                node: Select {
                                    check: None,
                                    order: vec![],
                                    groupby: vec![],
                                    limit: None,
                                    offset: None,
                                    select: vec![Simple {
                                        field: Field { name: "id", json_path: None },
                                        alias: None,
                                        cast: None
                                    },],
                                    from: ("clients", None),
                                    join_tables: vec![],
                                    //from_alias: None,
                                    where_: ConditionTree {
                                        operator: And,
                                        conditions: vec![Single {
                                            field: Field { name: "id", json_path: None },
                                            filter: Filter::Col(
                                                Qi("api", "projects"),
                                                Field {
                                                    name: "client_id",
                                                    json_path: None
                                                }
                                            ),
                                            negate: false,
                                        }]
                                    }
                                }
                            },
                            alias: None,
                            hint: None,
                            join: Some(Parent(ForeignKey {
                                name: "client_id_fk",
                                table: Qi("api", "projects"),
                                columns: vec!["client_id"],
                                referenced_table: Qi("api", "clients"),
                                referenced_columns: vec!["id"],
                            }),)
                        },
                        SubSelect {
                            query: Query {
                                sub_selects: vec![],
                                node: Select {
                                    check: None,
                                    order: vec![],
                                    groupby: vec![],
                                    limit: None,
                                    offset: None,
                                    select: vec![Simple {
                                        field: Field { name: "id", json_path: None },
                                        alias: None,
                                        cast: None
                                    },],
                                    from: ("tasks", None),
                                    join_tables: vec![],
                                    //from_alias: None,
                                    where_: ConditionTree {
                                        operator: And,
                                        conditions: vec![
                                            Single {
                                                field: Field {
                                                    name: "project_id",
                                                    json_path: None
                                                },
                                                filter: Filter::Col(Qi("api", "projects"), Field { name: "id", json_path: None }),
                                                negate: false,
                                            },
                                            Single {
                                                field: Field { name: "id", json_path: None },
                                                filter: Filter::Op("<", SingleVal(cow("500"), Some(cow("int")))),
                                                negate: false,
                                            },
                                            Group {
                                                negate: false,
                                                tree: ConditionTree {
                                                    operator: Or,
                                                    conditions: vec![
                                                        Single {
                                                            filter: Filter::Op("=", SingleVal(cow("11"), None)),
                                                            field: Field { name: "id", json_path: None },
                                                            negate: false
                                                        },
                                                        Single {
                                                            filter: Filter::Op("=", SingleVal(cow("12"), None)),
                                                            field: Field { name: "id", json_path: None },
                                                            negate: false
                                                        }
                                                    ]
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            hint: None,
                            alias: None,
                            join: Some(Child(ForeignKey {
                                name: "project_id_fk",
                                table: Qi("api", "tasks"),
                                columns: vec!["project_id"],
                                referenced_table: Qi("api", "projects"),
                                referenced_columns: vec!["id"],
                            }),)
                        }
                    ]
                }
            }
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "GET",
                "dummy",
                vec![("select", "id,name,unknown(id)")],
                None,
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::NoRelBetween {
                origin: s("projects"),
                target: s("unknown")
            })
            .map_err(|e| format!("{e}"))
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "GET",
                "dummy",
                vec![("select", "id-,na$me")],
                None,
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::ParseRequestError {
                message: s("\"failed to parse select parameter (id-,na$me)\" (line 1, column 3)"),
                //details: s("Unexpected `,` Unexpected `i` Expected letter, digit, `_` or ` `")
                details: s("0: at line 1, in Eof:\nid-,na$me\n  ^\n\n1: at line 1, in failed to parse select parameter:\nid-,na$me\n^\n\n")
            })
            .map_err(|e| format!("{e}"))
        );
    }

    #[test]
    fn test_parse_post() {
        let emtpy_hashmap: HashMap<&str, &str> = HashMap::new();
        let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        let headers = [("prefer", "return=representation")].iter().cloned().collect::<HashMap<_, _>>();
        let payload = r#"{"id":10, "name":"john"}"#;
        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id"), ("id", "gt.10"),],
                Some(payload),
                headers.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Ok(ApiRequest {
                schema_name: "api",
                get: vec![("select", "id"), ("id", "gt.10"),],
                preferences: Some(Preferences {
                    representation: Some(Representation::Full),
                    resolution: None,
                    count: None
                }),
                path: "dummy",
                method: "POST",
                read_only: false,
                accept_content_type: ApplicationJSON,
                headers: headers.clone(),
                cookies: emtpy_hashmap.clone(),
                query: Query {
                    node: Insert {
                        on_conflict: None,
                        select: vec![Simple {
                            field: Field { name: "id", json_path: None },
                            alias: None,
                            cast: None
                        },],
                        payload: Payload(cow(payload), None),
                        into: "projects",
                        columns: vec!["id", "name"],
                        check: ConditionTree {
                            operator: And,
                            conditions: vec![]
                        },
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![Single {
                                field: Field { name: "id", json_path: None },
                                filter: Filter::Op(">", SingleVal(cow("10"), Some(cow("int")))),
                                negate: false,
                            }]
                        },
                        returning: vec!["id"]
                    },
                    sub_selects: vec![]
                }
            })
        );
        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id,name"), ("id", "gt.10"), ("columns", "id,name"),],
                Some(payload),
                headers.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Ok(ApiRequest {
                schema_name: "api",
                get: vec![("select", "id,name"), ("id", "gt.10"), ("columns", "id,name"),],
                preferences: Some(Preferences {
                    representation: Some(Representation::Full),
                    resolution: None,
                    count: None
                }),
                path: "dummy",
                method: "POST",
                read_only: false,
                accept_content_type: ApplicationJSON,
                headers: headers.clone(),
                cookies: emtpy_hashmap.clone(),
                query: Query {
                    node: Insert {
                        on_conflict: None,
                        select: vec![
                            Simple {
                                field: Field { name: "id", json_path: None },
                                alias: None,
                                cast: None
                            },
                            Simple {
                                field: Field {
                                    name: "name",
                                    json_path: None
                                },
                                alias: None,
                                cast: None
                            },
                        ],
                        payload: Payload(cow(payload), None),
                        into: "projects",
                        columns: vec!["id", "name"],
                        check: ConditionTree {
                            operator: And,
                            conditions: vec![]
                        },
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![Single {
                                field: Field { name: "id", json_path: None },
                                filter: Filter::Op(">", SingleVal(cow("10"), Some(cow("int")))),
                                negate: false,
                            }]
                        },
                        returning: vec!["id", "name",]
                    },
                    sub_selects: vec![]
                }
            })
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id"), ("id", "gt.10"), ("columns", "id,1$name"),],
                Some(r#"{"id":10, "name":"john", "phone":"123"}"#),
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::ParseRequestError {
                message: s("\"failed to parse columns parameter (id,1$name)\" (line 1, column 5)"),
                //details: s("Unexpected `$` Expected `,`, whitespaces or end of input"),
                details: s("0: at line 1, in Eof:\nid,1$name\n    ^\n\n1: at line 1, in failed to parse columns parameter:\nid,1$name\n^\n\n")
            })
            .map_err(|e| format!("{e}"))
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id"), ("id", "gt.10"),],
                Some(r#"{"id":10, "name""#),
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err("Failed to deserialize json: EOF while parsing an object at line 1 column 16".to_string())
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id"), ("id", "gt.10"),],
                Some(r#"[{"id":10, "name":"john"},{"id":10, "phone":"123"}]"#),
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::InvalidBody {
                message: s("All object keys must match"),
            })
            .map_err(|e| format!("{e}"))
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "GET",
                "dummy",
                vec![("select", "id,name,unknown(id)")],
                None,
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::NoRelBetween {
                origin: s("projects"),
                target: s("unknown")
            })
            .map_err(|e| format!("{e}"))
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "GET",
                "dummy",
                vec![("select", "id-,na$me")],
                None,
                emtpy_hashmap.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Err(AppError::ParseRequestError {
                message: s("\"failed to parse select parameter (id-,na$me)\" (line 1, column 3)"),
                //details: s("Unexpected `,` Unexpected `i` Expected letter, digit, `_` or ` `")
                details: s("0: at line 1, in Eof:\nid-,na$me\n  ^\n\n1: at line 1, in failed to parse select parameter:\nid-,na$me\n^\n\n")
            })
            .map_err(|e| format!("{e}"))
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id"), ("id", "gt.10"),],
                Some(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#),
                headers.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Ok(ApiRequest {
                schema_name: "api",
                get: vec![("select", "id"), ("id", "gt.10"),],
                preferences: Some(Preferences {
                    representation: Some(Representation::Full),
                    resolution: None,
                    count: None
                }),
                path: "dummy",
                method: "POST",
                read_only: false,
                accept_content_type: ApplicationJSON,
                headers: headers.clone(),
                cookies: emtpy_hashmap.clone(),
                query: Query {
                    sub_selects: vec![],
                    node: Insert {
                        on_conflict: None,
                        select: vec![Simple {
                            field: Field { name: "id", json_path: None },
                            alias: None,
                            cast: None
                        },],
                        payload: Payload(cow(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#), None),
                        into: "projects",
                        columns: vec!["id", "name"],
                        check: ConditionTree {
                            operator: And,
                            conditions: vec![]
                        },
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![Single {
                                field: Field { name: "id", json_path: None },
                                filter: Filter::Op(">", SingleVal(cow("10"), Some(cow("int")))),
                                negate: false,
                            }]
                        },
                        returning: vec!["id"]
                    }
                }
            })
        );

        assert_eq!(
            parse(
                "api",
                "projects",
                &db_schema,
                "POST",
                "dummy",
                vec![("select", "id,name,tasks(id),clients(id)"), ("id", "gt.10"), ("tasks.id", "gt.20"),],
                Some(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#),
                headers.clone(),
                emtpy_hashmap.clone(),
                None
            )
            .map_err(|e| format!("{e}")),
            Ok(ApiRequest {
                schema_name: "api",
                get: vec![("select", "id,name,tasks(id),clients(id)"), ("id", "gt.10"), ("tasks.id", "gt.20"),],
                preferences: Some(Preferences {
                    representation: Some(Representation::Full),
                    resolution: None,
                    count: None
                }),
                path: "dummy",
                method: "POST",
                read_only: false,
                accept_content_type: ApplicationJSON,
                headers,
                cookies: emtpy_hashmap.clone(),
                query: Query {
                    sub_selects: vec![
                        SubSelect {
                            query: Query {
                                sub_selects: vec![],
                                node: Select {
                                    check: None,
                                    order: vec![],
                                    groupby: vec![],
                                    limit: None,
                                    offset: None,
                                    select: vec![Simple {
                                        field: Field { name: "id", json_path: None },
                                        alias: None,
                                        cast: None
                                    },],
                                    from: ("tasks", None),
                                    join_tables: vec![],
                                    //from_alias: None,
                                    where_: ConditionTree {
                                        operator: And,
                                        conditions: vec![
                                            Single {
                                                field: Field {
                                                    name: "project_id",
                                                    json_path: None
                                                },
                                                filter: Filter::Col(Qi("", "subzero_source"), Field { name: "id", json_path: None }),
                                                negate: false,
                                            },
                                            Single {
                                                field: Field { name: "id", json_path: None },
                                                filter: Filter::Op(">", SingleVal(cow("20"), Some(cow("int")))),
                                                negate: false,
                                            }
                                        ]
                                    }
                                }
                            },
                            hint: None,
                            alias: None,
                            join: Some(Child(ForeignKey {
                                name: "project_id_fk",
                                table: Qi("api", "tasks"),
                                columns: vec!["project_id"],
                                referenced_table: Qi("api", "projects"),
                                referenced_columns: vec!["id"],
                            }),)
                        },
                        SubSelect {
                            query: Query {
                                sub_selects: vec![],
                                node: Select {
                                    check: None,
                                    order: vec![],
                                    groupby: vec![],
                                    limit: None,
                                    offset: None,
                                    select: vec![Simple {
                                        field: Field { name: "id", json_path: None },
                                        alias: None,
                                        cast: None
                                    },],
                                    from: ("clients", None),
                                    join_tables: vec![],
                                    //from_alias: None,
                                    where_: ConditionTree {
                                        operator: And,
                                        conditions: vec![Single {
                                            field: Field { name: "id", json_path: None },
                                            filter: Filter::Col(
                                                Qi("", "subzero_source"),
                                                Field {
                                                    name: "client_id",
                                                    json_path: None
                                                }
                                            ),
                                            negate: false,
                                        }]
                                    }
                                }
                            },
                            alias: None,
                            hint: None,
                            join: Some(Parent(ForeignKey {
                                name: "client_id_fk",
                                table: Qi("api", "projects"),
                                columns: vec!["client_id"],
                                referenced_table: Qi("api", "clients"),
                                referenced_columns: vec!["id"],
                            }),)
                        },
                    ],
                    node: Insert {
                        on_conflict: None,
                        select: vec![
                            Simple {
                                field: Field { name: "id", json_path: None },
                                alias: None,
                                cast: None
                            },
                            Simple {
                                field: Field {
                                    name: "name",
                                    json_path: None
                                },
                                alias: None,
                                cast: None
                            },
                        ],
                        payload: Payload(cow(r#"[{"id":10, "name":"john"},{"id":10, "name":"123"}]"#), None),
                        into: "projects",
                        columns: vec!["id", "name"],
                        check: ConditionTree {
                            operator: And,
                            conditions: vec![]
                        },
                        where_: ConditionTree {
                            operator: And,
                            conditions: vec![Single {
                                field: Field { name: "id", json_path: None },
                                filter: Filter::Op(">", SingleVal(cow("10"), Some(cow("int")))),
                                negate: false,
                            }]
                        },
                        returning: vec!["client_id", "id", "name"]
                    }
                }
            })
        );
    }

    // #[test]
    // fn test_get_join_conditions(){
    //     let db_schema  = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
    //     assert_eq!( get_join("api"), &db_schema, &"projects(", &")tasks(", &mut None).map_err(|e| format!("){}",e),
    //         Ok(

    //                 Child(ForeignKey {
    //                     name: "project_id_fk",
    //                     table: Qi("api","tasks"),
    //                     columns: vec!["project_id"],
    //                     referenced_table: Qi("api","projects"),
    //                     referenced_columns: vec!["id"],
    //                 })

    //         )
    //     );
    //     assert_eq!( get_join("api"), &db_schema, &"tasks(", &")projects(", &mut None).map_err(|e| format!("){}",e),
    //         Ok(

    //                 Parent(ForeignKey {
    //                     name: "project_id_fk",
    //                     table: Qi("api","tasks"),
    //                     columns: vec!["project_id"],
    //                     referenced_table: Qi("api","projects"),
    //                     referenced_columns: vec!["id"],
    //                 })

    //         )
    //     );
    //     assert_eq!( get_join("api"), &db_schema, &"clients(", &")projects(", &mut None).map_err(|e| format!("){}",e),
    //         Ok(

    //                 Child(ForeignKey {
    //                     name: "client_id_fk",
    //                     table: Qi("api","projects"),
    //                     columns: vec!["client_id"],
    //                     referenced_table: Qi("api","clients"),
    //                     referenced_columns: vec!["id"],
    //                 })

    //         )
    //     );
    //     assert_eq!( get_join("api"), &db_schema, &"tasks(", &")users(", &mut None).map_err(|e| format!("){}",e),
    //         Ok(

    //                 Many(
    //                     Qi("api", "users_tasks"),
    //                     ForeignKey {
    //                         name: "task_id_fk",
    //                         table: Qi("api","users_tasks"),
    //                         columns: vec!["task_id"],
    //                         referenced_table: Qi("api","tasks"),
    //                         referenced_columns: vec!["id"],
    //                     },
    //                     ForeignKey {
    //                         name: "user_id_fk",
    //                         table: Qi("api","users_tasks"),
    //                         columns: vec!["user_id"],
    //                         referenced_table: Qi("api","users"),
    //                         referenced_columns: vec!["id"],
    //                     },
    //                 )

    //         )
    //     );
    //     assert_eq!( get_join("api"), &db_schema, &"tasks(", &")users(", &mut Some(")users_tasks(")).map_err(|e| format!("){}",e),
    //         Ok(

    //                 Many(
    //                     Qi("api", "users_tasks"),
    //                     ForeignKey {
    //                         name: "task_id_fk",
    //                         table: Qi("api","users_tasks"),
    //                         columns: vec!["task_id"],
    //                         referenced_table: Qi("api","tasks"),
    //                         referenced_columns: vec!["id"],
    //                     },
    //                     ForeignKey {
    //                         name: "user_id_fk",
    //                         table: Qi("api","users_tasks"),
    //                         columns: vec!["user_id"],
    //                         referenced_table: Qi("api","users"),
    //                         referenced_columns: vec!["id"],
    //                     },
    //                 )

    //         )
    //     );

    //     // let result = get_join("api"), &db_schema, &"users(", &")addresses", &mut None;
    //     // let expected = AppError::AmbiguousRelBetween {
    //     //     origin: s("users"), target: s("addresses"),
    //     //     relations: vec![
    //     //         Parent(
    //     //             ForeignKey {
    //     //                 name: "billing_address_id_fk",
    //     //                 table: Qi("api","users"),
    //     //                 columns: vec![
    //     //                     "billing_address_id",
    //     //                 ],
    //     //                 referenced_table: Qi("api","addresses"),
    //     //                 referenced_columns: vec![
    //     //                     "id",
    //     //                 ],
    //     //             },
    //     //         ),
    //     //         Parent(
    //     //             ForeignKey {
    //     //                 name: "shipping_address_id_fk",
    //     //                 table: Qi("api","users"),
    //     //                 columns: vec![
    //     //                     "shipping_address_id",
    //     //                 ],
    //     //                 referenced_table: Qi("api","addresses"),
    //     //                 referenced_columns: vec![
    //     //                     "id",
    //     //                 ],
    //     //             },
    //     //         ),
    //     //     ]
    //     // };
    //     // assert!(result.is_err());
    //     // let error = result.unwrap();

    //     // assert!(matches!(
    //     //     get_join("api"), &db_schema, &"users(", &")addresses", &mut None,
    //     //     1
    //     // );
    //     assert!(matches!(
    //         get_join("api"), &db_schema, &"users(", &")addresses", &mut None,
    //         Err(AppError::AmbiguousRelBetween {..})
    //     ));

    // }

    #[test]
    fn parse_preferences() {
        assert_eq!(
            preferences("return=minimal , resolution = merge-duplicates, count=planned, count=exact"),
            Ok((
                "",
                Preferences {
                    representation: Some(Representation::None),
                    resolution: Some(Resolution::MergeDuplicates),
                    count: Some(Count::ExactCount)
                },
            ))
        );
    }

    #[test]
    fn parse_filter() {
        assert_eq!(filter(&None, "gte.5"), Ok(("", Filter::Op(">=", SingleVal(cow("5"), None)))));
        assert_eq!(filter(&None, "in.(1,2,3)"), Ok(("", Filter::In(ListVal(["1", "2", "3"].map(cow).to_vec(), None)))));
        assert_eq!(filter(&None, "fts.word"), Ok(("", Filter::Fts("@@ to_tsquery", None, SingleVal(cow("word"), None)))));
    }

    #[test]
    fn parse_logic_condition() {
        let field = Field { name: "id", json_path: None };
        assert_eq!(
            logic_condition(None, None, "id.gte.5"),
            Ok((
                "",
                Single {
                    filter: Filter::Op(">=", SingleVal(cow("5"), None)),
                    field: field.clone(),
                    negate: false
                },
            ))
        );
        assert_eq!(
            logic_condition(None, None, "id.not.in.(1,2,3)"),
            Ok((
                "",
                Single {
                    filter: Filter::In(ListVal(vec![cow("1"), cow("2"), cow("3")], None)),
                    field: field.clone(),
                    negate: true
                },
            ))
        );
        assert_eq!(
            logic_condition(None, None, "id.fts.word"),
            Ok((
                "",
                Single {
                    filter: Filter::Fts("@@ to_tsquery", None, SingleVal(cow("word"), None)),
                    field: field.clone(),
                    negate: false
                },
            ))
        );
        assert_eq!(
            logic_condition(None, None, "not.or(id.gte.5, id.lte.10)"),
            Ok((
                "",
                Condition::Group {
                    negate: true,
                    tree: ConditionTree {
                        operator: Or,
                        conditions: vec![
                            Single {
                                filter: Filter::Op(">=", SingleVal(cow("5"), None)),
                                field: field.clone(),
                                negate: false
                            },
                            Single {
                                filter: Filter::Op("<=", SingleVal(cow("10"), None)),
                                field: field.clone(),
                                negate: false
                            }
                        ]
                    }
                },
            ))
        );
        assert_eq!(
            logic_condition(None, None, "not.or ( id.gte.5, id.lte.10, and(id.gte.2, id.lte.4))"),
            Ok((
                "",
                Condition::Group {
                    negate: true,
                    tree: ConditionTree {
                        operator: Or,
                        conditions: vec![
                            Single {
                                filter: Filter::Op(">=", SingleVal(cow("5"), None)),
                                field: field.clone(),
                                negate: false
                            },
                            Single {
                                filter: Filter::Op("<=", SingleVal(cow("10"), None)),
                                field: field.clone(),
                                negate: false
                            },
                            Condition::Group {
                                negate: false,
                                tree: ConditionTree {
                                    operator: And,
                                    conditions: vec![
                                        Single {
                                            filter: Filter::Op(">=", SingleVal(cow("2"), None)),
                                            field: field.clone(),
                                            negate: false
                                        },
                                        Single {
                                            filter: Filter::Op("<=", SingleVal(cow("4"), None)),
                                            field,
                                            negate: false
                                        }
                                    ]
                                }
                            }
                        ]
                    }
                },
            ))
        );
    }

    #[test]
    fn parse_operator() {
        assert_eq!(operator("gte."), Ok((".", ">=")));
        // assert_eq!(
        //     operator("gtv."),
        //     Err(Errors {
        //         position: PointerOffset::new("gtv.".as_ptr() as usize),
        //         errors: vec![Error::Message("unknown operator".into())]
        //     })
        // );
    }

    #[test]
    fn parse_fts_operator() {
        assert_eq!(fts_operator("plfts."), Ok((".", "@@ plainto_tsquery")));
        // assert_eq!(
        //     fts_operator("xfts."),
        //     Err(Errors {
        //         position: PointerOffset::new("xfts.".as_ptr() as usize),
        //         errors: vec![Error::Message("unknown fts operator".into())]
        //     })
        // );
    }

    #[test]
    fn parse_single_value() {
        assert_eq!(single_value(&None, "any123value"), Ok(("", SingleVal(cow("any123value"), None))));
        assert_eq!(single_value(&None, "any123value,another"), Ok(("", SingleVal(cow("any123value,another"), None))));
    }

    #[test]
    fn parse_logic_single_value() {
        assert_eq!(logic_single_value(&None, "any123value"), Ok(("", sv("any123value"))));
        assert_eq!(logic_single_value(&None, "any123value,another"), Ok((",another", sv("any123value"))));
        assert_eq!(logic_single_value(&None, "\"any 123 value,)\""), Ok(("", sv("any 123 value,)"))));
        assert_eq!(logic_single_value(&None, "{a, b, c}"), Ok(("", sv("{a, b, c}"))));
    }

    #[test]
    fn parse_list_element() {
        assert_eq!(list_element("any 123 value"), Ok(("", cow("any 123 value"))));
        assert_eq!(list_element("any123value,another"), Ok((",another", cow("any123value"))));
        assert_eq!(list_element("any123value)"), Ok((")", cow("any123value"))));
        assert_eq!(list_element("\"any123value,)\",another"), Ok((",another", cow("any123value,)"))));
    }

    #[test]
    fn parse_list_value() {
        assert_eq!(list_value(&None, "()"), Ok(("", ListVal(vec![], None))));
        assert_eq!(list_value(&None, "(any 123 value)"), Ok(("", ListVal(vec![cow("any 123 value")], None))));
        assert_eq!(list_value(&None, "(any123value,another)"), Ok(("", ListVal(vec![cow("any123value"), cow("another")], None))));
        assert_eq!(list_value(&None, "(\"any123 value\", another)"), Ok(("", ListVal(vec![cow("any123 value"), cow("another")], None))));
        assert_eq!(list_value(&None, "(\"any123 value\", 123)"), Ok(("", ListVal(vec![cow("any123 value"), cow("123")], None))));
        assert_eq!(list_value(&None, "(\"Double\\\"Quote\\\"McGraw\\\"\")"), Ok(("", ListVal(vec![cow("Double\"Quote\"McGraw\"")], None))));
    }

    #[test]
    fn parse_alias_separator() {
        assert_eq!(alias_separator(":abc"), Ok(("abc", ":")));
        assert_eq!(alias_separator("::abc").is_err(), true);
    }

    #[test]
    fn parse_json_path() {
        assert_eq!(json_path("->key"), Ok(("", vec![JArrow(JKey("key"))])));

        assert_eq!(json_path("->>51"), Ok(("", vec![J2Arrow(JIdx("51"))])));

        assert_eq!(json_path("->key1->>key2"), Ok(("", vec![JArrow(JKey("key1")), J2Arrow(JKey("key2"))])));

        assert_eq!(json_path("->key1->>key2,rest"), Ok((",rest", vec![JArrow(JKey("key1")), J2Arrow(JKey("key2"))])));
    }

    #[test]
    fn parse_field_name() {
        assert_eq!(field_name("field with space "), Ok(("", "field with space")));
        assert_eq!(field_name("field12"), Ok(("", "field12")));
        assert_ne!(field_name("field,invalid"), Ok(("", "field,invalid")));
        assert_eq!(field_name("field-name"), Ok(("", "field-name")));
        assert_eq!(field_name("field-name->"), Ok(("->", "field-name")));
        assert_eq!(quoted_value("\"field name\""), Ok(("", "field name")));
    }

    #[test]
    fn parse_order() {
        assert_eq!(
            order("field"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: None,
                    null_order: None
                },]
            ))
        );
        assert_eq!(
            order("field.asc"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: Some(OrderDirection::Asc),
                    null_order: None
                },]
            ))
        );
        assert_eq!(
            order("field.desc"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: Some(OrderDirection::Desc),
                    null_order: None
                },]
            ))
        );
        assert_eq!(
            order("field.desc.nullsfirst"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: Some(OrderDirection::Desc),
                    null_order: Some(OrderNulls::NullsFirst)
                },]
            ))
        );
        assert_eq!(
            order("field.desc.nullslast"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: Some(OrderDirection::Desc),
                    null_order: Some(OrderNulls::NullsLast)
                },]
            ))
        );
        assert_eq!(
            order("field.nullslast"),
            Ok((
                "",
                vec![OrderTerm {
                    term: Field {
                        name: "field",
                        json_path: None
                    },
                    direction: None,
                    null_order: Some(OrderNulls::NullsLast)
                },]
            ))
        );
        assert_eq!(
            order("field,field.asc,field.desc.nullslast"),
            Ok((
                "",
                vec![
                    OrderTerm {
                        term: Field {
                            name: "field",
                            json_path: None
                        },
                        direction: None,
                        null_order: None
                    },
                    OrderTerm {
                        term: Field {
                            name: "field",
                            json_path: None
                        },
                        direction: Some(OrderDirection::Asc),
                        null_order: None
                    },
                    OrderTerm {
                        term: Field {
                            name: "field",
                            json_path: None
                        },
                        direction: Some(OrderDirection::Desc),
                        null_order: Some(OrderNulls::NullsLast)
                    },
                ],
            ))
        );
    }

    #[test]
    fn parse_columns() {
        assert_eq!(columns("col1, col2 "), Ok(("", vec!["col1", "col2"])));

        // assert_eq!(
        //     columns(position::Stream::new("id,# name")),
        //     Err(Errors {
        //         position: SourcePosition { line: 1, column: 4 },
        //         errors: vec![
        //             Error::Unexpected('#'.into()),
        //             Error::Expected("whitespace".into()),
        //             Error::Expected('"'.into()),
        //             Error::Expected("letter".into()),
        //             Error::Expected("digit".into()),
        //             Error::Expected('_'.into()),
        //             Error::Expected(' '.into()),
        //         ]
        //     })
        // );

        // assert_eq!(
        //     columns("col1, col2, "),
        //     Err(Errors {
        //         position: SourcePosition { line: 1, column: 13 },
        //         errors: vec![
        //             Error::Unexpected("end of input".into()),
        //             Error::Expected("whitespace".into()),
        //             Error::Expected('"'.into()),
        //             Error::Expected("letter".into()),
        //             Error::Expected("digit".into()),
        //             Error::Expected('_'.into()),
        //             Error::Expected(' '.into()),
        //         ]
        //     })
        // );

        // assert_eq!(columns(position::Stream::new("col1, col2 col3")), Err(Errors {
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
            name: "field",
            json_path: None,
        };
        assert_eq!(field("field"), Ok(("", result)));
        let result = Field {
            name: "field",
            json_path: Some(vec![JArrow(JKey("key"))]),
        };
        assert_eq!(field("field->key"), Ok(("", result)));
    }

    #[test]
    fn parse_tree_path() {
        let result = (
            vec!["sub", "path"],
            Field {
                name: "field",
                json_path: Some(vec![JArrow(JKey("key"))]),
            },
        );
        assert_eq!(tree_path("sub.path.field->key"), Ok(("", result)));
        //assert!(false);
    }

    #[test]
    fn parse_logic_tree_path() {
        assert_eq!(logic_tree_path("and"), Ok(("", (vec![], false, And))));
        assert_eq!(logic_tree_path("not.or"), Ok(("", (vec![], true, Or))));
        assert_eq!(logic_tree_path("sub.path.and"), Ok(("", (vec!["sub", "path"], false, And))));
        assert_eq!(logic_tree_path("sub.path.not.or"), Ok(("", (vec!["sub", "path"], true, Or))));
    }

    #[test]
    fn parse_select_item() {
        assert_eq!(
            select_item("alias:$sum(field)-p(city)-o(city.desc)"),
            Ok((
                "",
                Item(Func {
                    alias: Some("alias"),
                    fn_name: "sum",
                    parameters: vec![FunctionParam::Fld(Field {
                        name: "field",
                        json_path: None
                    })],
                    partitions: vec![Field {
                        name: "city",
                        json_path: None
                    }],
                    orders: vec![OrderTerm {
                        term: Field {
                            name: "city",
                            json_path: None
                        },
                        direction: Some(OrderDirection::Desc),
                        null_order: None,
                    }],
                }),
            ))
        );
        assert_eq!(
            select_item("alias:$upper(field, '10')"),
            Ok((
                "",
                Item(Func {
                    alias: Some("alias"),
                    fn_name: "upper",
                    parameters: vec![
                        FunctionParam::Fld(Field {
                            name: "field",
                            json_path: None
                        }),
                        FunctionParam::Val(SingleVal(cow("10"), None), None),
                    ],
                    partitions: vec![],
                    orders: vec![],
                }),
            ))
        );

        assert_eq!(
            select_item("alias:column"),
            Ok((
                "",
                Item(Simple {
                    field: Field {
                        name: "column",
                        json_path: None
                    },
                    alias: Some("alias"),
                    cast: None
                }),
            ))
        );

        assert_eq!(
            select_item("column::cast"),
            Ok((
                "",
                Item(Simple {
                    field: Field {
                        name: "column",
                        json_path: None
                    },
                    alias: None,
                    cast: Some("cast")
                }),
            ))
        );

        assert_eq!(
            select_item("alias:column::cast"),
            Ok((
                "",
                Item(Simple {
                    field: Field {
                        name: "column",
                        json_path: None
                    },
                    alias: Some("alias"),
                    cast: Some("cast")
                }),
            ))
        );

        assert_eq!(
            select_item("column"),
            Ok((
                "",
                Item(Simple {
                    field: Field {
                        name: "column",
                        json_path: None
                    },
                    alias: None,
                    cast: None
                }),
            ))
        );

        assert_eq!(
            select_item("table!hint( column0->key, column1 ,  alias2:column2 )"),
            Ok((
                "",
                Sub(Box::new(SubSelect {
                    query: Query {
                        sub_selects: vec![],
                        node: Select {
                            check: None,
                            order: vec![],
                            groupby: vec![],
                            limit: None,
                            offset: None,
                            select: vec![
                                Simple {
                                    field: Field {
                                        name: "column0",
                                        json_path: Some(vec![JArrow(JKey("key"))])
                                    },
                                    alias: None,
                                    cast: None
                                },
                                Simple {
                                    field: Field {
                                        name: "column1",
                                        json_path: None
                                    },
                                    alias: None,
                                    cast: None
                                },
                                Simple {
                                    field: Field {
                                        name: "column2",
                                        json_path: None
                                    },
                                    alias: Some("alias2"),
                                    cast: None
                                },
                            ],
                            from: ("table", None),
                            join_tables: vec![],
                            //from_alias: None,
                            where_: ConditionTree {
                                operator: And,
                                conditions: vec![]
                            }
                        }
                    },
                    alias: None,
                    hint: Some("hint"),
                    join: None
                })),
            ))
        );

        assert_eq!(
            select_item("table.hint ( column0->key, column1 ,  alias2:column2 )"),
            Ok((
                "",
                Sub(Box::new(SubSelect {
                    query: Query {
                        sub_selects: vec![],
                        node: Select {
                            check: None,
                            order: vec![],
                            groupby: vec![],
                            limit: None,
                            offset: None,
                            select: vec![
                                Simple {
                                    field: Field {
                                        name: "column0",
                                        json_path: Some(vec![JArrow(JKey("key"))])
                                    },
                                    alias: None,
                                    cast: None
                                },
                                Simple {
                                    field: Field {
                                        name: "column1",
                                        json_path: None
                                    },
                                    alias: None,
                                    cast: None
                                },
                                Simple {
                                    field: Field {
                                        name: "column2",
                                        json_path: None
                                    },
                                    alias: Some("alias2"),
                                    cast: None
                                },
                            ],
                            from: ("table", None),
                            join_tables: vec![],
                            //from_alias: None,
                            where_: ConditionTree {
                                operator: And,
                                conditions: vec![]
                            }
                        }
                    },
                    alias: None,
                    hint: Some("hint"),
                    join: None
                })),
            ))
        );
    }
}
