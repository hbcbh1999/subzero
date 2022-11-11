use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter::{zip, FromIterator};

use crate::api::{Condition::*, ContentType::*, Filter::*, Join::*, LogicOperator::*, QueryNode::*, SelectItem::*, SelectKind::*, *};
use crate::schema::{ObjectType::*, PgType::*, ProcReturnType::*, *};


use nom::{IResult,error::ParseError,
    combinator::{peek, recognize, eof, map, map_res, opt, value},
    sequence::{delimited, terminated, separated_pair, preceded, tuple},
    bytes::complete::{tag, take_while, take_while1, is_not},
    character::complete::{multispace0, char, alpha1, digit1, one_of},
    multi::{many0, many1, separated_list0, separated_list1, },
    branch::{alt},
};
use nom::{Err, error::{ErrorKind, Error}};

lazy_static! {
    static ref STAR: String = "*".to_string();
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

type Parsed<'a, T> = IResult<&'a str, T>;
type Field<'a> = (&'a str, Option<Vec<(&'a str,&'a str)>>);


/// A combinator that takes a parser `inner` and produces a parser that also consumes both leading and 
/// trailing whitespace, returning the output of `inner`.
fn ws<'a, F: 'a, O, E: ParseError<&'a str>>(inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O, E>
where F: FnMut(&'a str) -> IResult<&'a str, O, E> {
    delimited(multispace0,inner,multispace0)
}

fn dash(i: &str) -> Parsed<&str> { terminated(tag("-"), peek(is_not(">")))(i)}

fn field_name(i: &str) -> Parsed<&str> {
    recognize(separated_list1(dash, many1(alt((alpha1, digit1, tag("_") )))))(i)
}

fn function_name(i: &str) -> Parsed<&str> {
    recognize(separated_list1(dash, many1(alt((alpha1, digit1, tag("_") )))))(i)
}

fn alias_separator(i: &str) -> Parsed<&str> {
    terminated(tag(":"), peek(is_not(":")))(i)
    //tag(":")(i)
}

fn alias(i: &str) -> Parsed<&str> {
    terminated(recognize(many1(alt((alpha1,digit1,recognize(one_of("@._")))))),alias_separator)(i)
}

fn cast(i: &str) -> Parsed<&str> {
    preceded(tag("::"), recognize(many1(alt((alpha1, digit1)))))(i)
}

fn dot(i: &str) -> Parsed<&str> {
    tag(".")(i)
}

fn columns(i: &str) -> Parsed<Vec<&str>> {
    terminated(separated_list1(tag(","), ws(field_name)),eof)(i)
}

fn on_conflict(i: &str) -> Parsed<Vec<&str>> {
    terminated(separated_list1(tag(","), ws(field_name)),eof)(i)
}



fn integer(i: &str) -> Parsed<(&str, Option<&str>)> {
    let (input, integer) = recognize(many1(digit1))(i)?;
    Ok((input, (integer, Some("integer"))))
}

fn limit(i: &str) -> Parsed<(&str, Option<&str>)> {
    integer(i)
}

fn offset(i: &str) -> Parsed<(&str, Option<&str>)> {
    integer(i)
}

fn operator(i: &str) -> Parsed<&str> {
    map_res(alpha1, |o: &str|
        match OPERATORS.get(o) {
            Some(&op) => Ok(op),
            None => Err(Err::Error(("unknown operator", ErrorKind::Fail)))
        }
    )(i)
}

fn fts_operator(i: &str) -> Parsed<&str> {
    map_res(alpha1, |o: &str|
        match FTS_OPERATORS.get(o) {
            Some(&op) => Ok(op),
            None => Err(Err::Error(("unknown fts operator", ErrorKind::Fail)))
        }
    )(i)
}

fn arrow(i: &str) -> Parsed<&str> {
    alt((tag("->>"), tag("->")))(i)
}

fn signed_number(i: &str) -> Parsed<&str> {
    recognize(preceded(opt(char('-')), terminated(digit1, peek(alt((tag("->"), tag("::"), tag("."), tag(","), eof))))))(i)
}

fn json_operand(i: &str) -> Parsed<&str> {
    alt((signed_number, field_name))(i)
}

fn json_path(i: &str) -> Parsed<Vec<(&str,&str)>> {
    many1(tuple((arrow, json_operand)))(i)
}

fn field(i: &str) -> Parsed<Field> {
    tuple((field_name, opt(json_path)))(i) 
}

fn quoted_value(i: &str) -> Parsed<&str> {
    delimited(
        char('"'), 
        is_not("\""),
        char('"')
    )(i)
}

fn groupby(i: &str) -> Parsed<Vec<Field>> {
    terminated(
        separated_list1(tag(","), ws(field)),
        eof
    )(i)
}

type OrderTerm<'a> = (
    Field<'a>,
    Option<&'a str>,
    Option<&'a str>
);
fn order(i: &str) -> Parsed<Vec<OrderTerm>> {
    terminated(
        separated_list1(
            tag(","),
            ws(tuple((
                field,
                opt(preceded(dot, alt((tag("asc"), tag("desc"))))),
                opt(preceded(dot, alt((tag("nullsfirst"), tag("nullslast"))))),
            )))
        ),
        eof
    )(i)
}

type SingleValue<'a> = (&'a str, Option<&'a str>);
fn single_value<'a>(data_type: &'a Option<&'a str>, i: &'a str) -> Parsed<SingleValue> {
    Ok(("",(i, *data_type)))
}
// type Filter<'a> = (&'a str, SingleValue<'a>);
// fn filter<'a>(data_type: &'a Option<&'a str>, i: &'a str) -> Parsed<Filter> {
//     terminated(
//         alt((
//             map(tuple((operator, dot, tag("a"))), |(op,_,v)| vec![(op, Some(vec![v]))]),
//         )),
//         eof
//     )(i)
// }

#[cfg(test)]
pub mod tests {
    use super::*;
    //use std::error::Error;
    use nom::{Err, error::{ErrorKind, Error}};
    //use pretty_assertions::{assert_eq};

    #[test]
    fn parse_order(){
        assert_eq!(order("a"), Ok(("", vec![(("a", None), None, None)])));
        assert_eq!(order("a.asc"), Ok(("", vec![(("a", None), Some("asc"), None)])));
        assert_eq!(order("a.desc"), Ok(("", vec![(("a", None), Some("desc"), None)])));
        assert_eq!(order("a.asc.nullsfirst"), Ok(("", vec![(("a", None), Some("asc"), Some("nullsfirst"))])));
        assert_eq!(order("a.desc.nullslast"), Ok(("", vec![(("a", None), Some("desc"), Some("nullslast"))])));
        assert_eq!(order("a.asc,b.desc"), Ok(("", vec![(("a", None), Some("asc"), None), (("b", None), Some("desc"), None)])));
    }


    #[test]
    fn parse_gropuby() {
        assert_eq!(groupby("a,b,c->d"), Ok(("", vec![("a", None), ("b", None), ("c", Some(vec![("->", "d")]))])));
    }

    #[test]
    fn parse_quoted_value() {
        assert_eq!(quoted_value("\"foo\""), Ok(("", "foo")));
        //assert_eq!(quoted_value(r#""fo\"o"rest"#), Ok(("rest", "fo\\\"o")));
    }

    #[test]
    fn parse_field(){
        assert_eq!(field("foo"), Ok(("", ("foo", None))));
        assert_eq!(field("foo->bar"), Ok(("", ("foo", Some(vec![("->", "bar")])))));
    }

    #[test]
    fn parse_json_path(){
        assert_eq!(json_path("->>foo->1->>-2"), Ok(("", vec![("->>", "foo"), ("->", "1"), ("->>", "-2")])));
    }

    #[test]
    fn parse_fts_operator(){
        assert_eq!(fts_operator("fts"), Ok(("", "@@ to_tsquery")));
        assert_eq!(fts_operator("plfts"), Ok(("", "@@ plainto_tsquery")));
    }


    #[test]
    fn parse_operator(){
        assert_eq!(operator("eq"), Ok(("", "=")));
        assert_eq!(operator("neq"), Ok(("", "<>")));
        assert_eq!(operator("like"), Ok(("", "like")));
        assert_eq!(operator("cs"), Ok(("", "@>")));
    }

    #[test]
    fn parse_single_value(){
        assert_eq!(single_value(&None, "1"), Ok(("", ("1", None))));
        assert_eq!(single_value(&Some("int"), "1.1"), Ok(("", ("1.1", Some("int")))));
    }

    #[test]
    fn parse_on_conflict(){
        assert_eq!(on_conflict("a, b , c"), Ok(("", vec!["a","b","c"])));
    }

    #[test]
    fn parse_columns(){
        assert_eq!(columns("a, b , c"), Ok(("", vec!["a","b","c"])));
    }

    #[test]
    fn parse_cast(){
        assert_eq!(cast("::int"), Ok(("", "int")));
    }

    #[test]
    fn parse_alias(){
        assert_eq!(alias("a1.bc@_:cde"), Ok(("cde", "a1.bc@_")));
        assert_eq!(alias("abc::cde"), Err(Err::Error(Error::new(":cde", ErrorKind::IsNot))));
    }

    #[test]
    fn parse_dash() {
        assert_eq!(dash("-abc"), Ok(("abc", "-")));
        assert_eq!(dash("->abc"),Err(Err::Error(Error::new(">abc", ErrorKind::IsNot))));
    }

    #[test]
    fn parse_field_name() {
        assert_eq!(field_name("abc-cde"), Ok(("", "abc-cde")));
        assert_eq!(field_name("abc1-"), Ok(("-", "abc1")));
        assert_eq!(field_name("abc->cde"), Ok(("->cde", "abc")));
    }

    
}