// pub use http::Method;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

#[derive(Debug, PartialEq, Clone)]
pub enum Resolution {
    MergeDuplicates,
    IgnoreDuplicates,
}
#[derive(Debug, PartialEq, Clone)]
pub enum Representation {
    Full,
    None,
    HeadersOnly,
}
// #[derive(Debug, PartialEq)]
// pub enum Parameters {SingleObject, MultipleObjects}
#[derive(Debug, PartialEq, Clone)]
pub enum Count {
    ExactCount,
    PlannedCount,
    EstimatedCount,
}
// #[derive(Debug, PartialEq)]
// pub enum Transaction {Commit, Rollback}

#[derive(Debug, PartialEq, Clone)]
pub struct Preferences {
    pub resolution: Option<Resolution>,
    pub representation: Option<Representation>,
    // pub parameters: Option<Parameters>,
    pub count: Option<Count>,
    //pub transaction: Option<Transaction>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ApiRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub schema_name: &'a str,
    pub read_only: bool,
    pub accept_content_type: ContentType,
    pub query: Query,
    pub preferences: Option<Preferences>,
    pub headers: HashMap<&'a str, &'a str>,
    pub cookies: HashMap<&'a str, &'a str>,
    pub get: Vec<(&'a str, &'a str)>,
}

#[derive(Debug)]
pub struct ApiResponse {
    pub page_total: i64,
    pub total_result_set: Option<i64>,
    pub top_level_offset: i64,
    pub response_headers: Option<String>,
    pub response_status: Option<String>,
    pub body: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum ContentType {
    ApplicationJSON,
    SingularJSON,
    TextCSV,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ProcParam {
    pub name: String,
    pub type_: String,
    pub required: bool,
    pub variadic: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum CallParams {
    KeyParams(Vec<ProcParam>),
    OnePosParam(ProcParam),
}

#[derive(Debug, PartialEq, Clone)]
pub struct Query {
    pub node: QueryNode,
    pub sub_selects: Vec<SubSelect>,
}
pub struct Iter<T>(VecDeque<Vec<String>>, VecDeque<T>);
pub struct Visitor<'a, F>(VecDeque<Vec<String>>, VecDeque<&'a mut Query>, F);
impl Query {
    pub fn visit<R, F: FnMut(Vec<String>, &mut Self) -> R>(&mut self, f: F) -> Visitor<F> {
        Visitor(VecDeque::from([vec![self.node.name().clone()]]), VecDeque::from([self]), f)
    }
}
impl<'a> Iterator for Iter<&'a Query> {
    type Item = (Vec<String>, &'a QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                stack.extend(sub_selects.iter().map(|SubSelect { query, .. }| query));
                path.extend(sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().clone());
                        p
                    },
                ));
                #[allow(clippy::needless_borrow)]
                Some((current_path, &node))
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for Iter<&'a mut Query> {
    type Item = (Vec<String>, &'a mut QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                path.extend(sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().clone());
                        p
                    },
                ));
                stack.extend(sub_selects.iter_mut().map(|SubSelect { query, .. }| query));
                Some((current_path, &mut *node))
            }
            _ => None,
        }
    }
}

impl Iterator for Iter<Query> {
    type Item = (Vec<String>, QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                path.extend(sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().clone());
                        p
                    },
                ));
                stack.extend(sub_selects.into_iter().map(|SubSelect { query, .. }| query));
                Some((current_path, node))
            }
            _ => None,
        }
    }
}

impl<'a> IntoIterator for &'a Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter { Iter(VecDeque::from([vec![self.node.name().clone()]]), VecDeque::from([self])) }
}

impl<'a> IntoIterator for &'a mut Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter { Iter(VecDeque::from([vec![self.node.name().clone()]]), VecDeque::from([self])) }
}

impl IntoIterator for Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter { Iter(VecDeque::from([vec![self.node.name().clone()]]), VecDeque::from([self])) }
}

impl<R, F> Iterator for Visitor<'_, F>
where
    F: FnMut(Vec<String>, &mut Query) -> R,
{
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack, f) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(query)) => {
                let r = (f)(current_path.clone(), query);
                path.extend(query.sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().clone());
                        p
                    },
                ));
                stack.extend(query.sub_selects.iter_mut().map(|SubSelect { query, .. }| query));
                Some(r)
            }
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum QueryNode {
    FunctionCall {
        fn_name: Qi,
        parameters: CallParams,
        payload: Payload,
        return_table_type: Option<Qi>,
        is_scalar: bool,
        returns_single: bool,
        is_multiple_call: bool,
        returning: Vec<String>,
        select: Vec<SelectItem>,
        where_: ConditionTree,
        limit: Option<SingleVal>,
        offset: Option<SingleVal>,
        order: Vec<OrderTerm>,
    },
    Select {
        select: Vec<SelectItem>,
        from: (String, Option<String>),
        join_tables: Vec<String>,
        where_: ConditionTree,
        limit: Option<SingleVal>,
        offset: Option<SingleVal>,
        order: Vec<OrderTerm>,
        groupby: Vec<GroupByTerm>,
    },
    Insert {
        into: String,
        columns: Vec<ColumnName>,
        payload: Payload,
        where_: ConditionTree, //used only for put
        returning: Vec<String>,
        select: Vec<SelectItem>,
        on_conflict: Option<(Resolution, Vec<String>)>,
    },
    Delete {
        from: String,
        where_: ConditionTree,
        returning: Vec<String>,
        select: Vec<SelectItem>,
    },

    Update {
        table: String,
        columns: Vec<String>,
        payload: Payload,
        where_: ConditionTree,
        returning: Vec<String>,
        select: Vec<SelectItem>,
    },
}

impl QueryNode {
    pub fn name(&self) -> &String {
        match self {
            Self::FunctionCall { fn_name: Qi(_, n), .. } => n,
            Self::Select { from: (t, _), .. } => t,
            Self::Insert { into, .. } => into,
            Self::Delete { from, .. } => from,
            Self::Update { table, .. } => table,
        }
    }
    pub fn select(&self) -> &Vec<SelectItem> {
        match self {
            Self::Select { select, .. } |
            Self::Update { select, .. } |
            Self::Insert { select, .. } |
            Self::Delete { select, .. } | 
            Self::FunctionCall { select, .. } => select,
        }
    }
    pub fn where_(&self) -> &ConditionTree {
        match self {
            Self::Select { where_, .. } |
            Self::Update { where_, .. } |
            Self::Insert { where_, .. } |
            Self::Delete { where_, .. } | 
            Self::FunctionCall { where_, .. } => where_,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct OrderTerm {
    pub term: Field,
    pub direction: Option<OrderDirection>,
    pub null_order: Option<OrderNulls>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct GroupByTerm (pub Field);

#[derive(Debug, PartialEq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub enum OrderNulls {
    NullsFirst,
    NullsLast,
}

pub type JoinHint = String;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Qi(pub String, pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub table: Qi,
    pub columns: Vec<ColumnName>,
    pub referenced_table: Qi,
    pub referenced_columns: Vec<ColumnName>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Clone)]
pub enum Join {
    Child(ForeignKey),
    Parent(ForeignKey),
    Many(Qi, ForeignKey, ForeignKey),
}

#[derive(Debug, PartialEq)]
pub enum SelectKind {
    Item(SelectItem),
    Sub(Box<SubSelect>), //TODO! check performance implications for using box
}

#[derive(Debug, PartialEq, Clone)]
pub enum FunctionParam {
    Fld(Field),
    Val(SingleVal,Option<String>),
    Func {
        fn_name: String,
        parameters: Vec<FunctionParam>,
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum SelectItem {
    //TODO!!! better name
    Star,
    Simple {
        field: Field,
        alias: Option<String>,
        cast: Option<String>,
    },
    Func {
        fn_name: String,
        parameters: Vec<FunctionParam>,
        partitions: Vec<Field>,
        orders: Vec<OrderTerm>,
        alias: Option<String>,
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct SubSelect {
    pub query: Query,
    pub alias: Option<String>,
    pub hint: Option<JoinHint>,
    pub join: Option<Join>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, )]
pub struct ConditionTree {
    pub operator: LogicOperator,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    Group(Negate, ConditionTree),
    Single { 
        field: Field, 
        filter: Filter,
        #[serde(default, skip_serializing_if = "is_default")]
        negate: Negate
    },
    Foreign { left: (Qi, Field), right: (Qi, Field) },
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
pub enum TrileanVal {
    TriTrue,
    TriFalse,
    TriNull,
    TriUnknown,
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    Op(Operator, SingleVal),
    In(ListVal),
    Is(TrileanVal),
    Fts(Operator, Option<Language>, SingleVal),
    Col(Qi, Field),
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
pub struct Field {
    pub name: ColumnName,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub json_path: Option<Vec<JsonOperation>>, //TODO!! should contain some info about the data type so that fmt_field function could make better decisions
}

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
pub enum JsonOperation {
    #[serde(rename = "->")]
    JArrow(JsonOperand),
    #[serde(rename = "->>")]
    J2Arrow(JsonOperand),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonOperand {
    JKey(String),
    JIdx(String),
}
impl serde::Serialize for JsonOperand {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::JKey(s) => serializer.serialize_str(format!("'{}'", s).as_str()),
            Self::JIdx(s) => serializer.serialize_str(s),
        }
    }
}
impl<'de> serde::Deserialize<'de> for JsonOperand {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.starts_with('\'') && s.ends_with('\'') {
            Ok(Self::JKey(s[1..s.len() - 1].to_string()))
        } else {
            Ok(Self::JIdx(s))
        }
    }
}

pub type Operator = String;
pub type Negate = bool;
pub type Language = SingleVal;
pub type ColumnName = String;

#[derive(Debug, PartialEq, Clone)]
pub struct Payload(pub String, pub Option<String>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
pub struct SingleVal(pub String, pub Option<String>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize,)]
pub struct ListVal(pub Vec<String>, pub Option<String>);

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize, )]
#[serde(rename_all = "snake_case")]
pub enum LogicOperator {
    And,
    Or,
}


fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}

#[cfg(test)]
mod tests {
    //use std::collections::HashSet;
    use super::*;
    use pretty_assertions::assert_eq;
    fn s(s: &str) -> String { s.to_string() }
    #[test]
    fn serialize() {
        assert_eq!(r#"["schema","table"]"#, serde_json::to_string(&Qi(s("schema"),s("table"))).unwrap());
        assert_eq!(serde_json::from_str::<Qi>(r#"["schema","table"]"#).unwrap(), Qi(s("schema"),s("table")));
        assert_eq!(r#""and""#, serde_json::to_string(&LogicOperator::And).unwrap());
        assert_eq!(serde_json::from_str::<LogicOperator>(r#""and""#).unwrap(), LogicOperator::And);
        assert_eq!(r#"["10",null]"#, serde_json::to_string(&SingleVal(s("10"),None)).unwrap());
        assert_eq!(serde_json::from_str::<SingleVal>(r#"["10",null]"#).unwrap(), SingleVal(s("10"),None));
        assert_eq!(r#"[["1","2","3"],null]"#, serde_json::to_string(&ListVal(vec![s("1"),s("2"),s("3")],None)).unwrap());
        assert_eq!(serde_json::from_str::<ListVal>(r#"[["1","2","3"],null]"#).unwrap(), ListVal(vec![s("1"),s("2"),s("3")],None));
        assert_eq!(r#"{"name":"id"}"#, serde_json::to_string(&Field{name:s("id"), json_path:None}).unwrap());
        assert_eq!(serde_json::from_str::<Field>(r#"{"name":"id"}"#).unwrap(), Field{name:s("id"), json_path:None});
        assert_eq!(r#"{"op":["eq",["10",null]]}"#, serde_json::to_string(&Filter::Op(s("eq"), SingleVal(s("10"),None))).unwrap());
        assert_eq!(serde_json::from_str::<Filter>(r#"{"op":["eq",["10",null]]}"#).unwrap(), Filter::Op(s("eq"), SingleVal(s("10"),None)));
        assert_eq!(r#"{"name":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#, serde_json::to_string(&Field{name:s("id"), json_path:Some(
            vec![
                JsonOperation::JArrow(JsonOperand::JKey(s("id"))),
                JsonOperation::J2Arrow(JsonOperand::JIdx(s("0")))
            ]
        )}).unwrap());
        assert_eq!(serde_json::from_str::<Field>(r#"{"name":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#).unwrap(), Field{name:s("id"), json_path:Some(
            vec![
                JsonOperation::JArrow(JsonOperand::JKey(s("id"))),
                JsonOperation::J2Arrow(JsonOperand::JIdx(s("0")))
            ]
        )});
        assert_eq!(r#"{"single":{"field":{"name":"id"},"filter":{"op":["eq",["10",null]]}}}"#, serde_json::to_string(&Condition::Single{
            field: Field{name:s("id"), json_path:None},
            filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
            negate:false,
        }).unwrap());
        assert_eq!(serde_json::from_str::<Condition>(r#"{"single":{"field":{"name":"id"},"filter":{"op":["eq",["10",null]]}}}"#).unwrap(), Condition::Single{
            field: Field{name:s("id"), json_path:None},
            filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
            negate:false,
        });
        assert_eq!(serde_json::from_str::<Condition>(r#"{"group":[false,{"operator":"and","conditions":[{"single":{"field":{"name":"id"},"filter":{"op":["eq",["10",null]]}}}]}]}"#).unwrap(), Condition::Group(
            false,
            ConditionTree{
                operator:LogicOperator::And,
                conditions:vec![
                    Condition::Single{
                        field: Field{name:s("id"), json_path:None},
                        filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
                        negate:false,
                    },
                ],
            }
        ));
        assert_eq!(r#"{"group":[false,{"operator":"and","conditions":[{"single":{"field":{"name":"id"},"filter":{"op":["eq",["10",null]]}}}]}]}"#, serde_json::to_string(&Condition::Group(
            false,
            ConditionTree{
                operator:LogicOperator::And,
                conditions:vec![
                    Condition::Single{
                        field: Field{name:s("id"), json_path:None},
                        filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
                        negate:false,
                    },
                ],
            }
        )).unwrap());
    }
}
