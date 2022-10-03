use serde::{Deserialize, Serialize, Deserializer, Serializer, ser::SerializeStruct, ser::SerializeSeq};
use std::collections::{HashMap, VecDeque};
use serde_json::Value as JsonValue;
use crate::error::Result;
use QueryNode::*;

pub const DEFAULT_SAFE_SELECT_FUNCTIONS: &[&str] = &[
    "avg", "count", "every", "max", "min", "sum", "array_agg", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg", "string_agg",
    "corr", "covar_pop", "covar_samp", "regr_avgx", "regr_avgy", "regr_count", "regr_intercept", "regr_r2", "regr_slope", "regr_sxx", "regr_sxy", "regr_syy",
    "mode", "percentile_cont", "percentile_cont", "percentile_disc", "percentile_disc",
    "row_number", "rank", " dense_rank", "cume_dist", "percent_rank", "first_value", "last_value", "nth_value",
    "lower", "trim", "upper", "concat", "concat_ws", "format", "substr", "ceil", "truncate",
    "date_diff",
    "toHour", "dictGet", "dictHas", "dictGetOrDefault", "toUInt64"
    ];

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
#[derive(Debug, PartialEq, Clone)]
pub enum Count {
    ExactCount,
    PlannedCount,
    EstimatedCount,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Preferences {
    pub resolution: Option<Resolution>,
    pub representation: Option<Representation>,
    pub count: Option<Count>,
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
    pub fn insert_conditions(&mut self, conditions: Vec<(Vec<String>, Condition)>) -> Result<()> {
        self.insert_properties(conditions, |q, p| {
            let query_conditions: &mut Vec<Condition> = match &mut q.node {
                Select { where_, .. } => where_.conditions.as_mut(),
                Insert { where_, .. } => where_.conditions.as_mut(),
                Update { where_, .. } => where_.conditions.as_mut(),
                Delete { where_, .. } => where_.conditions.as_mut(),
                FunctionCall { where_, .. } => where_.conditions.as_mut(),
            };
            p.into_iter().for_each(|c| query_conditions.push(c));
            Ok(())
        })
    }
    pub fn insert_properties<T>(&mut self, mut properties: Vec<(Vec<String>, T)>, f: fn(&mut Query, Vec<T>) -> Result<()>) -> Result<()> {
        let node_properties = properties.drain_filter(|(path, _)| path.is_empty()).map(|(_, c)| c).collect::<Vec<_>>();
        if !node_properties.is_empty() {
            f(self, node_properties)?
        };
    
        for SubSelect { query: q, alias, .. } in self.sub_selects.iter_mut() {
            if let QueryNode::Select { from: (table, _), .. } = &mut q.node {
                let node_properties = properties
                    .drain_filter(|(path, _)| match path.get(0) {
                        Some(p) => {
                            if p == table || Some(p) == alias.as_ref() {
                                path.remove(0);
                                true
                            } else {
                                false
                            }
                        }
                        None => false,
                    })
                    .collect::<Vec<_>>();
                q.insert_properties(node_properties, f)?;
            }
        }
        Ok(())
    }
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
                    |SubSelect {query: Query { node, .. }, ..}| {
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
                    |SubSelect {query: Query { node, .. }, ..}| {
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
                    |SubSelect {query: Query { node, .. }, ..}| {
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
                    |SubSelect {query: Query { node, .. }, ..}| {
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
        check: ConditionTree,
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
        columns: Vec<ColumnName>,
        payload: Payload,
        check: ConditionTree,
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
    pub fn where_as_mut(&mut self) -> &mut ConditionTree {
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
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

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, )]
pub struct ConditionTree {
    #[serde(rename = "logic_op")]
    pub operator: LogicOperator,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize,)]
#[serde(rename_all = "snake_case", untagged)]
pub enum Condition {
    Group {
        #[serde(default, skip_serializing_if = "is_default")]
        negate: Negate,
        tree: ConditionTree
    },
    Single {
        #[serde(flatten)]
        field: Field,
        #[serde(flatten)]
        filter: Filter,
        #[serde(default, skip_serializing_if = "is_default")]
        negate: Negate
    },
    Foreign { left: (Qi, Field), right: (Qi, Field) },
    Raw {
        sql: String
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone,)]
pub enum TrileanVal {
    TriTrue,
    TriFalse,
    TriNull,
    TriUnknown,
}
impl Serialize for TrileanVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            TrileanVal::TriTrue => serializer.serialize_bool(true),
            TrileanVal::TriFalse => serializer.serialize_bool(false),
            TrileanVal::TriNull => serializer.serialize_none(),
            TrileanVal::TriUnknown => serializer.serialize_str("unknown"),
        }
    }
}
impl<'de> serde::Deserialize<'de> for TrileanVal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::Bool(true) => Ok(TrileanVal::TriTrue),
            JsonValue::Bool(false) => Ok(TrileanVal::TriFalse),
            JsonValue::Null => Ok(TrileanVal::TriNull),
            JsonValue::String(s) => {
                if s == "unknown" {
                    Ok(TrileanVal::TriUnknown)
                } else {
                    Err(serde::de::Error::custom(format!("invalid trilean value: {}", s)))
                }
            },
            _ => Err(serde::de::Error::custom(format!("invalid trilean value: {}", v))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize,)]
pub struct EnvVar {
    #[serde(rename = "env")]
    pub var: String,
    #[serde(rename = "env_part")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub part: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone,)]
pub enum Filter {
    Op(Operator, SingleVal),
    In(ListVal),
    Is(TrileanVal),
    Fts(Operator, Option<Language>, SingleVal),
    Col(Qi, Field),
    Env(Operator, EnvVar),
}
impl Serialize for Filter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self { 
            Filter::Op(operator, value) => FilterHelper::Op{operator: operator.clone(), value: value.clone()},
            Filter::In(value) => FilterHelper::In{value: value.clone()},
            Filter::Is(value) => FilterHelper::Is{value: value.clone()},
            Filter::Fts(operator, language, value) => FilterHelper::Fts{operator: operator.clone(), language: language.clone(), value: value.clone()},
            Filter::Col(qi, field) => FilterHelper::Col{qi: qi.clone(), field: field.clone()},
            Filter::Env(operator, var) => FilterHelper::Env{operator: operator.clone(), var: var.clone()},
        }.serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|v| match v {
            FilterHelper::Op{operator, value} => Filter::Op(operator, value),
            FilterHelper::In{value} => Filter::In(value),
            FilterHelper::Is{value} => Filter::Is(value),
            FilterHelper::Fts{operator, language, value} => Filter::Fts(operator, language, value),
            FilterHelper::Col{qi, field} => Filter::Col(qi, field),
            FilterHelper::Env{operator, var} => Filter::Env(operator, var),
        })
    }
}

// private
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
enum FilterHelper {
    Op{
        #[serde(rename = "op")]
        operator: Operator,
        #[serde(rename = "val")]
        value: SingleVal
    },
    In{
        #[serde(rename = "in")]
        value: ListVal
    },
    Is{
        #[serde(rename = "is")]
        value: TrileanVal
    },
    Fts{
        #[serde(rename = "fts_op")]
        operator: Operator,
        #[serde(default, skip_serializing_if = "is_default")]
        language: Option<Language>,
        #[serde(rename = "val")]
        value: SingleVal
    },

    Env{
        #[serde(rename = "op")]
        operator: Operator,
        #[serde(flatten)]
        var: EnvVar
    },
    Col{qi: Qi, field: Field},
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize,)]
pub struct Field {
    #[serde(rename = "column")]
    pub name: ColumnName,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub json_path: Option<Vec<JsonOperation>>, //TODO!! should contain some info about the data type so that fmt_field function could make better decisions
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize,)]
pub enum JsonOperation {
    #[serde(rename = "->")]
    JArrow(JsonOperand),
    #[serde(rename = "->>")]
    J2Arrow(JsonOperand),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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

#[derive(Debug, PartialEq, Eq, Hash, Clone,)]
pub struct SingleVal(
    pub String,
    pub Option<String>
);
impl Serialize for SingleVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            SingleVal(v, Some(t)) => {
                let mut rgb = serializer.serialize_struct("SingleVal", 2)?;
                rgb.serialize_field("v", v)?;
                rgb.serialize_field("t", t)?;
                rgb.end()
            },
            SingleVal(v, None) => serializer.serialize_str(v),
        }
    }
}
impl<'de> Deserialize<'de> for SingleVal     {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::String(s) => Ok(Self(s, None)),
            JsonValue::Object(o) => {
                match (o.get("v"), o.get("t")) {
                    (Some(JsonValue::String(v)), Some(JsonValue::String(t))) => Ok(SingleVal(v.clone(), Some(t.clone()))),
                    _ => Err(serde::de::Error::custom("Invalid SingleVal"))
                }
            }
            _ => Err(serde::de::Error::custom("Invalid SingleVal"))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone,)]
pub struct ListVal(
    pub Vec<String>,
    pub Option<String>
);

impl Serialize for ListVal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ListVal(v, Some(t)) => {
                let mut rgb = serializer.serialize_struct("ListVal", 2)?;
                rgb.serialize_field("v", v)?;
                rgb.serialize_field("t", t)?;
                rgb.end()
            },
            ListVal(v, None) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for element in v {
                    seq.serialize_element(element)?;
                }
                seq.end()
            },
        }
    }
}


impl<'de> Deserialize<'de> for ListVal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // TODO!!! flatten will eliminate None and we need to treat it as an error
        let to_str_vec = |v: &Vec<JsonValue>| v.iter().filter_map(|v:&JsonValue| v.as_str()).map(String::from).collect();
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::Array(v) => Ok(Self(to_str_vec(&v),None)),
            JsonValue::Object(o) => {
                match (o.get("v"), o.get("t")) {
                    (Some(JsonValue::Array(v)), Some(JsonValue::String(t))) => Ok(Self(to_str_vec(v), Some(t.clone()))),
                    _ => Err(serde::de::Error::custom("Invalid SingleVal"))
                }
            }
            _ => Err(serde::de::Error::custom("Invalid SingleVal"))
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, )]
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
    use super::*;
    use pretty_assertions::assert_eq;
    fn s(s: &str) -> String { s.to_string() }
    #[test]
    fn serialize() {
        assert_eq!(r#"["schema","table"]"#, serde_json::to_string(&Qi(s("schema"),s("table"))).unwrap());
        assert_eq!(serde_json::from_str::<Qi>(r#"["schema","table"]"#).unwrap(), Qi(s("schema"),s("table")));
        assert_eq!(r#""and""#, serde_json::to_string(&LogicOperator::And).unwrap());
        assert_eq!(serde_json::from_str::<LogicOperator>(r#""and""#).unwrap(), LogicOperator::And);
        assert_eq!(r#""10""#, serde_json::to_string(&SingleVal(s("10"),None)).unwrap());
        assert_eq!(serde_json::from_str::<SingleVal>(r#""10""#).unwrap(), SingleVal(s("10"),None));
        assert_eq!(r#"["1","2","3"]"#, serde_json::to_string(&ListVal(vec![s("1"),s("2"),s("3")],None)).unwrap());
        assert_eq!(serde_json::from_str::<ListVal>(r#"["1","2","3"]"#).unwrap(), ListVal(vec![s("1"),s("2"),s("3")],None));
        assert_eq!(r#"{"column":"id"}"#, serde_json::to_string(&Field{name:s("id"), json_path:None}).unwrap());
        assert_eq!(serde_json::from_str::<Field>(r#"{"column":"id"}"#).unwrap(), Field{name:s("id"), json_path:None});
        assert_eq!(r#"{"op":"eq","val":"10"}"#, serde_json::to_string(&Filter::Op(s("eq"), SingleVal(s("10"),None))).unwrap());
        assert_eq!(serde_json::from_str::<Filter>(r#"{"op":"eq","val":"10"}"#).unwrap(), Filter::Op(s("eq"), SingleVal(s("10"),None)));
        assert_eq!(r#"{"column":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#, serde_json::to_string(&Field{name:s("id"), json_path:Some(
            vec![
                JsonOperation::JArrow(JsonOperand::JKey(s("id"))),
                JsonOperation::J2Arrow(JsonOperand::JIdx(s("0")))
            ]
        )}).unwrap());
        assert_eq!(serde_json::from_str::<Field>(r#"{"column":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#).unwrap(), Field{name:s("id"), json_path:Some(
            vec![
                JsonOperation::JArrow(JsonOperand::JKey(s("id"))),
                JsonOperation::J2Arrow(JsonOperand::JIdx(s("0")))
            ]
        )});
        assert_eq!(r#"{"column":"id","op":"eq","val":"10"}"#, serde_json::to_string(&Condition::Single{
            field: Field{name:s("id"), json_path:None},
            filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
            negate:false,
        }).unwrap());
        assert_eq!(serde_json::from_str::<Condition>(r#"{"column":"id","op":"eq","val":"10"}"#).unwrap(), Condition::Single{
            field: Field{name:s("id"), json_path:None},
            filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
            negate:false,
        });
        assert_eq!(serde_json::from_str::<Condition>(r#"{"tree":{"logic_op":"and","conditions":[{"column":"id","op":"eq","val":"10"}]}}"#).unwrap(), Condition::Group{
            negate: false,
            tree: ConditionTree{
                operator:LogicOperator::And,
                conditions:vec![
                    Condition::Single{
                        field: Field{name:s("id"), json_path:None},
                        filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
                        negate:false,
                    },
                ],
            }
        });
        assert_eq!(r#"{"tree":{"logic_op":"and","conditions":[{"column":"id","op":"eq","val":"10"}]}}"#, serde_json::to_string(&Condition::Group{
            negate: false,
            tree: ConditionTree{
                operator:LogicOperator::And,
                conditions:vec![
                    Condition::Single{
                        field: Field{name:s("id"), json_path:None},
                        filter: Filter::Op(s("eq"), SingleVal(s("10"),None)),
                        negate:false,
                    },
                ],
            }
        }).unwrap());
        assert_eq!(serde_json::from_str::<Condition>(r#"{"sql":"false"}"#).unwrap(), Condition::Raw{sql:s("false")});
    }
}
