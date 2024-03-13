use serde::{Deserialize, Serialize, Deserializer, Serializer, ser::SerializeStruct, ser::SerializeSeq};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque, BTreeMap};
use serde_json::Value as JsonValue;
//use serde_json::value::RawValue as JsonRawValue;
use crate::error::Result;
use QueryNode::*;

pub const DEFAULT_SAFE_SELECT_FUNCTIONS: &[&str] = &[
    "avg",
    "count",
    "every",
    "max",
    "min",
    "sum",
    "array_agg",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "string_agg",
    "corr",
    "covar_pop",
    "covar_samp",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "mode",
    "percentile_cont",
    "percentile_cont",
    "percentile_disc",
    "percentile_disc",
    "row_number",
    "rank",
    "dense_rank",
    "cume_dist",
    "percent_rank",
    "first_value",
    "last_value",
    "nth_value",
    "lower",
    "trim",
    "upper",
    "concat",
    "concat_ws",
    "format",
    "substr",
    "ceil",
    "truncate",
    "date_diff",
    "toHour",
    "dictGet",
    "dictHas",
    "dictGetOrDefault",
    "toUInt64",
];

pub const STAR: &str = "*";
lazy_static! {
    // static ref STAR: String = "*".to_string();
    pub static ref OPERATORS: HashMap<&'static str, &'static str> = [
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
    pub static ref FTS_OPERATORS: HashMap<&'static str, &'static str> = [
         ("fts", "@@ to_tsquery")
        ,("plfts", "@@ plainto_tsquery")
        ,("phfts", "@@ phraseto_tsquery")
        ,("wfts", "@@ websearch_to_tsquery")

    ].iter().copied().collect();

    pub static ref OPERATORS_START: Vec<String> = {
        OPERATORS.keys().chain(["not","in"].iter()).chain(FTS_OPERATORS.keys()).map(|&op| format!("{op}.") )
        .chain(FTS_OPERATORS.keys().map(|&op| format!("{op}(") ))
        .collect()
    };

    pub static ref ALL_OPERATORS: HashMap<&'static str, &'static str> = {
        let mut m = OPERATORS.clone();
        m.extend(FTS_OPERATORS.clone());
        m
    };
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Resolution {
    MergeDuplicates,
    IgnoreDuplicates,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Representation {
    Full,
    None,
    HeadersOnly,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Count {
    ExactCount,
    PlannedCount,
    EstimatedCount,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Preferences {
    pub resolution: Option<Resolution>,
    pub representation: Option<Representation>,
    pub count: Option<Count>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ApiRequest<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub schema_name: &'a str,
    pub read_only: bool,
    pub accept_content_type: ContentType,
    pub query: Query<'a>,
    pub preferences: Option<Preferences>,
    pub headers: HashMap<&'a str, &'a str>,
    pub cookies: HashMap<&'a str, &'a str>,
    pub get: Vec<(&'a str, &'a str)>,
}

#[derive(Debug)]
pub struct ApiResponse {
    pub page_total: u64,
    pub total_result_set: Option<u64>,
    pub top_level_offset: u64,
    pub response_headers: Option<String>,
    pub response_status: Option<String>,
    pub body: String,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ContentType {
    ApplicationJSON,
    SingularJSON,
    TextCSV,
    Other(String),
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct ProcParam<'a> {
    pub name: &'a str,
    pub type_: &'a str,
    pub required: bool,
    pub variadic: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum CallParams<'a> {
    KeyParams(Vec<ProcParam<'a>>),
    OnePosParam(ProcParam<'a>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query<'a> {
    pub node: QueryNode<'a>,
    pub sub_selects: Vec<SubSelect<'a>>,
}

pub struct Iter<T>(VecDeque<Vec<String>>, VecDeque<T>);

pub struct Visitor<'a, 'b, F>(VecDeque<Vec<String>>, VecDeque<&'b mut Query<'a>>, F);

impl<'a, 'b> Query<'a> {
    pub fn insert_conditions(&'b mut self, conditions: Vec<(Vec<&'a str>, Condition<'a>)>) -> Result<()> {
        self.insert_properties(conditions, |q, p| {
            let query_conditions: &mut Vec<Condition<'a>> = match &mut q.node {
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
    pub fn insert_properties<T>(&'b mut self, mut properties: Vec<(Vec<&str>, T)>, f: fn(&mut Query<'a>, Vec<T>) -> Result<()>) -> Result<()> {
        //let node_properties = properties.drain_filter(|(path, _)| path.is_empty()).map(|(_, c)| c).collect::<Vec<_>>();
        let mut node_properties = vec![];
        let mut i = 0;
        while i < properties.len() {
            if properties[i].0.is_empty() {
                let (_, c) = properties.remove(i);
                node_properties.push(c);
            } else {
                i += 1;
            }
        }

        if !node_properties.is_empty() {
            f(self, node_properties)?
        };

        for SubSelect { query: q, alias, .. } in self.sub_selects.iter_mut() {
            if let QueryNode::Select { from: (table, _), .. } = &mut q.node {
                // let node_properties = properties.drain_filter(|(path, _)| match path.first() {
                //         Some(&p) => {
                //             if p == *table || Some(p) == *alias {
                //                 path.remove(0);
                //                 true
                //             } else {
                //                 false
                //             }
                //         }
                //         None => false,
                //     });
                let mut node_properties = vec![];
                let mut i = 0;
                while i < properties.len() {
                    match properties[i].0.first() {
                        Some(&p) => {
                            if p == *table || Some(p) == *alias {
                                let (mut path, c) = properties.remove(i);
                                path.remove(0);
                                node_properties.push((path, c));
                            } else {
                                i += 1
                            }
                        }
                        None => i += 1,
                    }
                }
                q.insert_properties(node_properties, f)?;
            }
        }
        Ok(())
    }

    pub fn visit<R, F: FnMut(Vec<String>, &'b mut Self) -> R>(&'b mut self, f: F) -> Visitor<'a, 'b, F> {
        Visitor(VecDeque::from([vec![String::from(self.node.name())]]), VecDeque::from([self]), f)
    }
}

impl<'a, 'b> Iterator for Iter<&'b Query<'a>> {
    type Item = (Vec<String>, &'b QueryNode<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                stack.extend(sub_selects.iter().map(|SubSelect { query, .. }| query));
                //let cp = current_path;
                path.extend(sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().to_string());
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

impl<'a, 'b> Iterator for Iter<&'b mut Query<'a>> {
    type Item = (Vec<String>, &'b mut QueryNode<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                // sub_selects.iter_mut().for_each(|SubSelect { query, .. }| {

                //     let mut p = current_path.clone();
                //     p.push(query.node.name().to_string());
                //     path.push_back(p);

                //     stack.push_back(query);
                // });
                // let stack_extend = sub_selects.iter_mut().map(|SubSelect { query, .. }| query).collect::<Vec<_>>();
                // stack_extend.into_iter().for_each(|q| stack.push_back(q));
                // let path_extend = sub_selects.iter().map(
                //     |SubSelect {
                //          query: Query { node, .. }, ..
                //      }| {
                //         let mut p = current_path.clone();
                //         p.push(node.name().to_string());
                //         p
                //     },
                // ).collect::<Vec<_>>();

                // path.extend(path_extend);
                sub_selects.iter_mut().for_each(|SubSelect { query, .. }| {
                    let mut p = current_path.clone();
                    p.push(query.node.name().to_string());
                    path.push_back(p);
                    stack.push_back(query);
                });

                Some((current_path, &mut *node))
            }
            _ => None,
        }
    }
}

impl<'a> Iterator for Iter<Query<'a>> {
    type Item = (Vec<String>, QueryNode<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                path.extend(sub_selects.iter().map(
                    |SubSelect {
                         query: Query { node, .. }, ..
                     }| {
                        let mut p = current_path.clone();
                        p.push(node.name().to_string());
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

impl<'a> IntoIterator for &Query<'a> {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().to_string()]]), VecDeque::from([self]))
    }
}

impl<'a> IntoIterator for &mut Query<'a> {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().to_string()]]), VecDeque::from([self]))
    }
}

impl<'a> IntoIterator for Query<'a> {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().to_string()]]), VecDeque::from([self]))
    }
}

// impl<'a, R, F> Iterator for Visitor<'a, F>
// where
//     F: FnMut(Vec<String>, &'a mut Query<'a>) -> R,
// {
//     type Item = R;
//     fn next(&mut self) -> Option<Self::Item> {
//         let Self(path, stack, f) = self;
//         match (path.pop_front(), stack.pop_front()) {
//             (Some(current_path), Some(query)) => {

//                 query.sub_selects.iter_mut().for_each(|SubSelect { query, .. }| {
//                     let mut p = current_path.clone();
//                     p.push(query.node.name().to_string());
//                     path.push_back(p);
//                     stack.push_back(query);
//                 });
//                 let r = (f)(current_path, query);
//                 // path.extend(query.sub_selects.iter().map(
//                 //     |SubSelect {
//                 //          query: Query { node, .. }, ..
//                 //      }| {
//                 //         let mut p = current_path.clone();
//                 //         p.push(node.name().to_string());
//                 //         p
//                 //     },
//                 // ));
//                 // stack.extend(query.sub_selects.iter_mut().map(|SubSelect { query, .. }| query));
//                 Some(r)
//             }
//             _ => None,
//         }
//     }
// }

// #[derive(Serialize, Deserialize, Clone, Debug)]
// #[serde(untagged)]
// pub enum ParamValue<'a> {
//     #[serde(borrow)]
//     Variadic(Vec<&'a JsonRawValue>),

//     #[serde(borrow)]
//     Single(&'a JsonRawValue),
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ParamValues<'a> {
    Parsed(BTreeMap<&'a str, JsonValue>),
    Raw(&'a str),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryNode<'a> {
    FunctionCall {
        fn_name: Qi<'a>,
        parameters: CallParams<'a>,
        payload: Payload<'a>,
        //parameter_values: ParamValues<'a>,
        return_table_type: Option<Qi<'a>>,
        is_scalar: bool,
        returns_single: bool,
        is_multiple_call: bool,
        returning: Vec<&'a str>,
        select: Vec<SelectItem<'a>>,
        where_: ConditionTree<'a>,
        limit: Option<SingleVal<'a>>,
        offset: Option<SingleVal<'a>>,
        order: Vec<OrderTerm<'a>>,
    },
    Select {
        select: Vec<SelectItem<'a>>,
        from: (&'a str, /*alias_sufix*/ Option<&'a str>),
        join_tables: Vec<&'a str>,
        where_: ConditionTree<'a>,
        check: Option<ConditionTree<'a>>, //used only for second stages select in cases where the db does not support returning
        limit: Option<SingleVal<'a>>,
        offset: Option<SingleVal<'a>>,
        order: Vec<OrderTerm<'a>>,
        groupby: Vec<GroupByTerm<'a>>,
    },
    Insert {
        into: &'a str,
        columns: Vec<ColumnName<'a>>,
        payload: Payload<'a>,
        check: ConditionTree<'a>,
        where_: ConditionTree<'a>, //used only for put
        returning: Vec<&'a str>,
        select: Vec<SelectItem<'a>>,
        on_conflict: Option<(Resolution, Vec<&'a str>)>,
    },
    Delete {
        from: &'a str,
        where_: ConditionTree<'a>,
        returning: Vec<&'a str>,
        select: Vec<SelectItem<'a>>,
    },

    Update {
        table: &'a str,
        columns: Vec<ColumnName<'a>>,
        payload: Payload<'a>,
        check: ConditionTree<'a>,
        where_: ConditionTree<'a>,
        returning: Vec<&'a str>,
        select: Vec<SelectItem<'a>>,
    },
}

impl<'a> QueryNode<'a> {
    pub fn name(&self) -> &str {
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
            Self::Select { select, .. }
            | Self::Update { select, .. }
            | Self::Insert { select, .. }
            | Self::Delete { select, .. }
            | Self::FunctionCall { select, .. } => select,
        }
    }
    pub fn where_(&self) -> &ConditionTree {
        match self {
            Self::Select { where_, .. }
            | Self::Update { where_, .. }
            | Self::Insert { where_, .. }
            | Self::Delete { where_, .. }
            | Self::FunctionCall { where_, .. } => where_,
        }
    }
    // pub fn where_as_mut(&mut self) -> &mut ConditionTree {
    //     match self {
    //         Self::Select { where_, .. }
    //         | Self::Update { where_, .. }
    //         | Self::Insert { where_, .. }
    //         | Self::Delete { where_, .. }
    //         | Self::FunctionCall { where_, .. } => where_,
    //     }
    // }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OrderTerm<'a> {
    pub term: Field<'a>,
    pub direction: Option<OrderDirection>,
    pub null_order: Option<OrderNulls>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct GroupByTerm<'a>(pub Field<'a>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum OrderNulls {
    NullsFirst,
    NullsLast,
}

pub type JoinHint<'a> = &'a str;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Deserialize, Serialize)]
pub struct Qi<'a>(pub &'a str, pub &'a str);

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct ForeignKey<'a> {
    pub name: &'a str,
    #[serde(borrow)]
    pub table: Qi<'a>,
    #[serde(borrow)]
    pub columns: Vec<&'a str>,
    #[serde(borrow)]
    pub referenced_table: Qi<'a>,
    #[serde(borrow)]
    pub referenced_columns: Vec<&'a str>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Join<'a> {
    #[serde(borrow)]
    Child(ForeignKey<'a>),
    #[serde(borrow)]
    Parent(ForeignKey<'a>),
    Many(#[serde(borrow)] Qi<'a>, #[serde(borrow)] ForeignKey<'a>, #[serde(borrow)] ForeignKey<'a>),
}

#[derive(Debug, PartialEq)]
pub enum SelectKind<'a> {
    Item(SelectItem<'a>),
    Sub(Box<SubSelect<'a>>), //TODO! check performance implications for using box
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum FunctionParam<'a> {
    #[serde(borrow)]
    Fld(Field<'a>),
    Val(SingleVal<'a>, Option<&'a str>),
    Func {
        fn_name: &'a str,
        #[serde(borrow)]
        parameters: Vec<FunctionParam<'a>>,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum SelectItem<'a> {
    //TODO!!! better name
    Star,
    Simple {
        field: Field<'a>,
        alias: Option<&'a str>,
        cast: Option<&'a str>,
    },
    Func {
        fn_name: &'a str,
        parameters: Vec<FunctionParam<'a>>,
        partitions: Vec<Field<'a>>,
        orders: Vec<OrderTerm<'a>>,
        alias: Option<&'a str>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubSelect<'a> {
    pub query: Query<'a>,
    pub alias: Option<&'a str>,
    pub hint: Option<JoinHint<'a>>,
    pub join: Option<Join<'a>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct ConditionTree<'a> {
    #[serde(rename = "logic_op")]
    pub operator: LogicOperator,
    #[serde(borrow)]
    pub conditions: Vec<Condition<'a>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
pub enum Condition<'a> {
    Group {
        #[serde(default, skip_serializing_if = "is_default")]
        negate: Negate,
        #[serde(borrow)]
        tree: ConditionTree<'a>,
    },
    Single {
        #[serde(borrow, flatten)]
        field: Field<'a>,
        #[serde(borrow, flatten)]
        filter: Filter<'a>,
        #[serde(default, skip_serializing_if = "is_default")]
        negate: Negate,
    },
    Foreign {
        #[serde(borrow)]
        left: (Qi<'a>, Field<'a>),
        #[serde(borrow)]
        right: (Qi<'a>, Field<'a>),
    },
    Raw {
        sql: &'a str,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
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
                    Err(serde::de::Error::custom(format!("invalid trilean value: {s}")))
                }
            }
            _ => Err(serde::de::Error::custom(format!("invalid trilean value: {v}"))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct EnvVar<'a> {
    #[serde(rename = "env")]
    pub var: &'a str,
    #[serde(rename = "env_part")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(borrow, default)]
    pub part: Option<&'a str>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum Filter<'a> {
    Op(Operator<'a>, SingleVal<'a>),
    In(ListVal<'a>),
    Is(TrileanVal),
    Fts(Operator<'a>, Option<Language<'a>>, SingleVal<'a>),
    Col(Qi<'a>, Field<'a>),
    Env(Operator<'a>, EnvVar<'a>),
}
impl<'a> Serialize for Filter<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Filter::Op(operator, value) => FilterHelper::Op {
                operator,
                value: value.clone(),
            },
            Filter::In(value) => FilterHelper::In { value: value.clone() },
            Filter::Is(value) => FilterHelper::Is { value: value.clone() },
            Filter::Fts(operator, language, value) => FilterHelper::Fts {
                operator,
                language: language.clone(),
                value: value.clone(),
            },
            Filter::Col(qi, field) => FilterHelper::Col {
                qi: qi.clone(),
                field: field.clone(),
            },
            Filter::Env(operator, var) => FilterHelper::Env { operator, var: var.clone() },
        }
        .serialize(serializer)
    }
}
impl<'a, 'de: 'a> Deserialize<'de> for Filter<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|v| match v {
            FilterHelper::Op { operator, value } => Filter::Op(operator, value),
            FilterHelper::In { value } => Filter::In(value),
            FilterHelper::Is { value } => Filter::Is(value),
            FilterHelper::Fts { operator, language, value } => Filter::Fts(operator, language, value),
            FilterHelper::Col { qi, field } => Filter::Col(qi, field),
            FilterHelper::Env { operator, var } => Filter::Env(operator, var),
        })
    }
}

// private
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", untagged)]
enum FilterHelper<'a> {
    Op {
        #[serde(borrow, rename = "op")]
        operator: Operator<'a>,
        #[serde(borrow, rename = "val")]
        value: SingleVal<'a>,
    },
    In {
        #[serde(borrow, rename = "in")]
        value: ListVal<'a>,
    },
    Is {
        #[serde(rename = "is")]
        value: TrileanVal,
    },
    Fts {
        #[serde(borrow, rename = "fts_op")]
        operator: Operator<'a>,
        #[serde(borrow, default, skip_serializing_if = "is_default")]
        language: Option<Language<'a>>,
        #[serde(borrow, rename = "val")]
        value: SingleVal<'a>,
    },

    Env {
        #[serde(borrow, rename = "op")]
        operator: Operator<'a>,
        #[serde(borrow, flatten)]
        var: EnvVar<'a>,
    },
    Col {
        #[serde(borrow)]
        qi: Qi<'a>,
        #[serde(borrow)]
        field: Field<'a>,
    },
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Field<'a> {
    #[serde(borrow, rename = "column")]
    pub name: ColumnName<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(borrow, default)]
    pub json_path: Option<Vec<JsonOperation<'a>>>, //TODO!! should contain some info about the data type so that fmt_field function could make better decisions
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum JsonOperation<'a> {
    #[serde(borrow, rename = "->")]
    JArrow(JsonOperand<'a>),
    #[serde(borrow, rename = "->>")]
    J2Arrow(JsonOperand<'a>),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum JsonOperand<'a> {
    JKey(&'a str),
    JIdx(&'a str),
}
impl<'a> serde::Serialize for JsonOperand<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::JKey(s) => serializer.serialize_str(format!("'{s}'").as_str()),
            Self::JIdx(s) => serializer.serialize_str(s),
        }
    }
}
impl<'a, 'de: 'a> serde::Deserialize<'de> for JsonOperand<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        //deserialize everything into a &str and then parse it
        let s: &'de str = serde::Deserialize::deserialize(deserializer)?;
        // trim white spaces
        let s = s.trim();
        if s.starts_with('\'') && s.ends_with('\'') {
            Ok(Self::JKey(&s[1..s.len() - 1]))
        } else {
            Ok(Self::JIdx(s))
        }
    }
}

pub type Operator<'a> = &'a str;
pub type Negate = bool;
pub type Language<'a> = SingleVal<'a>;
pub type ColumnName<'a> = &'a str;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Payload<'a>(pub Cow<'a, str>, pub Option<Cow<'a, str>>);

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SingleVal<'a>(pub Cow<'a, str>, pub Option<Cow<'a, str>>);
impl<'a> Serialize for SingleVal<'a> {
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
            }
            SingleVal(v, None) => serializer.serialize_str(v),
        }
    }
}
impl<'a, 'de: 'a> Deserialize<'de> for SingleVal<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::String(s) => Ok(Self(Cow::Owned(s), None)),
            JsonValue::Object(o) => match (o.get("v"), o.get("t")) {
                (Some(JsonValue::String(v)), Some(JsonValue::String(t))) => Ok(SingleVal(Cow::Owned(v.clone()), Some(Cow::Owned(t.clone())))),
                _ => Err(serde::de::Error::custom("Invalid SingleVal")),
            },
            _ => Err(serde::de::Error::custom("Invalid SingleVal")),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ListVal<'a>(pub Vec<Cow<'a, str>>, pub Option<Cow<'a, str>>);

impl<'a> Serialize for ListVal<'a> {
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
            }
            ListVal(v, None) => {
                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for element in v {
                    seq.serialize_element(element)?;
                }
                seq.end()
            }
        }
    }
}

impl<'a, 'de: 'a> Deserialize<'de> for ListVal<'a> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // TODO!!! flatten will eliminate None and we need to treat it as an error
        let to_str_vec = |v: &Vec<JsonValue>| {
            v.iter()
                .filter_map(|v: &JsonValue| v.as_str())
                .map(|s| Cow::Owned(s.to_string()))
                .collect()
        };
        let v = JsonValue::deserialize(deserializer)?;
        match v {
            JsonValue::Array(v) => Ok(Self(to_str_vec(&v), None)),
            JsonValue::Object(o) => match (o.get("v"), o.get("t")) {
                (Some(JsonValue::Array(v)), Some(JsonValue::String(t))) => Ok(Self(to_str_vec(v), Some(Cow::Owned(t.clone())))),
                _ => Err(serde::de::Error::custom("Invalid SingleVal")),
            },
            _ => Err(serde::de::Error::custom("Invalid SingleVal")),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
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
    //fn ss(s: &str) -> String { s.to_string() }
    fn cow(s: &str) -> Cow<str> {
        Cow::Borrowed(s)
    }
    #[test]
    fn serialize() {
        assert_eq!(r#"["schema","table"]"#, serde_json::to_string(&Qi("schema", "table")).unwrap());
        assert_eq!(serde_json::from_str::<Qi>(r#"["schema","table"]"#).unwrap(), Qi("schema", "table"));
        assert_eq!(r#""and""#, serde_json::to_string(&LogicOperator::And).unwrap());
        assert_eq!(serde_json::from_str::<LogicOperator>(r#""and""#).unwrap(), LogicOperator::And);
        assert_eq!(r#""10""#, serde_json::to_string(&SingleVal(cow("10"), None)).unwrap());
        assert_eq!(serde_json::from_str::<SingleVal>(r#""10""#).unwrap(), SingleVal(cow("10"), None));
        assert_eq!(r#"["1","2","3"]"#, serde_json::to_string(&ListVal(vec![cow("1"), cow("2"), cow("3")], None)).unwrap());
        assert_eq!(serde_json::from_str::<ListVal>(r#"["1","2","3"]"#).unwrap(), ListVal(vec![cow("1"), cow("2"), cow("3")], None));
        assert_eq!(r#"{"column":"id"}"#, serde_json::to_string(&Field { name: "id", json_path: None }).unwrap());
        assert_eq!(serde_json::from_str::<Field>(r#"{"column":"id"}"#).unwrap(), Field { name: "id", json_path: None });
        assert_eq!(r#"{"op":"eq","val":"10"}"#, serde_json::to_string(&Filter::Op("eq", SingleVal(cow("10"), None))).unwrap());
        assert_eq!(serde_json::from_str::<Filter>(r#"{"op":"eq","val":"10"}"#).unwrap(), Filter::Op("eq", SingleVal(cow("10"), None)));
        assert_eq!(
            r#"{"column":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#,
            serde_json::to_string(&Field {
                name: "id",
                json_path: Some(vec![
                    JsonOperation::JArrow(JsonOperand::JKey("id")),
                    JsonOperation::J2Arrow(JsonOperand::JIdx("0"))
                ])
            })
            .unwrap()
        );
        assert_eq!(
            serde_json::from_str::<Field>(r#"{"column":"id","json_path":[{"->":"'id'"},{"->>":"0"}]}"#).unwrap(),
            Field {
                name: "id",
                json_path: Some(vec![
                    JsonOperation::JArrow(JsonOperand::JKey("id")),
                    JsonOperation::J2Arrow(JsonOperand::JIdx("0"))
                ])
            }
        );
        assert_eq!(
            r#"{"column":"id","op":"eq","val":"10"}"#,
            serde_json::to_string(&Condition::Single {
                field: Field { name: "id", json_path: None },
                filter: Filter::Op("eq", SingleVal(cow("10"), None)),
                negate: false,
            })
            .unwrap()
        );
        assert_eq!(
            serde_json::from_str::<Condition>(r#"{"column":"id","op":"eq","val":"10"}"#).unwrap(),
            Condition::Single {
                field: Field { name: "id", json_path: None },
                filter: Filter::Op("eq", SingleVal(cow("10"), None)),
                negate: false,
            }
        );
        assert_eq!(
            serde_json::from_str::<Condition>(r#"{"tree":{"logic_op":"and","conditions":[{"column":"id","op":"eq","val":"10"}]}}"#).unwrap(),
            Condition::Group {
                negate: false,
                tree: ConditionTree {
                    operator: LogicOperator::And,
                    conditions: vec![Condition::Single {
                        field: Field { name: "id", json_path: None },
                        filter: Filter::Op("eq", SingleVal(cow("10"), None)),
                        negate: false,
                    },],
                }
            }
        );
        assert_eq!(
            r#"{"tree":{"logic_op":"and","conditions":[{"column":"id","op":"eq","val":"10"}]}}"#,
            serde_json::to_string(&Condition::Group {
                negate: false,
                tree: ConditionTree {
                    operator: LogicOperator::And,
                    conditions: vec![Condition::Single {
                        field: Field { name: "id", json_path: None },
                        filter: Filter::Op("eq", SingleVal(cow("10"), None)),
                        negate: false,
                    },],
                }
            })
            .unwrap()
        );
        assert_eq!(serde_json::from_str::<Condition>(r#"{"sql":"false"}"#).unwrap(), Condition::Raw { sql: "false" });
    }
}
