
use serde::{Deserialize, Serialize};
pub use http::Method;
use std::collections::{HashMap,VecDeque};

#[derive(Debug, PartialEq)]
pub enum Resolution {MergeDuplicates, IgnoreDuplicates}
#[derive(Debug, PartialEq)]
pub enum Representation {Full, None, HeadersOnly}
// #[derive(Debug, PartialEq)]
// pub enum Parameters {SingleObject, MultipleObjects}
#[derive(Debug, PartialEq)]
pub enum Count {ExactCount, PlannedCount, EstimatedCount}
// #[derive(Debug, PartialEq)]
// pub enum Transaction {Commit, Rollback}

#[derive(Debug, PartialEq)]
pub struct Preferences {
    pub resolution: Option<Resolution>,
    pub representation: Option<Representation>,
    // pub parameters: Option<Parameters>,
    pub count: Option<Count>,
    //pub transaction: Option<Transaction>,
}

#[derive(Debug, PartialEq)]
pub struct ApiRequest<'r> {
    pub method: Method,
    pub path: String,
    pub accept_content_type: ContentType,
    pub query: Query,
    pub preferences: Option<Preferences>,
    pub headers: &'r HashMap<&'r str, &'r str>,
    pub cookies: &'r HashMap<&'r str, &'r str>,
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct Query {
    pub node: QueryNode,
    pub sub_selects: Vec<SubSelect>,
}
pub struct Iter<T>(VecDeque<Vec<String>>,VecDeque<T>);
pub struct Visitor<'a, F>(VecDeque<Vec<String>>,VecDeque<&'a mut Query>, F);
impl Query {
    pub fn visit<R, F: FnMut(Vec<String>, &mut Self) -> R>(&mut self, f: F) -> Visitor<F> {
        Visitor(VecDeque::from([vec![self.node.name().clone()]]),VecDeque::from([self]), f)
    }
}
impl<'a> Iterator for Iter<&'a Query> {
    type Item = (Vec<String>, &'a QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                stack.extend(sub_selects.iter().map(|SubSelect {query,..}| query));
                path.extend(sub_selects.iter().map(|SubSelect { query: Query { node, .. }, .. }| {
                    let mut p = current_path.clone();
                    p.push(node.name().clone());
                    p
                }));
                Some((current_path, &node))
            }
            _ => None
        }
    }
}

impl<'a> Iterator for Iter<&'a mut Query> {
    type Item = (Vec<String>, &'a mut QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                path.extend(sub_selects.iter().map(|SubSelect { query: Query { node, .. }, .. }| {
                    let mut p = current_path.clone();
                    p.push(node.name().clone());
                    p
                }));
                stack.extend(sub_selects.iter_mut().map(|SubSelect {query,..}| query));
                Some((current_path, &mut *node))
            }
            _ => None
        }
    }
}

impl Iterator for Iter<Query> {
    type Item = (Vec<String>, QueryNode);
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(Query { node, sub_selects, .. })) => {
                path.extend(sub_selects.iter().map(|SubSelect { query: Query { node, .. }, .. }| {
                    let mut p = current_path.clone();
                    p.push(node.name().clone());
                    p
                }));
                stack.extend(sub_selects.into_iter().map(|SubSelect {query,..}| query));
                Some((current_path,node))
            }
            _ => None
        }
    }
}

impl<'a> IntoIterator for &'a Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().clone()]]),VecDeque::from([self]))
    }
}

impl<'a> IntoIterator for &'a mut Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().clone()]]),VecDeque::from([self]))
    }
}

impl IntoIterator for Query {
    type Item = <Self::IntoIter as Iterator>::Item;
    type IntoIter = Iter<Self>;
    fn into_iter(self) -> Self::IntoIter {
        Iter(VecDeque::from([vec![self.node.name().clone()]]),VecDeque::from([self]))
    }
}

impl<R, F> Iterator for Visitor<'_, F> where F: FnMut(Vec<String>, &mut Query) -> R {
    type Item = R;
    fn next(&mut self) -> Option<Self::Item> {
        let Self(path, stack, f) = self;
        match (path.pop_front(), stack.pop_front()) {
            (Some(current_path), Some(query)) => {
                let r = (f)(current_path.clone(), query);
                path.extend(query.sub_selects.iter().map(|SubSelect { query: Query { node, .. }, .. }| {
                    let mut p = current_path.clone();
                    p.push(node.name().clone());
                    p
                }));
                stack.extend(query.sub_selects.iter_mut().map(|SubSelect {query,..}| query));
                Some(r)
            }
            _ => None
        }
    }
}

#[derive(Debug, PartialEq)]
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
        order: Vec<OrderTerm>
    },
    Select {
        select: Vec<SelectItem>,
        from: (String, Option<String>),
        join_tables: Vec<String>,
        where_: ConditionTree,
        limit: Option<SingleVal>,
        offset: Option<SingleVal>,
        order: Vec<OrderTerm>
    },
    Insert {
        into: String,
        columns: Vec<String>,
        payload: Payload,
        where_: ConditionTree, //used only for put
        returning: Vec<String>,
        select: Vec<SelectItem>,
        //, onConflict :: Maybe (PreferResolution, [FieldName])
    },
    Delete {
        from: String,
        where_: ConditionTree, //used only for put
        returning: Vec<String>,
        select: Vec<SelectItem>,
    }

}

impl QueryNode {
    pub fn name(&self) -> &String {
        match self {
            Self::FunctionCall {fn_name:Qi(_,n),..} => n,
            Self::Select {from:(t,_),..} => t,
            Self::Insert {into,..} => into,
            Self::Delete {from,..} => from,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct OrderTerm {
    pub term:  Field,
    pub direction: Option<OrderDirection>,
    pub null_order:  Option<OrderNulls>,
}

#[derive(Debug, PartialEq)]
pub enum OrderDirection { Asc, Desc}

#[derive(Debug, PartialEq)]
pub enum OrderNulls { NullsFirst, NullsLast }

pub type JoinHint = String;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Qi (pub String, pub String);

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub table: Qi,
    pub columns: Vec<String>,
    pub referenced_table: Qi,
    pub referenced_columns: Vec<String>
}

#[derive(Debug, PartialEq, Clone)]
pub enum Join {
    Child (ForeignKey),
    Parent (ForeignKey),
    Many (Qi, ForeignKey, ForeignKey),
}

#[derive(Debug, PartialEq)]
pub enum SelectKind {
    Item(SelectItem),
    Sub(SubSelect),
}

#[derive(Debug, PartialEq)]
pub enum SelectItem {
    //TODO!!! better name
    Star,
    Simple {
        field: Field,
        alias: Option<String>,
        cast: Option<String>,
    },
}

#[derive(Debug, PartialEq)]
pub struct SubSelect {
    pub query: Query,
    pub alias: Option<String>,
    pub hint: Option<JoinHint>,
    pub join: Option<Join>
}

#[derive(Debug, PartialEq, Clone)]
pub struct ConditionTree {
    pub operator: LogicOperator,
    pub conditions: Vec<Condition>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Condition {
    Group (Negate, ConditionTree), 
    Single {
        field: Field,
        filter: Filter,
        negate: Negate,
    },
    Foreign {
        left: (Qi, Field),
        right: (Qi, Field)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TrileanVal {TriTrue, TriFalse, TriNull, TriUnknown}

#[derive(Debug, PartialEq, Clone)]
pub enum Filter {
    Op (Operator, SingleVal),
    In (ListVal),
    Is (TrileanVal),
    Fts (Operator, Option<Language>, SingleVal),
    Col (Qi, Field)
}

#[derive(Debug, PartialEq, Clone)]
pub struct Field {
    pub name: String,
    pub json_path: Option<Vec<JsonOperation>>
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonOperation {
    JArrow (JsonOperand),
    J2Arrow (JsonOperand)
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonOperand {
    JKey(String),
    JIdx(String),
}



pub type Operator = String;
pub type Negate = bool;
pub type Language = SingleVal;

#[derive(Debug, PartialEq)]
pub struct Payload(pub String);

#[derive(Debug,PartialEq,Clone)]
pub struct SingleVal(pub String);

#[derive(Debug,PartialEq,Clone)]
pub struct ListVal(pub Vec<String>);

#[derive(Debug, PartialEq, Clone)]
pub enum LogicOperator { And, Or }


// #[cfg(test)]
// mod tests {
//     use super::{*,Query::*, LogicOperator::*, SelectItem::*,};
//     fn s(s:&str) -> String {s.to_string()}
    
//     #[test]
//     fn query_iter() {
//         let q_c = Select { order: vec![], limit: None, offset: None,
//             select: vec![
//                 Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
//             ],
//             from: (s("users"),None),
//             join_tables: vec![],
//             where_: ConditionTree { operator: And, conditions: vec![]}
//         };
//         let q_a = Select { order: vec![], limit: None, offset: None,
//             select: vec![
//                 Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
//             ],
//             from: (s("clients"),None),
//             join_tables: vec![],
//             where_: ConditionTree { operator: And, conditions: vec![]}
//         };
//         let q_b = Select { order: vec![], limit: None, offset: None,
//             select: vec![
//                 Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
//                 SubSelect{
//                     query: q_c,
//                     alias: None,
//                     hint: None,
//                     join: None,
//                 },
//             ],
//             from: (s("tasks"),None),
//             join_tables: vec![],
//             where_: ConditionTree { operator: And, conditions: vec![]}
//         };
//         let q_root = Select { order: vec![], limit: None, offset: None,
//             select: vec![
//                 Simple {field: Field {name: s("id"), json_path: None}, alias: None, cast: None},
//                 SubSelect{
//                     query: q_a,
//                     alias: None,
//                     hint: None,
//                     join: None,
//                 },
//                 SubSelect{
//                     query: q_b,
//                     hint: None,
//                     alias: None,
//                     join: None
//                 }
//             ],
//             from: (s("projects"),None),
//             join_tables: vec![],
//             //from_alias: None,
//             where_: ConditionTree { operator: And, conditions: vec![] }
//         };
        

//         let iter = q_root.into_iter();
        
//         assert_eq!(iter.next().node().as_str(), Some("A"));
//         assert_eq!(iter.next().node().as_str(), Some("B"));
//         assert_eq!(iter.next().node().as_str(), Some("C"));
//         assert_eq!(iter.next().node().as_str(), Some("D"));
//     }
// }