
use serde::{Deserialize, Serialize};
pub use http::Method;
use std::collections::{HashMap,};

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
pub enum Query {
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
    }
    
}

// impl Query {
//     pub fn node(self) -> String {
//         match self {
//             Query::Select {from,..}=>from.0,
//             Query::FunctionCall {fn_name, ..} => fn_name.1,
//             Query::Insert {into, ..} => into
//         }
//     }
//     fn values(&self) -> Inspector {
//         Inspector { 
//             iter: 
//                 match self {
//                     Query::Select {select, ..} => select,
//                     Query::Insert {select, ..} => select,
//                     Query::FunctionCall { select, .. } => select
//                 }
//                 .iter()
//                 .filter_map(|i| match i { SelectItem::SubSelect { query, ..} => Some(query), _ => None})
//         }
        
//     }
// }



// struct Inspector<'a> {
//     iter: std::slice::Iter<'a, Query>
// }

// impl<'a> Iterator for Inspector<'a> {
//     type Item = &'a Query;
//     fn next(&mut self) -> Option<Self::Item> {
//         self.iter.next()
//     }
// }
// impl IntoIterator for Query {
//     type Item = Query;
//     type IntoIter = IntoIter<Query>;

//     fn into_iter(mut self) -> IntoIter<Query> { 
//         vec![self].into_iter()
//     }
// }
// impl<'a> IntoIterator for &'a Query
// impl<'a> IntoIterator for &'a mut Query

// pub struct Iter<T>(VecDeque<T>);
// impl<'a> Iterator for Iter<&'a Query> {
//     type Item = &'a Query;
//     fn next(&mut self) -> Option<Self::Item> {
//         let Self(stack) = self;
//         stack.pop_front().map(|q| {
//             stack.extend(
//                 match q {
//                     Query::Select {select, ..} => select,
//                     Query::Insert {select, ..} => select,
//                     Query::FunctionCall { select, .. } => select
//                 }.iter()
//                 .filter_map(|i| match i { SelectItem::SubSelect { query, ..} => Some(query), _ => None})
//                 // .flat_map(|o| Some(&**o.as_ref()?))
//             );
//             q
//         })
//     }
// }
// impl<'a> Iterator for Iter<&'a mut Query> {
//     type Item = &'a mut Query;
//     fn next(&mut self) -> Option<Self::Item> {
//         let Self(stack) = self;
//         stack.pop_front().map(|q| {
//             stack.extend(
//                 match q {
//                     Query::Select {select, ..} => select,
//                     Query::Insert {select, ..} => select,
//                     Query::FunctionCall { select, .. } => select
//                 }.iter_mut()
//                 .filter_map(|i| match i { SelectItem::SubSelect { query, ..} => Some(query), _ => None})
//                 .flat_map(|o| Some(&mut **o.as_mut()?))
//             );
//             q
//         })
//     }
// }
// impl Iterator for Iter<Query> {
//     type Item = Query;
//     fn next(&mut self) -> Option<Self::Item> {
//         let Self(stack) = self;
//         stack.pop_front().map(|q| {
//             stack.extend(
//                 match q {
//                     Query::Select {select, ..} => select,
//                     Query::Insert {select, ..} => select,
//                     Query::FunctionCall { select, .. } => select
//                 }.into_iter()
//                 .filter_map(|i| match i { SelectItem::SubSelect { query, ..} => Some(query), _ => None})
//                 //vec![].into_iter()
//                 //select(q).into_iter()
//                 //.filter_map(|i| match i { SelectItem::SubSelect { query, ..} => Some(query), _ => None})
//             );
//             q
//         })
//     }
// }
// impl<'a> IntoIterator for &'a Query {
//     type Item = <Self::IntoIter as Iterator>::Item;
//     type IntoIter = Iter<Self>;
//     fn into_iter(self) -> Self::IntoIter {
//         Iter(VecDeque::from([self]))
//     }
// }
// impl<'a> IntoIterator for &'a mut Query {
//     type Item = <Self::IntoIter as Iterator>::Item;
//     type IntoIter = Iter<Self>;
//     fn into_iter(self) -> Self::IntoIter {
//         Iter(VecDeque::from([self]))
//     }
// }
// impl IntoIterator for Query {
//     type Item = <Self::IntoIter as Iterator>::Item;
//     type IntoIter = Iter<Self>;
//     fn into_iter(self) -> Self::IntoIter {
//         Iter(VecDeque::from([self]))
//     }
// }
// fn subselects(i: &SelectItem) -> Option<&Query> {
//     match i {
//         SelectItem::SubSelect { query, ..} => Some(query),
//         _ => None
//     }
// }

// fn select(q: &Query) -> &Vec<SelectItem> {
//     match q {
//         Query::Select {select, ..} => select,
//         Query::Insert {select, ..} => select,
//         Query::FunctionCall { select, .. } => select
//     }
// }
// impl<'a> IntoIterator for &'a Query {
//     type Item = <Self::IntoIter as Iterator>::Item;
//     type IntoIter = Iter<Self>;
//     fn into_iter(self) -> Self::IntoIter {
//         Bft::new(&self, |q| {
//             match q {
//                 Query::Select {select, ..} => select,
//                 Query::Insert {select, ..} => select,
//                 Query::FunctionCall { select, .. } => select
//             }
//             .iter()
//             .filter_map(|s| 
//                 match s {
//                     SelectItem::SubSelect { query, ..} => Some(query),
//                     _ => None
//                 }
//             )
//         })
//     }
// }

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
pub enum SelectItem {
    //TODO!!! better name
    Star,
    Simple {
        field: Field,
        alias: Option<String>,
        cast: Option<String>,
    },
    SubSelect {
        query: Query,
        alias: Option<String>,
        hint: Option<JoinHint>,
        join: Option<Join>
    }
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