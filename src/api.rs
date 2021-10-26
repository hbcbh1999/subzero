
//use rocket::http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};

pub use http::Method;

#[derive(Debug, PartialEq)]
pub struct ApiRequest<'r> {
    // pub root: String,
    pub method: Method,
    // pub headers: &'r HeaderMap<'r>,
    pub query: Query<'r>,
}
/*
query {
    projects( where: {rating: {_gte: 4}} ) {
        id
        name
        client {
            id
            name
        }

        tasks( where: {completed: {_eq: true}} ){
            id
            name
        }
    }
}
*/

#[derive(Debug, PartialEq)]
pub enum Query<'r> {
    Select {
        select: Vec<SelectItem<'r>>,
        from: String,
        where_: ConditionTree,
    },
    Insert {
        into: String,
        columns: Vec<String>,
        payload: &'r str,
        where_: ConditionTree, //used only for put
        returning: Vec<String>,
        select: Vec<SelectItem<'r>>,
        //, onConflict :: Maybe (PreferResolution, [FieldName])
    }
    
}

pub type JoinHint = String;
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Qi (pub String, pub String);

//#[derive(Debug, PartialEq, Clone)]
// pub enum JoinType {Child, Parent, Many}

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey {
    pub name: String,
    pub table: Qi,
    pub columns: Vec<String>,
    pub referenced_table: Qi,
    pub referenced_columns: Vec<String>
}

//#[derive(Debug, PartialEq, Clone)]
// pub enum QueryType {
//     Select, Insert, Update, Delete, Upsert
// }


#[derive(Debug, PartialEq, Clone)]
pub enum Join {
    Child (ForeignKey),
    Parent (ForeignKey),
    Many (String, ForeignKey, ForeignKey),

    // pub kind: JoinType,
    // pub foreign_key: ForeignKey,
}

#[derive(Debug, PartialEq)]
pub enum SelectItem<'r> {
    //TODO!!! better name
    Simple {
        field: Field,
        alias: Option<String>,
    },
    SubSelect {
        query: Query<'r>,
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
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Filter {
    Op (Operator, SingleVal),
    In (ListVal),
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
pub type SingleVal = String;
pub type Language = String;
pub type ListVal = Vec<SingleVal>;
pub type Negate = bool;

#[derive(Debug, PartialEq, Clone)]
pub enum LogicOperator { And, Or }





