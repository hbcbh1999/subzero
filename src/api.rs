
//use rocket::http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};
use core::fmt::Debug;
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
#[derive(Debug,PartialEq,Clone)]
pub struct SingleVal(pub String);
pub type Language = String;
#[derive(Debug,PartialEq,Clone)]
pub struct ListVal(pub Vec<String>);
pub type Negate = bool;

// impl From<ListVal> for &str {
//     fn from(l: ListVal) -> Self {
//         match l {
//             ListVal(v) => format!("{{\"{}\"}}", v.join("\",\"")).as_str()
//         }
//     }
// }



// pub trait Param where Self: core::fmt::Debug + Sync{
//     fn as_strr<'a>(self) -> String;
// }

// impl From<&dyn Param> for String {
//     fn from(l: &dyn Param) -> Self {
//         l.as_strr()
//     }
// }

// impl Debug for dyn Param {
//     fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//         write!(f, "{:?}", self)
//     }
// }

// impl Param for SingleVal {
//     fn as_strr<'a>(self) -> String{
//         self
//     }
// }

// impl Param for ListVal {
//     fn as_strr<'a>(self) -> String{
//         match self {
//             ListVal(v) => format!("{{\"{}\"}}", v.join("\",\""))
//         }
//     }
// }

#[derive(Debug, PartialEq, Clone)]
pub enum LogicOperator { And, Or }





