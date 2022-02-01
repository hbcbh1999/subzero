
//use rocket::http::{HeaderMap, Method};
use serde::{Deserialize, Serialize};
use core::fmt::Debug;
pub use http::Method;
use std::collections::HashMap;

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
