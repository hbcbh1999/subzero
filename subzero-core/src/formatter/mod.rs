
use crate::api::{ListVal, SingleVal, Payload};
use crate::dynamic_statement::{SqlSnippet};
use std::fmt;

pub mod base;
#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug)]
pub enum Param<'a> {
    LV(&'a ListVal),
    SV(&'a SingleVal),
    PL(&'a Payload),
    TV(&'a str),
}
// helper type aliases
pub trait ToParam: fmt::Debug {
    fn to_param(&self) -> Param;
    fn to_data_type(&self) -> &Option<String>;
}

pub type SqlParam<'a> = (dyn ToParam + Sync + 'a);
pub type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;

impl ToParam for ListVal {
    fn to_param(&self) -> Param {Param::LV(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl ToParam for SingleVal {
    fn to_param(&self) -> Param {Param::SV(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl<'a> ToParam for &'a str {
    fn to_param(&self) -> Param {Param::TV(&self)}
    fn to_data_type(&self) -> &Option<String> {
        &None
    }
}

impl ToParam for Payload {
    fn to_param(&self) -> Param {Param::PL(self)}
    fn to_data_type(&self) -> &Option<String> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}
