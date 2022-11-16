use crate::api::{ListVal, SingleVal, Payload};
use crate::dynamic_statement::{SqlSnippet};
use std::fmt;
use std::borrow::Cow;

pub mod base;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug)]
pub enum Param<'a> {
    LV(&'a ListVal<'a>),
    SV(&'a SingleVal<'a>),
    PL(&'a Payload<'a>),
    Str(&'a str),
    StrOwned(&'a String),
}
// helper type aliases
pub trait ToParam: fmt::Debug {
    fn to_param(&self) -> Param;
    fn to_data_type(&self) -> &Option<Cow<str>>;
}

pub type SqlParam<'a> = (dyn ToParam + Sync + 'a);
pub type Snippet<'a> = SqlSnippet<'a, SqlParam<'a>>;

impl<'a> ToParam for ListVal<'a> {
    fn to_param(&self) -> Param { Param::LV(self) }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl<'a> ToParam for SingleVal<'a> {
    fn to_param(&self) -> Param { Param::SV(self) }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl<'a> ToParam for &'a str {
    fn to_param(&self) -> Param { Param::Str(self) }
    fn to_data_type(&self) -> &Option<Cow<str>> { &None }
}

impl<'a> ToParam for String {
    fn to_param(&self) -> Param { Param::StrOwned(self) }
    fn to_data_type(&self) -> &Option<Cow<str>> { &None }
}

impl<'a> ToParam for Payload<'a> {
    fn to_param(&self) -> Param { Param::PL(self) }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}
