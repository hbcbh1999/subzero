// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use crate::api::{ListVal, SingleVal, Payload};
pub use crate::dynamic_statement::SqlSnippet;
use std::fmt;
use std::borrow::Cow;

pub mod base;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "mysql")]
pub mod mysql;
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
    fn to_param(&self) -> Param {
        Param::LV(self)
    }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl<'a> ToParam for SingleVal<'a> {
    fn to_param(&self) -> Param {
        Param::SV(self)
    }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}

impl<'a> ToParam for &'a str {
    fn to_param(&self) -> Param {
        Param::Str(self)
    }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        &Some(Cow::Borrowed("text"))
    }
}

impl ToParam for String {
    fn to_param(&self) -> Param {
        Param::StrOwned(self)
    }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        &Some(Cow::Borrowed("text"))
    }
}

impl<'a> ToParam for Payload<'a> {
    fn to_param(&self) -> Param {
        Param::PL(self)
    }
    fn to_data_type(&self) -> &Option<Cow<str>> {
        //println!("to_data_type {:?}", &self);
        &self.1
    }
}
