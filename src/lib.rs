#![feature(drain_filter)]
#![feature(slice_concat_trait)]
#[macro_use]
extern crate combine;
#[macro_use]
extern crate lazy_static;
//#[macro_use] extern crate rocket;

pub mod api;
pub mod config;
pub mod dynamic_statement;
pub mod error;
pub mod formatter;
pub mod parser;
pub mod postgrest;
pub mod rocket_util;
pub mod schema;
pub mod vhosts;
