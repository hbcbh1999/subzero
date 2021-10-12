use snafu::{Snafu};
use crate::api::{Join};
// use combine::easy::Error as ParseError;
//use combine::error::StringStreamError;
//use combine;

#[derive(Debug, Snafu, PartialEq)]
#[snafu(visibility(pub))]
pub enum Error {

    #[snafu(display("ActionInappropriate"))]
    ActionInappropriate,

    #[snafu(display("InvalidRange"))]
    InvalidRange,

    #[snafu(display("InvalidBody"))]
    InvalidBody,

    #[snafu(display("ParseRequestError {}: {}", message, details))]
    ParseRequestError {message: String, details: String, parameter: String},

    #[snafu(display("NoRelBetween {} {}", origin, target))]
    NoRelBetween {origin: String, target: String},

    #[snafu(display("AmbiguousRelBetween {} {} {:?}", origin, target, relations))]
    AmbiguousRelBetween {origin: String, target: String, relations: Vec<Join>},

    #[snafu(display("InvalidFilters"))]
    InvalidFilters,

    #[snafu(display("UnacceptableSchema {:?}", schemas))]
    UnacceptableSchema {schemas: Vec<String>},

    #[snafu(display("UnknownRelation"))]
    UnknownRelation {relation: String},

    #[snafu(display("UnsupportedVerb"))]
    UnsupportedVerb,

}

pub type Result<T, E = Error> = std::result::Result<T, E>;