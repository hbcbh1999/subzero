use crate::api::{ContentType, ContentType::*, Join, Join::*};
use snafu::Snafu;
use serde_json::{json, Value as JsonValue, Error as SerdeError};
use std::str::Utf8Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("{}", source))]
    Serde { source: SerdeError },

    #[snafu(display("ActionInappropriate"))]
    ActionInappropriate,

    #[snafu(display("InvalidRange"))]
    InvalidRange,

    #[snafu(display("InvalidBody {}", message))]
    InvalidBody { message: String },

    #[snafu(display("InternalError {}", message))]
    InternalError { message: String },

    #[snafu(display("ParseRequestError {}: {}", message, details))]
    ParseRequestError { message: String, details: String },

    #[snafu(display("NoRelBetween {} {}", origin, target))]
    NoRelBetween { origin: String, target: String },

    #[snafu(display("AmbiguousRelBetween {} {} {:?}", origin, target, rel_hint))]
    AmbiguousRelBetween {
        origin: String,
        target: String,
        rel_hint: String,
        compressed_rel: JsonValue,
    },

    #[snafu(display("InvalidFilters"))]
    InvalidFilters,

    #[snafu(display("UnacceptableSchema {:?}", schemas))]
    UnacceptableSchema { schemas: Vec<String> },

    #[snafu(display("UnknownRelation {}", relation))]
    UnknownRelation { relation: String },

    #[snafu(display("NotFound {}", target))]
    NotFound { target: String },

    //schema proc_name argument_keys has_prefer_single_object content_type is_inv_post
    #[snafu(display("NoRpc {}.{}", schema, proc_name))]
    NoRpc {
        schema: String,
        proc_name: String,
        argument_keys: Vec<String>,
        has_prefer_single_object: bool,
        content_type: ContentType,
        is_inv_post: bool,
    },

    #[snafu(display("UnsupportedVerb"))]
    UnsupportedVerb,

    #[snafu(display("UnsupportedFeature: {}", message))]
    UnsupportedFeature { message: String },

    #[snafu(display("Failed to deserialize json: {}", source))]
    JsonDeserialize { source: serde_json::Error },

    #[snafu(display("Failed to deserialize csv: {}", source))]
    CsvDeserialize { source: csv::Error },

    #[snafu(display("Failed to deserialize utf8: {}", source))]
    Utf8Deserialize { source: Utf8Error },

    #[snafu(display("Failed to serialize json: {}", source))]
    JsonSerialize { source: serde_json::Error },

    #[snafu(display("JwtTokenInvalid {}", message))]
    JwtTokenInvalid { message: String },

    #[snafu(display("GucHeadersError"))]
    GucHeadersError,

    #[snafu(display("GucStatusError"))]
    GucStatusError,

    #[snafu(display("ContentTypeError {}", message))]
    ContentTypeError { message: String },

    #[snafu(display("SingularityError {}", count))]
    SingularityError { count: i64, content_type: String },

    #[snafu(display("LimitOffsetNotAllowedError"))]
    LimitOffsetNotAllowedError,

    #[snafu(display("OrderNotAllowedError"))]
    OrderNotAllowedError,

    #[snafu(display("PutMatchingPkError"))]
    PutMatchingPkError,

    #[snafu(display("PermissionDenied {}", details))]
    PermissionDenied { details: String },
}

impl Error {
    pub fn headers(&self) -> Vec<(String, String)> {
        match self {
            Error::JwtTokenInvalid { message } => vec![
                ("Content-Type".into(), "application/json".into()),
                (
                    "WWW-Authenticate".into(),
                    format!("Bearer error=\"invalid_token\", error_description=\"{}\"", message),
                ),
            ],
            _ => vec![("Content-Type".into(), "application/json".into())],
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Error::Serde { .. } => 400,
            Error::UnsupportedFeature { .. } => 400,
            Error::ContentTypeError { .. } => 415,
            Error::GucHeadersError => 500,
            Error::GucStatusError => 500,
            Error::InternalError { .. } => 500,
            Error::JwtTokenInvalid { .. } => 401,
            Error::PermissionDenied { .. } => 403,
            Error::ActionInappropriate => 405,
            Error::InvalidRange => 416,
            Error::InvalidBody { .. } => 400,
            Error::ParseRequestError { .. } => 400,
            Error::NoRelBetween { .. } => 400,
            Error::AmbiguousRelBetween { .. } => 300,
            Error::InvalidFilters => 405,
            Error::UnacceptableSchema { .. } => 406,
            Error::UnknownRelation { .. } => 400,
            Error::NotFound { .. } => 404,
            Error::NoRpc { .. } => 404,
            Error::UnsupportedVerb { .. } => 405,
            // Error::ReadFile { .. } => 500,
            Error::JsonDeserialize { .. } => 400,
            Error::LimitOffsetNotAllowedError => 400,
            Error::OrderNotAllowedError => 400,
            Error::CsvDeserialize { .. } => 400,
            Error::Utf8Deserialize { .. } => 400,
            Error::PutMatchingPkError => 400,
            Error::JsonSerialize { .. } => 500,
            Error::SingularityError { .. } => 406,
        }
    }

    pub fn json_body(&self) -> JsonValue {
        match self {
            Error::Serde { source } => {
                json!({ "message": format!("{}", source) })
            }
            Error::UnsupportedFeature { message } => json!({ "message": message }),
            Error::ContentTypeError { message } => json!({ "message": message }),
            Error::GucHeadersError => {
                json!({"message": "response.headers guc must be a JSON array composed of objects with a single key and a string value"})
            }
            Error::GucStatusError => {
                json!({"message":"response.status guc must be a valid status code"})
            }
            Error::ActionInappropriate => json!({"message": "Bad Request"}),
            Error::InvalidRange => json!({"message": "HTTP Range error"}),
            Error::InvalidBody { message } => json!({ "message": message }),
            Error::InternalError { message } => json!({ "message": message }),
            Error::PermissionDenied { details } => json!({ "message": "Permission denied".to_string(), "details": details }),
            Error::ParseRequestError { message, details } => {
                json!({"details": details, "message": message })
            }
            Error::JwtTokenInvalid { message } => json!({ "message": message }),
            Error::LimitOffsetNotAllowedError => {
                json!({"message": "Range header and limit/offset querystring parameters are not allowed"})
            }
            Error::OrderNotAllowedError => {
                json!({"message": "order querystring parameter not allowed"})
            }
            Error::NoRelBetween { origin, target } => json!({
                "message":
                    format!(
                        "Could not find foreign keys between these entities. No relationship found between {} and {}",
                        origin, target
                    )
            }),
            Error::AmbiguousRelBetween {
                origin,
                target,
                rel_hint,
                compressed_rel,
            } => json!({
                "details": compressed_rel, //relations.iter().map(compressed_rel).collect::<JsonValue>(),
                "hint":     format!("Try changing '{}' to one of the following: {}. Find the desired relationship in the 'details' key.",target, rel_hint),
                "message":  format!("Could not embed because more than one relationship was found for '{}' and '{}'", origin, target),
            }),
            Error::InvalidFilters => {
                json!({"message":"Filters must include all and only primary key columns with 'eq' operators"})
            }
            Error::PutMatchingPkError => {
                json!({"message":"Payload values do not match URL in primary key column(s)"})
            }
            //TODO!!! message i wrong, the error contains the name of the "unfound" schema
            Error::UnacceptableSchema { schemas } => json!({ "message": format!("The schema must be one of the following: {}", schemas.join(", ")) }),
            Error::UnknownRelation { relation } => {
                json!({ "message": format!("Unknown relation '{}'", relation) })
            }
            Error::NotFound { target } => {
                json!({ "message": format!("Entry '{}' not found", target) })
            }
            Error::UnsupportedVerb => json!({"message":"Unsupported HTTP verb"}),
            Error::NoRpc {
                schema,
                proc_name,
                argument_keys,
                has_prefer_single_object,
                content_type,
                is_inv_post,
            } => {
                let prms = format!("({})", argument_keys.join(", "));
                let msg_part = match (has_prefer_single_object, is_inv_post, content_type) {
                    (true, _, _) => " function with a single json or jsonb parameter".to_string(),
                    (_, true, &TextCSV) => " function with a single unnamed text parameter".to_string(),
                    //(_, true, CTOctetStream)     => " function with a single unnamed bytea parameter",
                    (_, true, &ApplicationJSON) => format!(
                        "{} function or the {}.{} function with a single unnamed json or jsonb parameter",
                        prms, schema, proc_name
                    ),
                    _ => format!("{} function", prms),
                };
                json!({
                    "hint": "If a new function was created in the database with this name and parameters, try reloading the schema cache.",
                    "message": format!("Could not find the {}.{}{} in the schema cache", schema, proc_name, msg_part)
                })
            }
            Error::JsonDeserialize { .. } => json!({ "message": format!("{}", self) }),
            Error::CsvDeserialize { .. } => json!({ "message": format!("{}", self) }),
            Error::Utf8Deserialize { .. } => json!({ "message": format!("{}", self) }),
            Error::JsonSerialize { .. } => json!({ "message": format!("{}", self) }),
            Error::SingularityError { count, content_type } => json!({
                "message": "JSON object requested, multiple (or no) rows returned",
                "details": format!("Results contain {} rows, {} requires 1 row", count, content_type)
            }),
        }
    }
}

pub fn rel_hint(joins: &[Join]) -> String {
    joins
        .iter()
        .map(|j| match j {
            Child(fk) => format!("'{}!{}'", fk.table.1, fk.name),
            Parent(fk) => format!("'{}!{}'", fk.referenced_table.1, fk.name),
            Many(t, _fk1, fk2) => format!("'{}!{}'", fk2.referenced_table.1, t.1),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn compressed_rel(join: &Join) -> JsonValue {
    match join {
        Child(fk) => json!({
            "cardinality": "one-to-many",
            "relationship": format!("{}[{}][{}]", fk.name, fk.referenced_columns.join(","), fk.columns.join(",")),
            "embedding": format!("{} with {}", fk.referenced_table.1, fk.table.1 )
        }),
        Parent(fk) => json!({
            "cardinality": "many-to-one",
            "relationship": format!("{}[{}][{}]", fk.name, fk.columns.join(","), fk.referenced_columns.join(",")),
            "embedding": format!("{} with {}", fk.table.1, fk.referenced_table.1 )
        }),
        Many(t, fk1, fk2) => json!({
            "cardinality": "many-to-many",
            "relationship": format!("{}.{}[{}][{}]", t.0, t.1, fk1.name, fk2.name),
            "embedding": format!("{} with {}", fk1.referenced_table.1, fk2.referenced_table.1 )
        }),
    }
}
