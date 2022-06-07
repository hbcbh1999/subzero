use crate::api::{ContentType, ContentType::*, Join, Join::*};
use snafu::Snafu;
// use combine::easy::Error as ParseError;
//use combine::error::StringStreamError;
//use combine;
use serde_json::{json, Value as JsonValue};
// use std::io::Cursor;
use hyper::Error as HyperError;
use http::Error as HttpError;

#[cfg(feature = "postgresql")]
use deadpool_postgres::PoolError as PgPoolError;
// use rocket::http::Status;
// use rocket::request::Request;
// use rocket::response::{self, Responder, Response};
use std::{io, path::PathBuf};
#[cfg(feature = "postgresql")]
use tokio_postgres::{Error as PgError};

#[cfg(feature = "sqlite")]
use rusqlite::Error as SqliteError;

#[cfg(feature = "sqlite")]
use r2d2::Error as SqlitePoolError;

#[cfg(feature = "sqlite")]
use tokio::task::JoinError;
//use combine::stream::easy::ParseError;
// use serde_json;

#[cfg(feature = "clickhouse")]
use clickhouse::error::Error as ClickhouseError;
#[cfg(feature = "clickhouse")]
use deadpool::managed::PoolError as ClickhousePoolError;



#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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

    #[snafu(display("AmbiguousRelBetween {} {} {:?}", origin, target, relations))]
    AmbiguousRelBetween { origin: String, target: String, relations: Vec<Join> },

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

    #[snafu(display("Failed to deserialize json: {}", message))]
    UnsupportedFeature { message: String },

    // #[snafu(display("PgError {} {} {} {}", code, message, details, hint))]
    // PgError {code: String, message: String, details: String, hint: String},
    #[snafu(display("Unable to read from {}: {}", path.display(), source))]
    ReadFile { source: io::Error, path: PathBuf },

    #[snafu(display("Failed to deserialize json: {}", source))]
    JsonDeserialize { source: serde_json::Error },

    #[snafu(display("Failed to deserialize csv: {}", source))]
    CsvDeserialize { source: csv::Error },

    #[snafu(display("Failed to serialize json: {}", source))]
    JsonSerialize { source: serde_json::Error },

    #[cfg(feature = "postgresql")]
    #[snafu(display("DbPoolError {}", source))]
    PgDbPoolError { source: PgPoolError },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("DbPoolError {}", source))]
    ClickhouseDbPoolError { source: ClickhousePoolError<ClickhouseError> },

    #[cfg(feature = "sqlite")]
    #[snafu(display("DbPoolError {}", source))]
    SqliteDbPoolError { source: SqlitePoolError },

    #[cfg(feature = "postgresql")]
    #[snafu(display("DbError {}", source))]
    PgDbError { source: PgError, authenticated: bool },

    #[cfg(feature = "sqlite")]
    #[snafu(display("DbError {}", source))]
    SqliteDbError { source: SqliteError, authenticated: bool },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("DbError {}", source))]
    ClickhouseDbError { source: ClickhouseError, authenticated: bool },

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

    #[snafu(display("PutMatchingPkError"))]
    PutMatchingPkError,

    #[cfg(feature = "sqlite")]
    #[snafu(display("ThreadError: {}", source))]
    ThreadError { source: JoinError },
    
    #[snafu(display("IoError: {}", source))]
    IoError { source: io::Error },
    
    #[snafu(display("ProxyError: {}", source))]
    ProxyError { source: HyperError },

    #[snafu(display("HttpRequestError: {}", source))]
    HttpRequestError { source: HttpError },
}

impl Error {
    pub fn headers(&self) -> Vec<(String, String)> {
        match self {
            #[cfg(feature = "postgresql")]
            Error::PgDbError { .. } => match self.status_code() {
                401 => vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("WWW-Authenticate".into(), "Bearer".into()),
                ],
                _ => vec![("Content-Type".into(), "application/json".into())],
            },
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
            Error::HttpRequestError { .. } => 500,
            Error::ProxyError {..} => 500,
            Error::IoError { ..} => 500,
            #[cfg(feature = "sqlite")]
            Error::ThreadError { .. } => 500,
            Error::UnsupportedFeature { .. } => 400,
            Error::ContentTypeError { .. } => 415,
            Error::GucHeadersError => 500,
            Error::GucStatusError => 500,
            Error::InternalError { .. } => 500,
            Error::JwtTokenInvalid { .. } => 401,
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
            Error::ReadFile { .. } => 500,
            Error::JsonDeserialize { .. } => 400,
            Error::LimitOffsetNotAllowedError => 400,
            Error::CsvDeserialize { .. } => 400,
            Error::PutMatchingPkError => 400,
            Error::JsonSerialize { .. } => 500,
            Error::SingularityError { .. } => 406,
            #[cfg(feature = "sqlite")]
            Error::SqliteDbPoolError { .. } => 503,

            #[cfg(feature = "postgresql")]
            Error::PgDbPoolError { .. } => 503,

            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbPoolError { .. } => 503,

            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbError { .. } => 503,
            // Error::DbPoolError { source } => match source {
            //     PgPoolError::Timeout(_) => 503,
            //     PgPoolError::Backend(_) => 503,
            //     PgPoolError::Closed => 503,
            //     PgPoolError::NoRuntimeSpecified => 503,
            //     PgPoolError::PostCreateHook(_) => 503,
            //     PgPoolError::PreRecycleHook(_) => 503,
            //     PgPoolError::PostRecycleHook(_) => 503,
            // },
            #[cfg(feature = "sqlite")]
            Error::SqliteDbError {
                ..
                // source,
                // authenticated,
            } => 500,
            #[cfg(feature = "postgresql")]
            Error::PgDbError {
                source,
                authenticated,
            } => match source.code() {
                Some(c) => match c.code().chars().collect::<Vec<char>>()[..] {
                    ['0', '8', ..] => 503,            // pg connection err
                    ['0', '9', ..] => 500,            // triggered action exception
                    ['0', 'L', ..] => 403,            // invalid grantor
                    ['0', 'P', ..] => 403,            // invalid role specification
                    ['2', '3', '5', '0', '3'] => 409, // foreign_key_violation
                    ['2', '3', '5', '0', '5'] => 409, // unique_violation
                    ['2', '5', '0', '0', '6'] => 405, // read_only_sql_transaction
                    ['2', '5', ..] => 500,            // invalid tx state
                    ['2', '8', ..] => 403,            // invalid auth specification
                    ['2', 'D', ..] => 500,            // invalid tx termination
                    ['3', '8', ..] => 500,            // external routine exception
                    ['3', '9', ..] => 500,            // external routine invocation
                    ['3', 'B', ..] => 500,            // savepoint exception
                    ['4', '0', ..] => 500,            // tx rollback
                    ['5', '3', ..] => 503,            // insufficient resources
                    ['5', '4', ..] => 413,            // too complex
                    ['5', '5', ..] => 500,            // obj not on prereq state
                    ['5', '7', ..] => 500,            // operator intervention
                    ['5', '8', ..] => 500,            // system error
                    ['F', '0', ..] => 500,            // conf file error
                    ['H', 'V', ..] => 500,            // foreign data wrapper error
                    ['P', '0', '0', '0', '1'] => 400, // default code for "raise"
                    ['P', '0', ..] => 500,            // PL/pgSQL Error
                    ['X', 'X', ..] => 500,            // internal Error
                    ['4', '2', '8', '8', '3'] => 404, // undefined function
                    ['4', '2', 'P', '0', '1'] => 404, // undefined table
                    ['4', '2', '5', '0', '1'] => {
                        if *authenticated {
                            403
                        } else {
                            401
                        }
                    }
                    ['P', 'T', a, b, c] => {
                        [a, b, c].iter().collect::<String>().parse::<u16>().unwrap_or(500)
                    }
                    _ => 400,
                },
                None => 500,
            },
        }
    }

    pub fn json_body(&self) -> JsonValue {
        match self {
            Error::HttpRequestError { source } => {json!({ "message": format!("Proxy error {}", source) })},
            Error::ProxyError { source } => {json!({ "message": format!("Proxy error {}", source) })}
            Error::IoError { source } => {json!({ "message": format!("IO error {}", source) })}
            Error::UnsupportedFeature {message} => json!({ "message": message }),
            #[cfg(feature = "sqlite")]
            Error::ThreadError { .. } => json!({"message":"internal thread error"}),
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
            Error::ParseRequestError { message, details } => {
                json!({"details": details, "message": message })
            }
            Error::JwtTokenInvalid { message } => json!({ "message": message }),
            Error::LimitOffsetNotAllowedError => {
                json!({"message": "Range header and limit/offset querystring parameters are not allowed for PUT"})
            }
            // Error::NoRelBetween {origin, target}  => json!({
            //     "hint":"If a new foreign key between these entities was created in the database, try reloading the schema cache.",
            //     "hint"    .= ("Verify that '" <> parent <> "' and '" <> child <> "' exist in the schema '" <> schema <> "' and that there is a foreign key relationship between them. If a new relationship was created, try reloading the schema cache." :: Text),
            //     "message": format!("Could not find a relationship between {} and {} in the schema cache", origin, target)
            // }),
            Error::NoRelBetween { origin, target } => json!({
                "message":
                    format!(
                        "Could not find foreign keys between these entities. No relationship found between {} and {}",
                        origin, target
                    )
            }),
            Error::AmbiguousRelBetween { origin, target, relations } => json!({
                "details": relations.iter().map(compressed_rel).collect::<JsonValue>(),
                "hint":     format!("Try changing '{}' to one of the following: {}. Find the desired relationship in the 'details' key.",target, rel_hint(relations)),
                "message":  format!("Could not embed because more than one relationship was found for '{}' and '{}'", origin, target),
            }),
            Error::InvalidFilters => {
                json!({"message":"Filters must include all and only primary key columns with 'eq' operators"})
            }
            Error::PutMatchingPkError => {
                json!({"message":"Payload values do not match URL in primary key column(s)"})
            }
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
            Error::ReadFile { source, path } => {
                json!({ "message": format!("Failed to read file {} ({})", path.to_str().unwrap(), source) })
            }
            Error::JsonDeserialize { .. } => json!({ "message": format!("{}", self) }),
            Error::CsvDeserialize { .. } => json!({ "message": format!("{}", self) }),
            Error::JsonSerialize { .. } => json!({ "message": format!("{}", self) }),
            #[cfg(feature = "postgresql")]
            Error::PgDbPoolError { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbPoolError { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "sqlite")]
            Error::SqliteDbPoolError { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbError { source, .. } => {
                json!({ "message": format!("Unhandled db error: {}", source) })
            }
            Error::SingularityError { count, content_type } => json!({
                "message": "JSON object requested, multiple (or no) rows returned",
                "details": format!("Results contain {} rows, {} requires 1 row", count, content_type)
            }),
            #[cfg(feature = "postgresql")]
            Error::PgDbError { source, .. } => match source.as_db_error() {
                Some(db_err) => match db_err.code().code().chars().collect::<Vec<char>>()[..] {
                    ['P', 'T', ..] => json!({
                        "details": match db_err.detail() {Some(v) => v.into(), None => JsonValue::Null},
                        "hint": match db_err.hint() {Some(v) => v.into(), None => JsonValue::Null}
                    }),
                    _ => json!({
                        "code": db_err.code().code(),
                        "message": db_err.message(),
                        "details": match db_err.detail() {Some(v) => v.into(), None => JsonValue::Null},
                        "hint": match db_err.hint() {Some(v) => v.into(), None => JsonValue::Null}
                    }),
                },
                None => json!({ "message": format!("Unhandled db error {}", source) }),
            },

            #[cfg(feature = "sqlite")]
            Error::SqliteDbError { source, .. } => {
                json!({ "message": format!("Unhandled db error: {}", source) })
            }
        }
    }
}

fn rel_hint(joins: &[Join]) -> String {
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

fn compressed_rel(join: &Join) -> JsonValue {
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

// compressedRel :: Relationship -> JSON.Value
// compressedRel Relationship{..} =
//   let
//     fmtTbl Table{..} = tableSchema <> "." <> tableName
//     fmtEls els = "[" <> T.intercalate ", " els <> "]"
//   in
//   JSON.object $
//     ("embedding" .= (tableName relTable <> " with " <> tableName relForeignTable :: Text))
//     : case relCardinality of
//         M2M Junction{..} -> [
//             "cardinality" .= ("many-to-many" :: Text)
//           , "relationship" .= (fmtTbl junTable <> fmtEls [junConstraint1] <> fmtEls [junConstraint2])
//           ]
//         M2O cons -> [
//             "cardinality" .= ("many-to-one" :: Text)
//           , "relationship" .= (cons <> fmtEls (colName <$> relColumns) <> fmtEls (colName <$> relForeignColumns))
//           ]
//         O2M cons -> [
//             "cardinality" .= ("one-to-many" :: Text)
//           , "relationship" .= (cons <> fmtEls (colName <$> relColumns) <> fmtEls (colName <$> relForeignColumns))
//           ]

pub type Result<T, E = Error> = std::result::Result<T, E>;
