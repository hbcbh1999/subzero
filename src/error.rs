use snafu::{Snafu};
use crate::api::{Join, Join::*, ContentType, ContentType::*};
// use combine::easy::Error as ParseError;
//use combine::error::StringStreamError;
//use combine;
use serde_json::{json, Value as JsonValue};
use std::io::Cursor;

use rocket::request::Request;
use rocket::response::{self, Response, Responder};
use rocket::http::{Status};
use std::{io, path::PathBuf};
use deadpool_postgres::PoolError;
use tokio_postgres::Error as PgError;
//use combine::stream::easy::ParseError;
// use serde_json;

#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for Error {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let status = Status::from_code(self.status_code()).unwrap();
        let body = self.json_body().to_string();
        let mut response = Response::build();
        response.status(status);
        response.sized_body(body.len(), Cursor::new(body));
        
        for (h,v) in self.headers() {
            response.raw_header(h,v);
        }

        response.ok()
    }
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {

    #[snafu(display("ActionInappropriate"))]
    ActionInappropriate,

    #[snafu(display("InvalidRange"))]
    InvalidRange,

    #[snafu(display("InvalidBody {}", message))]
    InvalidBody {message: String},

    #[snafu(display("InternalError {}", message))]
    InternalError {message: String},

    #[snafu(display("ParseRequestError {}: {}", message, details))]
    ParseRequestError {message: String, details: String},

    #[snafu(display("NoRelBetween {} {}", origin, target))]
    NoRelBetween {origin: String, target: String},

    #[snafu(display("AmbiguousRelBetween {} {} {:?}", origin, target, relations))]
    AmbiguousRelBetween {origin: String, target: String, relations: Vec<Join>},

    #[snafu(display("InvalidFilters"))]
    InvalidFilters,

    #[snafu(display("UnacceptableSchema {:?}", schemas))]
    UnacceptableSchema {schemas: Vec<String>},

    #[snafu(display("UnknownRelation {}", relation))]
    UnknownRelation {relation: String},

    #[snafu(display("NotFound"))]
    NotFound,

    //schema proc_name argument_keys has_prefer_single_object content_type is_inv_post
    #[snafu(display("NoRpc {}.{}", schema, proc_name))]
    NoRpc {schema: String, proc_name: String, argument_keys: Vec<String>, has_prefer_single_object: bool,  content_type: ContentType,  is_inv_post: bool},

    #[snafu(display("UnsupportedVerb"))]
    UnsupportedVerb,

    // #[snafu(display("PgError {} {} {} {}", code, message, details, hint))]
    // PgError {code: String, message: String, details: String, hint: String},

    #[snafu(display("Unable to read from {}: {}", path.display(), source))]
    ReadFile  { source: io::Error, path: PathBuf },

    #[snafu(display("Failed to deserialize json: {}", source))]
    JsonDeserialize  { source: serde_json::Error },

    #[snafu(display("Failed to deserialize csv: {}", source))]
    CsvDeserialize  { source: csv::Error },

    #[snafu(display("Failed to serialize json: {}", source))]
    JsonSerialize  { source: serde_json::Error },

    #[snafu(display("DbPoolError {}", source))]
    DbPoolError { source: PoolError },

    #[snafu(display("DbError {}", source))]
    DbError { source: PgError, authenticated: bool },

    #[snafu(display("JwtTokenInvalid {}", message))]
    JwtTokenInvalid { message: String },

    #[snafu(display("GucHeadersError"))]
    GucHeadersError,
    
    #[snafu(display("GucStatusError"))]
    GucStatusError,

    #[snafu(display("ContentTypeError {}", message))]
    ContentTypeError { message: String },

    #[snafu(display("SingularityError {}", count))]
    SingularityError { count: i64, content_type: String }

}

impl Error {
    fn headers(&self) -> Vec<(String, String)> {
        match self {
            Error::DbError { .. } => match self.status_code() {
                401 => vec![("Content-Type".into(), "application/json".into()),("WWW-Authenticate".into(), "Bearer".into())],
                _ =>  vec![("Content-Type".into(), "application/json".into())]
            },
            Error::JwtTokenInvalid { message } => vec![("Content-Type".into(), "application/json".into()),("WWW-Authenticate".into(), format!("Bearer error=\"invalid_token\", error_description=\"{}\"", message))],
            _ => vec![("Content-Type".into(), "application/json".into())]
        }
    }

    fn status_code(&self) -> u16 {
        match self {
            Error::ContentTypeError { .. } => 415,
            Error::GucHeadersError => 500,
            Error::GucStatusError => 500,
            Error::InternalError { .. } => 500,
            Error::JwtTokenInvalid { .. } => 401,
            Error::ActionInappropriate => 405,
            Error::InvalidRange => 416,
            Error::InvalidBody {..} => 400,
            Error::ParseRequestError {..}  => 400,
            Error::NoRelBetween {..}  => 400,
            Error::AmbiguousRelBetween {..}  => 300,
            Error::InvalidFilters => 405,
            Error::UnacceptableSchema {..} => 406,
            Error::UnknownRelation {..}  => 400,
            Error::NotFound => 404,
            Error::NoRpc {..} => 404,
            Error::UnsupportedVerb {..} => 405,
            Error::ReadFile  { .. }  => 500,
            Error::JsonDeserialize  { .. }  => 400,
            Error::CsvDeserialize  { .. }  => 400,
            Error::JsonSerialize  { .. }  => 500,
            Error::SingularityError { .. } => 406,
            Error::DbPoolError { source }  => match source {
                PoolError::Timeout(_) => 503,
                PoolError::Backend(_) => 503,
                PoolError::Closed => 503,
                PoolError::NoRuntimeSpecified => 503,
                PoolError::PostCreateHook(_) => 503,
                PoolError::PreRecycleHook(_) => 503,
                PoolError::PostRecycleHook(_) => 503,
            },
            Error::DbError { source, authenticated }  => match source.code() {
                Some(c) => match c.code().chars().collect::<Vec<char>>()[..] {
                    ['0','8',..] => 503, // pg connection err
                    ['0','9',..] => 500, // triggered action exception
                    ['0','L',..] => 403, // invalid grantor
                    ['0','P',..] => 403, // invalid role specification
                    ['2','3','5','0','3']   => 409, // foreign_key_violation
                    ['2','3','5','0','5']   => 409, // unique_violation
                    ['2','5','0','0','6']   => 405, // read_only_sql_transaction
                    ['2','5',..] => 500, // invalid tx state
                    ['2','8',..] => 403, // invalid auth specification
                    ['2','D',..] => 500, // invalid tx termination
                    ['3','8',..] => 500, // external routine exception
                    ['3','9',..] => 500, // external routine invocation
                    ['3','B',..] => 500, // savepoint exception
                    ['4','0',..] => 500, // tx rollback
                    ['5','3',..] => 503, // insufficient resources
                    ['5','4',..] => 413, // too complex
                    ['5','5',..] => 500, // obj not on prereq state
                    ['5','7',..] => 500, // operator intervention
                    ['5','8',..] => 500, // system error
                    ['F','0',..] => 500, // conf file error
                    ['H','V',..] => 500, // foreign data wrapper error
                    ['P','0','0','0','1']   => 400, // default code for "raise"
                    ['P','0',..] => 500, // PL/pgSQL Error
                    ['X','X',..] => 500, // internal Error
                    ['4','2','8','8','3']   => 404, // undefined function
                    ['4','2','P','0','1']   => 404, // undefined table
                    ['4','2','5','0','1']   => if *authenticated { 403 } else { 401 },
                    ['P','T',a,b,c] =>   match [a,b,c].iter().collect::<String>().parse::<u16>(){ Ok(c) => c, Err(_) => 500 },
                    _         => 400
                },
                None => 500
            }
        }
    }

    fn json_body(&self) -> JsonValue {
        match self {
            Error::ContentTypeError {message}=> json!({"message": message}),
            Error::GucHeadersError => json!({"message": "response.headers guc must be a JSON array composed of objects with a single key and a string value"}),
            Error::GucStatusError => json!({"message":"response.status guc must be a valid status code"}),
            Error::ActionInappropriate => json!({"message": "Bad Request"}),
            Error::InvalidRange => json!({"message": "HTTP Range error"}),
            Error::InvalidBody {message} => json!({"message": message}),
            Error::InternalError {message} => json!({"message": message}),
            Error::ParseRequestError { message, details }  => json!({"details": details, "message": message }),
            Error::JwtTokenInvalid {message} => json!({"message": message}),
            // Error::NoRelBetween {origin, target}  => json!({
            //     "hint":"If a new foreign key between these entities was created in the database, try reloading the schema cache.",
            //     "message": format!("Could not find a relationship between {} and {} in the schema cache", origin, target)
            // }),
            Error::NoRelBetween {origin, target}  => json!({
                "message": format!("Could not find foreign keys between these entities. No relationship found between {} and {}", origin, target)
            }),
            Error::AmbiguousRelBetween {origin, target, relations}  => json!({
                "details": relations.iter().map(compressed_rel).collect::<JsonValue>(),
                "hint":     format!("Try changing '{}' to one of the following: {}. Find the desired relationship in the 'details' key.",target, rel_hint(relations)),
                "message":  format!("Could not embed because more than one relationship was found for '{}' and '{}'", origin, target),
            }),
            Error::InvalidFilters => json!({"message":"Filters must include all and only primary key columns with 'eq' operators"}),
            Error::UnacceptableSchema {schemas} => json!({"message":format!("The schema must be one of the following: {}", schemas.join(", "))}),
            // Error::UnknownRelation {..}  => 400,
            Error::NotFound => json!({}),
            Error::UnsupportedVerb => json!({"message":"Unsupported HTTP verb"}),
            Error::NoRpc {schema, proc_name, argument_keys, has_prefer_single_object, content_type, is_inv_post} => {
                let prms = format!("({})", argument_keys.join(", "));
                let msg_part = match (has_prefer_single_object, is_inv_post, content_type) {
                    (true, _, _)                 => format!(" function with a single json or jsonb parameter"),
                    (_, true, &TextCSV)       => format!(" function with a single unnamed text parameter"),
                    //(_, true, CTOctetStream)     => " function with a single unnamed bytea parameter",
                    (_, true, &ApplicationJSON) => format!("{} function or the {}.{} function with a single unnamed json or jsonb parameter", prms, schema, proc_name),
                    _                            => format!("{} function", prms),
                };
                json!({
                    "hint": "If a new function was created in the database with this name and parameters, try reloading the schema cache.",
                    "message": format!("Could not find the {}.{}{} in the schema cache", schema, proc_name, msg_part)
                })
            },
            // Error::ReadFile  { .. }  => 500,
            Error::JsonDeserialize {..} => json!({"message": format!("{}", self)}),
            Error::CsvDeserialize {..} => json!({"message": format!("{}", self)}),
            Error::JsonSerialize {..} => json!({"message": format!("{}", self)}),
            Error::DbPoolError { source }  => json!({"message": format!("Db pool error {}", source)}),
            Error::SingularityError { count, content_type } => json!({
                "message": "JSON object requested, multiple (or no) rows returned",
                "details": format!("Results contain {} rows, {} requires 1 row", count, content_type)
            }),
            Error::DbError { source, .. }  => match source.as_db_error() {
                Some(db_err) => match db_err.code().code().chars().collect::<Vec<char>>()[..] {
                    ['P','T',..] => json!({
                        "details": match db_err.detail() {Some(v) => v.into(), None => JsonValue::Null},
                        "hint": match db_err.hint() {Some(v) => v.into(), None => JsonValue::Null}
                    }),
                    _         => json!({
                        "code": db_err.code().code(),
                        "message": db_err.message(),
                        "details": match db_err.detail() {Some(v) => v.into(), None => JsonValue::Null},
                        "hint": match db_err.hint() {Some(v) => v.into(), None => JsonValue::Null}
                    })
                },
                None => json!({"message": format!("Unhandled db error {}", source)})
            }
            e  => json!({"message": format!("Unhandled error {}", e)}),
        }
    }
}

fn rel_hint(joins: &Vec<Join>)->String {
    joins.iter()
    .map(|j| 
        match j {
            Child(fk) => format!("'{}!{}'", fk.table.1, fk.name),
            Parent(fk) => format!("'{}!{}'", fk.referenced_table.1, fk.name),
            Many(t, _fk1, fk2) => format!("'{}!{}'", fk2.referenced_table.1, t.1),
        }
    )
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