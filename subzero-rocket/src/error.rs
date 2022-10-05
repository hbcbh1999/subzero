use snafu::Snafu;
use serde_json::{json, Value as JsonValue};
use std::{io, path::PathBuf};
use subzero_core::error::Error as SubzeroCoreError;

#[cfg(feature = "postgresql")]
use deadpool_postgres::PoolError as PgPoolError;

#[cfg(feature = "postgresql")]
use tokio_postgres::{Error as PgError};

#[cfg(feature = "sqlite")]
use rusqlite::Error as SqliteError;

#[cfg(feature = "sqlite")]
use r2d2::Error as SqlitePoolError;

#[cfg(feature = "sqlite")]
use tokio::task::JoinError;

#[cfg(feature = "clickhouse")]
use deadpool::managed::PoolError as ClickhousePoolError;

#[cfg(feature = "clickhouse")]
use http::Error as HttpError;

#[cfg(feature = "clickhouse")]
use hyper::Error as SourceHyperError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unable to read from {}: {}", path.display(), source))]
    ReadFile { source: io::Error, path: PathBuf },

    #[cfg(feature = "postgresql")]
    #[snafu(display("DbPoolError {}", source))]
    PgDbPool { source: PgPoolError },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("DbPoolError {}", source))]
    ClickhouseDbPool { source: ClickhousePoolError<HttpError> },

    #[cfg(feature = "sqlite")]
    #[snafu(display("DbPoolError {}", source))]
    SqliteDbPool { source: SqlitePoolError },

    #[cfg(feature = "postgresql")]
    #[snafu(display("DbError {}", source))]
    PgDb { source: PgError, authenticated: bool },

    #[cfg(feature = "sqlite")]
    #[snafu(display("DbError {}", source))]
    SqliteDb { source: SqliteError, authenticated: bool },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("DbError {}", source))]
    ClickhouseDb { source: HttpError, authenticated: bool },

    #[cfg(feature = "sqlite")]
    #[snafu(display("ThreadError: {}", source))]
    Thread { source: JoinError },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("HttpRequestError: {}", source))]
    HttpRequest { source: HttpError },

    #[cfg(feature = "clickhouse")]
    #[snafu(display("HttpRequestError: {}", source))]
    Hyper { source: SourceHyperError },

    #[snafu(display("{}", source))]
    Core { source: SubzeroCoreError },

    #[snafu(display("InternalError {}", message))]
    Internal { message: String },
}

impl Error {
    pub fn headers(&self) -> Vec<(String, String)> {
        match self {
            Error::Core { source } => source.headers(),
            #[cfg(feature = "postgresql")]
            Error::PgDb { .. } => match self.status_code() {
                401 => vec![
                    ("Content-Type".into(), "application/json".into()),
                    ("WWW-Authenticate".into(), "Bearer".into()),
                ],
                _ => vec![("Content-Type".into(), "application/json".into())],
            },
            _ => vec![("Content-Type".into(), "application/json".into())],
        }
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Error::ReadFile { .. } => 500,

            #[cfg(feature = "clickhouse")]
            Error::Hyper { .. } => 500,
            Error::Internal { .. } => 500,
            #[cfg(feature = "clickhouse")]
            Error::HttpRequest { .. } => 500,
            Error::Core { source } => source.status_code(),
            #[cfg(feature = "sqlite")]
            Error::Thread { .. } => 500,
            #[cfg(feature = "sqlite")]
            Error::SqliteDbPool { .. } => 503,

            #[cfg(feature = "postgresql")]
            Error::PgDbPool { .. } => 503,

            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbPool { .. } => 503,

            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDb { .. } => 503,

            #[cfg(feature = "sqlite")]
            Error::SqliteDb { .. } => 500,

            #[cfg(feature = "postgresql")]
            Error::PgDb { source, authenticated } => match source.code() {
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
                    ['P', 'T', a, b, c] => [a, b, c].iter().collect::<String>().parse::<u16>().unwrap_or(500),
                    _ => 400,
                },
                None => 500,
            },
        }
    }

    pub fn json_body(&self) -> JsonValue {
        match self {
            Error::ReadFile { source, path } => {
                json!({ "message": format!("Failed to read file {} ({})", path.to_str().unwrap(), source) })
            }
            Error::Internal { message } => json!({ "message": message }),
            #[cfg(feature = "clickhouse")]
            Error::HttpRequest { source } => {
                json!({ "message": format!("{}", source) })
            }
            #[cfg(feature = "clickhouse")]
            Error::Hyper { source } => {
                json!({ "message": format!("{}", source) })
            }
            Error::Core { source } => source.json_body(),
            #[cfg(feature = "sqlite")]
            Error::Thread { .. } => json!({"message":"internal thread error"}),
            #[cfg(feature = "postgresql")]
            Error::PgDbPool { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDbPool { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "sqlite")]
            Error::SqliteDbPool { source } => {
                json!({ "message": format!("Db pool error {}", source) })
            }
            #[cfg(feature = "clickhouse")]
            Error::ClickhouseDb { source, .. } => {
                json!({ "message": format!("Unhandled db error: {}", source) })
            }
            #[cfg(feature = "postgresql")]
            Error::PgDb { source, .. } => match source.as_db_error() {
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
            Error::SqliteDb { source, .. } => {
                json!({ "message": format!("Unhandled db error: {}", source) })
            }
        }
    }
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn to_core_error(e: SubzeroCoreError) -> Error { Error::Core { source: e } }
