use mysql_async::prelude::*;
use mysql_async::{Pool, Error as MysqlError, Conn, TxOpts, IsolationLevel, Value, Row, FromRowError, Opts};

use snafu::ResultExt;
use subzero_core::error::JsonSerializeSnafu;
use tokio::time::{Duration, sleep};
use crate::config::{VhostConfig, SchemaStructure::*};
// use log::{debug};
use subzero_core::{
    api::{ApiRequest, ApiResponse, ContentType::*, SingleVal, ListVal, Payload},
    error::{
        Error::{SingularityError, PutMatchingPkError, PermissionDenied},
    },
    schema::{DbSchema},
    formatter::{
        Param,
        Param::*,
        mysql::{fmt_main_query, generate},
        ToParam, Snippet, SqlParam,
    },
    error::{JsonDeserializeSnafu},
};
use subzero_core::dynamic_statement::{param, sql, JoinIterator};
use crate::error::{Result, Error, *};
use async_trait::async_trait;

use super::{Backend, DbSchemaWrap, include_files};

use std::{collections::HashMap, fs};
use std::path::Path;
use http::Method;

#[derive(Debug, PartialEq, Eq, Clone)]
struct DbResponse {
    page_total: i64,
    total_result_set: Option<i64>,
    body: String,
    constraints_satisfied: bool,
    response_headers: Option<String>,
    response_status: Option<String>,
}

impl FromRow for DbResponse {
    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        let page_total: i64 = row.get(0).unwrap();
        let total_result_set: Option<i64> = row.get(1).unwrap();
        let body: String = row.get(2).unwrap();
        let constraints_satisfied: bool = row.get(3).unwrap();
        let response_headers: Option<String> = row.get(4).unwrap();
        let response_status: Option<String> = row.get(5).unwrap();
        Ok(DbResponse {
            page_total,
            total_result_set,
            body,
            constraints_satisfied,
            response_headers,
            response_status,
        })
    }
}

#[derive(Debug)]
struct WrapParam<'a>(Param<'a>);

impl ToValue for WrapParam<'_> {
    fn to_value(&self) -> Value {
        match self {
            WrapParam(SV(SingleVal(v, ..))) => Value::Bytes(v.as_bytes().to_vec()),
            WrapParam(Str(v)) => Value::Bytes(v.as_bytes().to_vec()),
            WrapParam(StrOwned(v)) => Value::Bytes(v.as_bytes().to_vec()),
            WrapParam(PL(Payload(v, ..))) => Value::Bytes(v.as_bytes().to_vec()),
            WrapParam(LV(ListVal(v, ..))) => Value::Bytes(serde_json::to_string(v).unwrap_or_default().as_bytes().to_vec()), //WrapParam(LV(ListVal(v, ..))) => Ok(ToSqlOutput::Owned(Text(serde_json::to_string(v).unwrap_or_default()))),
        }
    }
    // fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
    //     match self {
    //         WrapParam(SV(SingleVal(v, ..))) => {
    //             out.put_slice(v.as_bytes());
    //             Ok(IsNull::No)
    //         }
    //         WrapParam(Str(v)) => {
    //             out.put_slice(v.as_bytes());
    //             Ok(IsNull::No)
    //         }
    //         WrapParam(StrOwned(v)) => {
    //             out.put_slice(v.as_bytes());
    //             Ok(IsNull::No)
    //         }
    //         WrapParam(PL(Payload(v, ..))) => {
    //             out.put_slice(v.as_bytes());
    //             Ok(IsNull::No)
    //         }
    //         WrapParam(LV(ListVal(v, ..))) => {
    //             if !v.is_empty() {
    //                 out.put_slice(
    //                     format!(
    //                         "{{\"{}\"}}",
    //                         v.iter()
    //                             .map(|e| e.replace('\\', "\\\\").replace('\"', "\\\""))
    //                             .collect::<Vec<_>>()
    //                             .join("\",\"")
    //                     )
    //                     .as_str()
    //                     .as_bytes(),
    //                 );
    //             } else {
    //                 out.put_slice(r#"{}"#.as_bytes());
    //             }

    //             Ok(IsNull::No)
    //         }
    //     }
    // }

    // fn accepts(_ty: &Type) -> bool { true }

    // fn encode_format(&self) -> Format { Format::Text }

    // to_sql_checked!();
}

fn wrap_param(p: &'_ (dyn ToParam + Sync)) -> WrapParam<'_> { WrapParam(p.to_param()) }
fn to_value<'a>(p: &'a WrapParam<'a>) -> Value { p.to_value() }

pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select "
        + if env.is_empty() {
            sql("null")
        } else {
            env.iter().map(|(k, v)| format!("@{k} = ") + param(v as &SqlParam)).join(",")
        }
}

async fn execute(pool: &Pool, authenticated: bool, request: &ApiRequest<'_>, env: &HashMap<&str, &str>, config: &VhostConfig) -> Result<ApiResponse> {
    // println!("------------ pool before {:?}", pool);
    let mut client = pool.get_conn().await.context(MysqlDbSnafu { authenticated })?;

    let (main_statement, main_parameters, _) = generate(fmt_main_query(request.schema_name, request, env).context(CoreSnafu)?);

    let opts = TxOpts::default()
        .with_readonly(request.read_only)
        .with_isolation_level(Some(IsolationLevel::ReadCommitted))
        .clone();

    let mut transaction = client.start_transaction(opts).await.context(MysqlDbSnafu { authenticated })?;
    let (env_query, env_parameters, _) = generate(fmt_env_query(env));
    debug!("env_query: {}\n{:?}", env_query, env_parameters);
    // let env_stm = transaction
    //     .prep(env_query.as_str())
    //     .await
    //     .context(MysqlDbSnafu { authenticated })?;

    // let _ = transaction.exec(env_stm, env_parameters).await.context(MysqlDbSnafu { authenticated })?;
    transaction
        .exec_drop(
            &env_query,
            env_parameters
                .into_iter()
                .map(wrap_param)
                .collect::<Vec<_>>()
                .iter()
                .map(to_value)
                .collect::<Vec<_>>(),
        )
        .await
        .context(MysqlDbSnafu { authenticated })?;
    // if let Some((s, f)) = &config.db_pre_request {
    //     let fn_schema = match s.as_str() {
    //         "" => request.schema_name,
    //         _ => s.as_str(),
    //     };

    //     let pre_request_statement = format!(r#"select "{f}".* from "{fn_schema}"."{f}"()"#);
    //     debug!("pre_statement {}", pre_request_statement);
    //     // let pre_request_stm = transaction
    //     //     .exec(pre_request_statement.as_str(), [])
    //     //     .await
    //     //     .context(PgDbSnafu { authenticated })?;
    //     transaction.query_drop(&pre_request_statement).await.context(MysqlDbSnafu { authenticated })?;
    // }

    debug!("main_statement {}\n{:?}", main_statement, main_parameters);

    // let main_stm = transaction
    //     .prepare_cached(main_statement.as_str())
    //     .await
    //     .context(PgDbSnafu { authenticated })?;

    let response: DbResponse = transaction
        .exec_first(
            &main_statement,
            main_parameters
                .into_iter()
                .map(wrap_param)
                .collect::<Vec<_>>()
                .iter()
                .map(to_value)
                .collect::<Vec<_>>(),
        )
        .await
        .context(MysqlDbSnafu { authenticated })?
        .unwrap();

    let constraints_satisfied: bool = response.constraints_satisfied;
    if !constraints_satisfied {
        transaction.rollback().await.context(MysqlDbSnafu { authenticated })?;
        return Err(to_core_error(PermissionDenied {
            details: "check constraint of an insert/update permission has failed".to_string(),
        }));
    }

    let api_response = ApiResponse {
        page_total: response.page_total,
        total_result_set: response.total_result_set,
        top_level_offset: 0,
        response_headers: response.response_headers,
        response_status: response.response_status,
        body: response.body,
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        transaction.rollback().await.context(MysqlDbSnafu { authenticated })?;
        return Err(to_core_error(SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        }));
    }

    if request.method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        transaction.rollback().await.context(MysqlDbSnafu { authenticated })?;
        return Err(to_core_error(PutMatchingPkError));
    }

    if config.db_tx_rollback {
        transaction.rollback().await.context(MysqlDbSnafu { authenticated })?;
    } else {
        transaction.commit().await.context(MysqlDbSnafu { authenticated })?;
    }

    // println!("------------ pool after {:?}", pool);
    Ok(api_response)
}

pub struct MySQLBackend {
    //vhost: String,
    config: VhostConfig,
    pool: Pool,
    db_schema: DbSchemaWrap,
}

#[async_trait]
impl Backend for MySQLBackend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection

        let opts = Opts::from_url(&config.db_uri).map_err(|_| Error::Internal {
            message: "invalid mysql connection string".to_string(),
        })?;
        let pool = Pool::new(opts);
        //read db schema
        let db_schema: DbSchemaWrap = match config.db_schema_structure.clone() {
            SqlFile(f) => match fs::read_to_string(vec![&f, &format!("mysql_{f}")].into_iter().find(|f| Path::new(f).exists()).unwrap_or(&f)) {
                Ok(q) => match wait_for_mysql_connection(&vhost, &pool).await {
                    Ok(mut client) => {
                        let authenticated = false;
                        let query = include_files(q);
                        let schemas_json = serde_json::to_string(&config.db_schemas).context(JsonSerializeSnafu).context(CoreSnafu)?;
                        match client.exec_first(&query, vec![schemas_json]).await {
                            Ok(Some(s)) => {
                                //println!("db schema loaded: {s}");
                                //let s: String = row.get(0);
                                Ok(DbSchemaWrap::new(s, |s| {
                                    serde_json::from_str::<DbSchema>(s.as_str())
                                        .context(JsonDeserializeSnafu)
                                        .context(CoreSnafu)
                                }))
                            }
                            Ok(None) => Err(Error::Internal {
                                message: "db schema not found".to_string(),
                            }),
                            Err(e) => Err(e).context(MysqlDbSnafu { authenticated }),
                        }
                    }
                    Err(e) => Err(e).context(MysqlDbSnafu { authenticated: false }),
                },
                Err(e) => Err(e).context(ReadFileSnafu { path: f }),
            },
            JsonFile(f) => match fs::read_to_string(&f) {
                Ok(s) => Ok(DbSchemaWrap::new(s, |s| {
                    serde_json::from_str::<DbSchema>(s.as_str())
                        .context(JsonDeserializeSnafu)
                        .context(CoreSnafu)
                })),
                Err(e) => Err(e).context(ReadFileSnafu { path: f }),
            },
            JsonString(s) => Ok(DbSchemaWrap::new(s, |s| {
                serde_json::from_str::<DbSchema>(s.as_str())
                    .context(JsonDeserializeSnafu)
                    .context(CoreSnafu)
            })),
        }?;

        if let Err(e) = db_schema.with_schema(|s| s.as_ref()) {
            let message = format!("Backend init failed: {e}");
            return Err(crate::Error::Internal { message });
        }

        Ok(MySQLBackend { config, pool, db_schema })
    }
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse> {
        execute(&self.pool, authenticated, request, env, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema { self.db_schema.borrow_schema().as_ref().unwrap() }
    fn config(&self) -> &VhostConfig { &self.config }
}

async fn wait_for_mysql_connection(vhost: &String, db_pool: &Pool) -> Result<Conn, MysqlError> {
    let mut i = 1;
    let mut time_since_start = 0;
    let max_delay_interval = 10;
    let max_retry_interval = 30;
    let mut client = db_pool.get_conn().await;
    while let Err(e) = client {
        println!("[{vhost}] Failed to connect to MySQL {e:?}");
        let time = Duration::from_secs(i);
        println!("[{}] Retrying the MySQL connection in {:?} seconds..", vhost, time.as_secs());
        sleep(time).await;
        client = db_pool.get_conn().await;
        i *= 2;
        if i > max_delay_interval {
            i = max_delay_interval
        };
        time_since_start += i;
        if time_since_start > max_retry_interval {
            break;
        }
    }
    match client {
        Err(_) => {}
        _ => println!("[{vhost}] Connection to MySQL successful"),
    }
    client
}
