use tokio_postgres::{IsolationLevel};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime, Timeouts, Object, PoolError};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::{MakeTlsConnector};
use snafu::ResultExt;
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
        postgresql::{fmt_main_query, generate},
        ToParam, Snippet, SqlParam,
    },
    error::{JsonDeserializeSnafu},
};
use subzero_core::dynamic_statement::{param, sql, JoinIterator};
use postgres_types::{to_sql_checked, Format, IsNull, ToSql, Type};
use crate::error::{Result, *};
use async_trait::async_trait;

use super::{Backend, DbSchemaWrap, include_files};

use std::{collections::HashMap, fs};
use std::path::Path;
use http::Method;
use bytes::{BufMut, BytesMut};
use std::error::Error;
#[derive(Debug)]
struct WrapParam<'a>(Param<'a>);

impl ToSql for WrapParam<'_> {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        match self {
            WrapParam(SV(SingleVal(v, ..))) => {
                out.put_slice(v.as_bytes());
                Ok(IsNull::No)
            }
            WrapParam(Str(v)) => {
                out.put_slice(v.as_bytes());
                Ok(IsNull::No)
            }
            WrapParam(StrOwned(v)) => {
                out.put_slice(v.as_bytes());
                Ok(IsNull::No)
            }
            WrapParam(PL(Payload(v, ..))) => {
                out.put_slice(v.as_bytes());
                Ok(IsNull::No)
            }
            WrapParam(LV(ListVal(v, ..))) => {
                if !v.is_empty() {
                    out.put_slice(
                        format!(
                            "{{\"{}\"}}",
                            v.iter()
                                .map(|e| e.replace('\\', "\\\\").replace('\"', "\\\""))
                                .collect::<Vec<_>>()
                                .join("\",\"")
                        )
                        .as_str()
                        .as_bytes(),
                    );
                } else {
                    out.put_slice(r#"{}"#.as_bytes());
                }

                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool { true }

    fn encode_format(&self, _ty: &Type) -> Format { Format::Text }

    to_sql_checked!();
}

fn wrap_param(p: &'_ (dyn ToParam + Sync)) -> WrapParam<'_> { WrapParam(p.to_param()) }
fn cast_param<'a>(p: &'a WrapParam<'a>) -> &'a (dyn ToSql + Sync) { p as &(dyn ToSql + Sync) }

pub fn fmt_env_query<'a>(env: &'a HashMap<&'a str, &'a str>) -> Snippet<'a> {
    "select "
        + if env.is_empty() {
            sql("null")
        } else {
            env.iter()
                .map(|(k, v)| "set_config(" + param(k as &SqlParam) + ", " + param(v as &SqlParam) + ", true)")
                .join(",")
        }
}
async fn execute<'a>(
    schema: &DbSchema<'a>, pool: &Pool, authenticated: bool, request: &ApiRequest<'_>, env: &HashMap<&str, &str>, config: &VhostConfig,
) -> Result<ApiResponse> {
    let mut client = pool.get().await.context(PgDbPoolSnafu)?;
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema, request.schema_name, request, env).context(CoreSnafu)?);

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(request.read_only)
        .start()
        .await
        .context(PgDbSnafu { authenticated })?;
    let (env_query, env_parameters, _) = generate(fmt_env_query(env));
    debug!("env_query: {}\n{:?}", env_query, env_parameters);
    let env_stm = transaction
        .prepare_cached(env_query.as_str())
        .await
        .context(PgDbSnafu { authenticated })?;
    transaction
        .query(
            &env_stm,
            env_parameters
                .into_iter()
                .map(wrap_param)
                .collect::<Vec<_>>()
                .iter()
                .map(cast_param)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
        .context(PgDbSnafu { authenticated })?;
    if let Some((s, f)) = &config.db_pre_request {
        let fn_schema = match s.as_str() {
            "" => request.schema_name,
            _ => s.as_str(),
        };

        let pre_request_statement = format!(r#"select "{f}".* from "{fn_schema}"."{f}"()"#);
        debug!("pre_statement {}", pre_request_statement);
        let pre_request_stm = transaction
            .prepare_cached(pre_request_statement.as_str())
            .await
            .context(PgDbSnafu { authenticated })?;
        transaction.query(&pre_request_stm, &[]).await.context(PgDbSnafu { authenticated })?;
    }

    debug!("main_statement {}\n{:?}", main_statement, main_parameters);

    let main_stm = transaction
        .prepare_cached(main_statement.as_str())
        .await
        .context(PgDbSnafu { authenticated })?;

    let rows = transaction
        .query(
            &main_stm,
            main_parameters
                .into_iter()
                .map(wrap_param)
                .collect::<Vec<_>>()
                .iter()
                .map(cast_param)
                .collect::<Vec<_>>()
                .as_slice(),
        )
        .await
        .context(PgDbSnafu { authenticated })?;

    let constraints_satisfied: bool = rows[0].get("constraints_satisfied");
    if !constraints_satisfied {
        transaction.rollback().await.context(PgDbSnafu { authenticated })?;
        return Err(to_core_error(PermissionDenied {
            details: "check constraint of an insert/update permission has failed".to_string(),
        }));
    }

    let api_response = ApiResponse {
        page_total: rows[0].get::<_, i64>("page_total") as u64,
        total_result_set: rows[0].get::<_, Option<i64>>("total_result_set").map(|v| v as u64),
        top_level_offset: 0,
        response_headers: rows[0].get("response_headers"),
        response_status: rows[0].get("response_status"),
        body: rows[0].get("body"),
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        transaction.rollback().await.context(PgDbSnafu { authenticated })?;
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
        transaction.rollback().await.context(PgDbSnafu { authenticated })?;
        return Err(to_core_error(PutMatchingPkError));
    }

    if config.db_tx_rollback {
        transaction.rollback().await.context(PgDbSnafu { authenticated })?;
    } else {
        transaction.commit().await.context(PgDbSnafu { authenticated })?;
    }

    Ok(api_response)
}

pub struct PostgreSQLBackend {
    //vhost: String,
    config: VhostConfig,
    pool: Pool,
    db_schema: DbSchemaWrap,
}

#[async_trait]
impl Backend for PostgreSQLBackend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection
        let pg_uri = config.db_uri.clone();
        let pg_config = pg_uri.parse::<tokio_postgres::Config>().unwrap();
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        };
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
        builder.set_verify(SslVerifyMode::NONE);
        let tls_connector = MakeTlsConnector::new(builder.build());

        let mgr = Manager::from_config(pg_config, tls_connector, mgr_config);
        let timeouts = Timeouts {
            create: Some(Duration::from_millis(5000)),
            wait: None,
            recycle: None,
        };
        let pool = Pool::builder(mgr)
            .runtime(Runtime::Tokio1)
            .max_size(config.db_pool)
            .timeouts(timeouts)
            .build()
            .unwrap();

        //read db schema
        let db_schema: DbSchemaWrap = match config.db_schema_structure.clone() {
            SqlFile(f) => match fs::read_to_string(
                vec![&f, &format!("postgresql_{f}")]
                    .into_iter()
                    .find(|f| Path::new(f).exists())
                    .unwrap_or(&f),
            ) {
                Ok(q) => match wait_for_pg_connection(&vhost, &pool).await {
                    Ok(mut client) => {
                        let authenticated = false;
                        let query = include_files(q);
                        let transaction = client
                            .build_transaction()
                            .isolation_level(IsolationLevel::Serializable)
                            .read_only(true)
                            .start()
                            .await
                            .context(PgDbSnafu { authenticated })?;
                        let _ = transaction.query("set local schema ''", &[]).await;
                        match transaction.query(&query, &[&config.db_schemas]).await {
                            Ok(rows) => {
                                transaction.commit().await.context(PgDbSnafu { authenticated })?;
                                //println!("db schema loaded: {}", rows[0].get::<usize, &str>(0));
                                let s: String = rows[0].get(0);
                                Ok(DbSchemaWrap::new(s, |s| {
                                    serde_json::from_str::<DbSchema>(s.as_str())
                                        .context(JsonDeserializeSnafu)
                                        .context(CoreSnafu)
                                }))
                            }
                            Err(e) => {
                                transaction.rollback().await.context(PgDbSnafu { authenticated })?;
                                Err(e).context(PgDbSnafu { authenticated })
                            }
                        }
                    }
                    Err(e) => Err(e).context(PgDbPoolSnafu),
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

        Ok(PostgreSQLBackend { config, pool, db_schema })
    }
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse> {
        execute(self.db_schema(), &self.pool, authenticated, request, env, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema { self.db_schema.borrow_schema().as_ref().unwrap() }
    fn config(&self) -> &VhostConfig { &self.config }
}

async fn wait_for_pg_connection(vhost: &String, db_pool: &Pool) -> Result<Object, PoolError> {
    let mut i = 1;
    let mut time_since_start = 0;
    let max_delay_interval = 10;
    let max_retry_interval = 30;
    let mut client = db_pool.get().await;
    while let Err(e) = client {
        println!("[{vhost}] Failed to connect to PostgreSQL {e:?}");
        let time = Duration::from_secs(i);
        println!("[{}] Retrying the PostgreSQL connection in {:?} seconds..", vhost, time.as_secs());
        sleep(time).await;
        client = db_pool.get().await;
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
        _ => println!("[{vhost}] Connection to PostgreSQL successful"),
    }
    client
}
