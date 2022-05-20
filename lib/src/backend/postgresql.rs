use tokio_postgres::{types::ToSql, IsolationLevel};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime, Timeouts, Object, PoolError};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::{MakeTlsConnector};
use snafu::ResultExt;
use tokio::time::{Duration, sleep};
use crate::{
    api::{ApiRequest, ApiResponse, ContentType::*, },
    config::{VhostConfig,SchemaStructure::*},
    dynamic_statement::generate,
    error::{Result, *},
    schema::{DbSchema},
    dynamic_statement::{param, JoinIterator, SqlSnippet},
    formatter::postgresql::fmt_main_query,
};
use async_trait::async_trait;

use super::Backend;

use std::{collections::HashMap, fs};
use std::path::Path;
use serde_json::{Value as JsonValue};
use http::Method;

// use futures::future;

fn get_postgrest_env(role: Option<&String>, search_path: &[String], request: &ApiRequest, jwt_claims: &Option<JsonValue>, use_legacy_gucs: bool) -> HashMap<String, String> {
    let mut env = HashMap::new();
    if let Some(r) = role {
        env.insert("role".to_string(), r.clone());
        
    }
    
    env.insert("request.method".to_string(), format!("{}", request.method));
    env.insert("request.path".to_string(), request.path.to_string());
    //pathSql = setConfigLocal mempty ("request.path", iPath req)
    
    env.insert("search_path".to_string(), search_path.join(", "));
    if use_legacy_gucs {
        if let Some(r) = role {
            env.insert("request.jwt.claim.role".to_string(), r.clone());
        }
        
        env.extend(request.headers.iter().map(|(k, v)| (format!("request.header.{}", k.to_lowercase()), v.to_string())));
        env.extend(request.cookies.iter().map(|(k, v)| (format!("request.cookie.{}", k), v.to_string())));
        env.extend(request.get.iter().map(|(k, v)| (format!("request.get.{}", k), v.to_string())));
        match jwt_claims {
            Some(v) => match v.as_object() {
                Some(claims) => {
                    env.extend(claims.iter().map(|(k, v)| (
                        format!("request.jwt.claim.{}", k),
                        match v {
                            JsonValue::String(s) => s.clone(),
                            _ => format!("{}", v),
                        }
                    )));
                }
                None => {}
            },
            None => {}
        }
    }
    else {
        env.insert("request.headers".to_string(), 
            serde_json::to_string(
                &request
                .headers
                .iter()
                .map(|(k, v)| (k.to_lowercase(), v.to_string()))
                .collect::<Vec<_>>()
            ).unwrap()
        );
        env.insert("request.cookies".to_string(), 
            serde_json::to_string(
                &request
                .cookies
                .iter()
                .map(|(k, v)| (k, v.to_string()))
                .collect::<Vec<_>>()
            ).unwrap()
        );
        env.insert("request.get".to_string(), 
            serde_json::to_string(
                &request
                .get
                .iter()
                .map(|(k, v)| (k, v.to_string()))
                .collect::<Vec<_>>()
            ).unwrap()
        );
        match jwt_claims {
            Some(v) => match v.as_object() {
                Some(claims) => {
                    env.insert("request.jwt.claims".to_string(), serde_json::to_string(&claims).unwrap());                    
                }
                None => {}
            },
            None => {}
        }
    }
    
    env
}

fn get_postgrest_env_query<'a>(env: &'a HashMap<String, String>) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    "select "
        + env
            .iter()
            .map(|(k, v)| "set_config(" + param(k as &(dyn ToSql + Sync + 'a)) + ", " + param(v as &(dyn ToSql + Sync + 'a)) + ", true)")
            .join(",")
}

async fn execute<'a>(
    method: &Method, pool: &'a Pool, readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest, role: Option<&String>,
    jwt_claims: &Option<JsonValue>, config: &VhostConfig
) -> Result<ApiResponse> {
    let mut client = pool.get().await.context(PgDbPoolError)?;

    
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, request)?);
    let env = get_postgrest_env(role, &[schema_name.clone()], request, jwt_claims, config.db_use_legacy_gucs);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));
    //println!("{}\n{}\n{:?}", main_statement, env_statement, env_parameters);
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(readonly)
        .start()
        .await
        .context(PgDbError { authenticated })?;

    //paralel
    // let (env_stm, main_stm) = future::try_join(
    //         transaction.prepare_cached(env_statement.as_str()),
    //         transaction.prepare_cached(main_statement.as_str())
    //     ).await.context(PgDbError { authenticated })?;
    
    // let (_, rows) = future::try_join(
    //     transaction.query(&env_stm, env_parameters.as_slice()),
    //     transaction.query(&main_stm, main_parameters.as_slice())
    // ).await.context(PgDbError { authenticated })?;

    
    let env_stm = transaction
        .prepare_cached(env_statement.as_str())
        .await
        .context(PgDbError { authenticated })?;
    let _ = transaction
        .query(&env_stm, env_parameters.as_slice())
        .await
        .context(PgDbError { authenticated })?;

    if let Some((s, f)) = &config.db_pre_request {
        let fn_schema = match s.as_str() {
            "" => schema_name,
            _ => s,
        };

        let pre_request_statement = format!(r#"select "{}"."{}"()"#, fn_schema, f);
        let pre_request_stm = transaction
            .prepare_cached(pre_request_statement.as_str())
            .await
            .context(PgDbError { authenticated })?;
        transaction.query(&pre_request_stm, &[]).await.context(PgDbError { authenticated })?;
    }

    let main_stm = transaction
        .prepare_cached(main_statement.as_str())
        .await
        .context(PgDbError { authenticated })?;

    let rows = transaction
        .query(&main_stm, main_parameters.as_slice())
        .await
        .context(PgDbError { authenticated })?;

    
    let api_response = ApiResponse {
        page_total: rows[0].get("page_total"),
        total_result_set: rows[0].get("total_result_set"),
        top_level_offset: 0,
        response_headers: rows[0].get("response_headers"),
        response_status: rows[0].get("response_status"),
        body: rows[0].get("body"),
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        transaction.rollback().await.context(PgDbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    if method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        transaction.rollback().await.context(PgDbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    if config.db_tx_rollback {
        transaction.rollback().await.context(PgDbError { authenticated })?;
    } else {
        transaction.commit().await.context(PgDbError { authenticated })?;
    }

    Ok(api_response)
}

pub struct PostgreSQLBackend {
    //vhost: String,
    config: VhostConfig,
    pool: Pool,
    db_schema: DbSchema,
}

#[async_trait]
impl Backend for PostgreSQLBackend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection
        let pg_uri = config.db_uri.clone();
        let pg_config = pg_uri.parse::<tokio_postgres::Config>().unwrap();
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
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
        let db_schema = match &config.db_schema_structure {
            SqlFile(f) => match fs::read_to_string(
                vec![f, &format!("postgresql_{}", f)].into_iter().find(|f| Path::new(f).exists()).unwrap_or(f)
            ) {
                Ok(q) => match wait_for_pg_connection(&vhost, &pool).await {
                    Ok(mut client) => {
                        let authenticated = false;
                        let transaction = client
                            .build_transaction()
                            .isolation_level(IsolationLevel::Serializable)
                            .read_only(true)
                            .start()
                            .await
                            .context(PgDbError { authenticated})?;
                        let _ = transaction.query("set local schema ''", &[]).await;
                        match transaction.query(&q, &[&config.db_schemas]).await {
                            Ok(rows) => {
                                transaction.commit().await.context(PgDbError { authenticated })?;
                                serde_json::from_str::<DbSchema>(rows[0].get(0)).context(JsonDeserialize)
                            }
                            Err(e) => {
                                transaction.rollback().await.context(PgDbError { authenticated })?;
                                Err(e).context(PgDbError { authenticated })
                            }
                        }
                    }
                    Err(e) => Err(e).context(PgDbPoolError),
                },
                Err(e) => Err(e).context(ReadFile { path: f }),
            },
            JsonFile(f) => match fs::read_to_string(f) {
                Ok(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
                Err(e) => Err(e).context(ReadFile { path: f }),
            },
            JsonString(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
        }?;

        Ok(PostgreSQLBackend {config, pool, db_schema})
    }
    async fn execute(&self,
        method: &Method, readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest, role: Option<&String>,
        jwt_claims: &Option<JsonValue>
    ) -> Result<ApiResponse> {
        execute(method, &self.pool, readonly, authenticated, schema_name, request, role, jwt_claims, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema { &self.db_schema }
    fn config(&self) -> &VhostConfig { &self.config }
}

async fn wait_for_pg_connection(vhost: &String, db_pool: &Pool) -> Result<Object, PoolError> {

    let mut i = 1;
    let mut time_since_start = 0;
    let max_delay_interval = 10;
    let max_retry_interval = 30;
    let mut client = db_pool.get().await;
    while let Err(e)  = client {
        println!("[{}] Failed to connect to PostgreSQL {:?}", vhost, e);
        let time = Duration::from_secs(i);
        println!("[{}] Retrying the PostgreSQL connection in {:?} seconds..", vhost, time.as_secs());
        sleep(time).await;
        client = db_pool.get().await;
        i *= 2;
        if i > max_delay_interval { i = max_delay_interval };
        time_since_start += i;
        if time_since_start > max_retry_interval { break }
    };
    match client {
        Err(_) =>{},
        _ => println!("[{}] Connection to PostgreSQL successful", vhost)
    }
    client
}