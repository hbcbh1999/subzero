// use tokio_postgres::{types::ToSql, IsolationLevel};
// use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime, Timeouts, Object, PoolError};
// use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
// use postgres_openssl::{MakeTlsConnector};
//use clickhouse::{Client, query::Query, error::Error as ClickhouseErr};
use http::Uri;
use hyper::{Client, client::HttpConnector, Body};
use form_urlencoded::Serializer;
use deadpool::{managed,};
use snafu::ResultExt;
use tokio::time::{Duration, sleep};
use crate::{
    api::{ApiRequest, ApiResponse, ContentType::*, SingleVal, Payload, ListVal},
    config::{VhostConfig,SchemaStructure::*},
    dynamic_statement::{generate_fn, SqlSnippet, SqlSnippetChunk},
    error::{Result, *},
    schema::{DbSchema},
    //dynamic_statement::{param, JoinIterator, SqlSnippet},
    formatter::clickhouse::{fmt_main_query, Param::*, SqlParam},
};
use log::{debug};
use async_trait::async_trait;

use super::Backend;

use std::{fs};
use std::path::Path;
use serde_json::{Value as JsonValue};
use http::Method;

type HttpClient = (String, Client<HttpConnector>);
struct Manager {
    uri: String,
}
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
#[async_trait]
impl managed::Manager for Manager {
    type Type = HttpClient;
    type Error = HttpRequestError;
    
    async fn create(&self) -> Result<HttpClient, HttpRequestError> {
        let mut connector = HttpConnector::new();

        // TODO: make configurable in `Client::builder()`.
        connector.set_keepalive(Some(TCP_KEEPALIVE));

        let client = hyper::Client::builder()
            .pool_idle_timeout(POOL_IDLE_TIMEOUT)
            .build(connector);
        Ok((self.uri, client))
    }
    
    async fn recycle(&self, _: &mut HttpClient) -> managed::RecycleResult<HttpRequestError> {
        Ok(())
    }
}

type Pool = managed::Pool<Manager>;

macro_rules! param_placeholder_format {() => {"{{p_{pos}:String}}"};}
generate_fn!();

async fn execute<'a>(
    pool: &'a Pool, authenticated: bool, request: &ApiRequest, _role: Option<&String>,
    _jwt_claims: &Option<JsonValue>, _config: &VhostConfig
) -> Result<ApiResponse> {
    let o = pool.get().await.unwrap();//.context(ClickhouseDbPoolError)?;
    let base_url = o.0.clone();
    let client = &o.1;
    let (main_statement, main_parameters, _) = generate(fmt_main_query(&request.schema_name, request)?);
    let mut parameters = vec![("query".to_string(), main_statement)];
    for (k, v) in main_parameters.iter().enumerate() {
        let p = match v.to_param() {
            SV(SingleVal(v)) => v.to_string(),
            LV(ListVal(v)) => format!("[{}]",v.join(",")),
            PL(Payload(v)) => v.to_string(),
        };
        parameters.push((format!("p_{}",k+1), p));
    }
    let http_body = Serializer::new(String::new()).extend_pairs(
        parameters.iter().map(|(k, v)| { (&k[..], &v[..]) })
    ).finish();

    let http_request = hyper::Request::builder()
        .uri(base_url)
        .method(http::Method::POST)
        .body(Body::from(http_body)).context(HttpRequestError)?;

    let http_response = match client.request(http_request).await {
        Ok(r) => Ok(r),
        Err(e) => Err(Error::InternalError { message: e.to_string() }),
    }?;
    let (parts, body) = http_response.into_parts();
    let status = parts.status.as_u16();
    let headers = parts.headers;
    let bytes = hyper::body::to_bytes(body).await.context(ProxyError)?;
    let body = String::from_utf8(bytes.to_vec()).unwrap_or("".to_string());

    let api_response = ApiResponse {
        page_total: 1,
        total_result_set: None,
        top_level_offset: 0,
        response_headers: None,
        response_status: None,
        body,
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        //transaction.rollback().await.context(PgDbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    if request.method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        //transaction.rollback().await.context(PgDbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    // if config.db_tx_rollback {
    //     //transaction.rollback().await.context(PgDbError { authenticated })?;
    // } else {
    //     //transaction.commit().await.context(PgDbError { authenticated })?;
    // }

    Ok(api_response)
}

pub struct ClickhouseBackend {
    //vhost: String,
    config: VhostConfig,
    pool: Pool,
    db_schema: DbSchema,
}

#[async_trait]
impl Backend for ClickhouseBackend {
    async fn init(_vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection
        //let _ch_uri = config.db_uri.parse::<Uri>().unwrap();
        
        let mgr = Manager {uri: config.db_uri.clone()};
        let pool = Pool::builder(mgr)
            .max_size(config.db_pool)
            .build()
            .unwrap();
        
        //read db schema
        let db_schema = match &config.db_schema_structure {
            SqlFile(f) => match fs::read_to_string(
                vec![f, &format!("clickhouse_{}", f)].into_iter().find(|f| Path::new(f).exists()).unwrap_or(f)
            ) {
                Ok(q) => match pool.get().await {
                    Ok(client) => {
                        let authenticated = false;
                        // let transaction = client
                        //     .build_transaction()
                        //     .isolation_level(IsolationLevel::Serializable)
                        //     .read_only(true)
                        //     .start()
                        //     .await
                        //     .context(PgDbError { authenticated})?;
                        // let _ = transaction.query("set local schema ''", &[]).await;
                        // match transaction.query(&q, &[&config.db_schemas]).await {
                        let lv = ListVal(config.db_schemas.clone());
                        let params:Vec<&SqlParam> = vec![&lv];
                        let main_stm = params.into_iter().fold(client.query(&q), bind);
                        let mut cursor = main_stm.fetch::<String>().context(ClickhouseDbError {authenticated})?;
                        match cursor.next().await.context(ClickhouseDbError {authenticated})? {
                            Some(s) => {
                                // transaction.commit().await.context(PgDbError { authenticated })?;
                                debug!("introspection query response: {:?}", s);
                                serde_json::from_str::<DbSchema>(&s).context(JsonDeserialize)
                            }
                            None => {
                                //transaction.rollback().await.context(PgDbError { authenticated })?;
                                debug!("got no rows error");
                                Err(Error::InternalError { message: "clickhouse structure query did not return any rows".to_string() })
                            }
                        }
                    }
                    Err(e) => Err(e).context(ClickhouseDbPoolError),
                },
                Err(e) => Err(e).context(ReadFile { path: f }),
            },
            JsonFile(f) => match fs::read_to_string(f) {
                Ok(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
                Err(e) => Err(e).context(ReadFile { path: f }),
            },
            JsonString(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
        }?;

        Ok(ClickhouseBackend {config, pool, db_schema})
    }
    async fn execute(
        &self, authenticated: bool, request: &ApiRequest, role: Option<&String>, jwt_claims: &Option<JsonValue>
    ) -> Result<ApiResponse> {
        execute(&self.pool, authenticated, request, role, jwt_claims, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema { &self.db_schema }
    fn config(&self) -> &VhostConfig { &self.config }
}

// async fn wait_for_pg_connection(vhost: &String, db_pool: &Pool) -> Result<Object, PoolError> {

//     let mut i = 1;
//     let mut time_since_start = 0;
//     let max_delay_interval = 10;
//     let max_retry_interval = 30;
//     let mut client = db_pool.get().await;
//     while let Err(e)  = client {
//         println!("[{}] Failed to connect to PostgreSQL {:?}", vhost, e);
//         let time = Duration::from_secs(i);
//         println!("[{}] Retrying the PostgreSQL connection in {:?} seconds..", vhost, time.as_secs());
//         sleep(time).await;
//         client = db_pool.get().await;
//         i *= 2;
//         if i > max_delay_interval { i = max_delay_interval };
//         time_since_start += i;
//         if time_since_start > max_retry_interval { break }
//     };
//     match client {
//         Err(_) =>{},
//         _ => println!("[{}] Connection to PostgreSQL successful", vhost)
//     }
//     client
// }