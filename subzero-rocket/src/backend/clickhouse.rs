use hyper::{Client, client::HttpConnector, Body, Uri};
use url::{Url};
use formdata::{FormData, generate_boundary, write_formdata};
use deadpool::{managed};
use snafu::ResultExt;
use tokio::time::{Duration};
use crate::error::{Result, *};
use crate::config::{VhostConfig, SchemaStructure::*};
use subzero_core::{
    api::{ApiRequest, ApiResponse, SingleVal, Payload, ListVal},
    error::{Error, JsonDeserializeSnafu, JsonSerializeSnafu},
    schema::{DbSchema},
    formatter::{
        Param::*,
        clickhouse::{fmt_main_query, generate},
    },
};
use std::collections::HashMap;
use async_trait::async_trait;
use http::Error as HttpError;
// use log::{debug};
use super::{Backend, DbSchemaWrap, include_files};

use std::{fs};
use std::path::Path;
use serde_json::{Value as JsonValue};
use http::Method;
use base64::{Engine as _, engine::general_purpose};

type HttpClient = (Url, Uri, Client<HttpConnector>);
type Pool = managed::Pool<Manager>;
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
    type Error = HttpError;

    async fn create(&self) -> Result<HttpClient, HttpError> {
        let mut connector = HttpConnector::new();

        // TODO: make configurable in `Client::builder()`.
        connector.set_keepalive(Some(TCP_KEEPALIVE));

        let client = hyper::Client::builder().pool_idle_timeout(POOL_IDLE_TIMEOUT).build(connector);
        Ok((self.uri.parse::<Url>().unwrap(), self.uri.parse::<Uri>().unwrap(), client))
    }

    async fn recycle(&self, _: &mut HttpClient) -> managed::RecycleResult<HttpError> { Ok(()) }
}

async fn execute(
    pool: &Pool, _authenticated: bool, request: &ApiRequest<'_>, env: &HashMap<&str, &str>, _config: &VhostConfig,
) -> Result<ApiResponse> {
    let o = pool.get().await.unwrap(); //.context(ClickhouseDbPoolError)?;
    let uri = &o.0;
    let base_url = &o.1;
    let client = &o.2;
    let (main_statement, main_parameters, _) = generate(fmt_main_query(request.schema_name, request, env).context(CoreSnafu)?);
    debug!("main_statement {}", main_statement);
    let mut parameters = vec![("query".to_string(), main_statement)];
    for (k, v) in main_parameters.iter().enumerate() {
        let p = match v.to_param() {
            SV(SingleVal(v, _)) => v.to_string(),
            LV(ListVal(v, _)) => format!("[{}]", v.join(",")),
            PL(Payload(v, _)) => v.to_string(),
            StrOwned(v) => v.clone(),
            Str(v) => v.to_string(),
        };
        parameters.push((format!("param_p{}", k + 1), p));
    }
    debug!("parameters {:?}", parameters);

    let formdata = FormData {
        fields: parameters,
        files: vec![],
    };
    let mut http_body: Vec<u8> = Vec::new();
    let boundary = generate_boundary();
    write_formdata(&mut http_body, &boundary, &formdata).expect("write_formdata error");

    let mut http_request = hyper::Request::builder().uri(base_url).method(http::Method::POST).header(
        "Content-Type",
        format!("multipart/form-data; boundary={}", std::str::from_utf8(boundary.as_slice()).unwrap()),
    );
    //.body(Body::from(http_body)).context(HttpRequestError)?;
    if uri.username() != "" {
        http_request = http_request.header(
            hyper::header::AUTHORIZATION,
            format!("Basic {}",general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", uri.username(), uri.password().unwrap_or_default()))),
        );
    }

    let http_req = http_request.body(Body::from(http_body)).context(HttpRequestSnafu)?;
    let http_response = match client.request(http_req).await {
        Ok(r) => Ok(r),
        Err(e) => Err(Error::InternalError { message: e.to_string() }),
    }
    .context(CoreSnafu)?;
    let (parts, body) = http_response.into_parts();
    let status = parts.status.as_u16();
    let headers = parts.headers;
    debug!("status {:?}", status);
    debug!("headers {:?}", headers);
    let bytes = hyper::body::to_bytes(body).await.context(HyperSnafu)?;
    let body = String::from_utf8(bytes.to_vec()).unwrap_or_default();
    let page_total = match headers.get("x-clickhouse-summary") {
        Some(s) => match serde_json::from_str::<JsonValue>(s.to_str().unwrap_or("")) {
            Ok(v) => {
                debug!("read_rows {:?}", v["read_rows"].as_str());
                v["read_rows"].as_str().unwrap_or("0").parse().unwrap_or(0)
            }
            Err(_) => 0,
        },
        None => 0,
    };
    debug!("page_total {:?}", page_total);
    let api_response = ApiResponse {
        page_total,
        total_result_set: None,
        top_level_offset: 0,
        response_headers: None,
        response_status: None,
        body,
    };

    // if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
    //     //transaction.rollback().await.context(PgDbError { authenticated })?;
    //     return Err(Error::SingularityError {
    //         count: api_response.page_total,
    //         content_type: "application/vnd.pgrst.object+json".to_string(),
    //     });
    // }

    if request.method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        //transaction.rollback().await.context(PgDbError { authenticated })?;
        return Err(to_core_error(Error::PutMatchingPkError));
    }

    // if config.db_tx_rollback {
    //     //transaction.rollback().await.context(PgDbError { authenticated })?;
    // } else {
    //     //transaction.commit().await.context(PgDbError { authenticated })?;
    // }

    Ok(api_response)
}

pub struct ClickhouseBackend {
    config: VhostConfig,
    pool: Pool,
    db_schema: DbSchemaWrap,
}
fn replace_json_str(v: &mut JsonValue) -> Result<()> {
    match v {
        JsonValue::Object(o) => {
            if let Some(s) = o.get_mut("check_json_str") {
                if let Some(ss) = s.as_str() {
                    let j = serde_json::from_str::<JsonValue>(ss).context(JsonDeserializeSnafu).context(CoreSnafu)?;
                    *s = JsonValue::Null;
                    o.insert("check".to_string(), j);
                }
            }
            if let Some(s) = o.get_mut("using_json_str") {
                if let Some(ss) = s.as_str() {
                    let j = serde_json::from_str::<JsonValue>(ss).context(JsonDeserializeSnafu).context(CoreSnafu)?;
                    *s = JsonValue::Null;
                    o.insert("using".to_string(), j);
                }
            }
            for (_, v) in o {
                replace_json_str(v)?;
            }
            Ok(())
        }
        JsonValue::Array(a) => {
            for v in a {
                replace_json_str(v)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[async_trait]
impl Backend for ClickhouseBackend {
    async fn init(_vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection
        let mgr = Manager { uri: config.db_uri.clone() };
        let pool = Pool::builder(mgr).max_size(config.db_pool).build().unwrap();
        //read db schema
        let db_schema: DbSchemaWrap = match config.db_schema_structure.clone() {
            SqlFile(f) => match fs::read_to_string(
                vec![&f, &format!("clickhouse_{f}")]
                    .into_iter()
                    .find(|f| Path::new(f).exists())
                    .unwrap_or(&f),
            ) {
                Ok(q) => match pool.get().await {
                    Ok(o) => {
                        let uri = &o.0;
                        let base_url = &o.1;
                        let client = &o.2;
                        let query = include_files(q);
                        //println!("query {}", query);
                        let formdata = FormData {
                            fields: vec![
                                ("param_p1".to_owned(), format!("['{}']", config.db_schemas.join("','"))),
                                ("query".to_owned(), query),
                            ],
                            files: vec![],
                        };
                        let mut http_body: Vec<u8> = Vec::new();
                        let boundary = generate_boundary();
                        write_formdata(&mut http_body, &boundary, &formdata).expect("write_formdata error");

                        let mut http_request = hyper::Request::builder().uri(base_url).method(http::Method::POST).header(
                            "Content-Type",
                            format!("multipart/form-data; boundary={}", std::str::from_utf8(boundary.as_slice()).unwrap()),
                        );
                        //.body(Body::from(http_body)).context(HttpRequestError)?;
                        if uri.username() != "" {
                            http_request = http_request.header(
                                hyper::header::AUTHORIZATION,
                                format!("Basic {}", general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", uri.username(), uri.password().unwrap_or_default()))),
                            );
                        }

                        let http_req = http_request.body(Body::from(http_body)).context(HttpRequestSnafu)?;
                        let http_response = match client.request(http_req).await {
                            Ok(r) => Ok(r),
                            Err(e) => Err(Error::InternalError { message: e.to_string() }),
                        }
                        .context(CoreSnafu)?;
                        let (parts, body) = http_response.into_parts();

                        let _status = parts.status.as_u16();
                        let _headers = parts.headers;
                        let bytes = hyper::body::to_bytes(body).await.context(HyperSnafu)?;
                        let s = String::from_utf8(bytes.to_vec()).unwrap_or_default();
                        //println!("json schema original:\n{:?}\n", s);
                        // clickhouse query returns check_json_str and using_json_str as string
                        // so we first parse it into a JsonValue and then convert those two fileds into json
                        let mut v: JsonValue = serde_json::from_str(&s).context(JsonDeserializeSnafu).context(CoreSnafu)?;
                        //recursivley iterate through the json and convert check_json_str and using_json_str into json
                        // println!("json value before replace:\n{:?}\n", v);
                        // recursively iterate through the json and apply the f function
                        replace_json_str(&mut v)?;
                        let s = serde_json::to_string_pretty(&v).context(JsonSerializeSnafu).context(CoreSnafu)?;

                        //let schema: DbSchema = serde_json::from_str(&s).context(JsonDeserialize).context(CoreError)?;
                        //println!("schema {:?}", schema);
                        Ok(DbSchemaWrap::new(s, |s| {
                            serde_json::from_str::<DbSchema>(s.as_str())
                                .context(JsonDeserializeSnafu)
                                .context(CoreSnafu)
                        }))
                    }
                    Err(e) => Err(e).context(ClickhouseDbPoolSnafu),
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

        Ok(ClickhouseBackend { config, pool, db_schema })
    }
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse> {
        execute(&self.pool, authenticated, request, env, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema { self.db_schema.borrow_schema().as_ref().unwrap() }
    fn config(&self) -> &VhostConfig { &self.config }
}
