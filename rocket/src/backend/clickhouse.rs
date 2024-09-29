// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
// use hyper::{Client, client::HttpConnector, Body, Uri};
use reqwest::{Client, Url};
// use url::Url;
use http::Uri;
//use formdata::{FormData, generate_boundary, write_formdata};
use deadpool::managed;
//use rocket::http::hyper::body;
use snafu::ResultExt;
use tokio::time::Duration;
use crate::error::{Result, *};
use crate::config::{VhostConfig, SchemaStructure::*};
use subzero_core::{
    api::{ApiRequest, ApiResponse, SingleVal, Payload, ListVal},
    error::{Error as CoreError, JsonDeserializeSnafu, JsonSerializeSnafu},
    schema::{DbSchema, replace_json_str},
    formatter::{
        Param::*,
        clickhouse::{fmt_main_query, generate},
    },
};
use std::collections::HashMap;
//use std::panic::resume_unwind;
use async_trait::async_trait;
// use http::Error as HttpError;
// use log::{debug};
use super::{Backend, DbSchemaWrap, include_files};

use std::fs;
use std::path::Path;
use serde_json::Value as JsonValue;
use http::Method;
use base64::{Engine as _, engine::general_purpose};

type HttpClient = (Url, Uri, Client);
type Pool = managed::Pool<Manager>;
struct Manager {
    uri: String,
}
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

// ClickHouse uses 3s by default.
// See https://github.com/ClickHouse/ClickHouse/blob/368cb74b4d222dc5472a7f2177f6bb154ebae07a/programs/server/config.xml#L201
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);
impl managed::Manager for Manager {
    type Type = HttpClient;
    type Error = reqwest::Error;

    async fn create(&self) -> Result<HttpClient, reqwest::Error> {
        let client = Client::builder()
            .tcp_keepalive(Some(TCP_KEEPALIVE))
            .pool_idle_timeout(Some(POOL_IDLE_TIMEOUT))
            .build()?;
        Ok((self.uri.parse::<Url>().unwrap(), self.uri.parse::<Uri>().unwrap(), client))
    }

    async fn recycle(&self, _: &mut HttpClient, _: &managed::Metrics) -> managed::RecycleResult<reqwest::Error> {
        Ok(())
    }
}

async fn execute<'a>(
    schema: &DbSchema<'a>, pool: &Pool, _authenticated: bool, request: &ApiRequest<'_>, env: &HashMap<&str, &str>, _config: &VhostConfig,
) -> Result<ApiResponse> {
    let o = pool.get().await.unwrap(); //.context(ClickhouseDbPoolError)?;
    let uri = &o.0;
    let base_url = &o.1;
    let client = &o.2;
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema, request.schema_name, request, env).context(CoreSnafu)?);
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

    let form = parameters.into_iter().fold(reqwest::multipart::Form::new(), |form, (k, v)| {
        form.text(k, v)
    });
    // let formdata = FormData {
    //     fields: parameters,
    //     files: vec![],
    // };
    // let mut http_body: Vec<u8> = Vec::new();
    // let boundary = generate_boundary();
    // write_formdata(&mut http_body, &boundary, &formdata).expect("write_formdata error");

    let mut http_request = client
        .post(base_url.to_string())
        .multipart(form);
    //.body(Body::from(http_body)).context(HttpRequestError)?;
    if uri.username() != "" {
        http_request = http_request.header(
            reqwest::header::AUTHORIZATION,
            format!("Basic {}", general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", uri.username(), uri.password().unwrap_or_default()))),
        );
    }

    //let http_request = http_request.body(http_body);
    let http_response = http_request.send().await.context(ReqwestSnafu)?;
    let page_total = {
        let headers = http_response.headers();
        debug!("headers {:?}", headers);
        match headers.get("x-clickhouse-summary") {
        Some(s) => match serde_json::from_str::<JsonValue>(s.to_str().unwrap_or("")) {
            Ok(v) => {
                debug!("read_rows {:?}", v["read_rows"].as_str());
                v["read_rows"].as_str().unwrap_or("0").parse().unwrap_or(0)
            }
            Err(_) => 0,
        },
        None => 0,
    }};
    let status = http_response.status();
    let body = http_response.text().await.context(ReqwestSnafu)?;
    debug!("status {:?}", status);
    //debug!("headers {:?}", headers);
    // let bytes = hyper::body::to_bytes(body).await.context(HyperSnafu)?;
    // let body = String::from_utf8(bytes.to_vec()).unwrap_or_default();
    
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
        return Err(to_core_error(CoreError::PutMatchingPkError));
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
                Ok(q) => {
                    let o = pool.get().await.context(ClickhouseDbPoolSnafu)?;
                    let uri = &o.0;
                    let base_url = &o.1;
                    let client = &o.2;
                    let query = include_files(q);
                    let parameters = vec![
                            ("param_p1".to_owned(), format!("['{}']", config.db_schemas.join("','"))),
                            ("query".to_owned(), query),
                    ];
                    let form = parameters.into_iter().fold(reqwest::multipart::Form::new(), |form, (k, v)| {
                        form.text(k, v)
                    });
                    let mut http_request = client
                        .post(base_url.to_string())
                        .multipart(form);
                    if uri.username() != "" {
                        http_request = http_request.header(
                            reqwest::header::AUTHORIZATION,
                            format!(
                                "Basic {}",
                                general_purpose::STANDARD_NO_PAD.encode(format!("{}:{}", uri.username(), uri.password().unwrap_or_default()))
                            ),
                        );
                    }

                    let http_response = http_request.send().await.context(ReqwestSnafu)?;
                    let s = http_response.text().await.context(ReqwestSnafu)?;
                    //println!("s: {}", s);
                    let mut v: JsonValue = serde_json::from_str(&s).context(JsonDeserializeSnafu).context(CoreSnafu)?;
                    replace_json_str(&mut v).context(CoreSnafu)?;
                    let s = serde_json::to_string_pretty(&v).context(JsonSerializeSnafu).context(CoreSnafu)?;

                    Ok(DbSchemaWrap::new(s, |s| {
                        serde_json::from_str::<DbSchema>(s.as_str())
                            .context(JsonDeserializeSnafu)
                            .context(CoreSnafu)
                    }))
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
        execute(self.db_schema(), &self.pool, authenticated, request, env, &self.config).await
    }
    fn db_schema(&self) -> &DbSchema {
        self.db_schema.borrow_schema().as_ref().unwrap()
    }
    fn config(&self) -> &VhostConfig {
        &self.config
    }
}
