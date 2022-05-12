#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rocket;
use http::{Method, HeaderMap};
// use lazy_static::__Deref;
use snafu::{OptionExt, ResultExt};
use std::collections::HashMap;
use std::convert::TryInto;
use std::net::IpAddr;

use figment::{
    providers::{Env, Format, Toml},
    Figment, Profile,
};
use rocket::{
    routes,
    fs::{FileServer, Options},
    http::{uri::Origin, CookieJar, Header, Status, ContentType as HTTPContentType},
    Build, Config as RocketConfig, Rocket, State,
};
use subzero::{
    config::VhostConfig,
    error::{GucStatusError, Error, ProxyError},
    frontend::postgrest,
    api::ContentType::{SingularJSON, TextCSV, ApplicationJSON},
    backend::{Backend},
    deno::{DenoProxy}
};

#[cfg(feature = "postgresql")]
use subzero::backend::postgresql::PostgreSQLBackend;
#[cfg(feature = "sqlite")]
use subzero::backend::sqlite::SQLiteBackend;

mod rocket_util;
use rocket_util::{AllHeaders, ApiResponse, ProxyResponse, QueryString, RocketError};

type DbBackend = Box<dyn Backend + Send + Sync>;
lazy_static! {
    static ref SINGLE_CONTENT_TYPE: HTTPContentType = HTTPContentType::parse_flexible("application/vnd.pgrst.object+json").unwrap();
}

// define rocket request handlers, they are just wrappers around handle_request function
// since rocket does not allow yet a single function to handle multiple verbs
// #[get("/")]
// fn index() -> &'static str { "Hello, world!" }

#[get("/<table>?<parameters..>")]
async fn get<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::GET, &table, origin, parameters, None, cookies, headers, db_backend).await
}

#[post("/<table>?<parameters..>", data = "<body>")]
async fn post<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::POST, &table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[delete("/<table>?<parameters..>", data = "<body>")]
async fn delete<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::DELETE, &table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[patch("/<table>?<parameters..>", data = "<body>")]
async fn patch<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PATCH, &table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[put("/<table>?<parameters..>", data = "<body>")]
async fn put<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PUT, &table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[get("/<_..>")]
async fn proxy_get<'a>( origin: &Origin<'a>, headers: AllHeaders<'a>, client_ip: IpAddr, proxy: &State<DenoProxy>) -> Result<ProxyResponse, RocketError>{
    proxy_deno(&Method::GET, origin, headers, None, client_ip, proxy).await
}

#[post("/<_..>", data = "<body>")]
async fn proxy_post<'a>( origin: &Origin<'a>, headers: AllHeaders<'a>, body: String, client_ip: IpAddr, proxy: &State<DenoProxy>) -> Result<ProxyResponse, RocketError>{
    proxy_deno(&Method::POST, origin, headers, Some(body), client_ip, proxy).await
}

#[patch("/<_..>", data = "<body>")]
async fn proxy_patch<'a>( origin: &Origin<'a>, headers: AllHeaders<'a>, body: String, client_ip: IpAddr, proxy: &State<DenoProxy>) -> Result<ProxyResponse, RocketError>{
    proxy_deno(&Method::PATCH, origin, headers, Some(body), client_ip, proxy).await
}

#[delete("/<_..>", data = "<body>")]
async fn proxy_delete<'a>( origin: &Origin<'a>, headers: AllHeaders<'a>, body: String, client_ip: IpAddr, proxy: &State<DenoProxy>) -> Result<ProxyResponse, RocketError>{
    proxy_deno(&Method::DELETE, origin, headers, Some(body), client_ip, proxy).await
}

#[put("/<_..>", data = "<body>")]
async fn proxy_put<'a>( origin: &Origin<'a>, headers: AllHeaders<'a>, body: String, client_ip: IpAddr, proxy: &State<DenoProxy>) -> Result<ProxyResponse, RocketError>{
    proxy_deno(&Method::PUT, origin, headers, Some(body), client_ip, proxy).await
}





// main request handler
// this is mostly to align types between rocket and subzero functions
async fn handle_request(
    method: &Method, table: &String, origin: &Origin<'_>, parameters: QueryString, body: Option<String>, cookies: &CookieJar<'_>,
    headers: AllHeaders<'_>, db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    let (status, response_content_type, response_headers, response_body) = postgrest::handle(
        table,
        method,
        origin.path().to_string(),
        parameters.0,
        body,
        headers.iter().map(|h| (h.name().as_str().to_lowercase().to_string(), h.value().to_string())).collect(),
        cookies.iter().map(|c| (c.name().to_string(), c.value().to_string())).collect(),
        db_backend,
    )
    .await
    .map_err(|e| RocketError(e))?;

    let http_content_type = match response_content_type {
        SingularJSON => SINGLE_CONTENT_TYPE.clone(),
        TextCSV => HTTPContentType::CSV,
        ApplicationJSON => HTTPContentType::JSON,
    };

    Ok(ApiResponse {
        response: (
            Status::from_code(status).context(GucStatusError).map_err(|e| RocketError(e))?,
            (http_content_type, response_body),
        ),
        headers: response_headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

// proxy request to deno
async fn proxy_deno<'a>(
    method: &Method, origin: &Origin<'a>, headers: AllHeaders<'a>, body: Option<String>, client_ip: IpAddr, proxy: &DenoProxy
) -> Result<ProxyResponse, RocketError>{
    let url = origin.to_string();
    let hdrs = headers.iter().map(|h| (h.name().as_str().to_string(), h.value().to_string())).collect::<HashMap<_,_>>();
    let hdrs2: HeaderMap = (&hdrs).try_into().unwrap_or_default();
    let ip = client_ip.to_string();
    match proxy.forward(method, url.as_str(), hdrs2, body, ip.as_str() ).await {
        Ok(response) => {
            let (parts, body) = response.into_parts();
            let status = parts.status.as_u16();
            let headers = parts.headers;
            let body = hyper::body::to_bytes(body).await.context(ProxyError).map_err(|e| RocketError(e))?;
            Ok(ProxyResponse { status, headers, body})
        },
        Err(e) => {
            Err(RocketError(Error::InternalError { message: e.to_string() }))
        }
    }
}

// main function where we read the configuration and initialize the rocket webserver
#[allow(unreachable_code)]
async fn start() -> Result<Rocket<Build>, Error> {
    #[cfg(debug_assertions)]
    let profile = RocketConfig::DEBUG_PROFILE;

    #[cfg(not(debug_assertions))]
    let profile = RocketConfig::RELEASE_PROFILE;

    // try to read the configuration from both a file and env vars
    // this configuration includes both subzero specific settings (VhostConfig type)
    // and rocket configuration
    let config = Figment::from(RocketConfig::default())
        .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
        .merge(Env::prefixed("SUBZERO_").split("__").ignore(&["PROFILE"]).global())
        .select(Profile::from_env_or("SUBZERO_PROFILE", profile));

    // extract the subzero specific part of the configuration
    let vhost_config: VhostConfig = config.extract().expect("config");
    
    #[allow(unused_variables)]
    let url_prefix = vhost_config.url_prefix.clone().unwrap_or("/".to_string());
    //initialize the backend
    #[allow(unused_variables)]
    let backend: Box<dyn Backend + Send + Sync> = match vhost_config.db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => Box::new(PostgreSQLBackend::init("default".to_string(), vhost_config.clone()).await?),
        #[cfg(feature = "sqlite")]
        "sqlite" => Box::new(SQLiteBackend::init("default".to_string(), vhost_config.clone()).await?),
        t => panic!("unsupported database type: {}", t),
    };

    
    // initialize the web server
    let mut server = rocket::custom(config)
        .manage(backend)
        .mount(&url_prefix, routes![get, post, delete, patch, put])
        .mount(format!("{}/rpc", &url_prefix), routes![get, post]);

    if let Some(deno_config) = &vhost_config.deno {
        let deno_proxy = DenoProxy::new(deno_config).await?;
        server = server.manage(deno_proxy);
        for p in &deno_config.paths {
            server = server.mount(p, routes![proxy_get, proxy_post, proxy_delete, proxy_patch, proxy_put]);
        }
    }
    if let Some(static_dir) = &vhost_config.static_files_dir {
        let options = Options::Index;
        server = server.mount("/", FileServer::new(static_dir, options).rank(-100));
    }
    Ok(server)
}

#[launch]
async fn rocket() -> Rocket<Build> {
    match start().await {
        Ok(r) => r,
        Err(e) => panic!("{}", e),
    }
}


// #[cfg(test)]
// #[macro_use]
// extern crate lazy_static;

#[cfg(test)]
#[path = "../tests/haskell_test.rs"]
mod haskell_test;

#[cfg(feature = "postgresql")]
#[cfg(test)]
#[path = "../tests/basic/mod.rs"]
mod basic;

#[cfg(feature = "postgresql")]
#[cfg(test)]
#[path = "../tests/postgrest/mod.rs"]
mod postgrest_core;

#[cfg(feature = "sqlite")]
#[cfg(test)]
#[path = "../tests/sqlite/mod.rs"]
mod sqlite;
