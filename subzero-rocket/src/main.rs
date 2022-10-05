#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rocket;
use http::{Method};
use snafu::{OptionExt, ResultExt};
use std::collections::HashMap;
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
mod frontend;
use frontend::postgrest;
mod config;
use config::VhostConfig;
use subzero_core::{
    error::{GucStatusError},
    api::ContentType::{SingularJSON, TextCSV, ApplicationJSON},
};
mod error;
use error::{Error, Core};

mod backend;
use backend::{Backend};

#[cfg(feature = "postgresql")]
use backend::postgresql::PostgreSQLBackend;

#[cfg(feature = "clickhouse")]
use backend::clickhouse::ClickhouseBackend;

#[cfg(feature = "sqlite")]
use backend::sqlite::SQLiteBackend;

mod rocket_util;
use rocket_util::{AllHeaders, ApiResponse, QueryString, RocketError};

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
    table: &'a str, origin: &Origin<'_>, parameters: QueryString<'a>, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::GET, table, origin, parameters, None, cookies, headers, db_backend).await
}

#[post("/<table>?<parameters..>", data = "<body>")]
async fn post<'a>(
    table: &'a str, origin: &Origin<'_>, parameters: QueryString<'a>, body: &'a str, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::POST, table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[delete("/<table>?<parameters..>", data = "<body>")]
async fn delete<'a>(
    table: &'a str, origin: &Origin<'_>, parameters: QueryString<'a>, body: &'a str, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::DELETE, table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[patch("/<table>?<parameters..>", data = "<body>")]
async fn patch<'a>(
    table: &'a str, origin: &Origin<'_>, parameters: QueryString<'a>, body: &'a str, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PATCH, table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

#[put("/<table>?<parameters..>", data = "<body>")]
async fn put<'a>(
    table: &'a str, origin: &Origin<'_>, parameters: QueryString<'a>, body: &'a str, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PUT, table, origin, parameters, Some(body), cookies, headers, db_backend).await
}

// main request handler
// this is mostly to align types between rocket and subzero functions
#[allow(clippy::too_many_arguments)]
async fn handle_request(
    method: &Method, table: &str, origin: &Origin<'_>, parameters: QueryString<'_>, body: Option<&str>, cookies: &CookieJar<'_>,
    headers: AllHeaders<'_>, db_backend: &State<DbBackend>,
) -> Result<ApiResponse, RocketError> {
    let headers_str = headers
        .iter()
        .map(|h| (h.name().as_str().to_lowercase(), h.value().to_string()))
        .collect::<HashMap<_, _>>();
    let (status, response_content_type, response_headers, response_body) = postgrest::handle(
        table,
        method,
        origin.path().to_string().as_str(),
        parameters.0,
        body,
        headers_str.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect(),
        cookies.iter().map(|c| (c.name(), c.value())).collect(),
        db_backend,
    )
    .await
    .map_err(RocketError)?;

    let http_content_type = match response_content_type {
        SingularJSON => SINGLE_CONTENT_TYPE.clone(),
        TextCSV => HTTPContentType::CSV,
        ApplicationJSON => HTTPContentType::JSON,
    };

    Ok(ApiResponse {
        response: (
            Status::from_code(status).context(GucStatusError).context(Core).map_err(RocketError)?,
            (http_content_type, response_body),
        ),
        headers: response_headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
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

    let default_prefix = "/".to_string();
    #[allow(unused_variables)]
    let url_prefix = vhost_config.url_prefix.clone().unwrap_or(default_prefix);

    // initialize the web server
    let mut server = rocket::custom(config)
        .mount(&url_prefix, routes![get, post, delete, patch, put])
        .mount(format!("{}/rpc", &url_prefix), routes![get, post]);

    //initialize the backend
    #[allow(unused_variables)]
    let backend: Box<dyn Backend + Send + Sync> = match vhost_config.db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => Box::new(PostgreSQLBackend::init("default".to_string(), vhost_config.clone()).await?),
        #[cfg(feature = "clickhouse")]
        "clickhouse" => Box::new(ClickhouseBackend::init("default".to_string(), vhost_config.clone()).await?),
        #[cfg(feature = "sqlite")]
        "sqlite" => Box::new(SQLiteBackend::init("default".to_string(), vhost_config.clone()).await?),
        t => panic!("unsupported database type: {}", t),
    };

    server = server.manage(backend);

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
#[path = "../tests/postgresql/mod.rs"]
mod postgresql;

#[cfg(feature = "sqlite")]
#[cfg(test)]
#[path = "../tests/sqlite/mod.rs"]
mod sqlite;

#[cfg(feature = "clickhouse")]
#[cfg(test)]
#[path = "../tests/clickhouse/mod.rs"]
mod clickhouse;
