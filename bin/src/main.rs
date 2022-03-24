#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rocket;

use std::{sync::Arc};
use dashmap::DashMap;
use http::Method;
use snafu::OptionExt;
use figment::{
    providers::{Env, Format, Toml},
    Figment, Profile,
};
use rocket::{
    routes,
    http::{uri::Origin, CookieJar, Header, Status, ContentType as HTTPContentType},
    Build, Config as RocketConfig, Rocket, State,
};
use subzero::{
    config::Config,
    error::{GucStatusError, Error},
    frontend::postgrest,
    api::ContentType::{SingularJSON, TextCSV, ApplicationJSON},
};

mod rocket_util;
use rocket_util::{AllHeaders, ApiResponse, QueryString, RocketError};

mod vhosts;
use vhosts::{create_resources, get_resources, VhostResources};

type Resources = Arc<DashMap<String, VhostResources>>;
lazy_static! {
    static ref SINGLE_CONTENT_TYPE: HTTPContentType = HTTPContentType::parse_flexible("application/vnd.pgrst.object+json").unwrap();
}

async fn handle_request(
    method: &Method, table: &String, origin: &Origin<'_>, parameters: &QueryString<'_>, body: Option<String>, cookies: &CookieJar<'_>,
    headers: AllHeaders<'_>, vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    let vhost = headers.get_one("Host");
    let resources = get_resources(vhost, vhosts).map_err(|e| RocketError(e))?;
    let (status, content_type, headers, body) = postgrest::handle(
        table,
        method,
        origin.path().to_string(),
        parameters,
        body,
        headers.iter().map(|h| (h.name().as_str().to_string(), h.value().to_string())).collect(),
        cookies.iter().map(|c| (c.name().to_string(), c.value().to_string())).collect(),
        &resources.backend,
    )
    .await
    .map_err(|e| RocketError(e))?;

    let http_content_type = match content_type {
        SingularJSON => SINGLE_CONTENT_TYPE.clone(),
        TextCSV => HTTPContentType::CSV,
        ApplicationJSON => HTTPContentType::JSON,
    };

    Ok(ApiResponse {
        response: (
            Status::from_code(status).context(GucStatusError).map_err(|e| RocketError(e))?,
            (http_content_type, body),
        ),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

#[get("/")]
fn index() -> &'static str { "Hello, world!" }

#[get("/<table>?<parameters..>")]
async fn get<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString<'a>, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::GET, &table, origin, &parameters, None, cookies, headers, vhosts).await
}

#[post("/<table>?<parameters..>", data = "<body>")]
async fn post<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::POST, &table, origin, &parameters, Some(body), cookies, headers, vhosts).await
}

#[delete("/<table>?<parameters..>", data = "<body>")]
async fn delete<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::DELETE, &table, origin, &parameters, Some(body), cookies, headers, vhosts).await
}

#[patch("/<table>?<parameters..>", data = "<body>")]
async fn patch<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PATCH, &table, origin, &parameters, Some(body), cookies, headers, vhosts).await
}

#[put("/<table>?<parameters..>", data = "<body>")]
async fn put<'a>(
    table: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>,
    vhosts: &State<Resources>,
) -> Result<ApiResponse, RocketError> {
    handle_request(&Method::PUT, &table, origin, &parameters, Some(body), cookies, headers, vhosts).await
}

async fn start() -> Result<Rocket<Build>, Error> {
    #[cfg(debug_assertions)]
    let profile = RocketConfig::DEBUG_PROFILE;

    #[cfg(not(debug_assertions))]
    let profile = RocketConfig::RELEASE_PROFILE;

    let config = Figment::from(RocketConfig::default())
        .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
        .merge(Env::prefixed("SUBZERO_").split("__").ignore(&["PROFILE"]).global())
        .select(Profile::from_env_or("SUBZERO_PROFILE", profile));

    let app_config: Config = config.extract().expect("config");
    let vhost_resources = Arc::new(DashMap::new());
    println!("Found {} configured vhosts", app_config.vhosts.len());
    let mut server = rocket::custom(config).manage(vhost_resources.clone()).mount("/", routes![index]);

    for (vhost, vhost_config) in app_config.vhosts {
        let vhost_resources = vhost_resources.clone();
        match &vhost_config.url_prefix {
            Some(p) => {
                server = server
                    .mount(p, routes![get, post, delete, patch, put])
                    .mount(format!("{}/rpc", p), routes![get, post]);
            }
            None => {
                server = server
                    .mount("/", routes![get, post, delete, patch, put])
                    .mount("/rpc", routes![get, post]);
            }
        }
        //tokio::spawn(async move {
        //sleep(Duration::from_millis(30 * 1000)).await;
        match create_resources(&vhost, vhost_config, vhost_resources).await {
            Ok(_) => println!("[{}] loaded config", vhost),
            Err(e) => println!("[{}] config load failed ({})", vhost, e),
        }
        //});
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
