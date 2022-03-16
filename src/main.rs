#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate rocket;
use dashmap::DashMap;
use http::Method;

use rocket::{
    routes,
    http::{uri::Origin, CookieJar, Header, Status},
    Build, Config as RocketConfig, Rocket, State,
};
use snafu::OptionExt;

use subzero::{
    config::Config,
    error::{GucStatusError, Result},
    frontend::postgrest::handle as handle_request
};

mod rocket_util;
use rocket_util::{cookies_as_hashmap, headers_as_hashmap, to_rocket_content_type, AllHeaders, ApiResponse, QueryString, Vhost};

mod vhosts;
use vhosts::{create_resources, get_resources, VhostResources};

use figment::{
    providers::{Env, Format, Toml},
    Figment, Profile,
};

use std::{sync::Arc};

#[get("/")]
fn index() -> &'static str { "Hello, world!" }

#[get("/<root>?<parameters..>")]
async fn get<'a>(
    root: String, origin: &Origin<'_>, parameters: QueryString<'a>, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhost: Vhost<'a>,
    vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let (status, content_type, headers, body) = handle_request(
        &resources.config,
        &root,
        &Method::GET,
        origin.path().to_string(),
        &parameters,
        &resources.db_schema,
        &resources.db_pool,
        None,
        headers_as_hashmap(&headers),
        cookies_as_hashmap(cookies),
    )
    .await?;

    Ok(ApiResponse {
        response: (Status::from_code(status).context(GucStatusError)?, (to_rocket_content_type(content_type), body)),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

#[post("/<root>?<parameters..>", data = "<body>")]
async fn post<'a>(
    root: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhost: Vhost<'a>,
    vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let (status, content_type, headers, body) = handle_request(
        &resources.config,
        &root,
        &Method::POST,
        origin.path().to_string(),
        &parameters,
        &resources.db_schema,
        &resources.db_pool,
        Some(body),
        headers_as_hashmap(&headers),
        cookies_as_hashmap(cookies),
    )
    .await?;

    Ok(ApiResponse {
        response: (Status::from_code(status).context(GucStatusError)?, (to_rocket_content_type(content_type), body)),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

#[delete("/<root>?<parameters..>", data = "<body>")]
async fn delete<'a>(
    root: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhost: Vhost<'a>,
    vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let (status, content_type, headers, body) = handle_request(
        &resources.config,
        &root,
        &Method::DELETE,
        origin.path().to_string(),
        &parameters,
        &resources.db_schema,
        &resources.db_pool,
        Some(body),
        headers_as_hashmap(&headers),
        cookies_as_hashmap(cookies),
    )
    .await?;

    Ok(ApiResponse {
        response: (Status::from_code(status).context(GucStatusError)?, (to_rocket_content_type(content_type), body)),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

#[patch("/<root>?<parameters..>", data = "<body>")]
async fn patch<'a>(
    root: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhost: Vhost<'a>,
    vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let (status, content_type, headers, body) = handle_request(
        &resources.config,
        &root,
        &Method::PATCH,
        origin.path().to_string(),
        &parameters,
        &resources.db_schema,
        &resources.db_pool,
        Some(body),
        headers_as_hashmap(&headers),
        cookies_as_hashmap(cookies),
    )
    .await?;

    Ok(ApiResponse {
        response: (Status::from_code(status).context(GucStatusError)?, (to_rocket_content_type(content_type), body)),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

#[put("/<root>?<parameters..>", data = "<body>")]
async fn put<'a>(
    root: String, origin: &Origin<'_>, parameters: QueryString<'a>, body: String, cookies: &CookieJar<'a>, headers: AllHeaders<'a>, vhost: Vhost<'a>,
    vhosts: &State<Arc<DashMap<String, VhostResources>>>,
) -> Result<ApiResponse> {
    let resources = get_resources(&vhost, vhosts)?;
    let (status, content_type, headers, body) = handle_request(
        &resources.config,
        &root,
        &Method::PUT,
        origin.path().to_string(),
        &parameters,
        &resources.db_schema,
        &resources.db_pool,
        Some(body),
        headers_as_hashmap(&headers),
        cookies_as_hashmap(cookies),
    )
    .await?;

    Ok(ApiResponse {
        response: (Status::from_code(status).context(GucStatusError)?, (to_rocket_content_type(content_type), body)),
        headers: headers.into_iter().map(|(n, v)| Header::new(n, v)).collect::<Vec<_>>(),
    })
}

async fn start() -> Result<Rocket<Build>> {
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
    let mut server = rocket::custom(config)
        .manage(vhost_resources.clone())
        .mount("/", routes![index]);

    for (vhost, vhost_config) in app_config.vhosts {
        let vhost_resources = vhost_resources.clone();
        match &vhost_config.url_prefix {
            Some(p) => {
                server = server
                .mount(p, routes![get, post, delete, patch, put])
                .mount(format!("{}/rpc",p), routes![get, post]);
            },
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
