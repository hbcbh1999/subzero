use http::Method;
use jsonpath_lib::select;
use jsonwebtoken::{decode, errors::ErrorKind, DecodingKey, Validation};
use serde_json::{from_value, Value as JsonValue};
use snafu::ResultExt;

#[cfg(feature = "sqlite")]
use tokio::task;

use subzero_core::api::{ApiResponse};

use crate::backend::Backend;

use subzero_core::{
    api::{ContentType, ContentType::*, Preferences, QueryNode::*, Representation, Resolution::*, ApiRequest},
    error::{*},
    parser::postgrest::parse,
    permissions::{check_safe_functions, check_privileges, insert_policy_conditions},
};

use crate::error::{Result, CoreSnafu, to_core_error};

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

fn get_current_timestamp() -> u64 {
    //TODO!!! optimize this to run once per second
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs()
}

// fn validate_fn_param(config: &VhostConfig, p: &FunctionParam) -> Result<()> {
//     match p {
//         FunctionParam::Func { fn_name, parameters } => {
//             if !config.db_allowed_select_functions.contains(&fn_name) {
//                 return Err(to_core_error(Error::ParseRequestError {
//                     details: format!("calling: '{}' is not allowed", fn_name),
//                     message: "Unsafe functions called".to_string(),
//                 }));
//             }
//             for p in parameters {
//                 validate_fn_param(config, p)?;
//             }
//             Ok(())
//         },
//         _ => {Ok(())}
//     }
// }

fn get_env(role: Option<&String>, request: &ApiRequest, jwt_claims: &Option<JsonValue>, use_legacy_gucs: bool) -> HashMap<String, String> {
    let mut env = HashMap::new();
    let search_path = &[String::from(request.schema_name)];
    if let Some(r) = role {
        env.insert("role".to_string(), r.clone());
    }

    env.insert("request.method".to_string(), request.method.to_string());
    env.insert("request.path".to_string(), request.path.to_string());
    //pathSql = setConfigLocal mempty ("request.path", iPath req)

    env.insert("search_path".to_string(), search_path.join(", "));
    if use_legacy_gucs {
        if let Some(r) = role {
            env.insert("request.jwt.claim.role".to_string(), r.clone());
        }

        env.extend(
            request
                .headers
                .iter()
                .map(|(k, v)| (format!("request.header.{}", k.to_lowercase()), v.to_string())),
        );
        env.extend(request.cookies.iter().map(|(k, v)| (format!("request.cookie.{}", k), v.to_string())));
        env.extend(request.get.iter().map(|(k, v)| (format!("request.get.{}", k), v.to_string())));
        match jwt_claims {
            Some(v) => {
                if let Some(claims) = v.as_object() {
                    env.extend(claims.iter().map(|(k, v)| {
                        (
                            format!("request.jwt.claim.{}", k),
                            match v {
                                JsonValue::String(s) => s.clone(),
                                _ => format!("{}", v),
                            },
                        )
                    }));
                }
            }
            None => {}
        }
    } else {
        env.insert(
            "request.headers".to_string(),
            serde_json::to_string(&request.headers.iter().map(|(k, v)| (k.to_lowercase(), v.to_string())).collect::<Vec<_>>()).unwrap(),
        );
        env.insert(
            "request.cookies".to_string(),
            serde_json::to_string(&request.cookies.iter().map(|(k, v)| (k, v.to_string())).collect::<Vec<_>>()).unwrap(),
        );
        env.insert(
            "request.get".to_string(),
            serde_json::to_string(&request.get.iter().map(|(k, v)| (k, v.to_string())).collect::<Vec<_>>()).unwrap(),
        );
        match jwt_claims {
            Some(v) => {
                if let Some(claims) = v.as_object() {
                    env.insert("request.jwt.claims".to_string(), serde_json::to_string(&claims).unwrap());
                }
            }
            None => {}
        }
    }

    env
}

#[allow(clippy::borrowed_box)]
#[allow(clippy::too_many_arguments)]
pub async fn handle<'a>(
    root: &'a str, method: &Method, path: &'a str, get: Vec<(&'a str, &'a str)>, body: Option<&'a str>, headers: HashMap<&'a str, &'a str>,
    cookies: HashMap<&'a str, &'a str>, backend: &Box<dyn Backend + Send + Sync>,
) -> Result<(u16, ContentType, Vec<(String, String)>, String)> {
    #![allow(unused_variables)]
    #![allow(unreachable_code)]
    let mut response_headers = vec![];
    let config = backend.config();
    let db_schema = backend.db_schema();
    let schema_name = &(match (config.db_schemas.len() > 1, method, headers.get("accept-profile"), headers.get("content-profile")) {
        (false, ..) => Ok(config.db_schemas.get(0).unwrap().clone()),
        (_, &Method::DELETE, _, Some(content_profile))
        | (_, &Method::POST, _, Some(content_profile))
        | (_, &Method::PATCH, _, Some(content_profile))
        | (_, &Method::PUT, _, Some(content_profile)) => {
            if config.db_schemas.contains(&String::from(*content_profile)) {
                Ok(content_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        (_, _, Some(accept_profile), _) => {
            if config.db_schemas.contains(&String::from(*accept_profile)) {
                Ok(accept_profile.to_string())
            } else {
                Err(Error::UnacceptableSchema {
                    schemas: config.db_schemas.clone(),
                })
            }
        }
        _ => Ok(config.db_schemas.get(0).unwrap().clone()),
    }
    .context(CoreSnafu)?);

    if config.db_schemas.len() > 1 {
        response_headers.push(("Content-Profile".to_string(), schema_name.clone()));
    }

    // check jwt
    let jwt_claims = match &config.jwt_secret {
        Some(key) => match headers.get("authorization") {
            Some(a) => {
                let token_str: Vec<&str> = a.split(' ').collect();
                match token_str[..] {
                    ["Bearer", t] | ["bearer", t] => {
                        let validation = Validation {
                            validate_exp: false,
                            ..Default::default()
                        };
                        match decode::<JsonValue>(t, &DecodingKey::from_secret(key.as_bytes()), &validation) {
                            Ok(c) => {
                                if let Some(exp) = c.claims.get("exp") {
                                    if from_value::<u64>(exp.clone()).context(JsonSerializeSnafu).context(CoreSnafu)? < get_current_timestamp() - 1 {
                                        return Err(to_core_error(Error::JwtTokenInvalid {
                                            message: "JWT expired".to_string(),
                                        }));
                                    }
                                }
                                Ok(Some(c.claims))
                            }
                            Err(err) => match *err.kind() {
                                ErrorKind::InvalidToken => Err(Error::JwtTokenInvalid { message: format!("{}", err) }),
                                _ => Err(Error::JwtTokenInvalid { message: format!("{}", err) }),
                            },
                        }
                    }
                    _ => Ok(None),
                }
            }
            None => Ok(None),
        },
        None => Ok(None),
    }
    .context(CoreSnafu)?;

    let (role, authenticated) = match &jwt_claims {
        Some(claims) => match select(claims, format!("${}", config.role_claim_key).as_str()) {
            Ok(v) => match &v[..] {
                [JsonValue::String(s)] => Ok((Some(s), true)),
                _ => Ok((config.db_anon_role.as_ref(), true)),
            },
            Err(e) => Err(Error::JwtTokenInvalid { message: format!("{}", e) }),
        },
        None => Ok((config.db_anon_role.as_ref(), false)),
    }
    .context(CoreSnafu)?;

    debug!("role: {:?}, jwt_claims: {:?}", role, jwt_claims);

    // do not allow unauthenticated requests when there is no anonymous role setup
    if let (None, false) = (role, authenticated) {
        return Err(to_core_error(Error::JwtTokenInvalid {
            message: "unauthenticated requests not allowed".to_string(),
        }));
    }

    // parse request and generate the query
    let mut request =
        parse(schema_name, root, db_schema, method.as_str(), path, get, body, headers, cookies, config.db_max_rows).context(CoreSnafu)?;
    // in case when the role is not set (but authenticated through jwt) the query will be executed with the privileges
    // of the "authenticator" role unless the DbSchema has internal privileges set
    check_privileges(db_schema, schema_name, role.unwrap_or(&String::default()), &request).map_err(to_core_error)?;
    check_safe_functions(&request, &config.db_allowed_select_functions).map_err(to_core_error)?;
    insert_policy_conditions(db_schema, schema_name, role.unwrap_or(&String::default()), &mut request.query).map_err(to_core_error)?;

    // when using internal privileges not switch "current_role"
    let env_role = if db_schema.use_internal_permissions { None } else { role };

    let _env = get_env(env_role, &request, &jwt_claims, config.db_use_legacy_gucs);
    let env = _env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<HashMap<_, _>>();

    let response: ApiResponse = match config.db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => backend.execute(authenticated, &request, &env).await?,

        #[cfg(feature = "clickhouse")]
        "clickhouse" => backend.execute(authenticated, &request, &env).await?,

        #[cfg(feature = "sqlite")]
        "sqlite" => task::block_in_place(|| backend.execute(authenticated, &request, &env)).await?,

        t => panic!("unsuported database type: {}", t),
    };

    // create and return the response to the client
    let page_total = response.page_total;
    let total_result_set = response.total_result_set;
    let top_level_offset = response.top_level_offset;
    let content_type = match (&request.accept_content_type, &request.query.node) {
        (SingularJSON, _)
        | (
            _,
            FunctionCall {
                returns_single: true,
                is_scalar: false,
                ..
            },
        ) => SingularJSON,
        (TextCSV, _) => TextCSV,
        _ => ApplicationJSON,
    };

    let content_range = match (method, &request.query.node, page_total, total_result_set) {
        (&Method::POST, Insert { .. }, _pt, t) => content_range_header(1, 0, t),
        (&Method::DELETE, Delete { .. }, pt, t) => content_range_header(1, top_level_offset + pt - 1, t),
        (_, _, pt, t) => content_range_header(top_level_offset, top_level_offset + pt - 1, t),
    };

    response_headers.push(("Content-Range".to_string(), content_range));
    if let Some(response_headers_str) = response.response_headers {
        match serde_json::from_str(response_headers_str.as_str()) {
            Ok(JsonValue::Array(headers_json)) => {
                for h in headers_json {
                    match h {
                        JsonValue::Object(o) => {
                            for (k, v) in o.into_iter() {
                                match v {
                                    JsonValue::String(s) => {
                                        response_headers.push((k, s));
                                        Ok(())
                                    }
                                    _ => Err(Error::GucHeadersError),
                                }
                                .context(CoreSnafu)?
                            }
                            Ok(())
                        }
                        _ => Err(Error::GucHeadersError),
                    }
                    .context(CoreSnafu)?
                }
                Ok(())
            }
            _ => Err(Error::GucHeadersError),
        }
        .context(CoreSnafu)?
    }

    #[rustfmt::skip]
    let mut status = match (method, &request.query.node, page_total, total_result_set, &request.preferences) {
        (&Method::POST, Insert { .. }, ..) => 201,
        (&Method::DELETE, Delete { .. }, ..,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::DELETE, Delete { .. }, ..) => 204,
        (&Method::PATCH, Update { columns, .. }, 0, _, _) if !columns.is_empty() => 404,
        (&Method::PATCH, Update { .. }, ..,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PATCH, Update { .. }, ..) => 204,
        (&Method::PUT,Insert { .. },..,Some(Preferences {representation: Some(Representation::Full),..}),) => 200,
        (&Method::PUT, Insert { .. }, ..) => 204,
        (.., pt, t, _) => content_range_status(top_level_offset, top_level_offset + pt - 1, t),
    };

    if let Some(Preferences { resolution: Some(r), .. }) = request.preferences {
        response_headers.push((
            "Preference-Applied".to_string(),
            match r {
                MergeDuplicates => "resolution=merge-duplicates".to_string(),
                IgnoreDuplicates => "resolution=ignore-duplicates".to_string(),
            },
        ));
    }

    let response_status: Option<String> = response.response_status;
    if let Some(response_status_str) = response_status {
        status = response_status_str.parse::<u16>().map_err(|_| Error::GucStatusError).context(CoreSnafu)?;
    }

    Ok((status, content_type, response_headers, response.body))
}

fn content_range_header(lower: i64, upper: i64, total: Option<i64>) -> String {
    let range_string = if total != Some(0) && lower <= upper {
        format!("{}-{}", lower, upper)
    } else {
        "*".to_string()
    };
    let total_string = match total {
        Some(t) => format!("{}", t),
        None => "*".to_string(),
    };
    format!("{}/{}", range_string, total_string)
}

fn content_range_status(lower: i64, upper: i64, total: Option<i64>) -> u16 {
    match (lower, upper, total) {
        //(_, _, None) => 200,
        (l, _, Some(t)) if l > t => 406,
        (l, u, Some(t)) if (1 + u - l) < t => 206,
        _ => 200,
    }
}
