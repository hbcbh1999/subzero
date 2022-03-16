use tokio_postgres::{types::ToSql, IsolationLevel};
use deadpool_postgres::Pool;

use crate::{
    api::{ApiRequest, ApiResponse, ContentType::*, },
    config::VhostConfig,
    dynamic_statement::generate,
    error::{Result, *},
    schema::DbSchema,
    dynamic_statement::{param, JoinIterator, SqlSnippet},
    formatter::postgresql::fmt_main_query,
};

use std::{
    collections::HashMap,
};
use serde_json::{Value as JsonValue};
use http::Method;
use snafu::ResultExt;
// use futures::future;

fn get_postgrest_env(role: Option<&String>, search_path: &Vec<String>, request: &ApiRequest, jwt_claims: &Option<JsonValue>, use_legacy_gucs: bool) -> HashMap<String, String> {
    let mut env = HashMap::new();
    if let Some(r) = role {
        env.insert("role".to_string(), r.clone());
        env.insert("request.jwt.claim.role".to_string(), r.clone());
    }
    
    env.insert("request.method".to_string(), format!("{}", request.method));
    env.insert("request.path".to_string(), format!("{}", request.path));
    //pathSql = setConfigLocal mempty ("request.path", iPath req)
    
    env.insert("search_path".to_string(), search_path.join(", ").to_string());
    if use_legacy_gucs {
        env.extend(
            request
                .headers
                .iter()
                .map(|(k, v)| (format!("request.header.{}", k.to_lowercase()), v.to_string())),
        );
        env.extend(request.cookies.iter().map(|(k, v)| (format!("request.cookie.{}", k), v.to_string())));
        match jwt_claims {
            Some(v) => match v.as_object() {
                Some(claims) => {
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

pub async fn execute<'a>(
    method: &Method, pool: &'a Pool, readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest, role: Option<&String>,
    jwt_claims: &Option<JsonValue>, config: &VhostConfig, _db_schema: &DbSchema
) -> Result<ApiResponse> {
    let mut client = pool.get().await.context(DbPoolError)?;

    
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, request)?);
    let env = get_postgrest_env(role, &vec![schema_name.clone()], request, jwt_claims, config.db_use_legacy_gucs);
    let (env_statement, env_parameters, _) = generate(get_postgrest_env_query(&env));

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .read_only(readonly)
        .start()
        .await
        .context(DbError { authenticated })?;

    //paralel
    // let (env_stm, main_stm) = future::try_join(
    //         transaction.prepare_cached(env_statement.as_str()),
    //         transaction.prepare_cached(main_statement.as_str())
    //     ).await.context(DbError { authenticated })?;
    
    // let (_, rows) = future::try_join(
    //     transaction.query(&env_stm, env_parameters.as_slice()),
    //     transaction.query(&main_stm, main_parameters.as_slice())
    // ).await.context(DbError { authenticated })?;

    
    let env_stm = transaction
        .prepare_cached(env_statement.as_str())
        .await
        .context(DbError { authenticated })?;
    let _ = transaction
        .query(&env_stm, env_parameters.as_slice())
        .await
        .context(DbError { authenticated })?;

    if let Some((s, f)) = &config.db_pre_request {
        let fn_schema = match s.as_str() {
            "" => schema_name,
            _ => &s,
        };

        let pre_request_statement = format!(r#"select "{}"."{}"()"#, fn_schema, f);
        let pre_request_stm = transaction
            .prepare_cached(pre_request_statement.as_str())
            .await
            .context(DbError { authenticated })?;
        transaction.query(&pre_request_stm, &[]).await.context(DbError { authenticated })?;
    }

    let main_stm = transaction
        .prepare_cached(main_statement.as_str())
        .await
        .context(DbError { authenticated })?;

    let rows = transaction
        .query(&main_stm, main_parameters.as_slice())
        .await
        .context(DbError { authenticated })?;

    
    let api_response = ApiResponse {
        page_total: rows[0].get("page_total"),
        total_result_set: rows[0].get("total_result_set"),
        top_level_offset: 0,
        response_headers: rows[0].get("response_headers"),
        response_status: rows[0].get("response_status"),
        body: rows[0].get("body"),
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        transaction.rollback().await.context(DbError { authenticated })?;
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
        transaction.rollback().await.context(DbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    if config.db_tx_rollback {
        transaction.rollback().await.context(DbError { authenticated })?;
    } else {
        transaction.commit().await.context(DbError { authenticated })?;
    }

    Ok(api_response)
}

