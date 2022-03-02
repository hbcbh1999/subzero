// use std::collections::HashMap;

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params_from_iter;
use crate::{
    formatter::sqlite::{fmt_main_query, return_representation},
    api::{
        Condition, Field, SelectItem, Filter, ListVal,
        ApiRequest, ApiResponse, ContentType::*, Query, QueryNode::*, Preferences, Count
    },
    config::VhostConfig,
    dynamic_statement::generate,
    error::{Result, *},
    schema::DbSchema,
};
use serde_json::{Value as JsonValue, json};
use http::Method;
use snafu::ResultExt;
use log::{debug};

pub fn execute(
    method: &Method, pool: &Pool<SqliteConnectionManager>, _readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest<'_>,
    _role: &String, _jwt_claims: &Option<JsonValue>, config: &VhostConfig, _db_schema: &DbSchema
) -> Result<ApiResponse> {
    let conn = pool.get().unwrap();

    conn.execute_batch("BEGIN DEFERRED").context(DbError { authenticated })?;
    //let transaction = conn.transaction().context(DbError { authenticated })?;
    let primary_key_column = "rowid"; //every table has this (TODO!!! check)
    let return_representation = return_representation(request);
    let (second_stage_select, first_stage_reponse) = match request {
        ApiRequest {query: Query { node: node@Insert {into:table,where_,select,..}, sub_selects },..} |
        ApiRequest {query: Query { node: node@Update {table,where_,select,..}, sub_selects },..} |
        ApiRequest {query: Query { node: node@Delete {from:table,where_,select,..}, sub_selects },..} => {
            //sqlite does not support returining in CTEs so we must do a two step process
            
            let primary_key_field = Field {name: primary_key_column.to_string(), json_path: None};
            
            // here we eliminate the sub_selects and also select back
            let mut mutate_request = request.clone();
            match &mut mutate_request {
                ApiRequest { query: Query { sub_selects, node: Insert {returning, select, ..}}, ..} |
                ApiRequest { query: Query { sub_selects, node: Delete {returning, select, ..}}, ..} |
                ApiRequest { query: Query { sub_selects, node: Update {returning, select, ..}}, ..} => {
                    returning.clear();
                    returning.push(primary_key_column.to_string());
                    select.clear();
                    select.push(SelectItem::Simple {field: primary_key_field.clone(), alias: None,cast: None});
                    sub_selects.clear();
                }
                _ => {}
            }
            
            let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, &mutate_request)?);
            debug!("pre_statement: {}", main_statement);
            let mut mutate_stmt = conn.prepare(main_statement.as_str()).context(DbError { authenticated })?;
            let mut rows = mutate_stmt.query(params_from_iter(main_parameters.iter())).context(DbError { authenticated })?;
            let mut ids:Vec<i64> = vec![];
            while let Some(r) = rows.next().context(DbError { authenticated })? {
                ids.push(r.get(0).context(DbError { authenticated })?)
            }
            let mut select_request = request.clone();
            let mut select_where = where_.to_owned();
            select_where.conditions.insert(0, Condition::Single {field: primary_key_field, filter: Filter::In(ListVal(ids.iter().map(|i| i.to_string()).collect())), negate: false});
            select_request.method = Method::GET;
            select_request.query = Query {
                node: Select {
                    from: (table.to_owned(), None),
                    join_tables: vec![],
                    where_: select_where,
                    select: select.iter().cloned().collect(),
                    limit:None,
                    offset:None,
                    order:vec![],
                },
                sub_selects: sub_selects.iter().cloned().collect()
            };

            let response  = match node {
                Delete { .. } => {
                    let count = match &request.preferences {
                        Some(Preferences {
                            count: Some(Count::ExactCount),
                            ..
                        }) => true,
                        _ => false,
                    };
                    
                    Some(ApiResponse {
                        page_total: ids.len() as i64,
                        total_result_set: if count { Some(ids.len() as i64) } else {None},
                        top_level_offset: 0,
                        body: if return_representation {
                            serde_json::to_string(&ids.iter().map(|i| 
                                json!({primary_key_column:i})
                            ).collect::<Vec<_>>()).context(JsonSerialize)? 
                        } else {"".to_string()},
                        response_headers: None,
                        response_status: None
                    })
                },
                _ => None
            };

            Ok((Some(select_request),response))
        },
        _ => {
            Ok((None,None))
        }
    }.map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})?;

    if let Some(r) = first_stage_reponse { // this is a special case for delete
        if config.db_tx_rollback {
            conn.execute_batch("ROLLBACK").context(DbError { authenticated })?;
        } else {
            conn.execute_batch("COMMIT").context(DbError { authenticated })?;
        }
        return Ok(r)
    }

    let final_request = match &second_stage_select {
        Some(r) => r,
        None => request
    };
    
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, final_request).map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})?);
    debug!("main_statement: {}", main_statement);
    let mut main_stm = conn
        .prepare_cached(main_statement.as_str())
        .map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})
        .context(DbError { authenticated })?;

    let mut rows = main_stm
        .query(params_from_iter(main_parameters.iter()))
        .map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})
        .context(DbError { authenticated })?;

    let main_row = rows.next().context(DbError { authenticated }).map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})?.unwrap();
    
    let api_response = {
        Ok(ApiResponse {
            page_total: main_row.get("page_total").context(DbError { authenticated })?,       //("page_total"),
            total_result_set: main_row.get("total_result_set").context(DbError { authenticated })?, //("total_result_set"),
            top_level_offset: 0,
            body: if return_representation {main_row.get("body").context(DbError { authenticated })?} else {"".to_string()},             //("body"),
            response_headers: main_row.get("response_headers").context(DbError { authenticated })?, //("response_headers"),
            response_status: main_row.get("response_status").context(DbError { authenticated })?,  //("response_status"),
        })
    }.map_err(|e| { let _ = conn.execute_batch("ROLLBACK"); e})?;

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        conn.execute_batch("ROLLBACK").context(DbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    if method == &Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        conn.execute_batch("ROLLBACK").context(DbError { authenticated })?;
        return Err(Error::PutMatchingPkError);
    }

    if config.db_tx_rollback {
        conn.execute_batch("ROLLBACK").context(DbError { authenticated })?;
    } else {
        conn.execute_batch("COMMIT").context(DbError { authenticated })?;
    }

    Ok(api_response)
}
