use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params_from_iter;
use crate::{
    formatter::sqlite::{fmt_main_query, return_representation},
    api::{
        Condition, Field, SelectItem, Filter, ListVal,
        ApiRequest, ApiResponse, ContentType::*, Query, QueryNode::*, 
    },
    config::VhostConfig,
    dynamic_statement::generate,
    error::{Result, *},
    schema::DbSchema,
};
use serde_json::{Value as JsonValue};
use http::Method;
use snafu::ResultExt;

pub fn execute(
    method: &Method, pool: &Pool<SqliteConnectionManager>, _readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest<'_>,
    _role: &String, _jwt_claims: &Option<JsonValue>, config: &VhostConfig, _db_schema: &DbSchema
) -> Result<ApiResponse> {
    let conn = pool.get().unwrap();

    conn.execute_batch("BEGIN DEFERRED").context(DbError { authenticated })?;
    //let transaction = conn.transaction().context(DbError { authenticated })?;

    let second_stage_select = match request {
        ApiRequest {query: Query { node: Insert {into:table,where_,select,..}, sub_selects },..} |
        ApiRequest {query: Query { node: Update {table,where_,select,..}, sub_selects },..} => {
            //sqlite does not support returining in CTEs so we must do a two step process
            let primary_key_column = "rowid"; //evey table has this (TODO!!! check)
            let primary_key_field = Field {name: primary_key_column.to_string(), json_path: None};
            
            // here we eliminate the sub_selects and also select back
            let mut insert_request = request.clone();
            match &mut insert_request {
                ApiRequest { query: Query { sub_selects, node: Insert {returning, select, ..}}, ..} |
                ApiRequest { query: Query { sub_selects, node: Update {returning, select, ..}}, ..} => {
                    returning.clear();
                    returning.push(primary_key_column.to_string());
                    select.clear();
                    select.push(SelectItem::Simple {field: primary_key_field.clone(), alias: None,cast: None});
                    sub_selects.clear();
                }
                _ => {}
            }
            
            let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, &insert_request)?);
            println!("main_insert_statement: {} \n{}", main_parameters.len(), main_statement);
            let mut insert_stmt = conn.prepare(main_statement.as_str())
                .map_err(|e| {
                    let _ = conn.execute_batch("ROLLBACK");
                    e
                })
                .context(DbError { authenticated })?;
            let mut rows = insert_stmt
                .query(params_from_iter(main_parameters.iter()))
                .map_err(|e| {
                    let _ = conn.execute_batch("ROLLBACK");
                    e
                })
                .context(DbError { authenticated })?;
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
            Some(select_request)
        },
        _ => {
            None
        }
    };

    let final_request = match &second_stage_select {
        Some(r) => r,
        None => request
    };
    
    let (main_statement, main_parameters, _) = generate(fmt_main_query(schema_name, final_request)?);
    println!("main_statement: {} \n{}", main_parameters.len(), main_statement);
    // for p in params_from_iter(main_parameters.iter()) {
    //     println!("p {:?}", p.to_sql());
    // }
    // for p in main_parameters.iter() {
    //     println!("p {:?}", p.to_sql());
    // }
    let mut main_stm = conn
        .prepare_cached(main_statement.as_str())
        .map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            e
        })
        .context(DbError { authenticated })?;

    let mut rows = main_stm
        .query(params_from_iter(main_parameters.iter()))
        .map_err(|e| {
            let _ = conn.execute_batch("ROLLBACK");
            e
        })
        .context(DbError { authenticated })?;

    let main_row = rows.next().context(DbError { authenticated })?.unwrap();
    let return_representation = return_representation(request);
    let api_response = ApiResponse {
        page_total: main_row.get("page_total").context(DbError { authenticated })?,       //("page_total"),
        total_result_set: main_row.get("total_result_set").context(DbError { authenticated })?, //("total_result_set"),
        top_level_offset: 0,
        body: if return_representation {main_row.get("body").context(DbError { authenticated })?} else {"".to_string()},             //("body"),
        response_headers: main_row.get("response_headers").context(DbError { authenticated })?, //("response_headers"),
        response_status: main_row.get("response_status").context(DbError { authenticated })?,  //("response_status"),
    };

    println!("{:?} {:?}", return_representation, api_response);

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        conn.execute_batch("ROLLBACK").context(DbError { authenticated })?;
        return Err(Error::SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        });
    }

    //println!("before check {:?} {:?}", method, page_total);

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
