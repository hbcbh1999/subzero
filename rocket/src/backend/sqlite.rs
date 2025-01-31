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
// use std::collections::HashMap;

use r2d2::Pool;
use r2d2::CustomizeConnection;
use std::borrow::Cow;
use r2d2_sqlite::SqliteConnectionManager;
use rocket::log::private::debug;
use crate::config::{VhostConfig, SchemaStructure::*};
use subzero_core::{
    formatter::{
        Param,
        Param::*,
        sqlite::{fmt_main_query, generate, return_representation},
        ToParam,
    },
    api::{
        Condition, Field, SelectItem, Filter, ListVal, SingleVal, Payload, ApiRequest, ApiResponse, ContentType::*, Query, QueryNode::*, Preferences,
        Count,
    },
    error::{JsonSerializeSnafu, JsonDeserializeSnafu},
    error::Error::{SingularityError, PutMatchingPkError, PermissionDenied},
    schema::DbSchema,
};
//use rocket::log::private::debug;
use crate::error::{Result, *};
use std::path::Path;
use serde_json::{json, Value};
use http::Method;
use snafu::ResultExt;
use async_trait::async_trait;
use rusqlite::vtab::array;

use std::collections::HashMap;
use std::fs;
use super::{Backend, include_files, DbSchemaWrap};
use tokio::task;
use rusqlite::{
    Connection,
    params_from_iter,
    types::{ToSqlOutput, Value::*, ValueRef},
    //types::Value,
    Result as SqliteResult,
    ToSql,
    functions::FunctionFlags,
};
//use std::rc::Rc;

#[derive(Debug)]
struct WrapParam<'a>(Param<'a>);
impl ToSql for WrapParam<'_> {
    fn to_sql(&self) -> SqliteResult<ToSqlOutput<'_>> {
        match self {
            //WrapParam(LV(ListVal(v, ..))) => Ok(ToSqlOutput::Array(Rc::new(v.iter().map(|v| Value::from(v.clone())).collect()))),
            WrapParam(LV(ListVal(v, ..))) => Ok(ToSqlOutput::Owned(Text(serde_json::to_string(v).unwrap_or_default()))),
            WrapParam(SV(SingleVal(v, ..))) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(v.as_bytes()))),
            WrapParam(Str(v)) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(v.as_bytes()))),
            WrapParam(StrOwned(v)) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(v.as_bytes()))),
            WrapParam(PL(Payload(v, ..))) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(v.as_bytes()))),
        }
    }
}

fn wrap_param(p: &'_ (dyn ToParam + Sync)) -> WrapParam<'_> {
    WrapParam(p.to_param())
}

//generate_fn!();

//TODO: refactor transaction rollback
fn execute(
    db_schema: &DbSchema<'_>, pool: &Pool<SqliteConnectionManager>, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>,
    config: &VhostConfig,
) -> Result<ApiResponse> {
    let conn = pool.get().unwrap();

    conn.execute_batch("BEGIN DEFERRED").context(SqliteDbSnafu { authenticated })?;
    //let transaction = conn.transaction().context(SqliteDb { authenticated })?;
    let return_representation = return_representation(request.method, &request.query, &request.preferences);

    let api_response = match request {
        ApiRequest {
            query:
                Query {
                    node: node @ Insert {
                        into: table, where_, select, ..
                    },
                    sub_selects,
                },
            ..
        }
        | ApiRequest {
            query: Query {
                node: node @ Update { table, where_, select, .. },
                sub_selects,
            },
            ..
        }
        | ApiRequest {
            query:
                Query {
                    node: node @ Delete {
                        from: table, where_, select, ..
                    },
                    sub_selects,
                },
            ..
        } => {
            //sqlite does not support returining in CTEs so we must do a two step process
            let schema_obj = db_schema.get_object(request.schema_name, table).context(CoreSnafu)?;
            let primary_key_column = schema_obj
                .columns
                .iter()
                .find(|&(_, c)| c.primary_key)
                .map(|(_, c)| c.name)
                .unwrap_or("rowid");
            //let primary_key_column = "rowid"; //every table has this (TODO!!! check)
            let primary_key_field = Field {
                name: primary_key_column,
                json_path: None,
            };
            let is_delete = matches!(node, Delete { .. });
            // here we eliminate the sub_selects and also select back
            let mut mutate_request = request.clone();
            match &mut mutate_request {
                ApiRequest {
                    query:
                        Query {
                            sub_selects,
                            node: Insert { returning, select, .. },
                        },
                    ..
                }
                | ApiRequest {
                    query:
                        Query {
                            sub_selects,
                            node: Delete { returning, select, .. },
                        },
                    ..
                }
                | ApiRequest {
                    query:
                        Query {
                            sub_selects,
                            node: Update { returning, select, .. },
                        },
                    ..
                } => {
                    //return only the primary key column
                    returning.clear();
                    returning.push(primary_key_column);
                    select.clear();
                    select.push(SelectItem::Simple {
                        field: primary_key_field.clone(),
                        alias: Some(primary_key_column),
                        cast: None,
                    });

                    if !is_delete {
                        select.push(SelectItem::Simple {
                            field: Field {
                                name: "_subzero_check__constraint",
                                json_path: None,
                            },
                            alias: None,
                            cast: None,
                        });
                    }
                    // no need for aditional data from joined tables
                    sub_selects.clear();
                }
                _ => {}
            }
            //debug!("mutated request query: {:?}", mutate_request.query);
            let env1 = env.clone();
            let (mutate_statement, mutate_parameters, _) = generate(
                fmt_main_query(db_schema, request.schema_name, &mutate_request, &env1)
                    .context(CoreSnafu)
                    .inspect_err(|_| {
                        let _ = conn.execute_batch("ROLLBACK");
                    })?,
            );
            debug!("mutate_statement: {}\n{:?}", mutate_statement, mutate_parameters);
            let mut mutate_stmt = conn
                .prepare(mutate_statement.as_str())
                .context(SqliteDbSnafu { authenticated })
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })?;
            let mutate_params = params_from_iter(mutate_parameters.into_iter().map(wrap_param));
            let mut rows = mutate_stmt
                .query(mutate_params)
                .context(SqliteDbSnafu { authenticated })
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })?;
            let mut ids: Vec<(i64, bool)> = vec![];
            while let Some(r) = rows.next().context(SqliteDbSnafu { authenticated }).inspect_err(|_| {
                let _ = conn.execute_batch("ROLLBACK");
            })? {
                ids.push((
                    r.get(0).context(SqliteDbSnafu { authenticated }).inspect_err(|_| {
                        let _ = conn.execute_batch("ROLLBACK");
                    })?, //rowid
                    if is_delete {
                        true
                    } else {
                        r.get(1).context(SqliteDbSnafu { authenticated }).inspect_err(|_| {
                            let _ = conn.execute_batch("ROLLBACK");
                        })?
                    }, //constraint check
                ))
            }
            debug!("ids: {:?}", ids);

            // check if all rows pased the permission check
            if ids.iter().any(|(_, p)| !p) {
                _ = conn.execute_batch("ROLLBACK");
                return Err(to_core_error(PermissionDenied {
                    details: "check constraint of an insert/update permission has failed".to_string(),
                }));
            }

            // in case of delete se can not do  "select back" so we jut return the deleted ids
            if is_delete {
                let count = matches!(
                    &request.preferences,
                    Some(Preferences {
                        count: Some(Count::ExactCount),
                        ..
                    })
                );
                if config.db_tx_rollback {
                    conn.execute_batch("ROLLBACK").context(SqliteDbSnafu { authenticated })?;
                } else {
                    conn.execute_batch("COMMIT").context(SqliteDbSnafu { authenticated })?;
                }
                return Ok(ApiResponse {
                    page_total: ids.len() as u64,
                    total_result_set: if count { Some(ids.len() as u64) } else { None },
                    top_level_offset: 0,
                    body: if return_representation {
                        serde_json::to_string(&ids.iter().map(|(i, _)| json!({ primary_key_column: i })).collect::<Vec<_>>())
                            .context(JsonSerializeSnafu)
                            .context(CoreSnafu)?
                    } else {
                        "".to_string()
                    },
                    response_headers: None,
                    response_status: None,
                });
            };

            // create the second stage select
            let mut select_request = request.clone();
            let mut select_where = where_.to_owned();
            // add the primary key condition to the where clause
            select_where.conditions.insert(
                0,
                Condition::Single {
                    field: primary_key_field,
                    filter: Filter::In(ListVal(ids.iter().map(|(i, _)| Cow::Owned(i.to_string())).collect(), None)),
                    negate: false,
                },
            );
            select_request.method = "GET";

            // set the request query to be a select
            select_request.query = Query {
                node: Select {
                    check: None,
                    from: (table.to_owned(), Some("subzero_source")),
                    join_tables: vec![], //todo!! this should probably not be empty
                    where_: select_where,
                    select: if select.is_empty() {
                        vec![SelectItem::Simple {
                            field: Field { name: "id", json_path: None },
                            alias: None,
                            cast: None,
                        }]
                    } else {
                        select.to_vec()
                    },
                    limit: None,
                    offset: None,
                    order: vec![],
                    groupby: vec![],
                },
                sub_selects: sub_selects.to_vec(),
            };

            let (main_statement, main_parameters, _) = generate(
                fmt_main_query(db_schema, select_request.schema_name, &select_request, env)
                    .context(CoreSnafu)
                    .inspect_err(|_| {
                        let _ = conn.execute_batch("ROLLBACK");
                    })?,
            );
            debug!("main_statement: {}\n{:?}", main_statement, main_parameters);
            let mut main_stm = conn
                .prepare_cached(main_statement.as_str())
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })
                .context(SqliteDbSnafu { authenticated })?;
            let parameters = params_from_iter(main_parameters.into_iter().map(wrap_param));
            let mut rows = main_stm
                .query(parameters)
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })
                .context(SqliteDbSnafu { authenticated })?;

            let response_row = rows
                .next()
                .context(SqliteDbSnafu { authenticated })
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })?
                .unwrap();

            {
                Ok(ApiResponse {
                    page_total: response_row.get("page_total").context(SqliteDbSnafu { authenticated })?, //("page_total"),
                    total_result_set: response_row.get("total_result_set").context(SqliteDbSnafu { authenticated })?, //("total_result_set"),
                    top_level_offset: 0,
                    body: if return_representation {
                        response_row.get("body").context(SqliteDbSnafu { authenticated })?
                    } else {
                        "".to_string()
                    }, //("body"),
                    response_headers: response_row.get("response_headers").context(SqliteDbSnafu { authenticated })?, //("response_headers"),
                    response_status: response_row.get("response_status").context(SqliteDbSnafu { authenticated })?,   //("response_status"),
                })
            }
            .inspect_err(|_| {
                let _ = conn.execute_batch("ROLLBACK");
            })?
        }
        _ => {
            let (main_statement, main_parameters, _) = generate(
                fmt_main_query(db_schema, request.schema_name, request, env)
                    .context(CoreSnafu)
                    .inspect_err(|_| {
                        let _ = conn.execute_batch("ROLLBACK");
                    })?,
            );
            debug!("main_statement: {}\n{:?}", main_statement, main_parameters);
            let mut main_stm = conn
                .prepare_cached(main_statement.as_str())
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })
                .context(SqliteDbSnafu { authenticated })?;
            let parameters = params_from_iter(main_parameters.into_iter().map(wrap_param));
            let mut rows = main_stm
                .query(parameters)
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })
                .context(SqliteDbSnafu { authenticated })?;

            let response_row = rows
                .next()
                .context(SqliteDbSnafu { authenticated })
                .inspect_err(|_| {
                    let _ = conn.execute_batch("ROLLBACK");
                })?
                .unwrap();

            {
                Ok(ApiResponse {
                    page_total: response_row.get("page_total").context(SqliteDbSnafu { authenticated })?, //("page_total"),
                    total_result_set: response_row.get("total_result_set").context(SqliteDbSnafu { authenticated })?, //("total_result_set"),
                    top_level_offset: 0,
                    body: if return_representation {
                        response_row.get("body").context(SqliteDbSnafu { authenticated })?
                    } else {
                        "".to_string()
                    }, //("body"),
                    response_headers: response_row.get("response_headers").context(SqliteDbSnafu { authenticated })?, //("response_headers"),
                    response_status: response_row.get("response_status").context(SqliteDbSnafu { authenticated })?,   //("response_status"),
                })
            }
            .inspect_err(|_| {
                let _ = conn.execute_batch("ROLLBACK");
            })?
        }
    };

    if request.accept_content_type == SingularJSON && api_response.page_total != 1 {
        conn.execute_batch("ROLLBACK").context(SqliteDbSnafu { authenticated })?;
        return Err(to_core_error(SingularityError {
            count: api_response.page_total,
            content_type: "application/vnd.pgrst.object+json".to_string(),
        }));
    }

    if request.method == Method::PUT && api_response.page_total != 1 {
        // Makes sure the querystring pk matches the payload pk
        // e.g. PUT /items?id=eq.1 { "id" : 1, .. } is accepted,
        // PUT /items?id=eq.14 { "id" : 2, .. } is rejected.
        // If this condition is not satisfied then nothing is inserted,
        conn.execute_batch("ROLLBACK").context(SqliteDbSnafu { authenticated })?;
        return Err(to_core_error(PutMatchingPkError));
    }

    if config.db_tx_rollback {
        conn.execute_batch("ROLLBACK").context(SqliteDbSnafu { authenticated })?;
    } else {
        conn.execute_batch("COMMIT").context(SqliteDbSnafu { authenticated })?;
    }

    Ok(api_response)
}

pub struct SQLiteBackend {
    //vhost: String,
    config: VhostConfig,
    pool: Pool<SqliteConnectionManager>,
    db_schema: DbSchemaWrap,
}

fn cs(x: String, y: String) -> SqliteResult<bool> {
    let x_json: Result<Value, _> = serde_json::from_str(&x);
    let y_json: Result<Value, _> = serde_json::from_str(&y);

    match (x_json, y_json) {
        (Ok(x_val), Ok(y_val)) => Ok(json_contains(&x_val, &y_val)),
        _ => Ok(false), // Return false in case of any parsing error
    }
}

fn json_contains(x: &Value, y: &Value) -> bool {
    match (x, y) {
        (Value::Object(x_map), Value::Object(y_map)) => y_map
            .iter()
            .all(|(key, y_val)| x_map.get(key).map_or(false, |x_val| json_contains(x_val, y_val))),
        (Value::Array(x_arr), Value::Array(y_arr)) => y_arr.iter().all(|y_item| x_arr.iter().any(|x_item| json_contains(x_item, y_item))),
        (Value::Array(x_arr), _) => x_arr.iter().any(|x_item| json_contains(x_item, y)),
        _ => x == y,
    }
}

#[derive(Debug)]
struct MyConnectionCustomizer;
// we implement this only for testing in rust, the user will register his own functions
impl CustomizeConnection<Connection, rusqlite::Error> for MyConnectionCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), rusqlite::Error> {
        conn.create_scalar_function("cs", 2, FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
            let x = ctx.get::<String>(0)?;
            let y = ctx.get::<String>(1)?;
            cs(x, y)
        })
    }
}

#[async_trait]
impl Backend for SQLiteBackend {
    async fn init(_vhost: String, config: VhostConfig) -> Result<Self> {
        //setup db connection
        let db_file = config.db_uri.clone();
        let manager = SqliteConnectionManager::file(db_file).with_init(|c| array::load_module(c));
        let pool = Pool::builder()
            .connection_customizer(Box::new(MyConnectionCustomizer))
            .max_size(config.db_pool as u32)
            .build(manager)
            .unwrap();

        //read db schema
        let db_schema: DbSchemaWrap = match config.db_schema_structure.clone() {
            SqlFile(f) => match fs::read_to_string(vec![&f, &format!("sqlite_{f}")].into_iter().find(|f| Path::new(f).exists()).unwrap_or(&f)) {
                Ok(q) => match pool.get() {
                    Ok(conn) => task::block_in_place(|| {
                        let authenticated = false;
                        let query = include_files(q);
                        //println!("schema query: {query}");
                        let mut stmt = conn.prepare(query.as_str()).context(SqliteDbSnafu { authenticated })?;
                        let mut rows = stmt.query([]).context(SqliteDbSnafu { authenticated })?;
                        match rows.next().context(SqliteDbSnafu { authenticated })? {
                            Some(r) => {
                                //println!("json db_schema: {}", r.get::<usize, String>(0).context(SqliteDbSnafu { authenticated })?.as_str());
                                let s: String = r.get::<usize, String>(0).context(SqliteDbSnafu { authenticated })?;
                                Ok(DbSchemaWrap::new(s, |s| {
                                    serde_json::from_str::<DbSchema>(s.as_str())
                                        .context(JsonDeserializeSnafu)
                                        .context(CoreSnafu)
                                }))
                            }
                            None => Err(Error::Internal {
                                message: "sqlite structure query did not return any rows".to_string(),
                            }),
                        }
                    }),
                    Err(e) => Err(e).context(SqliteDbPoolSnafu),
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
        Ok(SQLiteBackend { config, pool, db_schema })
    }
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse> {
        execute(self.db_schema(), &self.pool, authenticated, request, env, &self.config)
    }
    fn db_schema(&self) -> &DbSchema {
        self.db_schema.borrow_schema().as_ref().unwrap()
    }
    fn config(&self) -> &VhostConfig {
        &self.config
    }
}
