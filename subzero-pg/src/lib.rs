

use pgrx::prelude::*;

//use std::fs;
use subzero_core::schema::DbSchema;
use subzero_core::api::ApiRequest;
use subzero_core::parser::postgrest;
use std::borrow::Cow;
use subzero_core::formatter::{
    Param::*,
    ToParam,
    postgresql::{generate, fmt_main_query_internal},
};
use subzero_core::
    api::{SingleVal, ListVal, Payload, };
use subzero_core::error::{*};
use ouroboros::self_referencing;
use parking_lot::RwLock;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

static DB_SCHEMA: RwLock<Option<DbSchemaWrap>> = RwLock::new(None);

#[self_referencing]
pub struct DbSchemaWrap {
    schema_string: String,
    #[covariant]
    #[borrows(schema_string)]
    schema: Result<DbSchema<'this>, String>,
}
impl DbSchemaWrap {
    pub fn schema(&self) -> &DbSchema {
        self.borrow_schema().as_ref().unwrap()
    }
}


pgrx::pg_module_magic!();
extension_sql_file!("../sql/init.sql", bootstrap);


#[derive(Debug, Serialize, Deserialize, PostgresType)]
pub struct Request<'a> {
    method: &'a str,
    path: &'a str,
    get: Vec<(&'a str, &'a str)>,//pgrx::Json,
    headers: HashMap<&'a str, &'a str>,//pgrx::Json,
    cookies: HashMap<&'a str, &'a str>,//pgrx::Json,
    body: Option<&'a str>,
    env: HashMap<&'a str, &'a str>,//pgrx::Json,
}

#[derive(Debug, Serialize, Deserialize, PostgresType)]
pub struct Response {
    page_total: Option<i64>,
    total_result_set: Option<i64>,
    body: String,
    constraints_satisfied: bool,
    headers: Option<String>,
    status: Option<String>,
}

#[pg_extern]
fn introspect(
    schemas: &str,
    allow_login_roles: bool,
    custom_relations: Option<&str>,
    custom_permissions: Option<&str>,
) -> bool {
    let schemas = schemas.split(',').collect::<Vec<_>>();
    let query = "select introspect_schemas($1, $2, $3, $4)";
    let json = Spi::connect(|client| {
        client
            .select(query, None, Some(vec![
                (PgBuiltInOids::TEXTARRAYOID.oid(), schemas.into_datum())
                , (PgBuiltInOids::BOOLOID.oid(), allow_login_roles.into_datum())
                , (PgBuiltInOids::JSONOID.oid(), custom_permissions.into_datum())
                , (PgBuiltInOids::JSONOID.oid(), custom_relations.into_datum())
            ]))?
            .first()
            .get_one::<String>()
    });
    let db_schema = match json {
        Ok(Some(ss)) => {
            Ok(DbSchemaWrap::new(ss, |s| serde_json::from_str::<DbSchema>(s).map_err(|e| e.to_string())))
        }
        Err(e) => {
            Err(format!("Failed to introspect: {}", e))
        }
        _ => {
            Err("Failed to introspect".into())
        }
    };
    match db_schema {
        Ok(s) => {
            *DB_SCHEMA.write() = Some(s);
            true
        }
        Err(e) => {
            warning!("subzero introspection failed: {}", e);
            false
        }
    }
}



#[pg_extern]
fn handle(
    schema: &str,
    relation: &str,
    request: Request<'_>,
    max_rows: Option<i64>
) -> Result<Response, String> {
    let s = DB_SCHEMA.read();
    let _db_schema = match s.as_ref(){
        Some(d) => d.schema(),
        None => {
            error!("Schema not introspected, you need to run subzero.introspect first");
        }
    };

    let _max_rows = max_rows.map(|m| m.to_string());

    let parsed_request = postgrest::parse(
        schema,
        relation,
        _db_schema,
        request.method, 
        request.path, 
        request.get, 
        request.body, 
        request.headers, 
        request.cookies, 
        _max_rows.as_deref()
        //None
    ).map_err(|e| e.to_string())?;

    let ApiRequest {
        method,
        schema_name,
        query,
        preferences,
        accept_content_type,
        ..
    } = parsed_request;

    let (main_statement, main_parameters, _) = generate(
        fmt_main_query_internal(_db_schema, schema_name, method, &accept_content_type, &query, &preferences, &request.env)
        .map_err(|e| e.to_string())?
    );
    //info!("main_statement: {}", main_statement);
    //info!("main_parameters: {:?}", main_parameters);
    let parameters = convert_params(main_parameters).map_err(|e| e.to_string())?;
    //info!("parameters: {:?}", parameters);
    let response:Result<Response,spi::SpiError> = Spi::connect(|client| {
        let row = client
            .select(&main_statement,
                None,
                Some(parameters)
            )?
            .first();
        //info!("row: {:?}, {:?}", row, row.columns());
        Ok(
        Response {
            page_total: row.get::<i64>(1)?,
            total_result_set: row.get::<i64>(2)?,
            body: row.get::<String>(3)?.unwrap_or_default(),
            constraints_satisfied: row.get::<bool>(4)?.unwrap_or(false),
            headers: row.get::<String>(5)?,
            status: row.get::<String>(6)?,
        }
        )
    });
    
    response.map_err(|e| e.to_string())
}

fn to_oid(dt: &str) -> pg_sys::Oid {
    match dt {
        "boolean" => bool::type_oid(),
        "integer" => i32::type_oid(),
        "bigint" => i64::type_oid(),
        "text" => pg_sys::TEXTOID,
        "json" => pg_sys::JSONOID,
        "jsonb" => pg_sys::JSONBOID,
        "uuid" => pg_sys::UUIDOID,
        "date" => pg_sys::DATEOID,
        "timestamp" => pg_sys::TIMESTAMPOID,
        "timestamptz" => pg_sys::TIMESTAMPTZOID,
        "time" => pg_sys::TIMEOID,
        "timetz" => pg_sys::TIMETZOID,
        "interval" => pg_sys::INTERVALOID,
        "numeric" | "decimal" => f64::type_oid(),
        "bytea" => pg_sys::BYTEAOID,
        "point" => pg_sys::POINTOID,
        "line" => pg_sys::LINEOID,
        "lseg" => pg_sys::LSEGOID,
        "box" => pg_sys::BOXOID,
        "path" => pg_sys::PATHOID,
        "polygon" => pg_sys::POLYGONOID,
        "circle" => pg_sys::CIRCLEOID,
        "cidr" => pg_sys::CIDROID,
        "inet" => pg_sys::INETOID,
        "macaddr" => pg_sys::MACADDROID,
        "bit" => pg_sys::BITOID,
        "varbit" => pg_sys::VARBITOID,
        "tsvector" => pg_sys::TSVECTOROID,
        "tsquery" => pg_sys::TSQUERYOID,
        _ => pg_sys::TEXTOID,
    }
}

fn to_datum_param(data_type: &Option<Cow<str>>, value: &str) -> Result<(PgOid, Option<pg_sys::Datum>), Error> {
    let data_type = match data_type {
        Some(t) => t,
        None => "text",
    };
    let oid = to_oid(data_type);
    let datum = match data_type {
        "boolean" => {
            let v = value.parse::<bool>().map_err(|e| Error::ParseRequestError {
                message: String::from("failed to parse bool"),
                details: format!("{e}"),
            })?;
            v.into_datum()
        }
        "integer" => {
            let v = value.parse::<i32>().map_err(|e| Error::ParseRequestError {
                message: String::from("failed to parse i32"),
                details: format!("{e}"),
            })?;
            v.into_datum()
        }
        "bigint" => {
            let v = value.parse::<i64>().map_err(|e| Error::ParseRequestError {
                message: String::from("failed to parse i64"),
                details: format!("{e}"),
            })?;
            v.into_datum()
        }
        "numeric" | "decimal" => {
            let v = value.parse::<f64>().map_err(|e| Error::ParseRequestError {
                message: String::from("failed to parse numeric/decimal"),
                details: format!("{e}"),
            })?;
            v.into_datum()
        }
        _ => {
            value.into_datum() // just send over a str
        }
    };
    Ok((oid.into(), datum))
}

fn convert_params(params: Vec<&(dyn ToParam + Sync)>) -> Result<Vec<(PgOid, Option<pg_sys::Datum>)>, Error> {
    params
        .iter()
        .map(|p| match p.to_param() {
            SV(SingleVal(v, d)) => to_datum_param(d, v.as_ref()),
            LV(ListVal(v, d)) => {
                let inner_type = match d {
                    Some(t) => t.as_ref().replace("[]", ""),
                    None => String::from("text"),
                };
                let inner_type_oid = to_oid(inner_type.as_str());
                let oid = unsafe { pg_sys::get_array_type(inner_type_oid) };
                let vv = match inner_type.as_str() {
                    "boolean" => v
                        .iter()
                        .map(|i| i.as_ref().parse::<bool>())
                        .collect::<Result<Vec<bool>, _>>()
                        .map_err(|e| Error::ParseRequestError {
                            message: String::from("failed to parse bool"),
                            details: format!("{e}"),
                        })?
                        .into_datum(),
                    "integer" => v
                        .iter()
                        .map(|i| i.as_ref().parse::<i32>())
                        .collect::<Result<Vec<i32>, _>>()
                        .map_err(|e| Error::ParseRequestError {
                            message: String::from("failed to parse i32"),
                            details: format!("{e}"),
                        })?
                        .into_datum(),
                    "bigint" => v
                        .iter()
                        .map(|i| i.as_ref().parse::<i64>())
                        .collect::<Result<Vec<i64>, _>>()
                        .map_err(|e| Error::ParseRequestError {
                            message: String::from("failed to parse i64"),
                            details: format!("{e}"),
                        })?
                        .into_datum(),
                    "numeric" | "decimal" => v
                        .iter()
                        .map(|i| i.as_ref().parse::<f64>())
                        .collect::<Result<Vec<f64>, _>>()
                        .map_err(|e| Error::ParseRequestError {
                            message: String::from("failed to parse numeric/decimal"),
                            details: format!("{e}"),
                        })?
                        .into_datum(),
                    _ => v.iter().map(|i| i.as_ref()).collect::<Vec<&str>>().into_datum(),
                };
                Ok((oid.into(), vv))
            }
            PL(Payload(v, d)) => to_datum_param(d, v.as_ref()),
            Str(v) => to_datum_param(&Some(Cow::Borrowed("text")), v),
            StrOwned(v) => to_datum_param(&Some(Cow::Borrowed("text")), v.as_str()),
        })
        .collect::<Result<Vec<_>, _>>()
}
// This function is called when the extension is loaded into a PostgreSQL database
// You can use it to setup or configure your extension
#[pg_guard]
pub extern "C" fn _PG_init() {
    info!("subzero extension loaded");
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::collections::HashMap;
    use crate::Request;
    #[pg_test]
    fn test_introspect() {
        assert!(crate::introspect("public", false, None, None));
    }

    #[pg_test]
    fn test_handle() {
        _ = Spi::run("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
        _ = Spi::run("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')");
    
        assert!(crate::introspect("public", false, None, None));
        let response = crate::handle(
            "public",
            "users",
            Request{
                method: "GET",
                path: "/users",
                get: vec![
                    ("select", "id,name"),
                    ("id", "eq.2")
                ],
                headers: HashMap::new(),
                cookies: HashMap::new(),
                body: None,
                env: HashMap::from([("role", "user")])
            },
            None
        );
        info!("response: {:?}", response);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.page_total, Some(1));
        assert_eq!(response.total_result_set, None);
        assert_eq!(response.body, r#"[{"id":2,"name":"Bob"}]"#);
    }

}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
