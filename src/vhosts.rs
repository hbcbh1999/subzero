use dashmap::DashMap;
#[cfg(feature = "postgresql")]
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime, Timeouts, Object, PoolError};
#[cfg(feature = "postgresql")]
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
#[cfg(feature = "postgresql")]
use postgres_openssl::{MakeTlsConnector};
// #[cfg(feature = "postgresql")]
// use futures::future::ok;
use snafu::ResultExt;
#[cfg(feature = "postgresql")]
use tokio::time::{Duration, sleep};
#[cfg(feature = "postgresql")]
use tokio_postgres::{IsolationLevel};

#[cfg(feature = "sqlite")]
use r2d2::Pool;
#[cfg(feature = "sqlite")]
use r2d2_sqlite::SqliteConnectionManager;
#[cfg(feature = "sqlite")]
use rusqlite::vtab::array;

use crate::{
    config::{SchemaStructure::*, VhostConfig},
    error::{Result, *},
    schema::DbSchema,
};
#[cfg(feature = "sqlite")]
use tokio::task;
use std::{fs, sync::Arc};

#[cfg(not(any(feature = "sqlite", feature = "postgresql")))]
use std::collections::HashMap;

pub struct VhostResources {
    #[cfg(feature = "postgresql")]
    pub db_pool: Pool,
    #[cfg(feature = "sqlite")]
    pub db_pool: Pool<SqliteConnectionManager>,
    #[cfg(not(any(feature = "sqlite", feature = "postgresql")))]
    pub db_pool: Option<String>,
    pub db_schema: DbSchema,
    pub config: VhostConfig,
}

pub fn get_resources<'a>(vhost: &Option<&str>, store: &'a Arc<DashMap<String, VhostResources>>) -> Result<&'a VhostResources> {
    let gg = match vhost {
        None => store.get("default"),
        Some(v) => match store.get(*v) {
            Some(r) => Some(r),
            None => store.get("default"),
        },
    };

    if gg.is_some() {
        Ok(gg.unwrap().value())
    } else {
        Err(Error::NotFound { target: "vhost".to_string() })
    }
}

#[cfg(feature="postgresql")]
async fn wait_for_pg_connection(vhost: &String, db_pool: &Pool) -> Result<Object, PoolError> {

    let mut i = 1;
    let mut time_since_start = 0;
    let max_delay_interval = 10;
    let max_retry_interval = 30;
    let mut client = db_pool.get().await;
    while let Err(e)  = client {
        println!("[{}] Failed to connect to PostgreSQL {:?}", vhost, e);
        let time = Duration::from_secs(i);
        println!("[{}] Retrying the PostgreSQL connection in {:?} seconds..", vhost, time.as_secs());
        sleep(time).await;
        client = db_pool.get().await;
        i *= 2;
        if i > max_delay_interval { i = max_delay_interval };
        time_since_start += i;
        if time_since_start > max_retry_interval { break }
    };
    match client {
        Err(_) =>{},
        _ => println!("[{}] Connection to PostgreSQL successful", vhost)
    }
    client
}
pub async fn create_resources(vhost: &String, config: VhostConfig, store: Arc<DashMap<String, VhostResources>>) -> Result<()> {
    //setup db connection
    #[cfg(feature = "postgresql")]
    let pg_uri = config.db_uri.clone();
    #[cfg(feature = "postgresql")]
    let pg_config = pg_uri.parse::<tokio_postgres::Config>().unwrap();
    #[cfg(feature = "postgresql")]
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    #[cfg(feature = "postgresql")]
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    #[cfg(feature = "postgresql")]
    builder.set_verify(SslVerifyMode::NONE);
    #[cfg(feature = "postgresql")]
    let tls_connector = MakeTlsConnector::new(builder.build());

    #[cfg(feature = "postgresql")]
    let mgr = Manager::from_config(pg_config, tls_connector, mgr_config);
    #[cfg(feature = "postgresql")]
    let timeouts = Timeouts {
        create: Some(Duration::from_millis(5000)),
        wait: None,
        recycle: None,
    };
    #[cfg(feature = "postgresql")]
    let db_pool = Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .max_size(config.db_pool)
        .timeouts(timeouts)
        .build()
        .unwrap();

    #[cfg(not(any(feature = "sqlite", feature = "postgresql")))]
    let db_pool = None;

    #[cfg(feature = "sqlite")]
    let db_file = config.db_uri.clone();

    #[cfg(feature = "sqlite")]
    let manager = SqliteConnectionManager::file(db_file).with_init(|c| array::load_module(&c));
    #[cfg(feature = "sqlite")]
    let db_pool = Pool::builder().max_size(config.db_pool as u32).build(manager).unwrap();
    //read db schema
    let db_schema = match &config.db_schema_structure {
        SqlFile(f) => match fs::read_to_string(f) {
            #[cfg(not(any(feature = "sqlite", feature = "postgresql")))]
            Ok(_s) => Ok(DbSchema { schemas: HashMap::new() }),
            #[cfg(feature = "sqlite")]
            Ok(q) => match db_pool.get() {
                Ok(conn) => {
                    task::block_in_place(|| {
                        let authenticated = false;
                        let mut stmt = conn.prepare(q.as_str()).context(DbError { authenticated })?;
                        let mut rows = stmt.query([]).context(DbError { authenticated })?;
                        match rows.next().context(DbError { authenticated })? {
                            Some(r) => {
                                serde_json::from_str::<DbSchema>(r.get::<usize,String>(0).context(DbError { authenticated })?.as_str()).context(JsonDeserialize)
                            },
                            None => Err(Error::InternalError { message: "sqlite structure query did not return any rows".to_string() }),
                        }
                    })
                }
                Err(e) => Err(e).context(DbPoolError),
            },
            #[cfg(feature = "postgresql")]
            Ok(q) => match wait_for_pg_connection(vhost, &db_pool).await { //match db_pool.get().await {
                Ok(mut client) => {
                    let authenticated = false;
                    let transaction = client
                        .build_transaction()
                        .isolation_level(IsolationLevel::Serializable)
                        .read_only(true)
                        .start()
                        .await
                        .context(DbError { authenticated})?;
                    let _ = transaction.query("set local schema ''", &[]).await;
                    match transaction.query(&q, &[&config.db_schemas]).await {
                        Ok(rows) => {
                            transaction.commit().await.context(DbError { authenticated })?;
                            serde_json::from_str::<DbSchema>(rows[0].get(0)).context(JsonDeserialize)
                        }
                        Err(e) => {
                            transaction.rollback().await.context(DbError { authenticated })?;
                            Err(e).context(DbError { authenticated })
                        }
                    }
                }
                Err(e) => Err(e).context(DbPoolError),
            },
            Err(e) => Err(e).context(ReadFile { path: f }),
        },
        JsonFile(f) => match fs::read_to_string(f) {
            Ok(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
            Err(e) => Err(e).context(ReadFile { path: f }),
        },
        JsonString(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
    }?;

    let key = vhost.clone();

    if let Some((_, _r)) = store.remove(&key) {
        #[cfg(feature = "postgresql")]
        _r.db_pool.close();
    }

    store.insert(key, VhostResources { db_pool, db_schema, config });
    Ok(())
}
