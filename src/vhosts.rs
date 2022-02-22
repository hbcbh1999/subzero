use dashmap::DashMap;
#[cfg(feature = "postgresql")]
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime, Timeouts};
// #[cfg(feature = "postgresql")]
// use futures::future::ok;
use snafu::ResultExt;
#[cfg(feature = "postgresql")]
use tokio::time::Duration;
#[cfg(feature = "postgresql")]
use tokio_postgres::{IsolationLevel, NoTls};

#[cfg(feature = "sqlite")]
use r2d2::Pool;
#[cfg(feature = "sqlite")]
use r2d2_sqlite::SqliteConnectionManager;
#[cfg(feature = "sqlite")]
use rusqlite::vtab::array;

use subzero::{
    config::{SchemaStructure::*, VhostConfig},
    error::{Result, *},
    schema::DbSchema,
};

use std::{fs, sync::Arc};

#[cfg(feature = "clickhouse")]
use std::collections::HashMap;
#[cfg(feature = "sqlite")]
use std::collections::HashMap;

pub struct VhostResources {
    #[cfg(feature = "postgresql")]
    pub db_pool: Pool,
    #[cfg(feature = "sqlite")]
    pub db_pool: Pool<SqliteConnectionManager>,
    #[cfg(not(feature = "sqlite","postgresql"))]
    pub db_pool: Option<String>,
    pub db_schema: DbSchema,
    pub config: VhostConfig,
}

pub fn get_resources<'a>(
    vhost: &Option<&str>,
    store: &'a Arc<DashMap<String, VhostResources>>,
) -> Result<&'a VhostResources> {
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
        Err(Error::NotFound {
            target: "vhost".to_string(),
        })
    }
}

pub async fn create_resources(
    vhost: &String,
    config: VhostConfig,
    store: Arc<DashMap<String, VhostResources>>,
) -> Result<()> {
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
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
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

    #[cfg(not(feature = "sqlite","postgresql"))]
    let db_pool = None;

    #[cfg(feature = "sqlite")]
    let db_file = config.db_uri.clone();

    #[cfg(feature = "sqlite")]
    let manager = SqliteConnectionManager::file(db_file).with_init(|c| array::load_module(&c));
    #[cfg(feature = "sqlite")]
    let db_pool = Pool::builder()
        .max_size(config.db_pool as u32)
        .build(manager)
        .unwrap();
    //read db schema
    let db_schema = match &config.db_schema_structure {
        SqlFile(f) => match fs::read_to_string(f) {
            #[cfg(not(feature = "sqlite","postgresql"))]
            Ok(_s) => Ok(DbSchema {
                schemas: HashMap::new(),
            }),
            #[cfg(feature = "sqlite")]
            Ok(_s) => Ok(DbSchema {
                schemas: HashMap::new(),
            }),
            #[cfg(feature = "postgresql")]
            Ok(s) => match db_pool.get().await {
                Ok(mut client) => {
                    let transaction = client
                        .build_transaction()
                        .isolation_level(IsolationLevel::Serializable)
                        .read_only(true)
                        .start()
                        .await
                        .context(DbError {
                            authenticated: false,
                        })?;
                    let _ = transaction.query("set local schema ''", &[]).await;
                    match transaction.query(&s, &[&config.db_schemas]).await {
                        Ok(rows) => {
                            transaction.commit().await.context(DbError {
                                authenticated: false,
                            })?;
                            serde_json::from_str::<DbSchema>(rows[0].get(0))
                                .context(JsonDeserialize)
                        }
                        Err(e) => {
                            transaction.rollback().await.context(DbError {
                                authenticated: false,
                            })?;
                            Err(e).context(DbError {
                                authenticated: false,
                            })
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

    store.insert(
        key,
        VhostResources {
            db_pool,
            db_schema,
            config,
        },
    );
    Ok(())
}
