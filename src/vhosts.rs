
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Timeouts, Runtime};
use tokio_postgres::{NoTls,IsolationLevel};
use snafu::{ResultExt,};
use dashmap::{DashMap, };
use tokio::time::{Duration};

use crate::{
    
    schema::DbSchema,
    error::{*, Result},
    config::{VhostConfig, SchemaStructure::*},
};

use std::{
    fs,
    sync::{Arc,},
};


pub struct VhostResources {
    pub db_pool: Pool,
    pub db_schema: DbSchema,
    pub config: VhostConfig,
}


pub fn get_resources<'a>(vhost: &Option<&str>, store: &'a Arc<DashMap<String, VhostResources>>) -> Result<&'a VhostResources> {
    let gg = match vhost {
        None => store.get("default"),
        Some(v) => match store.get(*v) {
            Some(r) => Some(r),
            None => store.get("default")
        }
    };
    
    if gg.is_some() {
        Ok(gg.unwrap().value())
    }
    else {
        Err(Error::NotFound)
    }

}

pub async fn create_resources(vhost: &String, config: VhostConfig, store: Arc<DashMap<String, VhostResources>>) -> Result<()> {

    //setup db connection
    let pg_uri = config.db_uri.clone();
    let pg_config = pg_uri.parse::<tokio_postgres::Config>().unwrap();
    let mgr_config = ManagerConfig {recycling_method: RecyclingMethod::Fast};
    let mgr = Manager::from_config(pg_config, NoTls, mgr_config);
    let timeouts = Timeouts {
        create: Some(Duration::from_millis(5000)),
        wait: None,
        recycle: None,
    };
    let db_pool = Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .max_size(10)
        .timeouts(timeouts)
        .build().unwrap();

    //read db schema
    let db_schema = match &config.db_schema_structure {
        SqlFile(f) => match fs::read_to_string(f) {
            Ok(s) => {
                match db_pool.get().await{
                    Ok(mut client) => {
                        let transaction = client
                            .build_transaction()
                            .isolation_level(IsolationLevel::Serializable)
                            .read_only(true)
                            .start()
                            .await.context(DbError {authenticated:false})?;
                        let _ = transaction.query("set local schema ''", &[]).await;
                        match transaction.query(&s, &[&config.db_schemas]).await {
                            Ok(rows) => {
                                transaction.commit().await.context(DbError {authenticated:false})?;
                                serde_json::from_str::<DbSchema>(rows[0].get(0)).context(JsonDeserialize)
                            },
                            Err(e) => {
                                transaction.rollback().await.context(DbError {authenticated:false})?;
                                Err(e).context(DbError {authenticated:false})
                            }
                        }
                    },
                    Err(e) => Err(e).context(DbPoolError)
                }
            },
            Err(e) => Err(e).context(ReadFile {path: f})
        },
        JsonFile(f) => {
            match fs::read_to_string(f) {
                Ok(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize),
                Err(e) => Err(e).context(ReadFile {path: f})
            }
        },
        JsonString(s) => serde_json::from_str::<DbSchema>(s.as_str()).context(JsonDeserialize)
    }?;

    let key = vhost.clone();

    if let Some((_, r)) = store.remove(&key) {
        r.db_pool.close();
    }

    store.insert(key, VhostResources {db_pool,db_schema,config,});
    Ok(())
    
}
