use dashmap::{DashMap, mapref::one::Ref};
use subzero::{
    config::{VhostConfig},
    error::{Result, *},
    backend::{Backend},
};
#[cfg(feature = "postgresql")]
use subzero::backend::postgresql::PostgreSQLBackend;
#[cfg(feature = "sqlite")]
use subzero::backend::sqlite::SQLiteBackend;
use std::{sync::Arc};

pub struct VhostResources  {
    pub backend: Box<dyn Backend + Send + Sync>
}

pub fn get_resources<'a>(vhost: &Option<&str>, store: &'a Arc<DashMap<String, VhostResources>>) -> Result<Ref<'a, String, VhostResources>> {
    match vhost {
        None => match store.get("default") {
            Some(r) => Ok(r),
            None => Err(Error::NotFound { target: "vhost".to_string() })
        },
        Some(v) => match store.get(*v) {
            Some(r) => Ok(r),
            None => Err(Error::NotFound { target: v.to_string() })
        },
    }
}

pub async fn create_resources(vhost: &String, config: VhostConfig, store: Arc<DashMap<String, VhostResources>>) -> Result<()> {
    //setup db connection
    let backend:Box<dyn Backend + Send + Sync> = match config.db_type.as_str() {
        #[cfg(feature = "postgresql")]
        "postgresql" => Box::new(PostgreSQLBackend::init(vhost.clone(), config).await?),
        #[cfg(feature = "sqlite")]
        "sqlite" => Box::new(SQLiteBackend::init(vhost.clone(), config).await?),
        t => panic!("unsuported database type: {}", t),
    };

    let key = vhost.clone();

    if let Some((_, _r)) = store.remove(&key) {
        // #[cfg(feature = "postgresql")]
        // _r.db_pool.close();
    }
    store.insert(vhost.clone(), VhostResources { backend });
    Ok(())
}