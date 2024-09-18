use subzero_core::{
    api::{ApiRequest, ApiResponse},
    schema::DbSchema,
};

pub use subzero_core::schema::{include_files};
use std::collections::HashMap;
use crate::error::Result;
use crate::config::{VhostConfig};
use async_trait::async_trait;
use ouroboros::self_referencing;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "mysql")]
pub mod mysql;
#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[async_trait]
pub trait Backend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self>
    where
        Self: Sized;
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse>;
    fn db_schema(&self) -> &DbSchema;
    fn config(&self) -> &VhostConfig;
}

#[self_referencing]
pub struct DbSchemaWrap {
    schema_string: String,
    #[covariant]
    #[borrows(schema_string)]
    schema: Result<DbSchema<'this>>,
}
