use crate::{config::VhostConfig, error::Result, api::{ApiRequest,ApiResponse, Method}, schema::DbSchema};
use serde_json::{Value};
use async_trait::async_trait;

#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[async_trait]
pub trait Backend{
    async fn init(vhost: String, config: VhostConfig) -> Result<Self>
    where Self: Sized;
    async fn execute(&self,
        method: &Method, readonly: bool, authenticated: bool, schema_name: &String, request: &ApiRequest, role: Option<&String>,
        jwt_claims: &Option<Value>
    ) -> Result<ApiResponse>;
    fn db_schema(&self) -> &DbSchema;
    fn config(&self) -> &VhostConfig;
}