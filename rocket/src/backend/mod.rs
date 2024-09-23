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
use subzero_core::{
    api::{ApiRequest, ApiResponse},
    schema::DbSchema,
};

pub use subzero_core::schema::include_files;
use std::collections::HashMap;
use crate::error::Result;
use crate::config::VhostConfig;
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
