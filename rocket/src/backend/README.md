backend in this context refers to code responsible to taking in an AST (`ApiRequest`), transforming it into a SQL query with a specific dialect and executing it against the database.

all the backends basically implement this trait:

```rust
pub trait Backend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self>
    where Self: Sized;
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse>;
    fn db_schema(&self) -> &DbSchema;
    fn config(&self) -> &VhostConfig;
}
```