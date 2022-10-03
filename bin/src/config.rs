use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use subzero_core::api::{DEFAULT_SAFE_SELECT_FUNCTIONS};

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
#[serde(rename_all(serialize = "snake_case", deserialize = "snake_case"))]
pub enum SchemaStructure {
    SqlFile(String),
    JsonFile(String),
    JsonString(String),
}
impl Default for SchemaStructure {
    #[cfg(any(feature = "sqlite", feature = "postgresql"))]
    fn default() -> Self { SchemaStructure::SqlFile("introspection_query.sql".to_string()) }
    #[cfg(not(any(feature = "sqlite", feature = "postgresql")))]
    fn default() -> Self { SchemaStructure::JsonString(r#"{"schemas":[]}"#.to_string()) }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct Config {
    #[serde(with = "vhosts")]
    pub vhosts: HashMap<String, VhostConfig>,
}

mod vhosts {
    use super::VhostConfig;

    use std::collections::HashMap;

    use serde::de::{Deserialize, Deserializer};
    use serde::ser::Serializer;

    pub fn serialize<S>(map: &HashMap<String, VhostConfig>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_map(
            map.iter()
                .map(|(k, v)| (k.replace('.', "_"), v))
                .collect::<HashMap<String, &VhostConfig>>(),
        )
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, VhostConfig>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = HashMap::new();
        for (k, v) in HashMap::<String, VhostConfig>::deserialize(deserializer)? {
            map.insert(k.replace('_', "."), v);
        }
        Ok(map)
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq, Clone)]
pub struct VhostConfig {
    pub url_prefix: Option<String>,
    pub static_files_dir: Option<String>,
    #[serde(default = "db_type")]
    pub db_type: String,
    pub db_uri: String,
    #[serde(default = "db_schemas")]
    pub db_schemas: Vec<String>,
    #[serde(default)]
    pub db_schema_structure: SchemaStructure,
    pub db_anon_role: Option<String>,
    pub db_max_rows: Option<u32>,
    #[serde(default = "db_allowed_select_functions")]
    pub db_allowed_select_functions: Vec<String>,
    #[serde(default)]
    pub db_use_legacy_gucs: bool,
    #[serde(default = "db_pool")]
    pub db_pool: usize,
    #[serde(default)]
    pub db_tx_rollback: bool,
    #[serde(deserialize_with = "to_tuple", default)]
    pub db_pre_request: Option<(String, String)>,
    pub jwt_secret: Option<String>,
    pub jwt_aud: Option<String>,
    #[serde(default = "role_claim_key")]
    pub role_claim_key: String,
}

fn db_allowed_select_functions() -> Vec<String>{
    DEFAULT_SAFE_SELECT_FUNCTIONS.iter().map(|s| s.to_string()).collect()
}
fn db_type() -> String { "postgresql".to_string() }
fn db_schemas() -> Vec<String> { vec!["public".to_string()] }
fn role_claim_key() -> String { ".role".to_string() }
fn db_pool() -> usize { 10 }
fn to_tuple<'de, D>(deserializer: D) -> Result<Option<(String, String)>, D::Error>
where
    D: Deserializer<'de>,
{
    let o: Option<String> = Deserialize::deserialize(deserializer)?;
    Ok(match o {
        Some(s) => {
            let v: Vec<&str> = s.split('.').collect();
            match v[..] {
                [a, b] => Some((a.to_string(), b.to_string())),
                _ => Some(("".to_string(), s)),
            }
        }
        None => None,
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn deserialize_config() {
        let config = Config {
            vhosts: HashMap::from([(
                "domain.com".to_string(),
                VhostConfig {
                    url_prefix: None,
                    static_files_dir: None,
                    db_type: "postgresql".to_string(),
                    db_uri: "db_uri".to_string(),
                    db_schemas: vec!["db_schema".to_string()],
                    db_schema_structure: SchemaStructure::SqlFile("sql_file".to_string()),
                    db_anon_role: Some("anonymous".to_string()),
                    db_use_legacy_gucs: false,
                    db_tx_rollback: false,
                    db_pre_request: Some(("api".to_string(), "test".to_string())),
                    db_allowed_select_functions: vec![],
                    jwt_secret: None,
                    jwt_aud: None,
                    role_claim_key: ".role".to_string(),
                    db_pool: 10,
                    db_max_rows: None,
                },
            )]),
        };
        let json_config = r#"
        {"vhosts":{"domain_com":{"db_uri":"db_uri","db_schemas":["db_schema"],"db_schema_structure":{"sql_file":"sql_file"}, "db_anon_role": "anonymous", "db_pre_request": "api.test", "db_allowed_select_functions": []}}}
        "#;

        let deserialized_result = serde_json::from_str::<Config>(json_config);

        println!("deserialized_result = {:?}", deserialized_result);

        assert_eq!(deserialized_result.map_err(|e| format!("{}", e)), Ok(config));
        // let serialized_result = serde_json::to_string(&config);
        // println!("serialized_result = {:?}", serialized_result);
        //let serialized = serialized_result.unwrap_or("failed to serialize".to_string());
    }
}
