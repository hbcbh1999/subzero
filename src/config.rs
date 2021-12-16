
use std::collections::HashMap;

use serde::{Deserialize, Serialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all(serialize = "snake_case", deserialize = "snake_case"))]
pub enum SchemaStructure {
    SqlFile (String),
    JsonFile (String),
    JsonString (String)
}
impl Default for SchemaStructure {
    fn default() -> Self { SchemaStructure::SqlFile ("structure_query.sql".to_string()) }
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct  Config {
    pub vhosts: HashMap<String, VhostConfig>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VhostConfig {
    pub db_uri: String,
    pub db_schemas: Vec<String>,
    #[serde(default)]
    pub db_schema_structure: SchemaStructure,
    pub db_anon_role: String,
    #[serde(deserialize_with = "to_tuple", default)]
    pub db_pre_request: Option<(String, String)>,
    pub jwt_secret: Option<String>,
    pub jwt_aud: Option<String>,
    #[serde(default = "role_claim_key")]
    pub role_claim_key: String,
}

fn role_claim_key() -> String {".role".to_string()}
fn to_tuple<'de, D>(deserializer: D) -> Result<Option<(String, String)>, D::Error> where D: Deserializer<'de> {
    let o: Option<String> = Deserialize::deserialize(deserializer)?;
    Ok(match o {
        Some(s) => {
            let v:Vec<&str> = s.split('.').collect();
            match v[..] {
                [a, b] => Some((a.to_string(),b.to_string())),
                _ => Some(("".to_string(), s))
            }
        }
        None => None
    })
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use pretty_assertions::{assert_eq};
    use super::*;

    #[test]
    fn deserialize_config(){
        let config = Config{
            vhosts : HashMap::from([("default".to_string(),
            VhostConfig {
                db_uri: "db_uri".to_string(),
                db_schemas: vec!["db_schema".to_string()],
                db_schema_structure: SchemaStructure::SqlFile("sql_file".to_string()),
                db_anon_role: "anonymous".to_string(),
                db_pre_request: Some(("api".to_string(), "test".to_string())),
                jwt_secret: None,
                jwt_aud: None,
                role_claim_key: ".role".to_string(),
            })])
        };
        let json_config = r#"
        {"vhosts":{"default":{"db_uri":"db_uri","db_schemas":["db_schema"],"db_schema_structure":{"sql_file":"sql_file"}, "db_anon_role": "anonymous", "db_pre_request": "api.test"}}}
        "#;


        let deserialized_result  = serde_json::from_str::<Config>(json_config);
        
        println!("deserialized_result = {:?}", deserialized_result);

        assert_eq!(deserialized_result.map_err(|e| format!("{}",e)), Ok(config));

        // let serialized_result = serde_json::to_string(&config);
        // println!("serialized_result = {:?}", serialized_result);
        //let serialized = serialized_result.unwrap_or("failed to serialize".to_string());
    }
}
