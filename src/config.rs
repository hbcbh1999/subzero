
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[serde(rename_all(serialize = "snake_case", deserialize = "snake_case"))]
pub enum SchemaStructure {
    SqlFile (String),
    JsonFile (String),
    JsonString (String)
}
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Config {
    pub db_uri: String,
    pub db_schemas: Vec<String>,
    pub db_schema_structure: SchemaStructure
}

#[cfg(test)]
mod test {
    use pretty_assertions::{assert_eq};
    use super::*;

    #[test]
    fn deserialize(){
        let config = Config {
            db_uri: "db_uri".to_string(),
            db_schemas: vec!["db_schema".to_string()],
            db_schema_structure: SchemaStructure::SqlFile("sql_file".to_string())
        };
        let json_config = r#"
        {"db_uri":"db_uri","db_schemas":["db_schema"],"db_schema_structure":{"sql_file":"sql_file"}}
        "#;


        let deserialized_result  = serde_json::from_str::<Config>(json_config);
        
        println!("deserialized_result = {:?}", deserialized_result);

        assert_eq!(deserialized_result.map_err(|e| format!("{}",e)), Ok(config));

        // let serialized_result = serde_json::to_string(&config);
        // println!("serialized_result = {:?}", serialized_result);
        //let serialized = serialized_result.unwrap_or("failed to serialize".to_string());
    }
}
