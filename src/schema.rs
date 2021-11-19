use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::api::{ForeignKey, Qi, };

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone)]
pub struct DbSchema {
    #[serde(with = "schemas")]
    pub schemas: HashMap<String, Schema>,
}
mod schemas {
    use super::Schema;

    use std::collections::HashMap;

    use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    pub fn serialize<S>(map: &HashMap<String, Schema>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.collect_seq(map.values())
    }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Schema>, D::Error>
        where D: Deserializer<'de>
    {
        let mut map = HashMap::new();
        for schema in Vec::<Schema>::deserialize(deserializer)? {
            map.insert(schema.name.clone(), schema);
        }
        Ok(map)
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Schema {
    pub name: String,
    #[serde(with = "objects")]
    pub objects: HashMap<String, Object>
}
mod objects {
    use super::Object;

    use std::collections::HashMap;

    use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    pub fn serialize<S>(map: &HashMap<String, Object>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.collect_seq(map.values())
    }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Object>, D::Error>
        where D: Deserializer<'de>
    {
        let mut map = HashMap::new();
        for object in Vec::<Object>::deserialize(deserializer)? {
            map.insert(object.name.clone(), object);
        }
        Ok(map)
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Object {
    pub kind: ObjectType,
    pub name: String,
    #[serde(with = "columns")]
    pub columns: HashMap<String, Column>,
    #[serde(with = "foreign_keys")]
    pub foreign_keys: Vec<ForeignKey>,
}

mod foreign_keys {
    use super::{ForeignKeyDef, ForeignKey};

    //use std::collections::HashMap;

    use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    pub fn serialize<S>(v: &Vec<ForeignKey>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.collect_seq(v.iter().map(|f| 
            ForeignKeyDef {
                name: f.name.clone(),
                table: f.table.clone(),
                columns: f.columns.clone(),
                referenced_table: f.referenced_table.clone(),
                referenced_columns: f.referenced_columns.clone(),
            }
        ))
    }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<ForeignKey>, D::Error>
        where D: Deserializer<'de>
    {
        let mut v = vec![];
        for foreign_key in Vec::<ForeignKeyDef>::deserialize(deserializer)? {
            v.push(ForeignKey {
                name: foreign_key.name,
                table: foreign_key.table,
                columns: foreign_key.columns,
                referenced_table: foreign_key.referenced_table,
                referenced_columns: foreign_key.referenced_columns,
            });
        }
        Ok(v)
    }
}

mod columns {
    use super::Column;

    use std::collections::HashMap;

    use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    pub fn serialize<S>(map: &HashMap<String, Column>, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        serializer.collect_seq(map.values())
    }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Column>, D::Error>
        where D: Deserializer<'de>
    {
        let mut map = HashMap::new();
        for column in Vec::<Column>::deserialize(deserializer)? {
            map.insert(column.name.clone(), column);
        }
        Ok(map)
    }
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ForeignKeyDef {
    name: String,
    table: Qi,
    columns: Vec<String>,
    referenced_table: Qi,
    referenced_columns: Vec<String>
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum ObjectType { 
    #[serde(rename = "view")]
    View,

    #[serde(rename = "table")]
    Table
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Column {
    #[serde(default)]
    pub name: String,
    pub data_type: String,
    #[serde(default, skip_serializing_if = "is_default")]
    pub primary_key: bool,
}

fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    t == &T::default()
}





#[cfg(test)]
mod tests {
    //use std::collections::HashSet;
    use pretty_assertions::{assert_eq};
    use super::*;
    use super::ObjectType::*;
    fn s(s:&str) -> String {
        s.to_string()
    }
    fn t<T>((k,v):(&str, T)) -> (String, T) {
        (k.to_string(), v)
    }
    #[test]
    fn deserialize(){
        let db_schema = DbSchema {
            schemas: [
                ("api", Schema {
                    name: s("api"),
                    objects: [
                        ("tasks", Object {
                            kind: View,
                            name: s("tasks"),
                            columns: [
                                ("id", Column {
                                    name: s("id"),
                                    data_type: s("int"),
                                    primary_key: true,
                                }),
                                ("name", Column {
                                    name: s("name"),
                                    data_type: s("text"),
                                    primary_key: false,
                                })
                            ].iter().cloned().map(t).collect(),
                            foreign_keys: [
                                ForeignKey {
                                    name: s("project_id_fk"),
                                    table: Qi(s("api"),s("tasks")),
                                    columns: vec![s("project_id")],
                                    referenced_table: Qi(s("api"),s("projects")),
                                    referenced_columns:  vec![s("id")],
                                }
                            ].iter().cloned().collect()
                        })
                    ].iter().cloned().map(t).collect()
                })
            ].iter().cloned().map(t).collect()
        };

        let json_schema = r#"
            {
                "schemas":[
                    {
                        "name":"api",
                        "objects":[
                            {
                                "kind":"view",
                                "name":"tasks",
                                "columns":[
                                    {
                                        "name":"id",
                                        "data_type":"int",
                                        "primary_key":true
                                    },
                                    {
                                        "name":"name",
                                        "data_type":"text"
                                    }
                                ],
                                "foreign_keys":[
                                    {
                                        "name":"project_id_fk",
                                        "table":["api","tasks"],
                                        "columns": ["project_id"],
                                        "referenced_table":["api","projects"],
                                        "referenced_columns": ["id"]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        "#;

       
        let deserialized_result  = serde_json::from_str::<DbSchema>(json_schema);

        println!("deserialized_result = {:?}", deserialized_result);

        let deserialized  = deserialized_result.unwrap_or(DbSchema {schemas: HashMap::new()});

        assert_eq!(deserialized, db_schema);
        
        let serialized_result = serde_json::to_string(&db_schema);
        println!("serialized_result = {:?}", serialized_result);
        let serialized = serialized_result.unwrap_or(s("failed to serialize"));
        assert_eq!(serde_json::from_str::<serde_json::Value>(serialized.as_str()).unwrap(), serde_json::from_str::<serde_json::Value>(json_schema).unwrap());
    }

    // #[test]
    // fn hashset(){
    //     assert_eq!(HashSet::from([&"Einar", &"Olaf", &"Harald"]), HashSet::from([&"Olaf", &"Einar",  &"Harald"]));
    // }

}