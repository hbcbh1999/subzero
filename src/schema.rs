use serde::{Deserialize, Deserializer};
use std::collections::HashMap;
use crate::api::{ForeignKey, Qi, ProcParam};

#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct DbSchema {
    #[serde(with = "schemas")]
    pub schemas: HashMap<String, Schema>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Schema {
    pub name: String,
    #[serde(with = "objects")]
    pub objects: HashMap<String, Object>
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Object {
    pub kind: ObjectType,
    pub name: String,
    #[serde(with = "columns")]
    pub columns: HashMap<String, Column>,
    #[serde(with = "foreign_keys", default)]
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ObjectDef {
    //common files
    pub kind: String,
    pub name: String,
    #[serde(with = "columns", default)]
    pub columns: HashMap<String, Column>,
    #[serde(with = "foreign_keys", default)]
    pub foreign_keys: Vec<ForeignKey>,

    //fields for functions
    #[serde(default)]
    pub volatile: char,
    #[serde(default)]
    pub composite: bool,
    #[serde(default)]
    pub setof: bool,
    #[serde(default)]
    pub return_type: String,
    #[serde(default = "pg_catalog")]
    pub return_type_schema: String,
    #[serde(default, deserialize_with = "vec_procparam")]
    parameters: Vec<ProcParam>,
}


#[derive(Deserialize)]
#[serde(remote = "ProcParam")]
struct ProcParamDef {
    name: String,
    #[serde(alias = "type")]
    type_: String,
    required: bool,
    variadic: bool,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum ProcVolatility {Imutable, Stable, Volatile}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum ProcReturnType {
    One (PgType),
    SetOf (PgType),
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum PgType {
    Scalar,
    Composite (Qi)
}



#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum ObjectType { 
    #[serde(rename = "view")]
    View,

    #[serde(rename = "table")]
    Table,

    #[serde(rename = "function")]
    Function {
        volatile: ProcVolatility,
        return_type: ProcReturnType,
        #[serde(deserialize_with = "vec_procparam")]
        parameters: Vec<ProcParam>,
    },
}

fn vec_procparam<'de, D>(deserializer: D) -> Result<Vec<ProcParam>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper(#[serde(with = "ProcParamDef")] ProcParam);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(a)| a).collect())
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ForeignKeyDef {
    name: String,
    table: Qi,
    columns: Vec<String>,
    referenced_table: Qi,
    referenced_columns: Vec<String>
}



#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Column {
    #[serde(default)]
    pub name: String,
    pub data_type: String,
    // #[serde(default, skip_serializing_if = "is_default")]
    #[serde(default)]
    pub primary_key: bool,
}


// code for deserialization

mod schemas {
    use super::Schema;

    use std::collections::HashMap;

    //use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    // pub fn serialize<S>(map: &HashMap<String, Schema>, serializer: S) -> Result<S::Ok, S::Error>
    //     where S: Serializer
    // {
    //     serializer.collect_seq(map.values())
    // }


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

mod objects {
    use crate::api::Qi;

    use super::{Object, ObjectDef, ObjectType, ProcVolatility, ProcReturnType::*, PgType::*};

    use std::collections::HashMap;

    //use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    // pub fn serialize<S>(map: &HashMap<String, Object>, serializer: S) -> Result<S::Ok, S::Error>
    //     where S: Serializer
    // {
    //     serializer.collect_seq(map.values())
    // }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Object>, D::Error>
        where D: Deserializer<'de>
    {
        let mut map = HashMap::new();
        for o in Vec::<ObjectDef>::deserialize(deserializer)? {
            
            map.insert(o.name.clone(), match o.kind.as_str() {
                "function" => {
                    Object {
                        kind: ObjectType::Function {
                            volatile: match o.volatile{
                                'i' => ProcVolatility::Imutable,
                                's' => ProcVolatility::Stable,
                                _ => ProcVolatility::Volatile
                            },
                            return_type: match (o.setof, o.composite) {
                                (true,true) => SetOf(Composite(Qi(o.return_type_schema, o.return_type))),
                                (true,false) =>SetOf(Scalar),
                                (false,true) =>One(Composite(Qi(o.return_type_schema, o.return_type))),
                                (false,false) =>One(Scalar),
                            },
                            parameters: o.parameters,
                        },
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                    }
                }
                "view" => {
                    Object {
                        kind: ObjectType::View,
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                    }
                },
                _ => {
                    Object {
                        kind: ObjectType::Table,
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                    }
                },

            });
        }
        Ok(map)
    }
}

mod foreign_keys {
    use super::{ForeignKeyDef, ForeignKey};

    //use std::collections::HashMap;

    //use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    // pub fn serialize<S>(v: &Vec<ForeignKey>, serializer: S) -> Result<S::Ok, S::Error>
    //     where S: Serializer
    // {
    //     serializer.collect_seq(v.iter().map(|f| 
    //         ForeignKeyDef {
    //             name: f.name.clone(),
    //             table: f.table.clone(),
    //             columns: f.columns.clone(),
    //             referenced_table: f.referenced_table.clone(),
    //             referenced_columns: f.referenced_columns.clone(),
    //         }
    //     ))
    // }


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

    //use serde::ser::Serializer;
    use serde::de::{Deserialize, Deserializer};
    // pub fn serialize<S>(map: &HashMap<String, Column>, serializer: S) -> Result<S::Ok, S::Error>
    //     where S: Serializer
    // {
    //     serializer.collect_seq(map.values())
    // }


    pub fn deserialize<'de, D>(deserializer: D) -> Result<HashMap<String, Column>, D::Error> where D: Deserializer<'de>
    {
        let mut map = HashMap::new();
        for column in Vec::<Column>::deserialize(deserializer)? {
            map.insert(column.name.clone(), column);
        }
        Ok(map)
    }
}

// fn is_default<T: Default + PartialEq>(t: &T) -> bool {
//     t == &T::default()
// }

fn pg_catalog() -> String {
    "pg_catalog".to_string()
}





#[cfg(test)]
mod tests {
    //use std::collections::HashSet;
    use pretty_assertions::{assert_eq};
    use super::*;
    use super::{ObjectType::*, ProcParam};
    fn s(s:&str) -> String {
        s.to_string()
    }
    fn t<T>((k,v):(&str, T)) -> (String, T) {
        (k.to_string(), v)
    }
    #[test]
    fn deserialize_db_schema(){
        let db_schema = DbSchema {
            schemas: [
                ("api", Schema {
                    name: s("api"),
                    objects: [
                        ("myfunction", Object {
                            kind: Function {
                                volatile: ProcVolatility::Volatile,
                                return_type: ProcReturnType::SetOf(PgType::Scalar),
                                parameters: vec![
                                    ProcParam {
                                        name: s("a"),
                                        type_: s("integer"),
                                        required: true,
                                        variadic: false,
                                    }
                                ],
                            },
                            name: s("myfunction"),
                            columns: [].iter().cloned().map(t).collect(),
                            foreign_keys: [].iter().cloned().collect()
                        }),
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
                        }),
                        ("projects", Object {
                            kind: Table,
                            name: s("projects"),
                            columns: [
                                ("id", Column {
                                    name: s("id"),
                                    data_type: s("int"),
                                    primary_key: true,
                                })
                            ].iter().cloned().map(t).collect(),
                            foreign_keys: [].iter().cloned().collect()
                        }),
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
                                "kind":"function",
                                "name":"myfunction",
                                "volatile":"v",
                                "composite":false,
                                "setof":true,
                                "return_type":"int4",
                                "return_type_schema":"pg_catalog",
                                "parameters":[
                                    {
                                        "name":"a",
                                        "type":"integer",
                                        "required":true,
                                        "variadic":false
                                    }
                                ]
                            },
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
                            },
                            {
                                "kind":"table",
                                "name":"projects",
                                "columns":[
                                    {
                                        "name":"id",
                                        "data_type":"int",
                                        "primary_key":true
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
        
        // let serialized_result = serde_json::to_string(&db_schema);
        // println!("serialized_result = {:?}", serialized_result);
        // let serialized = serialized_result.unwrap_or(s("failed to serialize"));
        // assert_eq!(serde_json::from_str::<serde_json::Value>(serialized.as_str()).unwrap(), serde_json::from_str::<serde_json::Value>(json_schema).unwrap());
    }

    // #[test]
    // fn hashset(){
    //     assert_eq!(HashSet::from([&"Einar", &"Olaf", &"Harald"]), HashSet::from([&"Olaf", &"Einar",  &"Harald"]));
    // }

}