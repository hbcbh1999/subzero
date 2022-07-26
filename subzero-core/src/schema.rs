use crate::api::{ForeignKey, Join, Join::*, ProcParam, Qi, ColumnName, Condition};
use crate::error::*;
use serde::{Deserialize, Deserializer};
use snafu::OptionExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;
//use serde_json::{Value as JsonValue};



type HttpMethod = String;
type Permissions<T> = HashMap<(Role, HttpMethod), Vec<T>>;

#[derive(Debug, PartialEq, Deserialize, Clone)]
pub struct DbSchema {
    #[serde(deserialize_with = "deserialize_schemas")]
    pub schemas: HashMap<String, Schema>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Schema {
    pub name: String,
    #[serde(deserialize_with = "deserialize_objects")]
    pub objects: BTreeMap<String, Object>,
    #[serde(default)]
    join_tables: BTreeMap<(String, String), BTreeSet<String>>,
}

impl DbSchema {
    pub fn get_join(&self, current_schema: &String, origin: &String, target: &String, hint: &Option<String>) -> Result<Join> {
        // let match_fk = | s: &String, t:&String, n:&String | {
        //     | fk: &&ForeignKey |{
        //         &fk.name == n && &fk.referenced_table.0 == s
        //     }
        // };
        let schema = self.schemas.get(current_schema).context(UnacceptableSchema {
            schemas: vec![current_schema.to_owned()],
        })?;

        let origin_table = schema.objects.get(origin).context(UnknownRelation { relation: origin.to_owned() })?;

        match origin_table
            .foreign_keys
            .iter()
            .find(|&fk| &fk.name == target && &fk.referenced_table.0 == current_schema)
        {
            // the target is a foreign key name
            // projects?select=projects_client_id_fkey(*)
            // TODO! when views are involved there may be multiple fks with the same name
            Some(fk) => {
                
                if origin == &fk.table.1 {
                    Ok(Parent(fk.clone()))
                } else {
                    Ok(Child(fk.clone()))
                }
            }
            None => {
                match schema.objects.get(target) {
                    // the target is an existing table
                    Some(target_table) => {
                        
                        match hint {
                            Some(h) => {
                                // projects?select=clients!projects_client_id_fkey(*)
                                if let Some(fk) = origin_table
                                    .foreign_keys
                                    .iter()
                                    .find(|&fk| &fk.name == h && &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target)
                                {
                                    return Ok(Parent(fk.clone()));
                                }
                                if let Some(fk) = target_table
                                    .foreign_keys
                                    .iter()
                                    .find(|&fk| &fk.name == h && &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin)
                                {
                                    return Ok(Child(fk.clone()));
                                }

                                // users?select=tasks!users_tasks(*)
                                if let Some(join_table) = schema.objects.get(h) {
                                    let ofk1 = join_table.foreign_keys.iter().find(|fk| {
                                        &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin
                                    });
                                    let ofk2 = join_table.foreign_keys.iter().find(|fk| {
                                        &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target
                                        
                                    });
                                    if let (Some(fk1), Some(fk2)) = (ofk1, ofk2) {
                                        return Ok(Many(Qi(current_schema.clone(), join_table.name.clone()), fk1.clone(), fk2.clone()));
                                    } else {
                                        return Err(Error::NoRelBetween {
                                            origin: origin.to_owned(),
                                            target: target.to_owned(),
                                        });
                                    }
                                }

                                let mut joins = vec![];
                                // projects?select=clients!client_id(*)
                                if origin_table != target_table {
                                    joins.extend(
                                        origin_table
                                            .foreign_keys
                                            .iter()
                                            .filter(|&fk| {
                                                &fk.referenced_table.0 == current_schema
                                                    && &fk.referenced_table.1 == target
                                                    && fk.columns.len() == 1
                                                    && (fk.columns.contains(h) || fk.referenced_columns.contains(h))
                                            })
                                            .map(|fk| Parent(fk.clone()))
                                            .collect::<Vec<_>>(),
                                    );
                                }

                                // projects?select=clients!id(*)
                                joins.extend(
                                    target_table
                                        .foreign_keys
                                        .iter()
                                        .filter(|&fk| {
                                            &fk.referenced_table.0 == current_schema
                                                && &fk.referenced_table.1 == origin
                                                && fk.columns.len() == 1
                                                && (fk.columns.contains(h) || fk.referenced_columns.contains(h))
                                        })
                                        .map(|fk| Child(fk.clone()))
                                        .collect::<Vec<_>>(),
                                );

                                
                                if joins.len() == 1 {
                                    Ok(joins[0].clone())
                                } else if joins.is_empty() {
                                    Err(Error::NoRelBetween {
                                        origin: origin.to_owned(),
                                        target: target.to_owned(),
                                    })
                                } else {
                                    Err(Error::AmbiguousRelBetween {
                                        origin: origin.to_owned(),
                                        target: target.to_owned(),
                                        relations: joins,
                                    })
                                }

                                //Ok(joins)
                            }
                            // there is no hint, look for foreign keys between the two tables
                            None => {
                                // check child relations
                                // projects?select=tasks(*)
                                let child_joins = target_table
                                    .foreign_keys
                                    .iter()
                                    .filter(|&fk| &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin)
                                    .map(|fk| Child(fk.clone()))
                                    .collect::<Vec<_>>();

                                // check parent relations
                                // projects?select=clients(*)
                                let parent_joins = origin_table
                                    .foreign_keys
                                    .iter()
                                    .filter(|&fk| {
                                        &fk.referenced_table.0 == current_schema
                                            && &fk.referenced_table.1 == target
                                            && fk.table != fk.referenced_table
                                    })
                                    .map(|fk| Parent(fk.clone()))
                                    .collect::<Vec<_>>();

                                // check many to many relations
                                // users?select=tasks(*)
                                let many_joins = match schema.join_tables.get(&(origin.clone(), target.clone())) {
                                    None => vec![],
                                    Some(jt) => jt
                                        .iter()
                                        .filter_map(|t| schema.objects.get(t))
                                        .flat_map(|join_table| {
                                            let fks1 = join_table
                                                .foreign_keys
                                                .iter()
                                                .filter(|fk| {
                                                    &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == origin
                                                })
                                                .collect::<Vec<_>>();
                                            let fks2 = join_table
                                                .foreign_keys
                                                .iter()
                                                .filter(|fk| {
                                                    &fk.referenced_table.0 == current_schema && &fk.referenced_table.1 == target
                                                    
                                                })
                                                .collect::<Vec<_>>();
                                            let product = fks1
                                                .iter()
                                                .flat_map(|&fk1| {
                                                    fks2.iter().map(move |&fk2| {
                                                        Many(Qi(current_schema.clone(), join_table.name.clone()), fk1.clone(), fk2.clone())
                                                    })
                                                })
                                                .collect::<Vec<Join>>();
                                            product
                                        })
                                        .collect::<Vec<_>>(),
                                };

                                let mut joins = vec![];
                                joins.extend(child_joins);
                                joins.extend(parent_joins);
                                joins.extend(many_joins);

                                if joins.len() == 1 {
                                    Ok(joins[0].clone())
                                } else if joins.is_empty() {
                                    Err(Error::NoRelBetween {
                                        origin: origin.to_owned(),
                                        target: target.to_owned(),
                                    })
                                } else {
                                    Err(Error::AmbiguousRelBetween {
                                        origin: origin.to_owned(),
                                        target: target.to_owned(),
                                        relations: joins,
                                    })
                                }
                            }
                        }
                    }
                    // the target is not a table
                    None => {
                        // the target is a foreign key column
                        // projects?select=client_id(*)
                        let joins = origin_table
                            .foreign_keys
                            .iter()
                            .filter(|&fk| &fk.referenced_table.0 == current_schema && fk.columns.len() == 1 && fk.columns.contains(target))
                            .map(|fk| Parent(fk.clone()))
                            .collect::<Vec<_>>();
                        //Ok(joins)
                        if joins.len() == 1 {
                            Ok(joins[0].clone())
                        } else if joins.is_empty() {
                            Err(Error::NoRelBetween {
                                origin: origin.to_owned(),
                                target: target.to_owned(),
                            })
                        } else {
                            Err(Error::AmbiguousRelBetween {
                                origin: origin.to_owned(),
                                target: target.to_owned(),
                                relations: joins,
                            })
                        }
                    }
                }
            }
        }
    }
}

type Role = String;

#[derive(Debug, PartialEq, Clone)]
pub struct Object {
    pub kind: ObjectType,
    pub name: String,
    pub columns: HashMap<String, Column>,
    pub foreign_keys: Vec<ForeignKey>,
    pub column_level_permissions: Option<HashMap<(Role, HttpMethod), Vec<ColumnName>>>,
    pub row_level_permissions: Option<HashMap<(Role, HttpMethod), Vec<Condition>>>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ObjectDef {
    //common files
    pub kind: String,
    pub name: String,
    #[serde(deserialize_with = "deserialize_columns", default)]
    pub columns: HashMap<String, Column>,
    #[serde(deserialize_with = "deserialize_foreign_keys", default)]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(deserialize_with = "deserialize_column_level_permissions", default)]
    pub column_level_permissions: Option<HashMap<(Role, HttpMethod), Vec<ColumnName>>>,
    #[serde(deserialize_with = "deserialize_row_level_permissions", default)]
    pub row_level_permissions: Option<HashMap<(Role, HttpMethod), Vec<Condition>>>,

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
    #[serde(default, deserialize_with = "deserialize_vec_procparam")]
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
pub enum ProcVolatility {
    Imutable,
    Stable,
    Volatile,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum ProcReturnType {
    One(PgType),
    SetOf(PgType),
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub enum PgType {
    Scalar,
    Composite(Qi),
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
        #[serde(deserialize_with = "deserialize_vec_procparam")]
        parameters: Vec<ProcParam>,
    },
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ForeignKeyDef {
    name: String,
    table: Qi,
    columns: Vec<String>,
    referenced_table: Qi,
    referenced_columns: Vec<String>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct Column {
    #[serde(default)]
    pub name: ColumnName,
    pub data_type: String,
    // #[serde(default, skip_serializing_if = "is_default")]
    #[serde(default)]
    pub primary_key: bool,
}


fn deserialize_column_level_permissions<'de, D>(deserializer: D) -> Result<Option<Permissions<ColumnName>>, D::Error>
where D: Deserializer<'de>,
{
    let mut map = HashMap::new();
    let map_in = HashMap::<Role,HashMap::<HttpMethod,Vec::<ColumnName>>>::deserialize(deserializer);
    for (role, rules) in map_in? {
        for (method, columns) in rules {
            map.insert((role.clone(), method), columns);
        }
    }
    Ok(Some(map))
}

fn deserialize_row_level_permissions<'de, D>(deserializer: D) -> Result<Option<Permissions<Condition>>, D::Error>
where D: Deserializer<'de>,
{
    let mut map = HashMap::new();
    let map_in = HashMap::<Role,HashMap::<HttpMethod,Vec::<Condition>>>::deserialize(deserializer);
    for (role, rules) in map_in? {
        for (method, conditions) in rules {
            map.insert((role.clone(), method), conditions);
        }
    }
    Ok(Some(map))
}

fn deserialize_vec_procparam<'de, D>(deserializer: D) -> Result<Vec<ProcParam>, D::Error>
where D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper(#[serde(with = "ProcParamDef")] ProcParam);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(a)| a).collect())
}

fn deserialize_schemas<'de, D>(deserializer: D) -> Result<HashMap<String, Schema>, D::Error>
where D: Deserializer<'de>,
{
    let mut map = HashMap::new();
    for mut schema in Vec::<Schema>::deserialize(deserializer)? {
        let join_tables: BTreeMap<(String, String), Vec<String>> = schema
            .objects
            .iter()
            .flat_map(|(n, o)| match o.kind {
                ObjectType::Function { .. } => vec![],
                _ => o
                    .foreign_keys
                    .iter()
                    .flat_map(|fk1| {
                        o.foreign_keys
                            .iter()
                            .filter(|&fk2| fk2 != fk1 && fk1.referenced_table.0 == schema.name && fk2.referenced_table.0 == schema.name)
                            .flat_map(|fk2| {
                                vec![
                                    ((fk1.referenced_table.1.clone(), fk2.referenced_table.1.clone()), n.clone()),
                                    ((fk2.referenced_table.1.clone(), fk1.referenced_table.1.clone()), n.clone()),
                                ]
                            })
                            .collect::<Vec<((String, String), String)>>()
                    })
                    .collect::<Vec<((String, String), String)>>(),
            })
            .fold(BTreeMap::new(), |mut acc, (k, v)| {
                acc.entry(k).or_default().push(v);
                acc
            });
        for (k, v) in join_tables {
            schema.join_tables.insert(k, BTreeSet::from_iter(v.into_iter()));
        }
        map.insert(schema.name.clone(), schema);
    }
    Ok(map)
}

fn deserialize_objects<'de, D>(deserializer: D) -> Result<BTreeMap<String, Object>, D::Error>
where D: Deserializer<'de>,
{
    let mut map = BTreeMap::new();
    for o in Vec::<ObjectDef>::deserialize(deserializer)? {
        map.insert(
            o.name.clone(),
            match o.kind.as_str() {
                "function" => {
                    Object {
                        kind: ObjectType::Function {
                            volatile: match o.volatile {
                                'i' => ProcVolatility::Imutable,
                                's' => ProcVolatility::Stable,
                                _ => ProcVolatility::Volatile,
                            },
                            return_type: match (o.setof, o.composite) {
                                (true, true) => ProcReturnType::SetOf(PgType::Composite(Qi(o.return_type_schema, o.return_type))),
                                (true, false) => ProcReturnType::SetOf(PgType::Scalar),
                                (false, true) => ProcReturnType::One(PgType::Composite(Qi(o.return_type_schema, o.return_type))),
                                (false, false) => ProcReturnType::One(PgType::Scalar),
                            },
                            parameters: o.parameters,
                        },
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                        column_level_permissions: o.column_level_permissions,
                        row_level_permissions: None,
                        //join_for,
                    }
                }
                "view" => {
                    Object {
                        kind: ObjectType::View,
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                        column_level_permissions: o.column_level_permissions,
                        row_level_permissions: None,
                    }
                }
                _ => {
                    Object {
                        kind: ObjectType::Table,
                        name: o.name,
                        columns: o.columns,
                        foreign_keys: o.foreign_keys,
                        column_level_permissions: o.column_level_permissions,
                        row_level_permissions: o.row_level_permissions,
                    }
                }
            },
        );
    }
    Ok(map)
}

fn deserialize_foreign_keys<'de, D>(deserializer: D) -> Result<Vec<ForeignKey>, D::Error>
where D: Deserializer<'de>,
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

fn deserialize_columns<'de, D>(deserializer: D) -> Result<HashMap<String, Column>, D::Error>
where D: Deserializer<'de>,
{
    let mut map = HashMap::new();
    for column in Vec::<Column>::deserialize(deserializer)? {
        map.insert(column.name.clone(), column);
    }
    Ok(map)
}


// fn is_default<T: Default + PartialEq>(t: &T) -> bool {
//     t == &T::default()
// }

fn pg_catalog() -> String { "pg_catalog".to_string() }

#[cfg(test)]
mod tests {
    //use std::collections::HashSet;
    use super::*;
    use crate::api::{Field, Filter, SingleVal};
    use super::{ObjectType::*, ProcParam, };
    use crate::error::Error as AppError;
    use pretty_assertions::assert_eq;
    fn s(s: &str) -> String { s.to_string() }
    fn t<T>((k, v): (&str, T)) -> (String, T) { (k.to_string(), v) }
    #[test]
    fn deserialize_db_schema() {
        let db_schema = DbSchema {
            schemas: [(
                "api",
                Schema {
                    name: s("api"),
                    objects: [
                        (
                            "myfunction",
                            Object {
                                kind: Function {
                                    volatile: ProcVolatility::Volatile,
                                    return_type: ProcReturnType::SetOf(PgType::Scalar),
                                    parameters: vec![ProcParam {
                                        name: s("a"),
                                        type_: s("integer"),
                                        required: true,
                                        variadic: false,
                                    }],
                                },
                                name: s("myfunction"),
                                columns: [].iter().cloned().map(t).collect(),
                                foreign_keys: [].to_vec(),
                                column_level_permissions: None,
                                row_level_permissions: None,
                            },
                        ),
                        (
                            "tasks",
                            Object {
                                kind: View,
                                name: s("tasks"),
                                columns: [
                                    (
                                        "id",
                                        Column {
                                            name: s("id"),
                                            data_type: s("int"),
                                            primary_key: true,
                                        },
                                    ),
                                    (
                                        "name",
                                        Column {
                                            name: s("name"),
                                            data_type: s("text"),
                                            primary_key: false,
                                        },
                                    ),
                                ]
                                .iter()
                                .cloned()
                                .map(t)
                                .collect(),
                                foreign_keys: [ForeignKey {
                                    name: s("project_id_fk"),
                                    table: Qi(s("api"), s("tasks")),
                                    columns: vec![s("project_id")],
                                    referenced_table: Qi(s("api"), s("projects")),
                                    referenced_columns: vec![s("id")],
                                }].to_vec(),
                                column_level_permissions: None,
                                row_level_permissions: None,
                            },
                        ),
                        (
                            "projects",
                            Object {
                                kind: Table,
                                name: s("projects"),
                                columns: [(
                                    "id",
                                    Column {
                                        name: s("id"),
                                        data_type: s("int"),
                                        primary_key: true,
                                    },
                                )]
                                .iter()
                                .cloned()
                                .map(t)
                                .collect(),
                                foreign_keys: [].to_vec(),
                                column_level_permissions: Some(
                                    vec![
                                        (
                                            (s("role"), s("get")),
                                            vec![s("id"),s("name"),
                                            ],
                                        ),
                                    ]
                                    .iter().cloned().collect(),
                                ),
                                //row_level_permissions: None,
                                row_level_permissions: Some(
                                    vec![
                                        (
                                            (s("role"), s("get")),
                                            vec![
                                                Condition::Single{
                                                    field: Field{name:s("id"), json_path:None},
                                                    filter: Filter::Op(s("eq"), SingleVal(s("10"),Some(s("int")))),
                                                    negate:false,
                                                }
                                            ],
                                        ),
                                    ]
                                    .iter().cloned().collect(),
                                ),
                            },
                        ),
                    ]
                    .iter()
                    .cloned()
                    .map(t)
                    .collect(),
                    join_tables: [].iter().cloned().collect(),
                },
            )]
            .iter()
            .cloned()
            .map(t)
            .collect(),
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
                                ],
                                "foreign_keys":[],
                                "column_level_permissions":{
                                    "role": {
                                        "get": ["id","name"]
                                    }
                                },
                                "row_level_permissions": {
                                    "role": {
                                        "get": [
                                            {"single":{"field":{"name":"id"},"filter":{"op":["eq",["10","int"]]}}}
                                        ]
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
        "#;

        let deserialized_result = serde_json::from_str::<DbSchema>(json_schema);

        println!("deserialized_result = {:?}", deserialized_result);

        let deserialized = deserialized_result.unwrap_or(DbSchema { schemas: HashMap::new() });

        assert_eq!(deserialized, db_schema);

        // let serialized_result = serde_json::to_string(&db_schema);
        // println!("serialized_result = {:?}", serialized_result);
        // let serialized = serialized_result.unwrap_or(s("failed to serialize"));
        // assert_eq!(serde_json::from_str::<serde_json::Value>(serialized.as_str()).unwrap(), serde_json::from_str::<serde_json::Value>(json_schema).unwrap());
        
        //a sample sqlite schema
        let ss = r#"{
            "schemas":[
                {
                    "name":"_sqlite_public_",
                    "objects":[
                        {"kind":"table","name":"tbl1","columns":[{"name":"one","data_type":"varchar(10)","primary_key":false},{"name":"two","data_type":"smallint","primary_key":false}],"foreign_keys":[]},
                        {"kind":"table","name":"clients","columns":[{"name":"id","data_type":"INTEGER","primary_key":true},{"name":"name","data_type":"TEXT","primary_key":false}],"foreign_keys":[]},
                        {"kind":"table","name":"projects","columns":[{"name":"id","data_type":"INTEGER","primary_key":true},{"name":"name","data_type":"TEXT","primary_key":false},{"name":"client_id","data_type":"INTEGER","primary_key":false}],"foreign_keys":[{"name":"projects_client_id_fkey","table":["_sqlite_public_","projects"],"columns":["client_id"],"referenced_table":["_sqlite_public_","clients"],"referenced_columns":["id"]}]},{"kind":"view","name":"projects_view","columns":[{"name":"id","data_type":"INTEGER","primary_key":false},{"name":"name","data_type":"TEXT","primary_key":false},{"name":"client_id","data_type":"INTEGER","primary_key":false}],"foreign_keys":[]},
                        {"kind":"table","name":"tasks","columns":[{"name":"id","data_type":"INTEGER","primary_key":true},{"name":"name","data_type":"TEXT","primary_key":false},{"name":"project_id","data_type":"INTEGER","primary_key":false}],"foreign_keys":[{"name":"tasks_project_id_fkey","table":["_sqlite_public_","tasks"],"columns":["project_id"],"referenced_table":["_sqlite_public_","projects"],"referenced_columns":["id"]}]},
                        {"kind":"table","name":"users","columns":[{"name":"id","data_type":"INTEGER","primary_key":true},{"name":"name","data_type":"TEXT","primary_key":false}],"foreign_keys":[]},
                        {"kind":"table","name":"users_tasks","columns":[{"name":"user_id","data_type":"INTEGER","primary_key":false},{"name":"task_id","data_type":"INTEGER","primary_key":true}],"foreign_keys":[{"name":"users_tasks_task_id_fkey","table":["_sqlite_public_","users_tasks"],"columns":["task_id"],"referenced_table":["_sqlite_public_","tasks"],"referenced_columns":["id"]},{"name":"users_tasks_user_id_fkey","table":["_sqlite_public_","users_tasks"],"columns":["user_id"],"referenced_table":["_sqlite_public_","users"],"referenced_columns":["id"]}]},
                        {"kind":"table","name":"complex_items","columns":[{"name":"id","data_type":"INTEGER","primary_key":false},{"name":"name","data_type":"TEXT","primary_key":false},{"name":"settings","data_type":"TEXT","primary_key":false}],"foreign_keys":[]}
                    ]
                }
            ]
        }"#;
        let deserialized_result = serde_json::from_str::<DbSchema>(ss);
        println!("deserialized_result = {:?}", deserialized_result);
        assert!(deserialized_result.is_ok());
    }

    // #[test]
    // fn hashset(){
    //     assert_eq!(HashSet::from([&"Einar", &"Olaf", &"Harald"]), HashSet::from([&"Olaf", &"Einar",  &"Harald"]));
    // }

    #[test]
    fn test_get_join_conditions() {
        static JSON_SCHEMA: &str = r#"
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
                                        "setof":false,
                                        "return_type":"int4",
                                        "return_type_schema":"pg_catalog",
                                        "parameters":[
                                            {
                                                "name":"id",
                                                "type":"integer",
                                                "required":true,
                                                "variadic":false
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"addresses",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"location", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"users",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" },
                                            { "name":"billing_address_id", "data_type":"int" },
                                            { "name":"shipping_address_id", "data_type":"int" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"billing_address_id_fk",
                                                "table":["api","users"],
                                                "columns": ["billing_address_id"],
                                                "referenced_table":["api","addresses"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"shipping_address_id_fk",
                                                "table":["api","users"],
                                                "columns": ["shipping_address_id"],
                                                "referenced_table":["api","addresses"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"clients",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"projects",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"client_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"client_id_fk",
                                                "table":["api","projects"],
                                                "columns": ["client_id"],
                                                "referenced_table":["api","clients"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    },
                                    {
                                        "kind":"view",
                                        "name":"tasks",
                                        "columns":[
                                            { "name":"id", "data_type":"int", "primary_key":true },
                                            { "name":"project_id", "data_type":"int" },
                                            { "name":"name", "data_type":"text" }
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
                                        "kind":"view",
                                        "name":"users_tasks",
                                        "columns":[
                                            { "name":"task_id", "data_type":"int", "primary_key":true },
                                            { "name":"user_id", "data_type":"int", "primary_key":true }
                                            
                                        ],
                                        "foreign_keys":[
                                            {
                                                "name":"task_id_fk",
                                                "table":["api","users_tasks"],
                                                "columns": ["task_id"],
                                                "referenced_table":["api","tasks"],
                                                "referenced_columns": ["id"]
                                            },
                                            {
                                                "name":"user_id_fk",
                                                "table":["api","users_tasks"],
                                                "columns": ["user_id"],
                                                "referenced_table":["api","users"],
                                                "referenced_columns": ["id"]
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                "#;
        let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).unwrap();
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("projects"), &s("tasks"), &None)
                .map_err(|e| format!("{}", e)),
            Ok(Child(ForeignKey {
                name: s("project_id_fk"),
                table: Qi(s("api"), s("tasks")),
                columns: vec![s("project_id")],
                referenced_table: Qi(s("api"), s("projects")),
                referenced_columns: vec![s("id")],
            }))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("tasks"), &s("projects"), &None)
                .map_err(|e| format!("{}", e)),
            Ok(Parent(ForeignKey {
                name: s("project_id_fk"),
                table: Qi(s("api"), s("tasks")),
                columns: vec![s("project_id")],
                referenced_table: Qi(s("api"), s("projects")),
                referenced_columns: vec![s("id")],
            }))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("clients"), &s("projects"), &None)
                .map_err(|e| format!("{}", e)),
            Ok(Child(ForeignKey {
                name: s("client_id_fk"),
                table: Qi(s("api"), s("projects")),
                columns: vec![s("client_id")],
                referenced_table: Qi(s("api"), s("clients")),
                referenced_columns: vec![s("id")],
            }))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("tasks"), &s("users"), &None)
                .map_err(|e| format!("{}", e)),
            Ok(Many(
                Qi(s("api"), s("users_tasks")),
                ForeignKey {
                    name: s("task_id_fk"),
                    table: Qi(s("api"), s("users_tasks")),
                    columns: vec![s("task_id")],
                    referenced_table: Qi(s("api"), s("tasks")),
                    referenced_columns: vec![s("id")],
                },
                ForeignKey {
                    name: s("user_id_fk"),
                    table: Qi(s("api"), s("users_tasks")),
                    columns: vec![s("user_id")],
                    referenced_table: Qi(s("api"), s("users")),
                    referenced_columns: vec![s("id")],
                },
            ))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("tasks"), &s("users"), &Some(s("users_tasks")))
                .map_err(|e| format!("{}", e)),
            Ok(Many(
                Qi(s("api"), s("users_tasks")),
                ForeignKey {
                    name: s("task_id_fk"),
                    table: Qi(s("api"), s("users_tasks")),
                    columns: vec![s("task_id")],
                    referenced_table: Qi(s("api"), s("tasks")),
                    referenced_columns: vec![s("id")],
                },
                ForeignKey {
                    name: s("user_id_fk"),
                    table: Qi(s("api"), s("users_tasks")),
                    columns: vec![s("user_id")],
                    referenced_table: Qi(s("api"), s("users")),
                    referenced_columns: vec![s("id")],
                },
            ))
        );

        // let result = get_join(&s("api"), &db_schema, &s("users"), &s("addresses"), &mut None);
        // let expected = AppError::AmbiguousRelBetween {
        //     origin: s("users"), target: s("addresses"),
        //     relations: vec![
        //         Parent(
        //             ForeignKey {
        //                 name: s("billing_address_id_fk"),
        //                 table: Qi(s("api"),s("users")),
        //                 columns: vec![
        //                     s("billing_address_id"),
        //                 ],
        //                 referenced_table: Qi(s("api"),s("addresses")),
        //                 referenced_columns: vec![
        //                     s("id"),
        //                 ],
        //             },
        //         ),
        //         Parent(
        //             ForeignKey {
        //                 name: s("shipping_address_id_fk"),
        //                 table: Qi(s("api"),s("users")),
        //                 columns: vec![
        //                     s("shipping_address_id"),
        //                 ],
        //                 referenced_table: Qi(s("api"),s("addresses")),
        //                 referenced_columns: vec![
        //                     s("id"),
        //                 ],
        //             },
        //         ),
        //     ]
        // };
        // assert!(result.is_err());
        // let error = result.unwrap();

        // assert!(matches!(
        //     get_join(&s("api"), &db_schema, &s("users"), &s("addresses"), &mut None),
        //     1
        // );
        assert!(matches!(
            db_schema.get_join(&s("api"), &s("users"), &s("addresses"), &None),
            Err(AppError::AmbiguousRelBetween { .. })
        ));
    }
}
