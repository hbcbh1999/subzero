use crate::api::{ForeignKey, Join, Join::*, ProcParam, Qi, Condition};
use crate::error::*;
use serde::{Deserialize, Deserializer, Serialize};
use snafu::OptionExt;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::iter::FromIterator;
use log::debug;
use ColumnPermissions::*;
//use std::borrow::Cow;

pub type Role<'a> = &'a str;
#[derive(Debug, Eq, PartialEq, Hash, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Execute,
    Select,
    Insert,
    Update,
    Delete,
    All,
    Merge,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum ColumnPermissions<'a> {
    All,
    #[serde(borrow)]
    Specific(Vec<&'a str>),
}
impl<'a> Default for ColumnPermissions<'a> {
    fn default() -> Self { ColumnPermissions::All }
}

#[derive(Debug, PartialEq, Eq, Clone, Default, Serialize, Deserialize)]
pub struct Permissions<'a> {
    #[serde(borrow)]
    pub grants: HashMap<(Role<'a>, Action), ColumnPermissions<'a>>,
    #[serde(borrow)]
    pub policies: HashMap<(Role<'a>, Action), Vec<Policy<'a>>>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Policy<'a> {
    pub name: Option<&'a str>,
    pub restrictive: bool,
    #[serde(borrow)]
    pub using: Option<Vec<Condition<'a>>>,
    #[serde(borrow)]
    pub check: Option<Vec<Condition<'a>>>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct PermissionDef<'a> {
    #[serde(borrow)]
    pub role: Role<'a>,

    #[serde(borrow, default, skip_serializing_if = "is_default")]
    pub name: Option<&'a str>,

    #[serde(default, skip_serializing_if = "is_default", deserialize_with = "deserialize_bool_from_anything")]
    pub restrictive: bool,

    #[serde(default, skip_serializing_if = "is_default")]
    pub policy_for: Option<Vec<Action>>,
    #[serde(default, borrow, skip_serializing_if = "is_default")]
    pub check: Option<Vec<Condition<'a>>>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub using: Option<Vec<Condition<'a>>>,

    // #[serde(default, borrow, skip_serializing_if = "is_default")]
    // pub check_json_str: Option<Cow<'a, str>>,
    // #[serde(default, borrow, skip_serializing_if = "is_default")]
    // pub using_json_str: Option<Cow<'a, str>>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub grant: Option<Vec<Action>>,
    #[serde(default, borrow, skip_serializing_if = "is_default")]
    pub columns: Option<Vec<&'a str>>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct DbSchema<'a> {
    #[serde(default, deserialize_with = "deserialize_bool_from_anything")]
    pub use_internal_permissions: bool,
    #[serde(borrow, deserialize_with = "deserialize_schemas")]
    pub schemas: HashMap<&'a str, Schema<'a>>,
}

impl<'a> DbSchema<'a> {
    pub fn get_join(&self, current_schema: &'a str, origin: &'a str, target: &'a str, hint: &Option<&'a str>) -> Result<Join> {
        let schema = self.schemas.get(current_schema).context(UnacceptableSchemaSnafu {
            schemas: vec![current_schema.to_owned()],
        })?;

        let origin_table = schema.objects.get(origin).context(UnknownRelationSnafu { relation: origin.to_owned() })?;

        match origin_table
            .foreign_keys
            .iter()
            .find(|fk| fk.name == target && fk.referenced_table.0 == current_schema)
        {
            // the target is a foreign key name
            // projects?select=projects_client_id_fkey(*)
            // TODO! when views are involved there may be multiple fks with the same name
            Some(fk) => {
                if origin == fk.table.1 {
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
                                    .find(|&fk| &fk.name == h && fk.referenced_table.0 == current_schema && fk.referenced_table.1 == target)
                                {
                                    return Ok(Parent(fk.clone()));
                                }
                                if let Some(fk) = target_table
                                    .foreign_keys
                                    .iter()
                                    .find(|&fk| &fk.name == h && fk.referenced_table.0 == current_schema && fk.referenced_table.1 == origin)
                                {
                                    return Ok(Child(fk.clone()));
                                }

                                // users?select=tasks!users_tasks(*)
                                if let Some(join_table) = schema.objects.get(h) {
                                    let ofk1 = join_table
                                        .foreign_keys
                                        .iter()
                                        .find(|fk| fk.referenced_table.0 == current_schema && fk.referenced_table.1 == origin);
                                    let ofk2 = join_table
                                        .foreign_keys
                                        .iter()
                                        .find(|fk| fk.referenced_table.0 == current_schema && fk.referenced_table.1 == target);
                                    if let (Some(fk1), Some(fk2)) = (ofk1, ofk2) {
                                        return Ok(Many(Qi(current_schema, join_table.name), fk1.clone(), fk2.clone()));
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
                                                fk.referenced_table.0 == current_schema
                                                    && fk.referenced_table.1 == target
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
                                            fk.referenced_table.0 == current_schema
                                                && fk.referenced_table.1 == origin
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
                                        rel_hint: rel_hint(&joins),
                                        compressed_rel: joins.iter().map(compressed_rel).collect(),
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
                                    .filter(|&fk| fk.referenced_table.0 == current_schema && fk.referenced_table.1 == origin)
                                    .map(|fk| Child(fk.clone()))
                                    .collect::<Vec<_>>();

                                // check parent relations
                                // projects?select=clients(*)
                                let parent_joins = origin_table
                                    .foreign_keys
                                    .iter()
                                    .filter(|&fk| {
                                        fk.referenced_table.0 == current_schema && fk.referenced_table.1 == target && fk.table != fk.referenced_table
                                    })
                                    .map(|fk| Parent(fk.clone()))
                                    .collect::<Vec<_>>();

                                // check many to many relations
                                // users?select=tasks(*)
                                let many_joins = match schema.join_tables.get(&(origin, target)) {
                                    None => vec![],
                                    Some(jt) => jt
                                        .iter()
                                        .filter_map(|t| schema.objects.get(t))
                                        .flat_map(|join_table| {
                                            let fks1 = join_table
                                                .foreign_keys
                                                .iter()
                                                .filter(|fk| fk.referenced_table.0 == current_schema && fk.referenced_table.1 == origin)
                                                .collect::<Vec<_>>();
                                            let fks2 = join_table
                                                .foreign_keys
                                                .iter()
                                                .filter(|fk| fk.referenced_table.0 == current_schema && fk.referenced_table.1 == target)
                                                .collect::<Vec<_>>();
                                            let product = fks1
                                                .iter()
                                                .flat_map(|&fk1| {
                                                    fks2.iter()
                                                        .map(move |&fk2| Many(Qi(current_schema, join_table.name), fk1.clone(), fk2.clone()))
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
                                        rel_hint: rel_hint(&joins),
                                        compressed_rel: joins.iter().map(compressed_rel).collect(),
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
                            .filter(|&fk| fk.referenced_table.0 == current_schema && fk.columns.len() == 1 && fk.columns.contains(&target))
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
                                rel_hint: rel_hint(&joins),
                                compressed_rel: joins.iter().map(compressed_rel).collect(),
                            })
                        }
                    }
                }
            }
        }
    }

    pub fn has_select_privileges(&self, role: Role, current_schema: &str, origin: &str, columns: &ColumnPermissions) -> Result<()> {
        self.has_privileges(role, &Action::Select, current_schema, origin, columns)
    }
    pub fn has_insert_privileges(&self, role: Role, current_schema: &str, origin: &str, columns: &ColumnPermissions) -> Result<()> {
        self.has_privileges(role, &Action::Insert, current_schema, origin, columns)
    }
    pub fn has_update_privileges(&self, role: Role, current_schema: &str, origin: &str, columns: &ColumnPermissions) -> Result<()> {
        self.has_privileges(role, &Action::Update, current_schema, origin, columns)
    }
    pub fn has_delete_privileges(&self, role: Role, current_schema: &str, origin: &str) -> Result<()> {
        self.has_privileges(role, &Action::Delete, current_schema, origin, &All)
    }
    pub fn has_execute_privileges(&self, role: Role, current_schema: &str, origin: &str) -> Result<()> {
        self.has_privileges(role, &Action::Execute, current_schema, origin, &All)
    }

    fn has_privileges(&self, role: Role, action: &Action, current_schema: &str, origin: &str, columns: &ColumnPermissions) -> Result<()> {
        debug!("has_privileges: {:?} {:?} {:?} {:?} {:?}", role, action, current_schema, origin, columns);
        let schema = self.schemas.get(current_schema).context(UnacceptableSchemaSnafu {
            schemas: vec![current_schema.to_owned()],
        })?;
        let origin_table = schema.objects.get(origin).context(UnknownRelationSnafu { relation: origin.to_owned() })?;
        let grants = &origin_table.permissions.grants;
        let all_privileges = [grants.get(&(role, action.clone())), grants.get(&("public", action.clone()))];
        let column_permissions = match all_privileges {
            [Some(Specific(a)), Some(Specific(b))] => Ok(Specific(a.iter().chain(b.iter()).cloned().collect::<Vec<_>>())),
            [Some(All), _] | [_, Some(All)] => Ok(All),
            [Some(Specific(a)), None] | [None, Some(Specific(a))] => Ok(Specific(a.clone())),
            [None, None] => Err(Error::PermissionDenied {
                details: format!("no {:?} privileges for '{}.{}' table", &action, current_schema, origin),
            }),
        }?;

        // check if columns vector is contained in allowed_columns except for Delete/Execute action
        match column_permissions {
            All => Ok(()),
            Specific(allowed_columns) => match columns {
                All => Err(Error::PermissionDenied {
                    details: format!("no {:?} privileges for '{}.{}(*)'", &action, current_schema, origin),
                }),
                Specific(accessed_columns) => {
                    if ![Action::Delete, Action::Execute].contains(action) {
                        for c in accessed_columns {
                            if !allowed_columns.contains(c) {
                                return Err(Error::PermissionDenied {
                                    details: format!("no {:?} privileges for '{}.{}({})'", &action, current_schema, origin, c),
                                });
                            }
                        }
                    }
                    Ok(())
                }
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Schema<'a> {
    pub name: &'a str,
    #[serde(borrow, deserialize_with = "deserialize_objects")]
    pub objects: BTreeMap<&'a str, Object<'a>>,
    #[serde(borrow, default)]
    join_tables: BTreeMap<(&'a str, &'a str), BTreeSet<&'a str>>,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Object<'a> {
    #[serde(borrow)]
    pub kind: ObjectType<'a>,
    pub name: &'a str,
    #[serde(borrow)]
    pub columns: BTreeMap<&'a str, Column<'a>>,
    #[serde(borrow)]
    pub foreign_keys: Vec<ForeignKey<'a>>,
    #[serde(borrow)]
    pub permissions: Permissions<'a>,
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ObjectDef<'a> {
    //common fields
    pub kind: &'a str,
    pub name: &'a str,
    #[serde(borrow, deserialize_with = "deserialize_columns", default)]
    pub columns: BTreeMap<&'a str, Column<'a>>,
    #[serde(borrow, deserialize_with = "deserialize_foreign_keys", default)]
    pub foreign_keys: Vec<ForeignKey<'a>>,

    #[serde(borrow, deserialize_with = "deserialize_permissions", default)]
    pub permissions: Permissions<'a>,
    // #[serde(deserialize_with = "deserialize_grants", default)]
    // pub grants: Option<Permissions<ColumnName>>,
    // #[serde(deserialize_with = "deserialize_policies", default)]
    // pub policies: Option<Permissions<Policy>>,

    //fields for functions
    #[serde(default)]
    pub volatile: char,
    #[serde(default)]
    pub composite: bool,
    #[serde(default)]
    pub setof: bool,
    #[serde(default)]
    pub return_type: &'a str,
    #[serde(default = "pg_catalog")]
    pub return_type_schema: &'a str,
    #[serde(borrow, default, deserialize_with = "deserialize_vec_procparam")]
    parameters: Vec<ProcParam<'a>>,
}

#[derive(Deserialize)]
#[serde(remote = "ProcParam")]
struct ProcParamDef<'a> {
    name: &'a str,
    #[serde(alias = "type")]
    type_: &'a str,
    required: bool,
    variadic: bool,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub enum ProcVolatility {
    Imutable,
    Stable,
    Volatile,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub enum ProcReturnType<'a> {
    #[serde(borrow)]
    One(PgType<'a>),
    SetOf(PgType<'a>),
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub enum PgType<'a> {
    Scalar,
    #[serde(borrow)]
    Composite(Qi<'a>),
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub enum ObjectType<'a> {
    #[serde(rename = "view")]
    View,

    #[serde(rename = "table")]
    Table,

    #[serde(rename = "function")]
    Function {
        volatile: ProcVolatility,
        #[serde(borrow)]
        return_type: ProcReturnType<'a>,
        #[serde(deserialize_with = "deserialize_vec_procparam")]
        parameters: Vec<ProcParam<'a>>,
    },
}

#[derive(Debug, PartialEq, Clone, Deserialize)]
struct ForeignKeyDef<'a> {
    name: &'a str,
    table: Qi<'a>,
    columns: Vec<&'a str>,
    referenced_table: Qi<'a>,
    referenced_columns: Vec<&'a str>,
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct Column<'a> {
    #[serde(default)]
    pub name: &'a str,
    pub data_type: &'a str,
    // #[serde(default, skip_serializing_if = "is_default")]
    #[serde(default)]
    pub primary_key: bool,
}

//replace Action::All with specific actions
fn normalize_actions(actions: &[Action]) -> Vec<Action> {
    actions.iter().fold(vec![], |mut acc, a| {
        match a {
            Action::All => {
                acc.extend(vec![Action::Select, Action::Insert, Action::Update, Action::Delete]);
            }
            _ => acc.push(a.clone()),
        }
        acc
    })
}
fn deserialize_permissions<'de, D>(deserializer: D) -> Result<Permissions<'de>, D::Error>
where
    D: Deserializer<'de>,
{
    let permissions = Option::<Vec<PermissionDef>>::deserialize(deserializer)?;
    match permissions {
        Some(permissions) => {
            let mut grants = HashMap::new();
            let mut policies = HashMap::new();
            for p in permissions {
                match (p.grant, p.columns) {
                    (Some(actions), Some(columns)) => {
                        let actions_ = normalize_actions(&actions);
                        for a in actions_ {
                            if columns.is_empty() {
                                grants.insert((p.role, a), All);
                            } else {
                                let cols = grants.entry((p.role, a)).or_insert(Specific(Vec::new()));
                                if let Specific(cols) = cols {
                                    cols.extend(columns.iter());
                                }
                            }
                        }
                    }
                    (Some(actions), None) => {
                        let actions_ = normalize_actions(&actions);
                        for a in actions_ {
                            grants.insert((p.role, a), All);
                        }
                    }
                    _ => (),
                }
                match (p.policy_for, p.check, p.using /*, p.check_json_str, p.using_json_str*/) {
                    (actions, check @ Some(_), using /*, None, None*/) | (actions, check, using @ Some(_) /*, None, None*/) => {
                        let actions_ = match actions {
                            Some(actions) => {
                                if actions.is_empty() {
                                    vec![Action::All]
                                } else {
                                    actions
                                }
                            }
                            None => vec![Action::All],
                        };

                        for a in actions_ {
                            let pols = policies.entry((p.role, a.clone())).or_insert_with(Vec::new);
                            match (a, &check, &using) {
                                (Action::Select, _, Some(u)) => pols.push(Policy {
                                    name: p.name,
                                    restrictive: p.restrictive,
                                    check: None,
                                    using: Some(u.clone()),
                                }),
                                (Action::Insert, Some(c), _) => pols.push(Policy {
                                    name: p.name,
                                    restrictive: p.restrictive,
                                    check: Some(c.clone()),
                                    using: None,
                                }),
                                (Action::Update, c, u) | (Action::All, c, u) => pols.push(Policy {
                                    name: p.name,
                                    restrictive: p.restrictive,
                                    check: c.clone(),
                                    using: u.clone(),
                                }),
                                (Action::Delete, _, Some(u)) => pols.push(Policy {
                                    name: p.name,
                                    restrictive: p.restrictive,
                                    check: None,
                                    using: Some(u.clone()),
                                }),
                                _ => (),
                            }
                        }
                    }
                    //this branch is custom handling for clickouse where json manipulation is limited
                    //and check and using are returned as json strings
                    // (actions, None, None, check_str @ Some(_), using_str) | (actions, None, None, check_str, using_str @ Some(_)) => {
                    //     let actions_ = match actions {
                    //         Some(actions) => {
                    //             if actions.is_empty() {
                    //                 vec![Action::All]
                    //             } else {
                    //                 actions
                    //             }
                    //         }
                    //         None => vec![Action::All],
                    //     };
                    //     let check: Option<Vec<Condition>> = match check_str {
                    //         Some(check_str) => Some(serde_json::from_str(&check_str).map_err(serde::de::Error::custom)?),
                    //         None => None,
                    //     };
                    //     let using: Option<Vec<Condition>> = match using_str {
                    //         Some(using_str) => Some(serde_json::from_str(&using_str).map_err(serde::de::Error::custom)?),
                    //         None => None,
                    //     };
                    //     for a in actions_ {
                    //         let pols = policies.entry((p.role, a.clone())).or_insert_with(Vec::new);
                    //         match (a, &check, &using) {
                    //             (Action::Select, _, Some(u)) => pols.push(Policy {
                    //                 name: p.name,
                    //                 restrictive: p.restrictive,
                    //                 check: None,
                    //                 using: Some(u.clone()),
                    //             }),
                    //             (Action::Insert, Some(c), _) => pols.push(Policy {
                    //                 name: p.name,
                    //                 restrictive: p.restrictive,
                    //                 check: Some(c.clone()),
                    //                 using: None,
                    //             }),
                    //             (Action::Update, c, u) | (Action::All, c, u) => pols.push(Policy {
                    //                 name: p.name,
                    //                 restrictive: p.restrictive,
                    //                 check: c.clone(),
                    //                 using: u.clone(),
                    //             }),
                    //             (Action::Delete, _, Some(u)) => pols.push(Policy {
                    //                 name: p.name,
                    //                 restrictive: p.restrictive,
                    //                 check: None,
                    //                 using: Some(u.clone()),
                    //             }),
                    //             _ => (),
                    //         }
                    //     }
                    // }
                    _ => {}
                }
            }
            Ok(Permissions { grants, policies })
        }
        None => Ok(Permissions::default()),
    }
}

// fn deserialize_grants<'de, D>(deserializer: D) -> Result<Option<Permissions<ColumnName>>, D::Error>
// where D: Deserializer<'de>,
// {
//     let mut map = HashMap::new();
//     let map_in = HashMap::<Role,HashMap::<Action,Vec::<ColumnName>>>::deserialize(deserializer);
//     for (role, rules) in map_in? {
//         for (method, columns) in rules {
//             map.insert((role.clone(), method), columns);
//         }
//     }
//     Ok(Some(map))
// }

// fn deserialize_policies<'de, D>(deserializer: D) -> Result<Option<Permissions<Policy>>, D::Error>
// where D: Deserializer<'de>,
// {
//     let mut map = HashMap::new();
//     let policies = Option::<HashMap::<Role,HashMap::<Action,Vec::<Policy>>>>::deserialize(deserializer)?;
//     match policies {
//         Some(map_in) => {
//             for (role, rules) in map_in {
//                 for (method, conditions) in rules {
//                     map.insert((role.clone(), method), conditions);
//                 }
//             }
//             Ok(Some(map))
//         },
//         None => return Ok(None),
//     }

// }

fn deserialize_vec_procparam<'de: 'a, 'a, D>(deserializer: D) -> Result<Vec<ProcParam<'a>>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    struct Wrapper<'a>(#[serde(borrow, with = "ProcParamDef")] ProcParam<'a>);

    let v = Vec::deserialize(deserializer)?;
    Ok(v.into_iter().map(|Wrapper(a)| a).collect())
}

fn deserialize_schemas<'de, D>(deserializer: D) -> Result<HashMap<&'de str, Schema<'de>>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut map = HashMap::new();
    for mut schema in Vec::<Schema>::deserialize(deserializer)? {
        let join_tables: BTreeMap<(&str, &str), Vec<&str>> = schema
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
                                    ((fk1.referenced_table.1, fk2.referenced_table.1), *n),
                                    ((fk2.referenced_table.1, fk1.referenced_table.1), *n),
                                ]
                            })
                            .collect::<Vec<((&str, &str), &str)>>()
                    })
                    .collect::<Vec<((&str, &str), &str)>>(),
            })
            .fold(BTreeMap::new(), |mut acc, (k, v)| {
                acc.entry(k).or_default().push(v);
                acc
            });
        for (k, v) in join_tables {
            schema.join_tables.insert(k, BTreeSet::from_iter(v.into_iter()));
        }
        map.insert(schema.name, schema);
    }
    Ok(map)
}

fn deserialize_objects<'de, D>(deserializer: D) -> Result<BTreeMap<&'de str, Object<'de>>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut map = BTreeMap::new();
    for o in Vec::<ObjectDef>::deserialize(deserializer)? {
        map.insert(
            o.name,
            match o.kind {
                "function" => Object {
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
                    permissions: o.permissions,
                },
                "view" => Object {
                    kind: ObjectType::View,
                    name: o.name,
                    columns: o.columns,
                    foreign_keys: o.foreign_keys,
                    permissions: o.permissions,
                },
                _ => Object {
                    kind: ObjectType::Table,
                    name: o.name,
                    columns: o.columns,
                    foreign_keys: o.foreign_keys,
                    permissions: o.permissions,
                },
            },
        );
    }
    Ok(map)
}

fn deserialize_foreign_keys<'de, D>(deserializer: D) -> Result<Vec<ForeignKey<'de>>, D::Error>
where
    D: Deserializer<'de>,
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

fn deserialize_columns<'de, D>(deserializer: D) -> Result<BTreeMap<&'de str, Column<'de>>, D::Error>
where
    D: Deserializer<'de>,
{
    let mut map = BTreeMap::new();
    for column in Vec::<Column>::deserialize(deserializer)? {
        map.insert(column.name, column);
    }
    Ok(map)
}

fn deserialize_bool_from_anything<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    use std::f64::EPSILON;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AnythingOrBool {
        String(String),
        Int(i64),
        Float(f64),
        Boolean(bool),
    }

    match AnythingOrBool::deserialize(deserializer)? {
        AnythingOrBool::Boolean(b) => Ok(b),
        AnythingOrBool::Int(i) => match i {
            1 => Ok(true),
            0 => Ok(false),
            _ => Err(serde::de::Error::custom("The number is neither 1 nor 0")),
        },
        AnythingOrBool::Float(f) => {
            if (f - 1.0f64).abs() < EPSILON {
                Ok(true)
            } else if f == 0.0f64 {
                Ok(false)
            } else {
                Err(serde::de::Error::custom("The number is neither 1.0 nor 0.0"))
            }
        }
        AnythingOrBool::String(string) => {
            if let Ok(b) = string.parse::<bool>() {
                Ok(b)
            } else if let Ok(i) = string.parse::<i64>() {
                match i {
                    1 => Ok(true),
                    0 => Ok(false),
                    _ => Err(serde::de::Error::custom("The number is neither 1 nor 0")),
                }
            } else if let Ok(f) = string.parse::<f64>() {
                if (f - 1.0f64).abs() < EPSILON {
                    Ok(true)
                } else if f == 0.0f64 {
                    Ok(false)
                } else {
                    Err(serde::de::Error::custom("The number is neither 1.0 nor 0.0"))
                }
            } else {
                Err(serde::de::Error::custom(format!("Could not parse boolean from a string: {string}")))
            }
        }
    }
}

// fn is_default<T: Default + PartialEq>(t: &T) -> bool {
//     t == &T::default()
// }

fn pg_catalog<'a>() -> &'a str { "pg_catalog" }

#[cfg(test)]
mod tests {
    use super::*;
    use super::Action::*;
    use crate::api::{Field, Filter, EnvVar, Filter::*, SingleVal, ListVal, LogicOperator::*, ConditionTree, Condition, Condition::*, TrileanVal::*};
    use super::{ObjectType::*, ProcParam};
    use crate::error::Error as AppError;
    use serde_json::Value as JsonValue;
    use pretty_assertions::assert_eq;
    use std::borrow::Cow;
    fn cow(s: &str) -> Cow<str> { Cow::Borrowed(s) }
    fn s(s: &str) -> String { s.to_string() }

    #[test]
    fn deserialize_db_schema() {
        let db_schema = DbSchema {
            use_internal_permissions: false,
            schemas: vec![(
                "api",
                Schema {
                    name: "api",
                    objects: vec![
                        (
                            "myfunction",
                            Object {
                                kind: Function {
                                    volatile: ProcVolatility::Volatile,
                                    return_type: ProcReturnType::SetOf(PgType::Scalar),
                                    parameters: vec![ProcParam {
                                        name: "a",
                                        type_: "integer",
                                        required: true,
                                        variadic: false,
                                    }],
                                },
                                name: "myfunction",
                                columns: [].iter().cloned().collect(),
                                foreign_keys: [].to_vec(),
                                permissions: Permissions::default(),
                            },
                        ),
                        (
                            "tasks",
                            Object {
                                kind: View,
                                name: "tasks",
                                columns: [
                                    (
                                        "id",
                                        Column {
                                            name: "id",
                                            data_type: "int",
                                            primary_key: true,
                                        },
                                    ),
                                    (
                                        "name",
                                        Column {
                                            name: "name",
                                            data_type: "text",
                                            primary_key: false,
                                        },
                                    ),
                                ]
                                .iter()
                                .cloned()
                                .collect(),
                                foreign_keys: [ForeignKey {
                                    name: "project_id_fk",
                                    table: Qi("api", "tasks"),
                                    columns: vec!["project_id"],
                                    referenced_table: Qi("api", "projects"),
                                    referenced_columns: vec!["id"],
                                }]
                                .to_vec(),
                                permissions: Permissions::default(),
                            },
                        ),
                        (
                            "projects",
                            Object {
                                kind: Table,
                                name: "projects",
                                columns: [(
                                    "id",
                                    Column {
                                        name: "id",
                                        data_type: "int",
                                        primary_key: true,
                                    },
                                )]
                                .iter()
                                .cloned()
                                .collect(),
                                foreign_keys: [].to_vec(),
                                permissions: Permissions {
                                    grants: vec![(("role", Select), Specific(vec!["id", "name"]))].iter().cloned().collect(),
                                    policies: vec![(
                                        ("role", Select),
                                        vec![Policy {
                                            name: None,
                                            restrictive: false,
                                            using: Some(vec![Condition::Single {
                                                field: Field { name: "id", json_path: None },
                                                filter: Filter::Op("eq", SingleVal(cow("10"), Some(cow("int")))),
                                                negate: false,
                                            }]),
                                            check: None,
                                        }],
                                    )]
                                    .into_iter()
                                    .collect(),
                                },
                            },
                        ),
                    ]
                    .into_iter()
                    .collect(),
                    join_tables: [].iter().cloned().collect(),
                },
            )]
            .into_iter()
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
                                "permissions":[
                                    {
                                        "role":"role",
                                        "grant":["select"],
                                        "columns":["id","name"]
                                    },
                                    {
                                        "role":"role",
                                        "policy_for":["select"],
                                        "using":[
                                            {"column":"id","op":"eq","val":{"v":"10","t":"int"}}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        "#;

        let deserialized_result = serde_json::from_str::<DbSchema>(json_schema);

        println!("deserialized_result = {deserialized_result:?}");

        let deserialized = deserialized_result.unwrap_or(DbSchema {
            use_internal_permissions: false,
            schemas: HashMap::new(),
        });

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
        println!("deserialized_result = {deserialized_result:?}");
        assert!(deserialized_result.is_ok());
    }

    #[test]
    fn serialize_conditions() {
        let field = Field { name: "id", json_path: None };
        let negate = false;
        let conditions = vec![
            Single {
                field: field.clone(),
                negate,
                filter: Env("eq", EnvVar { var: "role", part: None }),
            },
            Single {
                field: field.clone(),
                negate,
                filter: Env(
                    "eq",
                    EnvVar {
                        var: "request.jwt.claim",
                        part: Some("user_id"),
                    },
                ),
            },
            Single {
                field: field.clone(),
                negate,
                filter: Op("eq", SingleVal(cow("hello"), None)),
            },
            Single {
                field: field.clone(),
                negate,
                filter: Op("eq", SingleVal(cow("hello"), Some(cow("text")))),
            },
            Single {
                field: field.clone(),
                negate,
                filter: In(ListVal(vec![cow("1"), cow("2"), cow("3")], None)),
            },
            Single {
                field: field.clone(),
                negate,
                filter: Is(TriTrue),
            },
            Single {
                field: field.clone(),
                negate: true,
                filter: Fts("eq", None, SingleVal(cow("hello"), None)),
            },
            Group {
                negate: false,
                tree: ConditionTree {
                    operator: And,
                    conditions: vec![
                        Single {
                            field: field.clone(),
                            negate,
                            filter: Op("eq", SingleVal(cow("hello"), None)),
                        },
                        Single {
                            field,
                            negate,
                            filter: In(ListVal(vec![cow("1"), cow("2"), cow("3")], None)),
                        },
                    ],
                },
            },
            //Single { field, filter: Col(Qi, field)},
        ];
        let conditions_json = r#"
        [
            {"column":"id","op":"eq","env":"role"},
            {"column":"id","op":"eq","env":"request.jwt.claim","env_part":"user_id"},
            {"column":"id","op":"eq","val":"hello"},
            {"column":"id","op":"eq","val":{"v":"hello","t":"text"}},
            {"column":"id","in":["1","2","3"]},
            {"column":"id","is":true},
            {"column":"id","fts_op":"eq","val":"hello","negate":true},
            {"tree":{
                "logic_op":"and",
                "conditions":[
                    {"column":"id","op":"eq","val":"hello"},
                    {"column":"id","in":["1","2","3"]}
                ]
            }}
        ]
        "#;

        let serialized_result = serde_json::to_string(&conditions).unwrap();
        println!("serialized_result = {serialized_result}");
        assert_eq!(
            serde_json::from_str::<JsonValue>(conditions_json).unwrap(),
            serde_json::from_str::<JsonValue>(&serialized_result).unwrap()
        );

        let deserialized_result = serde_json::from_str::<Vec<Condition>>(conditions_json);
        println!("deserialized_result = {deserialized_result:?}");
        assert_eq!(deserialized_result.unwrap(), conditions);
    }

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
                .map_err(|e| format!("{e}")),
            Ok(Child(ForeignKey {
                name: "project_id_fk",
                table: Qi("api", "tasks"),
                columns: vec!["project_id"],
                referenced_table: Qi("api", "projects"),
                referenced_columns: vec!["id"],
            }))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("tasks"), &s("projects"), &None)
                .map_err(|e| format!("{e}")),
            Ok(Parent(ForeignKey {
                name: "project_id_fk",
                table: Qi("api", "tasks"),
                columns: vec!["project_id"],
                referenced_table: Qi("api", "projects"),
                referenced_columns: vec!["id"],
            }))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("clients"), &s("projects"), &None)
                .map_err(|e| format!("{e}")),
            Ok(Child(ForeignKey {
                name: "client_id_fk",
                table: Qi("api", "projects"),
                columns: vec!["client_id"],
                referenced_table: Qi("api", "clients"),
                referenced_columns: vec!["id"],
            }))
        );
        assert_eq!(
            db_schema.get_join(&s("api"), &s("tasks"), &s("users"), &None).map_err(|e| format!("{e}")),
            Ok(Many(
                Qi("api", "users_tasks"),
                ForeignKey {
                    name: "task_id_fk",
                    table: Qi("api", "users_tasks"),
                    columns: vec!["task_id"],
                    referenced_table: Qi("api", "tasks"),
                    referenced_columns: vec!["id"],
                },
                ForeignKey {
                    name: "user_id_fk",
                    table: Qi("api", "users_tasks"),
                    columns: vec!["user_id"],
                    referenced_table: Qi("api", "users"),
                    referenced_columns: vec!["id"],
                },
            ))
        );
        assert_eq!(
            db_schema
                .get_join(&s("api"), &s("tasks"), &s("users"), &Some("users_tasks"))
                .map_err(|e| format!("{e}")),
            Ok(Many(
                Qi("api", "users_tasks"),
                ForeignKey {
                    name: "task_id_fk",
                    table: Qi("api", "users_tasks"),
                    columns: vec!["task_id"],
                    referenced_table: Qi("api", "tasks"),
                    referenced_columns: vec!["id"],
                },
                ForeignKey {
                    name: "user_id_fk",
                    table: Qi("api", "users_tasks"),
                    columns: vec!["user_id"],
                    referenced_table: Qi("api", "users"),
                    referenced_columns: vec!["id"],
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
        //                 table: Qi("api",s("users")),
        //                 columns: vec![
        //                     s("billing_address_id"),
        //                 ],
        //                 referenced_table: Qi("api",s("addresses")),
        //                 referenced_columns: vec![
        //                     s("id"),
        //                 ],
        //             },
        //         ),
        //         Parent(
        //             ForeignKey {
        //                 name: s("shipping_address_id_fk"),
        //                 table: Qi("api",s("users")),
        //                 columns: vec![
        //                     s("shipping_address_id"),
        //                 ],
        //                 referenced_table: Qi("api",s("addresses")),
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
