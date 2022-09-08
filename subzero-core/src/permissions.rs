
use crate::api::{ColumnName, ConditionTree};
use crate::api::{ApiRequest, FunctionParam, Query, SubSelect, QueryNode::*, SelectItem::Func, SelectItem,Qi, Condition, LogicOperator::*};
use crate::error::*;
use crate::schema::{Role, DbSchema, ColumnPermissions::*, ColumnPermissions, Action, Policy,};
use snafu::OptionExt;

fn get_select_columns_in_params(params: &Vec<FunctionParam>) -> Vec<ColumnName> {
    params.iter().fold(vec![], |mut acc, p| {
        match p {
            FunctionParam::Fld(f) => acc.push(f.name.clone()),
            FunctionParam::Func{parameters, ..} => acc.extend(get_select_columns_in_params(parameters)),
            _ => {}
        }
        acc
    })
}
fn get_select_columns(select: &Vec<SelectItem>) -> ColumnPermissions {
    select.iter().fold(Specific(vec![]), |cols, s| {
        match cols {
            All => All,
            Specific(mut acc) => {
                match s {
                    SelectItem::Simple{field, ..} => {
                        acc.push(field.name.clone());
                        Specific(acc)
                    },
                    SelectItem::Func{parameters, ..} => {
                        acc.extend(get_select_columns_in_params(parameters));
                        Specific(acc)
                    },
                    SelectItem::Star => All
                }
            }
        }
        
    })
}
// fn get_select_columns(select: &Vec<SelectItem>) -> Vec<ColumnName> {
//     select.iter().fold(vec![], |mut acc, s| {
//         match s {
//             SelectItem::Simple{field, ..} => {
//                 acc.push(field.name.clone());
//             },
//             SelectItem::Func{parameters, ..} => {
//                 acc.extend(get_select_columns_in_params(parameters));
//             }
//             _ => todo!("get_select_columns: {:?}", s)
//         }
//         acc
//     })
// }
// check privileges for a given action and user


pub fn insert_policy_conditions(db_schema: &DbSchema, current_schema: &String,  role: &Role, query: &mut Query) -> Result<()> {
    if db_schema.use_internal_permissions {
        let schema = db_schema.schemas.get(current_schema).context(UnacceptableSchema {schemas: vec![current_schema.to_owned()]})?;
        let (origin, action) = match &query.node {
            FunctionCall {fn_name: Qi(_, origin), ..} => (origin, Action::Execute),
            Select {from: (origin, _), ..} => (origin, Action::Select),
            Insert {into: origin, ..} => (origin, Action::Insert),
            Update {table: origin, ..}=> (origin, Action::Update),
            Delete {from: origin, ..} => (origin, Action::Delete),
        };
        let origin_table = schema.objects.get(origin).context(UnknownRelation { relation: origin.to_owned() })?;
        let policies = &origin_table.permissions.policies;
        let all_policies = [
            policies.get(&(role.clone(), action.clone())),
            policies.get(&("public".to_string(), action.clone())),
        ];
        let deny_policy_v = vec![Policy{using: Some(vec![Condition::Raw {sql: "false".to_string()}]), check: Some(vec![Condition::Raw {sql: "false".to_string()}])}];
        let policy_condition = all_policies.iter()
            .flat_map(|&vp| match vp {None => Some(&deny_policy_v), Some(v) => Some(v)}).flatten() //Remove None and flatten
            .fold(
                Condition::Group { negate: false, tree: ConditionTree{operator: Or, conditions:vec![]} },
                |mut acc, p| {
                    match acc {
                        Condition::Group { tree: ConditionTree{ref mut conditions, ..}, ..} => {
                            
                            match &p.check {
                                Some(check) => {
                                    conditions.extend(check.clone());
                                },
                                None => {}
                            }
                            // match &p.using {
                            //     Some(using) => {
                            //         conditions.extend(using.clone());
                            //     },
                            //     None => {}
                            // }
                        },
                        _ => {}
                    }
                    acc
                }
            );
        query.insert_conditions(vec![(vec![], policy_condition)])?;
        for SubSelect { query: q, .. } in query.sub_selects.iter_mut() {
            insert_policy_conditions(db_schema, current_schema, role, q)?;
        }
        Ok(())
    } else {
        Ok(())
    }
}

pub fn check_privileges(db_schema: &DbSchema, current_schema: &String,  user: &Role, request: &ApiRequest) -> Result<()> {
    if db_schema.use_internal_permissions {
        for (_path, n) in &request.query {

            // check specific privileges for the node
            match n {
                FunctionCall { fn_name: Qi(_, origin), .. } => {
                    db_schema.has_execute_privileges(user, current_schema, origin)?;
                },
                Insert { columns, into: origin, .. } => {
                    db_schema.has_insert_privileges(user, current_schema, origin, &Specific(columns.clone()))?;
                },
                Update { columns, table: origin, .. } => {
                    db_schema.has_update_privileges(user, current_schema, origin, &Specific(columns.clone()))?;
                },
                Delete {from: origin, ..} => {
                    db_schema.has_delete_privileges(user, current_schema, origin)?;
                },
                _ => {}
            };

            // check select privileges for the node
            let (select, origin) = match n {
                FunctionCall { select, fn_name: Qi(_, origin), .. }|
                Select { select, from: (origin, _), .. }|
                Insert { select, into: origin, .. }|
                Update { select, table: origin, .. }|
                Delete { select, from: origin, ..} => (select, origin)
            };
            let columns = get_select_columns(select);
            db_schema.has_select_privileges(user, current_schema, origin, &columns)?;
        }
        Ok(())
    } else {
        Ok(())
    }
}

fn validate_fn_param(safe_functions: &Vec<String>, p: &FunctionParam) -> Result<()> {
    match p {
        FunctionParam::Func { fn_name, parameters } => {
            if !safe_functions.contains(&fn_name) {
                return Err(Error::ParseRequestError { 
                    details: format!("calling: '{}' is not allowed", fn_name),
                    message: "Unsafe functions called".to_string(),
                });
            }
            for p in parameters {
                validate_fn_param(safe_functions, p)?;
            }
            Ok(())
        },
        _ => {Ok(())}
    }
}
// check only safe functions are called
pub fn check_safe_functions(request: &ApiRequest, safe_functions: &Vec<String>) -> Result<()> {
    for (_path, n) in &request.query {
        match n {
            FunctionCall { select, .. } |
            Select { select, .. } |
            Insert { select, .. } |
            Update { select, .. } |
            Delete { select, ..} => {
                for s in select {
                    if let Func {fn_name, parameters, ..} = s {
                        if !safe_functions.contains(fn_name) {
                            return Err(Error::ParseRequestError { 
                                details: format!("calling: '{}' is not allowed", fn_name),
                                message: "Unsafe functions called".to_string(),
                            });
                        }
                        for p in parameters {
                            validate_fn_param(safe_functions, p)?;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}