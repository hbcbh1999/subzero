
use std::collections::HashSet;

use crate::api::{ColumnName, ConditionTree};
use crate::api::{ApiRequest, FunctionParam, Query, SubSelect, QueryNode::*, SelectItem::Func, SelectItem,Qi, Condition, LogicOperator::*};
use crate::error::*;
use crate::schema::{Role, DbSchema, ColumnPermissions::*, ColumnPermissions, Action, Policy,};
use snafu::OptionExt;
use log::debug;

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

pub fn insert_policy_conditions(db_schema: &DbSchema, current_schema: &String,  role: &Role, query: &mut Query) -> Result<()> {
    if db_schema.use_internal_permissions {
        let schema = db_schema.schemas.get(current_schema).context(UnacceptableSchema {schemas: vec![current_schema.to_owned()]})?;
        // by looking at the query node we determine the relevant actions (policy types)
        let (origin, action, apply_select_policies) = match &query.node {
            FunctionCall {fn_name: Qi(_, origin), ..} => (origin, Action::Execute, false),
            Select {from: (origin, _), ..} => (origin, Action::Select, false),
            Insert {into: origin, returning, ..} => (origin, Action::Insert, returning.len() > 0),
            Update {table: origin, returning, ..} => (origin, Action::Update, returning.len() > 0),
            Delete {from: origin, returning, ..} => (origin, Action::Delete, returning.len() > 0),
        };
        let relevant_for_actions = if apply_select_policies {
            vec![Action::Select, action.clone()]
        } else {
            vec![action.clone()]
        };
        let origin_table = schema.objects.get(origin).context(UnknownRelation { relation: origin.to_owned() })?;
        let policies = &origin_table.permissions.policies;
        
        let mut all_policies = relevant_for_actions.iter().fold(vec![], |mut acc, action| {
            if let Some(pv) = policies.get(&(role.clone(), action.clone())) {
                acc.push(pv);
            }
            if let Some(pv) = policies.get(&("public".to_string(), action.clone())) {
                acc.push(pv);
            }
            acc
        });
        
        let deny_policy_v = vec![Policy{restrictive:false, using: Some(vec![Condition::Raw {sql: "false".to_string()}]), check: Some(vec![Condition::Raw {sql: "false".to_string()}])}];
        if all_policies.len() == 0 {
            all_policies.push(&deny_policy_v);
        }
        let all_unique_policies = all_policies.into_iter().flatten().collect::<HashSet<_>>();
        debug!("Policies for role {} and action {:?}: {:?}", role, action, all_unique_policies);
        let (toatal_policies, policy_condition) = all_unique_policies.into_iter()
            .fold(
                (0, Condition::Group { negate: false, tree: ConditionTree{operator: Or, conditions:vec![]} }),
                |(i,mut acc), p| {
                    match acc {
                        Condition::Group { tree: ConditionTree{ref mut conditions, ..}, ..} => {
                            match &p.using {
                                Some(using) => {
                                    conditions.extend(using.clone());
                                },
                                None => {}
                            }
                            match &p.check {
                                Some(check) => {
                                    conditions.extend(check.clone());
                                },
                                None => {}
                            }
                        },
                        _ => {}
                    }
                    (i+1,acc)
                }
            );
        if toatal_policies > 0 {
            debug!("Adding policy conditions: {:?}", policy_condition);
            query.insert_conditions(vec![(vec![], policy_condition)])?;
        }
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