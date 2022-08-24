
use crate::api::ColumnName;
use crate::api::{ApiRequest, FunctionParam, QueryNode::*, SelectItem::Func, SelectItem,Qi,};
use crate::error::{Error, Result};
use crate::schema::{Role, DbSchema};


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
fn get_select_columns(select: &Vec<SelectItem>) -> Vec<ColumnName> {
    select.iter().fold(vec![], |mut acc, s| {
        match s {
            SelectItem::Simple{field, ..} => {
                acc.push(field.name.clone());
            },
            SelectItem::Func{parameters, ..} => {
                acc.extend(get_select_columns_in_params(parameters));
            }
            _ => {},
        }
        acc
    })
}
// check privileges for a given action and user

pub fn check_privileges(db_schema: &DbSchema, current_schema: &String,  user: &Role, request: &ApiRequest) -> Result<()> {
    for (_path, n) in &request.query {

        // check specific privileges for the node
        match n {
            FunctionCall { fn_name: Qi(_, origin), .. } => {
                db_schema.has_execute_privileges(user, current_schema, origin)?;
            },
            Insert { columns, into: origin, .. } => {
                db_schema.has_insert_privileges(user, current_schema, origin, &columns)?;
            },
            Update { columns, table: origin, .. } => {
                db_schema.has_update_privileges(user, current_schema, origin, &columns)?;
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