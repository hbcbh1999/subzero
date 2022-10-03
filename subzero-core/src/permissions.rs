
use std::collections::HashSet;

use crate::api::{ColumnName, ConditionTree};
use crate::api::{ApiRequest, FunctionParam, Query, SubSelect, QueryNode::*, SelectItem::Func, SelectItem,Qi, Condition, LogicOperator::*};
use crate::error::*;
use crate::schema::{Role, DbSchema, ColumnPermissions::*, ColumnPermissions, Action, Policy, Object};
use snafu::OptionExt;
use log::debug;

fn get_select_columns_in_params(params: &[FunctionParam]) -> Vec<ColumnName> {
    params.iter().fold(vec![], |mut acc, p| {
        match p {
            FunctionParam::Fld(f) => acc.push(f.name.clone()),
            FunctionParam::Func{parameters, ..} => acc.extend(get_select_columns_in_params(parameters)),
            _ => {}
        }
        acc
    })
}

fn get_select_columns(select: &[SelectItem]) -> ColumnPermissions {
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

fn get_policies_for_relation<'a>(object: &'a Object, action: Action, role: &Role, permissive_policies: &mut Vec<&'a Policy>, restrictive_policies: &mut Vec<&'a Policy>) {
    if let Some(policies) = object.permissions.policies.get(&(role.clone(), action.clone())) {
        for p in policies {
            match p.restrictive {
                false => permissive_policies.push(p),
                true => restrictive_policies.push(p)
            }
        }
    }
    if let Some(policies) = object.permissions.policies.get(&("public".to_string(), action.clone())) {
        for p in policies {
            match p.restrictive {
                false => permissive_policies.push(p),
                true => restrictive_policies.push(p)
            }
        }
    }
    if let Some(policies) = object.permissions.policies.get(&(role.clone(), Action::All)) {
        for p in policies {
            match p.restrictive {
                false => permissive_policies.push(p),
                true => restrictive_policies.push(p)
            }
        }
    }
    if let Some(policies) = object.permissions.policies.get(&("public".to_string(), Action::All)) {
        for p in policies {
            match p.restrictive {
                false => permissive_policies.push(p),
                true => restrictive_policies.push(p)
            }
        }
    }
    debug!("get_policies_for_relation Object: {:?}, Action: {:?}, Role: {:?}, \nPermissive policies: {:?}, \nRestrictive policies: {:?}", object.name, action, role, permissive_policies, restrictive_policies);
}

fn add_security_quals(security_quals: &mut Vec<Condition>, restrictive_policies: &Vec<&Policy>, permissive_policies: &Vec<&Policy>) {
    /*
	 * First collect up the permissive quals.  If we do not find any
	 * permissive policies then no rows are visible (this is handled below).
	 */
    
    let mut permissive_quals = vec![];
    for p in permissive_policies {
        if let Some(using) = &p.using {
            permissive_quals.extend(using.clone());
        }
    }

    if !permissive_quals.is_empty() {
        /*
		 * We now know that permissive policies exist, so we can now add
		 * security quals based on the USING clauses from the restrictive
		 * policies.  Since these need to be combined together using AND, we
		 * can just add them one at a time.
		 */
        for p in restrictive_policies {
            if let Some(using) = &p.using {
                security_quals.extend(using.clone());
            }
        }
        
        /*
		 * Then add a single security qual combining together the USING
		 * clauses from all the permissive policies using OR.
		 */
        
        security_quals.push(Condition::Group { negate: false, tree: ConditionTree{operator: Or, conditions:permissive_quals} });
    }
    else {
        /*
		 * A permissive policy must exist for rows to be visible at all.
		 * Therefore, if there were no permissive policies found, return a
		 * single always-false clause.
		 */
        security_quals.push(Condition::Raw {sql: "false".to_string()});
    }
    
}

//macro QUAL_FOR_WCO that returns check options for a policy
macro_rules! QUAL_FOR_WCO {
    ($force_using:ident, $policy:ident) => {
        if !$force_using && $policy.check.is_some() {
            &$policy.check
        }
        else {
            &$policy.using
        }
    };
}


fn add_with_check_options(with_check_options: &mut Vec<Condition>, restrictive_policies: &Vec<&Policy>, permissive_policies: &Vec<&Policy>, force_using: bool) {
    let mut permissive_quals = vec![];
    /*
	 * First collect up the permissive policy clauses, similar to
	 * add_security_quals.
	 */
    
    for p in permissive_policies {
        if let Some(qual) = QUAL_FOR_WCO!(force_using, p) {
            permissive_quals.extend(qual.clone());
        }
    }

    /*
	 * There must be at least one permissive qual found or no rows are allowed
	 * to be added.  This is the same as in add_security_quals.
	 *
	 * If there are no permissive_quals then we fall through and return a
	 * single 'false' WCO, preventing all new rows.
	 */
    if !permissive_quals.is_empty() {
        /*
		 * Add a single WithCheckOption for all the permissive policy clauses,
		 * combining them together using OR.  This check has no policy name,
		 * since if the check fails it means that no policy granted permission
		 * to perform the update, rather than any particular policy being
		 * violated.
		 */
        
        with_check_options.push(Condition::Group { negate: false, tree: ConditionTree{operator: Or, conditions:permissive_quals} });

        /*
		 * Now add WithCheckOptions for each of the restrictive policy clauses
		 * (which will be combined together using AND).  We use a separate
		 * WithCheckOption for each restrictive policy to allow the policy
		 * name to be included in error reports if the policy is violated.
		 */

        for p in restrictive_policies {
            if let Some(qual) = QUAL_FOR_WCO!(force_using, p) {
                with_check_options.extend(qual.clone());
            }
        }
    }
    else {
        /*
		 * If there were no policy clauses to check new data, add a single
		 * always-false WCO (a default-deny policy).
		 */
        
        with_check_options.push(Condition::Raw {sql: "false".to_string()});
    }
}

fn get_row_security_policies(rel: &Object, role: &Role, action: Action, apply_select_policies: bool, has_on_conflict_update: bool) -> (Option<Condition>, Option<Condition>) {
    let mut security_quals = vec![];
    let mut with_check_options = vec![];
    let mut	permissive_policies = vec![];
	let mut restrictive_policies = vec![];
    /*
	 * In some cases, we need to apply USING policies (which control the
	 * visibility of records) associated with multiple command types (see
	 * specific cases below).
	 *
	 * When considering the order in which to apply these USING policies, we
	 * prefer to apply higher privileged policies, those which allow the user
	 * to lock records (UPDATE and DELETE), first, followed by policies which
	 * don't (SELECT).
	 *
	 * Note that the optimizer is free to push down and reorder quals which
	 * use leakproof functions.
	 *
	 * In all cases, if there are no policy clauses allowing access to rows in
	 * the table for the specific type of operation, then a single
	 * always-false clause (a default-deny policy) will be added (see
	 * add_security_quals).
	 */

	/*
	 * For a SELECT, if UPDATE privileges are required (eg: the user has
	 * specified FOR [KEY] UPDATE/SHARE), then add the UPDATE USING quals
	 * first.
	 *
	 * This way, we filter out any records from the SELECT FOR SHARE/UPDATE
	 * which the user does not have access to via the UPDATE USING policies,
	 * similar to how we require normal UPDATE rights for these queries.
	 */

    // this is SELECT FOR UPDATE/SHARE and we don't have that case
    // skip
    // if (commandType == CMD_SELECT && rte->requiredPerms & ACL_UPDATE) {}

    /*
	 * For SELECT, UPDATE and DELETE, add security quals to enforce the USING
	 * policies.  These security quals control access to existing table rows.
	 * Restrictive policies are combined together using AND, and permissive
	 * policies are combined together using OR.
	 */

	get_policies_for_relation(rel, action.clone(), role, &mut permissive_policies, &mut restrictive_policies);
    if matches!(action, Action::Select | Action::Update | Action::Delete) {
        add_security_quals(&mut security_quals, &restrictive_policies, &permissive_policies);
    }

    /*
	 * Similar to above, during an UPDATE, DELETE, or MERGE, if SELECT rights
	 * are also required (eg: when a RETURNING clause exists, or the user has
	 * provided a WHERE clause which involves columns from the relation), we
	 * collect up CMD_SELECT policies and add them via add_security_quals
	 * first.
	 *
	 * This way, we filter out any records which are not visible through an
	 * ALL or SELECT USING policy.
	 */

    if matches!(action, Action::Update | Action::Delete) && apply_select_policies {
        let mut select_permissive_policies = vec![];
        let mut select_restrictive_policies = vec![];
        get_policies_for_relation(rel, Action::Select, role, &mut select_permissive_policies, &mut select_restrictive_policies);
        add_security_quals(&mut security_quals, &select_restrictive_policies, &select_permissive_policies);
    }

    /*
	 * For INSERT and UPDATE, add withCheckOptions to verify that any new
	 * records added are consistent with the security policies.  This will use
	 * each policy's WITH CHECK clause, or its USING clause if no explicit
	 * WITH CHECK clause is defined.
	 */

    if matches!(action, Action::Insert | Action::Update) {
        add_with_check_options(&mut with_check_options, &restrictive_policies, &permissive_policies, false);
        /*
		 * Get and add ALL/SELECT policies, if SELECT rights are required for
		 * this relation (eg: when RETURNING is used).  These are added as WCO
		 * policies rather than security quals to ensure that an error is
		 * raised if a policy is violated; otherwise, we might end up silently
		 * dropping rows to be added.
		 */
        if apply_select_policies {
            let mut select_permissive_policies = vec![];
            let mut select_restrictive_policies = vec![];

            get_policies_for_relation(rel, Action::Select, role, &mut select_permissive_policies, &mut select_restrictive_policies);
            add_with_check_options(&mut with_check_options, &select_restrictive_policies, &select_permissive_policies, true);
        }

        /*
		 * For INSERT ... ON CONFLICT DO UPDATE we need additional policy
		 * checks for the UPDATE which may be applied to the same RTE.
		 */
        if action == Action::Insert && has_on_conflict_update {

            let mut conflict_permissive_policies = vec![];
			let mut conflict_restrictive_policies = vec![];
			let mut conflict_select_permissive_policies = vec![];
			let mut conflict_select_restrictive_policies = vec![];
            /* Get the policies that apply to the auxiliary UPDATE */
            get_policies_for_relation(rel, Action::Update, role, &mut conflict_permissive_policies, &mut conflict_restrictive_policies);
            
            /*
			 * Enforce the USING clauses of the UPDATE policies using WCOs
			 * rather than security quals.  This ensures that an error is
			 * raised if the conflicting row cannot be updated due to RLS,
			 * rather than the change being silently dropped.
			 */
            add_with_check_options(&mut with_check_options, &conflict_restrictive_policies, &conflict_permissive_policies, true);

            /*
			 * Get and add ALL/SELECT policies, as WCO_RLS_CONFLICT_CHECK WCOs
			 * to ensure they are considered when taking the UPDATE path of an
			 * INSERT .. ON CONFLICT DO UPDATE, if SELECT rights are required
			 * for this relation, also as WCO policies, again, to avoid
			 * silently dropping data.  See above.
			 */
            if apply_select_policies {
                get_policies_for_relation(rel, Action::Select, role, &mut conflict_select_permissive_policies, &mut conflict_select_restrictive_policies);
                add_with_check_options(&mut with_check_options, &conflict_select_restrictive_policies, &conflict_select_permissive_policies, true);
            }

            /* Enforce the WITH CHECK clauses of the UPDATE policies */
            add_with_check_options(&mut with_check_options, &conflict_restrictive_policies, &conflict_permissive_policies, false);

            /*
			 * Add ALL/SELECT policies as WCO_RLS_UPDATE_CHECK WCOs, to ensure
			 * that the final updated row is visible when taking the UPDATE
			 * path of an INSERT .. ON CONFLICT DO UPDATE, if SELECT rights
			 * are required for this relation.
			 */
            if apply_select_policies {
                add_with_check_options(&mut with_check_options, &conflict_select_restrictive_policies, &conflict_select_permissive_policies, true);
            }
        }
    }
    

    /*
	 * FOR MERGE, we fetch policies for UPDATE, DELETE and INSERT (and ALL)
	 * and set them up so that we can enforce the appropriate policy depending
	 * on the final action we take.
	 *
	 * We already fetched the SELECT policies above.
	 *
	 * We don't push the UPDATE/DELETE USING quals to the RTE because we don't
	 * really want to apply them while scanning the relation since we don't
	 * know whether we will be doing an UPDATE or a DELETE at the end. We
	 * apply the respective policy once we decide the final action on the
	 * target tuple.
	 *
	 * XXX We are setting up USING quals as WITH CHECK. If RLS prohibits
	 * UPDATE/DELETE on the target row, we shall throw an error instead of
	 * silently ignoring the row. This is different than how normal
	 * UPDATE/DELETE works and more in line with INSERT ON CONFLICT DO UPDATE
	 * handling.
	 */

    if action == Action::Merge {
        let mut merge_permissive_policies = vec![];
		let mut merge_restrictive_policies = vec![];

        /*
		 * Fetch the UPDATE policies and set them up to execute on the
		 * existing target row before doing UPDATE.
		 */
		get_policies_for_relation(rel, Action::Update, role, &mut merge_permissive_policies, &mut merge_restrictive_policies);

        /*
		 * WCO_RLS_MERGE_UPDATE_CHECK is used to check UPDATE USING quals on
		 * the existing target row.
		 */
        add_with_check_options(&mut with_check_options, &merge_restrictive_policies, &merge_permissive_policies, true);

        /*
		 * Same with DELETE policies.
		 */
		get_policies_for_relation(rel, Action::Delete, role, &mut merge_permissive_policies, &mut merge_restrictive_policies);

        /*
		 * No special handling is required for INSERT policies. They will be
		 * checked and enforced during ExecInsert(). But we must add them to
		 * withCheckOptions.
		 */
		get_policies_for_relation(rel, Action::Insert, role, &mut merge_permissive_policies, &mut merge_restrictive_policies);

        add_with_check_options(&mut with_check_options, &merge_restrictive_policies, &merge_permissive_policies, false);

        /* Enforce the WITH CHECK clauses of the UPDATE policies */
		add_with_check_options(&mut with_check_options, &merge_restrictive_policies, &merge_permissive_policies, false);
        
    }


    let security_qual_condition = if security_quals.is_empty() {
        None
    } else {
        // deduplicate security_quals using HashSet
        let mut security_quals_set = HashSet::new();
        for qual in security_quals {
            security_quals_set.insert(qual);
        }
        Some(Condition::Group { negate: false, tree: ConditionTree{operator: And, conditions:security_quals_set.into_iter().collect()} })
    };

    let with_check_option_condition = if with_check_options.is_empty() {
        None
    } else {
        // deduplicate with_check_options using HashSet
        let mut with_check_options_set = HashSet::new();
        for qual in with_check_options {
            with_check_options_set.insert(qual);
        }
        Some(Condition::Group { negate: false, tree: ConditionTree{operator: And, conditions:with_check_options_set.into_iter().collect()} })
    };
    (security_qual_condition,with_check_option_condition)
}

pub fn insert_policy_conditions(db_schema: &DbSchema, current_schema: &String,  role: &Role, query: &mut Query) -> Result<()> {
    if !db_schema.use_internal_permissions {
        return Ok(())
    }
    let schema = db_schema.schemas.get(current_schema).context(UnacceptableSchema {schemas: vec![current_schema.to_owned()]})?;
    // by looking at the query node we determine the relevant command type (policy types that need to be applied)
    let (origin, action, apply_select_policies, has_on_conflict_update) = match &query.node {
        FunctionCall {fn_name: Qi(_, origin), ..} => (origin, Action::Execute, false, false),
        Select {from: (origin, _), ..} => (origin, Action::Select, false, false),
        Insert {into: origin, returning, on_conflict, ..} => (origin, Action::Insert, !returning.is_empty(), on_conflict.is_some()),
        Update {table: origin, returning, ..} => (origin, Action::Update, !returning.is_empty(), false),
        Delete {from: origin, returning, ..} => (origin, Action::Delete, !returning.is_empty(), false),
    };
    let rel = schema.objects.get(origin).context(UnknownRelation { relation: origin.to_owned() })?;
    

    let (security_quals, with_check_options) = get_row_security_policies(rel, role, action, apply_select_policies, has_on_conflict_update);

    match query.node {
        Select {where_: ConditionTree {ref mut conditions, ..}, ..} |
        Insert {where_: ConditionTree {ref mut conditions, ..}, ..} |
        Update {where_: ConditionTree {ref mut conditions, ..}, ..} |
        Delete {where_: ConditionTree {ref mut conditions, ..}, ..} => 
            if let Some(condition) = security_quals {
                debug!("Adding policy condition: {:?}", condition);
                conditions.push(condition);
            },
        _ => {}
    }

    match query.node {
        Insert {check: ConditionTree {ref mut conditions, ..}, ..} |
        Update {check: ConditionTree {ref mut conditions, ..}, ..} => 
        if let Some(condition) = with_check_options {
            debug!("Adding policy with check options: {:?}", condition);
            conditions.push(condition);
        },
        _ => {}
    }

    for SubSelect { query: q, .. } in query.sub_selects.iter_mut() {
        insert_policy_conditions(db_schema, current_schema, role, q)?;
    }
    Ok(())


        // let relevant_for_actions = if apply_select_policies {
        //     vec![Action::Select, action.clone()]
        // } else {
        //     vec![action.clone()]
        // };
        // let origin_table = schema.objects.get(origin).context(UnknownRelation { relation: origin.to_owned() })?;
        // let policies = &origin_table.permissions.policies;
        
        // let mut all_policies = relevant_for_actions.iter().fold(vec![], |mut acc, action| {
        //     if let Some(pv) = policies.get(&(role.clone(), action.clone())) {
        //         acc.push(pv);
        //     }
        //     if let Some(pv) = policies.get(&("public".to_string(), action.clone())) {
        //         acc.push(pv);
        //     }
        //     acc
        // });
        
        // let deny_policy_v = vec![Policy{restrictive:false, using: Some(vec![Condition::Raw {sql: "false".to_string()}]), check: Some(vec![Condition::Raw {sql: "false".to_string()}])}];
        // if all_policies.len() == 0 {
        //     all_policies.push(&deny_policy_v);
        // }
        // let all_unique_policies = all_policies.into_iter().flatten().collect::<HashSet<_>>();
        // debug!("Policies for role {} and action {:?}: {:?}", role, action, all_unique_policies);
        // let (toatal_policies, policy_condition) = all_unique_policies.into_iter()
        //     .fold(
        //         (0, Condition::Group { negate: false, tree: ConditionTree{operator: Or, conditions:vec![]} }),
        //         |(i,mut acc), p| {
        //             match acc {
        //                 Condition::Group { tree: ConditionTree{ref mut conditions, ..}, ..} => {
        //                     match &p.using {
        //                         Some(using) => {
        //                             conditions.extend(using.clone());
        //                         },
        //                         None => {}
        //                     }
        //                     match &p.check {
        //                         Some(check) => {
        //                             conditions.extend(check.clone());
        //                         },
        //                         None => {}
        //                     }
        //                 },
        //                 _ => {}
        //             }
        //             (i+1,acc)
        //         }
        //     );
        // if toatal_policies > 0 {
        //     debug!("Adding policy conditions: {:?}", policy_condition);
        //     query.insert_conditions(vec![(vec![], policy_condition)])?;
        // }
        // for SubSelect { query: q, .. } in query.sub_selects.iter_mut() {
        //     insert_policy_conditions(db_schema, current_schema, role, q)?;
        // }
        // Ok(())
    
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
            if !safe_functions.contains(fn_name) {
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