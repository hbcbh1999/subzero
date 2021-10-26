use crate::api::{
    *,
    JsonOperation::*, SelectItem::*, JsonOperand::*, LogicOperator:: *,
    Condition::*, Filter::*, Join::*, Query::*,
};
use crate::dynamic_statement::{
    sql, param, SqlSnippet, SqlSnippetChunk, 
};
use postgres_types::{ToSql};

pub fn fmt_query<'a>(schema: &String, q: &'a Query) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)>
{
    match q {
        Select {select, from, where_} => {
            let qi = &Qi(schema.clone(),from.clone());
            let (select, joins): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(qi, s)).unzip();
            let where_snippet_ = fmt_condition_tree(qi, where_);
            let where_snippet = if where_snippet_.len() > 0 { "where\n" + where_snippet_ } else { where_snippet_ };
            let mut from = vec![sql(fmt_qi(qi))];
            from.extend(joins.into_iter().flatten());
            "\nselect\n"+join_snippets(select, ",\n")+"\nfrom "+join_snippets(from, "\n")+"\n"+where_snippet+"\n"
            
        },
        Insert {into, columns, payload, where_:_, returning, select} => {
            let qi = &Qi(schema.clone(),into.clone());
            let payload_param:&(dyn ToSql + Sync) = payload;
            let qi_payload = &Qi(schema.clone(),"subzero_source".to_string());
            let (select, joins): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(qi_payload, s)).unzip();
            //let mut where_snippet = fmt_condition_tree(qi, where_);
            //where_snippet = if where_snippet.len() > 0 { "where\n" + where_snippet } else { where_snippet };
            let mut from = vec![sql(fmt_qi(qi_payload))];
            from.extend(joins.into_iter().flatten());
            
            let insert_snipet = r#"
with subzero_source as (
with 
subzero_payload as ( select "# +param(payload_param)+format!(r#"::json as json_data),
subzero_body as (
select
case when json_typeof(json_data) = 'array'
then json_data
else json_build_array(json_data)
end as val
from
subzero_payload
)
insert into {} ({})
select {}
from json_populate_recordset(null {}, (select val from subzero_body)) _
returning {}
)"#,
            fmt_qi(qi), 
            columns.iter().map(fmt_identity).collect::<Vec<_>>().join(","),
            columns.iter().map(fmt_identity).collect::<Vec<_>>().join(","),
            fmt_qi(qi),
            //where_str,
            returning.iter().map(fmt_identity).collect::<Vec<_>>().join(",")
            );
            let select_snippet = "\nselect\n"+join_snippets(select, ",\n")+"\nfrom "+join_snippets(from, "\n")+"\n";
            insert_snipet + select_snippet
        }
    }
}

fn join_snippets<'a>(v: Vec<SqlSnippet<'a, (dyn ToSql + Sync + 'a)>>, s: & str) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    match v.into_iter().fold(
        SqlSnippet(vec![]),
        |SqlSnippet(mut acc), SqlSnippet(v)| {
            acc.push(SqlSnippetChunk::Sql(s.to_string()));
            acc.extend(v.into_iter());
            SqlSnippet(acc)
        }
    ) {
        SqlSnippet(mut v) => {
            v.remove(0);
            SqlSnippet(v)
        }
    }
}

fn fmt_condition_tree<'a>(qi: &Qi, t: &'a ConditionTree) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    
    match t {
        ConditionTree {operator, conditions} => {
            let sep = format!("\n{}\n",fmt_logic_operator(operator));
            join_snippets(
                conditions.iter().map(|c| fmt_condition(qi, c)).collect::<Vec<_>>(),
                sep.as_str()
            )
        }
    }
}

fn fmt_condition<'a>(qi: &Qi, c: &'a Condition) -> SqlSnippet<'a, (dyn ToSql + Sync + 'a)> {
    match c {
        Single {field, filter, negate} => {
            let fld = sql(format!("{}.{}", fmt_qi(qi), fmt_field(field)));

            if *negate {
                "not(" + fld + fmt_filter(filter) + ")"
            }
            else{
                fld + fmt_filter(filter)
            }
        },
        Group (negate, tree) => {
            if *negate {
                "not("+ fmt_condition_tree(qi, tree) + ")"
            }
            else{
                "("+ fmt_condition_tree(qi, tree) + ")"
            }
        }
        
    }
}

fn fmt_filter(f: &Filter) -> SqlSnippet<(dyn ToSql + Sync)>{

    match f {
        Op (o, v) => {
            let vv:&(dyn ToSql + Sync) = v;
            fmt_operator(o) + param(vv)
        }
        In (l) => {
            let ll:&(dyn ToSql + Sync) = l;
            fmt_operator(&"= any".to_string()) + ("(" + param(ll) + ")")
        },
        Fts (o, lng, v) => {
            let vv:&(dyn ToSql + Sync) = v;
            match lng {
                Some(l) => {
                    let ll:&(dyn ToSql + Sync) = l;
                    fmt_operator(o) + ("(" + param(ll) + "," + param(vv) + ")")
                }
                None => fmt_operator(o) + ("(" + param(vv) + ")")
            }
        },
        Col (qi, fld) => sql( format!("= {}.{}", fmt_qi(qi), fmt_field(fld)) )
    }
}

//fn fmt_select_item<'a >(qi: &Qi, i: &'a SelectItem) -> ((String, Vec<String>), Vec<&'a (dyn ToSql + Sync)>) {
fn fmt_select_item<'a >(qi: &Qi, i: &'a SelectItem) -> (SqlSnippet<'a, (dyn ToSql + Sync + 'a)>, Vec<SqlSnippet<'a, (dyn ToSql + Sync + 'a)>>) {
    match i {
        Simple {field, alias} => (sql(format!("{}.{}{}", fmt_qi(qi), fmt_field(field), fmt_alias(alias))), vec![]),
        SubSelect {query,alias,join,..} => match join {
            Some(j) => match j {
                Parent (fk) => {
                    let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                    let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                    let subquery = fmt_query(&qi.0, query);
                    
                    (
                        sql(format!("row_to_json({}.*) as {}",fmt_identity(&local_table_name), fmt_identity(alias_or_name))),
                        vec!["left join lateral ("+subquery+") as " +sql(fmt_identity(&local_table_name))+ " on true"]
                    )
                },
                Child (fk) => {
                    let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk.table.1));
                    let local_table_name = fmt_identity(&fk.table.1);
                    let subquery = fmt_query(&qi.0, query);
                    (
                        ("coalesce((select json_agg("+sql(local_table_name.clone())+".*) from ("+subquery+") as "+sql(local_table_name.clone())+"), '[]') as " + sql(alias_or_name)),
                        vec![]
                    )
                },
                Many (_table, _fk1, _fk2) => todo!()
            },
            None => panic!("unable to format join query without matching relation")
        }
    }
}

fn fmt_operator(o: &Operator) -> String{
    format!("{}", o)
}

fn fmt_logic_operator( o: &LogicOperator ) -> String {
    match o {
        And => format!("and"),
        Or => format!("or")
    }
}

fn fmt_identity(i: &String) -> String{
    format!("\"{}\"", i)
}

fn fmt_qi(qi: &Qi) -> String{
    if qi.1.as_str() == "subzero_source" {
        format!("{}", fmt_identity(&qi.1))
    }
    else {
        format!("{}.{}", fmt_identity(&qi.0), fmt_identity(&qi.1))
    }
}

fn fmt_field(f: &Field) -> String {
    format!("{}{}", fmt_identity(&f.name), fmt_json_path(&f.json_path))
}

fn fmt_alias(a: &Option<String>) -> String {
    match a {
        Some(aa) => format!(" as {}", aa),
        None => format!("")
    }
}

fn fmt_json_path(p: &Option<Vec<JsonOperation>>) -> String {
    match p {
        Some(j) => format!("{}", j.iter().map(fmt_json_operation).collect::<Vec<_>>().join("")),
        None => format!("")
    }
}

fn fmt_json_operation(j: &JsonOperation) -> String {
    match j {
        JArrow (o) => format!("->{}", fmt_json_operand(o)),
        J2Arrow (o) => format!("->>{}", fmt_json_operand(o)),
    }
}

fn fmt_json_operand(o: &JsonOperand) -> String{
    match o {
        JKey (k) => format!("'{}'", k),
        JIdx (i) => format!("{}", i),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq};
    use crate::dynamic_statement::generate;
    //use crate::api::{SelectItem::*};
    //use crate::api::LogicOperator::*;
    //use crate::api::{Condition::*, Filter::*};
    // use combine::stream::PointerOffset;
    // use combine::easy::{Error, Errors};
    // //use combine::error::StringStreamError;
    // use crate::error::Error as AppError;
    // use combine::EasyParser;
    use super::*;
    //use crate::parser::postgrest::tests::{JSON_SCHEMA};
    fn s(s:&str) -> String {
        s.to_string()
    }
    #[test]
    fn test_fmt_insert_query(){
        let payload = r#"[{"id":10, "a":"a field"}]"#;
        let q = Insert {
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                Simple {field: Field {name: s("b"), json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))])}, alias: None},
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        from: s("clients"),
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"),json_path: None},
                                filter: Filter::Col(Qi(s("api"),s("subzero_source")),Field {name: s("client_id"),json_path: None}),
                                negate: false,
                           }
                        ]}
                    },
                    alias: None,
                    hint: None,
                    join: Some(
                        Parent(ForeignKey {
                                name: s("client_id_fk"),
                                table: Qi(s("api"),s("projects")),
                                columns: vec![s("client_id")],
                                referenced_table: Qi(s("api"),s("clients")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                },
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        from: s("tasks"),
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("project_id"),json_path: None},
                                filter: Filter::Col(Qi(s("api"),s("subzero_source")),Field {name: s("id"),json_path: None}),
                                negate: false,
                            },
                            Single {filter: Op(s(">"),s("50")), field: Field {name: s("id"), json_path: None}, negate: false},
                            Single {filter: In(vec![s("51"), s("52")]), field: Field {name: s("id"), json_path: None}, negate: false}
                        ]}
                    },
                    hint: None,
                    alias: None,
                    join: Some(
                        Child(ForeignKey {
                                name: s("project_id_fk"),
                                table: Qi(s("api"),s("tasks")),
                                columns: vec![s("project_id")],
                                referenced_table: Qi(s("api"),s("projects")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                }
            ],
            into: s("projects"),
            where_: ConditionTree { operator: And, conditions: vec![
                // Single {filter: Op(s(">="),s("5")), field: Field {name: s("id"), json_path: None}, negate: false},
                // Single {filter: Op(s("<"),s("10")), field: Field {name: s("id"), json_path: None}, negate: true}
            ]},
            columns: vec![s("id"), s("a")],
            payload: payload,
            returning: vec![s("id"), s("a")],
        };

        let (query_str, parameters, _) = generate(fmt_query(&s("api"), &q));
        let p0:&(dyn ToSql + Sync) = &vec![&"51", &"52"];
        let pp: Vec<&(dyn ToSql + Sync)> = vec![&payload, &"50", p0];
        assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
        assert_eq!(query_str, s(
		r#"
		with subzero_source as (
			with 
				subzero_payload as ( select $1::json as json_data),
				subzero_body as (
					select
						case when json_typeof(json_data) = 'array'
						then json_data
						else json_build_array(json_data)
						end as val
					from
						subzero_payload
				)
				insert into "api"."projects" ("id","a")
				select "id","a"
				from json_populate_recordset(null "api"."projects", (select val from subzero_body)) _
				returning "id","a"
		)
		select
			"subzero_source"."a",
			"subzero_source"."b"->1->>'key',
			row_to_json("subzero_source_clients".*) as "clients",
			coalesce((select json_agg("tasks".*) from (
				select
					"api"."tasks"."id"
				from "api"."tasks"
				where
					"api"."tasks"."project_id"= "subzero_source"."id"
					and
					"api"."tasks"."id">$2
					and
					"api"."tasks"."id"= any($3)
			) as "tasks"), '[]') as "tasks"
		from "subzero_source"
		left join lateral (
			select
				"api"."clients"."id"
			from "api"."clients"
			where
				"api"."clients"."id"= "subzero_source"."client_id"
		) as "subzero_source_clients" on true
		"#
        ).replace("\t", ""));
    }

    #[test]
    fn test_fmt_select_query(){
        
        let q = Select {
            select: vec![
                Simple {field: Field {name: s("a"), json_path: None}, alias: None},
                Simple {field: Field {name: s("b"), json_path: Some(vec![JArrow(JIdx(s("1"))), J2Arrow(JKey(s("key")))])}, alias: None},
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        from: s("clients"),
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("id"),json_path: None},
                                filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("client_id"),json_path: None}),
                                negate: false,
                           }
                        ]}
                    },
                    alias: None,
                    hint: None,
                    join: Some(
                        Parent(ForeignKey {
                                name: s("client_id_fk"),
                                table: Qi(s("api"),s("projects")),
                                columns: vec![s("client_id")],
                                referenced_table: Qi(s("api"),s("clients")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                },
                SubSelect{
                    query: Select {
                        select: vec![
                            Simple {field: Field {name: s("id"), json_path: None}, alias: None},
                        ],
                        from: s("tasks"),
                        where_: ConditionTree { operator: And, conditions: vec![
                            Single {
                                field: Field {name: s("project_id"),json_path: None},
                                filter: Filter::Col(Qi(s("api"),s("projects")),Field {name: s("id"),json_path: None}),
                                negate: false,
                            },
                            Single {filter: Op(s(">"),s("50")), field: Field {name: s("id"), json_path: None}, negate: false},
                            Single {filter: In(vec![s("51"), s("52")]), field: Field {name: s("id"), json_path: None}, negate: false}
                        ]}
                    },
                    hint: None,
                    alias: None,
                    join: Some(
                        Child(ForeignKey {
                                name: s("project_id_fk"),
                                table: Qi(s("api"),s("tasks")),
                                columns: vec![s("project_id")],
                                referenced_table: Qi(s("api"),s("projects")),
                                referenced_columns: vec![s("id")],
                            }),
                    )
                }
            ],
            from: s("projects"),
            where_: ConditionTree { operator: And, conditions: vec![
                Single {filter: Op(s(">="),s("5")), field: Field {name: s("id"), json_path: None}, negate: false},
                Single {filter: Op(s("<"),s("10")), field: Field {name: s("id"), json_path: None}, negate: true}
            ]}
        };

        let (query_str, parameters, _) = generate(fmt_query(&s("api"), &q));
        let p0:&(dyn ToSql + Sync) = &vec![&"51", &"52"];
        let pp: Vec<&(dyn ToSql + Sync)> = vec![&"50", p0, &"5", &"10"];
        assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
        assert_eq!(query_str, s(
		r#"
		select
			"api"."projects"."a",
			"api"."projects"."b"->1->>'key',
			row_to_json("projects_clients".*) as "clients",
			coalesce((select json_agg("tasks".*) from (
				select
					"api"."tasks"."id"
				from "api"."tasks"
				where
					"api"."tasks"."project_id"= "api"."projects"."id"
					and
					"api"."tasks"."id">$1
					and
					"api"."tasks"."id"= any($2)
			) as "tasks"), '[]') as "tasks"
		from "api"."projects"
		left join lateral (
			select
				"api"."clients"."id"
			from "api"."clients"
			where
				"api"."clients"."id"= "api"."projects"."client_id"
		) as "projects_clients" on true
		where
			"api"."projects"."id">=$3
			and
			not("api"."projects"."id"<$4)
		"#
        ).replace("\t", ""));
    }
    
    
    #[test]
    fn test_fmt_condition_tree(){
        assert_eq!(
            format!("{:?}",generate(fmt_condition_tree(
                &Qi(s("schema"),s("table")),
                &ConditionTree {
                    operator: And,
                    conditions: vec![
                        Single {
                            field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                            filter: Op (s(">"), s("2")),
                            negate: false
                        },
                        Group (false, ConditionTree {
                            operator: And,
                            conditions: vec![
                                Single {
                                    field: Field {name:s("name"), json_path:None},
                                    filter: Op (s(">"), s("2")),
                                    negate: false
                                },
                                Single {
                                    field: Field {name:s("name"), json_path:None},
                                    filter: Op (s("<"), s("5")),
                                    negate: false
                                }
                            ]
                        })
                    ]
                }
            ))),
            format!("{:?}",(s("\"schema\".\"table\".\"name\"->'key'->>21>$1\nand\n(\"schema\".\"table\".\"name\">$2\nand\n\"schema\".\"table\".\"name\"<$3)"), vec![&s("2"), &s("2"), &s("5")], 4))
        );
    }
    
    #[test]
    fn test_fmt_condition(){
        assert_eq!(
            format!("{:?}",generate(fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                    filter: Op (s(">"), s("2")),
                    negate: false
                }
            ))),
            format!("{:?}",(s("\"schema\".\"table\".\"name\"->'key'->>21>$1"), vec![&s("2")], 2))
        );

        assert_eq!(
            format!("{:?}",generate(fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:None},
                    filter: In (vec![s("5"), s("6")]),
                    negate: true
                }
            ))),
            format!("{:?}",(s("not(\"schema\".\"table\".\"name\"= any($1))"), vec![vec![&s("5"), &s("6")]],2))
        );
    }
    
    #[test]
    fn test_fmt_filter(){
        assert_eq!(format!("{:?}",generate(fmt_filter(&Op (s(">"), s("2"))))), format!("{:?}",(&s(">$1"), vec![&s("2")], 2)));
        assert_eq!(format!("{:?}",generate(fmt_filter(&In (vec![s("5"), s("6")])))), format!("{:?}",(&s("= any($1)"), vec![vec![&s("5"), &s("6")]],2)));
        assert_eq!(format!("{:?}",generate(fmt_filter(&Fts (s("@@ to_tsquery"), Some(s("eng")), s("2"))))), format!("{:?}",(&s("@@ to_tsquery($1,$2)"), vec![&s("eng"), &s("2")],3)));
        let p :Vec<&(dyn ToSql + Sync)> = vec![];
        assert_eq!(format!("{:?}",generate(fmt_filter(&Col (Qi(s("api"),s("projects")), Field {name: s("id"), json_path: None})))), format!("{:?}",(&s("= \"api\".\"projects\".\"id\""), p, 1)));
    }
    
    #[test]
    fn test_fmt_operator(){
        assert_eq!(fmt_operator(&s(">")), s(">"));
    }
    
    #[test]
    fn test_fmt_logic_operator(){
        assert_eq!(fmt_logic_operator(&And), s("and"));
        assert_eq!(fmt_logic_operator(&Or), s("or"));
    }
    
    #[test]
    fn test_fmt_select_item(){
        let select = Simple {
            field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
            alias: Some(s("alias"))
        };
        let (select_item,_) = fmt_select_item(
            &Qi(s("schema"),s("table")), 
            &select
        );
        let (query_str,_,_) = generate(select_item);
        assert_eq!(query_str,s("\"schema\".\"table\".\"name\"->'key'->>21 as alias"));
    }
    
    #[test]
    fn test_fmt_qi(){
        assert_eq!(fmt_qi(&Qi(s("schema"),s("table"))), s("\"schema\".\"table\""));
    }
    
    #[test]
    fn test_fmt_field(){
        assert_eq!(
            fmt_field(&Field {name:s("name"), json_path:None}),
            s("\"name\"")
        );
        assert_eq!(
            fmt_field(&Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])}),
            s("\"name\"->'key'->>21")
        );
    }

    #[test]
    fn test_fmt_alias(){
        assert_eq!(fmt_alias(&Some(s("alias"))), s(" as alias"));
    }

    #[test]
    fn test_fmt_json_path(){
        assert_eq!(
            fmt_json_path(&Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])), 
            s("->'key'->>21")
        );
    }

    #[test]
    fn test_fmt_json_operation(){
        assert_eq!(fmt_json_operation(&JArrow(JKey(s("key")))), s("->'key'"));
        assert_eq!(fmt_json_operation(&J2Arrow(JIdx(s("21")))), s("->>21"));
    }

    #[test]
    fn test_fmt_json_operand(){
        assert_eq!(fmt_json_operand(&JKey(s("key"))),s("'key'"));
        assert_eq!(fmt_json_operand(&JIdx(s("23"))),s("23"));
    }
}