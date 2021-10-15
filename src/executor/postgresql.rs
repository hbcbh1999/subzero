use crate::api::*;
use crate::api::{
    JsonOperation::*, SelectItem::*, JsonOperand::*, LogicOperator:: *,
    Condition::*, Filter::*, Join::*, Query::*, 
};
use crate::api::Query;
use postgres_types::{ToSql};

pub fn fmt_query<'a>(schema: &String, q: &'a Query) -> (String, Vec<&'a (dyn ToSql + Sync)> )
{
    let (query_str, params) = match q {
        Select {select, from, where_} => {
            let qi = &Qi(schema.clone(),from.clone());
            let (select_and_joins, subselect_parameters): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(qi, s)).unzip();
            let (select, joins): (Vec<_>, Vec<_>) = select_and_joins.into_iter().unzip();
            let (where_, where_parameters) = fmt_condition_tree(qi, where_);
            let where_str = if where_.len() > 0 { format!("where\n{}", where_) } else { format!("") };
            let mut from = vec![fmt_qi(qi)];
        
            let mut parameters = vec![];
            parameters.extend(subselect_parameters.into_iter().flatten());
            parameters.extend(where_parameters);
            from.extend(joins.into_iter().flatten());
            (format!("\nselect\n{}\nfrom {}\n{}\n", select.join(",\n"), from.join("\n"), where_str), parameters)
        },
        Insert {into, columns, payload, where_, returning, select} => {
            let qi = &Qi(schema.clone(),into.clone());
            let qi_payload = &Qi(schema.clone(),"subzero_source".to_string());
            let (select_and_joins, subselect_parameters): (Vec<_>, Vec<_>) = select.iter().map(|s| fmt_select_item(qi_payload, s)).unzip();
            let (select, joins): (Vec<_>, Vec<_>) = select_and_joins.into_iter().unzip();
            let (where_, where_parameters) = fmt_condition_tree(qi, where_);
            let _where_str = if where_.len() > 0 { format!("where\n{}", where_) } else { format!("") };
            let mut from = vec![fmt_qi(qi_payload)];
        
            let mut parameters = vec![];
            parameters.extend(subselect_parameters.into_iter().flatten());
            parameters.extend(where_parameters);
            from.extend(joins.into_iter().flatten());
            
            let insert_str = format!(r#"
with subzero_source as (
with 
subzero_payload as ( select ?::json as json_data),
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
            let select_str = format!("\nselect\n{}\nfrom {}\n", select.join(",\n"), from.join("\n"));
            parameters.insert(0, payload);
            (format!("{}{}", insert_str, select_str), parameters)
        }
    };

    (query_str, params)
}

fn fmt_condition_tree<'a>(qi: &Qi, t: &'a ConditionTree) -> (String, Vec<&'a (dyn ToSql + Sync)>) {
    match t {
        ConditionTree {operator, conditions} => {
            let (c, p): (Vec<_>, Vec<_>) = conditions.iter()
            .map(|c| fmt_condition(qi, c)).unzip();

            (c.join(&format!("\n{}\n",fmt_logic_operator(operator))), p.into_iter().flatten().collect())
        }
    }
}

fn fmt_condition<'a>(qi: &Qi, c: &'a Condition) -> (String, Vec<&'a (dyn ToSql + Sync)>) {
    match c {
        Single {field, filter, negate} => {
            let (op, val) = fmt_filter(filter);
            let mut placeholder = "?";
            if op.as_str() == "= any" {
                placeholder = "(?)";
            }

            if val.len() == 0 {
                placeholder = "";
            }

            if *negate {
                (format!("not({}.{} {} {})", fmt_qi(qi), fmt_field(field), fmt_operator(&op), placeholder), val)
            }
            else{
                (format!("{}.{} {} {}", fmt_qi(qi), fmt_field(field), fmt_operator(&op), placeholder), val)
            }
        },
        Group (negate, tree) => {
            let (s,p) = fmt_condition_tree(qi, tree);
            if *negate {
                (format!("not({})", s), p)
            }
            else{
                (format!("({})", s), p)
            }
        }
        
    }
}

fn fmt_filter(f: &Filter) -> (String, Vec<&(dyn ToSql + Sync)>){
    match f {
        Op (o, v) => (fmt_operator(o), vec![v]),
        In (l) => (fmt_operator(&"= any".to_string()), vec![l]),
        Fts (o, lng, v) => match lng {
            Some(l) => (format!("{}(?, ?)", fmt_operator(o)), vec![l,v]),
            None => (format!("{}(?)", fmt_operator(o)), vec![v])
        },
        Col (qi, fld) => (format!("= {}.{}", fmt_qi(qi), fmt_field(fld)), vec![])
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


fn fmt_select_item<'a >(qi: &Qi, i: &'a SelectItem) -> ((String, Vec<String>), Vec<&'a (dyn ToSql + Sync)>) {
    match i {
        Simple {field, alias} => ((format!("{}.{}{}", fmt_qi(qi), fmt_field(field), fmt_alias(alias)), vec![]), vec![]),
        SubSelect {query,alias,join,..} => match join {
            Some(j) => match j {
                Parent (fk) => {
                    let alias_or_name = alias.as_ref().unwrap_or(&fk.referenced_table.1);
                    let local_table_name = format!("{}_{}", qi.1, alias_or_name);
                    let (query_str, parameters) = fmt_query(&qi.0, query);
                    (
                        (
                            format!("row_to_json({}.*) as {}",fmt_identity(&local_table_name), fmt_identity(alias_or_name)),
                            vec![format!("left join lateral ({}) as {} on true", query_str ,fmt_identity(&local_table_name))]
                        ),
                        parameters
                    )
                },
                Child (fk) => {
                    let alias_or_name = fmt_identity(alias.as_ref().unwrap_or(&fk.table.1));
                    let local_table_name = fmt_identity(&fk.table.1);
                    let (query_str, parameters) = fmt_query(&qi.0, query);
                    (
                        (
                            format!("coalesce((select json_agg({}.*) from ({}) as {}), '[]') as {}", local_table_name, query_str, local_table_name, alias_or_name),
                            vec![]
                        ),
                        parameters
                    )
                },
                Many (_table, _fk1, _fk2) => todo!()
            },
            None => panic!("unable to format join query without matching relation")
        }
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
                Single {filter: Op(s(">="),s("5")), field: Field {name: s("id"), json_path: None}, negate: false},
                Single {filter: Op(s("<"),s("10")), field: Field {name: s("id"), json_path: None}, negate: true}
            ]},
            columns: vec![s("id"), s("a")],
            payload: payload,
            returning: vec![s("id"), s("a")],
        };

        let (query_str, parameters) = fmt_query(&s("api"), &q);
        let p0:&(dyn ToSql + Sync) = &vec![&"51", &"52"];
        let pp: Vec<&(dyn ToSql + Sync)> = vec![&payload, &"50", p0, &"5", &"10"];
        assert_eq!(format!("{:?}", parameters), format!("{:?}", pp));
        assert_eq!(query_str, s(
		r#"
		with subzero_source as (
			with 
				subzero_payload as ( select ?::json as json_data),
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
					"api"."tasks"."project_id" = "subzero_source"."id" 
					and
					"api"."tasks"."id" > ?
					and
					"api"."tasks"."id" = any (?)
			) as "tasks"), '[]') as "tasks"
		from "subzero_source"
		left join lateral (
			select
				"api"."clients"."id"
			from "api"."clients"
			where
				"api"."clients"."id" = "subzero_source"."client_id" 
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

        let (query_str, parameters) = fmt_query(&s("api"), &q);
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
					"api"."tasks"."project_id" = "api"."projects"."id" 
					and
					"api"."tasks"."id" > ?
					and
					"api"."tasks"."id" = any (?)
			) as "tasks"), '[]') as "tasks"
		from "api"."projects"
		left join lateral (
			select
				"api"."clients"."id"
			from "api"."clients"
			where
				"api"."clients"."id" = "api"."projects"."client_id" 
		) as "projects_clients" on true
		where
			"api"."projects"."id" >= ?
			and
			not("api"."projects"."id" < ?)
		"#
        ).replace("\t", ""));
    }
    
    
    #[test]
    fn test_fmt_condition_tree(){
        assert_eq!(
            format!("{:?}",fmt_condition_tree(
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
            )),
            format!("{:?}",(s("\"schema\".\"table\".\"name\"->'key'->>21 > ?\nand\n(\"schema\".\"table\".\"name\" > ?\nand\n\"schema\".\"table\".\"name\" < ?)"), vec![&s("2"), &s("2"), &s("5")]))
        );
    }
    
    #[test]
    fn test_fmt_condition(){
        assert_eq!(
            format!("{:?}",fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                    filter: Op (s(">"), s("2")),
                    negate: false
                }
            )),
            format!("{:?}",(s("\"schema\".\"table\".\"name\"->'key'->>21 > ?"), vec![&s("2")]))
        );

        assert_eq!(
            format!("{:?}",fmt_condition(
                &Qi(s("schema"),s("table")),
                &Single {
                    field: Field {name:s("name"), json_path:None},
                    filter: In (vec![s("5"), s("6")]),
                    negate: true
                }
            )),
            format!("{:?}",(s("not(\"schema\".\"table\".\"name\" = any (?))"), vec![vec![&s("5"), &s("6")]]))
        );
    }
    
    #[test]
    fn test_fmt_filter(){
        assert_eq!(format!("{:?}",fmt_filter(&Op (s(">"), s("2")))), format!("{:?}",(&s(">"), vec![&s("2")])));
        assert_eq!(format!("{:?}",fmt_filter(&In (vec![s("5"), s("6")]))), format!("{:?}",(&s("= any"), vec![vec![&s("5"), &s("6")]])));
        assert_eq!(format!("{:?}",fmt_filter(&Fts (s("@@ to_tsquery"), Some(s("eng")), s("2")))), format!("{:?}",(&s("@@ to_tsquery(?, ?)"), vec![&s("eng"), &s("2")])));
        let p :Vec<&(dyn ToSql + Sync)> = vec![];
        assert_eq!(format!("{:?}",fmt_filter(&Col (Qi(s("api"),s("projects")), Field {name: s("id"), json_path: None}))), format!("{:?}",(&s("= \"api\".\"projects\".\"id\""), p)));
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
        assert_eq!(
            fmt_select_item(
                &Qi(s("schema"),s("table")), 
                &Simple {
                    field: Field {name:s("name"), json_path:Some(vec![JArrow(JKey(s("key"))), J2Arrow(JIdx(s("21")))])},
                    alias: Some(s("alias"))
                }
            ).0,
            (s("\"schema\".\"table\".\"name\"->'key'->>21 as alias"), vec![])
        );
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