with 
schemas as (
  select name from system.databases
  where name not in ('system', 'information_schema', 'INFORMATION_SCHEMA')
  and name in {p1:Array(String)}
),
tables as (
  select
    database,
    name,
    uuid,
    is_temporary,
    case as_select
      when '' then false
      else true
    end as is_view,
    comment
  from system.tables
  where
    database not in ('system', 'information_schema', 'INFORMATION_SCHEMA')
    and database in {p1:Array(String)}
    and is_temporary = 0
),
columns as (
  select
    database,
    table,
    name,
    type,
    comment,
    cast(is_in_primary_key,'Bool') as is_in_primary_key
  from system.columns
  where
    database not in ('system', 'information_schema', 'INFORMATION_SCHEMA')
    and database in {p1:Array(String)}
),
custom_relations as (
  select
    tupleElement(row,'constraint_name') as constraint_name,
    tupleElement(row,'table_schema') as table_schema,
    tupleElement(row,'table_name') as table_name,
    tupleElement(row,'columns') as columns,
    tupleElement(row,'foreign_table_schema') as foreign_table_schema,
    tupleElement(row,'foreign_table_name') as foreign_table_name,
    tupleElement(row,'foreign_columns') as foreign_columns
  from (
      select arrayJoin(
          JSONExtract(
              -- we expect a json file with the following structure
              -- '[
              --     {
              --         "constraint_name": "constraint_name",
              --         "table_schema": "default",
              --         "table_name": "tasks",
              --         "columns": ["project_id"],
              --         "foreign_table_schema": "default",
              --         "foreign_table_name": "projects",
              --         "foreign_columns": ["id"]
              --     }
              -- ]'
              '[]'--relations.json
              , 'Array(Tuple(constraint_name String, table_schema String, table_name String, columns Array(String), foreign_table_schema String, foreign_table_name String, foreign_columns Array(String)))'
          )
      ) as row
  )
),
relations as (
  select
    constraint_name,
    table_schema,
    table_name,
    columns,
    foreign_table_schema,
    foreign_table_name,
    foreign_columns
  from custom_relations
),
permissions as (
    select
      tupleElement(row, 'name') as name,
      tupleElement(row, 'restrictive') as restrictive,
      tupleElement(row, 'table_schema') as table_schema,
      tupleElement(row, 'table_name') as table_name,
      tupleElement(row, 'role') as role,
      tupleElement(row, 'grant') as "grant",
      tupleElement(row, 'columns') as columns,
      tupleElement(row, 'policy_for') as policy_for,
      tupleElement(row, 'check') as check_json_str,
      tupleElement(row, 'using') as using_json_str
    from (
      select arrayJoin(
          JSONExtract(
              '[]'--permissions.json
              , 'Array(Tuple(
                  name Nullable(String),
                  restrictive Boolean,
                  table_schema String,
                  table_name String,
                  "role" String,
                  "grant" Array(String),
                  columns Array(String),
                  policy_for Array(String),
                  check Nullable(String),
                  using Nullable(String)
                  ))'
          )
      ) as row
  )
),
json_schema as (
  select
    schemas_agg.array_agg as schemas
  from (
    select groupArray(r) as array_agg
    from (
      select
        cast(
          tuple(name, objects), 
          concat(
            'Tuple(',
            'name ', toTypeName(name), ',',
            'objects ', toTypeName(objects),
            ')'
          )
        ) as r
      from (
        select
          s.name as name,
          groupArray(
            cast(
              tuple(t.name, t.kind, t.columns, t.foreign_keys, t.permissions),
              concat(
                'Tuple(',
                'name ', toTypeName(t.name), ',',
                'kind ', toTypeName(t.kind), ',',
                'columns ', toTypeName(t.columns), ',',
                'foreign_keys ', toTypeName(t.foreign_keys), ',',
                'permissions ', toTypeName(t.permissions),
                ')'
              )
            )
          ) as objects
        from schemas s
        left join (
          select
            tt.database as database,
            tt.name as name,
            case tt.is_view
              when true then 'view'
              else 'table'
            end as kind,
            c.columns as columns,
            r.foreign_keys as foreign_keys,
            p.permissions as permissions
          from tables tt

          -- columns
          left any join (
            select
              database,
              table,
              cast(
                groupArray(
                    tuple(name, type, is_in_primary_key)
                ),
                'Array(Tuple( name String, data_type String, primary_key Boolean ))'
              ) 
              as columns
            from columns
            group by database, table
          ) c on c.database = tt.database and c.table = tt.name

          -- foreign keys
          left any join (
            select
              table_schema,
              table_name,
              cast(
              groupArray(
                  tuple(
                    constraint_name,
                    [table_schema, table_name],
                    columns,
                    [foreign_table_schema, foreign_table_name],
                    foreign_columns
                  )
              ),
              'Array(Tuple( name String, table Array(String), columns Array(String), referenced_table Array(String), referenced_columns Array(String) ))'
              ) 
              as foreign_keys
            from relations
            group by table_schema, table_name
          ) r on r.table_schema = tt.database and r.table_name = tt.name
          
          -- permissions
          left any join (
            select
              table_schema,
              table_name,
              cast(
                groupArray(
                    tuple(
                      name,
                      restrictive,
                      role,
                      grant,
                      columns,
                      policy_for,
                      check_json_str,
                      using_json_str
                    )
                ),
                'Array(Tuple(
                  name Nullable(String),
                  restrictive Boolean,
                  "role" String,
                  "grant" Array(String),
                  columns Array(String),
                  policy_for Array(String),
                  check_json_str Nullable(String),
                  using_json_str Nullable(String)
                  ))'
              ) 
              as permissions
            from permissions
            group by table_schema, table_name
          ) p on p.table_schema = tt.database and p.table_name = tt.name
        
        ) t on s.name = t.database
        group by s.name
      )
    )
  ) schemas_agg
)
select
    (select count(*) from permissions) > 0 as use_internal_permissions,
    schemas from json_schema
-- select * from permissions
format JSONEachRow
settings 
output_format_json_named_tuples_as_objects=1,
join_use_nulls=1
