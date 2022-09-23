
with
schemas as (
    select 'public' as schema_name
)
, tables as (
    select 'public' as table_schema, name as table_name, "type" as kind from sqlite_master
    where type in ('table', 'view') and name not like 'sqlite_%'
)
, columns as (
    select
      t.table_schema as table_schema,
      t.table_name as table_name,
      p.name,
      p."type" as data_type,
      p.pk as primary_key
    from
      tables t
      left outer join pragma_table_info((t.table_name)) p on t.table_name <> p.name
)
, primary_keys as (
    select * from columns where pk = true
)
, foreign_keys as (
    select
      t.table_name || '_' || json_extract(json_array(f."from"), '$[0]') || '_fkey' as constraint_name,
      'public' as table_schema,
      t.table_name as table_name,
      json_array(f."from") as columns,
      'public' as foreign_table_schema,
      f."table" as foreign_table_name,
      json_array(f."to") as foreign_columns
    from
      tables t
      left outer join pragma_foreign_key_list((t.table_name)) f on t.table_name <> f."table"
    where f.id not null
),
custom_relations as (
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
    select
        json_extract(value, '$.constraint_name') as constraint_name,
        json_extract(value, '$.table_schema') as table_schema,
        json_extract(value, '$.table_name') as table_name,
        json_extract(value, '$.columns') as columns,
        json_extract(value, '$.foreign_table_schema') as foreign_table_schema,
        json_extract(value, '$.foreign_table_name') as foreign_table_name,
        json_extract(value, '$.foreign_columns') as foreign_columns
    from json_each('{@relations.json#[]}')
),

relations as (
    select
        constraint_name,
        table_schema,
        table_name,
        json(columns) as columns,
        foreign_table_schema,
        foreign_table_name,
        json(foreign_columns) as foreign_columns
    from (
        select * from custom_relations
        union
        select * from foreign_keys
    
    )
),

permissions as (
    select
        json_extract(value, '$.name') as name,
        json_extract(value, '$.restrictive') as restrictive,
        json_extract(value, '$.table_schema') as table_schema,
        json_extract(value, '$.table_name') as table_name,
        json_extract(value, '$.role') as role,
        json_extract(value, '$.grant') as grant,
        json_extract(value, '$.columns') as columns,
        json_extract(value, '$.policy_for') as policy_for,
        json_extract(value, '$.check') as "check",
        json_extract(value, '$.using') as "using"
    from json_each('{@permissions.json#[]}')
)


select json_object(
    'use_internal_permissions', (select count(*) from permissions) > 0,
    'schemas',json_group_array(s.row)
) from (
    select json_object(
        'name', schema_name,
        'objects', (
            select json_group_array(o.row) from (
                select json_object(
                    'kind', t.kind, 
                    'name', t.table_name,
                    'columns', (
                        select json_group_array(c.row) from (
                            select json_object(
                                'name', cc.name,
                                'data_type', cc.data_type,
                                'primary_key', case cc.primary_key when 1 then json('true') else json('false') end
                            ) as row from columns cc
                            where t.table_schema = cc.table_schema and t.table_name = cc.table_name
                        ) c
                    ),
                    'foreign_keys', (
                        select json_group_array(f.row) from (
                            select json_object(
                                'name', ff.constraint_name, 
                                'table', json_array(ff.table_schema,ff."table_name"),
                                'columns', ff.columns,
                                'referenced_table', json_array(ff.foreign_table_schema,ff.foreign_table_name),
                                'referenced_columns', ff.foreign_columns
                            ) as row from relations ff
                            where t.table_schema = ff.table_schema and t.table_name = ff.table_name
                        ) f
                    ),
                    'permissions', (
                        select json_group_array(p.row) from (
                            select json_object(
                                'name', pp.name,
                                'restrictive', coalesce(pp.restrictive,0),
                                'role', pp.role,
                                'policy_for', pp.policy_for,
                                'check', pp."check",
                                'using', pp."using",
                                'grant', pp.grant,
                                'columns', pp.columns
                            ) as row from permissions pp
                            where t.table_schema = pp.table_schema and t.table_name = pp.table_name
                        ) p
                    )
                ) as row from tables t
                where t.table_schema = schema_name
            ) o
        )
    ) as row from schemas
) s
