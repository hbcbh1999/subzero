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
    select
        json_extract(value, '$.constraint_name') as constraint_name,
        json_extract(value, '$.table_schema') as table_schema,
        json_extract(value, '$.table_name') as table_name,
        json_extract(value, '$.columns') as columns,
        json_extract(value, '$.foreign_table_schema') as foreign_table_schema,
        json_extract(value, '$.foreign_table_name') as foreign_table_name,
        json_extract(value, '$.foreign_columns') as foreign_columns
    from json_each('[]')
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
    from json_each('[]')
)

select json_object(
    'use_internal_permissions', (select count(*) from permissions) > 0,
    'schemas',json_group_array(json(s.row))
) as json_schema from (
    select json_object(
        'name', schema_name,
        'objects', (
            select json_group_array(json(o.row)) from (
                select json_object(
                    'kind', t.kind, 
                    'name', t.table_name,
                    'columns', (
                        select json_group_array(json(c.row)) from (
                            select json_object(
                                'name', cc.name,
                                'data_type', cc.data_type,
                                'primary_key', case cc.primary_key when 1 then json('true') else json('false') end
                            ) as row from columns cc
                            where t.table_schema = cc.table_schema and t.table_name = cc.table_name
                        ) c
                    ),
                    'foreign_keys', (
                        select json_group_array(json(f.row)) from (
                            select json_object(
                                'name', ff.constraint_name, 
                                'table', json_array(ff.table_schema,ff."table_name"),
                                'columns', json(ff.columns),
                                'referenced_table', json_array(ff.foreign_table_schema,ff.foreign_table_name),
                                'referenced_columns', json(ff.foreign_columns)
                            ) as row from relations ff
                            where t.table_schema = ff.table_schema and t.table_name = ff.table_name
                        ) f
                    ),
                    'permissions', (
                        select json_group_array(json(p.row)) from (
                            select json_object(
                                'name', pp.name,
                                'restrictive', coalesce(pp.restrictive,0),
                                'role', pp.role,
                                'policy_for', json(pp.policy_for),
                                'check', json(pp."check"),
                                'using', json(pp."using"),
                                'grant', json(pp.grant),
                                'columns', json(pp.columns)
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
