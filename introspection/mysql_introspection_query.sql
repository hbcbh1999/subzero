with used_schemas as (
    select val
    from json_table(
        ?,
        '$[*]' columns ( val text path '$')
    ) as t
)
, schemas_ as (
    select schema_name
    from information_schema.schemata
    where schema_name not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    and schema_name = any( select val from used_schemas )
)
, tables as (
    select
        table_schema,
        table_name,
        case when table_type = 'BASE TABLE' then 'table' else 'view' end as kind
    from information_schema.tables
    where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    and table_schema = any( select val from used_schemas )
)
, columns as (
    select
        table_schema,
        table_name,
        column_name,
        data_type,
        case when column_key = 'PRI' then true else false end as primary_key
    from information_schema.columns
    where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
    and table_schema = any( select val from used_schemas )
)
, primary_keys as (
    select * from columns where primary_key = true
)
, foreign_keys as (
    select
        constraint_name,
        table_schema,
        table_name,
        json_array(column_name) as columns,
        referenced_table_schema,
        referenced_table_name,
        json_array(referenced_column_name) as referenced_columns
    from information_schema.key_column_usage
    where table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        and table_schema = any( select val as table_schema from used_schemas )
        and referenced_table_schema not in ('information_schema', 'mysql', 'performance_schema', 'sys')
        and referenced_table_name is not null
)
, custom_relations as (
    select
        constraint_name,
        table_schema,
        table_name,
        columns,
        foreign_table_schema,
        foreign_table_name,
        foreign_columns
    from json_table(
        '[]'--relations.json
        ,'$[*]' columns (
            constraint_name text path '$.constraint_name',
            table_schema text path '$.table_schema',
            table_name text path '$.table_name',
            columns json path '$.columns',
            foreign_table_schema text path '$.foreign_table_schema',
            foreign_table_name text path '$.foreign_table_name',
            foreign_columns json path '$.foreign_columns'
        )
    ) as t
)
, relations as (
    select * from custom_relations
    union
    select * from foreign_keys
)
, permissions as (
    select
        name,
        restrictive,
        table_schema,
        table_name,
        role,
        _grant as "grant",
        columns,
        policy_for,
        _check as "check",
        _using as "using"
    from json_table(
        '[]'--permissions.json
        ,'$[*]' columns (
            name text path '$.name',
            restrictive boolean path '$.restrictive',
            table_schema text path '$.table_schema',
            table_name text path '$.table_name',
            role text path '$.role',
            _grant json path '$.grant',
            columns json path '$.columns',
            policy_for json path '$.policy_for',
            _check json path '$.check',
            _using json path '$.using'
        )
    ) as t
)
select json_object(
    'use_internal_permissions', (select count(*) from permissions) > 0,
    'schemas', json_arrayagg(s.row)
) as json_schema
from (
    select json_object(
        'name', schema_name,
        'objects', coalesce((
            select json_arrayagg(o.row) from (
                select json_object(
                    'kind', t.kind, 
                    'name', t.table_name,
                    'columns', (
                        select json_arrayagg(c.row) from (
                            select json_object(
                                'name', cc.column_name,
                                'data_type', cc.data_type,
                                'primary_key', if(cc.primary_key, cast(true as json), cast(false as json))
                            ) as "row" from columns cc
                            where t.table_schema = cc.table_schema and t.table_name = cc.table_name
                        ) c
                    ),
                    'foreign_keys', coalesce((
                        select json_arrayagg(f.row) from (
                            select json_object(
                                'name', ff.constraint_name, 
                                'table', json_array(ff.table_schema, ff.table_name),
                                'columns', ff.columns,
                                'referenced_table', json_array(ff.foreign_table_schema,ff.foreign_table_name),
                                'referenced_columns', ff.foreign_columns
                            ) as "row" from relations ff
                            where t.table_schema = ff.table_schema and t.table_name = ff.table_name
                        ) f
                    ), json_array()),
                    'permissions', coalesce((
                        select json_arrayagg(p.row) from (
                            select json_object(
                                'name', pp.name,
                                'restrictive', coalesce(pp.restrictive,0),
                                'role', pp.role,
                                'policy_for', pp.policy_for,
                                'check', pp.check,
                                'using', pp.using,
                                'grant', pp.grant,
                                'columns', pp.columns
                            ) as "row" from permissions pp
                            where t.table_schema = pp.table_schema and t.table_name = pp.table_name
                        ) p
                    ), json_array())
                ) as "row"
                from tables t
                where s.schema_name = t.table_schema
            ) o
        ), json_array())
    ) as "row"
    from schemas_ s
) s