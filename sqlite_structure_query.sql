
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
      'constraint_name' as constraint_name,
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
)

select json_object('schemas',json_group_array(s.row)) from (
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
                                'name', ff."table_name" || '_' || json_extract(ff.columns, '$[0]') || '_fkey', 
                                'table', json_array(ff.table_schema,ff."table_name"),
                                'columns', ff.columns,
                                'referenced_table', json_array(ff.foreign_table_schema,ff.foreign_table_name),
                                'referenced_columns', ff.foreign_columns
                            ) as row from foreign_keys ff
                            where t.table_schema = ff.table_schema and t.table_name = ff.table_name
                        ) f
                    )
                ) as row from tables t
                where t.table_schema = schema_name
            ) o
        )
    ) as row from schemas
) s
