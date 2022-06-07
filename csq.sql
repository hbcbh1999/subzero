with 
schemas as (
  select name from system.databases
  where name not in ('system', 'information_schema', 'INFORMATION_SCHEMA')
  and name in ('default')
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
    and database in ('default')
    and is_temporary = 0
),

columns as (
  select
    database,
    table,
    name,
    type,
    comment,
    is_in_primary_key
  from system.columns
  where
    database not in ('system', 'information_schema', 'INFORMATION_SCHEMA')
    and database in ('default')
),

json_schema as (
  select cast(
    tuple(schemas_agg.array_agg),
    concat(
            'Tuple(', 
            'schemas ', toTypeName(schemas_agg.array_agg),
            ')'
        )
  ) as json_schema
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
              tuple(t.name, t.kind, t.columns),
              concat(
                'Tuple(',
                'name ', toTypeName(t.name), ',',
                'kind ', toTypeName(t.kind), ',',
                'columns ', toTypeName(t.columns),
                ')'
              )
            )
          ) as objects
        from schemas s
        left join (
          select
            tt.database,
            tt.name,
            case tt.is_view
              when true then 'view'
              else 'table'
            end as kind,
            groupArray(
              cast(
                tuple(c.name, c.type, c.primary_key),
                concat(
                  'Tuple(',
                  'name ', toTypeName(c.name), ',',
                  'type ', toTypeName(c.type), ',',
                  'primary_key ', toTypeName(c.primary_key),
                  ')'
                )
              )
            ) as columns
          from tables tt
          left join (
            select
              database,
              table,
              name,
              type,
              is_in_primary_key as primary_key
            from columns tc
          ) c on c.database = tt.database and c.table = tt.name
          group by tt.database, tt.name, tt.is_view
        
        ) t on s.name = t.database
        group by s.name
      )
      
    )
  ) schemas_agg
)

select json_schema from json_schema
format JSONEachRow
settings output_format_json_named_tuples_as_objects=1