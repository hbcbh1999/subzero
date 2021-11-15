\set api_schemas '''{test}'''
with recursive
      pks_fks as (
        -- pk + fk referencing col
        select
          conrelid as resorigtbl,
          unnest(conkey) as resorigcol
        from pg_constraint
        where contype IN ('p', 'f')
        union
        -- fk referenced col
        select
          confrelid,
          unnest(confkey)
        from pg_constraint
        where contype='f'
      ),
      views as (
        select
          c.oid       as view_id,
          n.nspname   as view_schema,
          c.relname   as view_name,
          r.ev_action as view_definition
        from pg_class c
        join pg_namespace n on n.oid = c.relnamespace
        join pg_rewrite r on r.ev_class = c.oid
        where c.relkind in ('v', 'm') and n.nspname = ANY(:api_schemas)
      ),
      transform_json as (
        select
          view_id, view_schema, view_name,
          -- the following formatting is without indentation on purpose
          -- to allow simple diffs, with less whitespace noise
          replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            regexp_replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
            replace(
              view_definition::text,
            -- This conversion to json is heavily optimized for performance.
            -- The general idea is to use as few regexp_replace() calls as possible.
            -- Simple replace() is a lot faster, so we jump through some hoops
            -- to be able to use regexp_replace() only once.
            -- This has been tested against a huge schema with 250+ different views.
            -- The unit tests do NOT reflect all possible inputs. Be careful when changing this!
            -- -----------------------------------------------
            -- pattern           | replacement         | flags
            -- -----------------------------------------------
            -- `,` is not part of the pg_node_tree format, but used in the regex.
            -- This removes all `,` that might be part of column names.
               ','               , ''
            -- The same applies for `{` and `}`, although those are used a lot in pg_node_tree.
            -- We remove the escaped ones, which might be part of column names again.
            ), E'\\{'            , ''
            ), E'\\}'            , ''
            -- The fields we need are formatted as json manually to protect them from the regex.
            ), ' :targetList '   , ',"targetList":'
            ), ' :resno '        , ',"resno":'
            ), ' :resorigtbl '   , ',"resorigtbl":'
            ), ' :resorigcol '   , ',"resorigcol":'
            -- Make the regex also match the node type, e.g. `{QUERY ...`, to remove it in one pass.
            ), '{'               , '{ :'
            -- Protect node lists, which start with `({` or `((` from the greedy regex.
            -- The extra `{` is removed again later.
            ), '(('              , '{(('
            ), '({'              , '{({'
            -- This regex removes all unused fields to avoid the need to format all of them correctly.
            -- This leads to a smaller json result as well.
            -- Removal stops at `,` for used fields (see above) and `}` for the end of the current node.
            -- Nesting can't be parsed correctly with a regex, so we stop at `{` as well and
            -- add an empty key for the followig node.
            ), ' :[^}{,]+'       , ',"":'              , 'g'
            -- For performance, the regex also added those empty keys when hitting a `,` or `}`.
            -- Those are removed next.
            ), ',"":}'           , '}'
            ), ',"":,'           , ','
            -- This reverses the "node list protection" from above.
            ), '{('              , '('
            -- Every key above has been added with a `,` so far. The first key in an object doesn't need it.
            ), '{,'              , '{'
            -- pg_node_tree has `()` around lists, but JSON uses `[]`
            ), '('               , '['
            ), ')'               , ']'
            -- pg_node_tree has ` ` between list items, but JSON uses `,`
            ), ' '             , ','
            -- `<>` in pg_node_tree is the same as `null` in JSON, but due to very poor performance of json_typeof
            -- we need to make this an empty array here to prevent json_array_elements from throwing an error
            -- when the targetList is null.
            ), '<>'              , '[]'
          )::json as view_definition
        from views
      ),
      target_entries as(
        select
          view_id, view_schema, view_name,
          json_array_elements(view_definition->0->'targetList') as entry
        from transform_json
      ),
      results as(
        select
          view_id, view_schema, view_name,
          (entry->>'resno')::int as view_column,
          (entry->>'resorigtbl')::oid as resorigtbl,
          (entry->>'resorigcol')::int as resorigcol
        from target_entries
      ),
      recursion as(
        select r.*
        from results r
        where view_schema = ANY (:api_schemas)
        union all
        select
          view.view_id,
          view.view_schema,
          view.view_name,
          view.view_column,
          tab.resorigtbl,
          tab.resorigcol
        from recursion view
        join results tab on view.resorigtbl=tab.view_id and view.resorigcol=tab.view_column
      )
      select
        sch.nspname as table_schema,
        tbl.relname as table_name,
        col.attname as table_column_name,
        rec.view_schema,
        rec.view_name,
        vcol.attname as view_column_name
      from recursion rec
      join pg_class tbl on tbl.oid = rec.resorigtbl
      join pg_attribute col on col.attrelid = tbl.oid and col.attnum = rec.resorigcol
      join pg_attribute vcol on vcol.attrelid = rec.view_id and vcol.attnum = rec.view_column
      join pg_namespace sch on sch.oid = tbl.relnamespace
      join pks_fks using (resorigtbl, resorigcol)
      order by view_schema, view_name, view_column_name;