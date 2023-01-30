-- select 
-- cast(
--   json_extract(
--     '{"foo": {"int": 10}}', 
--     '$.foo.int'
--   )
--   as unsigned
-- )


begin;

set @subzero_ids := '[]';
set @ignored_ids := '[]';

insert into clients (
  id, 
  name
)

with
subzero_payload as (
  select '[{"id": 1, "name":"new client"}, {"id": 3, "name":"new client3"}]' as val
  -- select '[{"name":"new client 5"}, { "name":"new client 6"}]' as val
  -- select '{"name":"name updated"}' as val
), 
subzero_body as (
  select t.*
    from subzero_payload p,
    json_table(
        case when json_type(p.val) = 'ARRAY' then p.val else concat('[',p.val,']') end,
        '$[*]'
        columns(
            id text path '$.id',
            name text path '$.name'
        )
    ) t
)

-- update clients _, subzero_body
-- set _.name = subzero_body.name
-- where id > 0
-- and ( (@ids := json_array_append(@ids, '$', id)) <> '[]' )
-- ;


select 
  if( ( (@subzero_ids := json_array_append(@subzero_ids, '$', id)) <> null ), id, id), 
  name
from subzero_body
on duplicate key update
  id = if( ( (@ignored_ids := json_array_append(@ignored_ids, '$', values(id))) <> null ), values(id), values(id))
    -- id = values(id)
    -- , name = values(name)
;

select * from clients;

select @subzero_ids as "affected_ids";
select @ignored_ids as "ignored_ids";

select t.val 
from
  json_table(
    @subzero_ids, 
    '$[*]' columns (val integer path '$')
) as t
left join json_table(
    @ignored_ids, 
    '$[*]' columns (val integer path '$')
) as t2 on t.val = t2.val
where t2.val is null;

rollback;


-- with  _subzero_query as (
--   select 
--     json_object(
--       'id', `subzero_source`.`id`, 'name', 
--       `subzero_source`.`name`
--     ) as row_ 
--   from 
--     `app`.`clients` as `subzero_source`
--   where 
--     `subzero_source`.`id` = any (
--       select 
--         * 
--       from 
--         json_table(
--           '[]', 
--           '$[*]' columns (val text path '$')
--         ) as t
--     )
-- ), 
-- _subzero_count_query AS (
--   select 
--     1
-- ) 
-- select 
--   count(*) as page_total, 
--   null as total_result_set, 
--   json_arrayagg(_subzero_t.row_) as body, 
--   true as constraints_satisfied, 
--   nullif(@response.headers, '') as response_headers, 
--   nullif(@response.status, '') as response_status 
-- from 
--   (
--     select 
--       * 
--     from 
--       _subzero_query
--   ) _subzero_t



-- with
-- subzero_payload as (
--   select '{"id": 1, "name":"new client"}' as val
--   -- select '[{"id": 1, "name":"new client"}, {"id": 2, "name":"new client2"}]' as val
-- ), 
-- subzero_body as (
--   select t.*
--     from subzero_payload p,
--     json_table(
--         case when json_type(p.val) = 'ARRAY' then p.val else concat('[',p.val,']') end,
--         '$[*]'
--         columns(
--             id text path '$.id',
--             name text path '$.name'
--         )
--     ) t
-- )

-- select * from subzero_body




-- set @ids := null;
-- update permissions_check
--     set hidden = 'Hidden changed'
-- where id = 20
--     and ( (@ids := concat_ws(',', id, @ids)) <> null )
-- ;
-- select @ids;

-- select * from permissions_check;




-- with payload as (
--     select 
--     '[
--         {"id":1,"name":"one"},
--         {"id":2,"name":"two"},
--         {"id":3,"name":"three"},
--         {"id":4,"name":"four"},
--         {"id":5,"name":"five"},
--         {"id":null,"name":"six"},
--         {"id":7,"name":"seven"},
--         {"id":8,"name":"eight"},
--         {"id":9,"name":null},
--         {"id":10,"name":"ten"},
--         {"id":11,"name":"eleven"}
--     ]' as val
-- )

-- select val->"$[*].id" as id, val->"$[*].name" as name
-- from payload
