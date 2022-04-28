drop role if exists webuser;
create role webuser;
grant webuser to :USER;


drop schema if exists basecamp cascade;
create schema basecamp;
set search_path = basecamp, public;


create table "user" ( 
  id                   serial primary key,
  name                 text not null
);

create table client ( 
  id           serial primary key,
  name         text not null,
  user_id      int not null references "user"(id)
);


create table project ( 
  id           serial primary key,
  name         text not null,
  client_id    int not null references client(id),
  user_id      int not null references "user"(id)
);



create table task ( 
  id           serial primary key,
  name         text not null,
  completed    bool not null default false,
  project_id   int not null references project(id),
  user_id      int not null references "user"(id)
);





create index client_user_id_index on client(user_id);
create index project_user_id_index on project(user_id);
create index project_client_id_index on project(client_id);
create index task_user_id_index on task(user_id);
create index task_project_id_index on task(project_id);




grant usage on schema basecamp to webuser;
grant select on basecamp.user, basecamp.client, basecamp.project, basecamp.task to webuser;

alter table basecamp.user enable row level security;
alter table basecamp.client enable row level security;
alter table basecamp.project enable row level security;
alter table basecamp.task enable row level security;

create policy user_access_policy on basecamp.user to webuser using (id = request.user_id());
create policy client_access_policy on basecamp.client to webuser using (user_id = request.user_id());
create policy project_access_policy on basecamp.project to webuser using (user_id = request.user_id());
create policy task_access_policy on basecamp.task to webuser using (user_id = request.user_id());


begin;

select
    set_config('size.u', '3', true),
    set_config('size.c', '30', true),
    set_config('size.p', '300', true),
    set_config('size.t', '3000', true)
;

\echo inserting data

\echo add users
insert into basecamp.user(name)
select 'user_' || (s.i::text)
from generate_series(1, current_setting('size.u')::int) s(i);

\echo add clients

insert into basecamp.client(name, user_id)
select
    'client_' || (c.i::text) as name,
    p.user_id
from generate_series(1, current_setting('size.c')::int) c(i)
inner join (
    select row_number() OVER () as rnum, t.id as user_id
    from (select t.id, generate_series(1, current_setting('size.c')::int / current_setting('size.u')::int) from basecamp.user t ) t
) p
on c.i = p.rnum;

\echo add projects
insert into basecamp.project(name, client_id, user_id)
select
    'project_' || (c.i::text) as name,
    p.client_id,
    p.user_id
from generate_series(1, current_setting('size.p')::int) c(i)
inner join (
    select row_number() OVER () as rnum, t.id as client_id, t.user_id
    from (select t.id, t.user_id, generate_series(1, current_setting('size.p')::int / current_setting('size.c')::int) from basecamp.client t ) t
) p
on c.i = p.rnum;

\echo add tasks
insert into basecamp.task(name, project_id, user_id)
select
    'task_' || (c.i::text) as name,
    p.project_id,
    p.user_id
from generate_series(1, current_setting('size.t')::int) c(i)
inner join (
    select row_number() OVER () as rnum, t.id as project_id, t.user_id
    from (select t.id, t.user_id, generate_series(1, current_setting('size.t')::int / current_setting('size.p')::int) from basecamp.project t ) t
) p
on c.i = p.rnum;

commit;

\echo done inserting data



-- rollback;
-- begin;
-- set local role to webuser;
-- --explain -- (FORMAT JSON)
-- select 
--     _query.param->'id' as id,
--     _result.root as result,
--     md5(_result.root::text) as hash
-- from json_array_elements(current_setting('query.basecamp_params', true)::json) _query (param)
-- left outer join lateral (
--     with t as (
--         select data.set_env(_query.param) as env_set
--     )
--     select coalesce(json_agg("root"), '[]') as "root"
--     from t,  (
--         select row_to_json("_0_root") as "root"
--         from (
--             select name
--             from basecamp.project
--         ) as "_0_root"
--     ) as "_2_root"
-- ) _result on ('true')
-- where md5(_result.root::text) != _query.param->>'hash'
-- ;
-- commit;
