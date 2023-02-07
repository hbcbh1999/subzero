set search_path = test, public, pg_catalog;

create table states (
    id         text primary key default firebase.new_id(),
    name       text unique,
    name_long  text
);

create table cities (
    id         text primary key default firebase.new_id(),
    name       text,
    state      text references states(name),
    county     text,
    alias      text
);

create table cities_temp (
    name       text,
    state      text,
    state_long text,
    county     text,
    alias      text
);

--\copy cities_temp from './test/fixtures/subzero.cities.csv' with delimiter '|' csv header;

insert into states (name, name_long)
select distinct on (state) state, state_long from cities_temp;

-- insert into cities (name, state, county, alias)
-- select name, state, county, alias from cities_temp;

drop table cities_temp;

grant usage on schema test to webuser;
grant all on table cities, states to postgrest_test_anonymous, webuser;
