create or replace function public.odata_context() returns text as $$
    select current_setting('request.header.OData-context', true);
$$ stable language sql;

create or replace function public.odata_type(t text) returns text as $$
    select concat(odata_type_prefix(),t);
$$ stable language sql;

create or replace function public.odata_id(t text, i anyelement) returns text as $$
    select concat(t,'(''',i::text,''')');
$$ stable language sql;

create or replace function public.odata_id_40() returns boolean as $$
select 
    case 
    when current_setting('request.header.OData-Version', true) = '4.0' then true
    else false
    end;
$$ stable language sql;

create or replace function public.odata_type_prefix() returns text as $$
select 
    case 
    when odata_id_40() then '#'
    else ''
    end;
$$ stable language sql;

-- this is handled in the query directly
-- create or replace function public.odata_count() returns text as $$
--     select 'not supported';
-- $$ stable language sql;

create or replace function public."odata_nextLink"() returns text as $$
    select current_setting('request.header.OData-nextLink', true);
$$ stable language sql;

create or replace function public."odata_deltaLink"() returns text as $$
    select 'not supported';
$$ stable language sql;

create or replace function public."odata_etag"() returns text as $$
    select 'not supported';
$$ stable language sql;

create or replace function public."odata_readLink"() returns text as $$
    select 'not supported';
$$ stable language sql;

create or replace function public."odata_editLink"() returns text as $$
    select 'not supported';
$$ stable language sql;

create or replace function public."odata_navigationLink"() returns text as $$
    select 'not supported';
$$ stable language sql;

create or replace function public."odata_associationLink"() returns text as $$
    select 'not supported';
$$ stable language sql;

