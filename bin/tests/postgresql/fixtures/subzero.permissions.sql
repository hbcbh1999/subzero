-- drop role if exists alice, bob;
-- create role alice;
-- create role bob;
-- grant alice, bob TO :USER;

set search_path = public;
drop table if exists permissions_check;

create table permissions_check (
    id int primary key,
    value text,
    hidden text,
    role text,
    public boolean
);
-- grant all on table permissions_check to postgrest_test_anonymous, alice, bob;


insert into permissions_check values (1, 'One Alice Public', 'Hidden', 'alice', true);
insert into permissions_check values (2, 'Two Bob Public', 'Hidden', 'bob', true);
insert into permissions_check values (3, 'Three Charlie Public', 'Hidden', 'charlie', true);
insert into permissions_check values (10, 'Ten Alice Private', 'Hidden', 'alice', false);
insert into permissions_check values (20, 'Twenty Bob Private', 'Hidden', 'bob', false);
