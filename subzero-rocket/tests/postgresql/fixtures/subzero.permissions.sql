-- drop role if exists alice, bob;
-- create role alice;
-- create role bob;
-- grant alice, bob TO :USER;

set search_path = public;
drop table if exists permissions_check;
drop table if exists permissions_check_child;

create table permissions_check (
    id int primary key,
    value text,
    hidden text,
    role text,
    public boolean
);

create table permissions_check_child (
    id int primary key,
    value text,
    role text,
    public boolean,
    parent_id int, -- references permissions_check(id)
    foreign key (parent_id) references permissions_check(id)
);
-- grant all on table permissions_check to postgrest_test_anonymous, alice, bob;


insert into permissions_check values (1, 'One Alice Public', 'Hidden', 'alice', true);
insert into permissions_check values (2, 'Two Bob Public', 'Hidden', 'bob', true);
insert into permissions_check values (3, 'Three Charlie Public', 'Hidden', 'charlie', true);
insert into permissions_check values (10, 'Ten Alice Private', 'Hidden', 'alice', false);
insert into permissions_check values (11, 'Eleven Alice Private', 'Hidden', 'alice', false);
insert into permissions_check values (20, 'Twenty Bob Private', 'Hidden', 'bob', false);
insert into permissions_check values (21, 'Twenty One Bob Private', 'Hidden', 'bob', false);

insert into permissions_check_child values (1, 'One Alice Public', 'alice', true, 1);
insert into permissions_check_child values (2, 'Two Bob Public', 'bob', true, 2);
insert into permissions_check_child values (3, 'Three Charlie Public', 'charlie', true, 3);
insert into permissions_check_child values (10, 'Ten Alice Private', 'alice', false, 10);
insert into permissions_check_child values (11, 'Eleven Alice Public', 'alice', true, 10);
insert into permissions_check_child values (12, 'Twelve Alice Public', 'alice', true, 10);
insert into permissions_check_child values (13, 'Thirteen Alice Private', 'alice', false, 10);
insert into permissions_check_child values (20, 'Twenty Bob Private', 'bob', false, 20);
insert into permissions_check_child values (21, 'Twenty One Bob Public', 'bob', true, 20);
insert into permissions_check_child values (22, 'Twenty Two Bob Public', 'bob', true, 20);
insert into permissions_check_child values (23, 'Twenty Three Bob Private', 'bob', false, 20);
