drop table if exists sample_users_tasks;
drop table if exists sample_users;
drop table if exists sample_tasks;
drop table if exists sample_projects;
drop table if exists sample_clients;




create table sample_clients (
    id UInt16,
    name text NOT NULL,
    primary key (id)
) ENGINE = MergeTree();

create table sample_projects (
    id UInt16,
    name text NOT NULL,
    client_id UInt16, -- references clients(id),
    primary key (id)
) ENGINE = MergeTree();


create table sample_tasks (
    id UInt16,
    name text NOT NULL,
    project_id UInt16, -- references projects(id),
    primary key (id)
) ENGINE = MergeTree();

create table sample_users (
    id UInt16,
    name text NOT NULL,
    primary key (id)
) ENGINE = MergeTree();

create table sample_users_tasks (
  user_id UInt16 NOT NULL, -- references users(id),
  task_id UInt16 NOT NULL, -- references tasks(id),
  primary key (task_id, user_id)
) ENGINE = MergeTree();


insert into sample_clients SELECT id, name FROM generateRandom('id UInt16, name text', 1, 10) LIMIT 10000000;

insert into sample_users SELECT id, name FROM generateRandom('id UInt16, name text', 1, 10) LIMIT 10000000;

insert into sample_projects SELECT id, name, client_id FROM generateRandom('id UInt16, name text, client_id UInt16', 1, 10) LIMIT 10000000;

insert into sample_tasks SELECT id, name, project_id FROM generateRandom('id UInt16, name text, project_id UInt16', 1, 10) LIMIT 10000000;

insert into sample_users_tasks SELECT * FROM generateRandom('user_id UInt16, task_id UInt16', 1, 10) LIMIT 10000000;





