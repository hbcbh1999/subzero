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
    parent_id int references permissions_check(id)
);

create table clients (
    id integer primary key,
    name text NOT NULL
);

create table projects (
    id integer primary key,
    name text NOT NULL,
    client_id integer references clients(id)
);

create view projects_view as select * from projects;

create table tasks (
    id integer primary key,
    name text NOT NULL,
    project_id integer references projects(id)
);

create table users (
    id integer primary key,
    name text NOT NULL
);

create table users_tasks (
  user_id integer NOT NULL references users(id),
  task_id integer NOT NULL references tasks(id),
  primary key (task_id, user_id)
);

create table complex_items (
    id integer NOT NULL,
    name text,
    settings text
);


