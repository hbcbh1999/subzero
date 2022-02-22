create table tbl1(one varchar(10), two smallint);
insert into tbl1 values('hello!',10);
insert into tbl1 values('goodbye', 20);


CREATE TABLE clients (
    id integer primary key,
    name text NOT NULL
);

CREATE TABLE projects (
    id integer primary key,
    name text NOT NULL,
    client_id integer REFERENCES clients(id)
);

CREATE TABLE tasks (
    id integer primary key,
    name text NOT NULL,
    project_id integer REFERENCES projects(id)
);

CREATE TABLE users (
    id integer primary key,
    name text NOT NULL
);

CREATE TABLE users_tasks (
  user_id integer NOT NULL REFERENCES users(id),
  task_id integer NOT NULL REFERENCES tasks(id),
  primary key (task_id, user_id)
);

CREATE TABLE complex_items (
    id integer NOT NULL,
    name text,
    settings text
);


INSERT INTO clients VALUES (1, 'Microsoft');
INSERT INTO clients VALUES (2, 'Apple');

INSERT INTO users VALUES (1, 'Angela Martin');
INSERT INTO users VALUES (2, 'Michael Scott');
INSERT INTO users VALUES (3, 'Dwight Schrute');

INSERT INTO projects VALUES (1, 'Windows 7', 1);
INSERT INTO projects VALUES (2, 'Windows 10', 1);
INSERT INTO projects VALUES (3, 'IOS', 2);
INSERT INTO projects VALUES (4, 'OSX', 2);
INSERT INTO projects VALUES (5, 'Orphan', NULL);

INSERT INTO tasks VALUES (1, 'Design w7', 1);
INSERT INTO tasks VALUES (2, 'Code w7', 1);
INSERT INTO tasks VALUES (3, 'Design w10', 2);
INSERT INTO tasks VALUES (4, 'Code w10', 2);
INSERT INTO tasks VALUES (5, 'Design IOS', 3);
INSERT INTO tasks VALUES (6, 'Code IOS', 3);
INSERT INTO tasks VALUES (7, 'Design OSX', 4);
INSERT INTO tasks VALUES (8, 'Code OSX', 4);

INSERT INTO users_tasks VALUES (1, 1);
INSERT INTO users_tasks VALUES (1, 2);
INSERT INTO users_tasks VALUES (1, 3);
INSERT INTO users_tasks VALUES (1, 4);
INSERT INTO users_tasks VALUES (2, 5);
INSERT INTO users_tasks VALUES (2, 6);
INSERT INTO users_tasks VALUES (2, 7);
INSERT INTO users_tasks VALUES (3, 1);
INSERT INTO users_tasks VALUES (3, 5);


INSERT INTO complex_items VALUES (1, 'One', '{"foo":{"int":1,"bar":"baz"}}');
INSERT INTO complex_items VALUES (2, 'Two', '{"foo":{"int":1,"bar":"baz"}}');
INSERT INTO complex_items VALUES (3, 'Three', '{"foo":{"int":1,"bar":"baz"}}');