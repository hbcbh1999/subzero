CREATE TABLE no_fk_projects (
    id integer NOT NULL,
    name text NOT NULL,
    client_id integer
);

GRANT ALL ON TABLE no_fk_projects TO postgrest_test_anonymous;


TRUNCATE TABLE no_fk_projects CASCADE;
INSERT INTO no_fk_projects VALUES (1, 'Windows 7', 1);
INSERT INTO no_fk_projects VALUES (2, 'Windows 10', 1);
INSERT INTO no_fk_projects VALUES (3, 'IOS', 2);
INSERT INTO no_fk_projects VALUES (4, 'OSX', 2);
INSERT INTO no_fk_projects VALUES (5, 'Orphan', NULL);