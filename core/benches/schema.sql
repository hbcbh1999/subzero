-- tasks table
CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    done BOOLEAN NOT NULL DEFAULT FALSE
);

-- tasks data
INSERT INTO tasks (title, description, done) VALUES
    ('Task 1', 'Description 1', FALSE),
    ('Task 2', 'Description 2', TRUE),
    ('Task 3', 'Description 3', FALSE),
    ('Task 4', 'Description 4', TRUE),
    ('Task 5', 'Description 5', FALSE),
    ('Task 6', 'Description 6', TRUE),
    ('Task 7', 'Description 7', FALSE),
    ('Task 8', 'Description 8', TRUE),
    ('Task 9', 'Description 9', FALSE),
    ('Task 10', 'Description 10', TRUE);
