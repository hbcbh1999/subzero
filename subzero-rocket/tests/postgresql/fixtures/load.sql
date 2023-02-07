-- Loads all fixtures for the PostgREST tests

\set ON_ERROR_STOP on

\ir sql/database.sql
\ir sql/roles.sql
\ir sql/schema.sql
\ir sql/jwt.sql
\ir sql/jsonschema.sql
\ir sql/privileges.sql
\ir sql/data.sql

\ir sql/subzero.request.sql
\ir sql/subzero.firebase.sql
\ir sql/subzero.basecamp.sql
\ir sql/subzero.cities.sql
\ir sql/subzero.odata.sql
\ir sql/subzero.aggregates.sql
\ir sql/subzero.custom_relations.sql
\ir sql/subzero.permissions.sql
\ir sql/subzero.basic.sql
