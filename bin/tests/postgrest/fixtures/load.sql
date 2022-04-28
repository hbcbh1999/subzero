-- Loads all fixtures for the PostgREST tests

\set ON_ERROR_STOP on

\ir database.sql
\ir roles.sql
\ir schema.sql
\ir jwt.sql
\ir jsonschema.sql
\ir privileges.sql
\ir data.sql

\ir subzero.request.sql
\ir subzero.firebase.sql
\ir subzero.basecamp.sql
\ir subzero.cities.sql
\ir subzero.odata.sql
\ir subzero.aggregates.sql
