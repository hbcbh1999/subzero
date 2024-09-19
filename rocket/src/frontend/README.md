frontend in this context means the code takes in the HTTP request and parses it into an internal representation according to a particular dialect (PostgREST for now).

After the request is parsed, the resulting AST (`ApiRequest`) is passed to a `Backend` which is responsible for transforming the AST into a SQL query and executing it against the database.