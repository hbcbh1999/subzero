package com.subzero.spring;

import com.subzero.swig.sbz_Statement;
import com.subzero.swig.sbz_DbSchema;
import com.subzero.swig.sbz_HTTPRequest;

import javax.sql.DataSource;

//import java.util.Date;
import java.sql.Date;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.Enumeration;
import java.util.HashMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
// import Optional from java.util
import java.util.Optional;


/**
 * The main class for Subzero. It is used to handle HTTP requests in the context of a spring application.
 */
public class Subzero {
    private sbz_DbSchema dbSchema;
    private DataSource dataSource;
    private String dbType;
    public String dbSchemaJson;
    private String demoModeMessage = "Subzero is running in demo mode. It will stop working after 15 minutes";


    /**
     * Constructor for Subzero, used when the json schema is already known (for example when it's cached in a file)
     * @param dataSource the data source to connect to the database
     * @param dbType the type of the database (postgresql, mysql, etc)
     * @param dbSchemaJson json representation of the database schema
     * @param licenseKey the license key for subzero (use null for demo mode)
     */
    public Subzero(DataSource dataSource, String dbType, String dbSchemaJson, String licenseKey) {
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.dbSchemaJson = dbSchemaJson;
        this.dbSchema = new sbz_DbSchema(dbType, dbSchemaJson, licenseKey);
        if (this.dbSchema.isDemo()) {
            System.out.println(demoModeMessage);
        }
    }

    /**
     * Constructor for Subzero, used when the json schema is not known and needs to be introspected from the database
     * @param dataSource the data source to connect to the database
     * @param dbType the type of the database (postgresql, mysql, etc)
     * @param dbSchemas the schemas to introspect and expose for the REST API
     * @param introspectionQueryDir the directory where the introspection queries are stored
     * @param useInternalPermissions Set this to false if you want to rely on the database permissions only
     *        and bypass permission checks in Subzero.
     * @param customRelations custom relations to be added to the schema.
     *        This is useful when the introspection does not capture all the relations in the database (ex. with views)
     *        The parameters is a json string with the following format:
     *        <pre>{@code
     *        [
     *          {
     *            "constraint_name": "projects_client_id_fkey",
     *            "table_schema": "public",
     *            "table_name": "projects",
     *            "columns": ["client_id"],
     *            "foreign_table_schema": "public",
     *            "foreign_table_name": "clients",
     *            "foreign_columns": ["id"],
     *          },
     *          ...
     *        ]
     *        }</pre>
     * @param customPermissions custom permissions to be added to the schema. This is used when you don't want to 
     *        rely on the database permissions (database roles) to control access to the data. The permission system is 
     *        still similar to the database RBAC+RLS, but it's managed by Subzero. The parameters is a json string.
     *        For the format, see this link: https://github.com/subzerocloud/showcase/blob/main/flyio-sqlite-litefs/permissions.js 
     * @param licenseKey the license key for subzero (use null for demo mode)
     */
    public Subzero(
            DataSource dataSource,
            String dbType,
            String[] dbSchemas,
            String introspectionQueryDir,
            Boolean useInternalPermissions,
            String customRelations,
            String customPermissions,
            String licenseKey) {
        this.dataSource = dataSource;
        this.dbType = dbType;
        String introspectionQuery = com.subzero.swig.Subzero.sbz_introspection_query(
                dbType,
                introspectionQueryDir,
                customRelations,
                customPermissions);
        try {
            Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(introspectionQuery.replaceAll("\\$\\d+", "?"));
            if (this.dbType.equals("postgresql")) {
                Array dbSchemasArr = conn.createArrayOf("text", dbSchemas);
                // TODO: refactor the query with a CTE so that we have $1 only once
                ps.setArray(1, dbSchemasArr);
                ps.setArray(2, dbSchemasArr);
                ps.setArray(3, dbSchemasArr);
                ps.setArray(4, dbSchemasArr);
                ps.setArray(5, dbSchemasArr);
                ps.setArray(6, dbSchemasArr);
                ps.setBoolean(7, true);
                ps.setArray(8, dbSchemasArr);
                ps.setArray(9, dbSchemasArr);
                ps.setArray(10, dbSchemasArr);
                ps.setArray(11, dbSchemasArr);
            }
            if (this.dbType.equals("mysql")) {
                // schemas as json array string
                String dbSchemasStr = "[\"" + String.join("\",\"", dbSchemas) + "\"]";
                ps.setString(1, dbSchemasStr);
            }
            // sqlite does not support schemas and we don't need to pass them as parameters
            // TODO: implement this for clickhouse where we have one parameter p1

            ResultSet rs = ps.executeQuery();
            rs.next();
            String dbSchemaJson = rs.getString("json_schema");
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonSchema = objectMapper.readTree(dbSchemaJson);
            ((ObjectNode) jsonSchema).put("use_internal_permissions", useInternalPermissions);
            this.dbSchemaJson = objectMapper.writeValueAsString(jsonSchema);
            this.dbSchema = new sbz_DbSchema(this.dbType, this.dbSchemaJson, licenseKey);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (this.dbSchema.isDemo()) {
            System.out.println(demoModeMessage);
        }
    }

    /**
     * Handles an HTTP request. It will execute the SQL statement and return the result as a JSON object.
     * @param schema_name the name of the database schema for the current request. This has to be one of the schemas
     *        that were introspected and exposed by Subzero. In the context of PostgreSQL, this is the schema name, in the
     *        context of MySQL, this is the database name.
     * @param prefix the prefix for the url. For example when the url is /api/v1/employees?select=id,name,
     *        the prefix is /api/v1/ (including the trailing slash)
     * @param role the role for the current request.
     * @param req the HTTP request object
     * @param res the HTTP response object
     * @param max_rows the maximum number of rows that can be returned by a select statement.
     * This should be an integer in string format. Use null for no limit.
     * @param env the environment variables for the current request. This is an array of strings in the format
     * ["name1", "value1", "name2", "value2", ...]. This is useful for passing to the SQL context aditional
     * information that is not part of the request itself.
     * Example:
     * 
     * <pre>{@code
     * [
     *   "request.method", "GET",
     *   "request.path", "/api/v1/employees",
     *   "request.jwt.claims", "{\"role\":\"admin\"}",
     *   "role", "admin"
     * ]
     * }</pre>
     */
    public void handleRequest(String schema_name, String prefix, String role, HttpServletRequest req,
            HttpServletResponse res,
            String[] env, String max_rows) {
        Connection conn = null;
        try {
            String method = req.getMethod();
            String uri; // = req.getRequestURI();
            StringBuffer requestURL = req.getRequestURL();
            String queryString = req.getQueryString();

            if (queryString == null) {
                uri = requestURL.toString();
            } else {
                uri = requestURL.append('?').append(queryString).toString();
            }
            String body = null;
            if (method == "POST" || method == "PUT" || method == "PATCH" || method == "DELETE") {
                body = req.getReader().lines().reduce("", (accumulator, actual) -> accumulator + actual);
            }

            Enumeration<String> headerNames = req.getHeaderNames();
            int headerCount = 0;
            while (headerNames.hasMoreElements()) {
                headerNames.nextElement();
                headerCount++;
            }

            // Adjusting for name-value pairs, so we double the size.
            String[] headers = new String[headerCount * 2];

            // Obtain a fresh enumeration to iterate through headers for population.
            headerNames = req.getHeaderNames();
            int i = 0;
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                headers[i++] = headerName;
                headers[i++] = req.getHeader(headerName);
            }
            headerCount = headers.length;

            int envCount = env.length;

            // throw new RuntimeException("test");
            // return;
            // System.out.println("Java env: [" + String.join(", ", env) + "]");
            sbz_HTTPRequest request = new sbz_HTTPRequest(method, uri, body, headers, headerCount, env, envCount);
            sbz_Statement statement = sbz_Statement.mainStatement(schema_name, prefix, role, this.dbSchema, request,
                    max_rows);
            conn = this.dataSource.getConnection();
            // start the transaction
            conn.setReadOnly(true);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            switch (this.dbType) {
                // these databases support the env statement
                case "postgresql":
                case "mysql":
                    if(this.dbType.equals("mysql")) {
                        PreparedStatement ps = conn.prepareStatement("set role ?");
                        ps.setString(1, role);
                        ps.executeQuery();
                    }
                    sbz_Statement envStatement = sbz_Statement.envStatement(dbSchema, request);
                    executeStatement(envStatement, conn);
                    
                break;
            }

            ResultSet rs = executeStatement(statement, conn);
            rs.next();
            String bodyColumn = rs.getString("body");
            res.setStatus(200);
            res.setHeader("Content-Type", "application/json");
            res.getWriter().write(bodyColumn);
            conn.commit();
            conn.close();
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                }
            }
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private ResultSet executeStatement(sbz_Statement statement, Connection conn) {
        String sql = statement.getSql().replaceAll("\\$\\d+", "?");
        String[] params = statement.getParams();
        String[] paramsTypes = statement.getParamsTypes();
        // System.out.println("SQL: " + sql);
        // System.out.println("Params: [" + String.join(", ", params) + "]");
        // System.out.println("ParamsTypes: [" + String.join(", ", paramsTypes) + "]");
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int j = 0; j < params.length; j++) {
                Object param = stringParamToJavaType(params[j], paramsTypes[j]);
                ps.setObject(j + 1, param);
            }
            ResultSet rs = ps.executeQuery();
            return rs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Object stringParamToJavaType(String param, String type) {
        switch (type.toLowerCase()) {
            case "text":
                return param;
            case "integer":
            case "int":
            case "smallint":
            case "bigint":
            case "int2":
            case "int4":
            case "int8":
            case "serial":
            case "bigserial":
                return Integer.parseInt(param);
            case "float":
            case "double":
            case "real":
            case "numeric":
                return Double.parseDouble(param);
            case "boolean":
            case "bool":
                return Boolean.parseBoolean(param);
            case "date":
            case "time":
            case "timestamp":
                return Date.valueOf(param);
            default:
                return param;
        }
    }

    /**
     * Returns the default environment variables for a request that can be passed to the handleRequest method
     * and passed to the SQL context. The environment variables set are the following:
     * - role: the role for the current request. In the context of PostgreSQL, this this env variable will
     *   change the current role for the session.
     * - request.method: the HTTP method for the current request
     * - request.path: the path for the current request
     * - request.headers: the headers for the current request, single parameter as a json string
     * - request.get: the query parameters for the current request, single parameter as a json string
     * - request.jwt.claims: the JWT claims for the current request, single parameter as a json string
     * @param role the user role for the current request
     * @param request the HTTP request object
     * @param jwtClaims the JWT claims for the current request
     * @return a map with the environment variables. While handleRequest expects String[], this method returns a map
     * for easier manipulation. The map can be converted to a String[] using the following code:
     * <pre>{@code
     * HashMap<String, String> env = subzero.getEnv("alice", req, Optional.of(jwtClaims));
     * String[] envArray = new String[env.size() * 2];
     * int i = 0;
     * for (String key : env.keySet()) {
     *    envArray[i++] = key;
     *    envArray[i++] = env.get(key);
     * }
     * }</pre>
     */
    public HashMap<String, String> getEnv(String role, HttpServletRequest request, Optional<ObjectNode> jwtClaims) {
        HashMap<String, String> env = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        if (role != null) {
            env.put("role", role);
        }

        switch (this.dbType) {
            case "mysql":
                env.put("subzero_ids", "[]");
                env.put("subzero_ignored_ids", "[]");
                break;
        }

        env.put("request.method", request.getMethod());
        env.put("request.path", request.getServletPath()); // Adjusted to use getServletPath for path

        HashMap<String, String> headers = new HashMap<>();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            headers.put(headerName, request.getHeader(headerName));
        }

        jwtClaims.ifPresent(claims -> {
            env.put("request.jwt.claims", claims.toString());
        });

        try {
            env.put("request.headers", objectMapper.writeValueAsString(headers));
            env.put("request.get", objectMapper.writeValueAsString(request.getParameterMap()));

            if (!jwtClaims.isPresent() && role != null) {
                HashMap<String, String> claims = new HashMap<>();
                claims.put("role", role);
                env.put("request.jwt.claims", objectMapper.writeValueAsString(claims)); // Using Gson to convert map to json string
            } else if (!jwtClaims.isPresent()) {
                env.put("request.jwt.claims", "{}");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return env;
    }
    
    private static final String LIB_BIN = "/";
    private final static String SUBZEROJNI = "subzerojni";
    
    static {
        try {
            System.loadLibrary(SUBZEROJNI);
        } catch (UnsatisfiedLinkError e) {
            loadFromJar();
        }
    }
    /**
     * When packaged into JAR extracts DLLs, places these into
     */
    private static void loadFromJar() {
        // we need to put both DLLs to temp dir
        String path = "SUBZERO_" + new java.util.Date().getTime();
        loadLib(path, SUBZEROJNI);
    }
    /**
     * Puts library to temp dir and loads to memory
     */
    private static void loadLib(String path, String name) {
        // depending on the OS, add the right extension
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            os = "windows";
        } else if (os.contains("nix") || os.contains("nux")) {
            os = "linux";
        }
        else if (os.contains("mac")) {
            os = "mac";
        } else {
            throw new RuntimeException("Unsupported OS: " + os);
        }
        switch (os) {
            case "linux":
                name = "lib" + name + ".so";
                break;
            case "mac":
                name = "lib" + name + ".dylib";
                break;
            case "windows":
                name = name + ".dll";
                break;
        }
        try {
            InputStream in = Subzero.class.getResourceAsStream(LIB_BIN + name);
            if (in == null) {
                throw new RuntimeException("Cannot find for os " + os + "-" + LIB_BIN + name);
            }
            File fileOut = new File(System.getProperty("java.io.tmpdir") + "/" + path + LIB_BIN + name);
            File parentDir = fileOut.getParentFile();
            if (!parentDir.exists() && !parentDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + parentDir);
            }
            try (OutputStream out = new FileOutputStream(fileOut)) {
                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            } finally {
                in.close();
            }
            
            System.load(fileOut.toString());
            fileOut.deleteOnExit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load required subzero native library", e);
        }
    }

}
