package cloud.subzero.rest;

import javax.sql.DataSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import cloud.subzero.SubzeroException;
import cloud.subzero.swig.sbz_DbSchema;
import cloud.subzero.swig.sbz_HTTPRequest;
import cloud.subzero.swig.sbz_Statement;
import cloud.subzero.swig.sbz_TwoStageStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The main class for Subzero. It is used to handle HTTP requests in the context of a spring application.
 */
public class RestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RestHandler.class);
    private sbz_DbSchema dbSchemaSbz;
    private DataSource dataSource;
    private String dbType;
    public String dbSchemaJson;
    public Map<String, Object> dbSchema;
    private String demoModeMessage = "Subzero is running in demo mode. It will stop working after 15 minutes";

    private static class InstantSerializer extends JsonSerializer<Instant> {
        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeNumber(value.getEpochSecond());
        }
    }

    /**
     * Constructor for Subzero, used when the json schema is already known (for example when it's cached in a file)
     * @param dataSource the data source to connect to the database
     * @param dbType the type of the database (postgresql, mysql, etc)
     * @param dbSchemaJson json representation of the database schema
     * @param licenseKey the license key for subzero (use null for demo mode)
     * @throws JsonProcessingException
     */
    @SuppressWarnings("unchecked")
    public RestHandler(DataSource dataSource, String dbType, String dbSchemaJson, String licenseKey)
    throws JsonProcessingException, SubzeroException {
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.dbSchemaJson = dbSchemaJson;
        this.dbSchemaSbz = new sbz_DbSchema(dbType, dbSchemaJson, licenseKey);
        ObjectMapper objectMapper = new ObjectMapper();
        
        this.dbSchema = objectMapper.readValue(dbSchemaJson, Map.class);
        
        if (this.dbSchemaSbz.isDemo()) {
            logger.info(demoModeMessage);
        }
    }

    /**
     * Constructor for Subzero, used when the json schema is not known and needs to be introspected from the database
     * @param dataSource the data source to connect to the database
     * @param dbType the type of the database (postgresql, mysql, etc)
     * @param dbSchemas the schemas to introspect and expose for the REST API
     * @param introspectionQueryDir the directory where the introspection queries are stored (optional, use null for default)
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
     * 
     * @throws SQLException
     * @throws JsonProcessingException
     */
    @SuppressWarnings("unchecked")
    public RestHandler(
            DataSource dataSource,
            String dbType,
            String[] dbSchemas,
            String introspectionQueryDir,
            Boolean useInternalPermissions,
            String customRelations,
            String customPermissions,
            String licenseKey)
        throws SQLException, JsonProcessingException, SubzeroException {
        this.dataSource = dataSource;
        this.dbType = dbType;
        String queryDir = null;
        if (introspectionQueryDir != null) {
            queryDir = introspectionQueryDir;
        } else {
            File file1 = extractResourceFile("/introspection/clickhouse_introspection_query.sql");
            File file2 = extractResourceFile("/introspection/mysql_introspection_query.sql");
            File file3 = extractResourceFile("/introspection/postgresql_introspection_query.sql");
            File file4 = extractResourceFile("/introspection/sqlite_introspection_query.sql");
            queryDir = file4.getParent();
            file1.deleteOnExit();
            file2.deleteOnExit();
            file3.deleteOnExit();
            file4.deleteOnExit();
            //System.out.println("Introspection queries extracted to " + queryDir);
        };

        String introspectionQuery = cloud.subzero.swig.Subzero.sbz_introspection_query(
                dbType,
                queryDir,
                customRelations,
                customPermissions);
        
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
        this.dbSchema = objectMapper.readValue(dbSchemaJson, Map.class);
        if (useInternalPermissions != null) {
            this.dbSchema.put("use_internal_permissions", useInternalPermissions);
        }
        this.dbSchemaJson = objectMapper.writeValueAsString(this.dbSchema);
        this.dbSchemaSbz = new sbz_DbSchema(this.dbType, this.dbSchemaJson, licenseKey);
        conn.close();

        if (this.dbSchemaSbz.isDemo()) {
            logger.info(demoModeMessage);
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
            String[] env, String max_rows) throws IOException, SubzeroException {
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
            Boolean isMutation = false;
            if ("POST".equals(method) || "PUT".equals(method) || "PATCH".equals(method) || "DELETE".equals(method)) {
                body = req.getReader().lines().reduce("", (accumulator, actual) -> accumulator + actual);
                isMutation = true;
            }

            Enumeration<String> headerNames = req.getHeaderNames();
            int headerCount = 0;
            while (headerNames.hasMoreElements()) {
                headerNames.nextElement();
                headerCount++;
            }

            String[] headers = new String[headerCount * 2];

            headerNames = req.getHeaderNames();
            int i = 0;
            while (headerNames.hasMoreElements()) {
                String headerName = headerNames.nextElement();
                headers[i++] = headerName;
                headers[i++] = req.getHeader(headerName);
            }
            headerCount = headers.length;

            int envCount = env.length;
            logger.debug("Creating sbz_HTTPRequest with:" +
                    " method: " + method +
                    " uri: " + uri +
                    " body (length): " + (body == null ? 0 : body.length()) +
                    " headerCount: " + headerCount +
                    " envCount: " + envCount);
            sbz_HTTPRequest request = new sbz_HTTPRequest(method, uri, body, headers, headerCount, env, envCount);
            sbz_Statement selecStatement = null;
            sbz_Statement mutateStatement = null;
            sbz_TwoStageStatement twoStageStatement = null;
            if (
                isMutation &&
                (this.dbType.equals("sqlite") || this.dbType.equals("mysql"))
                ) {
                // SQLite and MySQL do not support returning in a CTE, so we need to use a two stage statement
                twoStageStatement = new sbz_TwoStageStatement(schema_name, prefix, role, dbSchemaSbz, request,
                        max_rows);
                mutateStatement = twoStageStatement.mutateStatement();
            } else {
                selecStatement = sbz_Statement.mainStatement(schema_name, prefix, role, dbSchemaSbz, request, max_rows);
            }

            conn = this.dataSource.getConnection();
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            switch (this.dbType) {
                case "postgresql":
                case "mysql":
                    if (this.dbType.equals("mysql")) {
                        PreparedStatement ps = conn.prepareStatement("set role ?");
                        ps.setString(1, role);
                        ps.executeQuery();
                    }
                    sbz_Statement envStatement = sbz_Statement.envStatement(dbSchemaSbz, request);
                    executeStatement(envStatement, conn);
                    break;
            }

            ResultSet rs;
            if (isMutation && mutateStatement != null && twoStageStatement != null) {
                ResultSet rsMu = executeStatement(mutateStatement, conn);
                List<String> ids = new ArrayList<>();
                List<Boolean> constraintsSatisfied = new ArrayList<>();
                while (rsMu.next()) {
                    ids.add(rsMu.getString(1));
                    constraintsSatisfied.add(rsMu.getBoolean("_subzero_check__constraint"));
                }
                Boolean constraintsOk = constraintsSatisfied.stream().allMatch(c -> c == true);
                if (!constraintsOk) {
                    throw new SubzeroException("Permission denied", 403,
                            "check constraint of an insert/update permission has failed");
                }
                int idsSet = twoStageStatement.setIds(ids.toArray(new String[0]), ids.size());
                if (idsSet < 0) {
                    int l = cloud.subzero.swig.Subzero.sbz_last_error_length();
                    String buf = new String(new char[l]);
                    cloud.subzero.swig.Subzero.sbz_last_error_message(buf, l);
                    throw new SubzeroException("Error setting ids", 500, buf);
                }
                selecStatement = twoStageStatement.selectStatement();
                rs = executeStatement(selecStatement, conn);
            } else {
                rs = executeStatement(selecStatement, conn);
            }
            rs.next();
            Boolean constraintsSatisfied = rs.getBoolean("constraints_satisfied");
            if (constraintsSatisfied != null && !constraintsSatisfied) {
                throw new SubzeroException("Permission denied", 403,
                        "check constraint of an insert/update permission has failed");
            }

            int status = rs.getInt("response_status");
            if (rs.wasNull() || status == 0)
                status = 200;
            int pageTotal = rs.getInt("page_total");
            if (rs.wasNull())
                pageTotal = 0;
            int totalResultSet = rs.getInt("total_result_set");
            if (rs.wasNull())
                totalResultSet = -1;
            int offset = Integer.parseInt(req.getParameter("offset") == null ? "0" : req.getParameter("offset"));
            String responseHeadersStr = rs.getString("response_headers");
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> responseHeaders = responseHeadersStr != null
                    ? objectMapper.readValue(responseHeadersStr, Map.class)
                    : new HashMap<>();
            String bodyColumn = rs.getString("body");
            responseHeaders.put("content-length", String.valueOf(bodyColumn.getBytes(StandardCharsets.UTF_8).length));
            responseHeaders.put("content-type", "application/json;charset=UTF-8");
            responseHeaders.put("range-unit", "items");
            responseHeaders.put("content-range",
                    fmtContentRangeHeader(offset, offset + pageTotal - 1, totalResultSet < 0 ? null : totalResultSet));

            res.setStatus(status);
            for (String key : responseHeaders.keySet()) {
                res.setHeader(key, responseHeaders.get(key).toString());
            }
            conn.commit();
            conn.close();
            PrintWriter writer = res.getWriter();
            writer.write(bodyColumn);
        } catch (SQLException e) {
            logger.error("SQL error");
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                }
            }
            throw new RuntimeException(e);
        }
    }

    public static String fmtContentRangeHeader(int lower, int upper, Integer total) {
        String rangeString = (total != null && total != 0 && lower <= upper) ? lower + "-" + upper : "*";
        return total != null ? rangeString + "/" + total : rangeString + "/*";
    }

    private ResultSet executeStatement(sbz_Statement statement, Connection conn) {
        String sql = statement.getSql().replaceAll("\\$\\d+", "?");
        String[] params = statement.getParams();
        String[] paramsTypes = statement.getParamsTypes();
        logger.debug("Executing statement: " + sql);
        logger.debug("Params: [" + String.join(", ", params) + "]");
        logger.debug("ParamsTypes: [" + String.join(", ", paramsTypes) + "]");
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

    /*
     * Returns the schema for the current role.
     * This function is used to inform the UI about the (accessible) schema for the current role.
     * @param currentRole the role for the current request.
     * @param schemaName the name of the db schema (if you are exposing multiple schemas in the same database). Optional, use null for default.
     * The schema is returned in the following format:
     * <pre>{@code
     * {
     *      "projects": {
     *          "name": "projects",
     *          "columns": [
     *              {
     *                  "name": "id",
     *                  "data_type": "integer",
     *                  "primary_key": true
     *              },
     *              ...
     *          ],
     *          "foreign_keys": [
     *              {
     *                  "name": "projects_client_id_fkey",
     *                  "columns": ["client_id"],
     *                  "referenced_table": "clients",
     *                  "referenced_columns": ["id"]
     *              },
     *              ...
     *          ],
     *     },
     *     ...
     * }
     * }</pre>
     * 
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getSchema(String currentRole, String schemaName) {
        List<Map<String, Object>> allSchemas = (List<Map<String, Object>>) dbSchema.get("schemas");
        Map<String, Object> currentSchema = null;
        if (schemaName != null) {
            for (Map<String, Object> schema : allSchemas) {
                if (schema.get("name").equals(schemaName)) {
                    currentSchema = schema;
                }
            }
        } else {
            currentSchema = allSchemas.get(0);
        }
        if (currentSchema == null) {
            return Map.of();
        }
        List<Map<String, Object>> objects = (List<Map<String, Object>>) currentSchema.get("objects");
        Map<String, Object> allowedObjects = new HashMap<>();

        for (Map<String, Object> object : objects) {
            List<Map<String, Object>> permissions = (List<Map<String, Object>>) object.get("permissions");
            boolean isAllowed = permissions.stream()
                    .anyMatch(permission -> permission.get("role").equals(currentRole)
                            || permission.get("role").equals("public"));

            if (isAllowed) {
                Map<String, Object> transformedObject = new HashMap<>(object);
                List<Map<String, Object>> columns = (List<Map<String, Object>>) object.get("columns");
                List<Map<String, Object>> transformedColumns = new ArrayList<>();

                for (Map<String, Object> column : columns) {
                    Map<String, Object> transformedColumn = new HashMap<>();
                    transformedColumn.put("name", column.get("name"));
                    transformedColumn.put("data_type", column.get("data_type").toString().toLowerCase());
                    transformedColumn.put("primary_key", column.get("primary_key"));
                    transformedColumns.add(transformedColumn);
                }

                transformedObject.put("columns", transformedColumns);

                // Filter foreign keys based on allowed objects
                if (object.containsKey("foreign_keys")) {
                    List<Map<String, Object>> foreignKeys = (List<Map<String, Object>>) object.get("foreign_keys");
                    List<Map<String, Object>> filteredForeignKeys = filterForeignKeys(foreignKeys, objects,
                            currentRole);
                    transformedObject.put("foreign_keys", filteredForeignKeys);
                }

                allowedObjects.put((String) transformedObject.get("name"), transformedObject);
            }
        }

        return allowedObjects;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> filterForeignKeys(List<Map<String, Object>> foreignKeys,
            List<Map<String, Object>> objects, String currentRole) {
        List<Map<String, Object>> filteredForeignKeys = new ArrayList<>();

        for (Map<String, Object> fk : foreignKeys) {
            //String referencedTableName = (String) fk.get("referenced_table");
            List<String> referencedTable = (List<String>) fk.get("referenced_table");
            String referencedTableName = referencedTable.get(1);
            for (Map<String, Object> obj : objects) {
                if (obj.get("name").equals(referencedTableName)) {
                    List<Map<String, Object>> permissions = (List<Map<String, Object>>) obj.get("permissions");
                    boolean isAllowed = permissions.stream()
                            .anyMatch(permission -> permission.get("role").equals(currentRole)
                                    || permission.get("role").equals("public"));
                    if (isAllowed) {
                        filteredForeignKeys.add(fk);
                        break;
                    }
                }
            }
        }

        return filteredForeignKeys;
    }

    /*
     * Returns the permissions for the current role.
     * This functions is used to inform the UI about the permissions for the current role.
     * @param currentRole the role for the current request.
     * @param schemaName the name of the db schema (if you are exposing multiple schemas in the same database). Optional, use null for default.
     * The permissions are returned in the following format:
     * <pre>{@code
     * {
     *      "role1": [
     *          {
     *             "resource": "projects",
     *             "action": ["list", "show", "read", "export"]
     *          },
     *          {
     *              "resource": "clients",
     *              "action": ["list", "show", "read", "export"]
     *          }
     *      ]
     * }
     * }</pre>
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getPermissions(String currentRole, String schemaName) {
        List<Map<String, Object>> allSchemas = (List<Map<String, Object>>) dbSchema.get("schemas");
        Map<String, Object> currentSchema = null;
        if (schemaName != null) {
            for (Map<String, Object> schema : allSchemas) {
                if (schema.get("name").equals(schemaName)) {
                    currentSchema = schema;
                }
            }
        } else {
            currentSchema = allSchemas.get(0);
        }
        if (currentSchema == null) {
            return Map.of();
        }
        List<Map<String, Object>> objects = (List<Map<String, Object>>) currentSchema.get("objects");

        List<Map<String, Object>> allowedObjects = objects.stream()
                .filter(object -> {
                    List<Map<String, Object>> permissions = (List<Map<String, Object>>) object.get("permissions");
                    return permissions.stream()
                            .anyMatch(permission -> permission.get("role").equals(currentRole)
                                    || permission.get("role").equals("public"));
                })
                .toList();
        List<Map<String, Object>> userPermissions = allowedObjects.stream()
            .map(object -> {
                List<Map<String, Object>> permissions = ((List<Map<String, Object>>) object.get("permissions"))
                    .stream()
                    .filter(permission -> permission.get("role").equals(currentRole)
                                || permission.get("role").equals("public"))
                    .toList();
                return Map.of(
                    "name", object.get("name"),
                    "kind", object.get("kind"),
                    "columns", object.get("columns"),
                    "permissions", permissions
                );
            })
            .reduce(new ArrayList<Map<String, Object>>(), (acc, obj) -> {
                List<Map<String, Object>> permissions = (List<Map<String, Object>>) obj.get("permissions");
                
                for (Map<String, Object> permission : permissions) {
                    List<String> grants = (List<String>) permission.get("grant");
                    List<String> columns = (List<String>) permission.get("columns");
                    if (grants != null) {
                        List<String> actions = new ArrayList<>();
                        for (String grant : grants) {
                            actions.addAll(convertGrantToAction(grant));
                        }
                        Map<String, Object> permissionDetail = new HashMap<>();
                        permissionDetail.put("resource", obj.get("name"));
                        permissionDetail.put("action", actions);
                        // Assuming columns is an optional field
                        if (columns != null) {
                            permissionDetail.put("columns", columns);
                        }
                        acc.add(permissionDetail);
                    }
                }
                return acc;
            }, (acc, obj) -> {
                acc.addAll(obj);
                return acc;
            });
        return Map.of(currentRole, userPermissions);
    }

    private List<String> convertGrantToAction(String grant) {
        List<String> actions = new ArrayList<>();
        switch (grant) {
            case "select":
                actions.add("list");
                actions.add("show");
                actions.add("read");
                actions.add("export");
                break;
            case "insert":
                actions.add("create");
                break;
            case "update":
                actions.add("edit");
                actions.add("update");
                break;
            case "delete":
                actions.add("delete");
                break;
            case "all":
                actions.addAll(List.of("list", "show", "read", "export", "create", "edit", "update", "delete"));
                break;
            default:
                // Handle unknown grant
                break;
        }
        return actions;
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
    public Map<String, String> getEnv(String role, HttpServletRequest request, Map<String, Object> jwtClaims) {
        Map<String, String> env = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(Instant.class, new InstantSerializer());
        objectMapper.registerModule(module);

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

        Map<String, String> headers = new HashMap<>();
        Enumeration<String> headerNames = request.getHeaderNames();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            headers.put(headerName, request.getHeader(headerName));
        }

        try {
            if (jwtClaims != null) {
                env.put("request.jwt.claims", objectMapper.writeValueAsString(jwtClaims));
            } else if (jwtClaims == null && role != null) {
                env.put("request.jwt.claims", "{\"role\":\"" + role + "\"}");
            } else {
                env.put("request.jwt.claims", "{}");
            }
            env.put("request.headers", objectMapper.writeValueAsString(headers));
            env.put("request.get", objectMapper.writeValueAsString(request.getParameterMap()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return env;
    }

    private static final String LIB_BIN = "/";
    private final static String SUBZERO_LIB = "subzero";
    private final static String SUBZEROJNI_LIB = "subzerojni";

    static {
        try {
            System.loadLibrary(SUBZERO_LIB);
            System.loadLibrary(SUBZEROJNI_LIB);
        } catch (UnsatisfiedLinkError e) {
            loadNativeLibFromJar();
        }
    }

    /**
     * Load the native library from the jar file
     * This will try to load subzero and subzerojni native shared libraries from the jar file
     */
    public static void loadNativeLibFromJar() {
        // we need to put both DLLs to temp dir
        String path = "SUBZERO_" + new java.util.Date().getTime();
        loadLib(path, SUBZERO_LIB);
        loadLib(path, SUBZEROJNI_LIB);
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
        } else if (os.contains("mac")) {
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
            File fileOut = extractResourceFile(LIB_BIN + name);
            System.load(fileOut.toString());
            fileOut.deleteOnExit();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load required subzero native library", e);
        }
    }

    private static File extractResourceFile(String resource) {
        try {
            InputStream in = RestHandler.class.getResourceAsStream(resource);
            if (in == null) {
                throw new RuntimeException("Cannot find " + resource);
            }
            File fileOut = new File(System.getProperty("java.io.tmpdir") + "/" + resource);
            //System.out.println("Extracting " + resource + " to " + fileOut);
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

            //fileOut.deleteOnExit();
            return fileOut;
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract resource file " + resource, e);
        }
    }

}
