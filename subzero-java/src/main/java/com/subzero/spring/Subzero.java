package com.subzero.spring;

import com.subzero.swig.sbz_Statement;
import com.subzero.swig.sbz_DbSchema;
import com.subzero.swig.sbz_HTTPRequest;

import javax.sql.DataSource;
import java.sql.*;
import java.util.Enumeration;
import java.util.List;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class Subzero {
    private sbz_DbSchema dbSchema;
    private DataSource dataSource;
    private String dbType;
    
    public Subzero(DataSource dataSource, String dbType, String dbSchemaJson, String licenseKey) {
        this.dataSource = dataSource;
        this.dbType = dbType;
        this.dbSchema = new sbz_DbSchema(dbType, dbSchemaJson, licenseKey);
    }

    public Subzero(
            DataSource dataSource,
            String dbType,
            String[] dbSchemas, 
            String introspectionQueryDir,
            String customRelations,
            String customPermissions,
            String licenseKey
        ) {
        this.dataSource = dataSource;
        this.dbType = dbType;
        String introspectionQuery = com.subzero.swig.Subzero.sbz_introspection_query(
            dbType,
            introspectionQueryDir,
            customRelations,
            customPermissions
        );
        try {
            Connection conn = dataSource.getConnection();
            //System.out.println("Introspection Query: " + introspectionQuery);
            PreparedStatement ps = conn.prepareStatement(introspectionQuery.replaceAll("\\$\\d+", "?"));
            if (dbType.equals("postgresql")) {
                //String dbSchemasArrStr = "{" + String.join(",", dbSchemas) + "}";
                Array dbSchemasArr = conn.createArrayOf("text", dbSchemas);
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
            ResultSet rs = ps.executeQuery();
            rs.next();
            String dbSchemaJson = rs.getString("json_schema");
            //System.out.println("DB Schema JSON: " + dbSchemaJson);
            this.dbSchema = new sbz_DbSchema(dbType, dbSchemaJson, licenseKey);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //this.dbSchema = new sbz_DbSchema(db_type, db_schema_json, license_key);
    }

    public void handleRequest(String schema_name, String prefix, String role, HttpServletRequest req, HttpServletResponse res, String[] env) {
        this.handleRequest(schema_name, prefix, role, req, res, env, null);
    }
    public void handleRequest(String schema_name, String prefix, String role, HttpServletRequest req, HttpServletResponse res) {
        this.handleRequest(schema_name, prefix, role, req, res, new String[]{}, null);
    }

    public void handleRequest(String schema_name, String prefix, String role, HttpServletRequest req, HttpServletResponse res,
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

            //throw new RuntimeException("test");
            // return;
            //System.out.println("Java env: [" + String.join(", ", env) + "]");
            sbz_HTTPRequest request = new sbz_HTTPRequest(method, uri, body, headers, headerCount, env, envCount);
            sbz_Statement statement = sbz_Statement.mainStatement(schema_name, prefix, role, this.dbSchema, request,max_rows);
            conn = this.dataSource.getConnection();
            // start the transaction
            conn.setReadOnly(true);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

            // switch (this.dbType) {
            //     case "postgresql":
            //     case "mysql":
            //         sbz_Statement envStatement = sbz_Statement.envStatement(dbSchema, request);
            //         executeStatement(envStatement, conn);
            //         break;
            // }

            
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
        try{
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
}
