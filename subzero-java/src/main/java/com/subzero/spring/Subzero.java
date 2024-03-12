package com.subzero.spring;

import com.subzero.swig.sbz_Statement;
import com.subzero.swig.sbz_DbSchema;
import com.subzero.swig.sbz_HTTPRequest;
import javax.sql.DataSource;
import java.sql.*;
import java.util.Enumeration;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class Subzero {
    private sbz_DbSchema dbSchema;
    private DataSource dataSource;
    
    public Subzero(DataSource dataSource, String db_type, String db_schema_json, String license_key) {
        this.dataSource = dataSource;
        this.dbSchema = new sbz_DbSchema(db_type, db_schema_json, license_key);
    }

    public void handleRequest(String schema_name, String prefix, HttpServletRequest req, HttpServletResponse res, String[] env) {
        this.handleRequest(schema_name, prefix, req, res, env, null);
    }
    public void handleRequest(String schema_name, String prefix, HttpServletRequest req, HttpServletResponse res) {
        this.handleRequest(schema_name, prefix, req, res, new String[]{}, null);
    }

    public void handleRequest(String schema_name, String prefix, HttpServletRequest req, HttpServletResponse res,
            String[] env, String max_rows) {
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
            sbz_HTTPRequest request = new sbz_HTTPRequest(method, uri, body, headers, headerCount, env, envCount);
            sbz_Statement statement = sbz_Statement.mainStatement(schema_name, prefix, this.dbSchema, request, max_rows);
            String sql = statement.getSql().replaceAll("\\$\\d+", "?");
            String[] params = statement.getParams();
            String[] paramsTypes = statement.getParamsTypes();
            //String[] paramsTypes = statement.getParamsTypes();
            System.out.println("SQL: " + sql);
            System.out.println("Params: [" + String.join(", ", params) + "]");
            // execute the query
            Connection conn = this.dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(sql);
            for (int j = 0; j < params.length; j++) {
                Object param = stringParamToJavaType(params[j], paramsTypes[j]);
                ps.setObject(j + 1, param);
            }
            ResultSet rs = ps.executeQuery();
            rs.next();
            String bodyColumn = rs.getString("body");
            res.setStatus(200);
            res.setHeader("Content-Type", "application/json");
            res.getWriter().write(bodyColumn);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
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
