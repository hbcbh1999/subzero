This folder contains the Java bindings for subZero.

It exposes the [Subzero](https://docs.subzero.cloud/java_docs/com/subzero/spring/Subzero.html) class which can be used in the context of a Spring Boot application.

## Usage
The usage basically comes down to defining a Spring Boot controller

```java
package com.example.demo;

import java.io.IOException;
import java.util.Map;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Value;

// import the subzero class
import cloud.subzero.SubzeroException;
import cloud.subzero.rest.RestHandler;

@RestController
public class SubzeroController {
    private final RestHandler rest;
    private final DataSource dataSource;


    /**
     * This is the SubzeroController.
     * It initializes the Subzero instance which introspects the database and
     * provides a REST API for the frontend SDK to call.
     * @param dataSource
     * @param jdbcTemplate
     */
    public SubzeroController(
        @Value("${cloud.subzero.rest.db-schemas}") String[] dbSchemas,
        @Value("${cloud.subzero.rest.license-key}") String licenseKey,
        DataSource dataSource,
        JdbcTemplate jdbcTemplate
    ) {
        this.dataSource = dataSource;
        try {

            Connection conn = DataSourceUtils.getConnection(dataSource);
            this.rest = new RestHandler(
                    conn,
                    "postgresql",
                    dbSchemas,
                    null,
                    false,
                    null,
                    null,
                    licenseKey
            );
            DataSourceUtils.releaseConnection(conn, dataSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This method handles the REST API requests from the frontend SDK.
     * @param req Request object
     * @param res Response object
     * @param jwt JWT token
     */
    @RequestMapping("/rest/v1/**")
    @Transactional
    public void handleRequest(HttpServletRequest req, HttpServletResponse res, @AuthenticationPrincipal Jwt jwt)
    throws SubzeroException, IOException, SQLException {
        Connection conn = null;
        try {
            Map<String, Object> jwtClaims = jwt.getClaims();
            String role = (String) jwtClaims.get("role");
            Map<String, String> env = this.rest.getEnv(
                    role,
                    req,
                    jwtClaims);
            String[] envArray = new String[env.size() * 2];
            int i = 0;
            for (String key : env.keySet()) {
                envArray[i++] = key;
                envArray[i++] = env.get(key);
            }
            conn = DataSourceUtils.getConnection(this.dataSource);

            this.rest.handleRequest(conn, "public", "/rest/v1/", role, req, res, envArray, null);

        }
        catch (SubzeroException e) {
            res.setStatus(e.getHttpStatusCode());
            res.setContentType("application/json");
            res.setCharacterEncoding("UTF-8");
            res.getWriter().write(e.getMessage());
            throw e;
        }
        catch (Exception e) {
            e.printStackTrace();
            res.setStatus(500);
            res.setContentType("application/json");
            res.setCharacterEncoding("UTF-8");
            res.getWriter().write("{\"error\": \"Internal Server Error\"}");
            throw e;
        }
        finally {
            if (conn != null)
                DataSourceUtils.releaseConnection(conn, this.dataSource);
        }
    }
}
```