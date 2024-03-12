package com.subzero;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.stereotype.Controller;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.subzero.spring.Subzero;

@Controller
public class TestController {

    private final DataSource dataSource;
    private final String schema_json;
    private final Subzero subzero;
    @Autowired
    public TestController(DataSource dataSource) {
        this.dataSource = dataSource;
        this.schema_json = Util.getResourceFileContent("schema.json");
        this.subzero = new Subzero(dataSource, "postgresql", this.schema_json, null);
    }

    @GetMapping("/testquery")
    @ResponseBody
    public String testQuery() {
        String responseMessage = "No Project found";
        String query = "SELECT name FROM projects WHERE id = 1";

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next()) {
                responseMessage = resultSet.getString("name");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return "Error accessing database";
        }

        return responseMessage;
    }
    
    @RequestMapping("/rest/**")
    public void handleRequest(HttpServletRequest req, HttpServletResponse res) {
        try {
            this.subzero.handleRequest("public", "/rest/", req, res);
        } catch (Exception e) {
            // return the error message
            //e.printStackTrace();
            res.setStatus(500);
            res.setContentType("text/plain");
            res.setCharacterEncoding("UTF-8");
            try {
                res.getWriter().write("Error: " + e.getMessage());
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        
    }

}
