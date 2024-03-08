package com.subzero;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import com.subzero.swig.*;


// unit test for the lib generated by swig com.subzero.swig.Subzero
public class LibTest extends TestCase {
    String schema_json = "" +
        "{"+
        "    \"schemas\":["+
        "        {"+
        "            \"name\":\"public\","+
        "            \"objects\":["+
        "                {\"kind\":\"table\",\"name\":\"tbl1\",\"columns\":[{\"name\":\"one\",\"data_type\":\"varchar(10)\",\"primary_key\":false},{\"name\":\"two\",\"data_type\":\"smallint\",\"primary_key\":false}],\"foreign_keys\":[]},"+
        "                {\"kind\":\"table\",\"name\":\"clients\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]},"+
        "                {\"kind\":\"table\",\"name\":\"projects\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"client_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[{\"name\":\"projects_client_id_fkey\",\"table\":[\"_sqlite_public_\",\"projects\"],\"columns\":[\"client_id\"],\"referenced_table\":[\"_sqlite_public_\",\"clients\"],\"referenced_columns\":[\"id\"]}]},"+
        "                {\"kind\":\"view\",\"name\":\"projects_view\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"client_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[]},"+
        "                {\"kind\":\"table\",\"name\":\"tasks\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"project_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[{\"name\":\"tasks_project_id_fkey\",\"table\":[\"_sqlite_public_\",\"tasks\"],\"columns\":[\"project_id\"],\"referenced_table\":[\"_sqlite_public_\",\"projects\"],\"referenced_columns\":[\"id\"]}]},"+
        "                {\"kind\":\"table\",\"name\":\"users\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]},"+
        "                {\"kind\":\"table\",\"name\":\"users_tasks\",\"columns\":[{\"name\":\"user_id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"task_id\",\"data_type\":\"INTEGER\",\"primary_key\":true}],\"foreign_keys\":[{\"name\":\"users_tasks_task_id_fkey\",\"table\":[\"_sqlite_public_\",\"users_tasks\"],\"columns\":[\"task_id\"],\"referenced_table\":[\"_sqlite_public_\",\"tasks\"],\"referenced_columns\":[\"id\"]},{\"name\":\"users_tasks_user_id_fkey\",\"table\":[\"_sqlite_public_\",\"users_tasks\"],\"columns\":[\"user_id\"],\"referenced_table\":[\"_sqlite_public_\",\"users\"],\"referenced_columns\":[\"id\"]}]},"+
        "                {\"kind\":\"table\",\"name\":\"complex_items\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"settings\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]}"+
        "            ]"+
        "        }"+
        "    ]"+
        "}"+
        "";
    static {
        System.loadLibrary("subzerojni"); // "libsubzerojni.dylib" or "libsubzerojni.so""+
    }
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public LibTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(LibTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testStatement() {
        //public sbz_HTTPRequest(String method, String uri, String body, String[] headers, int headers_count, String[] env, int env_count) 
        String[] headers = new String[] {
            "Accept","application/json"
        };
        String[] env = new String[] {
            "user_id","1"
        };
        sbz_HTTPRequest req = new sbz_HTTPRequest(
            "GET",
            "http://example.com/api/projects?select=id,name",
            null,
            headers,
            headers.length,
            env,
            env.length
        );

        sbz_DbSchema db_schema = Subzero.sbz_db_schema_new("sqlite", this.schema_json, "license_key");
        if (db_schema == null) {
            System.out.println("Failed to create db_schema");
            assert(false);
        }
        sbz_Statement statement = Subzero.sbz_statement_new("public", "/api/", db_schema, req, null);
        if (statement == null) {
            System.out.println("Failed to create statement");
            assert(false);
        }

        String sql = statement.getSql();
        System.out.println("SQL: " + sql.toString());
        
        String[] params = statement.getParams();
        System.out.println("Params: ['" + String.join("','", params) + "']");
        // assert params content
        assertEquals(params.length, 1);
        assertEquals(params[0], "1");

        String[] params_types = statement.getParamsTypes();
        System.out.println("ParamsTypes: ['" + String.join("','", params_types) + "']");
        // assert params_types content
        assertEquals(params_types.length, 1);
        assertEquals(params_types[0], "text");
    }
}
