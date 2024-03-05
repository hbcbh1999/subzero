// test/test_all.c
#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include "subzero.h" // Adjust the path as needed

const char* db_schema_json = 
"{"
"    \"schemas\":["
"        {"
"            \"name\":\"public\","
"            \"objects\":["
"                {\"kind\":\"table\",\"name\":\"tbl1\",\"columns\":[{\"name\":\"one\",\"data_type\":\"varchar(10)\",\"primary_key\":false},{\"name\":\"two\",\"data_type\":\"smallint\",\"primary_key\":false}],\"foreign_keys\":[]},"
"                {\"kind\":\"table\",\"name\":\"clients\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]},"
"                {\"kind\":\"table\",\"name\":\"projects\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"client_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[{\"name\":\"projects_client_id_fkey\",\"table\":[\"_sqlite_public_\",\"projects\"],\"columns\":[\"client_id\"],\"referenced_table\":[\"_sqlite_public_\",\"clients\"],\"referenced_columns\":[\"id\"]}]},"
"                {\"kind\":\"view\",\"name\":\"projects_view\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"client_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[]},"
"                {\"kind\":\"table\",\"name\":\"tasks\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"project_id\",\"data_type\":\"INTEGER\",\"primary_key\":false}],\"foreign_keys\":[{\"name\":\"tasks_project_id_fkey\",\"table\":[\"_sqlite_public_\",\"tasks\"],\"columns\":[\"project_id\"],\"referenced_table\":[\"_sqlite_public_\",\"projects\"],\"referenced_columns\":[\"id\"]}]},"
"                {\"kind\":\"table\",\"name\":\"users\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":true},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]},"
"                {\"kind\":\"table\",\"name\":\"users_tasks\",\"columns\":[{\"name\":\"user_id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"task_id\",\"data_type\":\"INTEGER\",\"primary_key\":true}],\"foreign_keys\":[{\"name\":\"users_tasks_task_id_fkey\",\"table\":[\"_sqlite_public_\",\"users_tasks\"],\"columns\":[\"task_id\"],\"referenced_table\":[\"_sqlite_public_\",\"tasks\"],\"referenced_columns\":[\"id\"]},{\"name\":\"users_tasks_user_id_fkey\",\"table\":[\"_sqlite_public_\",\"users_tasks\"],\"columns\":[\"user_id\"],\"referenced_table\":[\"_sqlite_public_\",\"users\"],\"referenced_columns\":[\"id\"]}]},"
"                {\"kind\":\"table\",\"name\":\"complex_items\",\"columns\":[{\"name\":\"id\",\"data_type\":\"INTEGER\",\"primary_key\":false},{\"name\":\"name\",\"data_type\":\"TEXT\",\"primary_key\":false},{\"name\":\"settings\",\"data_type\":\"TEXT\",\"primary_key\":false}],\"foreign_keys\":[]}"
"            ]"
"        }"
"    ]"
"}";


START_TEST(test_hello_world)
{
    const char* hw = hello_world();
    ck_assert_str_eq(hw, "Hello, world!");
}
END_TEST

START_TEST(test_db_schema_new)
{
    const char* db_type = "sqlite";
    DbSchema* db_schema = db_schema_new(db_type, db_schema_json);
    const int err_len = last_error_length();
    if (err_len > 0) {
        char* err = (char*)malloc(err_len);
        last_error_message(err, err_len);
        printf("Error: %s\n", err);
        free(err);
    }
    // check that the db_schema is not NULL
    ck_assert_ptr_ne(db_schema, NULL);
    db_schema_free(db_schema);
}
END_TEST

/*
typedef struct Request {
  const char *method;
  const char *uri;
  const struct Tuple *headers;
  int headers_count;
  const char *body;
  const struct Tuple *env;
  int env_count;
} Request;

*/

START_TEST(test_statement_new){
    const char* db_type = "sqlite";
    DbSchema* db_schema = db_schema_new(db_type, db_schema_json);
    Tuple headers[] = {{"Content-Type", "application/json"}, {"Accept", "application/json"}};
    Tuple env[] = {{"role", "admin"}, {"path", "/home/user"}};
    Request req = {
        "GET",
        "http://localhost/rest/projects?select=id,name&id=eq.1",
        headers, 2,
        NULL, 
        env, 2
    };
    Statement* main_stmt = statement_new(
        "public",
        "/rest/",
        db_schema,
        &req,
        NULL
    );

    const int err_len = last_error_length();
    if (err_len > 0) {
        char* err = (char*)malloc(err_len);
        last_error_message(err, err_len);
        printf("Error: %s\n", err);
        free(err);
    }
    ck_assert_ptr_ne(main_stmt, NULL);
    const char* sql = statement_sql(main_stmt);
    const char *const * params = statement_params(main_stmt);
    const char *const * params_types = statement_params_types(main_stmt);
    int params_count = statement_params_count(main_stmt);

    // env vars are passed as hashmap and because of this the env cte is not consistent
    // so we only check for the _subzero_query cte and the 3rd param
    ck_assert_int_eq(params_count, 3);
    ck_assert_str_eq(params[2], "1");
    ck_assert_str_eq(params_types[2], "INTEGER");
    const char* start = strstr(sql, "_subzero_query as (");
    start = start + 19;
    const char* end = strstr(sql, ") ,");
    int len = end - start;
    char* subzero_query = (char*)malloc(len + 1);
    memcpy(subzero_query, start, len);
    subzero_query[len] = '\0';
    ck_assert_str_eq(subzero_query, 
        "  select \"projects\".\"id\", \"projects\".\"name\" from \"projects\", env    where \"projects\".\"id\" = ?     "
    );
    free(subzero_query);
    
    // ck_assert_str_eq(sql, 
    //     "with"
    //     " env as materialized (select ? as \"role\",? as \"path\"), "
    //     " _subzero_query as ("
    //     "  select \"projects\".\"id\", \"projects\".\"name\" from \"projects\", env    where \"projects\".\"id\" = ?   "
    //     "  ) ,"
    //     " _subzero_count_query as (select 1) "
    //     "select"
    //     " count(_subzero_t.row) AS page_total,"
    //     " null as total_result_set,"
    //     " json_group_array(json(_subzero_t.row)) as body, "
    //     " null as response_headers, "
    //     " null as response_status  "
    //     "from (  "
    //     "   select json_object('id', _subzero_query.\"id\",'name', _subzero_query.\"name\"     ) as row  "
    //     "   from _subzero_query "
    //     ") _subzero_t"
    // );
    statement_free(main_stmt);

}
END_TEST

START_TEST(test_two_stage_statement_new){
    const char* db_type = "sqlite";
    DbSchema* db_schema = db_schema_new(db_type, db_schema_json);
    Tuple headers[] = {{"Content-Type", "application/json"}, {"Accept", "application/json"}};
    Tuple env[] = {};
    Request req = {
        "POST",
        "http://localhost/rest/projects?select=id,name",
        headers, 2,
        "[{\"name\":\"project1\"}]", 
        env, 0
    };
    TwoStageStatement* main_stmt = two_stage_statement_new(
        "public",
        "/rest/",
        db_schema,
        &req,
        NULL
    );

    const int err_len = last_error_length();
    if (err_len > 0) {
        char* err = (char*)malloc(err_len);
        last_error_message(err, err_len);
        printf("Error: %s\n", err);
        free(err);
    }
    ck_assert_ptr_ne(main_stmt, NULL);
    const Statement* mutate_stmt = two_stage_statement_mutate(main_stmt);
    ck_assert_ptr_ne(mutate_stmt, NULL);

    const char* sql = statement_sql(mutate_stmt);
    const char *const * params = statement_params(mutate_stmt);
    const char *const * params_types = statement_params_types(mutate_stmt);
    int params_count = statement_params_count(mutate_stmt);
    ck_assert_int_eq(params_count, 1);
    ck_assert_str_eq(params[0], "[{\"name\":\"project1\"}]");
    // printf("mutate SQL: %s\n", sql);
    const char* expected_sql =
        "with"
        " env as materialized (select null) , "
        " subzero_payload as ( select ? as json_data ), subzero_body as ( select json_extract(value, '$.name') as \"name\" from (select value from json_each(( select case when json_type(json_data) = 'array' then json_data else json_array(json_data) end as val from subzero_payload ))) ) "
        "insert into \"projects\" (\"name\") "
        "select \"name\" "
        "from subzero_body _  "
        "where true  "
        "returning \"id\", 1  as _subzero_check__constraint ";
    ck_assert_str_eq(sql, expected_sql);
    ck_assert_str_eq(params_types[0], "text");
    
    // printf("mutate params: %s\n", params[0]);
    // printf("mutate params_types: %s\n", params_types[0]);
    // printf("mutate params_count: %d\n", params_count);

    const Statement* select_stmt = two_stage_statement_select(main_stmt);
    ck_assert_ptr_ne(select_stmt, NULL);
    const char* sql_select = statement_sql(select_stmt);
    const char *const * params_select = statement_params(select_stmt);
    const char *const * params_types_select = statement_params_types(select_stmt);
    int params_count_select = statement_params_count(select_stmt);

    printf("select SQL: %s\n", sql_select);
    printf("select params: %s\n", params_select[0]);
    printf("select params_types: %s\n", params_types_select[0]);
    printf("select params_count: %d\n", params_count_select);

    two_stage_statement_free(main_stmt);

}
END_TEST

Suite* subzero_suite(void)
{
    Suite *s = suite_create("subZero FFI Test Suite");
    TCase *tc_core = tcase_create("Core");
    tcase_add_test(tc_core, test_hello_world);
    tcase_add_test(tc_core, test_db_schema_new);
    tcase_add_test(tc_core, test_statement_new);
    tcase_add_test(tc_core, test_two_stage_statement_new);
    suite_add_tcase(s, tc_core);
    return s;
}

int main(void)
{
    int number_failed;
    Suite *s = subzero_suite();
    SRunner *sr = srunner_create(s);
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
