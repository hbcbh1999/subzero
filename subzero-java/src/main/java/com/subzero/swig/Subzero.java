/* ----------------------------------------------------------------------------
 * This file was automatically generated by SWIG (https://www.swig.org).
 * Version 4.2.1
 *
 * Do not make changes to this file unless you know what you are doing - modify
 * the SWIG interface file instead.
 * ----------------------------------------------------------------------------- */

package com.subzero.swig;

public class Subzero {
  public static sbz_HTTPRequest sbz_http_request_new_with_clone(String method, String uri, String body, String[] headers, int headers_count, String[] env, int env_count) {
    long cPtr = SubzeroJNI.sbz_http_request_new_with_clone(method, uri, body, headers, headers_count, env, env_count);
    return (cPtr == 0) ? null : new sbz_HTTPRequest(cPtr, false);
  }

  public static sbz_HTTPRequest sbz_http_request_new(String method, String uri, String body, String[] headers, int headers_count, String[] env, int env_count) {
    long cPtr = SubzeroJNI.sbz_http_request_new(method, uri, body, headers, headers_count, env, env_count);
    return (cPtr == 0) ? null : new sbz_HTTPRequest(cPtr, false);
  }

  public static void sbz_http_request_free(sbz_HTTPRequest request) {
    SubzeroJNI.sbz_http_request_free(sbz_HTTPRequest.getCPtr(request), request);
  }

  public static sbz_TwoStageStatement sbz_two_stage_statement_new(String schema_name, String path_prefix, sbz_DbSchema db_schema, sbz_HTTPRequest request, String max_rows) {
    long cPtr = SubzeroJNI.sbz_two_stage_statement_new(schema_name, path_prefix, sbz_DbSchema.getCPtr(db_schema), db_schema, sbz_HTTPRequest.getCPtr(request), request, max_rows);
    return (cPtr == 0) ? null : new sbz_TwoStageStatement(cPtr, false);
  }

  public static sbz_Statement sbz_two_stage_statement_mutate(sbz_TwoStageStatement two_stage_statement) {
    long cPtr = SubzeroJNI.sbz_two_stage_statement_mutate(sbz_TwoStageStatement.getCPtr(two_stage_statement), two_stage_statement);
    return (cPtr == 0) ? null : new sbz_Statement(cPtr, false);
  }

  public static sbz_Statement sbz_two_stage_statement_select(sbz_TwoStageStatement two_stage_statement) {
    long cPtr = SubzeroJNI.sbz_two_stage_statement_select(sbz_TwoStageStatement.getCPtr(two_stage_statement), two_stage_statement);
    return (cPtr == 0) ? null : new sbz_Statement(cPtr, false);
  }

  public static int sbz_two_stage_statement_set_ids(sbz_TwoStageStatement two_stage_statement, String[] ids, int ids_count) {
    return SubzeroJNI.sbz_two_stage_statement_set_ids(sbz_TwoStageStatement.getCPtr(two_stage_statement), two_stage_statement, ids, ids_count);
  }

  public static void sbz_two_stage_statement_free(sbz_TwoStageStatement two_stage_statement) {
    SubzeroJNI.sbz_two_stage_statement_free(sbz_TwoStageStatement.getCPtr(two_stage_statement), two_stage_statement);
  }

  public static sbz_Statement sbz_statement_new(String schema_name, String path_prefix, sbz_DbSchema db_schema, sbz_HTTPRequest request, String max_rows) {
    long cPtr = SubzeroJNI.sbz_statement_new(schema_name, path_prefix, sbz_DbSchema.getCPtr(db_schema), db_schema, sbz_HTTPRequest.getCPtr(request), request, max_rows);
    return (cPtr == 0) ? null : new sbz_Statement(cPtr, false);
  }

  public static String sbz_statement_sql(sbz_Statement statement) {
    return SubzeroJNI.sbz_statement_sql(sbz_Statement.getCPtr(statement), statement);
  }

  public static String[] sbz_statement_params(sbz_Statement statement) {
    return SubzeroJNI.sbz_statement_params(sbz_Statement.getCPtr(statement), statement);
}

  public static String[] sbz_statement_params_types(sbz_Statement statement) {
    return SubzeroJNI.sbz_statement_params_types(sbz_Statement.getCPtr(statement), statement);
}

  public static int sbz_statement_params_count(sbz_Statement statement) {
    return SubzeroJNI.sbz_statement_params_count(sbz_Statement.getCPtr(statement), statement);
  }

  public static void sbz_statement_free(sbz_Statement statement) {
    SubzeroJNI.sbz_statement_free(sbz_Statement.getCPtr(statement), statement);
  }

  public static void sbz_db_schema_free(sbz_DbSchema schema) {
    SubzeroJNI.sbz_db_schema_free(sbz_DbSchema.getCPtr(schema), schema);
  }

  public static sbz_DbSchema sbz_db_schema_new(String db_type, String db_schema_json, String license_key) {
    long cPtr = SubzeroJNI.sbz_db_schema_new(db_type, db_schema_json, license_key);
    return (cPtr == 0) ? null : new sbz_DbSchema(cPtr, false);
  }

  public static int sbz_db_schema_is_demo(sbz_DbSchema db_schema) {
    return SubzeroJNI.sbz_db_schema_is_demo(sbz_DbSchema.getCPtr(db_schema), db_schema);
  }

  public static int sbz_last_error_message(String buffer, int length) {
    return SubzeroJNI.sbz_last_error_message(buffer, length);
  }

  public static void sbz_clear_last_error() {
    SubzeroJNI.sbz_clear_last_error();
  }

  public static int sbz_last_error_length() {
    return SubzeroJNI.sbz_last_error_length();
  }

}
