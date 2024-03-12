%module Subzero

/* This tells SWIG to treat char ** as a special case when used as a parameter
   in a function call */
%typemap(in) char ** (jint size) {
    int i = 0;
    size = (*jenv)->GetArrayLength(jenv, $input);
    $1 = (char **) malloc((size+1)*sizeof(char *));
    /* make a copy of each string */
    for (i = 0; i<size; i++) {
        jstring j_string = (jstring)(*jenv)->GetObjectArrayElement(jenv, $input, i);
        const char * c_string = (*jenv)->GetStringUTFChars(jenv, j_string, 0);
        $1[i] = malloc((strlen(c_string)+1)*sizeof(char));
        strcpy($1[i], c_string);
        (*jenv)->ReleaseStringUTFChars(jenv, j_string, c_string);
        (*jenv)->DeleteLocalRef(jenv, j_string);
    }
    $1[i] = 0;
}

/* This cleans up the memory we malloc'd before the function call */
%typemap(freearg) char ** {
    int i;
    for (i=0; i<size$argnum-1; i++)
      free($1[i]);
    free($1);
}

/* This allows a C function to return a char ** as a Java String array */
%typemap(out) char ** {
    int i;
    int len=0;
    jstring temp_string;
    const jclass clazz = (*jenv)->FindClass(jenv, "java/lang/String");

    while ($1[len]) len++;    
    jresult = (*jenv)->NewObjectArray(jenv, len, clazz, NULL);
    /* exception checking omitted */

    for (i=0; i<len; i++) {
      temp_string = (*jenv)->NewStringUTF(jenv, *result++);
      (*jenv)->SetObjectArrayElement(jenv, jresult, i, temp_string);
      (*jenv)->DeleteLocalRef(jenv, temp_string);
    }
}

/* These 3 typemaps tell SWIG what JNI and Java types to use */
%typemap(jni) char ** "jobjectArray"
%typemap(jtype) char ** "String[]"
%typemap(jstype) char ** "String[]"

/* These 2 typemaps handle the conversion of the jtype to jstype typemap type
   and vice versa */
%typemap(javain) char ** "$javainput"
%typemap(javaout) char ** {
    return $jnicall;
}

//%include "documentation.i"

%{
    #include "subzero.h"
%}

typedef struct sbz_HTTPRequest {} sbz_HTTPRequest;
typedef struct sbz_DbSchema {} sbz_DbSchema;
typedef struct sbz_Statement {} sbz_Statement;
typedef struct sbz_TwoStageStatement {} sbz_TwoStageStatement;

%include "exception.i"

%exception {
    $action
    //if (!result) {
    const int err_len = sbz_last_error_length();
    if (err_len > 0) { // Check if there's an error
        char* err_msg = (char*)malloc(err_len);
        sbz_last_error_message(err_msg, err_len);
        (*jenv)->ThrowNew(jenv, (*jenv)->FindClass(jenv, "java/lang/RuntimeException"), err_msg);
        sbz_clear_last_error();
        free(err_msg);
    }
    //}
}

%include "subzero.h"


%extend sbz_HTTPRequest {
    sbz_HTTPRequest(
        const char *method,
        const char *uri,
        const char *body,
        const char** headers,
        int headers_count,
        const char** env,
        int env_count
    ) {
        return sbz_http_request_new_with_clone(method, uri, body, headers, headers_count, env, env_count);
    }
    ~sbz_HTTPRequest() {
        sbz_http_request_free($self);
    }
}

%extend sbz_DbSchema {
    sbz_DbSchema(const char *db_type, const char *db_schema_json, const char *license_key) {
        return sbz_db_schema_new(db_type, db_schema_json, license_key);
    }
    bool isDemo() {
        return sbz_db_schema_is_demo($self) > 0;
    }
    ~sbz_DbSchema() {
        sbz_db_schema_free($self);
    }
}

%extend sbz_Statement {
    sbz_Statement(const char *schema_name,
                  const char *path_prefix,
                  const struct sbz_DbSchema *db_schema,
                  const struct sbz_HTTPRequest *request,
                  const char *max_rows) {
        return sbz_statement_main_new(schema_name, path_prefix, db_schema, request, max_rows);
    }
    static sbz_Statement* mainStatement(
        const char *schema_name,
        const char *path_prefix,
        const struct sbz_DbSchema *db_schema,
        const struct sbz_HTTPRequest *request,
        const char *max_rows) {
        return sbz_statement_main_new(schema_name, path_prefix, db_schema, request, max_rows);
    }
    static sbz_Statement* envStatement(
        const struct sbz_DbSchema *db_schema,
        const struct sbz_HTTPRequest *request) {
        return sbz_statement_env_new(db_schema, request);
    }
    ~sbz_Statement() {
        sbz_statement_free($self);
    }
    char* getSql() {
        return (char *)sbz_statement_sql($self);
    }
    char** getParams() {
        return (char **)sbz_statement_params($self);
    }
    char** getParamsTypes() {
        return (char **)sbz_statement_params_types($self);
    }
}

%extend sbz_TwoStageStatement {
    sbz_TwoStageStatement(const char *schema_name,
                          const char *path_prefix,
                          const struct sbz_DbSchema *db_schema,
                          const struct sbz_HTTPRequest *request,
                          const char *max_rows) {
        return sbz_two_stage_statement_new(schema_name, path_prefix, db_schema, request, max_rows);
    }
    ~sbz_TwoStageStatement() {
        sbz_two_stage_statement_free($self);
    }
}

// Optionally, reset to mutable if needed elsewhere
// %mutable;
