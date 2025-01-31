// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
extern crate cbindgen;

use std::env;
use std::path::PathBuf;
use cbindgen::Config;

static HEADER: &str = r#"
/**
 * \mainpage SubZero C Shared Library Documentation
 *
 * \section intro_sec Introduction
 *
 * This library provides the low level C API for the SubZero.
 * It's intended to be used by other languages to interface with the SubZero library.
 * 
 * The purpose of this library is to take an HTTP request and return a SQL statement
 * that can be used to query a database and return the fully formed response body.
 * 
 * The HTTP request needs to conform to the SubZero HTTP request format (PostgREST compatible).
 *
 * 
 *
 * \section usage_sec Usage
 *
 * Here's a brief example on how to use the SubZero library:
 * \code
 * // include the header file
 * #include "subzero.h"
 * // ... other includes
 * 
 * const char* db_type = "sqlite";
 * // Constructing the JSON schema is tedious and for this reason we provide "introspection queries"
 * // for each database type that can be used to generate the schema JSON automatically
 * const char* db_schema_json = ""
 * "{"
 * "    \"schemas\": ["
 * "        {"
 * "            \"name\": \"public\","
 * "            \"objects\": ["
 * "                {"
 * "                    \"kind\": \"table\","
 * "                    \"name\": \"clients\","
 * "                    \"columns\": ["
 * "                        {"
 * "                            \"name\": \"id\","
 * "                            \"data_type\": \"INTEGER\","
 * "                            \"primary_key\": true"
 * "                        },"
 * "                        {"
 * "                            \"name\": \"name\","
 * "                            \"data_type\": \"TEXT\","
 * "                            \"primary_key\": false"
 * "                        }"
 * "                    ],"
 * "                    \"foreign_keys\": []"
 * "                }"
 * "            ]"
 * "        }"
 * "    ]"
 * "}"
 * ;
 * 
 * // main function
 * int main() {
 *   sbz_DbSchema* db_schema = sbz_db_schema_new(db_type, db_schema_json, NULL);
 *   if (db_schema == NULL) {
 *     const int err_len = sbz_last_error_length();
 *     char* err = (char*)malloc(err_len);
 *     sbz_last_error_message(err, err_len);
 *     printf("Error: %s\n", err);
 *     free(err);
 *     return -1;
 *   }
 *   
 *   const char* headers[] = {"Content-Type", "application/json", "Accept", "application/json"};
 *   const char* env[] = {"user_id", "1"};
 *   sbz_HTTPRequest* req = sbz_http_request_new(
 *      "GET",
 *      "http://localhost/rest/projects?select=id,name",
 *      NULL,
 *      headers, 4,
 *      env, 2
 *   );
 *   sbz_Statement* stmt = sbz_statement_new(
 *     "public",
 *     "/rest/",
 *     "user",
 *     db_schema,
 *     req,
 *     NULL
 *   );
 *  
 *   if (stmt == NULL) {
 *     const int err_len = sbz_last_error_length();
 *     char* err = (char*)malloc(err_len);
 *     sbz_last_error_message(err, err_len);
 *     printf("Error: %s\n", err);
 *     free(err);
 *     return -1;
 *   }
 *   
 *   const char* sql = sbz_statement_sql(stmt);
 *   const char *const * params = sbz_statement_params(stmt);
 *   const char *const * params_types = sbz_statement_params_types(stmt);
 *   int params_count = sbz_statement_params_count(stmt);
 *   printf("SQL: %s\n", sql);
 *   printf("params: %s\n", params[0]);
 *   printf("params_count: %d\n", params_count);
 *   printf("params_types: %s\n", params_types[0]);
 *   
 *   sbz_statement_free(stmt);
 *   sbz_http_request_free(req);
 *   sbz_db_schema_free(db_schema);
 *   return 0;
 * }
 * \endcode
 *
 * For more information, navigate to the [Files](files.html) section.
 */
"#;
fn main() {
    // Generate the C header file
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let target_dir = target_dir();
    let output_file = target_dir.join(format!("{}.h", package_name.replace("-ffi", ""))).display().to_string();
    let mut config = Config::default();
    config.language = cbindgen::Language::C;
    config.documentation_style = cbindgen::DocumentationStyle::Doxy;
    config.header = Some(HEADER.to_string());
    cbindgen::generate_with_config(crate_dir, config)
        .expect("Unable to generate bindings")
        .write_to_file(output_file);
}

/// Find the location of the `target/` directory. Note that this may be
/// overridden by `cmake`, so we also need to check the `CARGO_TARGET_DIR`
/// variable.
fn target_dir() -> PathBuf {
    if let Ok(target) = env::var("CARGO_TARGET_DIR") {
        PathBuf::from(target)
    } else {
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("..")
            .join("target")
            .join(env::var("PROFILE").unwrap())
    }
}
