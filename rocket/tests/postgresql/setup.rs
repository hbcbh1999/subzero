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
pub use demonstrate::demonstrate;
use rocket::http::{Cookie, Header};
use rocket::local::asynchronous::LocalRequest;
pub use std::env;
use std::path::PathBuf;
use std::process::Command;
pub use std::sync::Once;
use lazy_static::LazyStatic;
pub use crate::haskell_test;
pub use std::thread;
use tokio::runtime::Builder;
pub use rocket::local::asynchronous::Client;
pub use async_once::AsyncOnce;
use super::super::start;

pub static INIT_DB: Once = Once::new();
pub static INIT_CLIENT: Once = Once::new();

lazy_static! {
    static ref CLIENT_INNER: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
    pub static ref RUNTIME: tokio::runtime::Runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    pub static ref CLIENT: &'static AsyncOnce<Client> = {
        thread::spawn(move || {
            RUNTIME.block_on(async {
                CLIENT_INNER.get().await;
            })
        })
        .join()
        .expect("Thread panicked");
        &*CLIENT_INNER
    };
}

pub fn setup_db(init_db_once: &Once) {
    //let _ = env_logger::builder().is_test(true).try_init();
    init_db_once.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fixtures_dir = project_dir.join("tests/postgresql/fixtures");
        assert!(env::set_current_dir(fixtures_dir).is_ok());

        let postgresql_db_uri = option_env!("POSTGRESQL_DB_URI");
        let db_uri: String = match postgresql_db_uri {
            Some(db_uri) => db_uri.to_owned(),
            None => {
                let tmp_pg_cmd = project_dir.join("tests/bin/pg_tmp.sh");

                let output = Command::new(tmp_pg_cmd)
                    .arg("-k")
                    .arg("-t")
                    .arg("-u")
                    .arg("postgrest_test_authenticator")
                    .output()
                    .expect("failed to start temporary pg process");
                if !output.status.success() {
                    println!("status: {}", output.status);
                    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
                }

                assert!(output.status.success());
                let db_uri = String::from_utf8_lossy(&output.stdout).into_owned();
                let init_file = project_dir.join("tests/postgresql/fixtures/load.sql");
                let output = Command::new("psql")
                    .arg("-f")
                    .arg(init_file.to_str().unwrap())
                    .arg(db_uri.as_str())
                    .output()
                    .expect("failed to execute process");

                if !output.status.success() {
                    println!("status: {}", output.status);
                    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
                }
                assert!(output.status.success());
                db_uri
            }
        };
        env::set_var("SUBZERO_DB_URI", db_uri);
    });
}

pub fn setup_client<T>(init_client_once: &Once, client: &T)
where
    T: LazyStatic,
{
    init_client_once.call_once(|| {
        env::set_var("SUBZERO_CONFIG", "inexistent_config.toml");
        env::set_var("SUBZERO_DB_ANON_ROLE", "postgrest_test_anonymous");
        env::set_var("SUBZERO_DB_TX_ROLLBACK", "true");
        env::set_var("SUBZERO_DB_TYPE", "postgresql");
        env::set_var("SUBZERO_DB_SCHEMAS", "[test]");
        env::set_var("SUBZERO_DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_JWT_SECRET", "reallyreallyreallyreallyverysafe");
        env::set_var("SUBZERO_DB_USE_LEGACY_GUCS", "false");
        env::set_var("SUBZERO_URL_PREFIX", "/rest");
        // env::set_var(
        //     "SUBZERO_DB_SCHEMA_STRUCTURE",
        //     "{sql_file=../rocket/tests/postgresql/custom_introspection/postgresql_introspection_query.sql}",
        // );
        env::set_var("SUBZERO_DB_SCHEMA_STRUCTURE", "{sql_file=../../../../introspection/postgresql_introspection_query.sql}");
        env::set_var("SUBZERO_DISABLE_INTERNAL_PERMISSIONS", "true");
        env::remove_var("SUBZERO_DB_MAX_ROWS");
        lazy_static::initialize(client);
    });
}

pub fn normalize_url(url: &str) -> String {
    url.replace(' ', "%20").replace('"', "%22").replace('>', "%3E")
}
pub fn add_header<'a>(mut request: LocalRequest<'a>, name: &'static str, value: &'static str) -> LocalRequest<'a> {
    request.add_header(Header::new(name, value));
    if name == "Cookie" {
        let cookies = value.split(';').filter_map(|s| Cookie::parse_encoded(s.trim()).ok()).collect::<Vec<_>>();
        request.cookies(cookies)
    } else {
        request
    }
}
