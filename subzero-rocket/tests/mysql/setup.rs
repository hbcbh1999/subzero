//use super::super::start; //super in
//use rocket::local::asynchronous::Client;
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
use mysql::*;
use mysql::prelude::*;
use super::super::start;

pub static INIT_DB: Once = Once::new();
//pub static INIT_CLIENT: Once = Once::new();
lazy_static! {
    static ref CLIENT_INNER: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
    pub static ref RUNTIME: tokio::runtime::Runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    static ref MYSQL_POOL: Pool = {
        match std::env::var("SUBZERO_DB_URI") {
            Ok(u) => Pool::new(u.as_str()).unwrap(),
            Err(_) => panic!("SUBZERO_DB_URI not set"),
        }
    };
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
    init_db_once.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let fixtures_dir = project_dir.join("tests/mysql/fixtures");
        assert!(env::set_current_dir(&fixtures_dir).is_ok());
        let mysql_db_uri = option_env!("MYSQL_DB_URI");
        let db_uri: String = match mysql_db_uri {
            Some(db_uri) => db_uri.to_owned(),
            None => {
                // bin/ephemerial_db.sh -t mysql -s $(pwd)"/mysql/fixtures" -w 5 -u john -p "securepass" -q mydb
                let tmp_db_cmd = project_dir.join("tests/bin/ephemerial_db.sh");
                let start_time = std::time::Instant::now();
                println!("starting tmp db");
                // random int between 1 and 1000
                let random_int = rand::random::<u16>();
                let container_name = format!("mysql_test_db_{random_int}");
                let output = Command::new(tmp_db_cmd)
                    .arg("-t")
                    .arg("mysql")
                    .arg("-u")
                    .arg("authenticator")
                    .arg("-p")
                    .arg("authenticator")
                    .arg("-d")
                    .arg("public")
                    .arg("-s")
                    .arg(fixtures_dir.to_str().unwrap())
                    .arg("-w")
                    .arg("60")
                    .arg("-q")
                    .arg(&container_name)
                    .output()
                    .expect("failed to start temporary db process");
                println!("started tmp db in {}s", start_time.elapsed().as_secs());
                if !output.status.success() {
                    println!("status: {}", output.status);
                    println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
                    println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
                } else {
                    println!("stdout success: {}", String::from_utf8_lossy(&output.stdout));
                }

                assert!(output.status.success());

                let db_uri = String::from_utf8_lossy(&output.stdout);
                db_uri.into_owned()
            }
        };
        env::set_var("SUBZERO_DB_URI", db_uri);
    });
    let mut conn = MYSQL_POOL.get_conn().unwrap();
    let _ = conn.query_drop("call reset_auto_increment()");
}

pub fn setup_client<T>(init_client_once: &Once, client: &'static T)
where
    T: LazyStatic + Send + Sync + 'static,
    // pub fn setup_client<T>(init_client_once: &Once, client: &AsyncOnce<Client>)
{
    println!("setup_client");
    init_client_once.call_once(|| {
        env::set_var("SUBZERO_CONFIG", "inexistent_config.toml");
        env::set_var("SUBZERO_DB_ANON_ROLE", "mysql_test_anonymous");
        env::set_var("SUBZERO_DB_TX_ROLLBACK", "true");
        env::set_var("SUBZERO_DB_TYPE", "mysql");
        env::set_var("SUBZERO_DB_SCHEMAS", "[public]");
        //env::set_var("SUBZERO_DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_JWT_SECRET", "reallyreallyreallyreallyverysafe");
        //env::set_var("SUBZERO_DB_USE_LEGACY_GUCS", "true");
        env::set_var("SUBZERO_URL_PREFIX", "/rest");
        env::set_var("SUBZERO_DB_SCHEMA_STRUCTURE", "{sql_file=../../../../introspection/mysql_introspection_query.sql}");
        env::set_var("SUBZERO_DISABLE_INTERNAL_PERMISSIONS", "false");
        env::remove_var("SUBZERO_DB_MAX_ROWS");
        lazy_static::initialize(client);
    });
    println!("setup_client done");
}

pub fn normalize_url(url: &str) -> String { url.replace(' ', "%20").replace('"', "%22").replace('>', "%3E") }
pub fn add_header<'a>(mut request: LocalRequest<'a>, name: &'static str, value: &'static str) -> LocalRequest<'a> {
    request.add_header(Header::new(name, value));
    if name == "Cookie" {
        let cookies = value.split(';').filter_map(|s| Cookie::parse_encoded(s.trim()).ok()).collect::<Vec<_>>();
        request.cookies(cookies)
    } else {
        request
    }
}
