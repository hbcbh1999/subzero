use rocket::http::{Cookie, Header};
use rocket::local::asynchronous::LocalRequest;
pub use std::env;
use std::path::PathBuf;
use std::process::Command;
pub use std::sync::Once;
use lazy_static::LazyStatic;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::env::temp_dir;
use std::fs::File;
pub use demonstrate::demonstrate;
pub use crate::haskell_test;
pub use std::thread;
use tokio::runtime::Builder;
pub use rocket::local::asynchronous::Client;
pub use async_once::AsyncOnce;
use super::super::start;

pub static INIT_DB: Once = Once::new();
//pub static INIT_CLIENT: Once = Once::new();
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
        let fixtures_dir = project_dir.join("tests/sqlite/fixtures");
        assert!(env::set_current_dir(fixtures_dir).is_ok());
        let init_file = project_dir.join("tests/sqlite/fixtures/load.sql");
        let mut db = temp_dir();
        db.push(format!("{}.sqlite", thread_rng().sample_iter(&Alphanumeric).take(30).map(char::from).collect::<String>()));

        let file = File::create(&db).unwrap();
        drop(file);
        //debug!("created db file: {:?}", init_file);
        let output = Command::new("sqlite3")
            .arg(db.to_str().unwrap())
            .arg(format!(r#".read {}"#, init_file.to_str().unwrap()))
            .output()
            .expect("failed to setup sqlite db");
        if !output.status.success() {
            println!("status: {}", output.status);
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }
        assert!(output.status.success());

        let db_uri = db.to_str().unwrap();

        env::set_var("SUBZERO_DB_URI", db_uri);

        // let schema_file = project_dir.join("tests/sqlite/fixtures/schema.json");
        // env::set_var(
        //     "SUBZERO_DB_SCHEMA_STRUCTURE",
        //     format!(r#"{{json_file={}}}"#, schema_file.to_str().unwrap()),
        // );

        env::set_var("SUBZERO_DB_SCHEMA_STRUCTURE", "{sql_file=../../../../introspection/sqlite_introspection_query.sql}");
    });
}

pub fn setup_client<T>(init_client_once: &Once, client: &T)
where
    T: LazyStatic,
{
    init_client_once.call_once(|| {
        env::set_var("SUBZERO_CONFIG", "inexistent_config.toml");
        env::set_var("SUBZERO_DB_ANON_ROLE", "anonymous");
        env::set_var("SUBZERO_DB_TX_ROLLBACK", "true");
        env::set_var("SUBZERO_DB_TYPE", "sqlite");
        env::set_var("SUBZERO_DB_SCHEMAS", "[public]");
        env::set_var("SUBZERO_DB_USE_LEGACY_GUCS", "false");
        // env::set_var("SUBZERO_DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_DISABLE_INTERNAL_PERMISSIONS", "false");
        env::set_var("SUBZERO_JWT_SECRET", "reallyreallyreallyreallyverysafe");
        env::set_var("SUBZERO_URL_PREFIX", "/rest");
        lazy_static::initialize(client);
    });
}

pub fn normalize_url(url: &str) -> String {
    url.replace(' ', "%20").replace('\"', "%22").replace('>', "%3E")
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
