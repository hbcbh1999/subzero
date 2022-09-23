//use super::super::start; //super in
//use rocket::local::asynchronous::Client;
use rocket::http::{Cookie, Header};
use rocket::local::asynchronous::LocalRequest;
use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;
//use async_once::AsyncOnce;
use lazy_static::LazyStatic;
pub use crate::haskell_test;

pub static INIT_DB: Once = Once::new();
//static INIT_CLIENT: Once = Once::new();
// lazy_static! {

//     static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
//       Client::untracked(start().await.unwrap()).await.expect("valid client")
//     });

// }
//pub static MAX_ROWS: Option<&'static str> = None;

pub fn setup_db(init_db_once: &Once) {
    //let _ = env_logger::builder().is_test(true).try_init();
    init_db_once.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let tmp_ch_cmd = project_dir.join("tests/bin/clickhouse_tmp.sh");
        let fixtures_dir = project_dir.join("tests/clickhouse/fixtures/");
        let init_file = fixtures_dir.join("load.sql");
        let output = Command::new(tmp_ch_cmd)
            .arg("-t")
            // .arg("-u")
            // .arg("postgrest_test_authenticator")
            .arg("-w").arg("300")
            .arg("-o").arg(format!("--user_files_path={}", fixtures_dir.to_str().unwrap()))
            .output()
            .expect("failed to start temporary ch process");
        if !output.status.success() {
            debug!("status: {}", output.status);
            debug!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            debug!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }
        
        assert!(output.status.success());

        let db_uri = String::from_utf8_lossy(&output.stdout);

        let output = Command::new("tests/bin/ch_run_sql.sh")
            .arg(format!("{}",init_file.to_str().unwrap()))
            .arg(db_uri.clone().into_owned())
            .output()
            .expect("failed to execute process");
        
        if !output.status.success() {
            println!("status: {}", output.status);
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }
        assert!(output.status.success());

        //let init_file_2 = project_dir.join("tests/clickhouse/fixtures/nyc_taxi.sql");
        let init_file_2 = fixtures_dir.join("nyc_taxi.sql");
        let output = Command::new("tests/bin/ch_run_sql.sh")
            .arg(format!("{}",init_file_2.to_str().unwrap()))
            .arg(db_uri.clone().into_owned())
            .output()
            .expect("failed to execute process");
        
        if !output.status.success() {
            println!("status: {}", output.status);
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }
        assert!(output.status.success());

        env::set_var("SUBZERO_DB_URI", &*db_uri);
        env::set_var("SUBZERO_DB_SCHEMA_STRUCTURE", "{sql_file=../introspection/clickhouse_introspection_query.sql}");
        debug!("db init ok clickhouse");
    });

}

pub fn setup_client<T>(init_client_once: &Once, client: &T)
where
    T: LazyStatic,
{
    init_client_once.call_once(|| {
        
        env::set_var("SUBZERO_CONFIG", &"inexistent_config.toml");
        env::set_var("SUBZERO_DB_ANON_ROLE", &"default");
        env::set_var("SUBZERO_DB_TX_ROLLBACK", &"true");
        env::set_var("SUBZERO_DB_TYPE", &"clickhouse");
        env::set_var("SUBZERO_DB_SCHEMAS", "[public]");
        //env::set_var("SUBZERO_DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_JWT_SECRET", "reallyreallyreallyreallyverysafe");
        // env::set_var("SUBZERO_DB_USE_LEGACY_GUCS", "true");
        env::set_var("SUBZERO_URL_PREFIX", "/rest");
        env::remove_var("SUBZERO_DB_MAX_ROWS");
        lazy_static::initialize(client);
    });
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
