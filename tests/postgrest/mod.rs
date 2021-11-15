
use super::*; //super in
use serde_json::Value;

use rocket::{Rocket, Build, Config as RocketConfig};
use rocket::local::asynchronous::Client;
use rocket::http::Status;
use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
use std::sync::Once;
use std::process::Command;
use std::path::PathBuf;
use std::env;
extern crate speculate;
use demonstrate::demonstrate;

static INIT: Once = Once::new();

lazy_static! {
    static ref CONFIG: Figment = { 
        Figment::from(RocketConfig::default())
            .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
            .merge(Env::prefixed("SUBZERO_").ignore(&["PROFILE"]).global())
            .select(Profile::from_env_or("SUBZERO_PROFILE", Profile::const_new("debug")))
    };
    // static ref DB_SCHEMA: DbSchema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");
}

fn setup() {
    let _ = env_logger::builder().is_test(true).try_init();
    INIT.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        
        let tmp_pg_cmd = project_dir.join("tests/bin/pg_tmp.sh");
        let init_file = project_dir.join("tests/postgrest/fixtures/load.sql");

        let output = Command::new(tmp_pg_cmd).arg("-t").arg("-u").arg("postgrest_test_authenticator").output().expect("failed to start temporary pg process");
        println!("status: {}", output.status);
        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        let db_uri =  String::from_utf8_lossy(&output.stdout);
        env::set_var("SUBZERO_DB_URI", &*db_uri);

        env::set_var("SUBZERO_DB_SCHEMAS", "[test]");

        let output = Command::new("psql").arg("-f").arg(init_file.to_str().unwrap()).arg(db_uri.into_owned()).output().expect("failed to execute process");
        println!("status: {}", output.status);
        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        lazy_static::initialize(&CONFIG);
        // lazy_static::initialize(&DB_SCHEMA);
    });
}

async fn server() -> Rocket<Build> {
    //let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");
    start(&CONFIG).await.unwrap()
}


demonstrate! {
    #[rocket::async_test]
    async describe "postgrest" {
        use super::*;
        before {
            setup();
            let client = Client::tracked(server().await).await.expect("valid client");
        }
        it "hello world" {
            let response = client.get("/").dispatch().await;
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(response.into_string().await.unwrap(), "Hello, world!");
        }
    
        it "simple get" {
            let response = client.get("/rest/projects?select=id,name&id=gt.1&name=eq.IOS").dispatch().await;
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(response.into_string().await.unwrap(), r#"[{"id":3,"name":"IOS"}]"#);
        }
    
        it "simple get two" {
            let response = client.get("/rest/projects?id=in.(1,5)&select=id,clients(id)").dispatch().await;
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(
                serde_json::from_str::<Value>(response.into_string().await.unwrap().as_str()).unwrap(),
                serde_json::from_str::<Value>(r#"[{"id":1,"clients":{"id":1}},{"id":5,"clients":null}]"#).unwrap()
            );
        }
    }
}