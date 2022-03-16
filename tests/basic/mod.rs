use super::*; //super in this case is src/main.rs
use rocket::{Build, Rocket};
// use rocket::local::blocking::Client;
use rocket::http::Status;
use rocket::local::asynchronous::Client;

use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;
extern crate speculate;
use async_once::AsyncOnce;
use demonstrate::demonstrate;
static INIT: Once = Once::new();

lazy_static! {

    // static ref DB_SCHEMA: DbSchema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
        Client::untracked(server().await).await.expect("valid client")
      });
}

fn setup() {
    //let _ = env_logger::builder().is_test(true).try_init();
    INIT.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let tmp_pg_cmd = project_dir.join("tests/bin/pg_tmp.sh");
        let init_file = project_dir.join("tests/basic/fixtures/init.sql");

        let output = Command::new(tmp_pg_cmd)
            .arg("-t")
            .arg("-u")
            .arg("anonymous")
            .output()
            .expect("failed to start temporary pg process");
        println!("status: {}", output.status);
        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        let db_uri = String::from_utf8_lossy(&output.stdout);
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_URI", &*db_uri);
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS", "[public]");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE", &"anonymous");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE", format!(r#"{{sql_file=postgresql_structure_query.sql}}"#));
        env::set_var("SUBZERO_VHOSTS__DEFAULT__URL_PREFIX", "/rest");
        //env::set_var("SUBZERO_PORT", &"8001");

        let output = Command::new("psql")
            .arg("-f")
            .arg(init_file.to_str().unwrap())
            .arg(db_uri.into_owned())
            .output()
            .expect("failed to execute process");
        println!("status: {}", output.status);
        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        lazy_static::initialize(&CLIENT);
        //println!("{:?}", *CONFIG);

        // lazy_static::initialize(&DB_SCHEMA);
    });
}

async fn server() -> Rocket<Build> {
    //let db_schema = serde_json::from_str::<DbSchema>(JSON_SCHEMA).expect("failed to parse json schema");
    start().await.unwrap()
}

// #[rocket::async_test]
// async fn hello_world()
// {
//     setup();
//     let client = Client::tracked(server().await).expect("valid client");
//     let response = client.get("/").dispatch();
//     assert_eq!(response.status(), Status::Ok);
//     assert_eq!(response.into_string().unwrap(), "Hello, world!");
// }

// demonstrate! {

//     describe "basic" {
//         use super::*;
//         #[rocket::async_test]
//         async it "hello worlds" {
//             setup();
//             let client = Client::tracked(server().await).expect("valid client");
//             // let response = client.get("/").dispatch();
//             // assert_eq!(response.status(), Status::Ok);
//             // assert_eq!(response.into_string().unwrap(), "Hello, world!");
//         }
//     }
// }

demonstrate! {
    #[rocket::async_test]
    async describe "basic" {
        use super::*;
        before {
            setup();
            //let client = Client::tracked(server().await).await.expect("valid client");

        }
        it "hello world" {
            let client = CLIENT.get().await;
            let response = client.get("/").dispatch().await;
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(response.into_string().await.unwrap(), "Hello, world!");
        }

        it "simple get" {
            let client = CLIENT.get().await;
            let response = client.get("/rest/projects?select=id,name&id=gt.1&name=eq.IOS").dispatch().await;
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(response.into_string().await.unwrap(), r#"[{"id":3,"name":"IOS"}]"#);
        }

        it "simple get two" {
            let client = CLIENT.get().await;
            let response = client.get("/rest/projects?select=id&id=gt.1&name=eq.IOS").dispatch().await;
            println!("{:?}", response);
            assert_eq!(response.status(), Status::Ok);
            assert_eq!(response.into_string().await.unwrap(), r#"[{"id":3}]"#);

            //assert!(false);
        }
    }
}
