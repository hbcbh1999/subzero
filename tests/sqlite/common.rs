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
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use std::env::temp_dir;
use std::fs::File;

pub static INIT_DB: Once = Once::new();
pub fn setup_db(init_db_once: &Once) {
    //let _ = env_logger::builder().is_test(true).try_init();
    init_db_once.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let init_file = project_dir.join("tests/sqlite/fixtures/load.sql");
        let mut db = temp_dir();
        db.push(format!(
            "{}.sqlite",
            thread_rng().sample_iter(&Alphanumeric).take(30).map(char::from).collect::<String>()
        ));

        let file = File::create(&db).unwrap();
        drop(file);
        println!("created db file: {:?}", init_file);
        let output = Command::new("sqlite3")
            .arg(db.to_str().unwrap())
            .arg(format!(r#".read {}"#, init_file.to_str().unwrap()))
            .output()
            .expect("failed to setup sqlite db");
        println!("status: {}", output.status);
        println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        let db_uri = db.to_str().unwrap();

        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_URI", db_uri);

        // let schema_file = project_dir.join("tests/sqlite/fixtures/schema.json");
        // env::set_var(
        //     "SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE",
        //     format!(r#"{{json_file={}}}"#, schema_file.to_str().unwrap()),
        // );

        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMA_STRUCTURE", format!(r#"{{sql_file=sqlite_structure_query.sql}}"#));
    });
}

pub fn setup_client<T>(init_client_once: &Once, client: &T)
where
    T: LazyStatic,
{
    init_client_once.call_once(|| {
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE", &"postgrest_test_anonymous");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_TX_ROLLBACK", &"true");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS", "[_sqlite_public_]");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__JWT_SECRET", "reallyreallyreallyreallyverysafe");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__URL_PREFIX", "/rest");
        lazy_static::initialize(client);
    });
}

pub fn normalize_url(url: &String) -> String { url.replace(" ", "%20").replace("\"", "%22").replace(">", "%3E") }
pub fn add_header<'a>(
    mut request: LocalRequest<'a>,
    name: &'static str,
    value: &'static str,
) -> LocalRequest<'a> {
    request.add_header(Header::new(name, value));
    if name == "Cookie" {
        let cookies = value
            .split(';')
            .filter_map(|s| Cookie::parse_encoded(s.trim()).ok())
            .collect::<Vec<_>>();
        request.cookies(cookies)
    } else {
        request
    }
}

#[macro_export]
macro_rules! haskell_test {
    (@status $status_code:ident $status:literal) => {
        println!("matching status: ===\n{}\n====", $status );
        self::assert_eq!($status_code, $status);
    };
    (@header $headers:ident $name:literal $value:literal) => {
        println!("matching header: {}: {} against {:?}", $name, $value, $headers );
        assert!($headers.contains(&($name.to_string(), $value.to_string())));
    };
    (@add_header $request:ident $name:literal $value:literal) => {
        $request = add_header($request, $name, $value);
    };
    (@body_json $response:ident $json:literal) => {
        let body = match $response.into_string().await {
            Some(b) => b,
            None => "no body".to_string()
        };
        println!("expected: ===\n{}\n====", $json );
        println!("body: ===\n{}\n====", body );
        self::assert_eq!(
            serde_json::from_str::<Value>(body.as_str()).map_err(|e| format!("{}",e)),
            serde_json::from_str::<Value>($json).map_err(|e| format!("{}",e))
        );
    };
    (@body_text $response:ident $text:literal) => {
        let body = match $response.into_string().await {
            Some(b) => b,
            None => "no body".to_string()
        };
        println!("expected: ===\n{}\n====", $text );
        println!("body: ===\n{}\n====", body );
        self::assert_eq!(body.as_str(),$text);
    };
    (@body_str $response:ident $str:literal) => {
      let body = match $response.into_string().await {
          Some(b) => b,
          None => "no body".to_string()
      };
      let s = format!("\"{}\"", $str);
      println!("expected: ===\n{}\n====", s );
      println!("body: ===\n{}\n====", body );
      self::assert_eq!(body.as_str(),s);
  };
    (
        $(feature $feature:literal
        $(
            describe $describe:literal $dollar1:tt $(do)?
            $(
                it $it:literal $dollar2:tt $(do)?
                $(
                    //request methodGet "/images?name=eq.A.png" (acceptHdrs "application/octet-stream") ""
                    $(let $token_var:ident = $(authHeaderJWT)? $jwt_token:literal $(in)?)?

                    $(get $get1_url:literal)?
                    $(post $post_url:literal [json|$json_body:literal|])?

                    $(request methodGet $get2_url:literal
                        $([auth])?
                        $([
                          ($get_2_header_nn0:literal,$get_2_header_v0:literal)
                          $(,($get_2_header_nn1:literal,$get_2_header_v1:literal))?
                        ])?
                        $((acceptHdrs $get2_accept_header:literal))?
                        $($get2_body:literal)?
                    )?
                    $(request methodPost $post2_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $post2_accept_header:literal))?
                        $([
                            $(authHeaderJWT $post_2_jwt_token:literal , )?
                            ($post_2_header_nn0:literal,$post_2_header_v0:literal)
                            $(
                              ,($post_2_header_nn1:literal,$post_2_header_v1:literal)
                              $(,($post_2_header_nn2:literal,$post_2_header_v2:literal))?
                            )?
                        ])?
                        $([json|$json2_body:literal|])?
                        $([text|$text2_body:literal|])?
                        $($json22_body:literal)?
                    )?

                    $(request methodDelete $delete_url:literal
                        $([
                            ($delete_header_nn0:literal,$delete_header_v0:literal)
                            $(
                              ,($delete_header_nn1:literal,$delete_header_v1:literal)
                              $(,($delete_header_nn2:literal,$delete_header_v2:literal))?
                            )?
                        ])?
                        $($delete_body:literal)?
                    )?

                    $(request methodPatch $patch_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $patch_accept_header:literal))?
                        $([
                            $(authHeaderJWT $patch_jwt_token:literal , )?
                            ($patch_header_nn0:literal,$patch_header_v0:literal)
                            $(
                              ,($patch_header_nn1:literal,$patch_header_v1:literal)
                              $(,($patch_header_nn2:literal,$patch_header_v2:literal))?
                            )?
                        ])?
                        $([])?
                        $([json|$patch_json_body:literal|])?
                        $([text|$patch_text_body:literal|])?
                        $($patch_body:literal)?
                    )?

                    $(request methodPut $put_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $put_accept_header:literal))?
                        $([
                            $(authHeaderJWT $put_jwt_token:literal , )?
                            ($put_header_nn0:literal,$put_header_v0:literal)
                            $(
                              ,($put_header_nn1:literal,$put_header_v1:literal)
                              $(,($put_header_nn2:literal,$put_header_v2:literal))?
                            )?
                        ])?
                        $([])?
                        $([json|$put_json_body:literal|])?
                        $([text|$put_text_body:literal|])?
                        $($put_body:literal)?
                    )?

                    shouldRespondWith
                    $($status_simple:literal)?
                    $([json|$json:literal|])?
                    $([text|$text:literal|])?
                    $([str|$str:literal|])?
                    $({
                            $(matchStatus = $status:literal)?
                            $($(,)? matchHeaders = [
                                $($header_name:literal <:> $header_value:literal),*
                            ])?
                    })?
                )*
            )*
        )*
        )*
    ) => {
        demonstrate! {
            $(
            #[rocket::async_test]
            async describe $feature {
                use super::*;
                before {
                    setup_db(&INIT_DB);
                    setup_client(&INIT_CLIENT, &CLIENT);
                }
                  $(
                      describe $describe {
                          $(
                              it $it {
                                  $(
                                      {
                                          let client = CLIENT.get().await;

                                          $(
                                            let url = format!("/rest{}",$get1_url);
                                            let mut request = client.get(normalize_url(&url));
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                          )?
                                          $(
                                            let url = format!("/rest{}",$get2_url);
                                            let mut request = client.get(normalize_url(&url));
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                            $(request.add_header(Accept::from_str($get2_accept_header).unwrap());)?
                                            //$($(request.add_header(Header::new($get_2_header_nn,$get_2_header_v));),+)?
                                            $(
                                              haskell_test!(@add_header request $get_2_header_nn0 $get_2_header_v0);
                                              $(
                                                  haskell_test!(@add_header request $get_2_header_nn1 $get_2_header_v1);
                                              )?
                                            )?
                                          )?

                                          $(
                                            let url = format!("/rest{}",$post_url);
                                            let mut request = client.post(normalize_url(&url))
                                                .body($json_body);
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                          )?

                                          $(
                                            let url = format!("/rest{}",$post2_url);
                                            let mut request = client.post(normalize_url(&url))
                                                .body($($text2_body)? $($json2_body)? $($json22_body)?);
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                            $(request.add_header(Accept::from_str($post2_accept_header).unwrap());)?

                                            $(
                                              $(
                                                request.add_header(Header::new("Authorization", format!("Bearer {}",$post_2_jwt_token)));
                                              )?
                                              haskell_test!(@add_header request $post_2_header_nn0 $post_2_header_v0);
                                              $(
                                                  haskell_test!(@add_header request $post_2_header_nn1 $post_2_header_v1);
                                                  $(
                                                    haskell_test!(@add_header request $post_2_header_nn2 $post_2_header_v2);
                                                  )?
                                              )?
                                            )?
                                          )?

                                          $(
                                            let url = format!("/rest{}",$delete_url);
                                            let mut request = client.delete(normalize_url(&url))
                                                .body($($delete_body)?);
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                            //$(request.add_header(Accept::from_str($delete_accept_header).unwrap());)?

                                            $(
                                              haskell_test!(@add_header request $delete_header_nn0 $delete_header_v0);
                                              $(
                                                haskell_test!(@add_header request $delete_header_nn1 $delete_header_v1);
                                                  $(
                                                    haskell_test!(@add_header request $delete_header_nn2 $delete_header_v2);
                                                  )?
                                              )?
                                            )?
                                          )?

                                          $(
                                            let url = format!("/rest{}",$patch_url);
                                            let mut request = client.patch(normalize_url(&url))
                                                .body($($patch_text_body)? $($patch_json_body)? $($patch_body)?);
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                            $(request.add_header(Accept::from_str($patch_accept_header).unwrap());)?

                                            $(
                                              $(
                                                request.add_header(Header::new("Authorization", format!("Bearer {}",$patch_jwt_token)));
                                              )?
                                              haskell_test!(@add_header request $patch_header_nn0 $patch_header_v0);
                                              $(
                                                  haskell_test!(@add_header request $patch_header_nn1 $patch_header_v1);
                                                  $(
                                                    haskell_test!(@add_header request $patch_header_nn2 $patch_header_v2);
                                                  )?
                                              )?
                                            )?
                                          )?

                                          $(
                                            let url = format!("/rest{}",$put_url);
                                            let mut request = client.put(normalize_url(&url))
                                                .body($($put_text_body)? $($put_json_body)? $($put_body)?);
                                            request.add_header(Accept::from_str("*/*").unwrap());
                                            $(request.add_header(Accept::from_str($put_accept_header).unwrap());)?

                                            $(
                                              $(
                                                request.add_header(Header::new("Authorization", format!("Bearer {}",$put_jwt_token)));
                                              )?
                                              haskell_test!(@add_header request $put_header_nn0 $put_header_v0);
                                              $(
                                                  haskell_test!(@add_header request $put_header_nn1 $put_header_v1);
                                                  $(
                                                    haskell_test!(@add_header request $put_header_nn2 $put_header_v2);
                                                  )?
                                              )?
                                            )?
                                          )?


                                          println!("url ===\n{:?}\n", url);
                                          //request.add_header(Accept::from_str("*/*").unwrap());



                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$jwt_token)));
                                          )?
                                          //println!("request ===\n{:?}\n", request);
                                          let response = request.dispatch().await;
                                          let _status_code = response.status().code;
                                          let _headers = response.headers().iter().map(|h| (h.name().to_string(), h.value().to_string())).collect::<Vec<_>>();
                                          //let _headers = response.headers().clone();
                                          //println!("response ===\n{:?}\n", response);
                                          $(haskell_test!(@body_json response $json);)?
                                          $(haskell_test!(@body_text response $text);)?
                                          $(haskell_test!(@body_str response $str);)?

                                          $(haskell_test!(@status _status_code $status_simple);)?
                                          $($(haskell_test!(@status _status_code $status);)?)?
                                          $($($(haskell_test!(@header _headers $header_name $header_value);)*)?)?
                                          //assert!(false);
                                      }
                                  )?
                              }
                          )*
                      }
                  )*
            }
            )*

        }
    }
}
pub(crate) use haskell_test;
