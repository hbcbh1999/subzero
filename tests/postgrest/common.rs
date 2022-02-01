
use super::super::start; //super in
use rocket::local::asynchronous::Client;
use std::sync::Once;
use std::process::Command;
use std::path::PathBuf;
use std::env;
use async_once::AsyncOnce;

static INIT: Once = Once::new();
lazy_static! {
    
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
      Client::untracked(start().await.unwrap()).await.expect("valid client")
    });
}

pub fn setup() {
    //let _ = env_logger::builder().is_test(true).try_init();
    INIT.call_once(|| {
        // initialization code here
        let project_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        
        let tmp_pg_cmd = project_dir.join("tests/bin/pg_tmp.sh");
        let init_file = project_dir.join("tests/postgrest/fixtures/load.sql");

        let output = Command::new(tmp_pg_cmd).arg("-t").arg("-u").arg("postgrest_test_authenticator").output().expect("failed to start temporary pg process");
        // println!("status: {}", output.status);
        // println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        // println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        let db_uri =  String::from_utf8_lossy(&output.stdout);
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_URI", &*db_uri);
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_ANON_ROLE", &"postgrest_test_anonymous");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_TX_ROLLBACK", &"true");

        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS", "[test]");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_VHOSTS__DEFAULT__JWT_SECRET", "reallyreallyreallyreallyverysafe");

        let output = Command::new("psql").arg("-f").arg(init_file.to_str().unwrap()).arg(db_uri.into_owned()).output().expect("failed to execute process");
        // println!("status: {}", output.status);
        // println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        // println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        
        lazy_static::initialize(&CLIENT);
    });
}

pub fn normalize_url(url: &String) -> String {
    url
    .replace(" ", "%20")
    .replace("\"","%22")
    .replace(">","%3E")
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
                        //$([$(($get_2_header_nn:literal,$get_2_header_v:literal)),+])?
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
                        //$([ $( ($post_2_header_nn:literal,$post_2_header_v:literal) ),+])?
                        $([json|$json2_body:literal|])?
                        $([text|$text2_body:literal|])?
                        $($json22_body:literal)?
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
                    setup();
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
                                              request.add_header(Header::new($get_2_header_nn0,$get_2_header_v0));
                                              if $get_2_header_nn0 == "Cookie" {
                                                for cookie_str in $get_2_header_v0.split(';').map(|s| s.trim()) {
                                                  if let Ok(cookie) = Cookie::parse_encoded(cookie_str) {
                                                      request = request.cookie(cookie.into_owned());
                                                  }
                                                }
                                              }
                                              $(
                                                  request.add_header(Header::new($get_2_header_nn1,$get_2_header_v1));
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
                                              request.add_header(Header::new($post_2_header_nn0,$post_2_header_v0));
                                              if $post_2_header_nn0 == "Cookie" {
                                                for cookie_str in $post_2_header_v0.split(';').map(|s| s.trim()) {
                                                  if let Ok(cookie) = Cookie::parse_encoded(cookie_str) {
                                                      request = request.cookie(cookie.into_owned());
                                                  }
                                                }
                                              }
                                              $(
                                                  request.add_header(Header::new($post_2_header_nn1,$post_2_header_v1));
                                                  $(
                                                    request.add_header(Header::new($post_2_header_nn2,$post_2_header_v2));
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