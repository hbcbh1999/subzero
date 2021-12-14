
use super::*; //super in
use serde_json::Value;

use rocket::{Config as RocketConfig};
use rocket::local::asynchronous::Client;
use rocket::http::Accept;
use std::str::FromStr;
use figment::{Figment, Profile, };
use figment::providers::{Env, Toml, Format};
use std::sync::Once;
use std::process::Command;
use std::path::PathBuf;
use std::env;
use demonstrate::demonstrate;
use async_once::AsyncOnce;

static INIT: Once = Once::new();
use pretty_assertions::{assert_eq};

lazy_static! {
    static ref CONFIG: Figment = { 
        Figment::from(RocketConfig::default())
            .merge(Toml::file(Env::var_or("SUBZERO_CONFIG", "config.toml")).nested())
            .merge(Env::prefixed("SUBZERO_").ignore(&["PROFILE"]).global())
            .select(Profile::from_env_or("SUBZERO_PROFILE", Profile::const_new("debug")))
    };
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
      Client::untracked(start(&CONFIG).await.unwrap()).await.expect("valid client")
    });
}

fn setup() {
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
        env::set_var("SUBZERO_DB_URI", &*db_uri);
        env::set_var("SUBZERO_DB_ANON_ROLE", &"postgrest_test_anonymous");

        env::set_var("SUBZERO_DB_SCHEMAS", "[test]");
        env::set_var("SUBZERO_DB_PRE_REQUEST", "test.switch_role");
        env::set_var("SUBZERO_JWT_SECRET", "reallyreallyreallyreallyverysafe");

        let output = Command::new("psql").arg("-f").arg(init_file.to_str().unwrap()).arg(db_uri.into_owned()).output().expect("failed to execute process");
        // println!("status: {}", output.status);
        // println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        // println!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());

        lazy_static::initialize(&CONFIG);
        lazy_static::initialize(&CLIENT);
    });
}


macro_rules! haskell_test {
    (@status $status_code:ident $status:literal) => {
        println!("matching status: ===\n{}\n====", $status );
        self::assert_eq!($status_code, $status);
    };
    (@header $headers:ident $name:literal $value:literal) => {
        println!("matching header: {}: {}", $name, $value );
        self::assert_eq!($headers.get($name), Some(&$value.to_string()));
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
                   
                    $(request methodGet $get2_url:literal $([$header:ident])? $($get2_body:literal)?)?
                    $(
                        (acceptHdrs $accept_header:literal) ""
                    )?

                    $(post $post_url:literal [json|$json_body:literal|])?

                    $(request methodPost $post2_url:literal [$post_header:ident] $([json|$json2_body:literal|])? $($json22_body:literal)?)?

                    shouldRespondWith
                    $($status_simple:literal)?
                    $([json|$json:literal|])?
                    $([text|$text:literal|])?
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
                                            let mut request = client.get(url.replace(" ", "%20"));
                                          )?
                                          $(
                                            let url = format!("/rest{}",$get2_url);
                                            let mut request = client.get(url.replace(" ", "%20"));
                                          )?

                                          $(
                                            let url = format!("/rest{}",$post_url);
                                            let mut request = client.post(url.replace(" ", "%20"))
                                                .body($json_body);
                                          )?

                                          $(
                                            let url = format!("/rest{}",$post2_url);
                                            let mut request = client.post(url.replace(" ", "%20"))
                                                .body($($json2_body)? $($json22_body)?);
                                          )?


                                          println!("url ===\n{:?}\n", url);
                                          request.add_header(Accept::from_str("*/*").unwrap());

                                          $(
                                          request.add_header(Accept::from_str($accept_header).unwrap());
                                          )?

                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$jwt_token)));
                                          )?
                                          //println!("request ===\n{:?}\n", request);
                                          let response = request.dispatch().await;
                                          let _status_code = response.status().code;
                                          let _headers = response.headers().iter().map(|h| (h.name().to_string(), h.value().to_string())).collect::<HashMap<_,_>>();
                                          //println!("response ===\n{:?}\n", response);
                                          $(haskell_test!(@body_json response $json);)?
                                          $(haskell_test!(@body_text response $text);)?
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

haskell_test! {
feature "rpc"
  describe "a proc that returns a set" $ do
    it "returns proper json" $ do
      post "/rpc/getitemrange" [json| r#"{ "min": 2, "max": 4 }"# |] shouldRespondWith
        [json| r#"[ {"id": 3}, {"id":4} ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/rpc/getitemrange?min=2&max=4" shouldRespondWith
        [json| r#"[ {"id": 3}, {"id":4} ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
feature "auth"
  describe "all" $
    it "denies access to tables that anonymous does not own" $
      get "/authors_only" shouldRespondWith
        [json| r#"{
          "hint":null,
          "details":null,
          "code":"42501",
          "message":"permission denied for table authors_only"} "#|]
      { matchStatus = 401
      , matchHeaders = ["WWW-Authenticate" <:> "Bearer"]
      }
    it "denies access to tables that postgrest_test_author does not own" $
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIn0.Xod-F15qsGL0WhdOCr2j3DdKuTw9QJERVgoFD3vGaWA" in
      request methodGet "/private_table" [auth]
        shouldRespondWith
          [json|r#" {
            "hint":null,
            "details":null,
            "code":"42501",
            "message":"permission denied for table private_table"} "#|]
        { matchStatus = 403
        , matchHeaders = []
        }

    it "denies execution on functions that anonymous does not own" $
      post "/rpc/privileged_hello" [json|r#"{"name": "anonymous"}"#|] shouldRespondWith 401

    it "allows execution on a function that postgrest_test_author owns" $
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIn0.Xod-F15qsGL0WhdOCr2j3DdKuTw9QJERVgoFD3vGaWA" in
      request methodPost "/rpc/privileged_hello" [auth] [json|r#"{"name": "jdoe"}"#|]
        shouldRespondWith [json|r#""Privileged hello to jdoe""#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "returns jwt functions as jwt tokens" $
      request methodPost "/rpc/login" [single]
        [json|r#" { "id": "jdoe", "pass": "1234" } "#|]
        shouldRespondWith [json|r#" {"token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xuYW1lIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.KO-0PGp_rU-utcDBP6qwdd-Th2Fk-ICVt01I7QtTDWs"} "#|]
          { matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"]
          }

    it "sql functions can encode custom and standard claims" $
      request methodPost  "/rpc/jwt_test" [single] "{}"
        shouldRespondWith [json|r#" {"token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJqb2UiLCJzdWIiOiJmdW4iLCJhdWQiOiJldmVyeW9uZSIsImV4cCI6MTMwMDgxOTM4MCwibmJmIjoxMzAwODE5MzgwLCJpYXQiOjEzMDA4MTkzODAsImp0aSI6ImZvbyIsInJvbGUiOiJwb3N0Z3Jlc3RfdGVzdCIsImh0dHA6Ly9wb3N0Z3Jlc3QuY29tL2ZvbyI6dHJ1ZX0.G2REtPnOQMUrVRDA9OnkPJTd8R0tf4wdYOlauh1E2Ek"} "#|]
          { matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"]
          }

    it "sql functions can read custom and standard claims variables" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJmdW4iLCJqdGkiOiJmb28iLCJuYmYiOjEzMDA4MTkzODAsImV4cCI6OTk5OTk5OTk5OSwiaHR0cDovL3Bvc3RncmVzdC5jb20vZm9vIjp0cnVlLCJpc3MiOiJqb2UiLCJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWF0IjoxMzAwODE5MzgwfQ.V5fEpXfpb7feqwVqlcDleFdKu86bdwU2cBRT4fcMhXg"
      request methodPost "/rpc/reveal_big_jwt" [auth] "{}"
        shouldRespondWith [json|r#"[{"iss":"joe","sub":"fun","exp":9999999999,"nbf":1300819380,"iat":1300819380,"jti":"foo","http://postgrest.com/foo":true}]"#|]

    it "allows users with permissions to see their tables" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.B-lReuGNDwAlU1GOC476MlO0vAt9JNoHIlxg2vwMaO0"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith 200

    it "works with tokens which have extra fields" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIiwia2V5MSI6InZhbHVlMSIsImtleTIiOiJ2YWx1ZTIiLCJrZXkzIjoidmFsdWUzIiwiYSI6MSwiYiI6MiwiYyI6M30.b0eglDKYEmGi-hCvD-ddSqFl7vnDO5qkUaviaHXm3es"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith 200

    //-- this test will stop working 9999999999s after the UNIX EPOCH
    it "succeeds with an unexpired token" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjk5OTk5OTk5OTksInJvbGUiOiJwb3N0Z3Jlc3RfdGVzdF9hdXRob3IiLCJpZCI6Impkb2UifQ.Dpss-QoLYjec5OTsOaAc3FNVsSjA89wACoV-0ra3ClA"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith 200

    it "fails with an expired token" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE0NDY2NzgxNDksInJvbGUiOiJwb3N0Z3Jlc3RfdGVzdF9hdXRob3IiLCJpZCI6Impkb2UifQ.f8__E6VQwYcDqwHmr9PG03uaZn8Zh1b0vbJ9DYS0AdM"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith [json|r#" {"message":"JWT expired"} "#|]
          { matchStatus = 401
          , matchHeaders = [
              "WWW-Authenticate" <:>
              "Bearer error=\"invalid_token\", error_description=\"JWT expired\""
            ]
          }

    it "hides tables from users with invalid JWT" $ do
      let auth = authHeaderJWT "ey9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.y4vZuu1dDdwAl0-S00MCRWRYMlJ5YAMSir6Es6WtWx0"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith [json|r#" {"message":"InvalidToken"} "#|]
          { matchStatus = 401
          , matchHeaders = [
              "WWW-Authenticate" <:>
              "Bearer error=\"invalid_token\", error_description=\"InvalidToken\""
            ]
          }

    it "should fail when jwt contains no claims" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.CUIP5V9thWsGGFsFyGijSZf1fJMfarLHI9CEJL-TGNk"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith 401

    it "hides tables from users with JWT that contain no claims about role" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6Impkb2UifQ.RVlZDaSyKbFPvxUf3V_NQXybfRB4dlBIkAUQXVXLUAI"
      request methodGet "/authors_only" [auth] ""
        shouldRespondWith 401

    // it "recovers after 401 error with logged in user" $ do
    //   _ <- post "/authors_only" [json|r#" { "owner": "jdoe", "secret": "test content" } "#|]
    //   let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.B-lReuGNDwAlU1GOC476MlO0vAt9JNoHIlxg2vwMaO0"
    //   _ <- request methodPost "/rpc/problem" [auth] ""
    //   request methodGet "/authors_only" [auth] ""
    //     shouldRespondWith 200

  //describe "custom pre-request proc acting on id claim" $ do

    it "able to switch to postgrest_test_author role (id=1)" $
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MX0.gKw7qI50i9hMrSJW8BlTpdMEVmMXJYxlAqueGqpa_mE" in
      request methodPost "/rpc/get_current_user" [auth]
        [json|r#" {} "#|]
         shouldRespondWith [json|r#""postgrest_test_author""#|]
          { matchStatus = 200
          , matchHeaders = []
          }

    it "able to switch to postgrest_test_default_role (id=2)" $
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6Mn0.nwzjMI0YLvVGJQTeoCPEBsK983b__gxdpLXisBNaO2A" in
      request methodPost "/rpc/get_current_user" [auth]
        [json|r#" {} "#|]
         shouldRespondWith [json|r#""postgrest_test_default_role""#|]
          { matchStatus = 200
          , matchHeaders = []
          }

    it "raises error (id=3)" $
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6M30.OGxEJAf60NKZiTn-tIb2jy4rqKs_ZruLGWZ40TjrJsM" in
      request methodPost "/rpc/get_current_user" [auth]
        [json|r#" {} "#|]
         shouldRespondWith [json|r#"{"hint":"Please contact administrator","details":null,"code":"P0001","message":"Disabled ID --> 3"}"#|]
          { matchStatus = 400
          , matchHeaders = []
          }

  // it "allows 'Bearer' and 'bearer' as authentication schemes" $ do
  //   let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.B-lReuGNDwAlU1GOC476MlO0vAt9JNoHIlxg2vwMaO0"
  //   request methodGet "/authors_only" [authHeader "Bearer" token] ""
  //     shouldRespondWith 200
  //   request methodGet "/authors_only" [authHeader "bearer" token] ""
  //     shouldRespondWith 200


feature "query"
  describe "Querying a table with a column called count" $
    it "should not confuse count column with pg_catalog count aggregate" $
      get "/has_count_column" shouldRespondWith 200

  describe "Querying a table with a column called t" $
    it "should not conflict with internal postgrest table alias" $
      get "/clashing_column?select=t" shouldRespondWith 200

  describe "Querying a nonexistent table" $
    it "causes a 404" $
      get "/faketable" shouldRespondWith 404

  describe "Filtering response" $ do
    it "matches with equality" $
      get "/items?id=eq.5"
        shouldRespondWith [json|r#" [{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-0/*"] }

    it "matches with equality using not operator" $
      get "/items?id=not.eq.5&order=id"
        shouldRespondWith [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-13/*"] }

    it "matches with more than one condition using not operator" $
      get "/simple_pk?k=like.*yx&extra=not.eq.u" shouldRespondWith [json|"[]"|]

    it "matches with inequality using not operator" $ do
      get "/items?id=not.lt.14&order=id.asc"
        shouldRespondWith [json|r#" [{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }
      get "/items?id=not.gt.2&order=id.asc"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

    it "matches items IN" $
      get "/items?id=in.(1,3,5)"
        shouldRespondWith [json|r#" [{"id":1},{"id":3},{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-2/*"] }

    it "matches items NOT IN using not operator" $
      get "/items?id=not.in.(2,4,6,7,8,9,10,11,12,13,14,15)"
        shouldRespondWith [json|r#" [{"id":1},{"id":3},{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-2/*"] }

    it "matches nulls using not operator" $
      get "/no_pk?a=not.is.null" shouldRespondWith
        [json|r#" [{"a":"1","b":"0"},{"a":"2","b":"0"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches nulls in varchar and numeric fields alike" $ do
      get "/no_pk?a=is.null" shouldRespondWith
        [json|r#" [{"a": null, "b": null}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

      get "/nullable_integer?a=is.null" shouldRespondWith [json|r#"[{"a":null}]"#|]

    it "matches with like" $ do
      get "/simple_pk?k=like.*yx" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"}]"#|]
      get "/simple_pk?k=like.xy*" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"}]"#|]
      get "/simple_pk?k=like.*YY*" shouldRespondWith
        [json|r#"[{"k":"xYYx","extra":"v"}]"#|]

    it "matches with like using not operator" $
      get "/simple_pk?k=not.like.*yx" shouldRespondWith
        [json|r#"[{"k":"xYYx","extra":"v"}]"#|]

    it "matches with ilike" $ do
      get "/simple_pk?k=ilike.xy*&order=extra.asc" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"},{"k":"xYYx","extra":"v"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/simple_pk?k=ilike.*YY*&order=extra.asc" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"},{"k":"xYYx","extra":"v"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with ilike using not operator" $
      get "/simple_pk?k=not.ilike.xy*&order=extra.asc" shouldRespondWith [json|"[]"|]

    describe "Full text search operator" $ do
      it "finds matches with to_tsquery" $
        get "/tsearch?text_search_vector=fts.impossible" shouldRespondWith
          [json|r#" [{"text_search_vector": "'fun':5 'imposs':9 'kind':3" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can use lexeme boolean operators(&=%26, |=%7C, !) in to_tsquery" $ do
        get "/tsearch?text_search_vector=fts.fun%26possible" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=fts.impossible%7Cpossible" shouldRespondWith
          [json|r#" [
          {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
          {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=fts.fun%26!possible" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'fun':5 'imposs':9 'kind':3"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "finds matches with plainto_tsquery" $
        get "/tsearch?text_search_vector=plfts.The%20Fat%20Rats" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      //when (actualPgVersion >= pgVersion112) $ do
        it "finds matches with websearch_to_tsquery" $
            get "/tsearch?text_search_vector=wfts.The%20Fat%20Rats" shouldRespondWith
                [json|r#" [ {"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "can use boolean operators(and, or, -) in websearch_to_tsquery" $ do
          get "/tsearch?text_search_vector=wfts.fun%20and%20possible"
            shouldRespondWith
              [json|r#" [ {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/tsearch?text_search_vector=wfts.impossible%20or%20possible"
            shouldRespondWith
              [json|r#" [
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
                {"text_search_vector": "'also':2 'fun':3 'possibl':8"}]
                  "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/tsearch?text_search_vector=wfts.fun%20and%20-possible"
            shouldRespondWith
              [json|r#" [ {"text_search_vector": "'fun':5 'imposs':9 'kind':3"}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "finds matches with different dictionaries" $ do
        get "/tsearch?text_search_vector=fts(french).amusant" shouldRespondWith
          [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=plfts(french).amusant%20impossible" shouldRespondWith
          [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

        //when (actualPgVersion >= pgVersion112) $
            get "/tsearch?text_search_vector=wfts(french).amusant%20impossible"
                shouldRespondWith
                  [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can be negated with not operator" $ do
        get "/tsearch?text_search_vector=not.fts.impossible%7Cfat%7Cfun" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=not.fts(english).impossible%7Cfat%7Cfun" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=not.plfts.The%20Fat%20Rats" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
            {"text_search_vector": "'also':2 'fun':3 'possibl':8"},
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        //when (actualPgVersion >= pgVersion112) $
            get "/tsearch?text_search_vector=not.wfts(english).impossible%20or%20fat%20or%20fun"
                shouldRespondWith
                  [json|r#" [
                    {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
                    {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

//       //when (actualPgVersion >= pgVersion96) $
        describe "Use of the phraseto_tsquery function" $ do
          it "finds matches" $
            get "/tsearch?text_search_vector=phfts.The%20Fat%20Cats" shouldRespondWith
              [json|r#" [{"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "finds matches with different dictionaries" $
            get "/tsearch?text_search_vector=phfts(german).Art%20Spass" shouldRespondWith
              [json|r#" [{"text_search_vector": "'art':4 'spass':5 'unmog':7" }] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can be negated with not operator" $
            get "/tsearch?text_search_vector=not.phfts(english).The%20Fat%20Cats" shouldRespondWith
              [json|r#" [
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
                {"text_search_vector": "'also':2 'fun':3 'possibl':8"},
                {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
                {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can be used with or query param" $
            get "/tsearch?or=(text_search_vector.phfts(german).Art%20Spass, text_search_vector.phfts(french).amusant, text_search_vector.fts(english).impossible)" shouldRespondWith
              [json|r#"[
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3" },
                {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" },
                {"text_search_vector": "'art':4 'spass':5 'unmog':7"}
              ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with computed column" $
      get "/items?always_true=eq.true&order=id.asc" shouldRespondWith
        [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":5},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "order by computed column" $
      get "/items?order=anti_id.desc" shouldRespondWith
        [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":5},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "cannot access a computed column that is outside of the config schema" $
      get "/items?always_false=is.false" shouldRespondWith 400

    it "matches filtering nested items 2" $
      get "/clients?select=id,projects(id,tasks2(id,name))&projects.tasks.name=like.Design*" shouldRespondWith
        [json|r#" {
          "hint":"If a new foreign key between these entities was created in the database, try reloading the schema cache.",
          "message":"Could not find a relationship between projects and tasks2 in the schema cache"}"#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "matches filtering nested items" $
      get "/clients?select=id,projects(id,tasks(id,name))&projects.tasks.name=like.Design*" shouldRespondWith
        [json|r#"[{"id":1,"projects":[{"id":1,"tasks":[{"id":1,"name":"Design w7"}]},{"id":2,"tasks":[{"id":3,"name":"Design w10"}]}]},{"id":2,"projects":[{"id":3,"tasks":[{"id":5,"name":"Design IOS"}]},{"id":4,"tasks":[{"id":7,"name":"Design OSX"}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with cs operator" $
      get "/complex_items?select=id&arr_data=cs.{2}" shouldRespondWith
        [json|r#"[{"id":2},{"id":3}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with cd operator" $
      get "/complex_items?select=id&arr_data=cd.{1,2,4}" shouldRespondWith
        [json|r#"[{"id":1},{"id":2}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "Shaping response with select parameter" $ do
    it "selectStar works in absense of parameter" $
      get "/complex_items?id=eq.3" shouldRespondWith
        [json|r#"[{"id":3,"name":"Three","settings":{"foo":{"int":1,"bar":"baz"}},"arr_data":[1,2,3],"field-with_sep":1}]"#|]

    it "dash `-` in column names is accepted" $
      get "/complex_items?id=eq.3&select=id,field-with_sep" shouldRespondWith
        [json|r#"[{"id":3,"field-with_sep":1}]"#|]

    it "one simple column" $
      get "/complex_items?select=id" shouldRespondWith
        [json|r#" [{"id":1},{"id":2},{"id":3}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "rename simple column" $
      get "/complex_items?id=eq.1&select=myId:id" shouldRespondWith
        [json|r#" [{"myId":1}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

//     it "one simple column with casting (text)" $
//       get "/complex_items?select=id::text" shouldRespondWith
//         [json|r#" [{"id":"1"},{"id":"2"},{"id":"3"}] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }

//     it "rename simple column with casting" $
//       get "/complex_items?id=eq.1&select=myId:id::text" shouldRespondWith
//         [json|r#" [{"myId":"1"}] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "json column" $
      get "/complex_items?id=eq.1&select=settings" shouldRespondWith
        [json|r#" [{"settings":{"foo":{"int":1,"bar":"baz"}}}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

//     it "fails on bad casting (wrong cast type)" $
//       get "/complex_items?select=id::fakecolumntype"
//         shouldRespondWith [json|r#" {"hint":null,"details":null,"code":"42704","message":"type \"fakecolumntype\" does not exist"} "#|]
//         { matchStatus  = 400
//         , matchHeaders = []
//         }

    it "requesting parents and children" $
      get "/projects?id=eq.1&select=id, name, clients(*), tasks(id, name)" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parent and renaming primary key" $
      get "/projects?select=name,client:clients(clientId:id,name)" shouldRespondWith
        [json|r#"[
          {"name":"Windows 7","client":{"name": "Microsoft", "clientId": 1}},
          {"name":"Windows 10","client":{"name": "Microsoft", "clientId": 1}},
          {"name":"IOS","client":{"name": "Apple", "clientId": 2}},
          {"name":"OSX","client":{"name": "Apple", "clientId": 2}},
          {"name":"Orphan","client":null}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parent and specifying/renaming one key of the composite primary key" $ do
      get "/comments?select=*,users_tasks(userId:user_id)" shouldRespondWith
        [json|r#"[{"id":1,"commenter_id":1,"user_id":2,"task_id":6,"content":"Needs to be delivered ASAP","users_tasks":{"userId": 2}}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/comments?select=*,users_tasks(taskId:task_id)" shouldRespondWith
        [json|r#"[{"id":1,"commenter_id":1,"user_id":2,"task_id":6,"content":"Needs to be delivered ASAP","users_tasks":{"taskId": 6}}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parents and children while renaming them" $
      get "/projects?id=eq.1&select=myId:id, name, project_client:clients(*), project_tasks:tasks(id, name)" shouldRespondWith
        [json|r#"[{"myId":1,"name":"Windows 7","project_client":{"id":1,"name":"Microsoft"},"project_tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parents and filtering parent columns" $
      get "/projects?id=eq.1&select=id, name, clients(id)" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1}}]"#|]

    it "rows with missing parents are included" $
      get "/projects?id=in.(1,5)&select=id,clients(id)" shouldRespondWith
        [json|r#"[{"id":1,"clients":{"id":1}},{"id":5,"clients":null}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "rows with no children return [] instead of null" $
      get "/projects?id=in.(5)&select=id,tasks(id)" shouldRespondWith
        [json|r#"[{"id":5,"tasks":[]}]"#|]

    it "requesting children 2 levels" $
      get "/clients?id=eq.1&select=id,projects(id,tasks(id))" shouldRespondWith
        [json|r#"[{"id":1,"projects":[{"id":1,"tasks":[{"id":1},{"id":2}]},{"id":2,"tasks":[{"id":3},{"id":4}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation" $
      get "/tasks?select=id,users(id)" shouldRespondWith
        [json|r#"[{"id":1,"users":[{"id":1},{"id":3}]},{"id":2,"users":[{"id":1}]},{"id":3,"users":[{"id":1}]},{"id":4,"users":[{"id":1}]},{"id":5,"users":[{"id":2},{"id":3}]},{"id":6,"users":[{"id":2}]},{"id":7,"users":[{"id":2}]},{"id":8,"users":[]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation with rename" $
      get "/tasks?id=eq.1&select=id,theUsers:users(id)" shouldRespondWith
        [json|r#"[{"id":1,"theUsers":[{"id":1},{"id":3}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation reverse" $
      get "/users?select=id,tasks(id)" shouldRespondWith
        [json|r#"[{"id":1,"tasks":[{"id":1},{"id":2},{"id":3},{"id":4}]},{"id":2,"tasks":[{"id":5},{"id":6},{"id":7}]},{"id":3,"tasks":[{"id":1},{"id":5}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation using composite key" $
      get "/files?filename=eq.autoexec.bat&project_id=eq.1&select=filename,users_tasks(user_id,task_id)" shouldRespondWith
        [json|r#"[{"filename":"autoexec.bat","users_tasks":[{"user_id":1,"task_id":1},{"user_id":3,"task_id":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting data using many<->many relation defined by composite keys" $
      get "/users_tasks?user_id=eq.1&task_id=eq.1&select=user_id,files(filename,content)" shouldRespondWith
        [json|r##"[{"user_id":1,"files":[{"filename":"command.com","content":"#include <unix.h>"},{"filename":"autoexec.bat","content":"@ECHO OFF"},{"filename":"README.md","content":"# make $$$!"}]}]"##|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting data using many<->many (composite keys) relation using hint" $
      get "/users_tasks?user_id=eq.1&task_id=eq.1&select=user_id,files!touched_files(filename,content)" shouldRespondWith
        [json|r##"[{"user_id":1,"files":[{"filename":"command.com","content":"#include <unix.h>"},{"filename":"autoexec.bat","content":"@ECHO OFF"},{"filename":"README.md","content":"# make $$$!"}]}]"##|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting children with composite key" $
      get "/users_tasks?user_id=eq.2&task_id=eq.6&select=*, comments(content)" shouldRespondWith
        [json|r#"[{"user_id":2,"task_id":6,"comments":[{"content":"Needs to be delivered ASAP"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "computed columns" $ do
      it "computed column on table" $
        get "/items?id=eq.1&select=id,always_true" shouldRespondWith
          [json|r#"[{"id":1,"always_true":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    //   it "computed column on rpc" $
    //     get "/rpc/search?id=1&select=id,always_true" shouldRespondWith
    //       [json|r#"[{"id":1,"always_true":true}]"#|]
    //       { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "overloaded computed columns on both tables" $ do
        get "/items?id=eq.1&select=id,computed_overload" shouldRespondWith
          [json|r#"[{"id":1,"computed_overload":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/items2?id=eq.1&select=id,computed_overload" shouldRespondWith
          [json|r#"[{"id":1,"computed_overload":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

//       it "overloaded computed column on rpc" $
//         get "/rpc/search?id=1&select=id,computed_overload" shouldRespondWith
//           [json|r#"[{"id":1,"computed_overload":true}]"#|]
//           { matchHeaders = ["Content-Type" <:> "application/json"] }

//     //when (actualPgVersion >= pgVersion110) $ do
      describe "partitioned tables embedding" $ do
        it "can request a table as parent from a partitioned table" $
          get "/partitioned_a?id=in.(1,2)&select=id,name,reference_from_partitioned(id)&order=id.asc" shouldRespondWith
            [json|r#"
              [{"id":1,"name":"first","reference_from_partitioned":{"id":1}},
               {"id":2,"name":"first","reference_from_partitioned":null}] "#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "can request partitioned tables as children from a table" $
          get "/reference_from_partitioned?select=id,partitioned_a(id,name)&order=id.asc" shouldRespondWith
            [json|r#"
              [{"id":1,"partitioned_a":[{"id":1,"name":"first"}]},
               {"id":2,"partitioned_a":[]}] "#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

//         //when (actualPgVersion >= pgVersion121) $ do
          it "can request tables as children from a partitioned table" $
            get "/partitioned_a?id=in.(1,2)&select=id,name,reference_to_partitioned(id)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"name":"first","reference_to_partitioned":[]},
                 {"id":2,"name":"first","reference_to_partitioned":[{"id":2}]}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request a partitioned table as parent from a table" $
            get "/reference_to_partitioned?select=id,partitioned_a(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"partitioned_a":null},
                 {"id":2,"partitioned_a":{"id":2,"name":"first"}}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request partitioned tables as children from a partitioned table" $
            get "/partitioned_a?id=in.(1,2,4)&select=id,name,partitioned_b(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"name":"first","partitioned_b":[]},
                 {"id":2,"name":"first","partitioned_b":[{"id":2,"name":"first_b"}]},
                 {"id":4,"name":"second","partitioned_b":[{"id":4,"name":"second_b"}]}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request a partitioned table as parent from a partitioned table" $ do
            get "/partitioned_b?id=in.(2,4)&select=id,name,partitioned_a(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":2,"name":"first_b","partitioned_a":{"id":2,"name":"first"}},
                 {"id":4,"name":"second_b","partitioned_a":{"id":4,"name":"second"}}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request partitions as children from a partitioned table" $
            get "/partitioned_a?id=in.(1,2,4)&select=id,name,first_partition_b(id)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"name":"first","first_partition_b":[]},
                 {"id":2,"name":"first","first_partition_b":[{"id":2}]},
                 {"id":4,"name":"second","first_partition_b":[]}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request a partitioned table as parent from a partition" $
            get "/first_partition_b?select=id,name,partitioned_a(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"name":"first_b","partitioned_a":null},
                 {"id":2,"name":"first_b","partitioned_a":{"id":2,"name":"first"}}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request a partition as parent from a partitioned table" $
            get "/partitioned_b?id=in.(1,3,4)&select=id,name,second_partition_a(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":1,"name":"first_b","second_partition_a":null},
                 {"id":3,"name":"second_b","second_partition_a":null},
                 {"id":4,"name":"second_b","second_partition_a":{"id":4,"name":"second"}}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can request partitioned tables as children from a partition" $
            get "/second_partition_a?select=id,name,partitioned_b(id,name)&order=id.asc" shouldRespondWith
              [json|r#"
                [{"id":3,"name":"second","partitioned_b":[]},
                 {"id":4,"name":"second","partitioned_b":[{"id":4,"name":"second_b"}]}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "view embedding" $ do
      it "can detect fk relations through views to tables in the public schema" $
        get "/consumers_view?select=*,orders_view(*)" shouldRespondWith 200

      it "can detect fk relations through materialized views to tables in the public schema" $
        get "/materialized_projects?select=*,users(*)" shouldRespondWith 200

      it "can request two parents" $
        get "/articleStars?select=createdAt,article:articles(id),user:users(name)&limit=1"
          shouldRespondWith
            [json|r#"[{"createdAt":"2015-12-08T04:22:57.472738","article":{"id": 1},"user":{"name": "Angela Martin"}}]"#|]

      it "can detect relations in views from exposed schema that are based on tables in private schema and have columns renames" $
        get "/articles?id=eq.1&select=id,articleStars(users(*))" shouldRespondWith
          [json|r#"[{"id":1,"articleStars":[{"users":{"id":1,"name":"Angela Martin"}},{"users":{"id":2,"name":"Michael Scott"}},{"users":{"id":3,"name":"Dwight Schrute"}}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when requesting parents and children on views" $
        get "/projects_view?id=eq.1&select=id, name, clients(*), tasks(id, name)" shouldRespondWith
          [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when requesting parents and children on views with renamed keys" $
        get "/projects_view_alt?t_id=eq.1&select=t_id, name, clients(*), tasks(id, name)" shouldRespondWith
          [json|r#"[{"t_id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "detects parent relations when having many views of a private table" $ do
        get "/books?select=title,author:authors(name)&id=eq.5" shouldRespondWith
          [json|r#"[ { "title": "Farenheit 451", "author": { "name": "Ray Bradbury" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/forties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "1984", "author": { "name": "George Orwell" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/fifties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "The Catcher in the Rye", "author": { "name": "J.D. Salinger" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/sixties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "To Kill a Mockingbird", "author": { "name": "Harper Lee" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can detect fk relations through multiple views recursively when all views are in api schema" $ do
        get "/consumers_view_view?select=*,orders_view(*)" shouldRespondWith 200

      it "works with views that have subselects" $
        get "/authors_books_number?select=*,books(title)&id=eq.1" shouldRespondWith
          [json|r#"[ {"id":1, "name":"George Orwell","num_in_forties":1,"num_in_fifties":0,"num_in_sixties":0,"num_in_all_decades":1,
                   "books":[{"title":"1984"}]} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have case subselects" $
        get "/authors_have_book_in_decade?select=*,books(title)&id=eq.3" shouldRespondWith
          [json|r#"[ {"id":3,"name":"Antoine de Saint-Exupéry","has_book_in_forties":true,"has_book_in_fifties":false,"has_book_in_sixties":false,
                   "books":[{"title":"The Little Prince"}]} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have subselect in the FROM clause" $
        get "/forties_and_fifties_books?select=title,first_publisher,author:authors(name)&id=eq.1" shouldRespondWith
          [json|r#"[{"title":"1984","first_publisher":"Secker & Warburg","author":{"name":"George Orwell"}}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have subselects in a function call" $
        get "/authors_have_book_in_decade2?select=*,books(title)&id=eq.3"
          shouldRespondWith
            [json|r#"[ {"id":3,"name":"Antoine de Saint-Exupéry","has_book_in_forties":true,"has_book_in_fifties":false,
                     "has_book_in_sixties":false,"books":[{"title":"The Little Prince"}]} ]"#|]

      it "works with views that have CTE" $
        get "/odd_years_publications?select=title,publication_year,first_publisher,author:authors(name)&id=in.(1,2,3)" shouldRespondWith
          [json|r#"[
            {"title":"1984","publication_year":1949,"first_publisher":"Secker & Warburg","author":{"name":"George Orwell"}},
            {"title":"The Diary of a Young Girl","publication_year":1947,"first_publisher":"Contact Publishing","author":{"name":"Anne Frank"}},
            {"title":"The Little Prince","publication_year":1947,"first_publisher":"Reynal & Hitchcock","author":{"name":"Antoine de Saint-Exupéry"}} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when having a capitalized table name and camelCase fk column" $
        get "/foos?select=*,bars(*)" shouldRespondWith 200

      it "works when embedding a view with a table that has a long compound pk" $ do
        get "/player_view?select=id,contract(purchase_price)&id=in.(1,3,5,7)" shouldRespondWith
          [json|r#"
            [{"id":1,"contract":[{"purchase_price":10}]},
             {"id":3,"contract":[{"purchase_price":30}]},
             {"id":5,"contract":[{"purchase_price":50}]},
             {"id":7,"contract":[]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/contract?select=tournament,player_view(first_name)&limit=3" shouldRespondWith
          [json|r#"
            [{"tournament":"tournament_1","player_view":{"first_name":"first_name_1"}},
             {"tournament":"tournament_2","player_view":{"first_name":"first_name_2"}},
             {"tournament":"tournament_3","player_view":{"first_name":"first_name_3"}}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when embedding a view with a view that referes to a table that has a long compound pk" $ do
        get "/player_view?select=id,contract_view(purchase_price)&id=in.(1,3,5,7)" shouldRespondWith
          [json|r#"
            [{"id":1,"contract_view":[{"purchase_price":10}]},
             {"id":3,"contract_view":[{"purchase_price":30}]},
             {"id":5,"contract_view":[{"purchase_price":50}]},
             {"id":7,"contract_view":[]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/contract_view?select=tournament,player_view(first_name)&limit=3" shouldRespondWith
          [json|r#"
            [{"tournament":"tournament_1","player_view":{"first_name":"first_name_1"}},
             {"tournament":"tournament_2","player_view":{"first_name":"first_name_2"}},
             {"tournament":"tournament_3","player_view":{"first_name":"first_name_3"}}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can embed a view that has group by" $
        get "/projects_count_grouped_by?select=number_of_projects,client:clients(name)&order=number_of_projects" shouldRespondWith
          [json|r#"
            [{"number_of_projects":1,"client":null},
             {"number_of_projects":2,"client":{"name":"Microsoft"}},
             {"number_of_projects":2,"client":{"name":"Apple"}}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can embed a view that has a subselect containing a select in a where" $
        get "/authors_w_entities?select=name,entities,books(title)&id=eq.1" shouldRespondWith
          [json|r#" [{"name":"George Orwell","entities":[3, 4],"books":[{"title":"1984"}]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "aliased embeds" $ do
      it "works with child relation" $
        get "/space?select=id,zones:zone(id,name),stores:zone(id,name)&zones.zone_type_id=eq.2&stores.zone_type_id=eq.3" shouldRespondWith
          [json|r#"[
            { "id":1,
              "zones": [ {"id":1,"name":"zone 1"}, {"id":2,"name":"zone 2"}],
              "stores": [ {"id":3,"name":"store 3"}, {"id":4,"name":"store 4"}]}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with many to many relation" $
        get "/users?select=id,designTasks:tasks(id,name),codeTasks:tasks(id,name)&designTasks.name=like.*Design*&codeTasks.name=like.*Code*" shouldRespondWith
          [json|r#"[
             { "id":1,
               "designTasks":[ { "id":1, "name":"Design w7" }, { "id":3, "name":"Design w10" } ],
               "codeTasks":[ { "id":2, "name":"Code w7" }, { "id":4, "name":"Code w10" } ] },
             { "id":2,
               "designTasks":[ { "id":5, "name":"Design IOS" }, { "id":7, "name":"Design OSX" } ],
               "codeTasks":[ { "id":6, "name":"Code IOS" } ] },
             { "id":3,
               "designTasks":[ { "id":1, "name":"Design w7" }, { "id":5, "name":"Design IOS" } ],
               "codeTasks":[ ] }
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with an aliased child plus non aliased child" $
        get "/projects?select=id,name,designTasks:tasks(name,users(id,name))&designTasks.name=like.*Design*&designTasks.users.id=in.(1,2)" shouldRespondWith
          [json|r#"[
            {
              "id":1, "name":"Windows 7",
              "designTasks":[ { "name":"Design w7", "users":[ { "id":1, "name":"Angela Martin" } ] } ] },
            {
              "id":2, "name":"Windows 10",
              "designTasks":[ { "name":"Design w10", "users":[ { "id":1, "name":"Angela Martin" } ] } ] },
            {
              "id":3, "name":"IOS",
              "designTasks":[ { "name":"Design IOS", "users":[ { "id":2, "name":"Michael Scott" } ] } ] },
            {
              "id":4, "name":"OSX",
              "designTasks":[ { "name":"Design OSX", "users":[ { "id":2, "name":"Michael Scott" } ] } ] },
            {
              "id":5, "name":"Orphan",
              "designTasks":[ ] }
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with two aliased children embeds plus and/or" $
        get "/entities?select=id,children:child_entities(id,gChildren:grandchild_entities(id))&children.and=(id.in.(1,2,3))&children.gChildren.or=(id.eq.1,id.eq.2)" shouldRespondWith
          [json|r#"[
            { "id":1,
              "children":[
                {"id":1,"gChildren":[{"id":1}, {"id":2}]},
                {"id":2,"gChildren":[]}]},
            { "id":2,
              "children":[
                {"id":3,"gChildren":[]}]},
            { "id":3,"children":[]},
            { "id":4,"children":[]}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "ordering response" $ do
    it "by a column asc" $
      get "/items?id=lte.2&order=id.asc"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }


    it "by a column desc" $
      get "/items?id=lte.2&order=id.desc"
        shouldRespondWith [json|r#" [{"id":2},{"id":1}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }

    it "by a column with nulls first" $
      get "/no_pk?order=a.nullsfirst"
        shouldRespondWith [json|r#" [{"a":null,"b":null},
                              {"a":"1","b":"0"},
                              {"a":"2","b":"0"}
                              ] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column asc with nulls last" $
      get "/no_pk?order=a.asc.nullslast"
        shouldRespondWith [json|r#" [{"a":"1","b":"0"},
                              {"a":"2","b":"0"},
                              {"a":null,"b":null}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column desc with nulls first" $
      get "/no_pk?order=a.desc.nullsfirst"
        shouldRespondWith [json|r#" [{"a":null,"b":null},
                              {"a":"2","b":"0"},
                              {"a":"1","b":"0"}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column desc with nulls last" $
      get "/no_pk?order=a.desc.nullslast"
        shouldRespondWith [json|r#" [{"a":"2","b":"0"},
                              {"a":"1","b":"0"},
                              {"a":null,"b":null}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by two columns with nulls and direction specified" $
      get "/projects?select=client_id,id,name&order=client_id.desc.nullslast,id.desc"
        shouldRespondWith [json|r#"
          [{"client_id":2,"id":4,"name":"OSX"},
           {"client_id":2,"id":3,"name":"IOS"},
           {"client_id":1,"id":2,"name":"Windows 10"},
           {"client_id":1,"id":1,"name":"Windows 7"},
           {"client_id":null,"id":5,"name":"Orphan"}]
        "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-4/*"]
        }

    it "by a column with no direction or nulls specified" $
      get "/items?id=lte.2&order=id"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }

    it "without other constraints" $
      get "/items?order=id.asc" shouldRespondWith 200

    it "ordering embeded entities" $
      get "/projects?id=eq.1&select=id, name, tasks(id, name)&tasks.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","tasks":[{"id":2,"name":"Code w7"},{"id":1,"name":"Design w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded entities with alias" $
      get "/projects?id=eq.1&select=id, name, the_tasks:tasks(id, name)&tasks.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","the_tasks":[{"id":2,"name":"Code w7"},{"id":1,"name":"Design w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded entities, two levels" $
      get "/projects?id=eq.1&select=id, name, tasks(id, name, users(id, name))&tasks.order=name.asc&tasks.users.order=name.desc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","tasks":[{"id":2,"name":"Code w7","users":[{"id":1,"name":"Angela Martin"}]},{"id":1,"name":"Design w7","users":[{"id":3,"name":"Dwight Schrute"},{"id":1,"name":"Angela Martin"}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded parents does not break things" $
      get "/projects?id=eq.1&select=id, name, clients(id, name)&clients.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"}}]"#|]

    describe "order syntax errors" $ do
      it "gives meaningful error messages when asc/desc/nulls{first,last} are misspelled" $ do
        get "/items?order=id.ac" shouldRespondWith
          [json|r#"{"details":"Unexpected `a` Expected `nullsfirst` or `nullslast`","message":"\"failed to parse order (id.ac)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.descc" shouldRespondWith
          [json|r#"{"details":"Unexpected `c` Expected `,`, `whitespaces` or `end of input`","message":"\"failed to parse order (id.descc)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.nulsfist" shouldRespondWith
          [json|r#"{"details":"Unexpected `s`","message":"\"failed to parse order (id.nulsfist)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.nullslasttt" shouldRespondWith
          [json|r#"{"details":"Unexpected `t` Expected `,`, `whitespaces` or `end of input`","message":"\"failed to parse order (id.nullslasttt)\" (line 1, column 13)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.smth34" shouldRespondWith
          [json|r#"{"details":"Unexpected `s` Expected `nullsfirst` or `nullslast`","message":"\"failed to parse order (id.smth34)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "gives meaningful error messages when nulls{first,last} are misspelled after asc/desc" $ do
        get "/items?order=id.asc.nlsfst" shouldRespondWith
          [json|r#"{"details":"Unexpected `l`","message":"\"failed to parse order (id.asc.nlsfst)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.asc.nullslasttt" shouldRespondWith
          [json|r#"{"details":"Unexpected `t` Expected `,`, `whitespaces` or `end of input`","message":"\"failed to parse order (id.asc.nullslasttt)\" (line 1, column 17)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.asc.smth34" shouldRespondWith
          [json|r#"{"details":"Unexpected `s` Expected `nullsfirst` or `nullslast`","message":"\"failed to parse order (id.asc.smth34)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

//   describe "Accept headers" $ do
//     it "should respond an unknown accept type with 415" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype") ""
//         shouldRespondWith
//         [json|r#"{"message":"None of these Content-Types are available: text/unknowntype"}"#|]
//         { matchStatus  = 415
//         , matchHeaders = ["Content-Type" <:> "application/json"]
//         }

//     it "should respond correctly to */* in accept header" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "*/*") ""
//         shouldRespondWith 200

//     it "*/* should rescue an unknown type" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype, */*") ""
//         shouldRespondWith 200

//     // it "specific available preference should override */*" $ do
//     //   r <- request methodGet "/simple_pk"
//     //           (acceptHdrs "text/csv, */*") ""
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Type" "text/csv; charset=utf-8"

//     // it "honors client preference even when opposite of server preference" $ do
//     //   r <- request methodGet "/simple_pk"
//     //           (acceptHdrs "text/csv, application/json") ""
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Type" "text/csv; charset=utf-8"

//     it "should respond correctly to multiple types in accept header" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype, text/csv") ""
//         shouldRespondWith 200

//     it "should respond with CSV to 'text/csv' request" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/csv; version=1") ""
//         shouldRespondWith [text|"k,extra\nxyyx,u\nxYYx,v"|]
//         { matchStatus  = 200
//         , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
//         }

//   describe "Canonical location" $ do
//     it "Sets Content-Location with alphabetized params" $
//       get "/no_pk?b=eq.1&a=eq.1"
//         shouldRespondWith [json|"[]"|]
//         { matchStatus  = 200
//         , matchHeaders = ["Content-Location" <:> "/no_pk?a=eq.1&b=eq.1"]
//         }

//     // it "Omits question mark when there are no params" $ do
//     //   r <- get "/simple_pk"
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Location" "/simple_pk"

  describe "weird requests" $ do
    it "can query as normal" $ do
      get "/Escap3e;" shouldRespondWith
        [json|r#" [{"so6meIdColumn":1},{"so6meIdColumn":2},{"so6meIdColumn":3},{"so6meIdColumn":4},{"so6meIdColumn":5}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/ghostBusters" shouldRespondWith
        [json|r#" [{"escapeId":1},{"escapeId":3},{"escapeId":5}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails if an operator is not given" $
      get "/ghostBusters?id=0" shouldRespondWith [json|r#" {"details":"Unexpected `0` Expected `letter`, `in`, `not` or `.`","message":"\"failed to parse filter (0)\" (line 1, column 1)"} "#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "will embed a collection" $
      get "/Escap3e;?select=ghostBusters(*)" shouldRespondWith
        [json|r#" [{"ghostBusters":[{"escapeId":1}]},{"ghostBusters":[]},{"ghostBusters":[{"escapeId":3}]},{"ghostBusters":[]},{"ghostBusters":[{"escapeId":5}]}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "will select and filter a column that has spaces" $
      get "/Server%20Today?select=Just%20A%20Server%20Model&Just%20A%20Server%20Model=like.*91*" shouldRespondWith
        [json|r#"[
          {"Just A Server Model":" IBM,9113-550 (P5-550)"},
          {"Just A Server Model":" IBM,9113-550 (P5-550)"},
          {"Just A Server Model":" IBM,9131-52A (P5-52A)"},
          {"Just A Server Model":" IBM,9133-55A (P5-55A)"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "will select and filter a quoted column that has PostgREST reserved characters" $
      get "/pgrst_reserved_chars?select=%22:arr-%3Eow::cast%22,%22(inside,parens)%22,%22a.dotted.column%22,%22%20%20col%20%20w%20%20space%20%20%22&%22*id*%22=eq.1" shouldRespondWith
        [json|r#"[{":arr->ow::cast":" arrow-1 ","(inside,parens)":" parens-1 ","a.dotted.column":" dotted-1 ","  col  w  space  ":" space-1"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

//   describe "binary output" $ do
//     it "can query if a single column is selected" $
//       request methodGet "/images_base64?select=img&name=eq.A.png" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith [text|"iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCC"|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
//         }

//     it "can get raw output with Accept: text/plain" $
//       request methodGet "/projects?select=name&id=eq.1" (acceptHdrs "text/plain") ""
//         shouldRespondWith [text|"Windows 7"|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "text/plain; charset=utf-8"]
//         }

//     it "fails if a single column is not selected" $ do
//       request methodGet "/images?select=img,name&name=eq.A.png" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//       request methodGet "/images?select=*&name=eq.A.png"
//           (acceptHdrs "application/octet-stream")
//           ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//       request methodGet "/images?name=eq.A.png"
//           (acceptHdrs "application/octet-stream")
//           ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//     it "concatenates results if more than one row is returned" $
//       request methodGet "/images_base64?select=img&name=in.(A.png,B.png)" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith [text|"iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCCiVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEX///8AAP94wDzzAAAAL0lEQVQIW2NgwAb+HwARH0DEDyDxwAZEyGAhLODqHmBRzAcn5GAS///A1IF14AAA5/Adbiiz/0gAAAAASUVORK5CYII="|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
//         }

  // describe "values with quotes in IN and NOT IN" $ do
  //   it "succeeds when only quoted values are present" $ do
  //     get "/w_or_wo_comma_names?name=in.(\"Hebdon, John\")" shouldRespondWith
  //       [json|r#" [{"name":"Hebdon, John"}] "#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }
  //     get "/w_or_wo_comma_names?name=in.(\"Hebdon, John\",\"Williams, Mary\",\"Smith, Joseph\")" shouldRespondWith
  //       [json|r#" [{"name":"Hebdon, John"},{"name":"Williams, Mary"},{"name":"Smith, Joseph"}] "#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }
  //     get "/w_or_wo_comma_names?name=not.in.(\"Hebdon, John\",\"Williams, Mary\",\"Smith, Joseph\")&limit=3" shouldRespondWith
  //       [json|r#" [ { "name": "David White" }, { "name": "Larry Thompson" }, { "name": "Double O Seven(007)" }] "#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

//     it "succeeds w/ and w/o quoted values" $ do
//       get "/w_or_wo_comma_names?name=in.(David White,\"Hebdon, John\")" shouldRespondWith
//         [json|r#" [{"name":"Hebdon, John"},{"name":"David White"}] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }
//       get "/w_or_wo_comma_names?name=not.in.(\"Hebdon, John\",Larry Thompson,\"Smith, Joseph\")&limit=3" shouldRespondWith
//         [json|r#" [ { "name": "Williams, Mary" }, { "name": "David White" }, { "name": "Double O Seven(007)" }] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }
//       get "/w_or_wo_comma_names?name=in.(\"Double O Seven(007)\")" shouldRespondWith
//         [json|r#" [{"name":"Double O Seven(007)"}] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }

//     describe "escaped chars" $ do
//       it "accepts escaped double quotes" $
//         get "/w_or_wo_comma_names?name=in.(\"Double\\\"Quote\\\"McGraw\\\"\")" shouldRespondWith
//           [json|r#" [ { "name": "Double\"Quote\"McGraw\"" } ] "#|]
//           { matchHeaders = ["Content-Type" <:> "application/json"] }

//       it "accepts escaped backslashes" $ do
//         get "/w_or_wo_comma_names?name=in.(\"\\\\\")" shouldRespondWith
//           [json|r#" [{ "name": "\\" }] "#|]
//           { matchHeaders = ["Content-Type" <:> "application/json"] }
//         get "/w_or_wo_comma_names?name=in.(\"/\\\\Slash/\\\\Beast/\\\\\")" shouldRespondWith
//           [json|r#" [ { "name": "/\\Slash/\\Beast/\\" } ] "#|]
//           { matchHeaders = ["Content-Type" <:> "application/json"] }

//       it "passes any escaped char as the same char" $
//         get "/w_or_wo_comma_names?name=in.(\"D\\a\\vid W\\h\\ite\")" shouldRespondWith
//           [json|r#" [{ "name": "David White" }] "#|]
//           { matchHeaders = ["Content-Type" <:> "application/json"] }

//   describe "IN values without quotes" $ do
//     it "accepts single double quotes as values" $ do
//       get "/w_or_wo_comma_names?name=in.(\")" shouldRespondWith
//         [json|r#" [{ "name": "\"" }] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }
//       get "/w_or_wo_comma_names?name=in.(Double\"Quote\"McGraw\")" shouldRespondWith
//         [json|r#" [ { "name": "Double\"Quote\"McGraw\"" } ] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }

//     it "accepts backslashes as values" $ do
//       get "/w_or_wo_comma_names?name=in.(\\)" shouldRespondWith
//         [json|r#" [{ "name": "\\" }] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }
//       get "/w_or_wo_comma_names?name=in.(/\\Slash/\\Beast/\\)" shouldRespondWith
//         [json|r#" [ { "name": "/\\Slash/\\Beast/\\" } ] "#|]
//         { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "IN and NOT IN empty set" $ do
    describe "returns an empty result for IN when no value is present" $ do
      it "works for integer" $
        get "/items_with_different_col_types?int_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for text" $
        get "/items_with_different_col_types?text_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for bool" $
        get "/items_with_different_col_types?bool_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for bytea" $
        get "/items_with_different_col_types?bin_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for char" $
        get "/items_with_different_col_types?char_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for date" $
        get "/items_with_different_col_types?date_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for real" $
        get "/items_with_different_col_types?real_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for time" $
        get "/items_with_different_col_types?time_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "returns all results for not in when no value is present" $
      get "/items_with_different_col_types?int_data=not.in.()&select=int_data" shouldRespondWith
        [json|r#" [{"int_data": 1}] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "returns an empty result ignoring spaces" $
      get "/items_with_different_col_types?int_data=in.(    )" shouldRespondWith
        [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    // it "only returns an empty result set if the in value is empty" $
    //   get "/items_with_different_col_types?int_data=in.( ,3,4)"
    //     shouldRespondWith (
    //     if actualPgVersion >= pgVersion121 then
    //     [json|r#" {"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for type integer: \"\""} "#|]
    //     else
    //     [json|r#" {"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for integer: \"\""} "#|]
    //                         )
    //     { matchStatus = 400
    //     , matchHeaders = ["Content-Type" <:> "application/json"]
    //     }

  describe "Embedding when column name = table name" $ do
    it "works with child embeds" $
      get "/being?select=*,descendant(*)&limit=1" shouldRespondWith
        [json|r#"[{"being":1,"descendant":[{"descendant":1,"being":1},{"descendant":2,"being":1},{"descendant":3,"being":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "works with many to many embeds" $
      get "/being?select=*,part(*)&limit=1" shouldRespondWith
        [json|r#"[{"being":1,"part":[{"part":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "Foreign table" $ do
    it "can be queried by using regular filters" $
      get "/projects_dump?id=in.(1,2,3)" shouldRespondWith
        [json|r#" [{"id":1,"name":"Windows 7","client_id":1}, {"id":2,"name":"Windows 10","client_id":1}, {"id":3,"name":"IOS","client_id":2}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can be queried with select, order and limit" $
      get "/projects_dump?select=id,name&order=id.desc&limit=3" shouldRespondWith
        [json|r#" [{"id":5,"name":"Orphan"}, {"id":4,"name":"OSX"}, {"id":3,"name":"IOS"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  it "cannot use ltree(in public schema) extension operators if no extra search path added" $
    get "/ltree_sample?path=cd.Top.Science.Astronomy" shouldRespondWith 400

  describe "VIEW that has a source FK based on a UNIQUE key" $
    it "can be embedded" $
      get "/referrals?select=site,link:pages(url)" shouldRespondWith
        [json|r#" [
         {"site":"github.com",     "link":{"url":"http://postgrest.org/en/v6.0/api.html"}},
         {"site":"hub.docker.com", "link":{"url":"http://postgrest.org/en/v6.0/admin.html"}}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

//   it "shouldn't produce a Content-Profile header since only a single schema is exposed" $ do
//     r <- get "/items"
//     liftIO $ do
//       let respHeaders = simpleHeaders r
//       respHeaders `shouldSatisfy` noProfileHeader
}

// demonstrate! {
//     #[rocket::async_test]
//     async describe "postgrest" {
//         use super::*;
//         before {
//             setup();
//             //let client = Client::tracked(server().await).await.expect("valid client");
//         }

//         it "rows with missing parents are included" { haskell_get! {
//             get "/rest/projects?id=in.(1,5)&select=id,clients(id)" shouldRespondWith
//                 [json|r#"r#"[{"id":1,"clients":{"id":1}},{"id":5,"clients":null}]"#"#|]
//                 { matchStatus = 200
//                 , matchHeaders = ["Content-Type" <:> "application/json", "Content-Type" <:> "application/json"]
//                 }
//         }}

        

//         // it "hello world" {
//         //     let response = client.get("/").dispatch().await;
//         //     self::assert_eq!(response.status(), Status::Ok);
//         //     self::assert_eq!(response.into_string().await.unwrap(), "Hello, world!");
//         // }
    
//         // it "simple get" {
//         //     let response = client.get("/rest/projects?select=id,name&id=gt.1&name=eq.IOS").dispatch().await;
//         //     self::assert_eq!(response.status(), Status::Ok);
//         //     self::assert_eq!(response.into_string().await.unwrap(), r#"[{"id":3,"name":"IOS"}]"#);
//         // }

        
    
//         // it "simple get two" {
//         //     let response = client.get("/rest/projects?id=in.(1,5)&select=id,clients(id)").dispatch().await;
//         //     self::assert_eq!(response.status(), Status::Ok);
//         //     self::assert_eq!(
//         //         json![response.into_string().await.unwrap().as_str()],
//         //         json![r#"[{"id":1,"clients":{"id":1}},{"id":5,"clients":null}]"#]
//         //     );
//         // }
//     }
// }