use super::common::*;
use super::super::start;
use pretty_assertions::{assert_eq};
use serde_json::Value;
use rocket::http::{Accept,};
use std::str::FromStr;
use demonstrate::demonstrate;
use rocket::local::asynchronous::Client;
use async_once::AsyncOnce;
use std::sync::Once;
use std::env;
pub static INIT_CLIENT: Once = Once::new();

lazy_static! {
  pub static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
    env::set_var("SUBZERO_VHOSTS__DEFAULT__DB_SCHEMAS", "[تست]");
    Client::untracked(start().await.unwrap()).await.expect("valid client")
    
  });
}


haskell_test! {
feature "unicode"
describe "Reading and writing to unicode schema and table names" $
    it "Can read and write values" $ do
      get "/%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
        shouldRespondWith [text|"[]"|]

      request methodPost "/%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
          [("Prefer", "return=representation")]
          [json| r#"{ "هویت": 1 }"# |]
        shouldRespondWith
          [json| r#"[{ "هویت": 1 }]"# |]
          { matchStatus = 201 }

      // get "/%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
      //   shouldRespondWith
      //     [json| [{ "هویت": 1 }] |]

      request methodDelete "/%D9%85%D9%88%D8%A7%D8%B1%D8%AF"
          //[("Prefer", "tx=commit")]
          ""
        shouldRespondWith
          [text|""|]
          { matchStatus = 204
          , matchHeaders = [
            //matchHeaderAbsent hContentType
          ]
          }
}
