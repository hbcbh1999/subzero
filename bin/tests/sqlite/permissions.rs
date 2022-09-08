use super::super::start;
use super::common::*;

use async_once::AsyncOnce;
use demonstrate::demonstrate;
use pretty_assertions::assert_eq;
use rocket::http::Accept;
use rocket::local::asynchronous::Client;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Once;
use rocket::http::{Header};
static INIT_CLIENT: Once = Once::new();
lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "permissions"
  describe "grants" $ do
    it "admin can select all columns" $ do
      let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU"
      request methodGet "/permissions_check?select=id,value,hidden" [auth] ""
        shouldRespondWith
        [json|r#"[
          {"id":1,"value":"One","hidden":"Hidden"},
          {"id":2,"value":"Two","hidden":"Hidden"},
          {"id":3,"value":"Three","hidden":"Hidden"},
          {"id":10,"value":"Ten","hidden":"Hidden"},
          {"id":20,"value":"Twenty","hidden":"Hidden"}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "anonymous can select not select hidden column" $ do
      get "/permissions_check?select=id,value,hidden"
        shouldRespondWith
        [json|r#"{"details":"no Select privileges for 'public.permissions_check(hidden)'","message":"Permission denied"}"#|]
        { matchStatus = 400}
    it "anonymous can select permitted columns with rows filtered" $ do
      get "/permissions_check?select=id,value"
        shouldRespondWith
        [json|r#"[
          {"id":2,"value":"Two"},
          {"id":3,"value":"Three"},
          {"id":10,"value":"Ten"},
          {"id":20,"value":"Twenty"}
        ]"#|]
        { matchStatus = 200 }
  
}
