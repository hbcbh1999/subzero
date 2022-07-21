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
static INIT_CLIENT: Once = Once::new();

lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "custom_relations"

  describe "Custom Relations" $ do
    it "requesting parent with custom relations" $
        get "/no_fk_projects?select=name,client:clients(name)" shouldRespondWith
          [json|r#"[
            {"name":"Windows 7","client":{"name": "Microsoft"}},
            {"name":"Windows 10","client":{"name": "Microsoft"}},
            {"name":"IOS","client":{"name": "Apple"}},
            {"name":"OSX","client":{"name": "Apple"}},
            {"name":"Orphan","client":null}
          ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "requesting children with custom relations" $
        get "/clients?id=eq.1&select=id,projects:no_fk_projects(id)" shouldRespondWith
          [json|r#"[{"id":1,"projects":[{"id":1},{"id":2}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }




  }
