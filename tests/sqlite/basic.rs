use super::super::start;
use super::common::*;
use async_once::AsyncOnce;
use demonstrate::demonstrate;
use pretty_assertions::assert_eq;
use rocket::http::{Accept,};
use rocket::local::asynchronous::Client;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Once;
pub static INIT_CLIENT: Once = Once::new();

lazy_static! {
    pub static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async {
        Client::untracked(start().await.unwrap())
            .await
            .expect("valid client")
    });
}

haskell_test! {
feature "basic"
  describe "all" $
    it "simple select" $
      get "/tbl1?select=one,two" shouldRespondWith
        [json| r#"
            [
                {"one":"hello!","two":10},
                {"one":"goodbye","two":20}
            ]
        "#|]
      { matchStatus = 200
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }
    it "simple select with cast" $
      get "/tbl1?select=one,two::text" shouldRespondWith
        [json| r#"
            [
                {"one":"hello!","two":"10"},
                {"one":"goodbye","two":"20"}
            ]
        "#|]
      { matchStatus = 200
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }

    describe "embeding" $
      it "children" $
        get "/projects?select=id,name,tasks(id,name)&id=in.(1,2)" shouldRespondWith
          [json| r#"
          [
            {"id":1,"name":"Windows 7","tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]},
            {"id":2,"name":"Windows 10","tasks":[{"id":3,"name":"Design w10"},{"id":4,"name":"Code w10"}]}
          ]
          "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }
      it "parent" $
        get "/projects?select=id,name,client:clients(id,name)&id=in.(1,2,3)" shouldRespondWith
          [json| r#"
          [
            {"id":1,"name":"Windows 7","client":{"id":1,"name":"Microsoft"}},
            {"id":2,"name":"Windows 10","client":{"id":1,"name":"Microsoft"}},
            {"id":3,"name":"IOS","client":{"id":2,"name":"Apple"}}
          ]
          "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }
}