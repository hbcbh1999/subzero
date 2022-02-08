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
// use std::env;
pub static INIT_CLIENT: Once = Once::new();

lazy_static! {
  pub static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
    Client::untracked(start().await.unwrap()).await.expect("valid client")
    
  });
}


haskell_test! {
feature "delete"
  describe "Deleting" $ do
    describe "existing record" $ do
      it "succeeds with 204 and deletion count" $
        request methodDelete "/items?id=eq.1"
            // []
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [ 
                             // matchHeaderAbsent hContentType,
                             "Content-Range" <:> "*/*" ]
            }

      it "returns the deleted item and count if requested" $
        request methodDelete "/items?id=eq.2" [("Prefer", "return=representation, count=exact")] ""
          shouldRespondWith [json|r#"[{"id":2}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/1"]
          }

      it "ignores ?select= when return not set or return=minimal" $ do
        request methodDelete "/items?id=eq.3&select=id"
            //[]
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [ 
                             // matchHeaderAbsent hContentType,
                             "Content-Range" <:> "*/*" ]
            }
        request methodDelete "/items?id=eq.3&select=id"
            [("Prefer", "return=minimal")]
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [ 
                             // matchHeaderAbsent hContentType,
                             "Content-Range" <:> "*/*" ]
            }

      it "returns the deleted item and shapes the response" $
        request methodDelete "/complex_items?id=eq.2&select=id,name" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"id":2,"name":"Two"}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

      it "can rename and cast the selected columns" $
        request methodDelete "/complex_items?id=eq.3&select=ciId:id::text,ciName:name" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"ciId":"3","ciName":"Three"}]"#|]

      it "can embed (parent) entities" $
        request methodDelete "/tasks?id=eq.8&select=id,name,project:projects(id)" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"id":8,"name":"Code OSX","project":{"id":4}}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

    describe "known route, no records matched" $
      it "includes [] body if return=rep" $
        request methodDelete "/items?id=eq.101"
          [("Prefer", "return=representation")] ""
          shouldRespondWith [text|"[]"|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

    describe "totally unknown route" $
      it "fails with 404" $
        request methodDelete "/foozle?id=eq.101" 
          //[]
          ""
          shouldRespondWith 404

    describe "table with limited privileges" $ do
      it "fails deleting the row when return=representation and selecting all the columns" $
        request methodDelete "/app_users?id=eq.1" [("Prefer", "return=representation")]
          //mempty
          ""
          shouldRespondWith 401

      it "succeeds deleting the row when return=representation and selecting only the privileged columns" $
        request methodDelete "/app_users?id=eq.1&select=id,email" [("Prefer", "return=representation")]
          r#"{ "password": "passxyz" }"#
            shouldRespondWith [json|r#"[ { "id": 1, "email": "test@123.com" } ]"#|]
            { matchStatus  = 200
            , matchHeaders = ["Content-Range" <:> "*/*"]
            }

      it "suceeds deleting the row with no explicit select when using return=minimal" $
        request methodDelete "/app_users?id=eq.2"
            [("Prefer", "return=minimal")]
            //mempty
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus = 204
            , matchHeaders = [
              //matchHeaderAbsent hContentType
              ]
            }

      it "suceeds deleting the row with no explicit select by default" $
        request methodDelete "/app_users?id=eq.3"
            //[]
            //mempty
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus = 204
            , matchHeaders = [
              //matchHeaderAbsent hContentType
              ]
    }
}
