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
pub static INIT_DB: Once = Once::new();

lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "update"
describe "Patching record" $ do
    describe "to unknown uri" $
      it "indicates no table found by returning 404" $
        request methodPatch "/fake" []
          [json| r#"{ "real": false }"# |]
            shouldRespondWith 404

    describe "on an empty table" $
      it "indicates no records found to update by returning 404" $
        request methodPatch "/empty_table" []
            [json| r#"{ "extra":20 }"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 404,
              matchHeaders = [
                // matchHeaderAbsent hContentType
              ]
            }

    describe "with invalid json payload" $
      it "fails with 400 and error" $
        request methodPatch "/simple_pk" [] "}{ x = 2"
          shouldRespondWith
          [json|r#"{"message":"Failed to parse json body: expected value at line 1 column 1"}"#|]
          { matchStatus  = 400,
            matchHeaders = ["Content-Type" <:> "application/json"]
          }

    describe "with no payload" $
      it "fails with 400 and error" $
        request methodPatch "/items" [] ""
          shouldRespondWith
          [json|r#"{"message":"Failed to parse json body: EOF while parsing a value at line 1 column 0"}"#|]
          { matchStatus  = 400,
            matchHeaders = ["Content-Type" <:> "application/json"]
          }

    describe "in a nonempty table" $ do
      it "can update a single item" $ do
        request methodPatch "/items?id=eq.2"
            [json| r#"{ "id":42 }"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [
                              // matchHeaderAbsent hContentType ,
                             "Content-Range" <:> "0-0/*" ]
            }

      it "returns empty array when no rows updated and return=rep" $
        request methodPatch "/items?id=eq.999999"
          [("Prefer", "return=representation")] [json| r#"{ "id":999999 }"# |]
          shouldRespondWith [text|"[]"|]
          {
            matchStatus  = 404,
            matchHeaders = []
          }

      it "gives a 404 when no rows updated" $
        request methodPatch "/items?id=eq.99999999" []
          [json| r#"{ "id": 42 }"# |]
            shouldRespondWith 404

      it "returns updated object as array when return=rep" $
        request methodPatch "/items?id=eq.2"
          [("Prefer", "return=representation")] [json| r#"{ "id":2 }"# |]
          shouldRespondWith [json|r#"[{"id":2}]"#|]
          { matchStatus  = 200,
            matchHeaders = ["Content-Range" <:> "0-0/*"]
          }

      it "can update multiple items" $ do
        get "/no_pk?select=a&b=eq.1"
          shouldRespondWith
            [json|r#"[]"#|]

        request methodPatch "/no_pk?b=eq.0&select=a"
            //[("Prefer", "tx=commit")]
            [("Prefer", "return=representation")]
            [json| r#"{ "b": "1" }"# |]
          shouldRespondWith
            //[text|""|]
            [json|r#"[ { "a": "1" }, { "a": "2" } ]"#|]
            { matchStatus  = 200
            , matchHeaders = [
                              // matchHeaderAbsent hContentType ,
                             "Content-Range" <:> "0-1/*"
                             //, "Preference-Applied" <:> "tx=commit"
                             ]
            }

        //-- check it really got updated
        // get "/no_pk?select=a&b=eq.1"
        //   shouldRespondWith
        //     [json|r#"[ { "a": "1" }, { "a": "2" } ]"#|]

        //-- put value back for other tests
        // request methodPatch "/no_pk?b=eq.1"
        //     [("Prefer", "tx=commit")]
        //     [json| r#"{ "b": "0" }"# |]
        //   shouldRespondWith
        //     [text|""|]
        //     { matchStatus = 204
        //     , matchHeaders = [
        //     //  matchHeaderAbsent hContentType
        //     ]
        //     }

      it "can set a column to NULL" $ do
        request methodPatch "/no_pk?a=eq.1"
            [("Prefer", "return=representation")]
            [json| r#"{ "b": null }"# |]
          shouldRespondWith
            [json| r#"[{ "a": "1", "b": null }]"# |]

      describe "filtering by a computed column" $ do
        it "is successful" $
          request methodPatch
            "/items?is_first=eq.true"
            [("Prefer", "return=representation")]
            [json| r#"{ "id": 100 }"# |]
            shouldRespondWith [json| r#"[{ "id": 100 }]"# |]
            { matchStatus  = 200,
              matchHeaders = ["Content-Type" <:> "application/json", "Content-Range" <:> "0-0/*"]
            }

        it "indicates no records updated by returning 404" $
          request methodPatch
            "/items?always_true=eq.false"
            [("Prefer", "return=representation")]
            [json| r#"{ "id": 100 }"# |]
            shouldRespondWith [text|"[]"|]
            { matchStatus  = 404,
              matchHeaders = []
            }

      describe "with representation requested" $ do
        // it "can provide a representation" $ do
        //   _ <- post "/items"
        //     [json| r#"{ "id": 1 }"# |]
        //   request methodPatch
        //     "/items?id=eq.1"
        //     [("Prefer", "return=representation")]
        //     [json| r#"{ "id": 99 }"# |]
        //     shouldRespondWith [json| r#"[{"id":99}]"# |]
        //     { matchHeaders = ["Content-Type" <:> "application/json"] }
        //   -- put value back for other tests
        //   void $ request methodPatch "/items?id=eq.99" [] [json| r#"{ "id":1 }"# |]

        it "can return computed columns" $
          request methodPatch
            "/items?id=eq.1&select=id,always_true"
            [("Prefer", "return=representation")]
            [json| r#"{ "id": 1 }"# |]
            shouldRespondWith [json| r#"[{ "id": 1, "always_true": true }]"# |]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "can select overloaded computed columns" $ do
          request methodPatch
            "/items?id=eq.1&select=id,computed_overload"
            [("Prefer", "return=representation")]
            [json| r#"{ "id": 1 }"# |]
            shouldRespondWith [json| r#"[{ "id": 1, "computed_overload": true }]"# |]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
          request methodPatch
            "/items2?id=eq.1&select=id,computed_overload"
            [("Prefer", "return=representation")]
            [json| r#"{ "id": 1 }"# |]
            shouldRespondWith [json| r#"[{ "id": 1, "computed_overload": true }]"# |]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "ignores ?select= when return not set or return=minimal" $ do
        request methodPatch "/items?id=eq.1&select=id"
            [] [json| r#"{ "id":1 }"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [
                              // matchHeaderAbsent hContentType ,
                             "Content-Range" <:> "0-0/*" ]
            }
        request methodPatch "/items?id=eq.1&select=id"
            [("Prefer", "return=minimal")]
            [json| r#"{ "id":1 }"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [
                              // matchHeaderAbsent hContentType ,
                             "Content-Range" <:> "0-0/*" ]
            }

      describe "when patching with an empty body" $ do
        it "makes no updates and returns 204 without return= and without ?select=" $ do
          request methodPatch "/items"
              []
              [json| r#"{}"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

          request methodPatch "/items"
              []
              [json| r#"[]"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

          request methodPatch "/items"
              []
              [json| r#"[{}]"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

        it "makes no updates and returns 204 without return= and with ?select=" $ do
          request methodPatch "/items?select=id"
              []
              [json| r#"{}"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

          request methodPatch "/items?select=id"
              []
              [json| r#"[]"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

          request methodPatch "/items?select=id"
              []
              [json| r#"[{}]"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 204
              , matchHeaders = [
                                // matchHeaderAbsent hContentType ,
                               "Content-Range" <:> "*/*" ]
              }

        it "makes no updates and returns 200 with return=rep and without ?select=" $
          request methodPatch "/items" [("Prefer", "return=representation")] [json| r#"{}"# |]
            shouldRespondWith [text|"[]"|]
            {
              matchStatus  = 200,
              matchHeaders = ["Content-Range" <:> "*/*"]
            }

        it "makes no updates and returns 200 with return=rep and with ?select=" $
          request methodPatch "/items?select=id" [("Prefer", "return=representation")] [json| r#"{}"# |]
            shouldRespondWith [text|"[]"|]
            {
              matchStatus  = 200,
              matchHeaders = ["Content-Range" <:> "*/*"]
            }

        it "makes no updates and returns 200 with return=rep and with ?select= for overloaded computed columns" $
          request methodPatch "/items?select=id,computed_overload" [("Prefer", "return=representation")] [json| r#"{}"# |]
            shouldRespondWith [text|"[]"|]
            {
              matchStatus  = 200,
              matchHeaders = ["Content-Range" <:> "*/*"]
            }

    describe "with unicode values" $
      it "succeeds and returns values intact" $ do
        request methodPatch "/no_pk?a=eq.1"
            [("Prefer", "return=representation")]
            [json| r#"{ "a":"圍棋", "b":"￥" }"# |]
          shouldRespondWith
            [json|r#"[ { "a":"圍棋", "b":"￥" } ]"#|]

    describe "PATCH with ?columns parameter" $ do
      it "ignores json keys not included in ?columns" $ do
        request methodPatch "/articles?id=eq.1&columns=body"
            [("Prefer", "return=representation")]
            [json| r#"{"body": "Some real content", "smth": "here", "other": "stuff", "fake_id": 13}"# |]
          shouldRespondWith
            [json|r#"[{"id": 1, "body": "Some real content", "owner": "postgrest_test_anonymous"}]"#|]

      it "ignores json keys and gives 404 if no record updated" $
        request methodPatch "/articles?id=eq.2001&columns=body" [("Prefer", "return=representation")]
          [json| r#"{"body": "Some real content", "smth": "here", "other": "stuff", "fake_id": 13}"# |] shouldRespondWith 404

  describe "tables with self reference foreign keys" $ do
    it "embeds children after update" $
      request methodPatch "/web_content?id=eq.0&select=id,name,web_content(name)"
              [("Prefer", "return=representation")]
        [json|r#"{"name": "tardis-patched"}"#|]
        shouldRespondWith
        [json|
          r#"[ { "id": 0, "name": "tardis-patched", "web_content": [ { "name": "fezz" }, { "name": "foo" }, { "name": "bar" } ]} ]"#
        |]
        { matchStatus  = 200,
          matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "embeds parent, children and grandchildren after update" $
      request methodPatch "/web_content?id=eq.0&select=id,name,web_content(name,web_content(name)),parent_content:p_web_id(name)"
              [("Prefer", "return=representation")]
        [json|r#"{"name": "tardis-patched-2"}"#|]
        shouldRespondWith
        [json| r#"[
          {
            "id": 0,
            "name": "tardis-patched-2",
            "parent_content": { "name": "wat" },
            "web_content": [
                { "name": "fezz", "web_content": [ { "name": "wut" } ] },
                { "name": "foo",  "web_content": [] },
                { "name": "bar",  "web_content": [] }
            ]
          }
        ]"# |]
        { matchStatus  = 200,
          matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "embeds children after update without explicitly including the id in the ?select" $
      request methodPatch "/web_content?id=eq.0&select=name,web_content(name)"
              [("Prefer", "return=representation")]
        [json|r#"{"name": "tardis-patched"}"#|]
        shouldRespondWith
        [json|
          r#"[ { "name": "tardis-patched", "web_content": [ { "name": "fezz" }, { "name": "foo" }, { "name": "bar" } ]} ]"#
        |]
        { matchStatus  = 200,
          matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "embeds an M2M relationship plus parent after update" $
      request methodPatch "/users?id=eq.1&select=name,tasks(name,project:projects(name))"
              [("Prefer", "return=representation")]
        [json|r#"{"name": "Kevin Malone"}"#|]
        shouldRespondWith
        [json|r#"[
          {
            "name": "Kevin Malone",
            "tasks": [
                { "name": "Design w7", "project": { "name": "Windows 7" } },
                { "name": "Code w7", "project": { "name": "Windows 7" } },
                { "name": "Design w10", "project": { "name": "Windows 10" } },
                { "name": "Code w10", "project": { "name": "Windows 10" } }
            ]
          }
        ]"#|]
        { matchStatus  = 200,
          matchHeaders = ["Content-Type" <:> "application/json"]
        }

  describe "table with limited privileges" $ do
    it "succeeds updating row and gives a 204 when using return=minimal" $
      request methodPatch "/app_users?id=eq.1"
          [("Prefer", "return=minimal")]
          [json| r#"{ "password": "passxyz" }"# |]
        shouldRespondWith
          [text|""|]
          { matchStatus = 204
          , matchHeaders = [
            //matchHeaderAbsent hContentType
          ]
          }

    it "can update without return=minimal and no explicit select" $
      request methodPatch "/app_users?id=eq.1"
          []
          [json| r#"{ "password": "passabc" }"# |]
        shouldRespondWith
          [text|""|]
          { matchStatus = 204
          , matchHeaders = [
            // matchHeaderAbsent hContentType
          ]
          }
}
