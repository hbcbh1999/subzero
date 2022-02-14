use super::super::start;
use super::common::*;
use pretty_assertions::assert_eq;
use rocket::http::Accept;
use serde_json::Value;
use std::str::FromStr;

use demonstrate::demonstrate;

use async_once::AsyncOnce;
use rocket::local::asynchronous::Client;
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
feature "and or param"
  describe "used with GET" $ do
    describe "or param" $ do
      it "can do simple logic" $
        get "/entities?or=(id.eq.1,id.eq.2)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can negate simple logic" $
        get "/entities?not.or=(id.eq.1,id.eq.2)&select=id" shouldRespondWith
          [json|r#"[{ "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can be combined with traditional filters" $
        get "/entities?or=(id.eq.1,id.eq.2)&name=eq.entity 1&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "embedded levels" $ do
      it "can do logic on the second level" $
        get "/entities?child_entities.or=(id.eq.1,name.eq.child entity 2)&select=id,child_entities(id)" shouldRespondWith
          [json|r#"[
            {"id": 1, "child_entities": [ { "id": 1 }, { "id": 2 } ] }, { "id": 2, "child_entities": []},
            {"id": 3, "child_entities": []}, {"id": 4, "child_entities": []}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can do logic on the third level" $
        get "/entities?child_entities.grandchild_entities.or=(id.eq.1,id.eq.2)&select=id,child_entities(id,grandchild_entities(id))"
          shouldRespondWith
            [json|r#"[
              {"id": 1, "child_entities": [
                { "id": 1, "grandchild_entities": [ { "id": 1 }, { "id": 2 } ]},
                { "id": 2, "grandchild_entities": []},
                { "id": 4, "grandchild_entities": []},
                { "id": 5, "grandchild_entities": []}
              ]},
              {"id": 2, "child_entities": [
                { "id": 3, "grandchild_entities": []},
                { "id": 6, "grandchild_entities": []}
              ]},
              {"id": 3, "child_entities": []},
              {"id": 4, "child_entities": []}
            ]"#|]

    describe "and/or params combined" $ do
      it "can be nested inside the same expression" $
        get "/entities?or=(and(name.eq.entity 2,id.eq.2),and(name.eq.entity 1,id.eq.1))&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can be negated while nested" $
        get "/entities?or=(not.and(name.eq.entity 2,id.eq.2),not.and(name.eq.entity 1,id.eq.1))&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can be combined unnested" $
        get "/entities?and=(id.eq.1,name.eq.entity 1)&or=(id.eq.1,id.eq.2)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "operators inside and/or" $ do
      it "can handle eq and neq" $
        get "/entities?and=(id.eq.1,id.neq.2))&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle lt and gt" $
        get "/entities?or=(id.lt.2,id.gt.3)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle lte and gte" $
        get "/entities?or=(id.lte.2,id.gte.3)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle like and ilike" $
        get "/entities?or=(name.like.*1,name.ilike.*ENTITY 2)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle in" $
        get "/entities?or=(id.in.(1,2),id.in.(3,4))&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle is" $
        get "/entities?and=(name.is.null,arr.is.null)&select=id" shouldRespondWith
          [json|r#"[{ "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can handle fts" $ do
        get "/entities?or=(text_search_vector.fts.bar,text_search_vector.fts.baz)&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?or=(text_search_vector.plfts(german).Art%20Spass, text_search_vector.plfts(french).amusant%20impossible, text_search_vector.fts(english).impossible)" shouldRespondWith
          [json|r#"[
            {"text_search_vector": "'fun':5 'imposs':9 'kind':3" },
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" },
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      // when (actualPgVersion >= pgVersion112) $
      it "can handle wfts (websearch_to_tsquery)" $
        get "/tsearch?or=(text_search_vector.plfts(german).Art,text_search_vector.plfts(french).amusant,text_search_vector.not.wfts(english).impossible)"
        shouldRespondWith
          [json|r#"[
                {"text_search_vector": "'also':2 'fun':3 'possibl':8" },
                {"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" },
                {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" },
                {"text_search_vector": "'art':4 'spass':5 'unmog':7" }
          ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can handle cs and cd" $
        get "/entities?or=(arr.cs.{1,2,3},arr.cd.{1})&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 },{ "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can handle range operators" $ do
        get "/ranges?range=eq.[1,3]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=neq.[1,3]&select=id" shouldRespondWith
          [json|r#"[{ "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=lt.[1,10]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=gt.[8,11]&select=id" shouldRespondWith
          [json|r#"[{ "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=lte.[1,3]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=gte.[2,3]&select=id" shouldRespondWith
          [json|r#"[{ "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=cs.[1,2]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=cd.[1,6]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=ov.[0,4]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=sl.[9,10]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=sr.[3,4]&select=id" shouldRespondWith
          [json|r#"[{ "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=nxr.[4,7]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=nxl.[4,7]&select=id" shouldRespondWith
          [json|r#"[{ "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/ranges?range=adj.(3,10]&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can handle array operators" $ do
        get "/entities?arr=eq.{1,2,3}&select=id" shouldRespondWith
          [json|r#"[{ "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=neq.{1,2}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=lt.{2,3}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=lt.{2,0}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=gt.{1,1}&select=id" shouldRespondWith
          [json|r#"[{ "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=gt.{3}&select=id" shouldRespondWith
          [json|r#"[]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=lte.{2,1}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=lte.{1,2,3}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=lte.{1,2}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=cs.{1,2}&select=id" shouldRespondWith
          [json|r#"[{ "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=cd.{1,2,6}&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=ov.{3}&select=id" shouldRespondWith
          [json|r#"[{ "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?arr=ov.{2,3}&select=id" shouldRespondWith
          [json|r#"[{ "id": 2 }, { "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      describe "operators with not" $ do
        it "eq, cs, like can be negated" $
          get "/entities?and=(arr.not.cs.{1,2,3},and(id.not.eq.2,name.not.like.*3))&select=id" shouldRespondWith
            [json|r#"[{ "id": 1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        it "in, is, fts can be negated" $
          get "/entities?and=(id.not.in.(1,3),and(name.not.is.null,text_search_vector.not.fts.foo))&select=id" shouldRespondWith
            [json|r#"[{ "id": 2}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        it "lt, gte, cd can be negated" $
          get "/entities?and=(arr.not.cd.{1},or(id.not.lt.1,id.not.gte.3))&select=id" shouldRespondWith
            [json|r#"[{"id": 2}, {"id": 3}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        it "gt, lte, ilike can be negated" $
          get "/entities?and=(name.not.ilike.*ITY2,or(id.not.gt.4,id.not.lte.1))&select=id" shouldRespondWith
            [json|r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "and/or params with quotes" $ do
      it "eq can have quotes" $
        get "/grandchild_entities?or=(name.eq.\"(grandchild,entity,4)\",name.eq.\"(grandchild,entity,5)\")&select=id" shouldRespondWith
          [json|r#"[{ "id": 4 }, { "id": 5 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "like and ilike can have quotes" $
        get "/grandchild_entities?or=(name.like.\"*ity,4*\",name.ilike.\"*ITY,5)\")&select=id" shouldRespondWith
          [json|r#"[{ "id": 4 }, { "id": 5 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "in can have quotes" $
        get "/grandchild_entities?or=(id.in.(\"1\",\"2\"),id.in.(\"3\",\"4\"))&select=id" shouldRespondWith
          [json|r#"[{ "id": 1 }, { "id": 2 }, { "id": 3 }, { "id": 4 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "allows whitespace" $
      get "/entities?and=( and ( id.in.( 1, 2, 3 ) , id.eq.3 ) , or ( id.eq.2 , id.eq.3 ) )&select=id" shouldRespondWith
        [json|r#"[{ "id": 3 }]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "multiple and/or conditions" $ do
      it "cannot have zero conditions" $
        get "/entities?or=()" shouldRespondWith
          // [json|r#"{
          //   "details": "unexpected \")\" expecting field name (* or [a..z0..9_]), negation operator (not) or logic operator (and, or)",
          //   "message": "\"failed to parse logic tree (())\" (line 1, column 4)"
          // }"#|]
          [json|r#"{"details":"Unexpected `)` Expected `\"`, `letter`, `digit`, `_`, ` `, `not`, `.`, `and`, `or` or `whitespace`","message":"\"failed to parse logic tree (or())\" (line 1, column 4)"}"#|]
          { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can have a single condition" $ do
        get "/entities?or=(id.eq.1)&select=id" shouldRespondWith
          [json|r#"[{"id":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/entities?and=(id.eq.1)&select=id" shouldRespondWith
          [json|r#"[{"id":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can have three conditions" $ do
        get "/grandchild_entities?or=(id.eq.1, id.eq.2, id.eq.3)&select=id" shouldRespondWith
          [json|r#"[{"id":1}, {"id":2}, {"id":3}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/grandchild_entities?and=(id.in.(1,2), id.in.(3,1), id.in.(1,4))&select=id" shouldRespondWith
          [json|r#"[{"id":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "can have four conditions combining and/or" $ do
        get "/grandchild_entities?or=( id.eq.1, id.eq.2, and(id.in.(1,3), id.in.(2,3)), id.eq.4 )&select=id" shouldRespondWith
          [json|r#"[{"id":1}, {"id":2}, {"id":3}, {"id":4}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/grandchild_entities?and=( id.eq.1, not.or(id.eq.2, id.eq.3), id.in.(1,4), or(id.eq.1, id.eq.4) )&select=id" shouldRespondWith
          [json|r#"[{"id":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "used with POST" $
    it "includes related data with filters" $
      request methodPost "/child_entities?select=id,entities(id)&entities.or=(id.eq.2,id.eq.3)&entities.order=id"
          [("Prefer", "return=representation")]
          [json|r#"[
            {"id":7,"name":"entity 4","parent_id":1},
            {"id":8,"name":"entity 5","parent_id":2},
            {"id":9,"name":"entity 6","parent_id":3}
          ]"#|]
        shouldRespondWith
          [json|r#"[{"id": 7, "entities":null}, {"id": 8, "entities": {"id": 2}}, {"id": 9, "entities": {"id": 3}}]"#|]
          { matchStatus = 201 }

    // describe "used with PATCH" $
    // it "succeeds when using and/or params" $
    //   request methodPatch "/grandchild_entities?or=(id.eq.1,id.eq.2)&select=id,name"
    //     [("Prefer", "return=representation")]
    //     [json|r#"{ name : "updated grandchild entity"}"#|] shouldRespondWith
    //     [json|r#"[{ "id": 1, "name" : "updated grandchild entity"},{ "id": 2, "name" : "updated grandchild entity"}]"#|]
    //     { matchHeaders = ["Content-Type" <:> "application/json"] }

    // describe "used with DELETE" $
    // it "succeeds when using and/or params" $
    //   request methodDelete "/grandchild_entities?or=(id.eq.1,id.eq.2)&select=id,name"
    //       [("Prefer", "return=representation")]
    //       ""
    //     shouldRespondWith
    //       [json|r#"[{ "id": 1, "name" : "grandchild entity 1" },{ "id": 2, "name" : "grandchild entity 2" }]"#|]

    it "can query columns that begin with and/or reserved words" $
    get "/grandchild_entities?or=(and_starting_col.eq.smth, or_starting_col.eq.smth)" shouldRespondWith 200

    it "fails when using IN without () and provides meaningful error message" $
    get "/entities?or=(id.in.1,2,id.eq.3)" shouldRespondWith
      // [json|r#"{
      //   "details": "unexpected \"1\" expecting \"(\"",
      //   "message": "\"failed to parse logic tree ((id.in.1,2,id.eq.3))\" (line 1, column 10)"
      // }"#|]
      [json|r#"{"details":"Unexpected `i` Expected `not`, `.`, `and`, `or` or `whitespace`","message":"\" (or(id.in.1,2,id.eq.3))\" (line 1, column 7)"}"#|]
      { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails on malformed query params and provides meaningful error message" $ do
      get "/entities?or=)(" shouldRespondWith
        // [json|r#"{
        //   "details": "unexpected \")\" expecting \"(\"",
        //   "message": "\"failed to parse logic tree ()()\" (line 1, column 3)"
        // }"#|]
        [json|r#"{"details":"Unexpected `)` Expected `whitespace` or `(`","message":"\"failed to parse logic tree (or)()\" (line 1, column 3)"}"#|]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/entities?and=(ord(id.eq.1,id.eq.1),id.eq.2)" shouldRespondWith
        // [json|r#"{
        //   "details": "unexpected \"d\" expecting \"(\"",
        //   "message": "\"failed to parse logic tree ((ord(id.eq.1,id.eq.1),id.eq.2))\" (line 1, column 7)"
        // }"#|]
        [json|r#"{"details":"Unexpected `d` Expected `whitespace` or `(`","message":"\"failed to parse logic tree (and(ord(id.eq.1,id.eq.1),id.eq.2))\" (line 1, column 7)"}"#|]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/entities?or=(id.eq.1,not.xor(id.eq.2,id.eq.3))" shouldRespondWith
        // [json|r#"{
        //   "details": "unexpected \"x\" expecting logic operator (and, or)",
        //   "message": "\"failed to parse logic tree ((id.eq.1,not.xor(id.eq.2,id.eq.3)))\" (line 1, column 16)"
        // }"#|]
        [json|r#"{"details":"Unexpected `x` Expected `and` or `or`","message":"\"failed to parse logic tree (or(id.eq.1,not.xor(id.eq.2,id.eq.3)))\" (line 1, column 16)"}"#|]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
}
