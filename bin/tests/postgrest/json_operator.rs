use super::super::start;
use super::common::*;
use demonstrate::demonstrate;
use pretty_assertions::assert_eq;
use rocket::http::Accept;
use serde_json::Value;
use std::str::FromStr;

use async_once::AsyncOnce;
use rocket::local::asynchronous::Client;
use std::sync::Once;
static INIT_CLIENT: Once = Once::new();

lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}
haskell_test! {
feature "json operators"
  describe "Shaping response with select parameter" $ do
    it "obtains a json subfield one level with casting" $
      get "/complex_items?id=eq.1&select=settings->>foo::json" shouldRespondWith
        [json| r#"[{"foo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield one level with casting" $
      get "/complex_items?id=eq.1&select=myFoo:settings->>foo::json" shouldRespondWith
        [json| r#"[{"myFoo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails on bad casting (data of the wrong format)" $
      get "/complex_items?select=settings->foo->>bar::integer"
        shouldRespondWith
        // (
        // if actualPgVersion >= pgVersion121 then
        [json| r#"{"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for type integer: \"baz\""}"# |]
        // else
        // [json| r#"{"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for integer: \"baz\""}"# |]
        // )
        { matchStatus  = 400 , matchHeaders = [] }

    it "obtains a json subfield two levels (string)" $
      get "/complex_items?id=eq.1&select=settings->foo->>bar" shouldRespondWith
        [json| r#"[{"bar":"baz"}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield two levels (string)" $
      get "/complex_items?id=eq.1&select=myBar:settings->foo->>bar" shouldRespondWith
        [json| r#"[{"myBar":"baz"}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "obtains a json subfield two levels with casting (int)" $
      get "/complex_items?id=eq.1&select=settings->foo->>int::integer" shouldRespondWith
        [json| r#"[{"int":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield two levels with casting (int)" $
      get "/complex_items?id=eq.1&select=myInt:settings->foo->>int::integer" shouldRespondWith
        [json| r#"[{"myInt":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    //-- TODO the status code for the error is 404, this is because 42883 represents undefined function
    //-- this works fine for /rpc/unexistent requests, but for this case a 500 seems more appropriate
    it "fails when a double arrow ->> is followed with a single arrow ->" $ do
      get "/json_arr?select=data->>c->1"
        shouldRespondWith
        // (
        // if actualPgVersion >= pgVersion112 then
        [json|
          r#"{"hint":"No operator matches the given name and argument types. You might need to add explicit type casts.",
          "details":null,"code":"42883","message":"operator does not exist: text -> integer"}"# |]
        //   else
        // [json|
        //   r#"{"hint":"No operator matches the given name and argument type(s). You might need to add explicit type casts.",
        //   "details":null,"code":"42883","message":"operator does not exist: text -> integer"}"# |]
        //                     )
        { matchStatus  = 404 , matchHeaders = [] }
      get "/json_arr?select=data->>c->b"
        shouldRespondWith
        // (
        // if actualPgVersion >= pgVersion112 then
        [json|
          r#"{"hint":"No operator matches the given name and argument types. You might need to add explicit type casts.",
          "details":null,"code":"42883","message":"operator does not exist: text -> unknown"}"# |]
        //   else
        // [json|
        //   r#"{"hint":"No operator matches the given name and argument type(s). You might need to add explicit type casts.",
        //   "details":null,"code":"42883","message":"operator does not exist: text -> unknown"}"# |]
        //                     )
        { matchStatus  = 404 , matchHeaders = [] }

    describe "with array index" $ do
      it "can get array of ints and alias/cast it" $ do
        get "/json_arr?select=data->>0::int&id=in.(1,2)" shouldRespondWith
          [json| r#"[{"data":1}, {"data":4}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=idx0:data->>0::int,idx1:data->>1::int&id=in.(1,2)" shouldRespondWith
          [json| r#"[{"idx0":1,"idx1":2}, {"idx0":4,"idx1":5}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can get nested array of ints" $ do
        get "/json_arr?select=data->0->>1::int&id=in.(3,4)" shouldRespondWith
          [json| r#"[{"data":8}, {"data":7}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=data->0->0->>1::int&id=in.(3,4)" shouldRespondWith
          [json| r#"[{"data":null}, {"data":6}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can get array of objects" $ do
        get "/json_arr?select=data->0->>a&id=in.(5,6)" shouldRespondWith
          [json| r#"[{"a":"A"}, {"a":"[1, 2, 3]"}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=data->0->a->>2&id=in.(5,6)" shouldRespondWith
          [json| r#"[{"a":null}, {"a":"3"}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can get array in object keys" $ do
        get "/json_arr?select=data->c->>0::json&id=in.(7,8)" shouldRespondWith
          [json| r#"[{"c":1}, {"c":{"d": [4,5,6,7,8]}}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=data->c->0->d->>4::int&id=in.(7,8)" shouldRespondWith
          [json| r#"[{"d":null}, {"d":8}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "only treats well formed numbers as indexes" $
        get "/json_arr?select=data->0->0xy1->1->23-xy-45->1->xy-6->>0::int&id=eq.9" shouldRespondWith
          [json| r#"[{"xy-6":3}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "finishing json path with single arrow ->" $ do
      it "works when finishing with a key" $ do
        get "/json_arr?select=data->c&id=in.(7,8)" shouldRespondWith
          [json| r#"[{"c":[1,2,3]}, {"c":[{"d": [4,5,6,7,8]}]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=data->0->a&id=in.(5,6)" shouldRespondWith
          [json| r#"[{"a":"A"}, {"a":[1,2,3]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when finishing with an index" $ do
        get "/json_arr?select=data->0->a&id=in.(5,6)" shouldRespondWith
          [json| r#"[{"a":"A"}, {"a":[1,2,3]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/json_arr?select=data->c->0->d&id=eq.8" shouldRespondWith
          [json| r#"[{"d":[4,5,6,7,8]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "filtering response" $ do
    it "can filter by properties inside json column" $ do
      get "/json_table?data->foo->>bar=eq.baz" shouldRespondWith
        [json| r#"[{"data": {"id": 1, "foo": {"bar": "baz"}}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_table?data->foo->>bar=eq.fake" shouldRespondWith
        [json| r#"[]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can filter by properties inside json column using not" $
      get "/json_table?data->foo->>bar=not.eq.baz" shouldRespondWith
        [json| r#"[]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can filter by properties inside json column using ->>" $
      get "/json_table?data->>id=eq.1" shouldRespondWith
        [json| r#"[{"data": {"id": 1, "foo": {"bar": "baz"}}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can be filtered with and/or" $
      get "/grandchild_entities?or=(jsonb_col->a->>b.eq.foo, jsonb_col->>b.eq.bar)&select=id" shouldRespondWith
        [json|r#"[{"id": 4}, {"id": 5}]"#|] { matchStatus = 200, matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can filter by array indexes" $ do
      get "/json_arr?select=data&data->>0=eq.1" shouldRespondWith
        [json| r#"[{"data":[1, 2, 3]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->1->>2=eq.13" shouldRespondWith
        [json| r#"[{"data":[[9, 8, 7], [11, 12, 13]]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->1->>b=eq.B" shouldRespondWith
        [json| r#"[{"data":[{"a": "A"}, {"b": "B"}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->1->b->>1=eq.5" shouldRespondWith
        [json| r#"[{"data":[{"a": [1,2,3]}, {"b": [4,5]}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can filter jsonb" $ do
      get "/jsonb_test?data=eq.{\"e\":1}" shouldRespondWith
        [json| r#"[{"id":4,"data":{"e": 1}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/jsonb_test?data->a=eq.{\"b\":2}" shouldRespondWith
        [json| r#"[{"id":1,"data":{"a": {"b": 2}}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/jsonb_test?data->c=eq.[1,2,3]" shouldRespondWith
        [json| r#"[{"id":2,"data":{"c": [1, 2, 3]}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/jsonb_test?data->0=eq.{\"d\":\"test\"}" shouldRespondWith
        [json| r#"[{"id":3,"data":[{"d": "test"}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "ordering response" $ do
    it "orders by a json column property asc" $
      get "/json_table?order=data->>id.asc" shouldRespondWith
        [json| r#"[{"data": {"id": 0}}, {"data": {"id": 1, "foo": {"bar": "baz"}}}, {"data": {"id": 3}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "orders by a json column with two level property nulls first" $
      get "/json_table?order=data->foo->>bar.nullsfirst" shouldRespondWith
        [json| r#"[{"data": {"id": 3}}, {"data": {"id": 0}}, {"data": {"id": 1, "foo": {"bar": "baz"}}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    // describe "Patching record, in a nonempty table" $
    // it "can set a json column to escaped value" $ do
    //   request methodPatch "/json_table?data->>id=eq.3"
    //       [("Prefer", "return=representation")]
    //       [json| r#"{ "data": { "id":" \"escaped" } }"# |]
    //     shouldRespondWith
    //       [json| r#"[{ "data": { "id":" \"escaped" } }]"# |]

    describe "json array negative index" $ do
    it "can select with negative indexes" $ do
      get "/json_arr?select=data->>-1::int&id=in.(1,2)" shouldRespondWith
        [json| r#"[{"data":3}, {"data":6}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data->0->>-2::int&id=in.(3,4)" shouldRespondWith
        [json| r#"[{"data":8}, {"data":7}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data->-2->>a&id=in.(5,6)" shouldRespondWith
        [json| r#"[{"a":"A"}, {"a":"[1, 2, 3]"}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can filter with negative indexes" $ do
      get "/json_arr?select=data&data->>-3=eq.1" shouldRespondWith
        [json| r#"[{"data":[1, 2, 3]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->-1->>-3=eq.11" shouldRespondWith
        [json| r#"[{"data":[[9, 8, 7], [11, 12, 13]]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->-1->>b=eq.B" shouldRespondWith
        [json| r#"[{"data":[{"a": "A"}, {"b": "B"}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data&data->-1->b->>-1=eq.5" shouldRespondWith
        [json| r#"[{"data":[{"a": [1,2,3]}, {"b": [4,5]}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "should fail on badly formed negatives" $ do
      get "/json_arr?select=data->>-78xy" shouldRespondWith
        // [json|
        //   r#"{"details": "unexpected 'x' expecting digit, \"->\", \"::\" or end of input",
        //   "message": "\"failed to parse select parameter (data->>-78xy)\" (line 1, column 11)"}"# |]
        [json|r#"{"details":"Unexpected `x` Unexpected `-` Unexpected `d` Expected `digit`, `->`, `::`, `.`, `,`, `end of input`, `-`, `\"`, `letter`, `_` or ` `","message":"\"failed to parse select parameter (data->>-78xy)\" (line 1, column 11)"}"#|]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data->>--34" shouldRespondWith
        [json|r#"{"details":"Unexpected `-` Unexpected `d` Expected `digit`, `-`, `\"`, `letter`, `_` or ` `","message":"\"failed to parse select parameter (data->>--34)\" (line 1, column 9)"}"# |]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/json_arr?select=data->>-xy-4" shouldRespondWith
        // [json|
        //   r#"{"details":"unexpected \"x\" expecting digit",
        //   "message":"\"failed to parse select parameter (data->>-xy-4)\" (line 1, column 9)"}"# |]
        [json|r#"{"details":"Unexpected `x` Unexpected `-` Unexpected `d` Expected `digit`, `-`, `\"`, `letter`, `_` or ` `","message":"\"failed to parse select parameter (data->>-xy-4)\" (line 1, column 9)"}"#|]
        { matchStatus = 400, matchHeaders = ["Content-Type" <:> "application/json"] }

    it "obtains a composite type field" $ do
      get "/fav_numbers?select=num->i"
        shouldRespondWith
          [json| r#"[{"i":0.5},{"i":0.6}]"# |]
      get "/fav_numbers?select=num->>i"
        shouldRespondWith
          [json| r#"[{"i":"0.5"},{"i":"0.6"}]"# |]

    it "obtains an array item" $ do
      get "/arrays?select=a:numbers->0,b:numbers->1,c:numbers_mult->0->0,d:numbers_mult->1->2"
        shouldRespondWith
          [json| r#"[{"a":1,"b":2,"c":1,"d":6},{"a":11,"b":12,"c":11,"d":16}]"# |]
      get "/arrays?select=a:numbers->>0,b:numbers->>1,c:numbers_mult->0->>0,d:numbers_mult->1->>2"
        shouldRespondWith
          [json| r#"[{"a":"1","b":"2","c":"1","d":"6"},{"a":"11","b":"12","c":"11","d":"16"}]"# |]

    it "can filter composite type field" $
          get "/fav_numbers?num->>i=gt.0.5"
            shouldRespondWith
              [json| r#"[{"num":{"r":0.6,"i":0.6},"person":"B"}]"# |]

    it "can filter array item" $ do
      get "/arrays?select=id&numbers->0=eq.1"
        shouldRespondWith
          [json| r#"[{"id":0}]"# |]
      get "/arrays?select=id&numbers->>0=eq.11"
        shouldRespondWith
          [json| r#"[{"id":1}]"# |]
      get "/arrays?select=id&numbers_mult->1->1=eq.5"
        shouldRespondWith
          [json| r#"[{"id":0}]"# |]
      get "/arrays?select=id&numbers_mult->2->>2=eq.19"
        shouldRespondWith
          [json| r#"[{"id":1}]"# |]
          it "orders by composite type field" $ do
          get "/fav_numbers?order=num->i.asc"
            shouldRespondWith
              [json| r#"[{"num":{"r":0.5,"i":0.5},"person":"A"}, {"num":{"r":0.6,"i":0.6},"person":"B"}]"# |]
          get "/fav_numbers?order=num->>i.desc"
            shouldRespondWith
              [json| r#"[{"num":{"r":0.6,"i":0.6},"person":"B"}, {"num":{"r":0.5,"i":0.5},"person":"A"}]"# |]

    it "orders by array item" $ do
      get "/arrays?select=id&order=numbers->0.desc"
        shouldRespondWith
          [json| r#"[{"id":1},{"id":0}]"# |]
      get "/arrays?select=id&order=numbers->1.asc"
        shouldRespondWith
          [json| r#"[{"id":0},{"id":1}]"# |]
      get "/arrays?select=id&order=numbers_mult->0->0.desc"
        shouldRespondWith
          [json| r#"[{"id":1},{"id":0}]"# |]
      get "/arrays?select=id&order=numbers_mult->2->2.asc"
        shouldRespondWith
          [json| r#"[{"id":0},{"id":1}]"# |]
}
