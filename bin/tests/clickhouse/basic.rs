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
pub static INIT_CLIENT: Once = Once::new();

lazy_static! {
    pub static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "basic"
  // describe "Function calls in select" $ do
  //   it "can call a function on a row column" $
  //     get "/projects?select=name:$upper(name)" shouldRespondWith
  //       [json|r#"[
  //         {"name":"WINDOWS 7"},
  //         {"name":"WINDOWS 10"},
  //         {"name":"IOS"},
  //         {"name":"OSX"},
  //         {"name":"ORPHAN"}
  //       ]"#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }
  //   it "can NOT call unsafe functions" $
  //       get "/projects?select=name,random:$random(),tasks($randomagain())" shouldRespondWith
  //         [json|r#"{"details":"calling: 'random' is not allowed","message":"Unsafe functions called"}"#|]
  //         { matchStatus  = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
  //   it "can call a function with integer parameters" $
  //     get "/projects?select=name:$substr(name, '2')" shouldRespondWith
  //       [json|r#"[
  //         {"name":"indows 7"},
  //         {"name":"indows 10"},
  //         {"name":"OS"},
  //         {"name":"SX"},
  //         {"name":"rphan"}
  //       ]"#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }
  //   it "can call an aggregate function" $
  //     get "/users_tasks?select=user_id, total:$count(task_id)&groupby=user_id&order=user_id.asc" shouldRespondWith
  //       [json|r#"[
  //         {"user_id":1,"total":4},
  //         {"user_id":2,"total":3},
  //         {"user_id":3,"total":2}
  //       ]"#|]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }
  // describe "delete" $ do
  //     it "succeeds with 204 and deletion count" $
  //       request methodDelete "/projects?id=eq.5"
  //           // []
  //           ""
  //         shouldRespondWith
  //           [text|""|]
  //           { matchStatus  = 204
  //           , matchHeaders = [
  //                            // matchHeaderAbsent hContentType,
  //                            "Content-Range" <:> "*/*" ]
  //           }

  //     it "returns the deleted item and count if requested" $
  //       request methodDelete "/projects?id=eq.5" [("Prefer", "return=representation, count=exact")] ""
  //         shouldRespondWith [json|r#"[{"rowid":5}]"#|]
  //         { matchStatus  = 200
  //         , matchHeaders = ["Content-Range" <:> "*/1"]
  //         }
  // describe "upsert" $ do
  //   it "INSERTs and UPDATEs rows on pk conflict" $
  //     request methodPost "/clients?select=id,name" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
  //       [json| r#"[
  //         { "id": 1, "name": "Microsoft"},
  //         { "id": 3, "name": "Oracle"}
  //       ]"#|] shouldRespondWith [json| r#"[
  //         { "id": 1, "name": "Microsoft"},
  //         { "id": 3, "name": "Oracle"}
  //       ]"#|]
  //       { matchStatus = 201
  //       , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
  //       }

  //   it "INSERTs and ignores rows on pk conflict" $
  //     request methodPost "/clients?select=id,name" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
  //       [json| r#"[
  //         { "id": 1, "name": "Microsoft"},
  //         { "id": 3, "name": "Oracle"}
  //       ]"#|] shouldRespondWith [json| r#"[
  //         { "id": 3, "name": "Oracle"}
  //       ]"#|]
  //       { matchStatus = 201
  //       , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates", "Content-Type" <:> "application/json"]
  //       }

  // describe "updating" $ do
  //   it "basic no representation" $ do
  //     request methodPatch "/tasks?id=eq.1"
  //       [json|r#"{"name":"Design w7 updated"}"#|]
  //       shouldRespondWith
  //       [text|""|]
  //       { matchStatus  = 204
  //         , matchHeaders = [ "Content-Type" <:> "application/json"
  //                           //, "Location" <:> "/projects?id=eq.6"
  //                           , "Content-Range" <:> "0-0/*" ]
  //       }
  //   it "basic with representation" $ do
  //       request methodPatch "/tasks?select=id,name&id=in.(1,3)"
  //         [("Prefer", "return=representation, count=exact")]
  //         [json|r#"{"name":"updated"}"#|]
  //         shouldRespondWith
  //         [json|r#"[{"id":1,"name":"updated"},{"id":3,"name":"updated"}]"#|]
  //         { matchStatus  = 200
  //           , matchHeaders = [ "Content-Type" <:> "application/json"
  //                             //, "Location" <:> "/projects?id=eq.6"
  //                             , "Content-Range" <:> "0-1/2" ]
  //         }
  // describe "inserting" $ do
  //   it "basic no representation" $ do
  //     request methodPost "/clients"
  //       [json|r#"{"name":"new client"}"#|]
  //       shouldRespondWith
  //       [text|""|]
  //       { matchStatus  = 201
  //         , matchHeaders = [ "Content-Type" <:> "application/json"
  //                          //, "Location" <:> "/projects?id=eq.6"
  //                          , "Content-Range" <:> "*/*" ]
  //       }
  //   it "basic with representation" $ do
  //       request methodPost "/clients?select=id,name"
  //         [("Prefer", "return=representation,count=exact")]
  //         [json|r#"{"name":"new client"}"#|]
  //         shouldRespondWith
  //         [json|r#"[{"id":3,"name":"new client"}]"#|]
  //         { matchStatus  = 201
  //           , matchHeaders = [ "Content-Type" <:> "application/json"
  //                            //, "Location" <:> "/projects?id=eq.6"
  //                            , "Content-Range" <:> "*/1" ]
  //         }
        
  // describe "json operators" $ do
  //   it "obtains a json subfield one level with casting" $
  //     get "/complex_items?id=eq.1&select=settings->>foo" shouldRespondWith
  //       [json| r#"[{"foo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

  //   it "renames json subfield one level with casting" $
  //     get "/complex_items?id=eq.1&select=myFoo:settings->>foo" shouldRespondWith
  //       [json| r#"[{"myFoo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }


  //   it "obtains a json subfield two levels (string)" $
  //     get "/complex_items?id=eq.1&select=settings->foo->>bar" shouldRespondWith
  //       [json| r#"[{"bar":"baz"}]"# |]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

  //   it "renames json subfield two levels (string)" $
  //     get "/complex_items?id=eq.1&select=myBar:settings->foo->>bar" shouldRespondWith
  //       [json| r#"[{"myBar":"baz"}]"# |]
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

  //   it "obtains a json subfield two levels with casting (int)" $
  //     get "/complex_items?id=eq.1&select=settings->foo->>int::integer" shouldRespondWith
  //       [json| r#"[{"int":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

  //   it "renames json subfield two levels with casting (int)" $
  //     get "/complex_items?id=eq.1&select=myInt:settings->foo->>int::integer" shouldRespondWith
  //       [json| r#"[{"myInt":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
  //       { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "select" $
    it "simple" $
      get "/projects?select=id,name&id=in.(1,2)" shouldRespondWith
        [json| r#"
        [
          {"id":1,"name":"Windows 7"},
          {"id":2,"name":"Windows 10"}
        ]
        "#|]
      { matchStatus = 200
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }
      // get "/tbl1?select=one,two" shouldRespondWith
      //   [json| r#"
      //       [
      //           {"one":"hello!","two":10},
      //           {"one":"goodbye","two":20}
      //       ]
      //   "#|]
      // { matchStatus = 200
      // , matchHeaders = ["Content-Type" <:> "application/json"]
      // }
    // it "with cast" $
    //   get "/tbl1?select=one,two::text" shouldRespondWith
    //     [json| r#"
    //         [
    //             {"one":"hello!","two":"10"},
    //             {"one":"goodbye","two":"20"}
    //         ]
    //     "#|]
    //   { matchStatus = 200
    //   , matchHeaders = ["Content-Type" <:> "application/json"]
    //   }

  // describe "embeding" $
  //   it "children" $
  //     get "/projects?select=id,name,tasks(id,name)&id=in.(1,2)" shouldRespondWith
  //       [json| r#"
  //       [
  //         {"id":1,"name":"Windows 7","tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]},
  //         {"id":2,"name":"Windows 10","tasks":[{"id":3,"name":"Design w10"},{"id":4,"name":"Code w10"}]}
  //       ]
  //       "#|]
  //     { matchStatus = 200
  //     , matchHeaders = ["Content-Type" <:> "application/json"]
  //     }
  //   it "parent" $
  //     get "/projects?select=id,name,client:clients(id,name)&id=in.(1,2,3)" shouldRespondWith
  //       [json| r#"
  //       [
  //         {"id":1,"name":"Windows 7","client":{"id":1,"name":"Microsoft"}},
  //         {"id":2,"name":"Windows 10","client":{"id":1,"name":"Microsoft"}},
  //         {"id":3,"name":"IOS","client":{"id":2,"name":"Apple"}}
  //       ]
  //       "#|]
  //     { matchStatus = 200
  //     , matchHeaders = ["Content-Type" <:> "application/json"]
  //     }
  //   it "children and parent" $
  //     get "/projects?select=id,name,client:clients(id,name),tasks(id,name)&id=in.(1,2)" shouldRespondWith
  //       [json| r#"
  //       [
  //         {"id":1,"name":"Windows 7", "tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}],  "client":{"id":1,"name":"Microsoft"}},
  //         {"id":2,"name":"Windows 10","tasks":[{"id":3,"name":"Design w10"},{"id":4,"name":"Code w10"}],"client":{"id":1,"name":"Microsoft"}}
  //       ]
  //       "#|]
  //     { matchStatus = 200
  //     , matchHeaders = ["Content-Type" <:> "application/json"]
  //     }

  //   it "many" $
  //     get "/tasks?select=id,name,users(id,name)&id=in.(1,5)" shouldRespondWith
  //       [json| r#"
  //       [
  //         {"id":1,"name":"Design w7","users":[{"id":1,"name":"Angela Martin"},{"id":3,"name":"Dwight Schrute"}]},
  //         {"id":5,"name":"Design IOS","users":[{"id":2,"name":"Michael Scott"},{"id":3,"name":"Dwight Schrute"}]}
  //       ]
  //       "#|]
  //     { matchStatus = 200
  //     , matchHeaders = ["Content-Type" <:> "application/json"]
  //     }
}
