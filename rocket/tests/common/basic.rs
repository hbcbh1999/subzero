// Copyright (c) 2022-2025 subZero Cloud S.R.L
//
// This file is part of subZero - The All-in-One library suite for internal tools development
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
use super::setup::*;
use super::super::start;

static INIT_CLIENT: Once = Once::new();
lazy_static! {
    static ref CLIENT_INNER: AsyncOnce<Client> = AsyncOnce::new(async {
        env::set_var("SUBZERO_DB_SCHEMAS", "[public]");
        env::remove_var("SUBZERO_DB_PRE_REQUEST");
        env::set_var("SUBZERO_DB_USE_LEGACY_GUCS", "false");
        Client::untracked(start().await.unwrap()).await.expect("valid client")
    });
    static ref CLIENT: &'static AsyncOnce<Client> = {
        thread::spawn(move || {
            RUNTIME.block_on(async {
                CLIENT_INNER.get().await;
            })
        })
        .join()
        .expect("Thread panicked");
        &*CLIENT_INNER
    };
}

haskell_test! {
feature "basic"
  describe "Function calls in select" $ do
    it "can call a function on a row column" $
      get "/projects?select=name:$upper(name)" shouldRespondWith
        [json|r#"[
          {"name":"WINDOWS 7"},
          {"name":"WINDOWS 10"},
          {"name":"IOS"},
          {"name":"OSX"},
          {"name":"ORPHAN"}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    // it "can call more functions on a row column" $
    //   get "/projects?select=name:$upper(name),name2:$concat('X-'::text, name)" shouldRespondWith
    //     [json|r#"[
    //       {"name":"WINDOWS 7","name2":"X-Windows 7"},
    //       {"name":"WINDOWS 10","name2":"X-Windows 10"},
    //       {"name":"IOS","name2":"X-IOS"},
    //       {"name":"OSX","name2":"X-OSX"},
    //       {"name":"ORPHAN","name2":"X-Orphan"}
    //     ]"#|]
    //     { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can NOT call unsafe functions" $
        get "/projects?select=name,random:$random(),tasks($randomagain())" shouldRespondWith
          [json|r#"{"details":"calling: 'random' is not allowed","message":"Unsafe functions called"}"#|]
          { matchStatus  = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
    // it "can call a function with multiple parameters" $
    //   get "/projects?select=name:$concat('X-'::text, name)" shouldRespondWith
    //     [json|r#"[
    //       {"name":"X-Windows 7"},
    //       {"name":"X-Windows 10"},
    //       {"name":"X-IOS"},
    //       {"name":"X-OSX"},
    //       {"name":"X-Orphan"}
    //     ]"#|]
    //     { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call a function with integer parameters" $
      get "/projects?select=name:$substr(name, '2')" shouldRespondWith
        [json|r#"[
          {"name":"indows 7"},
          {"name":"indows 10"},
          {"name":"OS"},
          {"name":"SX"},
          {"name":"rphan"}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call an aggregate function" $
      get "/users_tasks?select=user_id, total:$count(task_id)&groupby=user_id&order=user_id.asc" shouldRespondWith
        [json|r#"[
          {"user_id":1,"total":4},
          {"user_id":2,"total":3},
          {"user_id":3,"total":2}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
  describe "delete" $ do
      it "succeeds with 204 and deletion count" $
        request methodDelete "/projects?id=eq.5"
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
        request methodDelete "/projects?select=id&id=eq.5" [("Prefer", "return=representation, count=exact")] ""
          shouldRespondWith [json|r#"[{"id":5}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/1"]
          }
  describe "upsert" $ do
    it "INSERTs and UPDATEs rows on pk conflict" $
      request methodPost "/clients?select=id,name" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
        [json| r#"[
          { "id": 1, "name": "Microsoft"},
          { "id": 3, "name": "Oracle"}
        ]"#|] shouldRespondWith [json| r#"[
          { "id": 1, "name": "Microsoft"},
          { "id": 3, "name": "Oracle"}
        ]"#|]
        { matchStatus = 201
        , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
        }

    it "INSERTs and ignores rows on pk conflict" $
      request methodPost "/clients?select=id,name" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
        [json| r#"[
          { "id": 1, "name": "Microsoft Changed"},
          { "id": 3, "name": "Oracle"}
        ]"#|] shouldRespondWith [json| r#"[
          { "id": 3, "name": "Oracle"}
        ]"#|]
        { matchStatus = 201
        , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates", "Content-Type" <:> "application/json"]
        }

  describe "updating" $ do
    it "basic no representation" $ do
      request methodPatch "/tasks?id=eq.1"
        [json|r#"{"name":"Design w7 updated"}"#|]
        shouldRespondWith
        [text|""|]
        { matchStatus  = 204
          , matchHeaders = [ "Content-Type" <:> "application/json"
                            //, "Location" <:> "/projects?id=eq.6"
                            , "Content-Range" <:> "0-0/*" ]
        }
    it "basic with representation" $ do
        request methodPatch "/tasks?select=id,name&id=in.(1,3)"
          [("Prefer", "return=representation, count=exact")]
          [json|r#"{"name":"updated"}"#|]
          shouldRespondWith
          [json|r#"[{"id":1,"name":"updated"},{"id":3,"name":"updated"}]"#|]
          { matchStatus  = 200
            , matchHeaders = [ "Content-Type" <:> "application/json"
                              //, "Location" <:> "/projects?id=eq.6"
                              , "Content-Range" <:> "0-1/2" ]
          }
    it "with embedding" $ do
      request methodPatch "/projects?select=id,name,client:clients(id),tasks(id)&id=in.(1,3)"
        [("Prefer", "return=representation, count=exact")]
        [json|r#"{"name":"updated"}"#|]
        shouldRespondWith
        [json|r#"[
          {"id":1,"name":"updated","client":{"id":1},"tasks":[{"id":1},{"id":2}]},
          {"id":3,"name":"updated","client":{"id":2},"tasks":[{"id":5},{"id":6}]}
        ]"#|]
        { matchStatus  = 200
          , matchHeaders = [ "Content-Type" <:> "application/json"
                            //, "Location" <:> "/projects?id=eq.6"
                            , "Content-Range" <:> "0-1/2" ]
        }
    it "with embedding many to many" $ do
        request methodPatch "/tasks?select=id,name,project:projects(id),users(id,name)&id=in.(1,3)"
          [("Prefer", "return=representation, count=exact")]
          [json|r#"{"name":"updated"}"#|]
          shouldRespondWith
          [json|r#"[
            {"id":1,"name":"updated","project":{"id":1},"users":[{"id":1,"name":"Angela Martin"},{"id":3,"name":"Dwight Schrute"}]},
            {"id":3,"name":"updated","project":{"id":2},"users":[{"id":1,"name":"Angela Martin"}]}
          ]"#|]
          { matchStatus  = 200
            , matchHeaders = [ "Content-Type" <:> "application/json"
                              //, "Location" <:> "/projects?id=eq.6"
                              , "Content-Range" <:> "0-1/2" ]
          }
  describe "inserting" $ do
    it "basic with representation" $ do
        request methodPost "/clients?select=id,name"
          [("Prefer", "return=representation,count=exact")]
          [json|r#"{"name":"new client"}"#|]
          shouldRespondWith
          [json|r#"[{"id":3,"name":"new client"}]"#|]
          { matchStatus  = 201
            , matchHeaders = [ "Content-Type" <:> "application/json"
                             //, "Location" <:> "/projects?id=eq.6"
                             , "Content-Range" <:> "*/1" ]
          }
    it "basic no representation" $ do
        request methodPost "/projects"
          [json|r#"{"name":"new project"}"#|]
          shouldRespondWith
          [text|""|]
          { matchStatus  = 201
            , matchHeaders = [ "Content-Type" <:> "application/json"
                              //, "Location" <:> "/projects?id=eq.6"
                              , "Content-Range" <:> "*/*" ]
          }

  describe "json operators" $ do

    it "filtering with cs works" $
      get "/complex_items?select=id&settings=cs.1" shouldRespondWith
        [json| r#"[{"id":3}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/complex_items?select=id&settings=cs.[\"a\"]" shouldRespondWith
        [json| r#"[{"id":3}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "obtains a json subfield one level with casting" $
      get "/complex_items?id=eq.1&select=settings->foo" shouldRespondWith
        [json| r#"[{"foo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield one level with casting" $
      get "/complex_items?id=eq.1&select=myFoo:settings->foo" shouldRespondWith
        [json| r#"[{"myFoo":{"int":1,"bar":"baz"}}]"# |] //-- the value of foo here is of type "text"
        { matchHeaders = ["Content-Type" <:> "application/json"] }


    it "obtains a json subfield two levels (string)" $
      get "/complex_items?id=eq.1&select=settings->foo->bar" shouldRespondWith
        [json| r#"[{"bar":"baz"}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield two levels (string)" $
      get "/complex_items?id=eq.1&select=myBar:settings->foo->bar" shouldRespondWith
        [json| r#"[{"myBar":"baz"}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "obtains a json subfield two levels" $
      get "/complex_items?id=eq.1&select=settings->foo->int" shouldRespondWith
        [json| r#"[{"int":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "renames json subfield two levels" $
      get "/complex_items?id=eq.1&select=myInt:settings->foo->int" shouldRespondWith
        [json| r#"[{"myInt":1}]"# |] //-- the value in the db is an int, but here we expect a string for now
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "select" $
    it "simple" $
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
    it "single row" $
      request methodGet "/projects?id=eq.1&select=id" [("Accept", "application/vnd.pgrst.object")] ""
        shouldRespondWith
        [json|r#"{"id":1}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }
    it "limit offset" $
      get "/projects?select=id&order=id&limit=3&offset=2" shouldRespondWith
        [json| r#"[{"id":3},{"id":4},{"id":5}]"#|]
        { matchStatus = 200}
    // it "with cast" $
    //   get "/tbl1?select=one,two::char" shouldRespondWith
    //     [json| r#"
    //         [
    //             {"one":"hello!","two":"10"},
    //             {"one":"goodbye","two":"20"}
    //         ]
    //     "#|]
    //   { matchStatus = 200
    //   , matchHeaders = ["Content-Type" <:> "application/json"]
    //   }

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
    it "children and parent" $
      get "/projects?select=id,name,client:clients(id,name),tasks(id,name)&id=in.(1,2)" shouldRespondWith
        [json| r#"
        [
          {"id":1,"name":"Windows 7", "tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}],  "client":{"id":1,"name":"Microsoft"}},
          {"id":2,"name":"Windows 10","tasks":[{"id":3,"name":"Design w10"},{"id":4,"name":"Code w10"}],"client":{"id":1,"name":"Microsoft"}}
        ]
        "#|]
      { matchStatus = 200
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }

    it "many" $
      get "/tasks?select=id,name,users(id,name)&id=in.(1,5)" shouldRespondWith
        [json| r#"
        [
          {"id":1,"name":"Design w7","users":[{"id":1,"name":"Angela Martin"},{"id":3,"name":"Dwight Schrute"}]},
          {"id":5,"name":"Design IOS","users":[{"id":2,"name":"Michael Scott"},{"id":3,"name":"Dwight Schrute"}]}
        ]
        "#|]
      { matchStatus = 200
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }
}
