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
        env::set_var("SUBZERO_DB_SCHEMAS", "[v1, v2]");
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
feature "multiple_schemas"
describe "multiple schemas in single instance" $ do
    describe "Reading tables on different schemas" $ do
      it "succeeds in reading table from default schema v1 if no schema is selected via header" $
        request methodGet "/parents" shouldRespondWith
          [json|r#"[
            {"id":1,"name":"parent v1-1"},
            {"id":2,"name":"parent v1-2"}
          ]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v1"]
          }

      it "succeeds in reading table from default schema v1 after explicitly passing it in the header" $
        request methodGet "/parents" [("Accept-Profile", "v1")] "" shouldRespondWith
          [json|r#"[
            {"id":1,"name":"parent v1-1"},
            {"id":2,"name":"parent v1-2"}
          ]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v1"]
          }

      it "succeeds in reading table from schema v2" $
        request methodGet "/parents" [("Accept-Profile", "v2")] "" shouldRespondWith
          [json|r#"[
            {"id":3,"name":"parent v2-3"},
            {"id":4,"name":"parent v2-4"}
          ]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
          }

      it "succeeds in reading another_table from schema v2" $
        request methodGet "/another_table" [("Accept-Profile", "v2")] "" shouldRespondWith
          [json|r#"[
            {"id":5,"another_value":"value 5"},
            {"id":6,"another_value":"value 6"}
          ]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
          }

      it "does not find another_table in schema v1" $
        request methodGet "/another_table" [("Accept-Profile", "v1")] "" shouldRespondWith 404

      it "fails trying to read table from unkown schema" $
        request methodGet "/parents" [("Accept-Profile", "unkown")] "" shouldRespondWith
          [json|r#"{"message":"The schema must be one of the following: v1, v2"}"#|]
          {
            matchStatus = 406
          }

    describe "Inserting tables on different schemas" $ do
      it "succeeds inserting on default schema and returning it" $
        request methodPost "/children"
            [("Prefer", "return=representation")]
            [json|r#"{"id": 0, "name": "child v1-1", "parent_id": 1}"#|]
          shouldRespondWith
            [json|r#"[{"id": 0, "name": "child v1-1", "parent_id": 1}]"#|]
            {
              matchStatus = 201
            , matchHeaders = ["Content-Profile" <:> "v1"]
            }

      it "succeeds inserting on the v1 schema and returning its parent" $
        request methodPost "/children?select=id,parent(*)"
            [("Prefer", "return=representation"), ("Content-Profile", "v1")]
            [json|r#"{"id": 0, "name": "child v1-2", "parent_id": 2}"#|]
          shouldRespondWith
            [json|r#"[{"id": 0, "parent": {"id": 2, "name": "parent v1-2"}}]"#|]
            {
              matchStatus = 201
            , matchHeaders = ["Content-Profile" <:> "v1"]
            }

      it "succeeds inserting on the v2 schema and returning its parent" $
        request methodPost "/children?select=id,parent(*)"
            [("Prefer", "return=representation"), ("Content-Profile", "v2")]
            [json|r#"{"id": 0, "name": "child v2-3", "parent_id": 3}"#|]
          shouldRespondWith
            [json|r#"[{"id": 0, "parent": {"id": 3, "name": "parent v2-3"}}]"#|]
            {
              matchStatus = 201
            , matchHeaders = ["Content-Profile" <:> "v2"]
            }

      it "fails when inserting on an unknown schema" $
        request methodPost "/children" [("Content-Profile", "unknown")]
          [json|r#"{"name": "child 4", "parent_id": 4}"#|]
          shouldRespondWith
          [json|r#"{"message":"The schema must be one of the following: v1, v2"}"#|]
          {
            matchStatus = 406
          }

    describe "calling procs on different schemas" $ do
      it "succeeds in calling the default schema proc" $
        request methodGet "/rpc/get_parents_below?id=6"
          shouldRespondWith
          [json|r#"[{"id":1,"name":"parent v1-1"}, {"id":2,"name":"parent v1-2"}]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v1"]
          }

      it "succeeds in calling the v1 schema proc and embedding" $
        request methodGet "/rpc/get_parents_below?id=6&select=id,name,children(id,name)" [("Accept-Profile", "v1")] ""
          shouldRespondWith
          [json| r#"[
            {"id":1,"name":"parent v1-1","children":[{"id":1,"name":"child v1-1"}]},
            {"id":2,"name":"parent v1-2","children":[{"id":2,"name":"child v1-2"}]}]"# |]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v1"]
          }

      it "succeeds in calling the v2 schema proc and embedding" $
        request methodGet "/rpc/get_parents_below?id=6&select=id,name,children(id,name)" [("Accept-Profile", "v2")] ""
          shouldRespondWith
          [json| r#"[
            {"id":3,"name":"parent v2-3","children":[{"id":1,"name":"child v2-3"}]},
            {"id":4,"name":"parent v2-4","children":[]}]"# |]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
          }

      it "succeeds in calling the v2 schema proc with POST by using Content-Profile" $
        request methodPost "/rpc/get_parents_below?select=id,name" [("Content-Profile", "v2")]
          [json|r#"{"id": "6"}"#|]
          shouldRespondWith
          [json| r#"[
            {"id":3,"name":"parent v2-3"},
            {"id":4,"name":"parent v2-4"}]"#|]
          {
            matchStatus = 200
          , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
          }

    // describe "Modifying tables on different schemas" $ do
    //   it "succeeds in patching on the v1 schema and returning its parent" $
    //     request methodPatch "/children?select=name,parent(name)&id=eq.1" [("Content-Profile", "v1"), ("Prefer", "return=representation")]
    //       [json|r#"{"name": "child v1-1 updated"}"#|]
    //       shouldRespondWith
    //       [json|r#"[{"name":"child v1-1 updated", "parent": {"name": "parent v1-1"}}]"#|]
    //       {
    //         matchStatus = 200
    //       , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v1"]
    //       }

    //   it "succeeds in patching on the v2 schema and returning its parent" $
    //     request methodPatch "/children?select=name,parent(name)&id=eq.1" [("Content-Profile", "v2"), ("Prefer", "return=representation")]
    //       [json|r#"{"name": "child v2-1 updated"}"#|]
    //       shouldRespondWith
    //       [json|r#"[{"name":"child v2-1 updated", "parent": {"name": "parent v2-3"}}]"#|]
    //       {
    //         matchStatus = 200
    //       , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
    //       }

    //   it "succeeds on deleting on the v2 schema" $ do
    //     request methodDelete "/children?id=eq.1"
    //         [("Content-Profile", "v2"), ("Prefer", "return=representation")]
    //         ""
    //       shouldRespondWith
    //         [json|r#"[{"id": 1, "name": "child v2-3", "parent_id": 3}]"#|]
    //         { matchHeaders = ["Content-Profile" <:> "v2"] }

    //   it "succeeds on PUT on the v2 schema" $
    //     request methodPut "/children?id=eq.111" [("Content-Profile", "v2"), ("Prefer", "return=representation")]
    //       [json| r#"[ { "id": 111, "name": "child v2-111", "parent_id": null } ]"#|]
    //       shouldRespondWith
    //       [json|r#"[{ "id": 111, "name": "child v2-111", "parent_id": null }]"#|]
    //       {
    //         matchStatus = 200
    //       , matchHeaders = ["Content-Type" <:> "application/json", "Content-Profile" <:> "v2"]
    //       }

    // describe "OpenAPI output" $ do
    //   it "succeeds in reading table definition from default schema v1 if no schema is selected via header" $ do
    //       req <- request methodGet "/" [] ""

    //       liftIO $ do
    //         simpleHeaders req `shouldSatisfy` matchHeader "Content-Profile" "v1"

    //         let def = simpleBody req ^? key "definitions" . key "parents"

    //         def `shouldBe` Just
    //             [aesonQQ|
    //               {
    //                 "type" : "object",
    //                 "properties" : {
    //                   "id" : {
    //                     "description" : "Note:\nThis is a Primary Key.<pk/>",
    //                     "format" : "integer",
    //                     "type" : "integer"
    //                   },
    //                   "name" : {
    //                     "format" : "text",
    //                     "type" : "string"
    //                   }
    //                 },
    //                 "required" : [
    //                   "id"
    //                 ]
    //               }
    //             |]

    //   it "succeeds in reading table definition from default schema v1 after explicitly passing it in the header" $ do
    //       r <- request methodGet "/" [("Accept-Profile", "v1")] ""

    //       liftIO $ do
    //         simpleHeaders r `shouldSatisfy` matchHeader "Content-Profile" "v1"

    //         let def = simpleBody r ^? key "definitions" . key "parents"

    //         def `shouldBe` Just
    //             [aesonQQ|
    //               {
    //                 "type" : "object",
    //                 "properties" : {
    //                   "id" : {
    //                     "description" : "Note:\nThis is a Primary Key.<pk/>",
    //                     "format" : "integer",
    //                     "type" : "integer"
    //                   },
    //                   "name" : {
    //                     "format" : "text",
    //                     "type" : "string"
    //                   }
    //                 },
    //                 "required" : [
    //                   "id"
    //                 ]
    //               }
    //             |]

    //   it "succeeds in reading table definition from schema v2" $ do
    //       r <- request methodGet "/" [("Accept-Profile", "v2")] ""

    //       liftIO $ do
    //         simpleHeaders r `shouldSatisfy` matchHeader "Content-Profile" "v2"

    //         let def = simpleBody r ^? key "definitions" . key "parents"

    //         def `shouldBe` Just
    //             [aesonQQ|
    //               {
    //                 "type" : "object",
    //                 "properties" : {
    //                   "id" : {
    //                     "description" : "Note:\nThis is a Primary Key.<pk/>",
    //                     "format" : "integer",
    //                     "type" : "integer"
    //                   },
    //                   "name" : {
    //                     "format" : "text",
    //                     "type" : "string"
    //                   }
    //                 },
    //                 "required" : [
    //                   "id"
    //                 ]
    //               }
    //             |]

    //   it "succeeds in reading another_table definition from schema v2" $ do
    //       r <- request methodGet "/" [("Accept-Profile", "v2")] ""

    //       liftIO $ do
    //         simpleHeaders r `shouldSatisfy` matchHeader "Content-Profile" "v2"

    //         let def = simpleBody r ^? key "definitions" . key "another_table"

    //         def `shouldBe` Just
    //             [aesonQQ|
    //               {
    //                 "type" : "object",
    //                 "properties" : {
    //                   "id" : {
    //                     "description" : "Note:\nThis is a Primary Key.<pk/>",
    //                     "format" : "integer",
    //                     "type" : "integer"
    //                   },
    //                   "another_value" : {
    //                     "format" : "text",
    //                     "type" : "string"
    //                   }
    //                 },
    //                 "required" : [
    //                   "id"
    //                 ]
    //               }
    //             |]

    //   it "doesn't find another_table definition in schema v1" $ do
    //     r <- request methodGet "/" [("Accept-Profile", "v1")] ""

    //     liftIO $ do
    //       let def = simpleBody r ^? key "definitions" . key "another_table"
    //       def `shouldBe` Nothing

    //   it "fails trying to read definitions from unkown schema" $
    //     request methodGet "/" [("Accept-Profile", "unkown")] "" shouldRespondWith
    //       [json|r#"{"message":"The schema must be one of the following: v1, v2"}"#|]
    //       {
    //         matchStatus = 406
    //       }
}
