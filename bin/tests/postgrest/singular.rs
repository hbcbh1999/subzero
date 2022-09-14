use super::super::start;
use super::common::*;
use async_once::AsyncOnce;
use demonstrate::demonstrate;
use rocket::local::asynchronous::Client;
use std::sync::Once;
static INIT_CLIENT: Once = Once::new();

lazy_static! {
  static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async{
    //TODO!!!! checks for not updated are invalid since the tests are configured to rollback all actions
    Client::untracked(start().await.unwrap()).await.expect("valid client")
  });
}

haskell_test! {
feature "singular"
describe "Requesting singular json object" $ do

describe "with GET request" $ do
  it "fails for zero rows" $
    request methodGet  "/items?id=gt.0&id=lt.0" [("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith 406

  it "will select an existing object" $ do
    request methodGet "/items?id=eq.5" [("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
        [json|r#"{"id":5}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }
    //-- also test without the +json suffix
    request methodGet "/items?id=eq.5"
        [("Accept", "application/vnd.pgrst.object")] ""
      shouldRespondWith
        [json|r#"{"id":5}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

  it "can combine multiple prefer values" $
    request methodGet "/items?id=eq.5" [("Accept", "application/vnd.pgrst.object+json"), ("Prefer","count=exact")] ""
      shouldRespondWith
        [json|r#"{"id":5}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

  it "can shape plurality singular object routes" $
    request methodGet "/projects_view?id=eq.1&select=id,name,clients(*),tasks(id,name)" [("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
        [json|r#"{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

describe "when updating rows" $ do
  it "works for one row with return=rep" $ do
    request methodPatch "/addresses?id=eq.1"
        [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"{ "address": "B Street" }"# |]
      shouldRespondWith
        [json|r#"{"id":1,"address":"B Street"}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

  it "works for one row with return=minimal" $
    request methodPatch "/addresses?id=eq.1"
        [("Prefer", "return=minimal"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"{ "address": "C Street" }"# |]
      shouldRespondWith
        [text|""|]
        { matchStatus  = 204
        , matchHeaders = [
          // matchHeaderAbsent hContentType
          ]
        }

  it "raises an error for multiple rows" $ do
    request methodPatch "/addresses"
        [/*("Prefer", "tx=commit"),*/ ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"{ "address": "zzz" }"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 4 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should not be updated, either
    get "/addresses?id=eq.1"
      shouldRespondWith
        [json|r#"[{"id":1,"address":"address 1"}]"#|]

  it "raises an error for multiple rows with return=rep" $ do
    request methodPatch "/addresses"
        [/*("Prefer", "tx=commit"),*/ ("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"{ "address": "zzz" }"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 4 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should not be updated, either
    get "/addresses?id=eq.1"
      shouldRespondWith
        [json|r#"[{"id":1,"address":"address 1"}]"#|]

  it "raises an error for zero rows" $
    request methodPatch "/items?id=gt.0&id=lt.0"
            [("Accept", "application/vnd.pgrst.object+json")] [json|r#"{"id":1}"#|]
      shouldRespondWith
              [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
              { matchStatus  = 406
              , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
              ]
              }

  it "raises an error for zero rows with return=rep" $
    request methodPatch "/items?id=gt.0&id=lt.0"
            [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")] [json|r#"{"id":1}"#|]
      shouldRespondWith
              [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
              { matchStatus  = 406
              , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
              ]
              }

describe "when creating rows" $ do
  it "works for one row with return=rep" $ do
    request methodPost "/addresses"
        [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"[ { "id": 102, "address": "xxx" } ]"# |]
      shouldRespondWith
        [json|r#"{"id":102,"address":"xxx"}"#|]
        { matchStatus  = 201
        , matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"]
        }

  it "works for one row with return=minimal" $ do
    request methodPost "/addresses"
        [("Prefer", "return=minimal"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"[ { "id": 103, "address": "xxx" } ]"# |]
      shouldRespondWith
      [text|""|]
        { matchStatus  = 201
        , matchHeaders = [
                          // matchHeaderAbsent hContentType,
                         "Content-Range" <:> "*/*" ]
        }

  it "raises an error when attempting to create multiple entities" $ do
    request methodPost "/addresses"
        [/*("Prefer", "tx=commit"),*/ ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"[ { "id": 200, "address": "xxx" }, { "id": 201, "address": "yyy" } ]"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 2 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should not exist, either
    get "/addresses?id=eq.200"
      shouldRespondWith
      [text|"[]"|]

  it "raises an error when attempting to create multiple entities with return=rep" $ do
    request methodPost "/addresses"
        [/*("Prefer", "tx=commit"),*/ ("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"[ { "id": 202, "address": "xxx" }, { "id": 203, "address": "yyy" } ]"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 2 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should not exist, either
    get "/addresses?id=eq.202"
      shouldRespondWith
      [text|"[]"|]

  it "raises an error regardless of return=minimal" $ do
    request methodPost "/addresses"
        [/*("Prefer", "tx=commit"),*/ ("Prefer", "return=minimal"), ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"[ { "id": 204, "address": "xxx" }, { "id": 205, "address": "yyy" } ]"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 2 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should not exist, either
    get "/addresses?id=eq.204"
      shouldRespondWith
      [text|"[]"|]

  it "raises an error when creating zero entities" $
    request methodPost "/addresses"
            [("Accept", "application/vnd.pgrst.object+json")]
            [json| r#"[ ]"# |]
      shouldRespondWith
              [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
              { matchStatus  = 406
              , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                                ]
              }

  it "raises an error when creating zero entities with return=rep" $
    request methodPost "/addresses"
            [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
            [json| r#"[ ]"# |]
      shouldRespondWith
              [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
              { matchStatus  = 406
              , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                                ]
              }

describe "when deleting rows" $ do
  it "works for one row with return=rep" $ do
    request methodDelete
      "/items?id=eq.11"
      [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")] ""
    shouldRespondWith [json|r#"{"id":11}"#|]

  it "works for one row with return=minimal" $ do
    request methodDelete
      "/items?id=eq.12"
      [("Prefer", "return=minimal"), ("Accept", "application/vnd.pgrst.object+json")] ""
    shouldRespondWith [text|""|]

  it "raises an error when attempting to delete multiple entities" $ do
    request methodDelete "/items?id=gt.0&id=lt.6"
        [/*("Prefer", "tx=commit"),*/ ("Accept", "application/vnd.pgrst.object+json")]
        ""
      shouldRespondWith
        [json|r#"{"details":"Results contain 5 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should still exist
    get "/items?id=gt.0&id=lt.6&order=id"
      shouldRespondWith
        [json| r#"[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]"# |]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-4/*"]
        }

  it "raises an error when attempting to delete multiple entities with return=rep" $ do
    request methodDelete "/items?id=gt.5&id=lt.11"
        [/*("Prefer", "tx=commit"),*/ ("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
        [json|r#"{"details":"Results contain 5 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- the rows should still exist
    get "/items?id=gt.5&id=lt.11"
      shouldRespondWith [json| r#"[{"id":6},{"id":7},{"id":8},{"id":9},{"id":10}]"# |]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-4/*"]
        }

  it "raises an error when deleting zero entities" $
    request methodDelete "/items?id=lt.0"
            [("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
            [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
            { matchStatus  = 406
            , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                              ]
            }

  it "raises an error when deleting zero entities with return=rep" $
    request methodDelete "/items?id=lt.0"
            [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
            [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
            { matchStatus  = 406
            , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                                    ]
            }

describe "when calling a stored proc" $ do
  it "fails for zero rows" $
    request methodPost "/rpc/getproject"
            [("Accept", "application/vnd.pgrst.object+json")] [json|r#"{ "id": 9999999}"#|]
      shouldRespondWith
            [json|r#"{"details":"Results contain 0 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
            { matchStatus  = 406
            , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
            ]
            }

  //-- this one may be controversial, should vnd.pgrst.object include
  //-- the likes of 2 and "hello?"
  it "succeeds for scalar result" $
    request methodPost "/rpc/sayhello"
      [("Accept", "application/vnd.pgrst.object+json")] [json|r#"{ "name": "world"}"#|]
      shouldRespondWith 200

  it "returns a single object for json proc" $
    request methodPost "/rpc/getproject"
        [("Accept", "application/vnd.pgrst.object+json")] [json|r#"{ "id": 1}"#|]
      shouldRespondWith
        [json|r#"{"id":1,"name":"Windows 7","client_id":1}"#|]
        { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

  it "fails for multiple rows" $
    request methodPost "/rpc/getallprojects"
            [("Accept", "application/vnd.pgrst.object+json")] "{}"
      shouldRespondWith
            [json|r#"{"details":"Results contain 5 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
            { matchStatus  = 406
            , matchHeaders = ["Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
              ]
            }

  it "fails for multiple rows with rolled back changes" $ do
    post "/rpc/getproject?select=id,name"
        [json| r#"{"id": 1}"# |]
      shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7"}]"#|]
    request methodPost "/rpc/setprojects"
        [/*("Prefer", "tx=commit"),*/ ("Accept", "application/vnd.pgrst.object+json")]
        [json| r#"{"id_l": 1, "id_h": 2, "name": "changed"}"# |]
      shouldRespondWith
        [json|r#"{"details":"Results contain 2 rows, application/vnd.pgrst.object+json requires 1 row","message":"JSON object requested, multiple (or no) rows returned"}"#|]
        { matchStatus  = 406
        , matchHeaders = [ "Content-Type" <:> "application/json" //"application/vnd.pgrst.object+json"
                         //, "Preference-Applied" <:> "tx=commit"
                         ]
        }

    //-- should rollback function
    post "/rpc/getproject?select=id,name"
        [json| r#"{"id": 1}"# |]
      shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7"}]"#|]
}
