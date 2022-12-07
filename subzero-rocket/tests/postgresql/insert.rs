use super::super::start;
use super::common::*;
use async_once::AsyncOnce;
use demonstrate::demonstrate;
use rocket::local::asynchronous::Client;
use std::sync::Once;
static INIT_CLIENT: Once = Once::new();
lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "insert"
    describe "disparate json types" $ do
      it "accepts disparate json types" $ do
        post "/menagerie"
          [json| r#"{
            "integer": 13, "double": 3.14159, "varchar": "testing!"
          , "boolean": false, "date": "1900-01-01", "money": "$3.99"
          , "enum": "foo"
          }"# |] shouldRespondWith [text|""|]
          { matchStatus  = 201
            // should not have content type set when body is empty
          // , matchHeaders = [matchHeaderAbsent hContentType]
          }

      it "filters columns in result using &select" $
        request methodPost "/menagerie?select=integer,varchar" [("Prefer", "return=representation")]
          [json| r#"[{
            "integer": 14, "double": 3.14159, "varchar": "testing!"
          , "boolean": false, "date": "1900-01-01", "money": "$3.99"
          , "enum": "foo"
          }]"# |] shouldRespondWith [json|r#"[{"integer":14,"varchar":"testing!"}]"#|]
          { matchStatus  = 201
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "ignores &select when return not set or using return=minimal" $ do
        request methodPost "/menagerie?select=integer,varchar"
            [json| r#"[{
              "integer": 15, "double": 3.14159, "varchar": "testing!",
              "boolean": false, "date": "1900-01-01", "money": "$3.99",
              "enum": "foo"
            }]"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [matchHeaderAbsent hContentType]
            }
        request methodPost "/menagerie?select=integer,varchar"
            [("Prefer", "return=minimal")]
            [json| r#"[{
              "integer": 16, "double": 3.14159, "varchar": "testing!",
              "boolean": false, "date": "1900-01-01", "money": "$3.99",
              "enum": "foo"
            }]"# |]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [matchHeaderAbsent hContentType]
            }

    describe "non uniform json array" $ do
      it "rejects json array that isnt exclusivily composed of objects" $
        post "/articles"
             [json| r#"[{"id": 100, "body": "xxxxx"}, 123, "xxxx", {"id": 111, "body": "xxxx"}]"# |]
        shouldRespondWith
             [json| r#"{"message":"Failed to deserialize json: invalid type: integer `123`, expected a map at line 1 column 34"}"# |]
             { matchStatus  = 400
             , matchHeaders = ["Content-Type" <:> "application/json"]
             }

      it "rejects json array that has objects with different keys" $
        post "/articles"
             [json| r#"[{"id": 100, "body": "xxxxx"}, {"id": 111, "body": "xxxx", "owner": "me"}]"# |]
        shouldRespondWith
             [json| r#"{"message":"All object keys must match"}"# |]
             { matchStatus  = 400
             , matchHeaders = ["Content-Type" <:> "application/json"]
             }

    describe "requesting full representation1" $ do
      it "includes related data after insert" $
        request methodPost "/projects?select=id,name,clients(id,name)"
                [("Prefer", "return=representation, count=exact")]
          [json|r#"{"id":6,"name":"New Project","client_id":2}"#|] shouldRespondWith [json|r#"[{"id":6,"name":"New Project","clients":{"id":2,"name":"Apple"}}]"#|]
          { matchStatus  = 201
          , matchHeaders = [ "Content-Type" <:> "application/json"
                           //, "Location" <:> "/projects?id=eq.6"
                           , "Content-Range" <:> "*/1" ]
          }

      it "can rename and (no)cast the selected columns" $
        request methodPost "/projects?select=pId:id,pName:name,cId:client_id"
                [("Prefer", "return=representation")]
          [json|r#"{"id":7,"name":"New Project","client_id":2}"#|] shouldRespondWith
          [json|r#"[{"pId":7,"pName":"New Project","cId":2}]"#|]
          { matchStatus  = 201
          , matchHeaders = [ "Content-Type" <:> "application/json"
                           //, "Location" <:> "/projects?id=eq.7"
                           , "Content-Range" <:> "*/*" ]
          }

      it "should not throw and return location header when selecting without PK" $
        request methodPost "/projects?select=name,client_id" [("Prefer", "return=representation")]
          [json|r#"{"id":10,"name":"New Project","client_id":2}"#|]
          shouldRespondWith
          [json|r#"[{"name":"New Project","client_id":2}]"#|]
          { matchStatus  = 201
          , matchHeaders = [ "Content-Type" <:> "application/json"
                           //, "Location" <:> "/projects?id=eq.10"
                           , "Content-Range" <:> "*/*" ]
          }

    // describe "requesting headers only representation" $ do
    //   it "should not throw and return location header when selecting without PK" $
    //     request methodPost "/projects?select=name,client_id"
    //         [("Prefer", "return=headers-only")]
    //         [json|r#"{"id":11,"name":"New Project","client_id":2}"#|]
    //       shouldRespondWith
    //         [text|""|]
    //         { matchStatus  = 201
    //         , matchHeaders = [ //"Location" <:> "/projects?id=eq.11"
    //                          //, matchHeaderAbsent hContentType
    //                          "Content-Range" <:> "*/*" ]
    //         }

    //   // when (actualPgVersion >= pgVersion110) $
    //     it "should not throw and return location header for partitioned tables when selecting without PK" $
    //       request methodPost "/car_models"
    //           [("Prefer", "return=headers-only")]
    //           [json|r#"{"name":"Enzo","year":2021}"#|]
    //         shouldRespondWith
    //           [text|""|]
    //           { matchStatus  = 201
    //           , matchHeaders = [ //"Location" <:> "/car_models?name=eq.Enzo&year=eq.2021"
    //                           //  , matchHeaderAbsent hContentType
    //                            "Content-Range" <:> "*/*" ]
    //           }

    describe "requesting no representation2" $
      it "should not throw and return no location header when selecting without PK" $
        request methodPost "/projects?select=name,client_id"
            [json|r#"{"id":12,"name":"New Project","client_id":2}"#|]
          shouldRespondWith
            [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [ matchHeaderAbsent hContentType
            //                  , matchHeaderAbsent hLocation ]
            }

    // describe "from an html form" $
    //   it "accepts disparate json types" $ do
    //     request methodPost "/menagerie"
    //         [("Content-Type", "application/x-www-form-urlencoded")]
    //          ("integer=7&double=2.71828&varchar=forms+are+fun&" <>
    //           "boolean=false&date=1900-01-01&money=$3.99&enum=foo")
    //       shouldRespondWith
    //         ""
    //         { matchStatus = 201
    //         , matchHeaders = [ matchHeaderAbsent hContentType ]
    //         }

    describe "with no pk supplied" $ do
      describe "into a table with auto-incrementing pk" $
        it "succeeds with 201 and location header" $ do
          // -- reset pk sequence first to make test repeatable
          // request methodPost "/rpc/reset_sequence"
          //     [("Prefer", "tx=commit")]
          //     [json|r#"{"name": "auto_incrementing_pk_id_seq", "value": 2}"#|]
          //   shouldRespondWith
          //     [str|""|]

          request methodPost "/auto_incrementing_pk"
              [("Prefer", "return=headers-only")]
              [json| r#"{ "non_nullable_string":"not null"}"# |]
            shouldRespondWith
              [text|""|]
              { matchStatus  = 201
              // , matchHeaders = [ "Location" <:> "/auto_incrementing_pk?id=eq.2"
              //                  //, matchHeaderAbsent hContentType
              //                   ]
              }

      describe "into a table with simple pk" $
        it "fails with 400 and error" $
          post "/simple_pk" [json| r#"{ "extra":"foo"}"# |]
          shouldRespondWith
          // (if actualPgVersion >= pgVersion130 then
            [json|r#"{"code":"23502","message":"null value in column \"k\" of relation \"simple_pk\" violates not-null constraint","details":"Failing row contains (null, foo).","hint":null}"#|]
          //  else
          //   [json|r#"{"hint":null,"details":"Failing row contains (null, foo).","code":"23502","message":"null value in column \"k\" violates not-null constraint"}"#|]
          // )
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      describe "into a table with no pk" $ do
        it "succeeds with 201 but no location header" $ do
          post "/no_pk"
              [json| r#"{ "a":"foo", "b":"bar" }"# |]
            shouldRespondWith
            [text|""|]
              { matchStatus  = 201
              // , matchHeaders = [ matchHeaderAbsent hContentType
              //                  , matchHeaderAbsent hLocation ]
              }

        it "returns full details of inserted record if asked" $ do
          request methodPost "/no_pk"
              [("Prefer", "return=representation")]
              [json| r#"{ "a":"bar", "b":"baz" }"# |]
            shouldRespondWith
              [json| r#"[{ "a":"bar", "b":"baz" }]"# |]
              { matchStatus  = 201
              // , matchHeaders = [matchHeaderAbsent hLocation]
              }

        it "returns empty array when no items inserted, and return=rep" $ do
          request methodPost "/no_pk"
              [("Prefer", "return=representation")]
              [json| r#"[]"# |]
            shouldRespondWith
              [json| r#"[]"# |]
              { matchStatus = 201 }

        it "can post nulls" $ do
          request methodPost "/no_pk"
              [("Prefer", "return=representation")]
              [json| r#"{ "a":null, "b":"foo" }"# |]
            shouldRespondWith
              [json| r#"[{ "a":null, "b":"foo" }]"# |]
              { matchStatus  = 201
              // , matchHeaders = [matchHeaderAbsent hLocation]
              }

    // describe "with compound pk supplied" $
    //   it "builds response location header appropriately" $ do
    //     request methodPost "/compound_pk"
    //         [("Prefer", "return=representation")]
    //         [json| r#"{ "k1":12, "k2":"Rock & R+ll" }"# |]
    //       shouldRespondWith
    //         [json|r#"[ { "k1":12, "k2":"Rock & R+ll", "extra": null } ]"#|]
    //         { matchStatus  = 201
    //         , matchHeaders = [ "Location" <:> "/compound_pk?k1=eq.12&k2=eq.Rock%20%26%20R%2Bll" ]
    //        }

    describe "with bulk insert" $
      it "returns 201 but no location header" $ do
        request methodPost "/compound_pk"
            [json| r#"[ {"k1":21, "k2":"hello world"}
                              , {"k1":22, "k2":"bye for now"}]"#
                            |]
          shouldRespondWith
          [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [ matchHeaderAbsent hContentType
            //                  , matchHeaderAbsent hLocation ]
            }

    describe "with invalid json payload" $
      it "fails with 400 and error" $
        post "/simple_pk" [json|"}{ x = 2"|]
        shouldRespondWith
        [json|r#"{"message":"Failed to parse json body"}"#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    describe "with no payload" $
      it "fails with 400 and error" $
        post "/simple_pk" [json|""|]
        shouldRespondWith
        [json|r#"{"message":"Failed to parse json body"}"#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    describe "with valid json payload" $
      it "succeeds and returns 201 created" $
        post "/simple_pk"
            [json| r#"{ "k":"k1", "extra":"e1" }"# |]
          shouldRespondWith
          [text|""|]
            { matchStatus = 201
            // , matchHeaders = [matchHeaderAbsent hContentType]
            }

    describe "attempting to insert a row with the same primary key" $
      it "fails returning a 409 Conflict" $
        post "/simple_pk"
            [json| r#"{ "k":"xyyx", "extra":"e1" }"# |]
          shouldRespondWith
            [json|r#"{"hint":null,"details":"Key (k)=(xyyx) already exists.","code":"23505","message":"duplicate key value violates unique constraint \"simple_pk_pkey\""}"#|]
            { matchStatus  = 409 }

    describe "attempting to insert a row with conflicting unique constraint" $
      it "fails returning a 409 Conflict" $
        post "/withUnique"  [json| r#"{ "uni":"nodup", "extra":"e2" }"# |] shouldRespondWith 409

    describe "jsonb" $ do
      it "serializes nested object" $ do
        //let inserted = [json| r#"{ "data": { "foo":"bar" } }"# |]
        request methodPost "/json_table"
                     [("Prefer", "return=representation")]
                     [json| r#"{ "data": { "foo":"bar" } }"# |]
          shouldRespondWith [json|r#"[{"data":{"foo":"bar"}}]"#|]
          { matchStatus  = 201
          }

      it "serializes nested array" $ do
        //let inserted = [json| r#"{ "data": [1,2,3] }"# |]
        request methodPost "/json_table"
                     [("Prefer", "return=representation")]
                     [json| r#"{ "data": [1,2,3] }"# |]
          shouldRespondWith [json|r#"[{"data":[1,2,3]}]"#|]
          { matchStatus  = 201
          }

    describe "empty objects" $ do
      it "successfully inserts a row with all-default columns" $ do
        post "/items"
            [json|r#"{}"#|]
          shouldRespondWith
          [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [matchHeaderAbsent hContentType]
            }
        post "/items" [json|r#"[{}]"#|] shouldRespondWith [text|""|]
          { matchStatus  = 201
          , matchHeaders = []
          }

      it "successfully inserts two rows with all-default columns" $
        post "/items"
            [json|r#"[{}, {}]"#|]
          shouldRespondWith
          [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [matchHeaderAbsent hContentType]
            }

      it "successfully inserts a row with all-default columns with prefer=rep" $ do
        // -- reset pk sequence first to make test repeatable
        // request methodPost "/rpc/reset_sequence"
        //     [("Prefer", "tx=commit")]
        //     [json|r#"{"name": "items_id_seq", "value": 20}"#|]
        //   shouldRespondWith
        //     [json|""|]

        request methodPost "/items"
            [("Prefer", "return=representation")]
            [json|r#"{}"#|]
          shouldRespondWith
            [json|r#"[{ "id": 18 }]"#|]
            { matchStatus  = 201 }

      it "successfully inserts a row with all-default columns with prefer=rep and &select=" $ do
        // -- reset pk sequence first to make test repeatable
        // request methodPost "/rpc/reset_sequence"
        //     [("Prefer", "tx=commit")]
        //     [json|r#"{"name": "items_id_seq", "value": 20}"#|]
        //   shouldRespondWith
        //     [json|""|]

        request methodPost "/items?select=id"
            [("Prefer", "return=representation")]
            [json|r#"{}"#|]
          shouldRespondWith [json|r#"[{ "id": 19 }]"#|]
            { matchStatus  = 201 }

    describe "POST with ?columns parameter" $ do
      it "ignores json keys not included in ?columns" $ do
        request methodPost "/articles?columns=id,body" [("Prefer", "return=representation")]
          [json| r#"{"id": 200, "body": "xxx", "smth": "here", "other": "stuff", "fake_id": 13}"# |] shouldRespondWith
          [json|r#"[{"id": 200, "body": "xxx", "owner": "postgrest_test_anonymous"}]"#|]
          { matchStatus  = 201
          , matchHeaders = [] }
        request methodPost "/articles?columns=id,body&select=id,body" [("Prefer", "return=representation")]
          [json| r#"[
            {"id": 201, "body": "yyy", "smth": "here", "other": "stuff", "fake_id": 13},
            {"id": 202, "body": "zzz", "garbage": "%%$&", "kkk": "jjj"},
            {"id": 203, "body": "aaa", "hey": "ho"} ]"# |] shouldRespondWith
          [json|r#"[
            {"id": 201, "body": "yyy"},
            {"id": 202, "body": "zzz"},
            {"id": 203, "body": "aaa"} ]"#|]
          { matchStatus  = 201
          , matchHeaders = [] }

      // -- TODO parse columns error message needs to be improved
      it "disallows blank ?columns" $
        post "/articles?columns="
          [json|r#"[
            {"id": 204, "body": "yyy"},
            {"id": 205, "body": "zzz"}]"#|]
          shouldRespondWith
          [json| r#"{"message":"\"failed to parse columns parameter ()\" (line 1, column 0)","details":"0: in IsA, got empty input\n\n1: in Alt, got empty input\n\n2: in Many1, got empty input\n\n3: in Alt, got empty input\n\n4: in failed to parse columns parameter, got empty input\n\n"}"# |]
          { matchStatus  = 400
          , matchHeaders = []
          }

      it "disallows array elements that are not json objects" $
        post "/articles?columns=id,body"
          [json|r#"[
            {"id": 204, "body": "yyy"},
            333,
            "asdf",
            {"id": 205, "body": "zzz"}]"#|] shouldRespondWith
          [json|r#"{
              "code": "22023",
              "details": null,
              "hint": null,
              "message": "argument of json_populate_recordset must be an array of objects"}"#|]
          { matchStatus  = 400
          , matchHeaders = []
          }

  describe "CSV insert" $ do
    // describe "disparate csv types" $
    //   it "succeeds with multipart response" $ do
    //     pendingWith "Decide on what to do with CSV insert"
    //     let inserted = [str|integer,double,varchar,boolean,date,money,enum
    //         |13,3.14159,testing!,false,1900-01-01,$3.99,foo
    //         |12,0.1,a string,true,1929-10-01,12,bar
    //         |]
    //     request methodPost "/menagerie" [("Content-Type", "text/csv"), ("Accept", "text/csv"), ("Prefer", "return=representation")] inserted
    //        shouldRespondWith ResponseMatcher
    //        { matchStatus  = 201
    //        , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
    //        , matchBody = bodyEquals inserted
    //        }

    describe "requesting full representation2" $ do
      it "returns full details of inserted record" $
        request methodPost "/no_pk"
                     [("Content-Type", "text/csv"), ("Accept", "text/csv"),  ("Prefer", "return=representation")]
                     "a,b\nbar,baz"
          shouldRespondWith [text|"a,b\nbar,baz"|]
          { matchStatus  = 201
          , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
          }

      it "can post nulls" $
        request methodPost "/no_pk"
                     [("Content-Type", "text/csv"), ("Accept", "text/csv"), ("Prefer", "return=representation")]
                     "a,b\nNULL,foo"
          shouldRespondWith [text|"a,b\n,foo"|]
          { matchStatus  = 201
          , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
          }

      it "only returns the requested column header with its associated data" $
        request methodPost "/projects?select=id"
                     [("Content-Type", "text/csv"), ("Accept", "text/csv"), ("Prefer", "return=representation")]
                     "id,name,client_id\n8,Xenix,1\n9,Windows NT,1"
          shouldRespondWith [text|"id\n8\n9"|]
          { matchStatus  = 201
          , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8",
                            "Content-Range" <:> "*/*"]
          }

    describe "with wrong number of columns" $
      it "fails for too few" $
        request methodPost "/no_pk" [("Content-Type", "text/csv")] "a,b\nfoo,bar\nbaz"
        shouldRespondWith
        [json|r#"{"message":"Failed to deserialize csv: CSV error: record 2 (line: 3, byte: 12): found record with 1 fields, but the previous record has 2 fields"}"#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    describe "with unicode values" $
      it "succeeds and returns usable location header" $ do
        request methodPost "/simple_pk2?select=extra,k"
            [("Prefer", "tx=commit"), ("Prefer", "return=representation")]
            [json| r#"{ "k":"圍棋", "extra":"￥" }"# |]
          shouldRespondWith
          [json|r#"[ { "k":"圍棋", "extra":"￥" } ]"#|]
          { matchStatus = 201 }

        // let Just location = lookup hLocation $ simpleHeaders p
        // get location
        //   shouldRespondWith
        //     [json|r#"[ { "k":"圍棋", "extra":"￥" } ]"#|]

        // request methodDelete location
        //     [("Prefer", "tx=commit")]
        //     ""
        //   shouldRespondWith
        //     ""
        //     { matchStatus = 204
        //     , matchHeaders = [matchHeaderAbsent hContentType]
        //     }

  describe "Row level permission" $
    it "set user_id when inserting rows" $ do
      request methodPost "/authors_only"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqZG9lIn0.B-lReuGNDwAlU1GOC476MlO0vAt9JNoHIlxg2vwMaO0", ("Prefer", "return=representation") ]
          [json| r#"{ "secret": "nyancat" }"# |]
        shouldRespondWith
          [json|r#"[{"owner":"jdoe","secret":"nyancat"}]"#|]
          { matchStatus  = 201 }

      request methodPost "/authors_only"
          // -- jwt token for jroe
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIiwiaWQiOiJqcm9lIn0.2e7mx0U4uDcInlbJVOBGlrRufwqWLINDIEDC1vS0nw8", ("Prefer", "return=representation") ]
          [json| r#"{ "secret": "lolcat", "owner": "hacker" }"# |]
        shouldRespondWith
          [json|r#"[{"owner":"jroe","secret":"lolcat"}]"#|]
          { matchStatus  = 201 }

  describe "tables with self reference foreign keys" $ do
    it "embeds parent after insert" $
      request methodPost "/web_content?select=id,name,parent_content:p_web_id(name)"
              [("Prefer", "return=representation")]
        [json|r#"{"id":6, "name":"wot", "p_web_id":4}"#|]
        shouldRespondWith
        [json|r#"[{"id":6,"name":"wot","parent_content":{"name":"wut"}}]"#|]
        { matchStatus  = 201
        , matchHeaders = [
          "Content-Type" <:> "application/json"
          //, "Location" <:> "/web_content?id=eq.6"
          ]
        }

  describe "table with limited privileges" $ do
    it "succeeds inserting if correct select is applied" $
      request methodPost "/limited_article_stars?select=article_id,user_id" [("Prefer", "return=representation")]
        [json| r#"{"article_id": 2, "user_id": 1}"# |] shouldRespondWith [json|r#"[{"article_id":2,"user_id":1}]"#|]
        { matchStatus  = 201
        , matchHeaders = []
        }

    it "fails inserting if more columns are selected" $
      request methodPost "/limited_article_stars?select=article_id,user_id,created_at" [("Prefer", "return=representation")]
        [json| r#"{"article_id": 2, "user_id": 2}"# |]
        shouldRespondWith
      // if actualPgVersion >= pgVersion112 then
      [json|r#"{"hint":null,"details":null,"code":"42501","message":"permission denied for view limited_article_stars"}"#|]
      //    else
      // [json|r#"{"hint":null,"details":null,"code":"42501","message":"permission denied for relation limited_article_stars"}"#|]
      //                                                                 )
      //   { matchStatus  = 401
      //   , matchHeaders = []
      //   }

    it "fails inserting if select is not specified" $
      request methodPost "/limited_article_stars" [("Prefer", "return=representation")]
        [json| r#"{"article_id": 3, "user_id": 1}"# |] shouldRespondWith
      //   (
      // if actualPgVersion >= pgVersion112 then
      [json|r#"{"hint":null,"details":null,"code":"42501","message":"permission denied for view limited_article_stars"}"#|]
      //    else
      // [json|r#"{"hint":null,"details":null,"code":"42501","message":"permission denied for relation limited_article_stars"}"#|]
      //                                                                 )
        { matchStatus  = 401
        , matchHeaders = []
        }

    it "can insert in a table with no select and return=minimal" $ do
      request methodPost "/insertonly"
          [("Prefer", "return=minimal")]
          [json| r#"{ "v":"some value" }"# |]
        shouldRespondWith
        [text|""|]
          { matchStatus = 201
          // , matchHeaders = [matchHeaderAbsent hContentType]
          }

  describe "Inserting into VIEWs" $ do
    describe "requesting no representation1" $
      it "succeeds with 201" $
        post "/compound_pk_view"
            [json|r#"{"k1":1,"k2":"test","extra":2}"#|]
          shouldRespondWith
          [text|""|]
            { matchStatus  = 201
            // , matchHeaders = [ matchHeaderAbsent hContentType
            //                  , matchHeaderAbsent hLocation ]
            }

    // describe "requesting header only representation" $ do
    //   it "returns a location header" $
    //     request methodPost "/compound_pk_view" [("Prefer", "return=headers-only")]
    //         [json|r#"{"k1":1,"k2":"test","extra":2}"#|]
    //       shouldRespondWith
    //         [text|""|]
    //         { matchStatus  = 201
    //         , matchHeaders = [ matchHeaderAbsent hContentType
    //                          , "Location" <:> "/compound_pk_view?k1=eq.1&k2=eq.test"
    //                          , "Content-Range" <:> "*/**/" ]
    //         }

    //   it "should not throw and return location header when a PK is null" $
    //     request methodPost "/test_null_pk_competitors_sponsors" [("Prefer", "return=headers-only")]
    //         [json|r#"{"id":1}"#|]
    //       shouldRespondWith
    //         ""
    //         { matchStatus  = 201
    //         , matchHeaders = [ matchHeaderAbsent hContentType
    //                          , "Location" <:> "/test_null_pk_competitors_sponsors?id=eq.1&sponsor_id=is.null"
    //                          , "Content-Range" <:> "*/**/" ]
    //         }
}
