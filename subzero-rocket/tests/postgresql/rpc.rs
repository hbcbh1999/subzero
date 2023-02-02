use super::setup::*;

haskell_test! {
feature "rpc"
  describe "a proc that returns a set" $ do
    it "returns proper json" $ do
      post "/rpc/getitemrange" [json| r#"{ "min": 2, "max": 4 }"# |] shouldRespondWith
        [json| r#"[ {"id": 3}, {"id":4} ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/rpc/getitemrange?min=2&max=4" shouldRespondWith
        [json| r#"[ {"id": 3}, {"id":4} ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "returns CSV" $ do
        request methodPost "/rpc/getitemrange"
                (acceptHdrs "text/csv")
                [json| r#"{ "min": 2, "max": 4 }"# |]
           shouldRespondWith [text|"id\n3\n4"|]
            { matchStatus = 200
            , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
            }
        request methodGet "/rpc/getitemrange?min=2&max=4"
                (acceptHdrs "text/csv") ""
           shouldRespondWith [text|"id\n3\n4"|]
            { matchStatus = 200
            , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
            }
  describe "unknown function" $ do
    it "returns 404" $
      post "/rpc/fakefunc" [json| r#"{}"# |] shouldRespondWith 404

    it "should fail with 404 on unknown proc name" $
      get "/rpc/fake" shouldRespondWith 404

    it "should fail with 404 on unknown proc args" $ do
      get "/rpc/sayhello" shouldRespondWith 404
      get "/rpc/sayhello?any_arg=value" shouldRespondWith 404

    it "should not ignore unknown args and fail with 404" $
      get "/rpc/add_them?a=1&b=2&smthelse=blabla" shouldRespondWith
      [json| r#"{
        "hint":"If a new function was created in the database with this name and parameters, try reloading the schema cache.",
        "message":"Could not find the test.add_them(a, b, smthelse) function in the schema cache" }"# |]
      { matchStatus  = 404
      , matchHeaders = ["Content-Type" <:> "application/json"]
      }

    it "should fail with 404 when no json arg is found with prefer single object" $
      // request methodPost "/rpc/sayhello"
      //   [("Prefer","params=single-object")]
      //   [json|"{}"|]
      // shouldRespondWith
      //   [json| r#"{
      //     "hint":"If a new function was created in the database with this name and parameters, try reloading the schema cache.",
      //     "message":"Could not find the test.sayhello function with a single json or jsonb parameter in the schema cache" }"# |]
      // { matchStatus  = 404
      // , matchHeaders = ["Content-Type" <:> "application/json"]
      // }

    // it "should fail with 404 for overloaded functions with unknown args" $ do
    //   get "/rpc/overloaded?wrong_arg=value" shouldRespondWith
    //     [json| r#"{
    //       "hint":"If a new function was created in the database with this name and parameters, try reloading the schema cache.",
    //       "message":"Could not find the test.overloaded(wrong_arg) function in the schema cache" }"# |]
    //     { matchStatus  = 404
    //     , matchHeaders = ["Content-Type" <:> "application/json"]
    //     }
    //   get "/rpc/overloaded?a=1&b=2&wrong_arg=value" shouldRespondWith
    //     [json| r#"{
    //       "hint":"If a new function was created in the database with this name and parameters, try reloading the schema cache.",
    //       "message":"Could not find the test.overloaded(a, b, wrong_arg) function in the schema cache" }"# |]
    //     { matchStatus  = 404
    //     , matchHeaders = ["Content-Type" <:> "application/json"]
    //     }

    it "works when having uppercase identifiers" $ do
        get "/rpc/quotedFunction?user=mscott&fullName=Michael Scott&SSN=401-32-XXXX" shouldRespondWith
          [json|r#"{"user": "mscott", "fullName": "Michael Scott", "SSN": "401-32-XXXX"}"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        post "/rpc/quotedFunction"
          [json|r#"{"user": "dschrute", "fullName": "Dwight Schrute", "SSN": "030-18-XXXX"}"#|]
          shouldRespondWith
          [json|r#"{"user": "dschrute", "fullName": "Dwight Schrute", "SSN": "030-18-XXXX"}"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }


        describe "shaping the response returned by a proc" $ do
          it "returns a project" $ do
            post "/rpc/getproject" [json| r#"{ "id": 1}"# |] shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7","client_id":1}]"#|]
            get "/rpc/getproject?id=1" shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7","client_id":1}]"#|]

          it "can filter proc results" $ do
            post "/rpc/getallprojects?id=gt.1&id=lt.5&select=id" [json| r#"{}"# |] shouldRespondWith
              [json|r#"[{"id":2},{"id":3},{"id":4}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
            get "/rpc/getallprojects?id=gt.1&id=lt.5&select=id" shouldRespondWith
              [json|r#"[{"id":2},{"id":3},{"id":4}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can limit proc results" $ do
            post "/rpc/getallprojects?id=gt.1&id=lt.5&select=id&limit=2&offset=1" [json| r#"{}"# |]
              shouldRespondWith [json|r#"[{"id":3},{"id":4}]"#|]
                 { matchStatus = 200
                 //, matchHeaders = ["Content-Range" <:> "1-2/*"]
                 }
            get "/rpc/getallprojects?id=gt.1&id=lt.5&select=id&limit=2&offset=1"
              shouldRespondWith [json|r#"[{"id":3},{"id":4}]"#|]
                 { matchStatus = 200
                 //, matchHeaders = ["Content-Range" <:> "1-2/*"]
                 }

          it "select works on the first level" $ do
            post "/rpc/getproject?select=id,name" [json| r#"{ "id": 1}"# |] shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7"}]"#|]
            get "/rpc/getproject?id=1&select=id,name" shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7"}]"#|]

        describe "foreign entities embedding" $ do
          it "can embed if related tables are in the exposed schema" $ do
            post "/rpc/getproject?select=id,name,client:clients(id),tasks(id)" [json| r#"{ "id": 1}"# |] shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7","client":{"id":1},"tasks":[{"id":1},{"id":2}]}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
            get "/rpc/getproject?id=1&select=id,name,client:clients(id),tasks(id)" shouldRespondWith
              [json|r#"[{"id":1,"name":"Windows 7","client":{"id":1},"tasks":[{"id":1},{"id":2}]}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "cannot embed if the related table is not in the exposed schema" $ do
            post "/rpc/single_article?select=*,article_stars(*)" [json|r#"{ "id": 1}"#|]
              shouldRespondWith 400
            get "/rpc/single_article?id=1&select=*,article_stars(*)"
              shouldRespondWith 400

          it "can embed if the related tables are in a hidden schema but exposed as views" $ do
            post "/rpc/single_article?select=id,articleStars(userId)"
                [json|r#"{ "id": 2}"#|]
              shouldRespondWith
                [json|r#"{"id": 2, "articleStars": [{"userId": 3}]}"#|]
            get "/rpc/single_article?id=2&select=id,articleStars(userId)"
              shouldRespondWith
                [json|r#"{"id": 2, "articleStars": [{"userId": 3}]}"#|]

          it "can embed an M2M relationship table" $
            get "/rpc/getallusers?select=name,tasks(name)&id=gt.1"
              shouldRespondWith [json|r#"[
                {"name":"Michael Scott", "tasks":[{"name":"Design IOS"}, {"name":"Code IOS"}, {"name":"Design OSX"}]},
                {"name":"Dwight Schrute","tasks":[{"name":"Design w7"}, {"name":"Design IOS"}]}
              ]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can embed an M2M relationship table that has a parent relationship table" $
            get "/rpc/getallusers?select=name,tasks(name,project:projects(name))&id=gt.1"
              shouldRespondWith [json|r#"[
                {"name":"Michael Scott","tasks":[
                  {"name":"Design IOS","project":{"name":"IOS"}},
                  {"name":"Code IOS","project":{"name":"IOS"}},
                  {"name":"Design OSX","project":{"name":"OSX"}}
                ]},
                {"name":"Dwight Schrute","tasks":[
                  {"name":"Design w7","project":{"name":"Windows 7"}},
                  {"name":"Design IOS","project":{"name":"IOS"}}
                ]}
              ]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          // when (actualPgVersion >= pgVersion110) $
            it "can embed if rpc returns domain of table type" $ do
              post "/rpc/getproject_domain?select=id,name,client:clients(id),tasks(id)"
                  [json| r#"{ "id": 1}"# |]
                shouldRespondWith
                  [json|r#"[{"id":1,"name":"Windows 7","client":{"id":1},"tasks":[{"id":1},{"id":2}]}]"#|]
              get "/rpc/getproject_domain?id=1&select=id,name,client:clients(id),tasks(id)"
                shouldRespondWith
                  [json|r#"[{"id":1,"name":"Windows 7","client":{"id":1},"tasks":[{"id":1},{"id":2}]}]"#|]

        describe "a proc that returns an empty rowset" $
          it "returns empty json array" $ do
            post "/rpc/test_empty_rowset" [json| r#"{}"# |] shouldRespondWith
              [json| r#"[]"# |]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
            get "/rpc/test_empty_rowset" shouldRespondWith
              [json| r#"[]"# |]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

        describe "proc return types" $ do
          describe "returns text" $ do
            it "returns proper json" $
              post "/rpc/sayhello" [json| r#"{ "name": "world" }"# |] shouldRespondWith
                [str|"Hello, world"|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

            it "can handle unicode" $
              post "/rpc/sayhello" [json| r#"{ "name": "￥" }"# |] shouldRespondWith
                [str|"Hello, ￥"|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns array" $
            post "/rpc/ret_array" [json|r#"{}"#|] shouldRespondWith
              [json|r#"[1, 2, 3]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns setof integers" $
            post "/rpc/ret_setof_integers"
                [json|r#"{}"#|]
              shouldRespondWith
                [json|r#"[1,2,3]"#|]

          it "returns enum value" $
            post "/rpc/ret_enum" [json|r#"{ "val": "foo" }"#|] shouldRespondWith
              [str|"foo"|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns domain value" $
            post "/rpc/ret_domain" [json|r#"{ "val": "8" }"#|] shouldRespondWith
              [json|"8"|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns range" $
            post "/rpc/ret_range" [json|r#"{ "low": 10, "up": 20 }"#|] shouldRespondWith
              [str|"[10,20)"|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns row of scalars" $
            post "/rpc/ret_scalars" [json|r#"{}"#|] shouldRespondWith
              [json|r#"[{"a":"scalars", "b":"foo", "c":1, "d":"[10,20)"}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "returns composite type in exposed schema" $
            post "/rpc/ret_point_2d"
                [json|r#"{}"#|]
              shouldRespondWith
                [json|r#"{"x": 10, "y": 5}"#|]

          it "cannot return composite type in hidden schema" $
            post "/rpc/ret_point_3d" [json|r#"{}"#|] shouldRespondWith 401

          // when (actualPgVersion >= pgVersion110) $
            it "returns domain of composite type" $
              post "/rpc/ret_composite_domain"
                  [json|r#"{}"#|]
                shouldRespondWith
                  [json|r#"{"x": 10, "y": 5}"#|]

          it "returns single row from table" $
            post "/rpc/single_article?select=id"
                [json|r#"{"id": 2}"#|]
              shouldRespondWith
                [json|r#"{"id": 2}"#|]

          it "returns null for void" $
            post "/rpc/ret_void"
                [json|r#"{}"#|]
              shouldRespondWith
                [json|"null"|]

          it "returns null for an integer with null value" $
            post "/rpc/ret_null"
                [json|r#"{}"#|]
              shouldRespondWith
                [text|"null"|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

          // describe "different types when overloaded" $ do
          //   it "returns composite type" $
          //     post "/rpc/ret_point_overloaded"
          //         [json|r#"{"x": 1, "y": 2}"#|]
          //       shouldRespondWith
          //         [json|r#"{"x": 1, "y": 2}"#|]

          //   it "returns json scalar with prefer single object" $
          //     request methodPost "/rpc/ret_point_overloaded" [("Prefer","params=single-object")]
          //       [json|r#"{"x": 1, "y": 2}"#|]
          //       shouldRespondWith
          //       [json|r#"{"x": 1, "y": 2}"#|]
          //       { matchHeaders = ["Content-Type" <:> "application/json"] }

        describe "proc argument types" $ do
          // -- different syntax for array needed for pg<10
          // when (actualPgVersion < pgVersion100) $
          //   it "accepts a variety of arguments (Postgres < 10)" $
          //     post "/rpc/varied_arguments"
          //         [json| r#"{
          //           "double": 3.1,
          //           "varchar": "hello",
          //           "boolean": true,
          //           "date": "20190101",
          //           "money": 0,
          //           "enum": "foo",
          //           "arr": "{a,b,c}",
          //           "integer": 43,
          //           "json": {"some key": "some value"},
          //           "jsonb": {"another key": [1, 2, "3"]}
          //         }"# |]
          //       shouldRespondWith
          //         [json| r#"{
          //           "double": 3.1,
          //           "varchar": "hello",
          //           "boolean": true,
          //           "date": "2019-01-01",
          //           "money": "$0.00",
          //           "enum": "foo",
          //           "arr": ["a", "b", "c"],
          //           "integer": 43,
          //           "json": {"some key": "some value"},
          //           "jsonb": {"another key": [1, 2, "3"]}
          //         }"# |]
          //         { matchHeaders = ["Content-Type" <:> "application/json"] }

          // when (actualPgVersion >= pgVersion100) $
            it "accepts a variety of arguments (Postgres >= 10)" $
              post "/rpc/varied_arguments"
                  [json| r#"{
                    "double": 3.1,
                    "varchar": "hello",
                    "boolean": true,
                    "date": "20190101",
                    "money": 0,
                    "enum": "foo",
                    "arr": ["a", "b", "c"],
                    "integer": 43,
                    "json": {"some key": "some value"},
                    "jsonb": {"another key": [1, 2, "3"]}
                  }"# |]
                shouldRespondWith
                  [json| r#"{
                    "double": 3.1,
                    "varchar": "hello",
                    "boolean": true,
                    "date": "2019-01-01",
                    "money": "$0.00",
                    "enum": "foo",
                    "arr": ["a", "b", "c"],
                    "integer": 43,
                    "json": {"some key": "some value"},
                    "jsonb": {"another key": [1, 2, "3"]}
                  }"# |]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "accepts a variety of arguments with GET" $
            // -- without JSON / JSONB here, because passing those via query string is useless - they just become a "json string" all the time
            get "/rpc/varied_arguments?double=3.1&varchar=hello&boolean=true&date=20190101&money=0&enum=foo&arr=%7Ba,b,c%7D&integer=43"
              shouldRespondWith
                  [json| r#"{
                    "double": 3.1,
                    "varchar": "hello",
                    "boolean": true,
                    "date": "2019-01-01",
                    "money": "$0.00",
                    "enum": "foo",
                    "arr": ["a", "b", "c"],
                    "integer": 43,
                    "json": {},
                    "jsonb": {}
                  }"# |]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

          // it "accepts a variety of arguments from an html form" $
          //   request methodPost "/rpc/varied_arguments"
          //       [("Content-Type", "application/x-www-form-urlencoded")]
          //       "double=3.1&varchar=hello&boolean=true&date=20190101&money=0&enum=foo&arr=%7Ba,b,c%7D&integer=43"
          //     shouldRespondWith
          //         [json| r#"{
          //           "double": 3.1,
          //           "varchar": "hello",
          //           "boolean": true,
          //           "date": "2019-01-01",
          //           "money": "$0.00",
          //           "enum": "foo",
          //           "arr": ["a", "b", "c"],
          //           "integer": 43,
          //           "json": {},
          //           "jsonb": {}
          //         }"# |]
          //       { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "parses embedded JSON arguments as JSON" $
            post "/rpc/json_argument"
                [json| r#"{ "arg": { "key": 3 } }"# |]
              shouldRespondWith
                [str|"object"|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

          // when (actualPgVersion < pgVersion100) $
          //   it "parses quoted JSON arguments as JSON (Postgres < 10)" $
          //     post "/rpc/json_argument"
          //         [json| r#"{ "arg": "{ \"key\": 3 }" }"# |]
          //       shouldRespondWith
          //         [json|"object"|]
          //         { matchHeaders = ["Content-Type" <:> "application/json"] }

          // when ((actualPgVersion >= pgVersion109 && actualPgVersion < pgVersion110)
          //       || actualPgVersion >= pgVersion114) $
          //   it "parses quoted JSON arguments as JSON string (from Postgres 10.9, 11.4)" $
          //     post "/rpc/json_argument"
          //         [json| r#"{ "arg": "{ \"key\": 3 }" }"# |]
          //       shouldRespondWith
          //         [json|"string"|]
          //         { matchHeaders = ["Content-Type" <:> "application/json"] }

        describe "improper input" $ do
          it "rejects unknown content type even if payload is good" $ do
            request methodPost "/rpc/sayhello"
              (acceptHdrs "audio/mpeg3") [json| r#"{ "name": "world" }"# |]
                shouldRespondWith 415
            request methodGet "/rpc/sayhello?name=world"
              (acceptHdrs "audio/mpeg3") ""
                shouldRespondWith 415
          // it "rejects malformed json payload" $ do
          //   p <- request methodPost "/rpc/sayhello"
          //     (acceptHdrs "application/json") "sdfsdf"
          //   liftIO $ do
          //     simpleStatus p `shouldBe` badRequest400
          //     isErrorFormat (simpleBody p) `shouldBe` True
          // it "treats simple plpgsql raise as invalid input" $ do
          //   p <- post "/rpc/problem" "{}"
          //   liftIO $ do
          //     simpleStatus p `shouldBe` badRequest400
          //     isErrorFormat (simpleBody p) `shouldBe` True
          // it "treats plpgsql assert as internal server error" $ do
          //   p <- post "/rpc/assert" "{}"
          //   liftIO $ do
          //     simpleStatus p `shouldBe` internalServerError500
          //     isErrorFormat (simpleBody p) `shouldBe` True

        // describe "unsupported verbs" $ do
        //   it "DELETE fails" $
        //     request methodDelete "/rpc/sayhello" [] ""
        //       shouldRespondWith
        //       [json|r#"{"message":"Bad Request"}"#|]
        //       { matchStatus  = 405
        //       , matchHeaders = ["Content-Type" <:> "application/json"]
        //       }
        //   it "PATCH fails" $
        //     request methodPatch "/rpc/sayhello" [] ""
        //       shouldRespondWith 405
        //   it "OPTIONS fails" $
        //     -- TODO: should return info about the function
        //     request methodOptions "/rpc/sayhello" [] ""
        //       shouldRespondWith 405

        // it "executes the proc exactly once per request" $ do
        //   // -- callcounter is persistent even with rollback, because it uses a sequence
        //   // -- reset counter first to make test repeatable
        //   request methodPost "/rpc/reset_sequence"
        //       [("Prefer", "tx=commit")]
        //       [json|r#"{"name": "callcounter_count", "value": 1}"#|]
        //     shouldRespondWith
        //       [json|""|]

        //   -- now the test
        //   post "/rpc/callcounter"
        //       [json|r#"{}"#|]
        //     shouldRespondWith
        //       [json|1|]

        //   post "/rpc/callcounter"
        //       [json|r#"{}"#|]
        //     shouldRespondWith
        //       [json|2|]

        describe "a proc that receives no parameters" $ do
          it "interprets empty string as empty json object on a post request" $
            post "/rpc/noparamsproc" [json|""|] shouldRespondWith
              [text| r#""Return value of no parameters procedure.""# |]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
          it "interprets empty string as a function with no args on a get request" $
            get "/rpc/noparamsproc" shouldRespondWith
              [text| r#""Return value of no parameters procedure.""# |]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "returns proper output when having the same return col name as the proc name" $ do
          post "/rpc/test" [json|r#"{}"#|] shouldRespondWith
            [json|r#"[{"test":"hello","value":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/rpc/test" shouldRespondWith
            [json|r#"[{"test":"hello","value":1}]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

        describe "procs with OUT/INOUT params" $ do
          it "returns an object result when there is a single OUT param" $ do
            get "/rpc/single_out_param?num=5"
              shouldRespondWith
                [json|r#"{"num_plus_one":6}"#|]

            get "/rpc/single_json_out_param?a=1&b=two"
              shouldRespondWith
                [json|r#"{"my_json": {"a": 1, "b": "two"}}"#|]

          it "returns an object result when there is a single INOUT param" $
            get "/rpc/single_inout_param?num=2"
              shouldRespondWith
                [json|r#"{"num":3}"#|]

          it "returns an object result when there are many OUT params" $
            get "/rpc/many_out_params"
              shouldRespondWith
                [json|r#"{"my_json":{"a": 1, "b": "two"},"num":3,"str":"four"}"#|]

          it "returns an object result when there are many INOUT params" $
            get "/rpc/many_inout_params?num=1&str=two&b=false"
              shouldRespondWith
                [json|r#"{"num":1,"str":"two","b":false}"#|]

        describe "procs with TABLE return" $ do
          it "returns an object result when there is a single-column TABLE return type" $
            get "/rpc/single_column_table_return"
              shouldRespondWith
                [json|r#"[{"a": "A"}]"#|]

          it "returns an object result when there is a multi-column TABLE return type" $
            get "/rpc/multi_column_table_return"
              shouldRespondWith
                [json|r#"[{"a": "A", "b": "B"}]"#|]

        describe "procs with VARIADIC params" $ do
          // when (actualPgVersion < pgVersion100) $
          //   it "works with POST (Postgres < 10)" $
          //     post "/rpc/variadic_param"
          //         [json| r#"{ "v": "{hi,hello,there}" }"# |]
          //       shouldRespondWith
          //         [json|r#"["hi", "hello", "there"]"#|]

          // when (actualPgVersion >= pgVersion100) $ do
            it "works with POST (Postgres >= 10)" $
              post "/rpc/variadic_param"
                  [json| r#"{ "v": ["hi", "hello", "there"] }"# |]
                shouldRespondWith
                  [json|r#"["hi", "hello", "there"]"#|]

            describe "works with GET and repeated params" $ do
              it "n=0 (through DEFAULT)" $
                get "/rpc/variadic_param"
                  shouldRespondWith
                    [json|r#"[]"#|]

              it "n equal 1" $
                get "/rpc/variadic_param?v=hi"
                  shouldRespondWith
                    [json|r#"["hi"]"#|]

              it "n bigger one" $
                get "/rpc/variadic_param?v=hi&v=there"
                  shouldRespondWith
                    [json|r#"["hi", "there"]"#|]

            // describe "works with POST and repeated params from html form" $ do
              // it "n=0 (through DEFAULT)" $
              //   request methodPost "/rpc/variadic_param"
              //       [("Content-Type", "application/x-www-form-urlencoded")]
              //       ""
              //     shouldRespondWith
              //       [json|r#"[]"#|]

              // it "n=1" $
              //   request methodPost "/rpc/variadic_param"
              //       [("Content-Type", "application/x-www-form-urlencoded")]
              //       "v=hi"
              //     shouldRespondWith
              //       [json|r#"["hi"]"#|]

              // it "n>1" $
              //   request methodPost "/rpc/variadic_param"
              //       [("Content-Type", "application/x-www-form-urlencoded")]
              //       "v=hi&v=there"
              //     shouldRespondWith
              //       [json|r#"["hi", "there"]"#|]

        it "returns last value for repeated params without VARIADIC" $
          get "/rpc/sayhello?name=ignored&name=world"
            shouldRespondWith
              [json|r#""Hello, world""#|]

        // when (actualPgVersion >= pgVersion100) $
          it "returns last value for repeated non-variadic params in function with other VARIADIC arguments" $
            get "/rpc/sayhello_variadic?name=ignored&name=world&v=unused"
              shouldRespondWith
                [json|"\"Hello, world\""|]

        it "can handle procs with args that have a DEFAULT value" $ do
          get "/rpc/many_inout_params?num=1&str=two"
            shouldRespondWith
              [json| r#"{"num":1,"str":"two","b":true}"#|]
          get "/rpc/three_defaults?b=4"
            shouldRespondWith
              [json|"8"|]

        it "can map a RAISE error code and message to a http status" $
          get "/rpc/raise_pt402"
            shouldRespondWith [json|r#"{ "hint": "Upgrade your plan", "details": "Quota exceeded" }"#|]
            { matchStatus  = 402
            , matchHeaders = ["Content-Type" <:> "application/json"]
            }

        it "defaults to status 500 if RAISE code is PT not followed by a number" $
          get "/rpc/raise_bad_pt"
            shouldRespondWith
            [json|r#"{"hint": null, "details": null}"#|]
            { matchStatus  = 500
            , matchHeaders = [ "Content-Type" <:> "application/json" ]
            }

      // describe "expects a single json object" $ do
      //   it "does not expand posted json into parameters" $
      //     request methodPost "/rpc/singlejsonparam"
      //       [("prefer","params=single-object")] [json| r#"{ "p1": 1, "p2": "text", "p3" : {"obj":"text"} }"# |] shouldRespondWith
      //       [json| r#"{ "p1": 1, "p2": "text", "p3" : {"obj":"text"} }"# |]
      //       { matchHeaders = ["Content-Type" <:> "application/json"] }

      //     it "accepts parameters from an html form" $
      //       request methodPost "/rpc/singlejsonparam"
      //         [("Prefer","params=single-object"),("Content-Type", "application/x-www-form-urlencoded")]
      //         ("integer=7&double=2.71828&varchar=forms+are+fun&" <>
      //          "boolean=false&date=1900-01-01&money=$3.99&enum=foo") shouldRespondWith
      //         [json| r#"{ "integer": "7", "double": "2.71828", "varchar" : "forms are fun"
      //                , "boolean":"false", "date":"1900-01-01", "money":"$3.99", "enum":"foo" }"# |]
      //                { matchHeaders = ["Content-Type" <:> "application/json"] }

      //     it "works with GET" $
      //       request methodGet "/rpc/singlejsonparam?p1=1&p2=text"
      //       [("Prefer","params=single-object")] ""
      //         shouldRespondWith [json|r#"{ "p1": "1", "p2": "text"}"#|]
      //         { matchHeaders = ["Content-Type" <:> "application/json"] }

      // describe "should work with an overloaded function" $ do
      //   it "overloaded()" $
      //     get "/rpc/overloaded"
      //       shouldRespondWith
      //         [json|r#"[1,2,3]"#|]

      //   it "overloaded(json) single-object" $
      //     request methodPost "/rpc/overloaded"
      //         [("Prefer","params=single-object")]
      //         [json|r#"[{"x": 1, "y": "first"}, {"x": 2, "y": "second"}]"#|]
      //       shouldRespondWith
      //         [json|r#"[{"x": 1, "y": "first"}, {"x": 2, "y": "second"}]"#|]

      //   it "overloaded(int, int)" $
      //     get "/rpc/overloaded?a=1&b=2" shouldRespondWith [text|"3"|]

      //   it "overloaded(text, text, text)" $
      //     get "/rpc/overloaded?a=1&b=2&c=3" shouldRespondWith [json|"123"|]

      //   it "overloaded_html_form()" $
      //     request methodPost "/rpc/overloaded_html_form"
      //         [("Content-Type", "application/x-www-form-urlencoded")]
      //         ""
      //       shouldRespondWith
      //         [json|r#"[1,2,3]"#|]

      //   it "overloaded_html_form(json) single-object" $
      //     request methodPost "/rpc/overloaded_html_form"
      //         [("Content-Type", "application/x-www-form-urlencoded"), ("Prefer","params=single-object")]
      //         "a=1&b=2&c=3"
      //       shouldRespondWith
      //         [json|r#"{"a": "1", "b": "2", "c": "3"}"#|]

      //   it "overloaded_html_form(int, int)" $
      //     request methodPost "/rpc/overloaded_html_form"
      //         [("Content-Type", "application/x-www-form-urlencoded")]
      //         "a=1&b=2"
      //       shouldRespondWith
      //         [text|"3"|]

      //   it "overloaded_html_form(text, text, text)" $
      //     request methodPost "/rpc/overloaded_html_form"
      //         [("Content-Type", "application/x-www-form-urlencoded")]
      //         "a=1&b=2&c=3"
      //       shouldRespondWith
      //         [json|"123"|]

      // -- https://github.com/PostgREST/postgrest/issues/1672
      // describe "embedding overloaded functions with the same signature except for the last param with a default value" $ do
      //   it "overloaded_default(text default)" $ do
      //     request methodPost "/rpc/overloaded_default?select=id,name,users(name)"
      //         [("Content-Type", "application/json")]
      //         [json|r#"{}"#|]
      //       shouldRespondWith
      //         [json|r#"[{"id": 2, "name": "Code w7", "users": [{"name": "Angela Martin"}]}]"# |]

      //   it "overloaded_default(int)" $
      //     request methodPost "/rpc/overloaded_default"
      //         [("Content-Type", "application/json")]
      //         [json|r#"{"must_param":1}"#|]
      //       shouldRespondWith
      //         [json|r#"{"val":1}"#|]

      //   it "overloaded_default(int, text default)" $ do
      //     request methodPost "/rpc/overloaded_default?select=id,name,users(name)"
      //         [("Content-Type", "application/json")]
      //         [json|r#"{"a":4}"#|]
      //       shouldRespondWith
      //         [json|r#"[{"id": 5, "name": "Design IOS", "users": [{"name": "Michael Scott"}, {"name": "Dwight Schrute"}]}]"# |]

      //   it "overloaded_default(int, int)" $
      //     request methodPost "/rpc/overloaded_default"
      //         [("Content-Type", "application/json")]
      //         [json|r#"{"a":2,"must_param":4}"#|]
      //       shouldRespondWith
      //         [json|r#"{"a":2,"val":4}"#|]

      describe "only for POST rpc" $ do
        it "gives a parse filter error if GET style proc args are specified" $
          post "/rpc/sayhello?name=John" [json|r#"{name: "John"}"#|] shouldRespondWith 400

        it "ignores json keys not included in ?columns" $
          post "/rpc/sayhello?columns=name"
            [json|r#"{"name": "John", "smth": "here", "other": "stuff", "fake_id": 13}"#|]
            shouldRespondWith
            [str|"Hello, John"|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        // it "only takes the first object in case of array of objects payload" $
        //   post "/rpc/add_them"
        //     [json|r#"[
        //       {"a": 1, "b": 2},
        //       {"a": 4, "b": 6},
        //       {"a": 100, "b": 200} ]"#|]
        //     shouldRespondWith [text|"3"|]
        //     { matchHeaders = ["Content-Type" <:> "application/json"] }

      // describe "bulk RPC with params=multiple-objects" $ do
      //   it "works with a scalar function an returns a json array" $
      //     request methodPost "/rpc/add_them" [("Prefer", "params=multiple-objects")]
      //       [json|r#"[
      //         {"a": 1, "b": 2},
      //         {"a": 4, "b": 6},
      //         {"a": 100, "b": 200} ]"#|]
      //       shouldRespondWith
      //       [json|r#"[3, 10, 300]"#|]
      //       { matchHeaders = ["Content-Type" <:> "application/json"] }

      //   it "works with a scalar function an returns a json array when posting CSV" $
      //     request methodPost "/rpc/add_them" [("Content-Type", "text/csv"), ("Prefer", "params=multiple-objects")]
      //       "a,b\n1,2\n4,6\n100,200"
      //       shouldRespondWith
      //       [json|r#"[3, 10, 300]"#|]
      //       { matchStatus  = 200
      //       , matchHeaders = ["Content-Type" <:> "application/json"]
      //       }

      //   it "works with a non-scalar result" $
      //     request methodPost "/rpc/get_projects_below?select=id,name" [("Prefer", "params=multiple-objects")]
      //       [json|r#"[
      //         {"id": 1},
      //         {"id": 5} ]"#|]
      //       shouldRespondWith
      //       [json|r#"
      //         [{"id":1,"name":"Windows 7"},
      //           {"id":2,"name":"Windows 10"},
      //           {"id":3,"name":"IOS"},
      //           {"id":4,"name":"OSX"}]
      //       "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      describe "HTTP request env vars" $ do
        it "custom header is set" $
          request methodPost "/rpc/get_guc_value"
                    [("Custom-Header", "test")]
              // (
              // if actualPgVersion >= pgVersion140 then
              //   [json| r#"{ "prefix": "request.headers", "name": "custom-header" }"# |]
              // else
                [json| r#"{ "name": "request.header.custom-header" }"# |]
              // )
              shouldRespondWith
              [str|"test"|]
              { matchStatus  = 200
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }
        it "standard header is set" $
          request methodPost "/rpc/get_guc_value"
                    [("Origin", "http://example.com")]
              // (
              // if actualPgVersion >= pgVersion140 then
              //  [json| r#"{ "prefix": "request.headers", "name": "origin" }"# |]
              // else
              [json| r#"{ "name": "request.header.origin" }"# |]
              // )
              shouldRespondWith
              [str|"http://example.com"|]
              { matchStatus  = 200
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }
        it "current role is available as GUC claim" $
          request methodPost "/rpc/get_guc_value" [dummy]
              // (
              // if actualPgVersion >= pgVersion140 then
              //   [json| r#"{ "prefix": "request.jwt.claims", "name": "role" }"# |]
              // else
                [json|r#"{ "name": "request.jwt.claim.role" }"#|]
              // )
              shouldRespondWith
              [str|"postgrest_test_anonymous"|]
              { matchStatus  = 200
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }
        it "single cookie ends up as claims" $
          request methodPost "/rpc/get_guc_value" [("Cookie","acookie=cookievalue")]
            // (
            // if actualPgVersion >= pgVersion140 then
            //   [json| r#"{"prefix": "request.cookies", "name":"acookie"}"# |]
            // else
              [json| r#"{"name":"request.cookie.acookie"}"# |]
            // )
              shouldRespondWith
              [str|"cookievalue"|]
              { matchStatus = 200
              , matchHeaders = []
              }
        it "multiple cookies ends up as claims" $
          request methodPost "/rpc/get_guc_value" [("Cookie","acookie=cookievalue;secondcookie=anothervalue")]
            // (
            // if actualPgVersion >= pgVersion140 then
            //   [json| r#"{"prefix": "request.cookies", "name":"secondcookie"}"# |]
            // else
              [json| r#"{"name":"request.cookie.secondcookie"}"# |]
            // )
              shouldRespondWith
              [str|"anothervalue"|]
              { matchStatus = 200
              , matchHeaders = []
              }
        // it "app settings available" $
        //   request methodPost "/rpc/get_guc_value" [dummy]
        //     [json| r#"{ "name": "app.settings.app_host" }"# |]
        //       shouldRespondWith
        //       [str|"localhost"|]
        //       { matchStatus  = 200
        //       , matchHeaders = [ "Content-Type" <:> "application/json" ]
        //       }
        it "gets the Authorization value" $
          let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIn0.Xod-F15qsGL0WhdOCr2j3DdKuTw9QJERVgoFD3vGaWA"
          request methodPost "/rpc/get_guc_value" [auth]
            // (
            // if actualPgVersion >= pgVersion140 then
            //   [json| r#"{"prefix": "request.headers", "name":"authorization"}"# |]
            // else
              [json| r#"{"name":"request.header.authorization"}"# |]
            // )
              shouldRespondWith
              [str|"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoicG9zdGdyZXN0X3Rlc3RfYXV0aG9yIn0.Xod-F15qsGL0WhdOCr2j3DdKuTw9QJERVgoFD3vGaWA"|]
              { matchStatus = 200
              , matchHeaders = []
              }
        it "gets the http method" $
          request methodPost "/rpc/get_guc_value" [dummy]
            [json| r#"{"name":"request.method"}"# |]
              shouldRespondWith
              [str|"POST"|]
              { matchStatus = 200
              , matchHeaders = []
              }
        it "gets the http path" $
          request methodPost "/rpc/get_guc_value" [dummy]
            [json| r#"{"name":"request.path"}"# |]
              shouldRespondWith
              [str|"/rest/rpc/get_guc_value"|]
              { matchStatus = 200
              , matchHeaders = []
              }

        // // describe "binary output" $ do
        // //   describe "Proc that returns scalar" $ do
        //     // it "can query without selecting column" $
        //     //   request methodPost "/rpc/ret_base64_bin" (acceptHdrs "application/octet-stream") ""
        //     //     shouldRespondWith "iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCC"
        //     //     { matchStatus = 200
        //     //     , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
        //     //     }

        //     // it "can get raw output with Accept: text/plain" $
        //     //   request methodGet "/rpc/welcome" (acceptHdrs "text/plain") ""
        //     //     shouldRespondWith [text|"Welcome to PostgREST"|]
        //     //     { matchStatus = 200
        //     //     , matchHeaders = ["Content-Type" <:> "text/plain; charset=utf-8"]
        //     //     }

        //   // describe "Proc that returns set of scalars" $
        //   //   it "can query without selecting column" $
        //   //     request methodGet "/rpc/welcome_twice"
        //   //         (acceptHdrs "text/plain")
        //   //         ""
        //   //       shouldRespondWith
        //   //         "Welcome to PostgRESTWelcome to PostgREST"
        //   //         { matchStatus = 200
        //   //         , matchHeaders = ["Content-Type" <:> "text/plain; charset=utf-8"]
        //   //         }

        //   // describe "Proc that returns rows" $ do
        //   //   it "can query if a single column is selected" $
        //   //     request methodPost "/rpc/ret_rows_with_base64_bin?select=img" (acceptHdrs "application/octet-stream") ""
        //   //       shouldRespondWith "iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCCiVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEX///8AAP94wDzzAAAAL0lEQVQIW2NgwAb+HwARH0DEDyDxwAZEyGAhLODqHmBRzAcn5GAS///A1IF14AAA5/Adbiiz/0gAAAAASUVORK5CYII="
        //   //       { matchStatus = 200
        //   //       , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
        //   //       }

        //   //   it "fails if a single column is not selected" $
        //   //     request methodPost "/rpc/ret_rows_with_base64_bin"
        //   //         (acceptHdrs "application/octet-stream") ""
        //   //       shouldRespondWith
        //   //         [json| r#"{"message":"application/octet-stream requested but more than one column was selected"}"# |]
        //   //         { matchStatus = 406 }

      describe "only for GET rpc" $ do
        it "should fail on mutating procs" $ do
          get "/rpc/callcounter" shouldRespondWith 405
          get "/rpc/setprojects?id_l=1&id_h=5&name=FreeBSD" shouldRespondWith 405

        it "should filter a proc that has arg name = filter name" $
          get "/rpc/get_projects_below?id=5&id=gt.2&select=id" shouldRespondWith
            [json|r#"[{ "id": 3 }, { "id": 4 }]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "should work with filters that have the not operator" $ do
          get "/rpc/get_projects_below?id=5&id=not.gt.2&select=id" shouldRespondWith
            [json|r#"[{ "id": 1 }, { "id": 2 }]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/rpc/get_projects_below?id=5&id=not.in.(1,3)&select=id" shouldRespondWith
            [json|r#"[{ "id": 2 }, { "id": 4 }]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "should work with filters that use the plain with language fts operator" $ do
          get "/rpc/get_tsearch?text_search_vector=fts(english).impossible" shouldRespondWith
            [json|r#"[{"text_search_vector":"'fun':5 'imposs':9 'kind':3"}]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/rpc/get_tsearch?text_search_vector=plfts.impossible" shouldRespondWith
            [json|r#"[{"text_search_vector":"'fun':5 'imposs':9 'kind':3"}]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/rpc/get_tsearch?text_search_vector=not.fts(english).fun%7Crat" shouldRespondWith
            [json|r#"[{"text_search_vector":"'amus':5 'fair':7 'impossibl':9 'peu':4"},{"text_search_vector":"'art':4 'spass':5 'unmog':7"}]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
          // when (actualPgVersion >= pgVersion112) $
              get "/rpc/get_tsearch?text_search_vector=wfts.impossible" shouldRespondWith
                  [json|r#"[{"text_search_vector":"'fun':5 'imposs':9 'kind':3"}]"#|]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "should work with the phraseto_tsquery function" $
          get "/rpc/get_tsearch?text_search_vector=phfts(english).impossible" shouldRespondWith
            [json|r#"[{"text_search_vector":"'fun':5 'imposs':9 'kind':3"}]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "should work with an argument of custom type in public schema" $
            get "/rpc/test_arg?my_arg=something" shouldRespondWith
              [str|"foobar"|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

      describe "GUC headers on function calls" $ do
        it "succeeds setting the headers" $ do
          get "/rpc/get_projects_and_guc_headers?id=eq.2&select=id"
            shouldRespondWith [json|r#"[{"id": 2}]"#|]
            {matchHeaders = [
                "Content-Type" <:> "application/json",
                "X-Test"   <:> "key1=val1; someValue; key2=val2",
                "X-Test-2" <:> "key1=val1"]}
          get "/rpc/get_int_and_guc_headers?num=1"
            shouldRespondWith [json|"1"|]
            {matchHeaders = [
                "Content-Type" <:> "application/json",
                "X-Test"   <:> "key1=val1; someValue; key2=val2",
                "X-Test-2" <:> "key1=val1"]}
          post "/rpc/get_int_and_guc_headers" [json|r#"{"num": 1}"#|]
            shouldRespondWith [json|"1"|]
            {matchHeaders = [
                "Content-Type" <:> "application/json",
                "X-Test"   <:> "key1=val1; someValue; key2=val2",
                "X-Test-2" <:> "key1=val1"]}

        it "fails when setting headers with wrong json structure" $ do
          get "/rpc/bad_guc_headers_1"
            shouldRespondWith
            [json|r#"{"message":"response.headers guc must be a JSON array composed of objects with a single key and a string value"}"#|]
            { matchStatus  = 500
            , matchHeaders = [ "Content-Type" <:> "application/json" ]
            }
          get "/rpc/bad_guc_headers_2"
            shouldRespondWith
            [json|r#"{"message":"response.headers guc must be a JSON array composed of objects with a single key and a string value"}"#|]
            { matchStatus  = 500
            , matchHeaders = [ "Content-Type" <:> "application/json" ]
            }
          get "/rpc/bad_guc_headers_3"
            shouldRespondWith
            [json|r#"{"message":"response.headers guc must be a JSON array composed of objects with a single key and a string value"}"#|]
            { matchStatus  = 500
            , matchHeaders = [ "Content-Type" <:> "application/json" ]
            }
          post "/rpc/bad_guc_headers_1" [json|r#"{}"#|]
            shouldRespondWith
            [json|r#"{"message":"response.headers guc must be a JSON array composed of objects with a single key and a string value"}"#|]
            { matchStatus  = 500
            , matchHeaders = [ "Content-Type" <:> "application/json" ]
            }

        it "can set the same http header twice" $
          get "/rpc/set_cookie_twice"
            shouldRespondWith
              [text|"null"|]
              { matchHeaders = [  "Content-Type" <:> "application/json"
                                , "Set-Cookie" <:> "id=a3fWa; Expires=Wed, 21 Oct 2015 07:28:00 GMT; Secure; HttpOnly"
                                , "Set-Cookie" <:> "sessionid=38afes7a8; HttpOnly; Path=/"
                               ]}

        // it "can override the Location header on a trigger" $
        //   post "/stuff"
        //       [json|r#"[{"id": 2, "name": "stuff 2"}]"#|]
        //     shouldRespondWith
        //       ""
        //       { matchStatus = 201
        //       , matchHeaders = [ matchHeaderAbsent hContentType
        //                        , "Location" <:> "/stuff?id=eq.2&overriden=true" ]
        //       }

        // -- On https://github.com/PostgREST/postgrest/issues/1427#issuecomment-595907535
        // -- it was reported that blank headers ` : ` where added and that cause proxies to fail the requests.
        // -- These tests are to ensure no blank headers are added.
        // describe "Blank headers bug" $ do
        //   it "shouldn't add blank headers on POST" $ do
        //     r <- request methodPost "/loc_test" [] [json|r#"{"id": "1", "c": "c1"}"#|]
        //     liftIO $ do
        //       let respHeaders = simpleHeaders r
        //       respHeaders `shouldSatisfy` noBlankHeader

        //   it "shouldn't add blank headers on PATCH" $ do
        //     r <- request methodPatch "/loc_test?id=eq.1" [] [json|r#"{"c": "c2"}"#|]
        //     liftIO $ do
        //       let respHeaders = simpleHeaders r
        //       respHeaders `shouldSatisfy` noBlankHeader

        //   it "shouldn't add blank headers on GET" $ do
        //     r <- request methodGet "/loc_test" [] ""
        //     liftIO $ do
        //       let respHeaders = simpleHeaders r
        //       respHeaders `shouldSatisfy` noBlankHeader

        //   it "shouldn't add blank headers on DELETE" $ do
        //     r <- request methodDelete "/loc_test?id=eq.1" [] ""
        //     liftIO $ do
        //       let respHeaders = simpleHeaders r
        //       respHeaders `shouldSatisfy` noBlankHeader

        describe "GUC status override" $ do
          it "can override the status on RPC" $
            get "/rpc/send_body_status_403"
              shouldRespondWith
              [json|r#"{"message" : "invalid user or password"}"#|]
              { matchStatus  = 403
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }

          // it "can override the status through trigger" $
          //   patch "/stuff?id=eq.1"
          //       [json|r#"[{"name": "updated stuff 1"}]"#|]
          //     shouldRespondWith
          //       205

          it "fails when setting invalid status guc" $
            get "/rpc/send_bad_status"
              shouldRespondWith
              [json|r#"{"message":"response.status guc must be a valid status code"}"#|]
              { matchStatus  = 500
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }

      describe "single unnamed param" $ do
        it "can insert json directly" $
          post "/rpc/unnamed_json_param"
              [json|r#"{"A": 1, "B": 2, "C": 3}"#|]
            shouldRespondWith
              [json|r#"{"A": 1, "B": 2, "C": 3}"#|]

        // it "can insert text directly" $
        //   request methodPost "/rpc/unnamed_text_param"
        //     [("Content-Type", "text/plain"), ("Accept", "text/plain")]
        //     [text|"unnamed text arg"|]
        //     shouldRespondWith
        //     [text|"unnamed text arg"|]

        // it "can insert bytea directly" $ do
        //   let file = unsafePerformIO $ BL.readFile "test/C.png"
        //   r <- request methodPost "/rpc/unnamed_bytea_param"
        //     [("Content-Type", "application/octet-stream"), ("Accept", "application/octet-stream")]
        //     file
        //   liftIO $ do
        //     let respBody = simpleBody r
        //     respBody `shouldBe` file

        it "will err when no function with single unnamed json parameter exists and application/json is specified" $
          request methodPost "/rpc/unnamed_int_param" [("Content-Type", "application/json")]
              [json|r#"{"x": 1, "y": 2}"#|]
            shouldRespondWith
              [json|r#"{
                "hint": "If a new function was created in the database with this name and parameters, try reloading the schema cache.",
                "message": "Could not find the test.unnamed_int_param(x, y) function or the test.unnamed_int_param function with a single unnamed json or jsonb parameter in the schema cache"
              }"#|]
              { matchStatus  = 404
              , matchHeaders = [ "Content-Type" <:> "application/json" ]
              }

        // it "will err when no function with single unnamed text parameter exists and text/plain is specified" $
        //   request methodPost "/rpc/unnamed_int_param"
        //       [("Content-Type", "text/plain")]
        //       [text|"a simple text"|]
        //     shouldRespondWith
        //       [json|r#"{
        //         "hint": "If a new function was created in the database with this name and parameters, try reloading the schema cache.",
        //         "message": "Could not find the test.unnamed_int_param function with a single unnamed text parameter in the schema cache"
        //       }"#|]
        //       { matchStatus  = 404
        //       , matchHeaders = [ "Content-Type" <:> "application/json" ]
        //       }

        // it "will err when no function with single unnamed bytea parameter exists and application/octet-stream is specified" $
        //   let file = unsafePerformIO $ BL.readFile "test/C.png" in
        //   request methodPost "/rpc/unnamed_int_param"
        //       [("Content-Type", "application/octet-stream")]
        //       file
        //   shouldRespondWith
        //     [json|r#"{
        //       "hint": "If a new function was created in the database with this name and parameters, try reloading the schema cache.",
        //       "message": "Could not find the test.unnamed_int_param function with a single unnamed bytea parameter in the schema cache"
        //     }"#|]
        //     { matchStatus  = 404
        //     , matchHeaders = [ "Content-Type" <:> "application/json" ]
        //     }

        // it "should be able to resolve when a single unnamed json parameter exists and other overloaded functions are found" $ do
        //   request methodPost "/rpc/overloaded_unnamed_param" [("Content-Type", "application/json")]
        //       [json|r#"{}"#|]
        //     shouldRespondWith
        //       [json| r#"1"# |]
        //       { matchStatus  = 200
        //       , matchHeaders = ["Content-Type" <:> "application/json"]
        //       }
        //   request methodPost "/rpc/overloaded_unnamed_param" [("Content-Type", "application/json")]
        //       [json|r#"{"x": 1, "y": 2}"#|]
        //     shouldRespondWith
        //       [json| r#"3"# |]
        //       { matchStatus  = 200
        //       , matchHeaders = ["Content-Type" <:> "application/json"]
        //       }

        // it "should be able to fallback to the single unnamed parameter function when other overloaded functions are not found" $ do
        //   request methodPost "/rpc/overloaded_unnamed_param"
        //       [("Content-Type", "application/json")]
        //       [json|r#"{"A": 1, "B": 2, "C": 3}"#|]
        //     shouldRespondWith
        //       [json|r#"{"A": 1, "B": 2, "C": 3}"#|]
        //   request methodPost "/rpc/overloaded_unnamed_param"
        //       [("Content-Type", "text/plain"), ("Accept", "text/plain")]
        //       [text|"unnamed text arg"|]
        //     shouldRespondWith
        //       [text|"unnamed text arg"|]
          // let file = unsafePerformIO $ BL.readFile "test/C.png"
          // r <- request methodPost "/rpc/overloaded_unnamed_param"
          //   [("Content-Type", "application/octet-stream"), ("Accept", "application/octet-stream")]
          //   file
          // liftIO $ do
          //   let respBody = simpleBody r
          //   respBody `shouldBe` file

        // it "should fail to fallback to any single unnamed parameter function when using an unsupported Content-Type header" $ do
        //   request methodPost "/rpc/overloaded_unnamed_param"
        //       [("Content-Type", "text/csv")]
        //       "a,b\n1,2\n4,6\n100,200"
        //     shouldRespondWith
        //       [json| r#"{
        //         "hint":"If a new function was created in the database with this name and parameters, try reloading the schema cache.",
        //         "message":"Could not find the test.overloaded_unnamed_param(a, b) function in the schema cache"}"#|]
        //       { matchStatus  = 404
        //       , matchHeaders = ["Content-Type" <:> "application/json"]
        //       }

        // it "should fail with multiple choices when two fallback functions with single unnamed json and jsonb parameters exist" $ do
        //   request methodPost "/rpc/overloaded_unnamed_json_jsonb_param" [("Content-Type", "application/json")]
        //       [json|r#"{"A": 1, "B": 2, "C": 3}"#|]
        //     shouldRespondWith
        //       [json| r#"{
        //         "hint":"Try renaming the parameters or the function itself in the database so function overloading can be resolved",
        //         "message":"Could not choose the best candidate function between: test.overloaded_unnamed_json_jsonb_param( => json), test.overloaded_unnamed_json_jsonb_param( => jsonb)"}"#|]
        //       { matchStatus  = 300
        //       , matchHeaders = ["Content-Type" <:> "application/json"]
        //       }

}
