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
feature "upsert"
  describe "with POST" $ do
      describe "when Prefer: resolution=merge-duplicates is specified" $ do
        it "INSERTs and UPDATEs rows on pk conflict" $
          request methodPost "/tiobe_pls" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json| r#"[
              { "name": "Javascript", "rank": 6 },
              { "name": "Java", "rank": 2 },
              { "name": "C", "rank": 1 }
            ]"#|] shouldRespondWith [json| r#"[
              { "name": "Javascript", "rank": 6 },
              { "name": "Java", "rank": 2 },
              { "name": "C", "rank": 1 }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
            }

        it "INSERTs and UPDATEs row on composite pk conflict" $
          request methodPost "/employees" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json| r#"[
              { "first_name": "Frances M.", "last_name": "Roe", "salary": "30000" },
              { "first_name": "Peter S.", "last_name": "Yang", "salary": 42000 }
            ]"#|] shouldRespondWith [json| r#"[
              { "first_name": "Frances M.", "last_name": "Roe", "salary": "$30,000.00", "company": "One-Up Realty", "occupation": "Author" },
              { "first_name": "Peter S.", "last_name": "Yang", "salary": "$42,000.00", "company": null, "occupation": null }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
            }

        //when (actualPgVersion >= pgVersion110) $
          it "INSERTs and UPDATEs rows on composite pk conflict for partitioned tables" $
            request methodPost "/car_models" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
              [json| r#"[
                { "name": "Murcielago", "year": 2001, "car_brand_name": null},
                { "name": "Roma", "year": 2021, "car_brand_name": "Ferrari" }
              ]"#|] shouldRespondWith [json| r#"[
                { "name": "Murcielago", "year": 2001, "car_brand_name": null},
                { "name": "Roma", "year": 2021, "car_brand_name": "Ferrari" }
              ]"#|]
              { matchStatus = 201
              , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
              }

        it "succeeds when the payload has no elements" $
          request methodPost "/articles" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json|r#"[]"#|] shouldRespondWith
            [json|r#"[]"#|] { matchStatus = 201 , matchHeaders = ["Content-Type" <:> "application/json"] }

        it "INSERTs and UPDATEs rows on single unique key conflict" $
          request methodPost "/single_unique?on_conflict=unique_key" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json| r#"[
              { "unique_key": 1, "value": "B" },
              { "unique_key": 2, "value": "C" }
            ]"#|] shouldRespondWith [json| r#"[
              { "unique_key": 1, "value": "B" },
              { "unique_key": 2, "value": "C" }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
            }

        it "INSERTs and UPDATEs rows on compound unique keys conflict" $
          request methodPost "/compound_unique?on_conflict=key1,key2" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json| r#"[
              { "key1": 1, "key2": 1, "value": "B" },
              { "key1": 1, "key2": 2, "value": "C" }
            ]"#|] shouldRespondWith [json| r#"[
              { "key1": 1, "key2": 1, "value": "B" },
              { "key1": 1, "key2": 2, "value": "C" }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates", "Content-Type" <:> "application/json"]
            }

      describe "when Prefer: resolution=ignore-duplicates is specified" $ do
        it "INSERTs and ignores rows on pk conflict" $
          request methodPost "/tiobe_pls" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
            [json|r#"[
              { "name": "PHP", "rank": 9 },
              { "name": "Python", "rank": 10 }
            ]"#|] shouldRespondWith [json|r#"[
              { "name": "PHP", "rank": 9 }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates", "Content-Type" <:> "application/json"]
            }

        it "INSERTs and ignores rows on composite pk conflict" $
          request methodPost "/employees" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
            [json|r#"[
              { "first_name": "Daniel B.", "last_name": "Lyon", "salary": "72000", "company": null, "occupation": null },
              { "first_name": "Sara M.", "last_name": "Torpey", "salary": 60000, "company": "Burstein-Applebee", "occupation": "Soil scientist" }
            ]"#|] shouldRespondWith [json|r#"[
              { "first_name": "Sara M.", "last_name": "Torpey", "salary": "$60,000.00", "company": "Burstein-Applebee", "occupation": "Soil scientist" }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates", "Content-Type" <:> "application/json"]
            }

        //when (actualPgVersion >= pgVersion110) $
          it "INSERTs and ignores rows on composite pk conflict for partitioned tables" $
            request methodPost "/car_models" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
              [json| r#"[
                { "name": "Murcielago", "year": 2001, "car_brand_name": "Ferrari" },
                { "name": "Huracán", "year": 2021, "car_brand_name": "Lamborghini" }
              ]"#|] shouldRespondWith [json| r#"[
                { "name": "Huracán", "year": 2021, "car_brand_name": "Lamborghini" }
              ]"#|]
              { matchStatus = 201
              , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates", "Content-Type" <:> "application/json"]
              }

        it "INSERTs and ignores rows on single unique key conflict" $
          request methodPost "/single_unique?on_conflict=unique_key"
              [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
              [json| r#"[
                { "unique_key": 1, "value": "B" },
                { "unique_key": 2, "value": "C" },
                { "unique_key": 3, "value": "D" }
              ]"#|]
            shouldRespondWith
              [json| r#"[
                { "unique_key": 2, "value": "C" },
                { "unique_key": 3, "value": "D" }
              ]"#|]
              { matchStatus = 201
              , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates"]
              }

        it "INSERTs and UPDATEs rows on compound unique keys conflict" $
          request methodPost "/compound_unique?on_conflict=key1,key2"
              [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
              [json| r#"[
                { "key1": 1, "key2": 1, "value": "B" },
                { "key1": 1, "key2": 2, "value": "C" },
                { "key1": 1, "key2": 3, "value": "D" }
              ]"#|]
            shouldRespondWith
              [json| r#"[
                { "key1": 1, "key2": 2, "value": "C" },
                { "key1": 1, "key2": 3, "value": "D" }
              ]"#|]
              { matchStatus = 201
              , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates"]
              }

      it "succeeds if the table has only PK cols and no other cols" $ do
        request methodPost "/only_pk" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
          [json|r#"[ { "id": 1 }, { "id": 2 }, { "id": 3} ]"#|]
          shouldRespondWith
          [json|r#"[ { "id": 3} ]"#|]
          { matchStatus = 201 ,
            matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates",
            "Content-Type" <:> "application/json"] }

        request methodPost "/only_pk" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
          [json|r#"[ { "id": 1 }, { "id": 2 }, { "id": 4} ]"#|]
          shouldRespondWith
          [json|r#"[ { "id": 1 }, { "id": 2 }, { "id": 4} ]"#|]
          { matchStatus = 201 ,
            matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates",
            "Content-Type" <:> "application/json"] }

      it "succeeds and ignores the Prefer: resolution header(no Preference-Applied present) if the table has no PK" $
        request methodPost "/no_pk" [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
          [json|r#"[ { "a": "1", "b": "0" } ]"#|]
          shouldRespondWith
          [json|r#"[ { "a": "1", "b": "0" } ]"#|] { matchStatus = 201 , matchHeaders = ["Content-Type" <:> "application/json"] }

      it "succeeds if not a single resource is created" $ do
        request methodPost "/tiobe_pls" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
          [json|r#"[ { "name": "Java", "rank": 1 } ]"#|] shouldRespondWith
          [json|r#"[]"#|] { matchStatus = 201 , matchHeaders = ["Content-Type" <:> "application/json"] }
        request methodPost "/tiobe_pls" [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
          [json|r#"[ { "name": "Java", "rank": 1 }, { "name": "C", "rank": 2 } ]"#|] shouldRespondWith
          [json|r#"[]"#|] { matchStatus = 201 , matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "with PUT" $ do
      describe "Restrictions" $ do
        // it "fails if Range is specified" $
        //   request methodPut "/tiobe_pls?name=eq.Javascript" [("Range", "0-5")]
        //     [json| r#"[ { "name": "Javascript", "rank": 1 } ]"#|]
        //     shouldRespondWith
        //     [json|r#"{"message":"Range header and limit/offset querystring parameters are not allowed for PUT"}"#|]
        //     { matchStatus = 400 , matchHeaders = ["Content-Type" <:> "application/json"] }

        it "fails if limit is specified" $
          request methodPut "/tiobe_pls?name=eq.Javascript&limit=1"
            [json| r#"[ { "name": "Javascript", "rank": 1 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Range header and limit/offset querystring parameters are not allowed for PUT"}"#|]
            { matchStatus = 400 , matchHeaders = ["Content-Type" <:> "application/json"] }

        it "fails if offset is specified" $
          request methodPut "/tiobe_pls?name=eq.Javascript&offset=1"
            [json| r#"[ { "name": "Javascript", "rank": 1 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Range header and limit/offset querystring parameters are not allowed for PUT"}"#|]
            { matchStatus = 400 , matchHeaders = ["Content-Type" <:> "application/json"] }

        it "rejects every other filter than pk cols eq" $ do
          request methodPut "/tiobe_pls?rank=eq.19"
            [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }

          request methodPut "/tiobe_pls?id=not.eq.Java"
            [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }
          request methodPut "/tiobe_pls?id=in.(Go)"
            [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }
          request methodPut "/tiobe_pls?and=(id.eq.Go)"
            [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }

        it "fails if not all composite key cols are specified as eq filters" $ do
          request methodPut "/employees?first_name=eq.Susan"
            [json| r#"[ { "first_name": "Susan", "last_name": "Heidt", "salary": "48000", "company": "GEX", "occupation": "Railroad engineer" } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }
          request methodPut "/employees?last_name=eq.Heidt"
            [json| r#"[ { "first_name": "Susan", "last_name": "Heidt", "salary": "48000", "company": "GEX", "occupation": "Railroad engineer" } ]"#|]
            shouldRespondWith
            [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
            { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }

      it "fails if the uri primary key does not match the payload primary key" $ do
        request methodPut "/tiobe_pls?name=eq.MATLAB" [json| r#"[ { "name": "Perl", "rank": 17 } ]"#|]
          shouldRespondWith
          [json|r#"{"message":"Payload values do not match URL in primary key column(s)"}"#|]
          { matchStatus = 400 , matchHeaders = ["Content-Type" <:> "application/json"] }
        request methodPut "/employees?first_name=eq.Wendy&last_name=eq.Anderson"
          [json| r#"[ { "first_name": "Susan", "last_name": "Heidt", "salary": "48000", "company": "GEX", "occupation": "Railroad engineer" } ]"#|]
          shouldRespondWith
          [json|r#"{"message":"Payload values do not match URL in primary key column(s)"}"#|]
          { matchStatus = 400 , matchHeaders = ["Content-Type" <:> "application/json"] }

      it "fails if the table has no PK" $
        request methodPut "/no_pk?a=eq.one&b=eq.two" [json| r#"[ { "a": "one", "b": "two" } ]"#|]
          shouldRespondWith
          [json|r#"{"message":"Filters must include all and only primary key columns with 'eq' operators"}"#|]
          { matchStatus = 405 , matchHeaders = ["Content-Type" <:> "application/json"] }

      describe "Inserting row" $ do
        it "succeeds on table with single pk col" $ do
          //-- assert that the next request will indeed be an insert
          get "/tiobe_pls?name=eq.Go"
            shouldRespondWith
              [json|r#"[]"#|]

          request methodPut "/tiobe_pls?name=eq.Go"
              [("Prefer", "return=representation")]
              [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]
            shouldRespondWith
              [json| r#"[ { "name": "Go", "rank": 19 } ]"#|]

        it "succeeds on table with composite pk" $ do
          //-- assert that the next request will indeed be an insert
          get "/employees?first_name=eq.Susan&last_name=eq.Heidt"
            shouldRespondWith
              [json|r#"[]"#|]

          request methodPut "/employees?first_name=eq.Susan&last_name=eq.Heidt"
              [("Prefer", "return=representation")]
              [json| r#"[ { "first_name": "Susan", "last_name": "Heidt", "salary": "48000", "company": "GEX", "occupation": "Railroad engineer" } ]"#|]
            shouldRespondWith
              [json| r#"[ { "first_name": "Susan", "last_name": "Heidt", "salary": "$48,000.00", "company": "GEX", "occupation": "Railroad engineer" } ]"#|]

        //when (actualPgVersion >= pgVersion110) $
          it "succeeds on a partitioned table with composite pk" $ do
            //-- assert that the next request will indeed be an insert
            get "/car_models?name=eq.Supra&year=eq.2021"
              shouldRespondWith
                [json|r#"[]"#|]

            request methodPut "/car_models?name=eq.Supra&year=eq.2021"
                [("Prefer", "return=representation")]
                [json| r#"[ { "name": "Supra", "year": 2021 } ]"#|]
              shouldRespondWith
                [json| r#"[ { "name": "Supra", "year": 2021, "car_brand_name": null } ]"#|]

        it "succeeds if the table has only PK cols and no other cols" $ do
          //-- assert that the next request will indeed be an insert
          get "/only_pk?id=eq.10"
            shouldRespondWith
              [json|r#"[]"#|]

          request methodPut "/only_pk?id=eq.10"
              [("Prefer", "return=representation")]
              [json|r#"[ { "id": 10 } ]"#|]
            shouldRespondWith
              [json|r#"[ { "id": 10 } ]"#|]

      describe "Updating row" $ do
        it "succeeds on table with single pk col" $ do
          //-- assert that the next request will indeed be an update
          get "/tiobe_pls?name=eq.Java"
            shouldRespondWith
              [json|r#"[ { "name": "Java", "rank": 1 } ]"#|]

          request methodPut "/tiobe_pls?name=eq.Java"
              [("Prefer", "return=representation")]
              [json| r#"[ { "name": "Java", "rank": 13 } ]"#|]
            shouldRespondWith
              [json| r#"[ { "name": "Java", "rank": 13 } ]"#|]

        //-- TODO: move this to SingularSpec?
        it "succeeds if the payload has more than one row, but it only puts the first element" $ do
          //-- assert that the next request will indeed be an update
          get "/tiobe_pls?name=eq.Java"
            shouldRespondWith
              [json|r#"[ { "name": "Java", "rank": 1 } ]"#|]

          request methodPut "/tiobe_pls?name=eq.Java"
              [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
              [json| r#"[ { "name": "Java", "rank": 19 }, { "name": "Swift", "rank": 12 } ]"# |]
            shouldRespondWith
              [json|r#"{ "name": "Java", "rank": 19 }"#|]
              { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

        it "succeeds on table with composite pk" $ do
          //-- assert that the next request will indeed be an update
          get "/employees?first_name=eq.Frances M.&last_name=eq.Roe"
            shouldRespondWith
              [json| r#"[ { "first_name": "Frances M.", "last_name": "Roe", "salary": "$24,000.00", "company": "One-Up Realty", "occupation": "Author" } ]"#|]

          request methodPut "/employees?first_name=eq.Frances M.&last_name=eq.Roe"
              [("Prefer", "return=representation")]
              [json| r#"[ { "first_name": "Frances M.", "last_name": "Roe", "salary": "60000", "company": "Gamma Gas", "occupation": "Railroad engineer" } ]"#|]
            shouldRespondWith
              [json| r#"[ { "first_name": "Frances M.", "last_name": "Roe", "salary": "$60,000.00", "company": "Gamma Gas", "occupation": "Railroad engineer" } ]"#|]

        //when (actualPgVersion >= pgVersion110) $
          it "succeeds on a partitioned table with composite pk" $ do
            //-- assert that the next request will indeed be an update
            get "/car_models?name=eq.DeLorean&year=eq.1981"
              shouldRespondWith
                [json| r#"[ { "name": "DeLorean", "year": 1981, "car_brand_name": "DMC" } ]"#|]

            request methodPut "/car_models?name=eq.DeLorean&year=eq.1981"
                [("Prefer", "return=representation")]
                [json| r#"[ { "name": "DeLorean", "year": 1981, "car_brand_name": null } ]"#|]
              shouldRespondWith
                [json| r#"[ { "name": "DeLorean", "year": 1981, "car_brand_name": null } ]"#|]

        it "succeeds if the table has only PK cols and no other cols" $ do
          //-- assert that the next request will indeed be an update
          get "/only_pk?id=eq.1"
            shouldRespondWith
              [json|r#"[ { "id": 1 } ]"#|]

          request methodPut "/only_pk?id=eq.1"
              [("Prefer", "return=representation")]
              [json|r#"[ { "id": 1 } ]"#|]
            shouldRespondWith
              [json|r#"[ { "id": 1 } ]"#|]

      //-- TODO: move this to SingularSpec?
      it "works with return=representation and vnd_pgrst_object_json" $
        request methodPut "/tiobe_pls?name=eq.Ruby"
          [("Prefer", "return=representation"), ("Accept", "application/vnd.pgrst.object+json")]
          [json| r#"[ { "name": "Ruby", "rank": 11 } ]"#|]
          shouldRespondWith [json|r#"{ "name": "Ruby", "rank": 11 }"#|] { matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

    describe "with a camel case pk column" $ do
      it "works with POST and merge-duplicates" $ do
        request methodPost "/UnitTest"
            [("Prefer", "return=representation"), ("Prefer", "resolution=merge-duplicates")]
            [json|r#"[
              { "idUnitTest": 1, "nameUnitTest": "name of unittest 1" },
              { "idUnitTest": 2, "nameUnitTest": "name of unittest 2" }
            ]"#|]
          shouldRespondWith
            [json|r#"[
              { "idUnitTest": 1, "nameUnitTest": "name of unittest 1" },
              { "idUnitTest": 2, "nameUnitTest": "name of unittest 2" }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=merge-duplicates"]
            }

      it "works with POST and ignore-duplicates headers" $ do
        request methodPost "/UnitTest"
            [("Prefer", "return=representation"), ("Prefer", "resolution=ignore-duplicates")]
            [json|r#"[
              { "idUnitTest": 1, "nameUnitTest": "name of unittest 1" },
              { "idUnitTest": 2, "nameUnitTest": "name of unittest 2" }
            ]"#|]
          shouldRespondWith
            [json|r#"[
              { "idUnitTest": 2, "nameUnitTest": "name of unittest 2" }
            ]"#|]
            { matchStatus = 201
            , matchHeaders = ["Preference-Applied" <:> "resolution=ignore-duplicates"]
            }

      it "works with PUT" $ do
        request methodPut "/UnitTest?idUnitTest=eq.1"
            [json| r#"[ { "idUnitTest": 1, "nameUnitTest": "unit test 1" } ]"#|]
          shouldRespondWith
            [text|""|]
            { matchStatus = 204
            , matchHeaders = [
              // matchHeaderAbsent hContentType
            ]
            }
        get "/UnitTest?idUnitTest=eq.1" shouldRespondWith
          [json| r#"[ { "idUnitTest": 1, "nameUnitTest": "unit test 1" } ]"#|]
}
