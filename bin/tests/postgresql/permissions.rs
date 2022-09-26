use super::super::start;
use super::common::*;
use async_once::AsyncOnce;
use demonstrate::demonstrate;
use rocket::local::asynchronous::Client;
use std::sync::Once;
static INIT_CLIENT: Once = Once::new();
use std::env;
lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { 
      env::set_var("SUBZERO_DB_SCHEMA_STRUCTURE", "{sql_file=../introspection/postgresql_introspection_query.sql}");
      env::set_var("SUBZERO_DB_SCHEMAS", "[public]");
      env::remove_var("SUBZERO_DB_PRE_REQUEST");
      env::remove_var("SUBZERO_DB_USE_LEGACY_GUCS");
      
      Client::untracked(start().await.unwrap()).await.expect("valid client") 
    });
}

haskell_test! {
feature "permissions"
    describe "grants" $ do
      it "admin can select all columns" $ do
        let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU"
        request methodGet "/permissions_check?select=id,value,hidden" [auth] ""
          shouldRespondWith
          { matchStatus = 200 }
      it "anonymous can select not select hidden column" $ do
        get "/permissions_check?select=id,value,hidden"
          shouldRespondWith
          [json|r#"{"details":"no Select privileges for 'public.permissions_check(hidden)'","message":"Permission denied"}"#|]
          { matchStatus = 403}
      it "anonymous can select permitted columns with rows filtered" $ do
        get "/permissions_check?select=id,value"
          shouldRespondWith
          { matchStatus = 200 }
      it "alice can select all columns" $ do
          let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk"
          request methodGet "/permissions_check?select=id,value,hidden,public,role" [auth] ""
            shouldRespondWith
            { matchStatus = 200 }
    describe "RLS" $ do
      it "admin can select all rows" $ do
        let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU"
        request methodGet "/permissions_check?select=id,value,hidden" [auth] ""
          shouldRespondWith
          [json|r#"[
            {"id":1,"value":"One Alice Public","hidden":"Hidden"},
            {"id":2,"value":"Two Bob Public","hidden":"Hidden"},
            {"id":3,"value":"Three Charlie Public","hidden":"Hidden"},
            {"id":10,"value":"Ten Alice Private","hidden":"Hidden"},
            {"id":20,"value":"Twenty Bob Private","hidden":"Hidden"}
          ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
      
      it "anonymous can select public rows" $ do
        get "/permissions_check?select=id,value"
          shouldRespondWith
          [json|r#"[
            {"id":1,"value":"One Alice Public"},
            {"id":2,"value":"Two Bob Public"},
            {"id":3,"value":"Three Charlie Public"}
          ]"#|]
          { matchStatus = 200 }
      it "alice can select public rows and her private rows" $ do
          let auth = authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk"
          request methodGet "/permissions_check?select=id,value,hidden,public,role" [auth] ""
            shouldRespondWith
            [json|r#"[
              {"id":1,"value":"One Alice Public","hidden":"Hidden","public":true,"role":"alice"},
              {"id":2,"value":"Two Bob Public","hidden":"Hidden","public":true,"role":"bob"},
              {"id":3,"value":"Three Charlie Public","hidden":"Hidden","public":true,"role":"charlie"},
              {"id":10,"value":"Ten Alice Private","hidden":"Hidden","public":false,"role":"alice"}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
    describe "insert" $
      it "admin can insert everything" $
        request methodPost "/permissions_check?select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU", ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Alice Private","hidden":"Hidden","public":false,"role":"alice"}"# |]
          shouldRespondWith
          [json|r#"[{"id":30,"value":"Thirty Alice Private","hidden":"Hidden","public":false,"role":"alice"}]"#|]
          { matchStatus  = 201 }
      it "alice can insert her private rows" $
        request methodPost "/permissions_check?select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Alice Private","hidden":"Hidden","public":false,"role":"alice"}"# |]
          shouldRespondWith
          [json|r#"[{"id":30,"value":"Thirty Alice Private","hidden":"Hidden","public":false,"role":"alice"}]"#|]
          { matchStatus  = 201 }
      it "alice can not insert rows for bob" $
        request methodPost "/permissions_check?select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Bob Private","hidden":"Hidden","public":false,"role":"bob"}"# |]
          shouldRespondWith
          [json|r#"{"details":"check constraint of an insert/update permission has failed","message":"Permission denied"}"#|]
          { matchStatus  = 403 }
      it "anonymous can not insert rows for bob" $
        request methodPost "/permissions_check?select=id,value"
          [ ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Bob Private","hidden":"Hidden","public":false,"role":"bob"}"# |]
          shouldRespondWith
          [json|r#"{"details":"no Insert privileges for 'public.permissions_check' table","message":"Permission denied"}"#|]
          { matchStatus  = 403 }
    describe "update" $
      it "admin can update everything" $
        request methodPatch "/permissions_check?id=eq.10&select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU", ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed"}"# |]
          shouldRespondWith
          [json|r#"[{"id":10,"value":"Ten Alice Private","hidden":"Hidden changed","public":false,"role":"alice"}]"#|]
          { matchStatus  = 200 }
        request methodPatch "/permissions_check?select=id"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU", ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed"}"# |]
          shouldRespondWith
          [json|r#"[{"id":1},{"id":2},{"id":3},{"id":10},{"id":20}]"#|]
          { matchStatus  = 200 }
      it "alice can update her private rows" $
        request methodPatch "/permissions_check?id=eq.10&select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed","public":true}"# |]
          shouldRespondWith
          [json|r#"[{"id":10,"value":"Ten Alice Private","hidden":"Hidden changed","public":true,"role":"alice"}]"#|]
          { matchStatus  = 200 }
        request methodPatch "/permissions_check?select=id"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed","public":true}"# |]
          shouldRespondWith
          [json|r#"[{"id":1},{"id":10}]"#|]
          { matchStatus  = 200 }
      it "alice can not update rows for bob even if they are public" $
        request methodPatch "/permissions_check?id=eq.2&select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed"}"# |]
          shouldRespondWith
          [json|r#"[]"#|]
          { matchStatus  = 404 }
      it "anonymous can not update rows for bob" $
        request methodPatch "/permissions_check?id=eq.10&select=id,value"
          [ ("Prefer", "return=representation") ]
          [json| r#"{"hidden":"Hidden changed"}"# |]
          shouldRespondWith
          [json|r#"{"details":"no Update privileges for 'public.permissions_check' table","message":"Permission denied"}"#|]
          { matchStatus  = 403 }
    describe "validation" $
      it "admin can not insert invalid values for hidden" $
        request methodPost "/permissions_check?select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWRtaW4ifQ.aMYD4kILQ5BBlRNB3HvK55sfex_OngpB_d28iAMq-WU", ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Alice Private","hidden":"Hidden invalid","public":false,"role":"alice"}"# |]
          shouldRespondWith
          [json|r#"{"details":"check constraint of an insert/update permission has failed","message":"Permission denied"}"#|]
          { matchStatus  = 403 }
      it "alice can not insert invalid values for hidden" $
        request methodPost "/permissions_check?select=id,value,hidden,public,role"
          [ authHeaderJWT "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoiYWxpY2UifQ.BHodFXgm4db4iFEIBdrFUdfmlNST3Ff9ilrfotJO1Jk", ("Prefer", "return=representation") ]
          [json| r#"{"id":30,"value":"Thirty Alice Private","hidden":"Hidden invalid","public":false,"role":"alice"}"# |]
          shouldRespondWith
          [json|r#"{"details":"check constraint of an insert/update permission has failed","message":"Permission denied"}"#|]
          { matchStatus  = 403 }
      
}
