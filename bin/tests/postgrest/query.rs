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
feature "query"
  describe "Querying a table with a column called count" $
    it "should not confuse count column with pg_catalog count aggregate" $
      get "/has_count_column" shouldRespondWith 200

  describe "Querying a table with a column called t" $
    it "should not conflict with internal postgrest table alias" $
      get "/clashing_column?select=t" shouldRespondWith 200

  describe "Querying a nonexistent table" $
    it "causes a 404" $
      get "/faketable" shouldRespondWith 404

  describe "Filtering response" $ do
    it "matches with equality" $
      get "/items?id=eq.5"
        shouldRespondWith [json|r#" [{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-0/*"] }

    it "matches with equality using not operator" $
      get "/items?id=not.eq.5&order=id"
        shouldRespondWith [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-13/*"] }

    it "matches with more than one condition using not operator" $
      get "/simple_pk?k=like.*yx&extra=not.eq.u" shouldRespondWith [json|"[]"|]

    it "matches with inequality using not operator" $ do
      get "/items?id=not.lt.14&order=id.asc"
        shouldRespondWith [json|r#" [{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }
      get "/items?id=not.gt.2&order=id.asc"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

    it "matches items IN" $
      get "/items?id=in.(1,3,5)"
        shouldRespondWith [json|r#" [{"id":1},{"id":3},{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-2/*"] }

    it "matches items NOT IN using not operator" $
      get "/items?id=not.in.(2,4,6,7,8,9,10,11,12,13,14,15)"
        shouldRespondWith [json|r#" [{"id":1},{"id":3},{"id":5}] "#|]
        { matchHeaders = ["Content-Range" <:> "0-2/*"] }

    it "matches nulls using not operator" $
      get "/no_pk?a=not.is.null" shouldRespondWith
        [json|r#" [{"a":"1","b":"0"},{"a":"2","b":"0"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches nulls in varchar and numeric fields alike" $ do
      get "/no_pk?a=is.null" shouldRespondWith
        [json|r#" [{"a": null, "b": null}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

      get "/nullable_integer?a=is.null" shouldRespondWith [json|r#"[{"a":null}]"#|]

    it "matches with like" $ do
      get "/simple_pk?k=like.*yx" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"}]"#|]
      get "/simple_pk?k=like.xy*" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"}]"#|]
      get "/simple_pk?k=like.*YY*" shouldRespondWith
        [json|r#"[{"k":"xYYx","extra":"v"}]"#|]

    it "matches with like using not operator" $
      get "/simple_pk?k=not.like.*yx" shouldRespondWith
        [json|r#"[{"k":"xYYx","extra":"v"}]"#|]

    it "matches with ilike" $ do
      get "/simple_pk?k=ilike.xy*&order=extra.asc" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"},{"k":"xYYx","extra":"v"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/simple_pk?k=ilike.*YY*&order=extra.asc" shouldRespondWith
        [json|r#"[{"k":"xyyx","extra":"u"},{"k":"xYYx","extra":"v"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with ilike using not operator" $
      get "/simple_pk?k=not.ilike.xy*&order=extra.asc" shouldRespondWith [json|"[]"|]

    describe "Full text search operator" $ do
      it "finds matches with to_tsquery" $
        get "/tsearch?text_search_vector=fts.impossible" shouldRespondWith
          [json|r#" [{"text_search_vector": "'fun':5 'imposs':9 'kind':3" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can use lexeme boolean operators(&=%26, |=%7C, !) in to_tsquery" $ do
        get "/tsearch?text_search_vector=fts.fun%26possible" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=fts.impossible%7Cpossible" shouldRespondWith
          [json|r#" [
          {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
          {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=fts.fun%26!possible" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'fun':5 'imposs':9 'kind':3"}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "finds matches with plainto_tsquery" $
        get "/tsearch?text_search_vector=plfts.The%20Fat%20Rats" shouldRespondWith
          [json|r#" [ {"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      //when (actualPgVersion >= pgVersion112) $ do
        it "finds matches with websearch_to_tsquery" $
            get "/tsearch?text_search_vector=wfts.The%20Fat%20Rats" shouldRespondWith
                [json|r#" [ {"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
                { matchHeaders = ["Content-Type" <:> "application/json"] }

        it "can use boolean operators(and, or, -) in websearch_to_tsquery" $ do
          get "/tsearch?text_search_vector=wfts.fun%20and%20possible"
            shouldRespondWith
              [json|r#" [ {"text_search_vector": "'also':2 'fun':3 'possibl':8"}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/tsearch?text_search_vector=wfts.impossible%20or%20possible"
            shouldRespondWith
              [json|r#" [
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
                {"text_search_vector": "'also':2 'fun':3 'possibl':8"}]
                  "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }
          get "/tsearch?text_search_vector=wfts.fun%20and%20-possible"
            shouldRespondWith
              [json|r#" [ {"text_search_vector": "'fun':5 'imposs':9 'kind':3"}] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "finds matches with different dictionaries" $ do
        get "/tsearch?text_search_vector=fts(french).amusant" shouldRespondWith
          [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=plfts(french).amusant%20impossible" shouldRespondWith
          [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

        //when (actualPgVersion >= pgVersion112) $
            get "/tsearch?text_search_vector=wfts(french).amusant%20impossible"
                shouldRespondWith
                  [json|r#" [{"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" }] "#|]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can be negated with not operator" $ do
        get "/tsearch?text_search_vector=not.fts.impossible%7Cfat%7Cfun" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=not.fts(english).impossible%7Cfat%7Cfun" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/tsearch?text_search_vector=not.plfts.The%20Fat%20Rats" shouldRespondWith
          [json|r#" [
            {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
            {"text_search_vector": "'also':2 'fun':3 'possibl':8"},
            {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
            {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        //when (actualPgVersion >= pgVersion112) $
            get "/tsearch?text_search_vector=not.wfts(english).impossible%20or%20fat%20or%20fun"
                shouldRespondWith
                  [json|r#" [
                    {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
                    {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
                  { matchHeaders = ["Content-Type" <:> "application/json"] }

//       //when (actualPgVersion >= pgVersion96) $
        describe "Use of the phraseto_tsquery function" $ do
          it "finds matches" $
            get "/tsearch?text_search_vector=phfts.The%20Fat%20Cats" shouldRespondWith
              [json|r#" [{"text_search_vector": "'ate':3 'cat':2 'fat':1 'rat':4" }] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "finds matches with different dictionaries" $
            get "/tsearch?text_search_vector=phfts(german).Art%20Spass" shouldRespondWith
              [json|r#" [{"text_search_vector": "'art':4 'spass':5 'unmog':7" }] "#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can be negated with not operator" $
            get "/tsearch?text_search_vector=not.phfts(english).The%20Fat%20Cats" shouldRespondWith
              [json|r#" [
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3"},
                {"text_search_vector": "'also':2 'fun':3 'possibl':8"},
                {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4"},
                {"text_search_vector": "'art':4 'spass':5 'unmog':7"}]"#|]
              { matchHeaders = ["Content-Type" <:> "application/json"] }

          it "can be used with or query param" $
            get "/tsearch?or=(text_search_vector.phfts(german).Art%20Spass, text_search_vector.phfts(french).amusant, text_search_vector.fts(english).impossible)" shouldRespondWith
              [json|r#"[
                {"text_search_vector": "'fun':5 'imposs':9 'kind':3" },
                {"text_search_vector": "'amus':5 'fair':7 'impossibl':9 'peu':4" },
                {"text_search_vector": "'art':4 'spass':5 'unmog':7"}
              ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with computed column" $
      get "/items?always_true=eq.true&order=id.asc" shouldRespondWith
        [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":5},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    // it "order by computed column" $
    //   get "/items?order=anti_id.desc" shouldRespondWith
    //     [json|r#" [{"id":1},{"id":2},{"id":3},{"id":4},{"id":5},{"id":6},{"id":7},{"id":8},{"id":9},{"id":10},{"id":11},{"id":12},{"id":13},{"id":14},{"id":15}] "#|]
    //     { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "cannot access a computed column that is outside of the config schema" $
      get "/items?always_false=is.false" shouldRespondWith 400

    it "matches filtering nested items 2" $
      get "/clients?select=id,projects(id,tasks2(id,name))&projects.tasks.name=like.Design*" shouldRespondWith
        [json|r#"{"message":"Could not find foreign keys between these entities. No relationship found between projects and tasks2"}"#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "matches filtering nested items" $
      get "/clients?select=id,projects(id,tasks(id,name))&projects.tasks.name=like.Design*" shouldRespondWith
        [json|r#"[{"id":1,"projects":[{"id":1,"tasks":[{"id":1,"name":"Design w7"}]},{"id":2,"tasks":[{"id":3,"name":"Design w10"}]}]},{"id":2,"projects":[{"id":3,"tasks":[{"id":5,"name":"Design IOS"}]},{"id":4,"tasks":[{"id":7,"name":"Design OSX"}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with cs operator" $
      get "/complex_items?select=id&arr_data=cs.{2}" shouldRespondWith
        [json|r#"[{"id":2},{"id":3}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "matches with cd operator" $
      get "/complex_items?select=id&arr_data=cd.{1,2,4}" shouldRespondWith
        [json|r#"[{"id":1},{"id":2}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "Shaping response with select parameter" $ do
    it "selectStar works in absense of parameter" $
      get "/complex_items?id=eq.3" shouldRespondWith
        [json|r#"[{"id":3,"name":"Three","settings":{"foo":{"int":1,"bar":"baz"}},"arr_data":[1,2,3],"field-with_sep":1}]"#|]

    it "dash `-` in column names is accepted" $
      get "/complex_items?id=eq.3&select=id,field-with_sep" shouldRespondWith
        [json|r#"[{"id":3,"field-with_sep":1}]"#|]

    it "one simple column" $
      get "/complex_items?select=id" shouldRespondWith
        [json|r#" [{"id":1},{"id":2},{"id":3}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "rename simple column" $
      get "/complex_items?id=eq.1&select=myId:id" shouldRespondWith
        [json|r#" [{"myId":1}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "one simple column with casting (text)" $
      get "/complex_items?select=id::text" shouldRespondWith
        [json|r#" [{"id":"1"},{"id":"2"},{"id":"3"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "rename simple column with casting" $
      get "/complex_items?id=eq.1&select=myId:id::text" shouldRespondWith
        [json|r#" [{"myId":"1"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "json column" $
      get "/complex_items?id=eq.1&select=settings" shouldRespondWith
        [json|r#" [{"settings":{"foo":{"int":1,"bar":"baz"}}}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails on bad casting (wrong cast type)" $
      get "/complex_items?select=id::fakecolumntype"
        shouldRespondWith [json|r#" {"hint":null,"details":null,"code":"42704","message":"type \"fakecolumntype\" does not exist"} "#|]
        { matchStatus  = 400
        , matchHeaders = []
        }

    it "requesting parents and children" $
      get "/projects?id=eq.1&select=id, name, clients(*), tasks(id, name)" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parent and renaming primary key" $
      get "/projects?select=name,client:clients(clientId:id,name)" shouldRespondWith
        [json|r#"[
          {"name":"Windows 7","client":{"name": "Microsoft", "clientId": 1}},
          {"name":"Windows 10","client":{"name": "Microsoft", "clientId": 1}},
          {"name":"IOS","client":{"name": "Apple", "clientId": 2}},
          {"name":"OSX","client":{"name": "Apple", "clientId": 2}},
          {"name":"Orphan","client":null}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parent and specifying/renaming one key of the composite primary key" $ do
      get "/comments?select=*,users_tasks(userId:user_id)" shouldRespondWith
        [json|r#"[{"id":1,"commenter_id":1,"user_id":2,"task_id":6,"content":"Needs to be delivered ASAP","users_tasks":{"userId": 2}}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/comments?select=*,users_tasks(taskId:task_id)" shouldRespondWith
        [json|r#"[{"id":1,"commenter_id":1,"user_id":2,"task_id":6,"content":"Needs to be delivered ASAP","users_tasks":{"taskId": 6}}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parents and children while renaming them" $
      get "/projects?id=eq.1&select=myId:id, name, project_client:clients(*), project_tasks:tasks(id, name)" shouldRespondWith
        [json|r#"[{"myId":1,"name":"Windows 7","project_client":{"id":1,"name":"Microsoft"},"project_tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting parents and filtering parent columns" $
      get "/projects?id=eq.1&select=id, name, clients(id)" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1}}]"#|]

    it "rows with missing parents are included" $
      get "/projects?id=in.(1,5)&select=id,clients(id)" shouldRespondWith
        [json|r#"[{"id":1,"clients":{"id":1}},{"id":5,"clients":null}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "rows with no children return [] instead of null" $
      get "/projects?id=in.(5)&select=id,tasks(id)" shouldRespondWith
        [json|r#"[{"id":5,"tasks":[]}]"#|]

    it "requesting children 2 levels" $
      get "/clients?id=eq.1&select=id,projects(id,tasks(id))" shouldRespondWith
        [json|r#"[{"id":1,"projects":[{"id":1,"tasks":[{"id":1},{"id":2}]},{"id":2,"tasks":[{"id":3},{"id":4}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation" $
      get "/tasks?select=id,users(id)" shouldRespondWith
        [json|r#"[{"id":1,"users":[{"id":1},{"id":3}]},{"id":2,"users":[{"id":1}]},{"id":3,"users":[{"id":1}]},{"id":4,"users":[{"id":1}]},{"id":5,"users":[{"id":2},{"id":3}]},{"id":6,"users":[{"id":2}]},{"id":7,"users":[{"id":2}]},{"id":8,"users":[]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation with rename" $
      get "/tasks?id=eq.1&select=id,theUsers:users(id)" shouldRespondWith
        [json|r#"[{"id":1,"theUsers":[{"id":1},{"id":3}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation reverse" $
      get "/users?select=id,tasks(id)" shouldRespondWith
        [json|r#"[{"id":1,"tasks":[{"id":1},{"id":2},{"id":3},{"id":4}]},{"id":2,"tasks":[{"id":5},{"id":6},{"id":7}]},{"id":3,"tasks":[{"id":1},{"id":5}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting many<->many relation using composite key" $
      get "/files?filename=eq.autoexec.bat&project_id=eq.1&select=filename,users_tasks(user_id,task_id)" shouldRespondWith
        [json|r#"[{"filename":"autoexec.bat","users_tasks":[{"user_id":1,"task_id":1},{"user_id":3,"task_id":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting data using many<->many relation defined by composite keys" $
      get "/users_tasks?user_id=eq.1&task_id=eq.1&select=user_id,files(filename,content)" shouldRespondWith
        [json|r##"[{"user_id":1,"files":[{"filename":"command.com","content":"#include <unix.h>"},{"filename":"autoexec.bat","content":"@ECHO OFF"},{"filename":"README.md","content":"# make $$$!"}]}]"##|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting data using many<->many (composite keys) relation using hint" $
      get "/users_tasks?user_id=eq.1&task_id=eq.1&select=user_id,files!touched_files(filename,content)" shouldRespondWith
        [json|r##"[{"user_id":1,"files":[{"filename":"command.com","content":"#include <unix.h>"},{"filename":"autoexec.bat","content":"@ECHO OFF"},{"filename":"README.md","content":"# make $$$!"}]}]"##|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "requesting children with composite key" $
      get "/users_tasks?user_id=eq.2&task_id=eq.6&select=*, comments(content)" shouldRespondWith
        [json|r#"[{"user_id":2,"task_id":6,"comments":[{"content":"Needs to be delivered ASAP"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "computed columns" $ do
      it "computed column on table" $
        get "/items?id=eq.1&select=id,always_true" shouldRespondWith
          [json|r#"[{"id":1,"always_true":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "computed column on rpc" $
        get "/rpc/search?id=1&select=id,always_true" shouldRespondWith
          [json|r#"[{"id":1,"always_true":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "overloaded computed columns on both tables" $ do
        get "/items?id=eq.1&select=id,computed_overload" shouldRespondWith
          [json|r#"[{"id":1,"computed_overload":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/items2?id=eq.1&select=id,computed_overload" shouldRespondWith
          [json|r#"[{"id":1,"computed_overload":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "overloaded computed column on rpc" $
        get "/rpc/search?id=1&select=id,computed_overload" shouldRespondWith
          [json|r#"[{"id":1,"computed_overload":true}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

   describe "partitioned tables embedding" $ do
    it "can request a table as parent from a partitioned table" $
      get "/car_models?name=in.(DeLorean,Murcielago)&select=name,year,car_brands(name)&order=name.asc" shouldRespondWith
        [json|
          r#"[{"name":"DeLorean","year":1981,"car_brands":{"name":"DMC"}},
            {"name":"Murcielago","year":2001,"car_brands":{"name":"Lamborghini"}}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can request partitioned tables as children from a table" $
      get "/car_brands?select=name,car_models(name,year)&order=name.asc&car_models.order=name.asc" shouldRespondWith
        [json|
          r#"[{"name":"DMC","car_models":[{"name":"DeLorean","year":1981}]},
            {"name":"Ferrari","car_models":[{"name":"F310-B","year":1997}]},
            {"name":"Lamborghini","car_models":[{"name":"Murcielago","year":2001},{"name":"Veneno","year":2013}]}]"# |]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    //when (actualPgVersion >= pgVersion121) $ do
      it "can request tables as children from a partitioned table" $
        get "/car_models?name=in.(DeLorean,F310-B)&select=name,year,car_racers(name)&order=name.asc" shouldRespondWith
          [json|
            r#"[{"name":"DeLorean","year":1981,"car_racers":[]},
              {"name":"F310-B","year":1997,"car_racers":[{"name":"Michael Schumacher"}]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can request a partitioned table as parent from a table" $
        get "/car_racers?select=name,car_models(name,year)&order=name.asc" shouldRespondWith
          [json|
            r#"[{"name":"Alain Prost","car_models":null},
              {"name":"Michael Schumacher","car_models":{"name":"F310-B","year":1997}}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can request partitioned tables as children from a partitioned table" $
        get "/car_models?name=in.(DeLorean,Murcielago,Veneno)&select=name,year,car_model_sales(date,quantity)&order=name.asc" shouldRespondWith
          [json|
            r#"[{"name":"DeLorean","year":1981,"car_model_sales":[{"date":"2021-01-14","quantity":7},{"date":"2021-01-15","quantity":9}]},
              {"name":"Murcielago","year":2001,"car_model_sales":[{"date":"2021-02-11","quantity":1},{"date":"2021-02-12","quantity":3}]},
              {"name":"Veneno","year":2013,"car_model_sales":[]}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can request a partitioned table as parent from a partitioned table" $ do
        get "/car_model_sales?date=in.(2021-01-15,2021-02-11)&select=date,quantity,car_models(name,year)&order=date.asc" shouldRespondWith
          [json|
            r#"[{"date":"2021-01-15","quantity":9,"car_models":{"name":"DeLorean","year":1981}},
              {"date":"2021-02-11","quantity":1,"car_models":{"name":"Murcielago","year":2001}}]"# |]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can request many to many relationships between partitioned tables ignoring the intermediate table partitions" $
        get "/car_models?select=name,year,car_dealers(name,city)&order=name.asc&limit=4" shouldRespondWith
          [json|
            r#"[{"name":"DeLorean","year":1981,"car_dealers":[{"name":"Springfield Cars S.A.","city":"Springfield"}]},
              {"name":"F310-B","year":1997,"car_dealers":[]},
              {"name":"Murcielago","year":2001,"car_dealers":[{"name":"The Best Deals S.A.","city":"Franklin"}]},
              {"name":"Veneno","year":2013,"car_dealers":[]}]"# |]
          { matchStatus  = 200
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "cannot request partitions as children from a partitioned table" $
        get "/car_models?id=in.(1,2,4)&select=id,name,car_model_sales_202101(id)&order=id.asc" shouldRespondWith
          //[json|
          //  r#"{"hint":"Verify that 'car_models' and 'car_model_sales_202101' exist in the schema 'test' and that there is a foreign key relationship between them. If a new relationship was created, try reloading the schema cache.",
          //    "message":"Could not find a relationship between 'car_models' and 'car_model_sales_202101' in the schema cache"}"# |]
          [json|r#"{"message":"Could not find foreign keys between these entities. No relationship found between car_models and car_model_sales_202101"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "cannot request a partitioned table as parent from a partition" $
        get "/car_model_sales_202101?select=id,name,car_models(id,name)&order=id.asc" shouldRespondWith
          //[json|
          //  r#"{"hint":"Verify that 'car_model_sales_202101' and 'car_models' exist in the schema 'test' and that there is a foreign key relationship between them. If a new relationship was created, try reloading the schema cache.",
          //    "message":"Could not find a relationship between 'car_model_sales_202101' and 'car_models' in the schema cache"}"# |]
          [json|r#"{"message":"Entry 'car_model_sales_202101' not found"}"#|]
          { matchStatus  = 404
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "cannot request a partition as parent from a partitioned table" $
        get "/car_model_sales?id=in.(1,3,4)&select=id,name,car_models_default(id,name)&order=id.asc" shouldRespondWith
          //[json|
          //  r#"{"hint":"Verify that 'car_model_sales' and 'car_models_default' exist in the schema 'test' and that there is a foreign key relationship between them. If a new relationship was created, try reloading the schema cache.",
          //    "message":"Could not find a relationship between 'car_model_sales' and 'car_models_default' in the schema cache"}"# |]
          [json|r#"{"message":"Could not find foreign keys between these entities. No relationship found between car_model_sales and car_models_default"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "cannot request partitioned tables as children from a partition" $
        get "/car_models_default?select=id,name,car_model_sales(id,name)&order=id.asc" shouldRespondWith
          //[json|
          //  r#"{"hint":"Verify that 'car_models_default' and 'car_model_sales' exist in the schema 'test' and that there is a foreign key relationship between them. If a new relationship was created, try reloading the schema cache.",
          //    "message":"Could not find a relationship between 'car_models_default' and 'car_model_sales' in the schema cache"}"# |]
          [json|r#"{"message":"Entry 'car_models_default' not found"}"#|]
          { matchStatus  = 404
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }


    describe "view embedding" $ do
      it "can detect fk relations through views to tables in the public schema" $
        get "/consumers_view?select=*,orders_view(*)" shouldRespondWith 200

      it "can detect fk relations through materialized views to tables in the public schema" $
        get "/materialized_projects?select=*,users(*)" shouldRespondWith 200

      it "can request two parents" $
        get "/articleStars?select=createdAt,article:articles(id),user:users(name)&limit=1"
          shouldRespondWith
            [json|r#"[{"createdAt":"2015-12-08T04:22:57.472738","article":{"id": 1},"user":{"name": "Angela Martin"}}]"#|]

      it "can detect relations in views from exposed schema that are based on tables in private schema and have columns renames" $
        get "/articles?id=eq.1&select=id,articleStars(users(*))" shouldRespondWith
          [json|r#"[{"id":1,"articleStars":[{"users":{"id":1,"name":"Angela Martin"}},{"users":{"id":2,"name":"Michael Scott"}},{"users":{"id":3,"name":"Dwight Schrute"}}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when requesting parents and children on views" $
        get "/projects_view?id=eq.1&select=id, name, clients(*), tasks(id, name)" shouldRespondWith
          [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when requesting parents and children on views with renamed keys" $
        get "/projects_view_alt?t_id=eq.1&select=t_id, name, clients(*), tasks(id, name)" shouldRespondWith
          [json|r#"[{"t_id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"},"tasks":[{"id":1,"name":"Design w7"},{"id":2,"name":"Code w7"}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "detects parent relations when having many views of a private table" $ do
        get "/books?select=title,author:authors(name)&id=eq.5" shouldRespondWith
          [json|r#"[ { "title": "Farenheit 451", "author": { "name": "Ray Bradbury" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/forties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "1984", "author": { "name": "George Orwell" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/fifties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "The Catcher in the Rye", "author": { "name": "J.D. Salinger" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/sixties_books?select=title,author:authors(name)&limit=1" shouldRespondWith
          [json|r#"[ { "title": "To Kill a Mockingbird", "author": { "name": "Harper Lee" } } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can detect fk relations through multiple views recursively when all views are in api schema" $ do
        get "/consumers_view_view?select=*,orders_view(*)" shouldRespondWith 200

      it "works with views that have subselects" $
        get "/authors_books_number?select=*,books(title)&id=eq.1" shouldRespondWith
          [json|r#"[ {"id":1, "name":"George Orwell","num_in_forties":1,"num_in_fifties":0,"num_in_sixties":0,"num_in_all_decades":1,
                   "books":[{"title":"1984"}]} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have case subselects" $
        get "/authors_have_book_in_decade?select=*,books(title)&id=eq.3" shouldRespondWith
          [json|r#"[ {"id":3,"name":"Antoine de Saint-Exupéry","has_book_in_forties":true,"has_book_in_fifties":false,"has_book_in_sixties":false,
                   "books":[{"title":"The Little Prince"}]} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have subselect in the FROM clause" $
        get "/forties_and_fifties_books?select=title,first_publisher,author:authors(name)&id=eq.1" shouldRespondWith
          [json|r#"[{"title":"1984","first_publisher":"Secker & Warburg","author":{"name":"George Orwell"}}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with views that have subselects in a function call" $
        get "/authors_have_book_in_decade2?select=*,books(title)&id=eq.3"
          shouldRespondWith
            [json|r#"[ {"id":3,"name":"Antoine de Saint-Exupéry","has_book_in_forties":true,"has_book_in_fifties":false,
                     "has_book_in_sixties":false,"books":[{"title":"The Little Prince"}]} ]"#|]

      it "works with views that have CTE" $
        get "/odd_years_publications?select=title,publication_year,first_publisher,author:authors(name)&id=in.(1,2,3)" shouldRespondWith
          [json|r#"[
            {"title":"1984","publication_year":1949,"first_publisher":"Secker & Warburg","author":{"name":"George Orwell"}},
            {"title":"The Diary of a Young Girl","publication_year":1947,"first_publisher":"Contact Publishing","author":{"name":"Anne Frank"}},
            {"title":"The Little Prince","publication_year":1947,"first_publisher":"Reynal & Hitchcock","author":{"name":"Antoine de Saint-Exupéry"}} ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when having a capitalized table name and camelCase fk column" $
        get "/foos?select=*,bars(*)" shouldRespondWith 200

      it "works when embedding a view with a table that has a long compound pk" $ do
        get "/player_view?select=id,contract(purchase_price)&id=in.(1,3,5,7)" shouldRespondWith
          [json|r#"
            [{"id":1,"contract":[{"purchase_price":10}]},
             {"id":3,"contract":[{"purchase_price":30}]},
             {"id":5,"contract":[{"purchase_price":50}]},
             {"id":7,"contract":[]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/contract?select=tournament,player_view(first_name)&limit=3" shouldRespondWith
          [json|r#"
            [{"tournament":"tournament_1","player_view":{"first_name":"first_name_1"}},
             {"tournament":"tournament_2","player_view":{"first_name":"first_name_2"}},
             {"tournament":"tournament_3","player_view":{"first_name":"first_name_3"}}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works when embedding a view with a view that referes to a table that has a long compound pk" $ do
        get "/player_view?select=id,contract_view(purchase_price)&id=in.(1,3,5,7)" shouldRespondWith
          [json|r#"
            [{"id":1,"contract_view":[{"purchase_price":10}]},
             {"id":3,"contract_view":[{"purchase_price":30}]},
             {"id":5,"contract_view":[{"purchase_price":50}]},
             {"id":7,"contract_view":[]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/contract_view?select=tournament,player_view(first_name)&limit=3" shouldRespondWith
          [json|r#"
            [{"tournament":"tournament_1","player_view":{"first_name":"first_name_1"}},
             {"tournament":"tournament_2","player_view":{"first_name":"first_name_2"}},
             {"tournament":"tournament_3","player_view":{"first_name":"first_name_3"}}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can embed a view that has group by" $
        get "/projects_count_grouped_by?select=number_of_projects,client:clients(name)&order=number_of_projects" shouldRespondWith
          [json|r#"
            [
            {"number_of_projects":1,"client":null},
            {"number_of_projects":2,"client":{"name":"Apple"}},
            {"number_of_projects":2,"client":{"name":"Microsoft"}}
            ] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "can embed a view that has a subselect containing a select in a where" $
        get "/authors_w_entities?select=name,entities,books(title)&id=eq.1" shouldRespondWith
          [json|r#" [{"name":"George Orwell","entities":[3, 4],"books":[{"title":"1984"}]}] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "aliased embeds" $ do
      it "works with child relation" $
        get "/space?select=id,zones:zone(id,name),stores:zone(id,name)&zones.zone_type_id=eq.2&stores.zone_type_id=eq.3" shouldRespondWith
          [json|r#"[
            { "id":1,
              "zones": [ {"id":1,"name":"zone 1"}, {"id":2,"name":"zone 2"}],
              "stores": [ {"id":3,"name":"store 3"}, {"id":4,"name":"store 4"}]}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with many to many relation" $
        get "/users?select=id,designTasks:tasks(id,name),codeTasks:tasks(id,name)&designTasks.name=like.*Design*&codeTasks.name=like.*Code*" shouldRespondWith
          [json|r#"[
             { "id":1,
               "designTasks":[ { "id":1, "name":"Design w7" }, { "id":3, "name":"Design w10" } ],
               "codeTasks":[ { "id":2, "name":"Code w7" }, { "id":4, "name":"Code w10" } ] },
             { "id":2,
               "designTasks":[ { "id":5, "name":"Design IOS" }, { "id":7, "name":"Design OSX" } ],
               "codeTasks":[ { "id":6, "name":"Code IOS" } ] },
             { "id":3,
               "designTasks":[ { "id":1, "name":"Design w7" }, { "id":5, "name":"Design IOS" } ],
               "codeTasks":[ ] }
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with an aliased child plus non aliased child" $
        get "/projects?select=id,name,designTasks:tasks(name,users(id,name))&designTasks.name=like.*Design*&designTasks.users.id=in.(1,2)" shouldRespondWith
          [json|r#"[
            {
              "id":1, "name":"Windows 7",
              "designTasks":[ { "name":"Design w7", "users":[ { "id":1, "name":"Angela Martin" } ] } ] },
            {
              "id":2, "name":"Windows 10",
              "designTasks":[ { "name":"Design w10", "users":[ { "id":1, "name":"Angela Martin" } ] } ] },
            {
              "id":3, "name":"IOS",
              "designTasks":[ { "name":"Design IOS", "users":[ { "id":2, "name":"Michael Scott" } ] } ] },
            {
              "id":4, "name":"OSX",
              "designTasks":[ { "name":"Design OSX", "users":[ { "id":2, "name":"Michael Scott" } ] } ] },
            {
              "id":5, "name":"Orphan",
              "designTasks":[ ] }
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "works with two aliased children embeds plus and/or" $
        get "/entities?select=id,children:child_entities(id,gChildren:grandchild_entities(id))&children.and=(id.in.(1,2,3))&children.gChildren.or=(id.eq.1,id.eq.2)" shouldRespondWith
          [json|r#"[
            { "id":1,
              "children":[
                {"id":1,"gChildren":[{"id":1}, {"id":2}]},
                {"id":2,"gChildren":[]}]},
            { "id":2,
              "children":[
                {"id":3,"gChildren":[]}]},
            { "id":3,"children":[]},
            { "id":4,"children":[]}
          ]"#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "ordering response" $ do
    it "by a column asc" $
      get "/items?id=lte.2&order=id.asc"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }


    it "by a column desc" $
      get "/items?id=lte.2&order=id.desc"
        shouldRespondWith [json|r#" [{"id":2},{"id":1}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }

    it "by a column with nulls first" $
      get "/no_pk?order=a.nullsfirst"
        shouldRespondWith [json|r#" [{"a":null,"b":null},
                              {"a":"1","b":"0"},
                              {"a":"2","b":"0"}
                              ] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column asc with nulls last" $
      get "/no_pk?order=a.asc.nullslast"
        shouldRespondWith [json|r#" [{"a":"1","b":"0"},
                              {"a":"2","b":"0"},
                              {"a":null,"b":null}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column desc with nulls first" $
      get "/no_pk?order=a.desc.nullsfirst"
        shouldRespondWith [json|r#" [{"a":null,"b":null},
                              {"a":"2","b":"0"},
                              {"a":"1","b":"0"}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by a column desc with nulls last" $
      get "/no_pk?order=a.desc.nullslast"
        shouldRespondWith [json|r#" [{"a":"2","b":"0"},
                              {"a":"1","b":"0"},
                              {"a":null,"b":null}] "#|]
        { matchStatus = 200
        , matchHeaders = ["Content-Range" <:> "0-2/*"]
        }

    it "by two columns with nulls and direction specified" $
      get "/projects?select=client_id,id,name&order=client_id.desc.nullslast,id.desc"
        shouldRespondWith [json|r#"
          [{"client_id":2,"id":4,"name":"OSX"},
           {"client_id":2,"id":3,"name":"IOS"},
           {"client_id":1,"id":2,"name":"Windows 10"},
           {"client_id":1,"id":1,"name":"Windows 7"},
           {"client_id":null,"id":5,"name":"Orphan"}]
        "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-4/*"]
        }

    it "by a column with no direction or nulls specified" $
      get "/items?id=lte.2&order=id"
        shouldRespondWith [json|r#" [{"id":1},{"id":2}] "#|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Range" <:> "0-1/*"]
        }

    it "without other constraints" $
      get "/items?order=id.asc" shouldRespondWith 200

    it "ordering embeded entities" $
      get "/projects?id=eq.1&select=id, name, tasks(id, name)&tasks.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","tasks":[{"id":2,"name":"Code w7"},{"id":1,"name":"Design w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded entities with alias" $
      get "/projects?id=eq.1&select=id, name, the_tasks:tasks(id, name)&tasks.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","the_tasks":[{"id":2,"name":"Code w7"},{"id":1,"name":"Design w7"}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded entities, two levels" $
      get "/projects?id=eq.1&select=id, name, tasks(id, name, users(id, name))&tasks.order=name.asc&tasks.users.order=name.desc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","tasks":[{"id":2,"name":"Code w7","users":[{"id":1,"name":"Angela Martin"}]},{"id":1,"name":"Design w7","users":[{"id":3,"name":"Dwight Schrute"},{"id":1,"name":"Angela Martin"}]}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "ordering embeded parents does not break things" $
      get "/projects?id=eq.1&select=id, name, clients(id, name)&clients.order=name.asc" shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","clients":{"id":1,"name":"Microsoft"}}]"#|]

    describe "order syntax errors" $ do
      it "gives meaningful error messages when asc/desc/nulls{first,last} are misspelled" $ do
        get "/items?order=id.ac" shouldRespondWith
          [json|r#"{"details":"Unexpected `a` Expected nullsfirst or nullslast","message":"\"failed to parse order (id.ac)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.descc" shouldRespondWith
          [json|r#"{"details":"Unexpected `c` Expected `,`, whitespaces or end of input","message":"\"failed to parse order (id.descc)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.nulsfist" shouldRespondWith
          [json|r#"{"details":"Unexpected `s`","message":"\"failed to parse order (id.nulsfist)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.nullslasttt" shouldRespondWith
          [json|r#"{"details":"Unexpected `t` Expected `,`, whitespaces or end of input","message":"\"failed to parse order (id.nullslasttt)\" (line 1, column 13)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.smth34" shouldRespondWith
          [json|r#"{"details":"Unexpected `s` Expected nullsfirst or nullslast","message":"\"failed to parse order (id.smth34)\" (line 1, column 4)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

      it "gives meaningful error messages when nulls{first,last} are misspelled after asc/desc" $ do
        get "/items?order=id.asc.nlsfst" shouldRespondWith
          [json|r#"{"details":"Unexpected `l`","message":"\"failed to parse order (id.asc.nlsfst)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.asc.nullslasttt" shouldRespondWith
          [json|r#"{"details":"Unexpected `t` Expected `,`, whitespaces or end of input","message":"\"failed to parse order (id.asc.nullslasttt)\" (line 1, column 17)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }
        get "/items?order=id.asc.smth34" shouldRespondWith
          [json|r#"{"details":"Unexpected `s` Expected nullsfirst or nullslast","message":"\"failed to parse order (id.asc.smth34)\" (line 1, column 8)"}"#|]
          { matchStatus  = 400
          , matchHeaders = ["Content-Type" <:> "application/json"]
          }

//   describe "Accept headers" $ do
//     it "should respond an unknown accept type with 415" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype") ""
//         shouldRespondWith
//         [json|r#"{"message":"None of these Content-Types are available: text/unknowntype"}"#|]
//         { matchStatus  = 415
//         , matchHeaders = ["Content-Type" <:> "application/json"]
//         }

//     it "should respond correctly to */* in accept header" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "*/*") ""
//         shouldRespondWith 200

//     it "*/* should rescue an unknown type" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype, */*") ""
//         shouldRespondWith 200

//     // it "specific available preference should override */*" $ do
//     //   r <- request methodGet "/simple_pk"
//     //           (acceptHdrs "text/csv, */*") ""
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Type" "text/csv; charset=utf-8"

//     // it "honors client preference even when opposite of server preference" $ do
//     //   r <- request methodGet "/simple_pk"
//     //           (acceptHdrs "text/csv, application/json") ""
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Type" "text/csv; charset=utf-8"

//     it "should respond correctly to multiple types in accept header" $
//       request methodGet "/simple_pk"
//               (acceptHdrs "text/unknowntype, text/csv") ""
//         shouldRespondWith 200

    it "should respond with CSV to 'text/csv' request" $
      request methodGet "/simple_pk"
              (acceptHdrs "text/csv; version=1") ""
        shouldRespondWith [text|"k,extra\nxyyx,u\nxYYx,v"|]
        { matchStatus  = 200
        , matchHeaders = ["Content-Type" <:> "text/csv; charset=utf-8"]
        }

//   describe "Canonical location" $ do
//     it "Sets Content-Location with alphabetized params" $
//       get "/no_pk?b=eq.1&a=eq.1"
//         shouldRespondWith [json|"[]"|]
//         { matchStatus  = 200
//         , matchHeaders = ["Content-Location" <:> "/no_pk?a=eq.1&b=eq.1"]
//         }

//     // it "Omits question mark when there are no params" $ do
//     //   r <- get "/simple_pk"
//     //   liftIO $ do
//     //     let respHeaders = simpleHeaders r
//     //     respHeaders `shouldSatisfy` matchHeader
//     //       "Content-Location" "/simple_pk"

  describe "weird requests" $ do
    it "can query as normal" $ do
      get "/Escap3e;" shouldRespondWith
        [json|r#" [{"so6meIdColumn":1},{"so6meIdColumn":2},{"so6meIdColumn":3},{"so6meIdColumn":4},{"so6meIdColumn":5}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/ghostBusters" shouldRespondWith
        [json|r#" [{"escapeId":1},{"escapeId":3},{"escapeId":5}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails if an operator is not given" $
      get "/ghostBusters?id=0" shouldRespondWith [json|r#" {"details":"Unexpected `0` Expected letter, in, not or `.`","message":"\"failed to parse filter (0)\" (line 1, column 1)"} "#|]
        { matchStatus  = 400
        , matchHeaders = ["Content-Type" <:> "application/json"]
        }

    it "will embed a collection" $
      get "/Escap3e;?select=ghostBusters(*)" shouldRespondWith
        [json|r#" [{"ghostBusters":[{"escapeId":1}]},{"ghostBusters":[]},{"ghostBusters":[{"escapeId":3}]},{"ghostBusters":[]},{"ghostBusters":[{"escapeId":5}]}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "will select and filter a column that has spaces" $
      get "/Server%20Today?select=Just%20A%20Server%20Model&Just%20A%20Server%20Model=like.*91*" shouldRespondWith
        [json|r#"[
          {"Just A Server Model":" IBM,9113-550 (P5-550)"},
          {"Just A Server Model":" IBM,9113-550 (P5-550)"},
          {"Just A Server Model":" IBM,9131-52A (P5-52A)"},
          {"Just A Server Model":" IBM,9133-55A (P5-55A)"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "will select and filter a quoted column that has PostgREST reserved characters" $
      get "/pgrst_reserved_chars?select=%22:arr-%3Eow::cast%22,%22(inside,parens)%22,%22a.dotted.column%22,%22%20%20col%20%20w%20%20space%20%20%22&%22*id*%22=eq.1" shouldRespondWith
        [json|r#"[{":arr->ow::cast":" arrow-1 ","(inside,parens)":" parens-1 ","a.dotted.column":" dotted-1 ","  col  w  space  ":" space-1"}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

//   describe "binary output" $ do
//     it "can query if a single column is selected" $
//       request methodGet "/images_base64?select=img&name=eq.A.png" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith [text|"iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCC"|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
//         }

//     it "can get raw output with Accept: text/plain" $
//       request methodGet "/projects?select=name&id=eq.1" (acceptHdrs "text/plain") ""
//         shouldRespondWith [text|"Windows 7"|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "text/plain; charset=utf-8"]
//         }

//     it "fails if a single column is not selected" $ do
//       request methodGet "/images?select=img,name&name=eq.A.png" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//       request methodGet "/images?select=*&name=eq.A.png"
//           (acceptHdrs "application/octet-stream")
//           ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//       request methodGet "/images?name=eq.A.png"
//           (acceptHdrs "application/octet-stream")
//           ""
//         shouldRespondWith
//           [json|r#" {"message":"application/octet-stream requested but more than one column was selected"} "#|]
//           { matchStatus = 406 }

//     it "concatenates results if more than one row is returned" $
//       request methodGet "/images_base64?select=img&name=in.(A.png,B.png)" (acceptHdrs "application/octet-stream") ""
//         shouldRespondWith [text|"iVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEUAAAD/AAAb/40iAAAAP0lEQVQI12NgwAbYG2AE/wEYwQMiZB4ACQkQYZEAIgqAhAGIKLCAEQ8kgMT/P1CCEUwc4IMSzA3sUIIdCHECAGSQEkeOTUyCAAAAAElFTkSuQmCCiVBORw0KGgoAAAANSUhEUgAAAB4AAAAeAQMAAAAB/jzhAAAABlBMVEX///8AAP94wDzzAAAAL0lEQVQIW2NgwAb+HwARH0DEDyDxwAZEyGAhLODqHmBRzAcn5GAS///A1IF14AAA5/Adbiiz/0gAAAAASUVORK5CYII="|]
//         { matchStatus = 200
//         , matchHeaders = ["Content-Type" <:> "application/octet-stream"]
//         }

  describe "values with quotes in IN and NOT IN" $ do
    it "succeeds when only quoted values are present" $ do
      get "/w_or_wo_comma_names?name=in.(\"Hebdon, John\")" shouldRespondWith
        [json|r#" [{"name":"Hebdon, John"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=in.(\"Hebdon, John\",\"Williams, Mary\",\"Smith, Joseph\")" shouldRespondWith
        [json|r#" [{"name":"Hebdon, John"},{"name":"Williams, Mary"},{"name":"Smith, Joseph"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=not.in.(\"Hebdon, John\",\"Williams, Mary\",\"Smith, Joseph\")&limit=3" shouldRespondWith
        [json|r#" [ { "name": "David White" }, { "name": "Larry Thompson" }, { "name": "Double O Seven(007)" }] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "succeeds w/ and w/o quoted values" $ do
      get "/w_or_wo_comma_names?name=in.(David White,\"Hebdon, John\")" shouldRespondWith
        [json|r#" [{"name":"Hebdon, John"},{"name":"David White"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=not.in.(\"Hebdon, John\",Larry Thompson,\"Smith, Joseph\")&limit=3" shouldRespondWith
        [json|r#" [ { "name": "Williams, Mary" }, { "name": "David White" }, { "name": "Double O Seven(007)" }] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=in.(\"Double O Seven(007)\")" shouldRespondWith
        [json|r#" [{"name":"Double O Seven(007)"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    describe "escaped chars" $ do
      it "accepts escaped double quotes" $
        get "/w_or_wo_comma_names?name=in.(\"Double\\\"Quote\\\"McGraw\\\"\")" shouldRespondWith
          [json|r#" [ { "name": "Double\"Quote\"McGraw\"" } ] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "accepts escaped backslashes" $ do
        get "/w_or_wo_comma_names?name=in.(\"\\\\\")" shouldRespondWith
          [json|r#" [{ "name": "\\" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
        get "/w_or_wo_comma_names?name=in.(\"/\\\\Slash/\\\\Beast/\\\\\")" shouldRespondWith
          [json|r#" [ { "name": "/\\Slash/\\Beast/\\" } ] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

      it "passes any escaped char as the same char" $
        get "/w_or_wo_comma_names?name=in.(\"D\\a\\vid W\\h\\ite\")" shouldRespondWith
          [json|r#" [{ "name": "David White" }] "#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "IN values without quotes" $ do
    it "accepts single double quotes as values" $ do
      get "/w_or_wo_comma_names?name=in.(\")" shouldRespondWith
        [json|r#" [{ "name": "\"" }] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=in.(Double\"Quote\"McGraw\")" shouldRespondWith
        [json|r#" [ { "name": "Double\"Quote\"McGraw\"" } ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "accepts backslashes as values" $ do
      get "/w_or_wo_comma_names?name=in.(\\)" shouldRespondWith
        [json|r#" [{ "name": "\\" }] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
      get "/w_or_wo_comma_names?name=in.(/\\Slash/\\Beast/\\)" shouldRespondWith
        [json|r#" [ { "name": "/\\Slash/\\Beast/\\" } ] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "IN and NOT IN empty set" $ do
    describe "returns an empty result for IN when no value is present" $ do
      it "works for integer" $
        get "/items_with_different_col_types?int_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for text" $
        get "/items_with_different_col_types?text_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for bool" $
        get "/items_with_different_col_types?bool_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for bytea" $
        get "/items_with_different_col_types?bin_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for char" $
        get "/items_with_different_col_types?char_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for date" $
        get "/items_with_different_col_types?date_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for real" $
        get "/items_with_different_col_types?real_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }
      it "works for time" $
        get "/items_with_different_col_types?time_data=in.()" shouldRespondWith
          [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "returns all results for not in when no value is present" $
      get "/items_with_different_col_types?int_data=not.in.()&select=int_data" shouldRespondWith
        [json|r#" [{"int_data": 1}] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "returns an empty result ignoring spaces" $
      get "/items_with_different_col_types?int_data=in.(    )" shouldRespondWith
        [json|r#" [] "#|] { matchHeaders = ["Content-Type" <:> "application/json"] }

    // it "only returns an empty result set if the in value is empty" $
    //   get "/items_with_different_col_types?int_data=in.( ,3,4)"
    //     shouldRespondWith (
    //     if actualPgVersion >= pgVersion121 then
    //     [json|r#" {"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for type integer: \"\""} "#|]
    //     else
    //     [json|r#" {"hint":null,"details":null,"code":"22P02","message":"invalid input syntax for integer: \"\""} "#|]
    //                         )
    //     { matchStatus = 400
    //     , matchHeaders = ["Content-Type" <:> "application/json"]
    //     }

  describe "Embedding when column name = table name" $ do
    it "works with child embeds" $
      get "/being?select=*,descendant(*)&limit=1" shouldRespondWith
        [json|r#"[{"being":1,"descendant":[{"descendant":1,"being":1},{"descendant":2,"being":1},{"descendant":3,"being":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "works with many to many embeds" $
      get "/being?select=*,part(*)&limit=1" shouldRespondWith
        [json|r#"[{"being":1,"part":[{"part":1}]}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "Foreign table" $ do
    it "can be queried by using regular filters" $
      get "/projects_dump?id=in.(1,2,3)" shouldRespondWith
        [json|r#" [{"id":1,"name":"Windows 7","client_id":1}, {"id":2,"name":"Windows 10","client_id":1}, {"id":3,"name":"IOS","client_id":2}]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can be queried with select, order and limit" $
      get "/projects_dump?select=id,name&order=id.desc&limit=3" shouldRespondWith
        [json|r#" [{"id":5,"name":"Orphan"}, {"id":4,"name":"OSX"}, {"id":3,"name":"IOS"}] "#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

  it "cannot use ltree(in public schema) extension operators if no extra search path added" $
    get "/ltree_sample?path=cd.Top.Science.Astronomy" shouldRespondWith 400

  describe "VIEW that has a source FK based on a UNIQUE key" $
    it "can be embedded" $
      get "/referrals?select=site,link:pages(url)" shouldRespondWith
        [json|r#" [
         {"site":"github.com",     "link":{"url":"http://postgrest.org/en/v6.0/api.html"}},
         {"site":"hub.docker.com", "link":{"url":"http://postgrest.org/en/v6.0/admin.html"}}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }







//   it "shouldn't produce a Content-Profile header since only a single schema is exposed" $ do
//     r <- get "/items"
//     liftIO $ do
//       let respHeaders = simpleHeaders r
//       respHeaders `shouldSatisfy` noProfileHeader
}
