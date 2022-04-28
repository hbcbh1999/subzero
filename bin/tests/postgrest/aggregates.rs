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
feature "aggregates"

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
    it "can call more functions on a row column" $
      get "/projects?select=name:$upper(name),name2:$concat('X-'::text, name)" shouldRespondWith
        [json|r#"[
          {"name":"WINDOWS 7","name2":"X-Windows 7"},
          {"name":"WINDOWS 10","name2":"X-Windows 10"},
          {"name":"IOS","name2":"X-IOS"},
          {"name":"OSX","name2":"X-OSX"},
          {"name":"ORPHAN","name2":"X-Orphan"}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can NOT call unsafe functions" $
        get "/projects?select=name,random:$random(),tasks($randomagain())" shouldRespondWith
          [json|r#"{"details":"calling: 'random' is not allowed","message":"Unsafe functions called"}"#|]
          { matchStatus  = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call a function with multiple parameters" $
      get "/projects?select=name:$concat('X-'::text, name)" shouldRespondWith
        [json|r#"[
          {"name":"X-Windows 7"},
          {"name":"X-Windows 10"},
          {"name":"X-IOS"},
          {"name":"X-OSX"},
          {"name":"X-Orphan"}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }
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
      get "/users_projects?select=user_id, total:$count(project_id)&groupby=user_id&order=user_id.asc" shouldRespondWith
        [json|r#"[
          {"user_id":1,"total":2},
          {"user_id":2,"total":2},
          {"user_id":3,"total":3}
        ]"#|]
        { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can call an aggregate function with groupby" $
        get "/product_orders?select=city, total_order_amount:$sum(order_amount)&groupby=city&order=city" shouldRespondWith
            [json|r#"[
                {"city":"Arlington","total_order_amount":"$37,000.00"},
                {"city":"GuildFord","total_order_amount":"$50,500.00"},
                {"city":"Shalford","total_order_amount":"$13,000.00"}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call an aggregate function with partition" $
        get "/product_orders?select=order_id,city,order_amount,grand_total:$sum(order_amount)-p(city)" shouldRespondWith
            [json|r#"[
                {"order_id":1002,"city":"Arlington","order_amount":"$20,000.00","grand_total":"$37,000.00"},
                {"order_id":1007,"city":"Arlington","order_amount":"$15,000.00","grand_total":"$37,000.00"},
                {"order_id":1008,"city":"Arlington","order_amount":"$2,000.00","grand_total":"$37,000.00"},
                {"order_id":1010,"city":"GuildFord","order_amount":"$500.00","grand_total":"$50,500.00"},
                {"order_id":1004,"city":"GuildFord","order_amount":"$15,000.00","grand_total":"$50,500.00"},
                {"order_id":1006,"city":"GuildFord","order_amount":"$25,000.00","grand_total":"$50,500.00"},
                {"order_id":1001,"city":"GuildFord","order_amount":"$10,000.00","grand_total":"$50,500.00"},
                {"order_id":1003,"city":"Shalford","order_amount":"$5,000.00","grand_total":"$13,000.00"},
                {"order_id":1009,"city":"Shalford","order_amount":"$1,000.00","grand_total":"$13,000.00"},
                {"order_id":1005,"city":"Shalford","order_amount":"$7,000.00","grand_total":"$13,000.00"}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call an aggregate function with order" $
        get "/product_orders?select=order_id,customer_name,city,order_amount,rank:$rank()-o(order_amount.desc)" shouldRespondWith
            [json|r#"[
                {"order_id":1006,"customer_name":"Paum Smith","city":"GuildFord","order_amount":"$25,000.00","rank":1},
                {"order_id":1002,"customer_name":"David Jones","city":"Arlington","order_amount":"$20,000.00","rank":2},
                {"order_id":1007,"customer_name":"Andrew Smith","city":"Arlington","order_amount":"$15,000.00","rank":3},
                {"order_id":1004,"customer_name":"Michael Smith","city":"GuildFord","order_amount":"$15,000.00","rank":3},
                {"order_id":1001,"customer_name":"David Smith","city":"GuildFord","order_amount":"$10,000.00","rank":5},
                {"order_id":1005,"customer_name":"David Williams","city":"Shalford","order_amount":"$7,000.00","rank":6},
                {"order_id":1003,"customer_name":"John Smith","city":"Shalford","order_amount":"$5,000.00","rank":7},
                {"order_id":1008,"customer_name":"David Brown","city":"Arlington","order_amount":"$2,000.00","rank":8},
                {"order_id":1009,"customer_name":"Robert Smith","city":"Shalford","order_amount":"$1,000.00","rank":9},
                {"order_id":1010,"customer_name":"Peter Smith","city":"GuildFord","order_amount":"$500.00","rank":10}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can call an aggregate function with partition and order" $
        get "/product_orders?select=order_id,customer_name,city,order_amount,row_number:$row_number()-p(city)-o(order_amount.desc)" shouldRespondWith
            [json|r#"[
                {"order_id":1002,"customer_name":"David Jones","city":"Arlington","order_amount":"$20,000.00","row_number":1},
                {"order_id":1007,"customer_name":"Andrew Smith","city":"Arlington","order_amount":"$15,000.00","row_number":2},
                {"order_id":1008,"customer_name":"David Brown","city":"Arlington","order_amount":"$2,000.00","row_number":3},
                {"order_id":1006,"customer_name":"Paum Smith","city":"GuildFord","order_amount":"$25,000.00","row_number":1},
                {"order_id":1004,"customer_name":"Michael Smith","city":"GuildFord","order_amount":"$15,000.00","row_number":2},
                {"order_id":1001,"customer_name":"David Smith","city":"GuildFord","order_amount":"$10,000.00","row_number":3},
                {"order_id":1010,"customer_name":"Peter Smith","city":"GuildFord","order_amount":"$500.00","row_number":4},
                {"order_id":1005,"customer_name":"David Williams","city":"Shalford","order_amount":"$7,000.00","row_number":1},
                {"order_id":1003,"customer_name":"John Smith","city":"Shalford","order_amount":"$5,000.00","row_number":2},
                {"order_id":1009,"customer_name":"Robert Smith","city":"Shalford","order_amount":"$1,000.00","row_number":3}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

  describe "Protected views" $ do
    it "can get a single item by id" $
        get "/protected_books?select=title&id=eq.5" shouldRespondWith
          [json|r#"[ { "title": "Farenheit 451" } ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "fails when no filters provided" $
        get "/protected_books?limit=1" shouldRespondWith
            [json|r#"{
                "hint":"",
                "details":"Please provide at least one of id, publication_year, author_id filters",
                "code":"P0001",
                "message":"Filter parameters not provided"
            }"#|]
            { matchStatus  = 400, matchHeaders = ["Content-Type" <:> "application/json"] }
    it "can get multiple items with a correct filter" $
        get "/protected_books?select=title&publication_year=gt.1960" shouldRespondWith
            [json|r#"[
                {"title":"Slaughterhouse-Five"},
                {"title":"One Flew Over the Cuckoo's Nest"}
            ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }

    it "can get items from protected view when embeded" $
        get "/authors?select=name,protected_books(title)&id=eq.1" shouldRespondWith
            [json|r#"[ {"name":"George Orwell","protected_books":[{"title":"1984"}]} ]"#|]
            { matchHeaders = ["Content-Type" <:> "application/json"] }




  }
