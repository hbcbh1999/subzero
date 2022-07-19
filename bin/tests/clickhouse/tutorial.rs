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
static INIT_CLIENT: Once = Once::new();

lazy_static! {
    static ref CLIENT: AsyncOnce<Client> = AsyncOnce::new(async { Client::untracked(start().await.unwrap()).await.expect("valid client") });
}

haskell_test! {
feature "tutorial"

  describe "analyze" $
    it "average tip amount" $
      request methodGet "/trips?select=$avg(tip_amount)"
      [("Accept", "application/vnd.pgrst.object+json")] ""
      shouldRespondWith
      [json| r#"{"avg":1.684652541245338}"#|]
      { matchStatus = 200, matchHeaders = ["Content-Type" <:> "application/vnd.pgrst.object+json"] }

    it "average cost based on the number of passengers" $
      get "/trips?select=passenger_count,average_total_amount:$avg(total_amount)&groupby=passenger_count"
      shouldRespondWith
      [json| r#"
      [
        {"passenger_count":0,"average_total_amount":24.007255122942084},
        {"passenger_count":1,"average_total_amount":15.97847068859104},
        {"passenger_count":2,"average_total_amount":17.16466424270739},
        {"passenger_count":3,"average_total_amount":16.762066723004487},
        {"passenger_count":4,"average_total_amount":17.415726662687486},
        {"passenger_count":5,"average_total_amount":16.318897374323292},
        {"passenger_count":6,"average_total_amount":15.994636453382169},
        {"passenger_count":7,"average_total_amount":63.62250089645386},
        {"passenger_count":8,"average_total_amount":36.400001207987465},
        {"passenger_count":9,"average_total_amount":6.800000190734863}
      ]
      "#|]
      { matchStatus = 200, matchHeaders = ["Content-Type" <:> "application/json"] }
    it "daily number of pickups per neighborhood" $
      get "/trips?select=pickup_date,pickup_ntaname,number_of_trips:$sum('1'::UInt8)&groupby=pickup_date,pickup_ntaname&order=pickup_date.asc&limit=3"
      shouldRespondWith
      [json| r#"
      [
        {"pickup_date":"2015-07-01","pickup_ntaname":"Central Harlem South","number_of_trips":"16"},
        {"pickup_date":"2015-07-01","pickup_ntaname":"Bushwick South","number_of_trips":"4"},
        {"pickup_date":"2015-07-01","pickup_ntaname":"Lincoln Square","number_of_trips":"252"}
      ]
      "#|]
    { matchStatus = 200, matchHeaders = ["Content-Type" <:> "application/json"] }
}
