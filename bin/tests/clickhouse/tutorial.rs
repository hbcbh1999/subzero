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
      get "/trips?select=passenger_count,average_total_amount:$ceil($avg(total_amount),'2'::integer)&groupby=passenger_count"
      shouldRespondWith
      [json| r#"
      [
        {"passenger_count":0,"average_total_amount":24.01},
        {"passenger_count":1,"average_total_amount":15.98},
        {"passenger_count":2,"average_total_amount":17.17},
        {"passenger_count":3,"average_total_amount":16.77},
        {"passenger_count":4,"average_total_amount":17.42},
        {"passenger_count":5,"average_total_amount":16.32},
        {"passenger_count":6,"average_total_amount":16},
        {"passenger_count":7,"average_total_amount":63.63},
        {"passenger_count":8,"average_total_amount":36.41},
        {"passenger_count":9,"average_total_amount":6.81}
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

    it "length of the trip" $
      get "/trips?select=avg_tip:$avg(tip_amount),avg_fare:$avg(fare_amount),avg_passenger:$avg(passenger_count),count:$count(),trip_minutes:$truncate($date_diff('second'::Text, pickup_datetime, dropoff_datetime))&groupby=trip_minutes&order=trip_minutes&limit=3"
      shouldRespondWith
      [json| r#"
      [
        {"avg_tip":3.6600000858306885,"avg_fare":17,"avg_passenger":1,"count":"1","trip_minutes":"-297"},
        {"avg_tip":1.2300000190734863,"avg_fare":11,"avg_passenger":1,"count":"1","trip_minutes":"-125"},
        {"avg_tip":54,"avg_fare":2.5,"avg_passenger":1,"count":"1","trip_minutes":"-73"}
      ]
      "#|]
      { matchStatus = 200, matchHeaders = ["Content-Type" <:> "application/json"] }
}
