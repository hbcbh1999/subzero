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
        env::set_var("SUBZERO_DB_MAX_ROWS", "2");
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
feature "limits"
describe "Requesting many items with server limits(max-rows) enabled" $ do
  it "restricts results" $
    get "/items?order=id"
      shouldRespondWith
        [json| r#"[{"id":1},{"id":2}]"# |]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

  // it "respects additional client limiting" $ do
  //   request methodGet  "/items"
  //       (rangeHdrs $ ByteRangeFromTo 0 0)
  //       ""
  //     shouldRespondWith
  //       [json| r#"[{"id":1}]"# |]
  //       { matchHeaders = ["Content-Range" <:> "0-0/*"] }

  it "works on all levels" $
    get "/users?select=id,tasks(id)&order=id.asc&tasks.order=id.asc"
      shouldRespondWith
        [json|r#"[{"id":1,"tasks":[{"id":1},{"id":2}]},{"id":2,"tasks":[{"id":5},{"id":6}]}]"#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

  it "succeeds in getting parent embeds despite the limit, see #647" $
    get "/tasks?select=id,project:projects(id)&id=gt.5"
      shouldRespondWith
        [json|r#"[{"id":6,"project":{"id":3}},{"id":7,"project":{"id":4}}]"#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

  it "can offset the parent embed, being consistent with the other embed types" $
    get "/tasks?select=id,project:projects(id)&id=gt.5&project.offset=1"
      shouldRespondWith
        [json|r#"[{"id":6,"project":null}, {"id":7,"project":null}]"#|]
        { matchHeaders = ["Content-Range" <:> "0-1/*"] }

  describe "count=exact" $ do
    it "uses the query planner guess when query rows bigger maxRows" $
      request methodGet "/getallprojects_view"
          [("Prefer", "count=exact")]
          ""
        shouldRespondWith
        [json|r#"[{"id":1,"name":"Windows 7","client_id":1},{"id":2,"name":"Windows 10","client_id":1}]"#|]
          { matchStatus  = 206
          , matchHeaders = [ "Content-Type" <:> "application/json"
                          , "Content-Range" <:> "0-1/5" ]
          }

    it "gives exact count when query rows smaller maxRows" $
      request methodGet "/getallprojects_view?id=lt.3"
          [("Prefer", "count=exact")]
          ""
        shouldRespondWith
          [json|r#"[{"id":1,"name":"Windows 7","client_id":1},{"id":2,"name":"Windows 10","client_id":1}]"#|]
          { matchHeaders = [ "Content-Type" <:> "application/json"
                          , "Content-Range" <:> "0-1/2" ]
          }

    it "only uses the query planner guess if it is indeed greater than the exact count" $
      request methodGet "/get_projects_above_view"
          [("Prefer", "count=exact")]
          ""
        shouldRespondWith
        [json|r#"[{"id":2,"name":"Windows 10","client_id":1},{"id":3,"name":"IOS","client_id":2}]"#|]
          { matchStatus  = 206
          , matchHeaders = [ "Content-Type" <:> "application/json"
                          , "Content-Range" <:> "0-1/4" ]
          }
}
