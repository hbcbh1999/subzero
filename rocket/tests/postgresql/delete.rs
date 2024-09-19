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
haskell_test! {
feature "delete"
  describe "Deleting" $ do
    describe "existing record" $ do
      it "succeeds with 204 and deletion count" $
        request methodDelete "/items?id=eq.1"
            // []
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus  = 204
            , matchHeaders = [
                             // matchHeaderAbsent hContentType,
                             "Content-Range" <:> "*/*" ]
            }

      it "returns the deleted item and count if requested" $
        request methodDelete "/items?id=eq.2" [("Prefer", "return=representation, count=exact")] ""
          shouldRespondWith [json|r#"[{"id":2}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/1"]
          }

      // it "ignores ?select= when return not set or return=minimal" $ do
      //   request methodDelete "/items?id=eq.3&select=id"
      //       //[]
      //       ""
      //     shouldRespondWith
      //       [text|""|]
      //       { matchStatus  = 204
      //       , matchHeaders = [
      //                        // matchHeaderAbsent hContentType,
      //                        "Content-Range" <:> "*/*" ]
      //       }
      //   request methodDelete "/items?id=eq.3&select=id"
      //       [("Prefer", "return=minimal")]
      //       ""
      //     shouldRespondWith
      //       [text|""|]
      //       { matchStatus  = 204
      //       , matchHeaders = [
      //                        // matchHeaderAbsent hContentType,
      //                        "Content-Range" <:> "*/*" ]
      //       }

      it "returns the deleted item and shapes the response" $
        request methodDelete "/complex_items?id=eq.2&select=id,name" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"id":2,"name":"Two"}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

      it "can rename and cast the selected columns" $
        request methodDelete "/complex_items?id=eq.3&select=ciId:id::text,ciName:name" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"ciId":"3","ciName":"Three"}]"#|]

      it "can embed (parent) entities" $
        request methodDelete "/tasks?id=eq.8&select=id,name,project:projects(id)" [("Prefer", "return=representation")] ""
          shouldRespondWith [json|r#"[{"id":8,"name":"Code OSX","project":{"id":4}}]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

    describe "known route, no records matched" $
      it "includes [] body if return=rep" $
        request methodDelete "/items?id=eq.101"
          [("Prefer", "return=representation")] ""
          shouldRespondWith [text|"[]"|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

    describe "totally unknown route" $
      it "fails with 404" $
        request methodDelete "/foozle?id=eq.101"
          //[]
          ""
          shouldRespondWith 404

    describe "table with limited privileges" $ do
      // it "fails deleting the row when return=representation and selecting all the columns" $
      //   request methodDelete "/app_users?id=eq.1" [("Prefer", "return=representation")]
      //     //mempty
      //     ""
      //     shouldRespondWith 401

      it "succeeds deleting the row when return=representation no select specified" $
        request methodDelete "/app_users?id=eq.1" [("Prefer", "return=representation")]
          //mempty
          ""
          shouldRespondWith [json|r#"[ { "id": 1, "email": "test@123.com" } ]"#|]
          { matchStatus  = 200
          , matchHeaders = ["Content-Range" <:> "*/*"]
          }

      it "succeeds deleting the row when return=representation and selecting only the privileged columns" $
        request methodDelete "/app_users?id=eq.1&select=id,email" [("Prefer", "return=representation")]
          r#"{ "password": "passxyz" }"#
            shouldRespondWith [json|r#"[ { "id": 1, "email": "test@123.com" } ]"#|]
            { matchStatus  = 200
            , matchHeaders = ["Content-Range" <:> "*/*"]
            }

      it "suceeds deleting the row with no explicit select when using return=minimal" $
        request methodDelete "/app_users?id=eq.2"
            [("Prefer", "return=minimal")]
            //mempty
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus = 204
            , matchHeaders = [
              //matchHeaderAbsent hContentType
              ]
            }

      it "suceeds deleting the row with no explicit select by default" $
        request methodDelete "/app_users?id=eq.3"
            //[]
            //mempty
            ""
          shouldRespondWith
            [text|""|]
            { matchStatus = 204
            , matchHeaders = [
              //matchHeaderAbsent hContentType
              ]
    }
}
