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
feature "custom_relations"

  describe "Custom Relations" $ do
    it "requesting parent with custom relations" $
        get "/no_fk_projects?select=name,client:clients(name)" shouldRespondWith
          [json|r#"[
            {"name":"Windows 7","client":{"name": "Microsoft"}},
            {"name":"Windows 10","client":{"name": "Microsoft"}},
            {"name":"IOS","client":{"name": "Apple"}},
            {"name":"OSX","client":{"name": "Apple"}},
            {"name":"Orphan","client":null}
          ]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }
    it "requesting children with custom relations" $
        get "/clients?id=eq.1&select=id,projects:no_fk_projects(id)" shouldRespondWith
          [json|r#"[{"id":1,"projects":[{"id":1},{"id":2}]}]"#|]
          { matchHeaders = ["Content-Type" <:> "application/json"] }




  }
