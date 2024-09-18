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
