use std::ops::Add;

#[derive(Debug, PartialEq)]
pub struct Snippet<'a, T>(Vec<SnippetChunk<'a, T>>);

#[derive(Debug, PartialEq)]
pub enum SnippetChunk<'a, T> {
    Sql (&'a str),
    Param (&'a T),
}

pub fn sql<'a, T>(s: &'a str) -> Snippet<'a, T> { Snippet(vec![SnippetChunk::Sql(s)]) }

pub fn param<'a, T>(p: &'a T) -> Snippet<'a, T> { Snippet(vec![SnippetChunk::Param(p)]) }


impl<T: Add<Output = T>> Add for Snippet<'_, T> {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (Snippet(l),Snippet(r)) => {
                let mut n = vec![];
                n.extend(l.into_iter());
                n.extend(r.into_iter());
                Snippet(n)
            }
        }
    }
}

impl<T: Add<Output = T>> Add<Snippet<'a, T>> for &'a str {
    type Output = Snippet<'a, T>;
    fn add(self, snippet: Snippet<'a, T>) -> Snippet<'a, T> {
        match snippet {
            Snippet(r) => {
                let mut n = vec![SnippetChunk::Sql(self)];
                n.extend(r.into_iter());
                Snippet(n)
            }
        }
    }
}

impl<T: Add<Output = T>> Add<&'a str> for Snippet<'a, T>{
    type Output = Snippet<'a, T>;
    fn add(self, s: &'a str) -> Snippet<'a, T> {
        match self {
            Snippet(l) => {
                let mut n = vec![];
                n.extend(l.into_iter());
                n.push(SnippetChunk::Sql(s));
                Snippet(n)
            }
        }
    }
}


/**
dynamicallyParameterized :: SnippetDefs.Snippet -> Decoders.Result result -> Bool -> Statement () result
dynamicallyParameterized (SnippetDefs.Snippet chunks) decoder prepared = let
  step (!paramId, !poking, !encoder) = \ case
    SnippetDefs.StringSnippetChunk sql -> (paramId, poking <> Poking.bytes sql, encoder)
    SnippetDefs.ParamSnippetChunk paramEncoder -> let
      newParamId = paramId + 1
      newPoking = poking <> Poking.word8 36 <> Poking.asciiIntegral paramId
      newEncoder = encoder <> paramEncoder
      in (newParamId, newPoking, newEncoder)
  in case foldl' step (1, mempty, mempty) chunks of
    (_, poking, encoder) -> Statement (ByteString.poking poking) encoder decoder prepared
*/

pub fn generate<'a, T>( s: Snippet<'a, T> ) -> (String, Vec<&T>, u32){
    match s {
        Snippet(c) => c.iter().fold(
            (String::new(), vec![], 1),
            |acc, v| {
                let (mut sql, mut params, pos) = acc;
                match v {
                    SnippetChunk::Sql(s) => {
                        sql.push_str(s);
                        (sql, params, pos)
                    },
                    SnippetChunk::Param(p) => {
                        sql.push_str(format!("${}", pos).as_str());
                        params.push(p);
                        (sql, params, pos + 1)
                    }
                }
            }
        )
    }
    
}

#[cfg(test)]
mod tests {
    use pretty_assertions::{assert_eq};
    use super::*;
    use super::SnippetChunk::*;
    #[test]
    fn concat_components(){
        assert_eq!(sql(&"select * from tbl where id = ") + param(&20), Snippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!("select * from tbl where id = " + param(&20), Snippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!(param(&20) + "=10", Snippet(vec![Param(&20),Sql(&"=10")]) );
        let query = "select * from tbl where id = ".to_string();
        assert_eq!( query.as_str() + param(&20), Snippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!(
            generate("select * from tbl where id > " + param(&20) + " and id < " + param(&30)),
            ("select * from tbl where id > $1 and id < $2".to_string(), vec![&20, &30], 3)
        );
    }
}