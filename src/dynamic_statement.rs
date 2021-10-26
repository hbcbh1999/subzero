use std::ops::Add;
// use std::slice::Join;

#[derive(Debug, PartialEq)]
pub struct SqlSnippet<'a, T:?Sized>(pub Vec<SqlSnippetChunk<'a, T>>);

#[derive(Debug, PartialEq)]
pub enum SqlSnippetChunk<'a, T:?Sized> 
{
    Sql (&'a str),
    Param (&'a T),
}
impl<'a, T:?Sized> SqlSnippet<'a, T>{
    pub fn len(&self) -> usize {
        match self {
            SqlSnippet(v) => v.len()
        }
    }
}

pub trait IntoSnippet<'a, T:?Sized> {
    fn into(self) -> SqlSnippet<'a, T>;
}

pub fn sql<'a, T:?Sized, A>(s: A) -> SqlSnippet<'a, T> where A: IntoSnippet<'a, T> {
    s.into()
}
pub fn param<'a, T:?Sized>(p: &'a T) -> SqlSnippet<'a, T> { SqlSnippet(vec![SqlSnippetChunk::Param(p)]) }


impl<'a, T:?Sized> IntoSnippet<'a, T> for &'a str {
    fn into(self) -> SqlSnippet<'a, T> {
        SqlSnippet(vec![SqlSnippetChunk::Sql(self)])
    }
}

impl<'a, T:?Sized> IntoSnippet<'a, T> for &'a String {
    fn into(self) -> SqlSnippet<'a, T> {
        SqlSnippet(vec![SqlSnippetChunk::Sql(self)])
    }
}

impl<T:?Sized> Add for SqlSnippet<'a, T> {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (SqlSnippet(l),SqlSnippet(r)) => {
                let mut n = vec![];
                n.extend(l.into_iter());
                n.extend(r.into_iter());
                SqlSnippet(n)
            }
        }
    }
}

impl<T:?Sized> Add<SqlSnippet<'a, T>> for &'a str {
    type Output = SqlSnippet<'a, T>;
    fn add(self, snippet: SqlSnippet<'a, T>) -> SqlSnippet<'a, T> {
        match snippet {
            SqlSnippet(r) => {
                let mut n = vec![SqlSnippetChunk::Sql(self)];
                n.extend(r.into_iter());
                SqlSnippet(n)
            }
        }
    }
}

impl<T:?Sized> Add<&'a str> for SqlSnippet<'a, T>{
    type Output = SqlSnippet<'a, T>;
    fn add(self, s: &'a str) -> SqlSnippet<'a, T> {
        match self {
            SqlSnippet(l) => {
                let mut n = vec![];
                n.extend(l.into_iter());
                n.push(SqlSnippetChunk::Sql(s));
                SqlSnippet(n)
            }
        }
    }
}

impl<T:?Sized> Add<SqlSnippet<'a, T>> for &'a String {
    type Output = SqlSnippet<'a, T>;
    fn add(self, snippet: SqlSnippet<'a, T>) -> SqlSnippet<'a, T> {
        match snippet {
            SqlSnippet(r) => {
                let mut n = vec![SqlSnippetChunk::Sql(self)];
                n.extend(r.into_iter());
                SqlSnippet(n)
            }
        }
    }
}

impl<T:?Sized> Add<&'a String> for SqlSnippet<'a, T>{
    type Output = SqlSnippet<'a, T>;
    fn add(self, s: &'a String) -> SqlSnippet<'a, T> {
        match self {
            SqlSnippet(l) => {
                let mut n = vec![];
                n.extend(l.into_iter());
                n.push(SqlSnippetChunk::Sql(s));
                SqlSnippet(n)
            }
        }
    }
}

// impl<T:?Sized> Join<&str> for [SqlSnippet<'a, T>] {
//     type Output = SqlSnippet<'a, T>;

//     fn join(slice: &Self, sep: &str) -> Output {
//         todo!();//unsafe { String::from_utf8_unchecked(join_generic_copy(slice, sep.as_bytes())) }
//     }
// }

pub fn generate<'a, T:?Sized>( s: SqlSnippet<'a, T> ) -> (String, Vec<&T>, u32){
    match s {
        SqlSnippet(c) => c.iter().fold(
            (String::new(), vec![], 1),
            |acc, v| {
                let (mut sql, mut params, pos) = acc;
                match v {
                    SqlSnippetChunk::Sql(s) => {
                        sql.push_str(s);
                        (sql, params, pos)
                    },
                    SqlSnippetChunk::Param(p) => {
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
    use postgres_types::{ToSql};

    use super::*;
    use super::SqlSnippetChunk::*;
    #[test]
    fn basic(){
        assert_eq!(sql("select * from tbl where id = ") + param(&20), SqlSnippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!("select * from tbl where id = " + param(&20), SqlSnippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!(param(&20) + "=10", SqlSnippet(vec![Param(&20),Sql(&"=10")]) );
        let query = "select * from tbl where id = ".to_string();
        assert_eq!( &query + param(&20), SqlSnippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!( query.as_str() + param(&20), SqlSnippet(vec![Sql(&"select * from tbl where id = "), Param(&20)]) );
        assert_eq!(
            generate(("select * from tbl where id > " + param(&20) + " and id < " + param(&30))),
            ("select * from tbl where id > $1 and id < $2".to_string(), vec![&20, &30], 3)
        );
    }

    #[test]
    fn dyn_parameters(){
        let p1: &(dyn ToSql + Sync) = &20;
        let p2: &(dyn ToSql + Sync) = &"name";
        let snippet = "select * from tbl where id > " + param(p1) + " and name = " + param(p2);
        let (q,p,i) = generate(snippet);
        assert_eq!(q, "select * from tbl where id > $1 and name = $2".to_string());
        assert_eq!(format!("{:?}",p), format!("{:?}",vec![p1, p2]));
        assert_eq!(i,3);
    }

}