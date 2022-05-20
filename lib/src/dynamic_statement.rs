use std::ops::Add;

#[derive(Debug, PartialEq)]
pub enum SqlSnippetChunk<'a, T: ?Sized> {
    Sql(String),
    Param(&'a T),
}

#[derive(Debug, PartialEq)]
pub struct SqlSnippet<'a, T: ?Sized>(pub Vec<SqlSnippetChunk<'a, T>>);

impl<'a, T: ?Sized> SqlSnippet<'a, T> {
    pub fn len(&self) -> usize {
        match self {
            SqlSnippet(v) => v.len(),
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait JoinIterator<'a, T: ?Sized> {
    fn join(self, sep: &str) -> SqlSnippet<'a, T>;
}

impl<'a, I, T: ?Sized + 'a> JoinIterator<'a, T> for I
where
    I: IntoIterator<Item = SqlSnippet<'a, T>>,
{
    fn join(self, sep: &str) -> SqlSnippet<'a, T> {
        match self.into_iter().fold(SqlSnippet(vec![]), |SqlSnippet(mut acc), SqlSnippet(v)| {
            acc.push(SqlSnippetChunk::Sql(sep.to_string()));
            acc.extend(v.into_iter());
            SqlSnippet(acc)
        }) {
            SqlSnippet(mut v) => {
                if !v.is_empty() {
                    v.remove(0);
                }
                SqlSnippet(v)
            }
        }
    }
}

pub trait IntoSnippet<'a, T: ?Sized> {
    fn into(self) -> SqlSnippet<'a, T>;
}

pub fn sql<'a, T: ?Sized, A>(s: A) -> SqlSnippet<'a, T>
where
    A: IntoSnippet<'a, T>,
{
    s.into()
}
pub fn param<T: ?Sized>(p: &T) -> SqlSnippet<T> { SqlSnippet(vec![SqlSnippetChunk::Param(p)]) }

impl<'a, T: ?Sized> IntoSnippet<'a, T> for &'a str {
    fn into(self) -> SqlSnippet<'a, T> { SqlSnippet(vec![SqlSnippetChunk::Sql(self.to_string())]) }
}

impl<'a, T: ?Sized> IntoSnippet<'a, T> for String {
    fn into(self) -> SqlSnippet<'a, T> { SqlSnippet(vec![SqlSnippetChunk::Sql(self)]) }
}

impl<'a, T: ?Sized> Add for SqlSnippet<'a, T> {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        match (self, other) {
            (SqlSnippet(mut l), SqlSnippet(r)) => {
                // let mut n = vec![];
                // n.extend(l.into_iter());
                // n.extend(r.into_iter());
                // SqlSnippet(n)
                l.extend(r.into_iter());
                SqlSnippet(l)
            }
        }
    }
}

impl<'a, T: ?Sized> Add<SqlSnippet<'a, T>> for &'a str {
    type Output = SqlSnippet<'a, T>;
    fn add(self, snippet: SqlSnippet<'a, T>) -> SqlSnippet<'a, T> {
        match snippet {
            SqlSnippet(mut r) => {
                // let mut n = vec![SqlSnippetChunk::Sql(self.to_string())];
                // n.extend(r.into_iter());
                // SqlSnippet(n)
                r.insert(0, SqlSnippetChunk::Sql(self.to_string()));
                SqlSnippet(r)
            }
        }
    }
}

impl<'a, T: ?Sized> Add<&'a str> for SqlSnippet<'a, T> {
    type Output = SqlSnippet<'a, T>;
    fn add(self, s: &'a str) -> SqlSnippet<'a, T> {
        match self {
            SqlSnippet(mut l) => {
                // let mut n = vec![];
                // n.extend(l.into_iter());
                // n.push(SqlSnippetChunk::Sql(s.to_string()));
                // SqlSnippet(n)
                l.push(SqlSnippetChunk::Sql(s.to_string()));
                SqlSnippet(l)
            }
        }
    }
}

impl<'a, T: ?Sized> Add<SqlSnippet<'a, T>> for String {
    type Output = SqlSnippet<'a, T>;
    fn add(self, snippet: SqlSnippet<'a, T>) -> SqlSnippet<'a, T> {
        match snippet {
            SqlSnippet(mut r) => {
                // let mut n = vec![SqlSnippetChunk::Sql(self)];
                // n.extend(r.into_iter());
                // SqlSnippet(n)
                r.insert(0, SqlSnippetChunk::Sql(self));
                SqlSnippet(r)
            }
        }
    }
}

impl<'a, T: ?Sized> Add<String> for SqlSnippet<'a, T> {
    type Output = SqlSnippet<'a, T>;
    fn add(self, s: String) -> SqlSnippet<'a, T> {
        match self {
            SqlSnippet(mut l) => {
                // let mut n = vec![];
                // n.extend(l.into_iter());
                // n.push(SqlSnippetChunk::Sql(s));
                // SqlSnippet(n)
                l.push(SqlSnippetChunk::Sql(s));
                SqlSnippet(l)
            }
        }
    }
}

pub fn generate<T: ?Sized>(s: SqlSnippet<T>) -> (String, Vec<&T>, u32) {
    match s {
        SqlSnippet(c) => c.iter().fold((String::new(), vec![], 1), |acc, v| {
            let (mut sql, mut params, pos) = acc;
            match v {
                SqlSnippetChunk::Sql(s) => {
                    sql.push_str(s);
                    (sql, params, pos)
                }
                SqlSnippetChunk::Param(p) => {
                    sql.push_str(format!("${}", pos).as_str());
                    params.push(p);
                    (sql, params, pos + 1)
                }
            }
        }),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "postgresql")]
    use postgres_types::ToSql;
    use pretty_assertions::assert_eq;

    use super::SqlSnippetChunk::*;
    use super::*;
    fn s(s: &str) -> String { s.to_string() }
    #[test]
    fn basic() {
        assert_eq!(
            sql("select * from tbl where id = ") + param(&20),
            SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)])
        );
        assert_eq!(
            "select * from tbl where id = " + param(&20),
            SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)])
        );
        assert_eq!(param(&20) + "=10", SqlSnippet(vec![Param(&20), Sql(s("=10"))]));
        let query = "select * from tbl where id = ".to_string();
        assert_eq!(query + param(&20), SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)]));
        //assert_eq!( query.as_str() + param(&20), SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)]) );
        assert_eq!(
            generate("select * from tbl where id > " + param(&20) + " and id < " + param(&30)),
            ("select * from tbl where id > $1 and id < $2".to_string(), vec![&20, &30], 3)
        );
    }

    #[cfg(feature = "postgresql")]
    #[test]
    fn dyn_parameters() {
        let p1: &(dyn ToSql + Sync) = &20;
        let p2: &(dyn ToSql + Sync) = &"name";
        let snippet = "select * from tbl where id > " + param(p1) + " and name = " + param(p2);
        let (q, p, i) = generate(snippet);
        assert_eq!(q, "select * from tbl where id > $1 and name = $2".to_string());
        assert_eq!(format!("{:?}", p), format!("{:?}", vec![p1, p2]));
        assert_eq!(i, 3);
    }
}
