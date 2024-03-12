use std::ops::Add;

#[derive(Debug, PartialEq, Eq)]
pub enum SqlSnippetChunk<'a, T: ?Sized> {
    Owned(String),
    Borrowed(&'a str),
    Param(&'a T),
}

#[derive(Debug, PartialEq, Eq)]
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
    fn join(self, sep: &'a str) -> SqlSnippet<'a, T>;
}

impl<'a, I, T: ?Sized + 'a> JoinIterator<'a, T> for I
where
    I: IntoIterator<Item = SqlSnippet<'a, T>>,
{
    fn join(self, sep: &'a str) -> SqlSnippet<'a, T> {
        match self.into_iter().fold(SqlSnippet(vec![]), |SqlSnippet(mut acc), SqlSnippet(v)| {
            acc.push(SqlSnippetChunk::Borrowed(sep));
            acc.extend(v);
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
pub fn param<T: ?Sized>(p: &T) -> SqlSnippet<T> {
    SqlSnippet(vec![SqlSnippetChunk::Param(p)])
}

impl<'a, T: ?Sized> IntoSnippet<'a, T> for &'a str {
    fn into(self) -> SqlSnippet<'a, T> {
        SqlSnippet(vec![SqlSnippetChunk::Borrowed(self)])
    }
}

impl<'a, T: ?Sized> IntoSnippet<'a, T> for String {
    fn into(self) -> SqlSnippet<'a, T> {
        SqlSnippet(vec![SqlSnippetChunk::Owned(self)])
    }
}

impl<'a, T: ?Sized> Add for SqlSnippet<'a, T> {
    type Output = Self;

    fn add(self, other: Self) -> Self::Output {
        let (SqlSnippet(mut l), SqlSnippet(r)) = (self, other);
        l.extend(r);
        SqlSnippet(l)
    }
}

impl<'a, T: ?Sized> Add<SqlSnippet<'a, T>> for &'a str {
    type Output = SqlSnippet<'a, T>;
    fn add(self, snippet: SqlSnippet<'a, T>) -> SqlSnippet<'a, T> {
        match snippet {
            SqlSnippet(mut r) => {
                r.insert(0, SqlSnippetChunk::Borrowed(self));
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
                l.push(SqlSnippetChunk::Borrowed(s));
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
                r.insert(0, SqlSnippetChunk::Owned(self));
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
                l.push(SqlSnippetChunk::Owned(s));
                SqlSnippet(l)
            }
        }
    }
}
// #[allow(unused_macros)]
// macro_rules! param_placeholder_format {
//     () => {
//         "${pos}{data_type:.0}"
//     };
// }
// #[allow(unused_imports)]
// pub(super) use param_placeholder_format;
#[allow(unused_macros)]
macro_rules! generate_fn {
    (@get_data_type $pp:ident false) => { &None as &Option<Cow<str>>};
    (@get_data_type $pp:ident true) => { $pp.to_data_type() };

    (@generate $use_data_type:tt $default_data_type:tt) => {
        pub fn generate<T: ?Sized +ToParam>(s: SqlSnippet<T>) -> (String, Vec<&T>, u32) {
            let default = $default_data_type.to_string();
            match s {
                SqlSnippet(c) => c.iter().fold((String::new(), vec![], 1), |acc, v| {
                    let (mut sql, mut params, pos) = acc;
                    match v {
                        SqlSnippetChunk::Owned(s) => {
                            sql.push_str(s);
                            (sql, params, pos)
                        }
                        SqlSnippetChunk::Borrowed(s) => {
                            sql.push_str(s);
                            (sql, params, pos)
                        },
                        SqlSnippetChunk::Param(p) => {
                            let data_type:&str = match generate_fn!(@get_data_type p $use_data_type) {
                                Some(s) => &*s,
                                None => &default,
                            };
                            sql.push_str(format!(param_placeholder_format!(), pos=pos.to_string(), data_type=data_type).as_str());
                            params.push(p);
                            (sql, params, pos + 1)
                        }
                    }
                }),
            }
        }
    };
    (true, $default_data_type:tt) => {
        generate_fn!(@generate true $default_data_type);
    };
    ($use_data_type:tt) => {
        generate_fn!(@generate $use_data_type "");
    };
    () => {
        generate_fn!(@generate false "");
    };

}
//generate_fn!();
#[allow(unused_imports)]
pub(super) use generate_fn;

// #[cfg(test)]
// mod tests {
//     #[cfg(feature = "postgresql")]
//     use postgres_types::ToSql;
//     use pretty_assertions::assert_eq;

//     use super::SqlSnippetChunk::*;
//     use super::*;
//     fn s(s: &str) -> String { s.to_string() }
//     #[test]
//     fn basic() {
//         assert_eq!(
//             sql("select * from tbl where id = ") + param(&20),
//             SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)])
//         );
//         assert_eq!(
//             "select * from tbl where id = " + param(&20),
//             SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)])
//         );
//         assert_eq!(param(&20) + "=10", SqlSnippet(vec![Param(&20), Sql(s("=10"))]));
//         let query = "select * from tbl where id = ".to_string();
//         assert_eq!(query + param(&20), SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)]));
//         //assert_eq!( query.as_str() + param(&20), SqlSnippet(vec![Sql(s("select * from tbl where id = ")), Param(&20)]) );
//         assert_eq!(
//             generate("select * from tbl where id > " + param(&20) + " and id < " + param(&30)),
//             ("select * from tbl where id > $1 and id < $2".to_string(), vec![&20, &30], 3)
//         );
//     }

//     #[cfg(feature = "postgresql")]
//     #[test]
//     fn dyn_parameters() {
//         let p1: &(dyn ToSql + Sync) = &20;
//         let p2: &(dyn ToSql + Sync) = &"name";
//         let snippet = "select * from tbl where id > " + param(p1) + " and name = " + param(p2);
//         let (q, p, i) = generate(snippet);
//         assert_eq!(q, "select * from tbl where id > $1 and name = $2".to_string());
//         assert_eq!(format!("{:?}", p), format!("{:?}", vec![p1, p2]));
//         assert_eq!(i, 3);
//     }
// }
