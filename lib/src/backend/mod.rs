use crate::{config::VhostConfig, error::Result, api::{ApiRequest,ApiResponse}, schema::DbSchema};
use serde_json::{Value};
use async_trait::async_trait;
use regex::Regex;
use std::{fs};
use std::path::Path;

#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive (Debug)]
enum SplitStr<'a> {
    Str(&'a str),
    Sep(&'a str),
}

fn split_keep<'a>(r: &Regex, text: &'a str) -> Vec<SplitStr<'a>> {
    let mut result = Vec::new();
    let mut last = 0;
    for (index, matched) in text.match_indices(r) {
        if last != index {
            result.push(SplitStr::Str(&text[last..index]));
        }
        result.push(SplitStr::Sep(matched));
        last = index + matched.len();
    }
    if last < text.len() {
        result.push(SplitStr::Str(&text[last..]));
    }
    result
}

pub fn include_files(template: String) -> String {
    let r = Regex::new(r"\{@[^}]+\}").expect("Invalid regex");
    split_keep(&r, template.as_str()).into_iter()
    .map(|v| match v {
        SplitStr::Str(s) => s.to_owned(),
        SplitStr::Sep(s) => {
            let file_name = &s[2..(s.len()-1)];
            //TODO!!! this allows including any file, should this be restricted in some way?
            let contents = fs::read_to_string(Path::new(file_name)).unwrap_or(format!("{{not found @{}}}", file_name));
            contents
        }
    })
    .collect::<Vec<_>>()
    .join("")
}

#[async_trait]
pub trait Backend{
    async fn init(vhost: String, config: VhostConfig) -> Result<Self> where Self: Sized;
    async fn execute(&self, authenticated: bool, request: &ApiRequest, role: Option<&String>, jwt_claims: &Option<Value>) -> Result<ApiResponse>;
    fn db_schema(&self) -> &DbSchema;
    fn config(&self) -> &VhostConfig;
}


#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_split_keep() {
        let seperator = Regex::new(r"[ ,.]+").expect("Invalid regex");
        let splits = split_keep(&seperator, "this... is a, test");
        //for split in splits {
        println!("{:?}", splits);
        //}
        assert!(false)
    }

    #[test]
    fn test_include_files() {
        let template = "this  in included =>{@include.html}<=".to_owned();
        let result = include_files(template);
        println!("{}", result);
        assert!(false)
    }
}