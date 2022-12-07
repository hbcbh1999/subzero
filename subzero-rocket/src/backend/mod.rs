use subzero_core::{
    api::{ApiRequest, ApiResponse},
    schema::DbSchema,
};
use std::collections::HashMap;
use crate::error::Result;
use crate::config::{VhostConfig};
use async_trait::async_trait;
use regex::Regex;
use std::{fs};
use std::path::Path;
// use log::{debug};
use ouroboros::self_referencing;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
#[cfg(feature = "postgresql")]
pub mod postgresql;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Debug)]
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
    let r = Regex::new(r"\{@[^#}]+(#[^\}]*)?\}").expect("Invalid regex");
    split_keep(&r, template.as_str())
        .into_iter()
        .map(|v| match v {
            SplitStr::Str(s) => s.to_owned(),
            SplitStr::Sep(s) => {
                let parts = &s[2..(s.len() - 1)].split('#').collect::<Vec<&str>>();
                debug!("parts {:?}", parts);
                let file_name = parts[0];
                let missing_msg = format!("{{not found @{file_name}}}");
                let default_val = parts.get(1).unwrap_or(&(missing_msg.as_str())).to_owned();
                //TODO!!! this allows including any file, should this be restricted in some way?
                let contents = fs::read_to_string(Path::new(file_name)).unwrap_or_else(|_| String::from(default_val));
                debug!("contents for {} {}", file_name, contents);
                contents
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

#[async_trait]
pub trait Backend {
    async fn init(vhost: String, config: VhostConfig) -> Result<Self>
    where
        Self: Sized;
    async fn execute(&self, authenticated: bool, request: &ApiRequest, env: &HashMap<&str, &str>) -> Result<ApiResponse>;
    fn db_schema(&self) -> &DbSchema;
    fn config(&self) -> &VhostConfig;
}

#[self_referencing]
pub struct DbSchemaWrap {
    schema_string: String,
    #[covariant]
    #[borrows(schema_string)]
    schema: Result<DbSchema<'this>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_split_keep() {
        let seperator = Regex::new(r"[ ,.]+").expect("Invalid regex");
        let splits = split_keep(&seperator, "this... is a, test");
        //for split in splits {
        println!("{splits:?}");
        //}
        // assert!(false)
    }

    #[test]
    fn test_include_files() {
        let template = "this  in included =>{@include.html}<=".to_owned();
        let result = include_files(template);
        println!("{result}");
        // assert!(false)
    }
}
