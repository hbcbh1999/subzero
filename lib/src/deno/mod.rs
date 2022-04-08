use http::{Method, HeaderMap, Response};
use hyper::client::{Client,HttpConnector};
use hyper::{Body};
use tokio::process::{Command, Child};
use crate::error::{Result, Error, IoError, HttpRequestError};
use crate::config::DenoConfig;
use snafu::{ResultExt};

#[derive(Debug)]
pub struct DenoProxy {
    base_url: String,
    client: Client<HttpConnector>,
    _deno: Child,
}

impl DenoProxy {
    pub async fn new(config: &DenoConfig) -> Result<Self> {
        let mut command = Command::new("deno");
        command.arg("run").kill_on_drop(true);
        config.parameters.split_whitespace().for_each(|param| {
            command.arg(param);
        });
        command.arg(config.script.clone());

        let _deno = command.spawn().context(IoError)?;
        Ok(DenoProxy {
            base_url: config.base_url.clone(),
            client: Client::new(),
            _deno,
        })
    }

    // proxy request to deno
    pub async fn forward( &self, method: &Method, url: &str, headers: HeaderMap, body: Option<String>, client_ip: &str)
        -> Result<Response<Body>> {
        let mut request = hyper::Request::builder()
            .uri(format!("{}{}", self.base_url, url))
            .method(method)
            .header("X-Forwarded-For", client_ip);
        let request_headers = request.headers_mut().unwrap();
        request_headers.extend(headers.into_iter());

        let request =  request.body(match body {
            Some(b) => Body::from(b),
            None => Body::empty()
        }).context(HttpRequestError)?;

        match self.client.request(request).await {
            Ok(response) => {
                Ok(response)
            },
            Err(e) => {
                Err(Error::InternalError { message: e.to_string() })
            }
        }
    }
}

// impl Drop for DenoProxy {
//     fn drop(&mut self) {
//         //
//     }
// }
