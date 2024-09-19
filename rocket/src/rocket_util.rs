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
//helpers related to rocket framework
use rocket::{
    form::{DataField, FromForm, Options, Result as FormResult, ValueField},
    http::{ContentType as HTTPContentType, Header, HeaderMap, Status},
    request::{FromRequest, Outcome, Request},
    response::{Responder, Response, Result},
};
use rocket::response::{self};
use std::io::Cursor;
use crate::error::Error;
use std::ops::Deref;
//use hyper::{http::HeaderMap as HyperHeaderMap};
//use bytes::{Bytes};
//use hyper_reverse_proxy;
//use hyper::{Client, client::HttpConnector};

#[derive(Debug)]
pub struct QueryString<'a>(pub Vec<(&'a str, &'a str)>);

#[rocket::async_trait]
impl<'v> FromForm<'v> for QueryString<'v> {
    type Context = Vec<(&'v str, &'v str)>;
    fn init(_opts: Options) -> Self::Context {
        vec![]
    }
    fn push_value(ctxt: &mut Self::Context, field: ValueField<'v>) {
        ctxt.push((field.name.source(), field.value));
    }
    async fn push_data(_ctxt: &mut Self::Context, _field: DataField<'v, '_>) {}
    fn finalize(this: Self::Context) -> FormResult<'v, Self> {
        Ok(QueryString(this))
    }
}

// impl<'r> Deref for QueryString {
//     type Target = Vec<(String, String)>;
//     fn deref(&self) -> &Self::Target { &self.0 }
// }

#[derive(Debug)]
pub struct AllHeaders<'r>(&'r HeaderMap<'r>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AllHeaders<'r> {
    type Error = std::convert::Infallible;
    async fn from_request(req: &'r rocket::Request<'_>) -> Outcome<Self, Self::Error> {
        Outcome::Success(AllHeaders(req.headers()))
    }
}

impl<'r> Deref for AllHeaders<'r> {
    type Target = HeaderMap<'r>;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug)]
pub struct ApiResponse {
    pub response: (Status, (HTTPContentType, String)),
    pub headers: Vec<Header<'static>>,
}

impl<'r> Responder<'r, 'static> for ApiResponse {
    fn respond_to(self, req: &'r Request<'_>) -> Result<'static> {
        let mut response = Response::build_from(self.response.respond_to(req)?);
        for h in self.headers {
            if h.name() != "content-type" {
                response.header_adjoin(h);
            }
        }
        response.ok()
    }
}

// #[derive(Debug)]
// pub struct ProxyResponse {
//     pub status: u16,
//     pub headers: HyperHeaderMap,
//     pub body: Bytes,
// }

// impl<'r> Responder<'r, 'static> for ProxyResponse {
//     fn respond_to(self, _req: &'r Request<'_>) -> Result<'static> {
//         let mut response = Response::new();
//         response.set_status(Status::from_code(self.status).unwrap_or_default());
//         for (n,v) in self.headers {
//             response.set_header(Header::new(n.unwrap().as_str().to_owned(), v.to_str().unwrap_or("").to_owned()));
//         }
//         response.set_sized_body(self.body.len(), Cursor::new(self.body));
//         Ok(response)
//     }
// }

// #[derive(Debug)]
// pub struct DenoProxyResponse {
//     pub body: String
// }

// #[rocket::async_trait]
// impl<'r> FromRequest<'r> for DenoProxyResponse {
//     type Error = std::convert::Infallible;
//     async fn from_request(req: &'r rocket::Request<'_>) -> Outcome<Self, Self::Error> {
//         let client = req.rocket().state::<Client<HttpConnector>>().unwrap();
//         let hreq = hyper::Request::builder()
//             .uri(format!("http://localhost:8080{}",req.uri().path().as_str()))
//             .header("User-Agent", "my-awesome-agent/1.0")
//             .body(hyper::Body::empty())
//             .unwrap();
//         match client.request(hreq).await {
//             Ok(res) => {
//                 let body = res.into_body();
//                 let body_bytes = hyper::body::to_bytes(body).await.unwrap();
//                 Outcome::Success(
//                     DenoProxyResponse{
//                         body: String::from_utf8(body_bytes.to_vec()).unwrap()
//                     }
//                 )
//             },
//             Err(e) => {
//                 println!("error: {:?}", e);
//                 Outcome::Forward(())
//             }
//         }

//     }
// }

pub struct RocketError(pub Error);
#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for RocketError {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let RocketError(err) = self;
        let status = Status::from_code(err.status_code()).unwrap();
        let body = err.json_body().to_string();
        let mut response = Response::build();
        response.status(status);
        response.sized_body(body.len(), Cursor::new(body));

        for (h, v) in err.headers() {
            response.raw_header(h, v);
        }

        response.ok()
    }
}
