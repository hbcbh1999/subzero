//helpers related to rocket framework
use rocket::{
    form::{DataField, FromForm, Options, Result as FormResult, ValueField},
    http::{ContentType as HTTPContentType, CookieJar, Header, HeaderMap, Status},
    request::{FromRequest, Outcome, Request},
    response::{Responder, Response, Result},
};
use rocket::response::{self,};
use std::io::Cursor;
use subzero::api::ContentType::{self, SingularJSON, TextCSV, ApplicationJSON};
use subzero::error::Error;
use std::{collections::HashMap, ops::Deref};

lazy_static! {
    static ref SINGLE_CONTENT_TYPE: HTTPContentType = HTTPContentType::parse_flexible("application/vnd.pgrst.object+json").unwrap();
}

#[derive(Debug)]
pub struct QueryString<'r>(Vec<(&'r str, &'r str)>);

#[rocket::async_trait]
impl<'v> FromForm<'v> for QueryString<'v> {
    type Context = Vec<(&'v str, &'v str)>;

    fn init(_opts: Options) -> Self::Context { vec![] }

    fn push_value(ctxt: &mut Self::Context, field: ValueField<'v>) { ctxt.push((field.name.source(), field.value)); }

    async fn push_data(_ctxt: &mut Self::Context, _field: DataField<'v, '_>) {}

    fn finalize(this: Self::Context) -> FormResult<'v, Self> { Ok(QueryString(this)) }
}

impl<'r> Deref for QueryString<'r> {
    type Target = Vec<(&'r str, &'r str)>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug)]
pub struct AllHeaders<'r>(&'r HeaderMap<'r>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AllHeaders<'r> {
    type Error = std::convert::Infallible;
    async fn from_request(req: &'r rocket::Request<'_>) -> Outcome<Self, Self::Error> { Outcome::Success(AllHeaders(req.headers())) }
}

impl<'r> Deref for AllHeaders<'r> {
    type Target = HeaderMap<'r>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug)]
pub struct ApiResponse {
    pub response: (Status, (HTTPContentType, String)),
    pub headers: Vec<Header<'static>>,
    //pub content_range: Header<'static>,
}

impl<'r> Responder<'r, 'static> for ApiResponse {
    fn respond_to(self, req: &'r Request<'_>) -> Result<'static> {
        let mut response = Response::build_from(self.response.respond_to(&req)?);
        for h in self.headers {
            response.header_adjoin(h);
        }
        response.ok()
    }
}

pub struct Vhost<'a>(pub Option<&'a str>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Vhost<'r> {
    type Error = ();

    async fn from_request(request: &'r Request<'_>) -> rocket::request::Outcome<Self, Self::Error> {
        Outcome::Success(Vhost(request.headers().get_one("Host")))
    }
}
impl<'r> Deref for Vhost<'r> {
    type Target = Option<&'r str>;
    fn deref(&self) -> &Self::Target { &self.0 }
}

pub struct RocketError(pub Error);
#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for RocketError {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let err = match self { RocketError(e) => e};
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
pub fn to_rocket_content_type(ct: ContentType) -> HTTPContentType {
    match ct {
        SingularJSON => SINGLE_CONTENT_TYPE.clone(),
        TextCSV => HTTPContentType::CSV,
        ApplicationJSON => HTTPContentType::JSON,
    }
}

pub fn cookies_as_hashmap<'a>(cookies: &'a CookieJar<'a>) -> HashMap<String, String> {
    cookies.iter().map(|c| (c.name().to_string(), c.value().to_string())).collect()
}
pub fn headers_as_hashmap<'a>(headers: &'a AllHeaders<'a>) -> HashMap<String, String> {
    headers.iter().map(|h| (h.name().as_str().to_string(), h.value().to_string())).collect()
}
