//helper related to rocket framework

use rocket::{
    http::{HeaderMap,},
    form::{FromForm, ValueField, DataField, Options, Result as FormResult},
    request::{FromRequest, Outcome, Request},
    response::{Responder, Result, Response},
    http::{Header, ContentType, Status,},
};

use std::ops::Deref;

#[derive(Debug)]
pub struct QueryString<'r> (Vec<(&'r str, &'r str)>);

#[rocket::async_trait]
impl<'v> FromForm<'v> for QueryString<'v> {
    type Context = Vec<(&'v str, &'v str)>;

    fn init(_opts: Options) -> Self::Context {
        vec![]
    }

    fn push_value(ctxt: &mut Self::Context, field: ValueField<'v>) {
        ctxt.push((field.name.source(), field.value));
    }

    async fn push_data(_ctxt: &mut Self::Context, _field: DataField<'v, '_>) {
    }

    fn finalize(this: Self::Context) -> FormResult<'v, Self> {
        Ok(QueryString(this))
    }
}

impl<'r> Deref for QueryString<'r> {
	type Target = Vec<(&'r str, &'r str)>;
	fn deref(&self) -> &Self::Target {&self.0}
}

#[derive(Debug)]
pub struct AllHeaders<'r>(&'r rocket::http::HeaderMap<'r>);

#[rocket::async_trait]
impl<'r> FromRequest<'r> for AllHeaders<'r> {
	type Error = std::convert::Infallible;
	async fn from_request( req: &'r rocket::Request<'_>) -> Outcome<Self, Self::Error> {
		Outcome::Success(AllHeaders(req.headers()))
	}
}

impl<'r> Deref for AllHeaders<'r> {
	type Target = HeaderMap<'r>;
	fn deref(&self) -> &Self::Target {&self.0}
}

#[derive(Debug)]
pub struct ApiResponse {
    pub response: (Status, (ContentType, String)),
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
	fn deref(&self) -> &Self::Target {&self.0}
}
