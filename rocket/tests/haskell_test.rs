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
#[macro_export]
macro_rules! haskell_test {
    (@status $status_code:ident $status:literal) => {
        println!("matching status: ===\n{}\n====", $status );
        self::assert_eq!($status_code, $status);
    };
    (@header $headers:ident $name:literal $value:literal) => {
        println!("matching header: {}: {} against {:?}", $name, $value, $headers );
        assert!($headers.contains(&($name.to_string(), $value.to_string())));
    };
    (@add_header $request:ident $name:literal $value:literal) => {
        $request = add_header($request, $name, $value);
    };
    (@body_json $response:ident $json:literal) => {
        let body = match $response.into_string().await {
            Some(b) => b,
            None => "no body".to_string()
        };
        println!("expected: ===\n{}\n====", $json );
        println!("body: ===\n{}\n====", body );
        self::assert_eq!(
            serde_json::from_str::<Value>(body.as_str()).map_err(|e| format!("{}",e)),
            serde_json::from_str::<Value>($json).map_err(|e| format!("{}",e))
        );
    };
    (@body_text $response:ident $text:literal) => {
        let body = match $response.into_string().await {
            Some(b) => b,
            None => "no body".to_string()
        };
        println!("expected: ===\n{}\n====", $text );
        println!("body: ===\n{}\n====", body );
        self::assert_eq!(body.as_str(),$text);
    };
    (@body_str $response:ident $str:literal) => {
      let body = match $response.into_string().await {
          Some(b) => b,
          None => "no body".to_string()
      };
      let s = format!("\"{}\"", $str);
      println!("expected: ===\n{}\n====", s );
      println!("body: ===\n{}\n====", body );
      self::assert_eq!(body.as_str(),s);
    };
    (
        $(feature $feature:literal
        $(
            describe $describe:literal $dollar1:tt $(do)?
            $(
                it $it:literal $dollar2:tt $(do)?
                $(
                    //request methodGet "/images?name=eq.A.png" (acceptHdrs "application/octet-stream") ""
                    $(let $token_var:ident = $(authHeaderJWT)? $jwt_token:literal $(in)?)?

                    $(get $get1_url:literal)?
                    $(post $post_url:literal [json|$json_body:literal|])?

                    $(request methodGet $get2_url:literal
                        $([auth])?
                        $([
                          ($get_2_header_nn0:literal,$get_2_header_v0:literal)
                          $(,($get_2_header_nn1:literal,$get_2_header_v1:literal))?
                        ])?
                        $((acceptHdrs $get2_accept_header:literal))?
                        $($get2_body:literal)?
                    )?
                    $(request methodPost $post2_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $post2_accept_header:literal))?
                        $([
                            $(authHeaderJWT $post_2_jwt_token:literal , )?
                            ($post_2_header_nn0:literal,$post_2_header_v0:literal)
                            $(
                              ,($post_2_header_nn1:literal,$post_2_header_v1:literal)
                              $(,($post_2_header_nn2:literal,$post_2_header_v2:literal))?
                            )?
                        ])?
                        $([json|$json2_body:literal|])?
                        $([text|$text2_body:literal|])?
                        $($json22_body:literal)?
                    )?

                    $(request methodDelete $delete_url:literal
                        $([
                            $(authHeaderJWT $delete_jwt_token:literal , )?
                            ($delete_header_nn0:literal,$delete_header_v0:literal)
                            $(
                              ,($delete_header_nn1:literal,$delete_header_v1:literal)
                              $(,($delete_header_nn2:literal,$delete_header_v2:literal))?
                            )?
                        ])?
                        $($delete_body:literal)?
                    )?

                    $(request methodPatch $patch_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $patch_accept_header:literal))?
                        $([
                            $(authHeaderJWT $patch_jwt_token:literal , )?
                            ($patch_header_nn0:literal,$patch_header_v0:literal)
                            $(
                              ,($patch_header_nn1:literal,$patch_header_v1:literal)
                              $(,($patch_header_nn2:literal,$patch_header_v2:literal))?
                            )?
                        ])?
                        $([])?
                        $([json|$patch_json_body:literal|])?
                        $([text|$patch_text_body:literal|])?
                        $($patch_body:literal)?
                    )?

                    $(request methodPut $put_url:literal
                        $([dummy])?
                        $([auth])?
                        $([single])?
                        $((acceptHdrs $put_accept_header:literal))?
                        $([
                            $(authHeaderJWT $put_jwt_token:literal , )?
                            ($put_header_nn0:literal,$put_header_v0:literal)
                            $(
                              ,($put_header_nn1:literal,$put_header_v1:literal)
                              $(,($put_header_nn2:literal,$put_header_v2:literal))?
                            )?
                        ])?
                        $([])?
                        $([json|$put_json_body:literal|])?
                        $([text|$put_text_body:literal|])?
                        $($put_body:literal)?
                    )?

                    shouldRespondWith
                    $($status_simple:literal)?
                    $([json|$json:literal|])?
                    $([text|$text:literal|])?
                    $([str|$str:literal|])?
                    $({
                            $(matchStatus = $status:literal)?
                            $($(,)? matchHeaders = [
                                $($header_name:literal <:> $header_value:literal),*
                            ])?
                    })?
                )*
            )*
        )*
        )*
    ) => {
        demonstrate! {
            $(
                  $(
                    describe $describe {
                      use super::{setup_db, setup_client, INIT_DB, INIT_CLIENT, CLIENT, normalize_url, haskell_test, add_header};
                      use pretty_assertions::assert_eq;
                      use rocket::http::{Accept, Header};
                      use serde_json::Value;
                      use std::str::FromStr;

                      before { setup_db(&INIT_DB); setup_client(&INIT_CLIENT, &CLIENT);}

                      $(

                          #[rocket::async_test]
                          async it $it {
                              $(
                                  {
                                      let client = CLIENT.get().await;

                                      $(
                                        let url = format!("/rest{}",$get1_url);
                                        let mut request = client.get(normalize_url(&url));
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                      )?
                                      $(
                                        let url = format!("/rest{}",$get2_url);
                                        let mut request = client.get(normalize_url(&url));
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                        $(request.add_header(Accept::from_str($get2_accept_header).unwrap());)?
                                        //$($(request.add_header(Header::new($get_2_header_nn,$get_2_header_v));),+)?
                                        $(
                                          haskell_test!(@add_header request $get_2_header_nn0 $get_2_header_v0);
                                          $(
                                              haskell_test!(@add_header request $get_2_header_nn1 $get_2_header_v1);
                                          )?
                                        )?
                                      )?

                                      $(
                                        let url = format!("/rest{}",$post_url);
                                        let mut request = client.post(normalize_url(&url))
                                            .body($json_body);
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                      )?

                                      $(
                                        let url = format!("/rest{}",$post2_url);
                                        let mut request = client.post(normalize_url(&url))
                                            .body($($text2_body)? $($json2_body)? $($json22_body)?);
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                        $(request.add_header(Accept::from_str($post2_accept_header).unwrap());)?

                                        $(
                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$post_2_jwt_token)));
                                          )?
                                          haskell_test!(@add_header request $post_2_header_nn0 $post_2_header_v0);
                                          $(
                                              haskell_test!(@add_header request $post_2_header_nn1 $post_2_header_v1);
                                              $(
                                                haskell_test!(@add_header request $post_2_header_nn2 $post_2_header_v2);
                                              )?
                                          )?
                                        )?
                                      )?

                                      $(
                                        let url = format!("/rest{}",$delete_url);
                                        let mut request = client.delete(normalize_url(&url))
                                            .body($($delete_body)?);
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                        //$(request.add_header(Accept::from_str($delete_accept_header).unwrap());)?

                                        $(
                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$delete_jwt_token)));
                                          )?
                                          haskell_test!(@add_header request $delete_header_nn0 $delete_header_v0);
                                          $(
                                            haskell_test!(@add_header request $delete_header_nn1 $delete_header_v1);
                                              $(
                                                haskell_test!(@add_header request $delete_header_nn2 $delete_header_v2);
                                              )?
                                          )?
                                        )?
                                      )?

                                      $(
                                        let url = format!("/rest{}",$patch_url);
                                        let mut request = client.patch(normalize_url(&url))
                                            .body($($patch_text_body)? $($patch_json_body)? $($patch_body)?);
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                        $(request.add_header(Accept::from_str($patch_accept_header).unwrap());)?

                                        $(
                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$patch_jwt_token)));
                                          )?
                                          haskell_test!(@add_header request $patch_header_nn0 $patch_header_v0);
                                          $(
                                              haskell_test!(@add_header request $patch_header_nn1 $patch_header_v1);
                                              $(
                                                haskell_test!(@add_header request $patch_header_nn2 $patch_header_v2);
                                              )?
                                          )?
                                        )?
                                      )?

                                      $(
                                        let url = format!("/rest{}",$put_url);
                                        let mut request = client.put(normalize_url(&url))
                                            .body($($put_text_body)? $($put_json_body)? $($put_body)?);
                                        request.add_header(Accept::from_str("*/*").unwrap());
                                        $(request.add_header(Accept::from_str($put_accept_header).unwrap());)?

                                        $(
                                          $(
                                            request.add_header(Header::new("Authorization", format!("Bearer {}",$put_jwt_token)));
                                          )?
                                          haskell_test!(@add_header request $put_header_nn0 $put_header_v0);
                                          $(
                                              haskell_test!(@add_header request $put_header_nn1 $put_header_v1);
                                              $(
                                                haskell_test!(@add_header request $put_header_nn2 $put_header_v2);
                                              )?
                                          )?
                                        )?
                                      )?


                                      println!("url ===\n{:?}\n", url);
                                      //request.add_header(Accept::from_str("*/*").unwrap());



                                      $(
                                        request.add_header(Header::new("Authorization", format!("Bearer {}",$jwt_token)));
                                      )?
                                      //println!("request ===\n{:?}\n", request);
                                      let response = request.dispatch().await;
                                      let _status_code = response.status().code;
                                      let _headers = response.headers().iter().map(|h| (h.name().to_string(), h.value().to_string())).collect::<Vec<_>>();
                                      //let _headers = response.headers().clone();
                                      //println!("response ===\n{:?}\n", response);
                                      $(haskell_test!(@body_json response $json);)?
                                      $(haskell_test!(@body_text response $text);)?
                                      $(haskell_test!(@body_str response $str);)?

                                      $(haskell_test!(@status _status_code $status_simple);)?
                                      $($(haskell_test!(@status _status_code $status);)?)?
                                      $($($(haskell_test!(@header _headers $header_name $header_value);)*)?)?
                                      //assert!(false);
                                  }
                              )?
                          }
                      )*
                    }
                  )*
            // }
            )*

        }
    }
}
//pub(crate) use haskell_test;
