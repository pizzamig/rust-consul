use super::Client;
use consul::{ConsulReply, ParseError, ProtocolError};
use error::Error;
use futures::{Async, Future, Poll, Stream};
use hyper::client::ResponseFuture;
use hyper::{Body, Chunk, HeaderMap, StatusCode, Uri};
use std::marker::PhantomData;
use std::str::FromStr;
use url::Url;

fn url_to_uri(uri: &Url) -> Result<Uri, Error> {
    Uri::from_str(uri.as_str()).map_err(std::convert::Into::into)
}

#[derive(Debug)]
pub struct BodyBuffer {
    inner: Body,
    buffer: Chunk,
}

impl BodyBuffer {
    pub fn new(inner: Body) -> BodyBuffer {
        BodyBuffer {
            inner,
            buffer: Chunk::default(),
        }
    }
}

impl Future for BodyBuffer {
    type Item = Chunk;
    type Error = crate::error::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("polling BodyBuffer");
        loop {
            match self.inner.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    let buffer = std::mem::replace(&mut self.buffer, Chunk::default());
                    return Ok(Async::Ready(buffer));
                }
                Ok(Async::Ready(Some(data))) => {
                    self.buffer.extend(data);
                    // loop, see if there is any more data here
                }
                Err(e) => return Err(Error::Http(e)),
            }
        }
    }
}

#[derive(Debug)]
pub struct FutureConsul<T> {
    pub state: FutureState,
    pub phantom: PhantomData<T>,
}

impl<T> Future for FutureConsul<T>
where
    T: ConsulReply,
{
    type Item = T::Reply;
    type Error = Error;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // poll()? pattern will bubble up the error
        match self.state.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(body) => T::parse(&body).map(Async::Ready).map_err(Error::BodyParse),
        }
    }
}

#[derive(Debug)]
pub enum FutureState {
    Init {
        base_uri: Url,
        client: Client,
    },
    PendingHeaders {
        base_uri: Url,
        client: Client,
        request: ResponseFuture,
    },
    PendingBody {
        base_uri: Url,
        client: Client,
        headers: HeaderMap,
        body: BodyBuffer,
    },
    Done,
    Working,
}

impl Future for FutureState {
    type Item = Chunk;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("polling FutureState");
        loop {
            match std::mem::replace(self, FutureState::Working) {
                FutureState::Init { base_uri, client } => {
                    trace!("querying uri: {}", base_uri);

                    let request = client.http_client.get(url_to_uri(&base_uri)?);
                    trace!("no response for now => PendingHeader");
                    *self = FutureState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                    };
                }
                FutureState::PendingHeaders {
                    base_uri,
                    client,
                    mut request,
                } => {
                    trace!("polling headers");

                    match request.poll()? {
                        Async::Ready(response_headers) => {
                            let status = response_headers.status();
                            let headers = response_headers.headers().clone();
                            let response_has_json_content_type = headers
                                .get(hyper::header::CONTENT_TYPE)
                                .map(|h| h.eq("application/json"))
                                .unwrap_or(false);
                            if status != StatusCode::OK {
                                let err = ProtocolError::NonOkResult(status);
                                return Err(Error::BodyParse(ParseError::Protocol(err)));
                            } else if !response_has_json_content_type {
                                let err = ProtocolError::ContentTypeNotJson;
                                return Err(Error::BodyParse(ParseError::Protocol(err)));
                            } else {
                                trace!("got headers {} {:?} => PendingBody", status, headers);
                                let body = BodyBuffer::new(response_headers.into_body());
                                *self = FutureState::PendingBody {
                                    base_uri,
                                    client,
                                    headers,
                                    body,
                                };
                            }
                        }
                        Async::NotReady => {
                            trace!("still no headers => PendingHeaders");
                            *self = FutureState::PendingHeaders {
                                base_uri,
                                client,
                                request,
                            };
                            return Ok(Async::NotReady);
                        }
                    }
                }
                FutureState::PendingBody {
                    base_uri,
                    client,
                    headers,
                    mut body,
                } => {
                    trace!("polling body");

                    if let Async::Ready(body) = body.poll()? {
                        *self = FutureState::Done;
                        return Ok(Async::Ready(body));
                    } else {
                        *self = FutureState::PendingBody {
                            base_uri,
                            client,
                            headers,
                            body,
                        };
                        return Ok(Async::NotReady);
                    }
                }

                // Dead end
                FutureState::Working | FutureState::Done => {
                    return Err(Error::InvalidState);
                }
            }
        }
    }
}
