//! Future-aware client for consul
//!
//! This library is an client for consul that gives you stream of changes
//! done in consul

//#![deny(missing_docs, missing_debug_implementations, warnings)]

mod consul;
mod error;
mod util;
extern crate http;
extern crate hyper;
extern crate hyper_tls;
#[macro_use]
extern crate log;
extern crate futures;
extern crate native_tls;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[cfg(test)]
#[macro_use]
extern crate serde_json;
#[cfg(not(test))]
extern crate serde_json;
extern crate tokio_core;
extern crate url;

use consul::{Agent, ConsulReply, HealthyServiceNodes, Services};
use consul::{ParseError, ProtocolError};
use error::Error;
use futures::{Async, Future, Poll, Stream};
use hyper::client::{Client as HttpClient, HttpConnector, ResponseFuture};
use hyper::header::HeaderMap;
use hyper::{Chunk, StatusCode, Uri};
use hyper_tls::HttpsConnector;
use std::fmt::{Debug, Display, Formatter, Write};
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio_core::reactor::{Handle, Timeout};
use url::Url;
use util::{BodyBuffer, FutureConsul, FutureState};

#[derive(Clone, Copy, Debug)]
struct Blocking {
    index: u64,
}

impl Blocking {
    fn from(headers: &HeaderMap) -> Result<Self, ()> {
        if headers.contains_key("X-Consul-Index") {
            let hv = headers
                .get("X-Consul-Index")
                .unwrap()
                .to_str()
                .unwrap()
                .parse()
                .unwrap();
            Ok(Blocking { index: hv })
        } else {
            Err(())
        }
    }

    fn to_string(self) -> String {
        let mut out = String::new();
        let _ = write!(out, "{}", self.index);
        out
    }

    fn add_to_uri(self, uri: &Url) -> Url {
        let mut uri = uri.clone();
        uri.query_pairs_mut()
            .append_pair("index", self.to_string().as_str())
            .finish();
        uri
    }
}

impl Default for Blocking {
    fn default() -> Blocking {
        Blocking { index: 0 }
    }
}

impl FromStr for Blocking {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let index = s.parse::<u64>()?;
        Ok(Blocking { index })
    }
}

impl Display for Blocking {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.index)
    }
}

/// Consul client
#[derive(Debug, Clone)]
pub struct Client {
    http_client: HttpClient<HttpsConnector<HttpConnector>>,
    base_uri: Url,
    handle: Handle,
}

impl Client {
    /// Allocate a new consul client
    pub fn new(base_uri: &str, handle: &Handle) -> Result<Client, Error> {
        let base_uri = Url::parse(base_uri)?;

        let connector = HttpsConnector::new(4)?;
        let http_client = HttpClient::builder()
            .keep_alive(true)
            .build::<_, hyper::Body>(connector);

        Ok(Client {
            http_client,
            base_uri,
            handle: handle.clone(),
        })
    }

    /// List services in the kernel and watch them
    pub fn services(&self) -> Watcher<Services> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/catalog/services");

        Watcher {
            state: WatcherState::Init {
                base_uri,
                client: self.clone(),
                error_strategy: ErrorStrategy::default(),
            },
            phantom: PhantomData::<Services>,
        }
    }

    /// Watch changes of nodes on a service
    pub fn watch_service(&self, name: &str, passing: bool) -> Watcher<HealthyServiceNodes> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/health/service/");
        let mut base_uri = base_uri.join(name).unwrap();
        if passing {
            base_uri
                .query_pairs_mut()
                .append_pair("passing", "true")
                .finish();
        }

        Watcher {
            state: WatcherState::Init {
                base_uri,
                client: self.clone(),
                error_strategy: ErrorStrategy::default(),
            },
            phantom: PhantomData::<HealthyServiceNodes>,
        }
    }

    /// Get agent informations
    pub fn agent(&self) -> FutureConsul<Agent> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/agent/self");

        FutureConsul {
            state: FutureState::Init {
                base_uri,
                client: self.clone(),
            },
            phantom: PhantomData::<Agent>,
        }
    }
}

#[derive(Debug)]
enum ErrorHandling {
    RetryBackoff,
}

#[derive(Debug)]
struct ErrorStrategy {
    request_timeout: Duration,
    on_error: ErrorHandling,
}

impl Default for ErrorStrategy {
    fn default() -> ErrorStrategy {
        ErrorStrategy {
            request_timeout: Duration::new(5, 0),
            on_error: ErrorHandling::RetryBackoff,
        }
    }
}

#[derive(Debug)]
struct ErrorState {
    strategy: ErrorStrategy,
    current_retries: u64,
    last_try: Option<Instant>,
    last_contact: Option<Instant>,
    last_ok: Option<Instant>,
    last_error: Option<ProtocolError>,
}

impl ErrorState {
    pub fn next_timeout(&self, handle: &Handle) -> DebugTimeout {
        let retries = if self.current_retries > 10 {
            10
        } else {
            self.current_retries
        };

        debug!("Will sleep for {} seconds and retry", retries);
        let duration = Duration::new(retries, 0);

        DebugTimeout(
            // TODO: meh unwrap()
            Timeout::new(duration, handle).unwrap(),
        )
    }
}

impl From<ErrorStrategy> for ErrorState {
    fn from(strategy: ErrorStrategy) -> ErrorState {
        ErrorState {
            strategy,
            current_retries: 0,
            last_try: None,
            last_contact: None,
            last_ok: None,
            last_error: None,
        }
    }
}

struct DebugTimeout(Timeout);

impl Debug for DebugTimeout {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Timeout")
    }
}

impl Future for DebugTimeout {
    type Item = ();
    type Error = io::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("Timeout::poll() called");
        let res = self.0.poll();

        trace!("res {:?}", res);
        res
    }
}

fn url_to_uri(uri: &Url) -> Uri {
    let out = Uri::from_str(uri.as_str());
    if out.is_err() {
        error!("url malformed: {:?}", uri);
    }

    // TODO: meh unwrap()
    out.unwrap()
}

#[derive(Debug)]
enum WatcherState {
    Init {
        base_uri: Url,
        client: Client,
        error_strategy: ErrorStrategy,
    },
    Completed {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        blocking: Blocking,
    },
    ErrorState {
        base_uri: Url,
        client: Client,
        blocking: Blocking,
        error_state: ErrorState,
        retry: Option<DebugTimeout>,
    },
    PendingHeaders {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        request: ResponseFuture,
        blocking: Blocking,
    },
    PendingBody {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        blocking: Blocking,
        headers: HeaderMap,
        body: BodyBuffer,
    },
    Working,
}

impl Stream for WatcherState {
    type Item = Result<Chunk, ProtocolError>;
    type Error = crate::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        trace!("polling WatcherState");
        loop {
            match mem::replace(self, WatcherState::Working) {
                WatcherState::Init {
                    base_uri,
                    client,
                    error_strategy,
                } => {
                    trace!("querying uri: {}", base_uri);

                    let request = client.http_client.get(url_to_uri(&base_uri));
                    trace!("{}: no response for now => PendingHeader", base_uri);
                    *self = WatcherState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                        error_state: error_strategy.into(),
                        blocking: Blocking::default(),
                    };
                }
                WatcherState::Completed {
                    base_uri,
                    client,
                    blocking,
                    mut error_state,
                } => {
                    let uri = blocking.add_to_uri(&base_uri);
                    trace!("querying uri: {}", uri);

                    error_state.last_try = Some(Instant::now());

                    let request = client.http_client.get(url_to_uri(&uri));
                    trace!("{}: no response for now => PendingHeader", base_uri);
                    *self = WatcherState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                        blocking,
                        error_state,
                    };
                }
                WatcherState::PendingHeaders {
                    base_uri,
                    client,
                    blocking,
                    mut request,
                    mut error_state,
                } => {
                    trace!("{}: polling headers", base_uri);

                    match request.poll() {
                        Err(e) => {
                            if e.is_connect() {
                                let err = ProtocolError::ConnectionRefused;
                                error_state.last_error = Some(err);
                                error_state.current_retries += 1;
                                *self = WatcherState::ErrorState {
                                    base_uri,
                                    client,
                                    blocking,
                                    error_state,
                                    retry: None,
                                };
                                return Ok(Async::Ready(Some(Err(err))));
                            } else {
                                error!("{}: got error, stopping: {}", base_uri, e);
                                return Err(e.into());
                            }
                        }
                        Ok(Async::Ready(response_headers)) => {
                            let status = response_headers.status();
                            let headers = response_headers.headers().clone();
                            let response_has_json_content_type = headers
                                .get(hyper::header::CONTENT_TYPE)
                                .map(|h| h.eq("application/json"))
                                .unwrap_or(false);
                            error_state.last_contact = Some(Instant::now());

                            if status != StatusCode::OK {
                                warn!("{}: got non-200 status: {}", base_uri, status);
                                let err = ProtocolError::NonOkResult(status);
                                error_state.last_error = Some(err);
                                error_state.current_retries += 1;
                                *self = WatcherState::ErrorState {
                                    base_uri,
                                    client,
                                    blocking,
                                    error_state,
                                    retry: None,
                                };
                                return Ok(Async::Ready(Some(Err(err))));
                            }

                            if !response_has_json_content_type {
                                warn!("{}: got non-json content: {:?}", base_uri, headers);
                                error_state.last_error = Some(ProtocolError::ContentTypeNotJson);
                                *self = WatcherState::ErrorState {
                                    base_uri,
                                    client,
                                    blocking,
                                    error_state,
                                    retry: None,
                                };
                            } else {
                                trace!(
                                    "{}: got headers {} {:?} => PendingBody",
                                    base_uri,
                                    status,
                                    headers
                                );
                                let body = BodyBuffer::new(response_headers.into_body());

                                *self = WatcherState::PendingBody {
                                    base_uri,
                                    client,
                                    blocking,
                                    headers,
                                    body,
                                    error_state,
                                };
                            };
                        }
                        Ok(Async::NotReady) => {
                            trace!("{}: still no headers => PendingHeaders", base_uri);
                            *self = WatcherState::PendingHeaders {
                                base_uri,
                                client,
                                blocking,
                                request,
                                error_state,
                            };
                            return Ok(Async::NotReady);
                        }
                    }
                }
                WatcherState::PendingBody {
                    base_uri,
                    client,
                    blocking,
                    headers,
                    mut body,
                    mut error_state,
                } => {
                    trace!("{}: polling body", base_uri);

                    if let Async::Ready(body) = body.poll()? {
                        debug!("{}: got content: {:?}", base_uri, body);
                        let new_blocking =
                            Blocking::from(&headers).map_err(|_| ProtocolError::BlockingMissing);
                        match new_blocking {
                            Err(err) => {
                                error!(
                                    "{}: got error while parsing blocking headers: {:?}, {:?}",
                                    base_uri, headers, err
                                );
                                error_state.last_error = Some(err);
                                error_state.current_retries += 1;
                                *self = WatcherState::ErrorState {
                                    base_uri,
                                    client,
                                    error_state,
                                    blocking,

                                    // The next call to poll() will start the
                                    // timer (don't generate a timer ifclient
                                    // does not need)
                                    retry: None,
                                };
                                return Ok(Async::Ready(Some(Err(err))));
                            }
                            Ok(blocking) => {
                                info!("{}: got blocking headers: {}", base_uri, blocking);
                                error_state.last_ok = Some(Instant::now());
                                error_state.last_error = None;
                                error_state.current_retries = 0;

                                *self = WatcherState::Completed {
                                    base_uri,
                                    client,
                                    blocking,
                                    error_state,
                                };

                                return Ok(Async::Ready(Some(Ok(body))));
                            }
                        }
                    } else {
                        trace!("{}: still no body => PendingBody", base_uri);

                        *self = WatcherState::PendingBody {
                            base_uri,
                            client,
                            headers,
                            blocking,
                            body,
                            error_state,
                        };
                        return Ok(Async::NotReady);
                    }
                }

                WatcherState::ErrorState {
                    base_uri,
                    client,
                    blocking,
                    error_state,
                    retry,
                } => {
                    trace!("{}: still no body => PendingBody", base_uri);
                    if let Some(mut retry) = retry {
                        // We have a timeout loaded, see if it resolved
                        if let Async::Ready(_) = retry.poll()? {
                            trace!("{}: timeout completed", base_uri);
                            *self = WatcherState::Completed {
                                base_uri,
                                client,
                                blocking,
                                error_state,
                            };
                        } else {
                            trace!("{}: timeout not completed", base_uri);
                            *self = WatcherState::ErrorState {
                                base_uri,
                                client,
                                blocking,
                                error_state,
                                retry: Some(retry),
                            };
                            return Ok(Async::NotReady);
                        }
                    } else {
                        let next_timeout = error_state.next_timeout(&client.handle);
                        trace!("{}: setting timeout", base_uri);
                        *self = WatcherState::ErrorState {
                            base_uri,
                            client,
                            blocking,
                            error_state,
                            retry: Some(next_timeout),
                        };
                        // loop will consume the poll
                    }
                }

                // Dead end
                WatcherState::Working => {
                    error!("watcher in working state, weird");
                    return Err(Error::InvalidState);
                }
            }
        }
    }
}

/// Watch changes made in consul and parse those changes
#[derive(Debug)]
pub struct Watcher<T> {
    state: WatcherState,
    phantom: PhantomData<T>,
}

impl<T> Watcher<T> {
    /// Whenever the stream yield an error. The stream closes and
    /// can't be consumed anymore. In such cases, you are required to reset
    /// the stream. It will then, sleep (according to the error strategy)
    /// and reconnect to consul.
    pub fn reset(&mut self) {
        let (base_uri, client, blocking, mut error_state) =
            match mem::replace(&mut self.state, WatcherState::Working) {
                WatcherState::Init {
                    base_uri,
                    client,
                    error_strategy,
                    ..
                } => (
                    base_uri,
                    client,
                    Blocking::default(),
                    ErrorState::from(error_strategy),
                ),
                WatcherState::Completed {
                    base_uri,
                    client,
                    blocking,
                    error_state,
                    ..
                }
                | WatcherState::ErrorState {
                    base_uri,
                    client,
                    blocking,
                    error_state,
                    ..
                }
                | WatcherState::PendingHeaders {
                    base_uri,
                    client,
                    blocking,
                    error_state,
                    ..
                }
                | WatcherState::PendingBody {
                    base_uri,
                    client,
                    blocking,
                    error_state,
                    ..
                } => (base_uri, client, blocking, error_state),
                WatcherState::Working => panic!("stream resetted while polled. State is invalid"),
            };
        error_state.last_error = Some(ProtocolError::StreamRestarted);
        self.state = WatcherState::ErrorState {
            base_uri,
            client,
            blocking,
            error_state,
            retry: None,
        };
    }
}

impl<T> Stream for Watcher<T>
where
    T: ConsulReply,
{
    type Item = Result<T::Reply, ParseError>;
    type Error = Error;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // poll()? pattern will bubble up the error
        match self.state.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(None) => Ok(Async::Ready(None)),
            Async::Ready(Some(Err(e))) => Ok(Async::Ready(Some(Err(e.into())))),
            Async::Ready(Some(Ok(body))) => Ok(Async::Ready(Some(T::parse(&body)))),
        }
    }
}
