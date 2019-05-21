use super::ParseError;
use http::uri::InvalidUri;
use hyper::error::Error as HyperError;
use native_tls::Error as TlsError;
use std::error::Error as ErrorTrait;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::io::Error as IoError;
use url::ParseError as UrlParseError;

/// General errors that breaks the stream
#[derive(Debug)]
pub enum Error {
    /// Error given internaly by hyper
    Http(HyperError),
    /// You have polled the watcher from two different threads
    InvalidState,
    /// You have given us an invalid url
    InvalidUrl(UrlParseError),
    /// Error while initializing tls
    Tls(TlsError),
    /// uncatched io error
    Io(IoError),
    /// consul response failed to parse
    BodyParse(ParseError),
    /// unexpected url conversion error (from Url to hyper::Uri
    InternalUrlToUri(InvalidUri),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        match *self {
            Error::Http(ref he) => write!(f, "http error: {}", he),
            Error::InvalidState => write!(f, "invalid state reached"),
            Error::InvalidUrl(ref pe) => write!(f, "invalid url: {}", pe),
            Error::Tls(ref te) => write!(f, "{}", te),
            Error::Io(ref ie) => write!(f, "{}", ie),
            Error::BodyParse(ref be) => write!(f, "{}", be),
            Error::InternalUrlToUri(ref ue) => write!(f, "{}", ue),
        }
    }
}

impl ErrorTrait for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Http(_) => "http error",
            Error::InvalidState => "invalid state reached",
            Error::InvalidUrl(_) => "invalid url",
            Error::Tls(_) => "Tls initialization problem",
            Error::Io(_) => "io problem",
            Error::BodyParse(_) => "body parse problem",
            Error::InternalUrlToUri(_) => "unexpected url to uri conversion issue",
        }
    }
}

impl From<UrlParseError> for Error {
    fn from(e: UrlParseError) -> Error {
        Error::InvalidUrl(e)
    }
}

impl From<TlsError> for Error {
    fn from(e: TlsError) -> Error {
        Error::Tls(e)
    }
}

impl From<HyperError> for Error {
    fn from(e: HyperError) -> Error {
        Error::Http(e)
    }
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Error {
        Error::Io(e)
    }
}

impl From<ParseError> for Error {
    fn from(e: ParseError) -> Error {
        Error::BodyParse(e)
    }
}

impl From<InvalidUri> for Error {
    fn from(e: InvalidUri) -> Error {
        Error::InternalUrlToUri(e)
    }
}
