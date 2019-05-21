use hyper::{Chunk, StatusCode};
use serde_json::{Error as JsonError, Value as JsonValue};
use std::error::Error as ErrorTrait;
use std::fmt::{Display, Formatter};
use std::net::IpAddr;

/// Errors related to blocking protocol as defined by consul
#[derive(Debug, Copy, Clone)]
pub enum ProtocolError {
    /// Consul did not reply with X-Consul-Index header
    BlockingMissing,
    /// Consul did not reply with Content-Type: application/json
    ContentTypeNotJson,
    /// Consul did not reply with 200 Ok status
    NonOkResult(StatusCode),
    /// connection refused to consul
    ConnectionRefused,
    /// we had an error, and consumer resetted the stream
    StreamRestarted,
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            ProtocolError::BlockingMissing => write!(f, "{}", self.description()),
            ProtocolError::ContentTypeNotJson => write!(f, "{}", self.description()),
            ProtocolError::NonOkResult(ref status) => {
                write!(f, "Non ok result from consul: {}", status)
            }
            ProtocolError::ConnectionRefused => write!(f, "connection refused to consul"),
            ProtocolError::StreamRestarted => write!(f, "consumer restarted the stream"),
        }
    }
}

impl ErrorTrait for ProtocolError {
    fn description(&self) -> &str {
        match *self {
            ProtocolError::BlockingMissing => "X-Consul-Index missing from response",
            ProtocolError::ContentTypeNotJson => "Consul replied with a non-json content",
            ProtocolError::NonOkResult(_) => "Non ok result from consul",
            ProtocolError::ConnectionRefused => "connection refused to consul",
            ProtocolError::StreamRestarted => "consumer restarted the stream",
        }
    }
}

/// Error that Watch may yield *in the stream*
#[derive(Debug)]
pub enum ParseError {
    /// Consul protocol error (missing header, unknown return format)
    Protocol(ProtocolError),
    /// Json result does not fit expected format
    UnexpectedJsonFormat,
    /// The data is not in json format
    BodyParsing(JsonError),
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        match *self {
            ParseError::Protocol(ref pe) => write!(f, "Protocol error: {}", pe),
            ParseError::UnexpectedJsonFormat => write!(f, "{}", self.description()),
            ParseError::BodyParsing(ref je) => write!(f, "Data not in json format: {}", je),
        }
    }
}

impl ErrorTrait for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Protocol(_) => "Protocol error",
            ParseError::UnexpectedJsonFormat => "Unexpected json format",
            ParseError::BodyParsing(_) => "Data not in json format",
        }
    }
}

impl From<ProtocolError> for ParseError {
    fn from(e: ProtocolError) -> ParseError {
        ParseError::Protocol(e)
    }
}

/// Trait for parsing types out of consul json replies
pub trait ConsulReply {
    /// The kind of replies this parser yields
    type Reply;
    /// Parse an http body and give back a result
    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError>;
}

pub struct ServiceNameTag {
    pub name: String,
    pub tags: Vec<String>,
}

fn from_json_to_services_tags(value: &JsonValue) -> Result<Vec<ServiceNameTag>, ParseError> {
    if let JsonValue::Object(ref map) = value {
        let mut out = Vec::with_capacity(map.len());
        for (k, v) in map.iter() {
            if v.is_array() {
                if k != "consul" {
                    let tag_vec: Vec<String> =
                        serde_json::from_value(v.to_owned()).map_err(ParseError::BodyParsing)?;
                    out.push(ServiceNameTag {
                        name: k.to_string(),
                        tags: tag_vec,
                    })
                }
            } else {
                return Err(ParseError::UnexpectedJsonFormat);
            }
        }
        Ok(out)
    } else {
        Err(ParseError::UnexpectedJsonFormat)
    }
}
/// Parse services list in consul
#[derive(Debug)]
pub struct Services {}

impl ConsulReply for Services {
    type Reply = Vec<ServiceNameTag>;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
        let v: JsonValue = serde_json::from_slice(&buf).map_err(ParseError::BodyParsing)?;
        from_json_to_services_tags(&v)
    }
}

/// Parse node list from services in consul
#[derive(Debug)]
pub struct HealthyServiceNodes {}

#[derive(Deserialize)]
pub struct JsonUpperNode {
    #[serde(rename = "Node")]
    pub node: JsonInnerNode,
    #[serde(rename = "Service")]
    pub service: JsonInnerService,
}

/// Node hosting services
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct JsonInnerNode {
    /// Node name
    #[serde(rename = "Node")]
    pub name: String,

    /// Node address
    #[serde(rename = "Address")]
    pub address: IpAddr,
}

/// Service information
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct JsonInnerService {
    /// Node name
    #[serde(rename = "Service")]
    pub name: String,

    /// Node address
    #[serde(rename = "Port")]
    pub port: u16,
}

impl ConsulReply for HealthyServiceNodes {
    type Reply = Vec<JsonUpperNode>;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
        let v: Vec<JsonUpperNode> =
            serde_json::from_slice(&buf).map_err(ParseError::BodyParsing)?;
        Ok(v)
    }
}
#[derive(Deserialize)]
struct JsonAgent {
    #[serde(rename = "Member")]
    member: JsonMember,
}

#[derive(Deserialize)]
struct JsonMember {
    #[serde(rename = "Addr")]
    addr: IpAddr,
}

/// Parse node list from services in consul
#[derive(Debug)]
pub struct Agent {
    /// public ip address used by this address
    pub member_address: IpAddr,
}

impl ConsulReply for Agent {
    type Reply = Agent;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
        let agent: JsonAgent = serde_json::from_slice(&buf).map_err(ParseError::BodyParsing)?;
        Ok(Agent {
            member_address: agent.member.addr,
        })
    }
}

#[cfg(test)]

mod test {
    use super::*;

    #[test]
    fn test_from_json_to_services_tags() {
        let json = json!( {
          "ais-ps-debugger": [
            "trv-env-dev",
            "trv-net-dev-internal",
            "ais-ps-debugger"
          ],
          "ais-reports-prod": [
            "trv-net-dev-internal",
            "trv-env-dev",
            "ais-reports-prod"
          ],
        });
        let result = from_json_to_services_tags(&json);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.iter().any(|x| x.name == "ais-ps-debugger"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-ps-debugger")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "ais-ps-debugger"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-ps-debugger")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "trv-net-dev-internal"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-ps-debugger")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "trv-env-dev"));
        assert!(result.iter().any(|x| x.name == "ais-reports-prod"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-reports-prod")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "trv-env-dev"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-reports-prod")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "trv-env-dev"));
        assert!(result
            .iter()
            .find(|&x| x.name == "ais-reports-prod")
            .unwrap()
            .tags
            .iter()
            .any(|t| t == "trv-env-dev"));
    }
}
