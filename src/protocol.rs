use core::str;

use std::sync::LazyLock;

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::{FramedRead, FramedWrite};

static CRLF: LazyLock<memchr::memmem::Finder<'static>> =
    LazyLock::new(|| memchr::memmem::Finder::new("\r\n"));

/// INFO operation options
#[derive(Clone, Debug, Deserialize)]
pub struct InfoOptions {
    /// The unique identifier of the NATS server.
    pub server_id: Box<str>,

    /// The name of the NATS server.
    pub server_name: Box<str>,

    /// The version of the NATS server.
    pub version: Box<str>,

    /// The version of golang the NATS server was built with.
    pub go: Box<str>,

    /// The IP address used to start the NATS server, by default this
    /// will be 0.0.0.0 and can be configured with -client_advertise host:port.
    pub host: Box<str>,

    /// The port number the NATS server is configured to listen on.
    pub port: u16,

    /// Whether the server supports headers.
    pub headers: bool,

    /// Maximum payload size, in bytes, that the server will accept from the client.
    pub max_payload: usize,

    /// An integer indicating the protocol version of the server. The server version
    /// 1.2.0 sets this to `1` to indicate that it supports the "Echo" feature.
    pub proto: i8,

    /// The internal client identifier in the server. This can be used to filter
    /// client connections in monitoring, correlate with error logs, etc...
    pub client_id: Option<u64>,

    /// If this is true, then the client should try to authenticate upon connect.
    pub auth_required: Option<bool>,

    /// If this is true, then the client must perform the TLS/1.2 handshake.
    /// Note, this used to be `ssl_required` and has been updated along with the protocol from SSL to TLS.
    pub tls_required: Option<bool>,

    /// If this is true, the client must provide a valid certificate during the TLS handshake.
    pub tls_verify: Option<bool>,

    /// If this is true, the client can provide a valid certificate during the TLS handshake.
    pub tls_available: Option<bool>,

    /// A list of server urls that a client can connect to.
    pub connect_urls: Option<Box<[Box<str>]>>,

    /// List of server urls that a websocket client can connect to.
    pub ws_connect_urls: Option<Box<[Box<str>]>>,

    /// If the server supports Lame Duck Mode notifications, and the current server has
    /// transitioned to lame duck, `ldm` will be set to `true`.
    pub ldm: Option<bool>,

    /// The git hash at which the NATS server was built.
    pub git_commit: Option<Box<str>>,

    /// Whether the server supports JetStream.
    pub jetstream: Option<bool>,

    /// The IP of the server.
    pub ip: Option<Box<str>>,

    /// The IP of the client.
    pub client_ip: Option<Box<str>>,

    /// The nonce for use in CONNECT.
    pub nonce: Option<Box<str>>,

    /// The name of the cluster.
    pub cluster: Option<Box<str>>,

    /// The configured NATS domain of the server.
    pub domain: Option<Box<str>>,
}

/// CONNECT operation options
#[derive(Clone, Debug, Serialize)]
pub struct ConnectOptions {
    /// Turns on +OK protocol acknowledgments.
    pub verbose: bool,

    /// Turns on additional strict format checking, e.g. for properly formed
    /// subjects.
    pub pedantic: bool,

    /// Indicates whether the client requires an SSL connection.
    pub tls_required: bool,

    /// Client authorization token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<Box<str>>,

    /// Connection username.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<Box<str>>,

    /// Connection password.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass: Option<Box<str>>,

    /// Optional client name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Box<str>>,

    /// The implementation language of the client.
    pub lang: Box<str>,

    /// The version of the client.
    pub version: Box<str>,

    /// Sending 0 (or absent) indicates client supports original protocol.
    /// Sending 1 indicates that the client supports dynamic reconfiguration
    /// of cluster topology changes by asynchronously receiving INFO messages
    /// with known servers it can reconnect to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<i8>,

    /// If set to `true`, the server (version 1.2.0+) will not send originating
    /// messages from this connection to its own subscriptions. Clients should
    /// set this to `true` only for server supporting this feature, which is
    /// when proto in the INFO protocol is set to at least 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<bool>,

    /// In case the server has responded with a `nonce` on `INFO`, then a NATS client
    /// must use this field to reply with the signed `nonce`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sig: Option<Box<str>>,

    /// The JWT that identifies a user permissions and account.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt: Option<Box<str>>,

    /// Enable quick replies for cases where a request is sent to a topic with no responders.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_responders: Option<bool>,

    /// Whether the client supports headers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<bool>,

    /// The public NKey to authenticate the client. This will be used to verify
    /// the signature (`sig`) against the `nonce` provided in the `INFO` message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nkey: Option<Box<str>>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            pedantic: false,
            tls_required: false,
            auth_token: None,
            user: None,
            pass: None,
            name: None,
            lang: "rust".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: None,
            echo: None,
            sig: None,
            jwt: None,
            no_responders: None,
            headers: None,
            nkey: None,
        }
    }
}

#[derive(Default)]
pub struct ServerOpDecoder;

#[derive(Debug)]
pub enum ServerOp {
    Info(InfoOptions),
    Msg {
        subject: Bytes,
        sid: Bytes,
        reply: Bytes,
        payload: Bytes,
    },
    Hmsg {
        subject: Bytes,
        sid: Bytes,
        reply: Bytes,
        headers: Bytes,
        payload: Bytes,
    },
    Ping,
    Pong,
    Ok,
    Err, // TODO: Payload
}

impl tokio_util::codec::Decoder for ServerOpDecoder {
    type Item = ServerOp;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            src.reserve(5_usize.saturating_sub(src.len()));
            return Ok(None);
        }
        let Some(i) = CRLF.find(src) else {
            return Ok(None);
        };
        if src.starts_with(b"INFO") {
            let opts = serde_json::from_slice(&src[4..i])?;
            src.advance(i.saturating_add(2));
            return Ok(Some(ServerOp::Info(opts)));
        }
        if src.starts_with(b"MSG") {
            let line = str::from_utf8(&src[3..i])
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
            let mut line = line.split_whitespace();
            let (subject, sid, reply, size) = match (
                line.next(),
                line.next(),
                line.next(),
                line.next(),
                line.next(),
            ) {
                (Some(subject), Some(sid), Some(size), None, ..) => (subject, sid, None, size),
                (Some(subject), Some(sid), Some(reply), Some(size), None) => {
                    (subject, sid, Some(reply), size)
                }
                _ => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "malformed MSG operation: {}",
                            String::from_utf8_lossy(&src[3..i])
                        ),
                    ))
                }
            };
            let size = size.parse().map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("MSG operation payload size is not valid: {err}"),
                )
            })?;
            let n = 4_usize.saturating_add(i).saturating_add(size);
            if src.len() < n {
                src.reserve(n.saturating_sub(src.len()));
                return Ok(None);
            }
            let subject = Bytes::copy_from_slice(subject.as_bytes());
            let sid = Bytes::copy_from_slice(sid.as_bytes());
            let reply = reply
                .map(str::as_bytes)
                .map(Bytes::copy_from_slice)
                .unwrap_or_default();
            src.advance(i.saturating_add(2));
            let mut payload = src.split_to(size.saturating_add(2)).freeze();
            payload.truncate(size);
            return Ok(Some(ServerOp::Msg {
                subject,
                sid,
                reply,
                payload,
            }));
        }
        if src.starts_with(b"PING") {
            src.advance(i.saturating_add(2));
            return Ok(Some(ServerOp::Ping));
        }
        if src.starts_with(b"PONG") {
            src.advance(i.saturating_add(2));
            return Ok(Some(ServerOp::Pong));
        }
        if src.starts_with(b"+OK") {
            src.advance(i.saturating_add(2));
            return Ok(Some(ServerOp::Ok));
        }
        if src.starts_with(b"-ERR") {
            src.advance(i.saturating_add(2));
            return Ok(Some(ServerOp::Err));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("unknown operation `{src:?}`"),
        ))
    }
}

pub enum ClientOp {
    Connect(ConnectOptions),
    Pub {
        subject: Bytes,
        reply: Bytes,
        payload: Bytes,
    },
    Hpub {
        subject: Bytes,
        reply: Bytes,
        headers: Bytes,
        payload: Bytes,
    },
    Sub {
        subject: Bytes,
        group: Bytes,
        sid: Bytes,
    },
    Unsub {
        sid: Bytes,
        max: Bytes,
    },
    Ping,
    Pong,
}

#[derive(Default)]
pub struct ClientOpEncoder;

impl tokio_util::codec::Encoder<ClientOp> for ClientOpEncoder {
    type Error = std::io::Error;

    fn encode(&mut self, op: ClientOp, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match op {
            ClientOp::Connect(info) => {
                // CONNECT {"option_name":option_value,...}␍␊
                dst.extend_from_slice(b"CONNECT");
                serde_json::to_writer(dst.writer(), &info)?;
                dst.extend_from_slice(b"\r\n");
            }
            ClientOp::Pub {
                subject,
                reply,
                payload,
            } => {
                // PUB <subject> [reply-to] <#bytes>␍␊[payload]␍␊
                dst.reserve(
                    27_usize // 3 + 7 + 20
                        .saturating_add(subject.len())
                        .saturating_add(reply.len())
                        .saturating_add(payload.len()),
                );
                dst.extend_from_slice(b"PUB ");
                dst.extend_from_slice(&subject);
                dst.put_u8(b' ');
                if !reply.is_empty() {
                    dst.extend_from_slice(&reply);
                    dst.put_u8(b' ');
                }
                if !payload.is_empty() {
                    dst.extend_from_slice(payload.len().to_string().as_bytes());
                    dst.extend_from_slice(b"\r\n");
                    dst.extend_from_slice(&payload);
                    dst.extend_from_slice(b"\r\n");
                } else {
                    dst.extend_from_slice(b"0\r\n\r\n");
                }
            }
            ClientOp::Hpub {
                subject,
                reply,
                headers,
                payload,
            } => {
                // HPUB <subject> [reply-to] <#header bytes> <#total bytes>␍␊[headers]␍␊␍␊[payload]␍␊
                dst.reserve(
                    66_usize // 4 + 12 + 20 + 20
                        .saturating_add(subject.len())
                        .saturating_add(reply.len())
                        .saturating_add(headers.len())
                        .saturating_add(payload.len()),
                );
                dst.extend_from_slice(b"HPUB ");
                dst.extend_from_slice(&subject);
                dst.put_u8(b' ');
                if !reply.is_empty() {
                    dst.extend_from_slice(&reply);
                    dst.put_u8(b' ');
                }
                let header_len = headers.len().to_string();
                dst.extend_from_slice(header_len.as_bytes());
                dst.put_u8(b' ');

                if !payload.is_empty() {
                    let total = headers.len().checked_add(payload.len()).ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            "total message size does not fit in usize",
                        )
                    })?;
                    dst.extend_from_slice(total.to_string().as_bytes());
                    dst.extend_from_slice(b"\r\n");
                    dst.extend_from_slice(&headers);
                    dst.extend_from_slice(&payload);
                    dst.extend_from_slice(b"\r\n");
                } else {
                    dst.extend_from_slice(header_len.as_bytes());
                    dst.extend_from_slice(b"\r\n");
                    dst.extend_from_slice(&headers);
                    dst.extend_from_slice(b"\r\n");
                }
            }
            ClientOp::Sub {
                subject,
                group,
                sid,
            } => {
                // SUB <subject> [queue group] <sid>␍␊
                dst.reserve(
                    8_usize // 3 + 5
                        .saturating_add(subject.len())
                        .saturating_add(group.len())
                        .saturating_add(sid.len()),
                );
                dst.extend_from_slice(b"SUB ");
                dst.extend_from_slice(&subject);
                dst.put_u8(b' ');
                if !group.is_empty() {
                    dst.extend_from_slice(&group);
                    dst.put_u8(b' ');
                }
                dst.extend_from_slice(&sid);
                dst.extend_from_slice(b"\r\n");
            }
            ClientOp::Unsub { sid, max } => {
                // UNSUB <sid> [max_msgs]␍␊
                dst.reserve(
                    9_usize // 5 + 4
                        .saturating_add(sid.len())
                        .saturating_add(max.len()),
                );
                dst.extend_from_slice(b"UNSUB ");
                dst.extend_from_slice(&sid);
                if !max.is_empty() {
                    dst.put_u8(b' ');
                    dst.extend_from_slice(&max);
                }
                dst.extend_from_slice(b"\r\n");
            }
            ClientOp::Ping => {
                // PING␍␊
                dst.extend_from_slice(b"PING\r\n");
            }
            ClientOp::Pong => {
                // PONG␍␊
                dst.extend_from_slice(b"PONG\r\n");
            }
        }
        Ok(())
    }
}

pub async fn connect_tcp(
    addr: impl ToSocketAddrs,
) -> std::io::Result<(
    FramedWrite<OwnedWriteHalf, ClientOpEncoder>,
    FramedRead<OwnedReadHalf, ServerOpDecoder>,
)> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let (rx, tx) = stream.into_split();
    Ok((
        FramedWrite::new(tx, ClientOpEncoder),
        FramedRead::new(rx, ServerOpDecoder::default()),
    ))
}
