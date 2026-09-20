use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use crate::websocket_sdk::{WebSocketConnection, WebSocketConnector};
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use codex_http_client::{HttpClientFactory, OutboundProxyPolicy, OutboundProxyRoute};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, RwLock};
use tokio_tungstenite::tungstenite::{
    Error as WsError, Message,
    client::IntoClientRequest,
    handshake::client::Request,
    protocol::{
        CloseFrame, WebSocketConfig,
        frame::{
            Frame,
            coding::{CloseCode, Data, OpCode},
        },
    },
};
use tokio_util::sync::CancellationToken;

use crate::{BridgeError, Output, runtime::run};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    proxy_policy: String,
    proxy_url: String,
    no_proxy: bool,
    handshake_timeout_ms: u64,
    loopback_direct: bool,
    tcp_nodelay: bool,
    max_message_size: Option<usize>,
    max_frame_size: Option<usize>,
    chatgpt_cookies: Option<Vec<String>>,
    write_frame_size: usize,
    max_handshake_body_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DialRequest {
    url: String,
    headers: Vec<(String, String)>,
}

#[derive(Serialize)]
struct Handshake {
    status_code: u16,
    headers: Vec<(String, String)>,
    body: String,
    body_truncated: bool,
}

struct Client {
    connector: WebSocketConnector,
    config: Config,
    cancel: CancellationToken,
}

struct Connection {
    client_id: u64,
    client: Arc<Client>,
    cancel: CancellationToken,
    pending: AsyncMutex<Option<Request>>,
    reader: AsyncMutex<Option<SplitStream<WebSocketConnection>>>,
    writer: AsyncMutex<Option<SplitSink<WebSocketConnection, Message>>>,
    // Keep data-message fragments contiguous, while allowing control frames
    // between fragments. Tokio's write-preferring RwLock prioritizes controls.
    data_write: AsyncMutex<()>,
    control_gate: RwLock<()>,
    peer_close_received: AtomicBool,
}

#[derive(Default)]
struct Registry {
    next_id: u64,
    clients: HashMap<u64, Arc<Client>>,
    connections: HashMap<u64, Arc<Connection>>,
}

impl Registry {
    fn next(&mut self) -> Result<u64, BridgeError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or_else(|| BridgeError::new("internal", "handle space exhausted"))?;
        Ok(self.next_id)
    }
}

fn registry() -> Result<MutexGuard<'static, Registry>, BridgeError> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| Mutex::new(Registry::default()))
        .lock()
        .map_err(|_| BridgeError::new("internal", "handle registry lock poisoned"))
}

impl From<WsError> for BridgeError {
    fn from(error: WsError) -> Self {
        if matches!(
            error,
            WsError::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::HandshakeIncomplete
            )
        ) {
            return Self::new("unexpected_eof", error.to_string());
        }
        if matches!(
            error,
            WsError::Protocol(
                tokio_tungstenite::tungstenite::error::ProtocolError::SendAfterClosing
            )
        ) {
            return Self::new("closing", "WebSocket closing handshake has started");
        }
        Self::new("websocket", error.to_string())
    }
}

fn write_error(conn: &Connection, error: WsError) -> BridgeError {
    if conn.peer_close_received.load(Ordering::Acquire) {
        // Do not let the Go writer cancel a reader which already has the peer's
        // terminal frame. That frame remains authoritative if acknowledgement fails.
        return BridgeError::new("closing", "WebSocket peer close has been received");
    }
    if let WsError::Io(ref io) = error
        && matches!(
            io.kind(),
            std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
        )
    {
        // The read direction may still contain the peer's terminal frame.
        // Let a concurrent reader finish before consumers classify this write.
        return BridgeError::new("closing", error.to_string());
    }
    error.into()
}

fn cancelled() -> BridgeError {
    BridgeError::new("closed", "connection is closed")
}

// Tungstenite retains the bytes read beyond the failed upgrade's headers. It
// does not retain the socket on failure, so never claim that an incomplete tail
// is the entire HTTP entity. Decode captured chunk framing before exposing it.
fn handshake_body(response: &http::Response<Option<Vec<u8>>>, limit: usize) -> (Vec<u8>, bool) {
    if response.status().is_informational() || matches!(response.status().as_u16(), 204 | 304) {
        return (Vec::new(), false);
    }
    let body = response.body().as_deref().unwrap_or_default();
    if response
        .headers()
        .get(http::header::TRANSFER_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("chunked"))
        })
    {
        let mut remaining = body;
        let mut decoded = Vec::new();
        loop {
            let Some(line_end) = remaining.windows(2).position(|w| w == b"\r\n") else {
                return (decoded, true);
            };
            let size = std::str::from_utf8(&remaining[..line_end])
                .ok()
                .and_then(|line| usize::from_str_radix(line.split(';').next()?.trim(), 16).ok());
            let Some(size) = size else {
                return (decoded, true);
            };
            remaining = &remaining[line_end + 2..];
            if size == 0 {
                let complete = remaining.starts_with(b"\r\n")
                    || remaining.windows(4).any(|w| w == b"\r\n\r\n");
                return (decoded, !complete);
            }
            let take = size.min(remaining.len()).min(limit - decoded.len());
            decoded.extend_from_slice(&remaining[..take]);
            if take < size
                || remaining.get(size..size.saturating_add(2)) != Some(b"\r\n".as_slice())
            {
                return (decoded, true);
            }
            remaining = &remaining[size + 2..];
        }
    }
    let expected = response
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    let length = expected.unwrap_or(body.len());
    let take = body.len().min(length).min(limit);
    (body[..take].to_vec(), expected.is_none() || take < length)
}

pub(crate) fn create(data: &[u8]) -> Result<Output, BridgeError> {
    let mut config: Config =
        serde_json::from_slice(data).map_err(|e| BridgeError::invalid(e.to_string()))?;
    let policy = match config.proxy_policy.as_str() {
        "" | "respect_system_proxy" => OutboundProxyPolicy::RespectSystemProxy,
        "reqwest_default" => OutboundProxyPolicy::ReqwestDefault,
        _ => return Err(BridgeError::invalid("invalid proxy policy")),
    };
    if !config.proxy_url.is_empty() && (config.no_proxy || config.loopback_direct) {
        return Err(BridgeError::invalid(
            "ProxyURL is mutually exclusive with NoProxy and LoopbackDirect",
        ));
    }
    if !config.proxy_url.is_empty() {
        crate::http_client::parse_proxy(&config.proxy_url)?;
    }
    if config.write_frame_size == 0 {
        config.write_frame_size = 32 * 1024;
    }
    if config.max_handshake_body_bytes == 0 {
        config.max_handshake_body_bytes = 64 * 1024;
    }
    if config.write_frame_size > 1024 * 1024 || config.max_handshake_body_bytes > 1024 * 1024 {
        return Err(BridgeError::invalid(
            "frame and handshake body limits must not exceed 1 MiB",
        ));
    }
    let cookies = config
        .chatgpt_cookies
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|value| HeaderValue::from_str(value).map_err(|e| BridgeError::invalid(e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let factory = HttpClientFactory::new(policy).with_chatgpt_cookies(cookies);
    let mut connector =
        WebSocketConnector::new(&factory).map_err(|e| BridgeError::new("tls", e.to_string()))?;
    if config.tcp_nodelay {
        connector = connector.with_tcp_nodelay();
    }
    let client = Arc::new(Client {
        connector,
        config,
        cancel: CancellationToken::new(),
    });
    let mut registry = registry()?;
    let id = registry.next()?;
    registry.clients.insert(id, client);
    Ok(Output::handle(id))
}

pub(crate) fn close(id: u64) -> Result<Output, BridgeError> {
    let (client, connections) = {
        let mut registry = registry()?;
        let client = registry.clients.remove(&id);
        let ids: Vec<_> = registry
            .connections
            .iter()
            .filter_map(|(key, conn)| (conn.client_id == id).then_some(*key))
            .collect();
        let connections: Vec<_> = ids
            .into_iter()
            .filter_map(|id| registry.connections.remove(&id))
            .collect();
        (client, connections)
    };
    if let Some(client) = client {
        client.cancel.cancel();
    }
    drop(connections);
    Ok(Output::handle(0))
}

pub(crate) fn create_connection(client_id: u64, data: &[u8]) -> Result<Output, BridgeError> {
    let client = registry()?
        .clients
        .get(&client_id)
        .cloned()
        .ok_or_else(|| BridgeError::new("closed", "client is closed"))?;
    let dial: DialRequest =
        serde_json::from_slice(data).map_err(|e| BridgeError::invalid(e.to_string()))?;
    let url = url::Url::parse(&dial.url).map_err(|e| BridgeError::invalid(e.to_string()))?;
    if !matches!(url.scheme(), "ws" | "wss") {
        return Err(BridgeError::invalid("URL must use ws or wss"));
    }
    // Normalize an empty path to '/' before Tungstenite writes the request line.
    let mut request = url.as_str().into_client_request()?;
    let mut headers = http::HeaderMap::new();
    for (name, value) in dial.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| BridgeError::invalid(e.to_string()))?;
        let value = STANDARD
            .decode(value)
            .map_err(|e| BridgeError::invalid(e.to_string()))?;
        let value =
            HeaderValue::from_bytes(&value).map_err(|e| BridgeError::invalid(e.to_string()))?;
        headers.append(name, value);
    }
    // Extend replaces generated defaults while retaining repeated custom values.
    request.headers_mut().extend(headers);
    let connection = Arc::new(Connection {
        client_id,
        cancel: client.cancel.child_token(),
        client,
        pending: AsyncMutex::new(Some(request)),
        reader: AsyncMutex::new(None),
        writer: AsyncMutex::new(None),
        data_write: AsyncMutex::new(()),
        control_gate: RwLock::new(()),
        peer_close_received: AtomicBool::new(false),
    });
    let mut registry = registry()?;
    if !registry.clients.contains_key(&client_id) {
        return Err(BridgeError::new("closed", "client is closed"));
    }
    let id = registry.next()?;
    registry.connections.insert(id, connection);
    Ok(Output::handle(id))
}

fn lookup(id: u64) -> Result<Arc<Connection>, BridgeError> {
    registry()?
        .connections
        .get(&id)
        .cloned()
        .ok_or_else(cancelled)
}

pub(crate) fn connect(id: u64) -> Result<Output, BridgeError> {
    let conn = lookup(id)?;
    run(async move {
        tokio::select! {
            biased;
            _ = conn.cancel.cancelled() => Err(cancelled()),
            result = async {
                let request = conn.pending.lock().await.take()
                    .ok_or_else(|| BridgeError::invalid("connection already started"))?;
                let options = &conn.client.config;
                let mut config = WebSocketConfig::default();
                if let Some(size) = options.max_message_size { config.max_message_size = Some(size); }
                if let Some(size) = options.max_frame_size { config.max_frame_size = Some(size); }
                config.write_buffer_size = 0;
                let dial = async {
                    if options.loopback_direct {
                        conn.client.connector.connect_loopback_direct(request, config).await
                    } else if options.no_proxy || !options.proxy_url.is_empty() {
                        let route = if options.no_proxy { OutboundProxyRoute::Direct }
                            else { OutboundProxyRoute::Proxy { url: options.proxy_url.clone(), no_proxy: None } };
                        conn.client.connector.connect_with_route(request, config, route, false).await
                    } else {
                        conn.client.connector.connect(request, config).await
                    }
                };
                let result = if options.handshake_timeout_ms > 0 {
                    tokio::time::timeout(Duration::from_millis(options.handshake_timeout_ms), dial).await
                        .map_err(|_| BridgeError::new("timeout", "WebSocket handshake timed out"))?
                } else { dial.await };
                let (socket, response) = match result {
                    Ok(value) => value,
                    Err(WsError::Http(response)) => {
                        let (body, truncated) = handshake_body(&response, options.max_handshake_body_bytes);
                        let metadata = Handshake { status_code: response.status().as_u16(),
                            headers: response.headers().iter().map(|(name,value)| (name.to_string(), STANDARD.encode(value.as_bytes()))).collect(),
                            body: STANDARD.encode(body), body_truncated: truncated };
                        return Err(BridgeError::new("handshake", format!("WebSocket upgrade rejected with HTTP {}", metadata.status_code))
                            .with_details(serde_json::to_value(metadata).map_err(|e| BridgeError::new("internal", e.to_string()))?));
                    }
                    Err(error) => return Err(error.into()),
                };
                let metadata = Handshake {
                    status_code: response.status().as_u16(),
                    headers: response.headers().iter()
                        .map(|(name, value)| (name.to_string(), STANDARD.encode(value.as_bytes()))).collect(),
                    body: String::new(),
                    body_truncated: false,
                };
                let data = serde_json::to_vec(&metadata).map_err(|e| BridgeError::new("internal", e.to_string()))?;
                let (writer, reader) = socket.split();
                *conn.writer.lock().await = Some(writer);
                *conn.reader.lock().await = Some(reader);
                Ok(Output::data(data))
            } => result,
        }
    })
}

pub(crate) fn read(id: u64) -> Result<Output, BridgeError> {
    let conn = lookup(id)?;
    run(async move {
        tokio::select! {
            biased;
            _ = conn.cancel.cancelled() => Err(cancelled()),
            result = async {
                let mut reader = conn.reader.lock().await;
                let reader = reader.as_mut().ok_or_else(|| BridgeError::invalid("connection not established"))?;
                if conn.peer_close_received.load(Ordering::Acquire) {
                    return Ok(Output::eof());
                }
                loop {
                    let message = match reader.next().await {
                        None | Some(Err(WsError::ConnectionClosed)) => return Ok(Output::eof()),
                        Some(result) => result?,
                    };
                    let (kind, payload) = match message {
                        Message::Text(text) => (1, text.as_bytes().to_vec()),
                        Message::Binary(data) => (2, data.to_vec()),
                        Message::Ping(data) => (9, data.to_vec()),
                        Message::Pong(data) => (10, data.to_vec()),
                        Message::Close(frame) => {
                            conn.peer_close_received.store(true, Ordering::Release);
                            let mut data = Vec::new();
                            if let Some(frame) = frame {
                                data.extend_from_slice(&u16::from(frame.code).to_be_bytes());
                                data.extend_from_slice(frame.reason.as_bytes());
                            }
                            (8, data)
                        }
                        Message::Frame(_) => continue,
                    };
                    if kind == 9 || kind == 8 {
                        // Send Tungstenite's queued automatic pong / close response.
                        let _priority = conn.control_gate.write().await;
                        if let Some(writer) = conn.writer.lock().await.as_mut() {
                            match writer.flush().await {
                                Ok(()) | Err(WsError::ConnectionClosed) => {},
                                // A failed acknowledgement cannot replace a received
                                // Close (e.g. 1009) with a retryable socket failure.
                                Err(_) if kind == 8 => {},
                                Err(error) => return Err(error.into()),
                            }
                        }
                    }
                    let mut data = Vec::with_capacity(payload.len() + 1);
                    data.push(kind);
                    data.extend_from_slice(&payload);
                    return Ok(Output::data(data));
                }
            } => result,
        }
    })
}

pub(crate) fn write(id: u64, kind: u8, data: &[u8]) -> Result<Output, BridgeError> {
    let conn = lookup(id)?;
    if matches!(kind, 8..=10) && data.len() > 125 {
        return Err(BridgeError::invalid(
            "control frame payload exceeds 125 bytes",
        ));
    }
    if kind == 1 {
        std::str::from_utf8(data).map_err(|e| BridgeError::invalid(e.to_string()))?;
    }
    if kind == 1 || kind == 2 {
        let data = Bytes::copy_from_slice(data);
        return run(async move {
            tokio::select! {
                biased;
                _ = conn.cancel.cancelled() => Err(cancelled()),
                result = async {
                    let _message = conn.data_write.lock().await;
                    let size = conn.client.config.write_frame_size;
                    let mut offset = 0;
                    loop {
                        let end = (offset + size).min(data.len());
                        let opcode = if offset > 0 { Data::Continue } else if kind == 1 { Data::Text } else { Data::Binary };
                        let frame = Frame::message(data.slice(offset..end), OpCode::Data(opcode), end == data.len());
                        {
                            let _priority = conn.control_gate.read().await;
                            let mut writer = conn.writer.lock().await;
                            writer.as_mut().ok_or_else(|| BridgeError::invalid("connection not established"))?
                                .send(Message::Frame(frame)).await.map_err(|error| write_error(&conn, error))?;
                        }
                        if end == data.len() { break; }
                        offset = end;
                        tokio::task::yield_now().await;
                    }
                    Ok(Output::handle(0))
                } => result,
            }
        });
    }
    let message = match kind {
        9 => Message::Ping(data.to_vec().into()),
        10 => Message::Pong(data.to_vec().into()),
        8 => {
            let frame = if data.is_empty() {
                None
            } else {
                if data.len() < 2 {
                    return Err(BridgeError::invalid("invalid close frame"));
                }
                let code = CloseCode::from(u16::from_be_bytes([data[0], data[1]]));
                if !code.is_allowed() {
                    return Err(BridgeError::invalid("invalid close status"));
                }
                let reason = std::str::from_utf8(&data[2..])
                    .map_err(|e| BridgeError::invalid(e.to_string()))?;
                Some(CloseFrame {
                    code,
                    reason: reason.into(),
                })
            };
            Message::Close(frame)
        }
        _ => return Err(BridgeError::invalid("invalid message type")),
    };
    run(async move {
        tokio::select! {
            biased;
            _ = conn.cancel.cancelled() => Err(cancelled()),
            result = async {
                let _priority = conn.control_gate.write().await;
                let mut writer = conn.writer.lock().await;
                writer.as_mut().ok_or_else(|| BridgeError::invalid("connection not established"))?
                    .send(message).await.map_err(|error| write_error(&conn, error))?;
                Ok(Output::handle(0))
            } => result,
        }
    })
}

pub(crate) fn close_connection(id: u64) -> Result<Output, BridgeError> {
    let conn = registry()?.connections.remove(&id);
    if let Some(conn) = conn {
        conn.cancel.cancel();
    }
    Ok(Output::handle(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_bodies_preserve_binary_data_and_mark_incomplete_entities() {
        let mut response = http::Response::builder()
            .status(429)
            .header("Content-Length", "4")
            .body(Some(vec![0, 255, b'x', b'y']))
            .unwrap();
        assert_eq!(
            handshake_body(&response, 4),
            (vec![0, 255, b'x', b'y'], false)
        );
        assert_eq!(handshake_body(&response, 2), (vec![0, 255], true));
        *response.body_mut() = Some(vec![0]);
        assert_eq!(handshake_body(&response, 4), (vec![0], true));
        response.headers_mut().remove(http::header::CONTENT_LENGTH);
        assert_eq!(handshake_body(&response, 4), (vec![0], true));
    }

    #[test]
    fn handshake_chunk_decoding_is_bounded_and_detects_truncation() {
        for (wire, limit, expected, truncated) in [
            ("2\r\nab\r\n2;x=y\r\ncd\r\n0\r\n\r\n", 8, "abcd", false),
            ("4\r\nabcd\r\n0\r\n\r\n", 4, "abcd", false),
            ("4\r\nabcd\r\n0\r\n\r\n", 2, "ab", true),
            ("4\r\nab", 8, "ab", true),
            ("4\r\nabcd\r\n", 8, "abcd", true),
            ("0\r\nHeader: value\r\n\r\n", 8, "", false),
            ("bogus\r\n", 8, "", true),
        ] {
            let response = http::Response::builder()
                .status(403)
                .header("Transfer-Encoding", "chunked")
                .body(Some(wire.as_bytes().to_vec()))
                .unwrap();
            assert_eq!(
                handshake_body(&response, limit),
                (expected.as_bytes().to_vec(), truncated)
            );
        }
    }
}
