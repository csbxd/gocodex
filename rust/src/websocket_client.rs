use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use codex_http_client::{HttpClientFactory, OutboundProxyPolicy};
use codex_websocket_client::{WebSocketConnection, WebSocketConnector};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use tokio_tungstenite::tungstenite::{
    Error as WsError, Message,
    client::IntoClientRequest,
    handshake::client::Request,
    protocol::{CloseFrame, WebSocketConfig, frame::coding::CloseCode},
};
use tokio_util::sync::CancellationToken;

use crate::{BridgeError, Output, runtime::run};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    proxy_policy: String,
    handshake_timeout_ms: u64,
    loopback_direct: bool,
    tcp_nodelay: bool,
    max_message_size: Option<usize>,
    max_frame_size: Option<usize>,
    chatgpt_cookies: Option<Vec<String>>,
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
        Self::new("websocket", error.to_string())
    }
}

fn cancelled() -> BridgeError {
    BridgeError::new("closed", "connection is closed")
}

pub(crate) fn create(data: &[u8]) -> Result<Output, BridgeError> {
    let config: Config =
        serde_json::from_slice(data).map_err(|e| BridgeError::invalid(e.to_string()))?;
    let policy = match config.proxy_policy.as_str() {
        "" | "respect_system_proxy" => OutboundProxyPolicy::RespectSystemProxy,
        "reqwest_default" => OutboundProxyPolicy::ReqwestDefault,
        _ => return Err(BridgeError::invalid("invalid proxy policy")),
    };
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
                let dial = async {
                    if options.loopback_direct {
                        conn.client.connector.connect_loopback_direct(request, config).await
                    } else {
                        conn.client.connector.connect(request, config).await
                    }
                };
                let (socket, response) = if options.handshake_timeout_ms > 0 {
                    tokio::time::timeout(Duration::from_millis(options.handshake_timeout_ms), dial).await
                        .map_err(|_| BridgeError::new("timeout", "WebSocket handshake timed out"))??
                } else { dial.await? };
                let metadata = Handshake {
                    status_code: response.status().as_u16(),
                    headers: response.headers().iter()
                        .map(|(name, value)| (name.to_string(), STANDARD.encode(value.as_bytes()))).collect(),
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
                        if let Some(writer) = conn.writer.lock().await.as_mut() {
                            match writer.flush().await {
                                Ok(()) | Err(WsError::ConnectionClosed) => {},
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
    let message = match kind {
        1 => Message::Text(
            std::str::from_utf8(data)
                .map_err(|e| BridgeError::invalid(e.to_string()))?
                .into(),
        ),
        2 => Message::Binary(data.to_vec().into()),
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
                let mut writer = conn.writer.lock().await;
                writer.as_mut().ok_or_else(|| BridgeError::invalid("connection not established"))?
                    .send(message).await?;
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
