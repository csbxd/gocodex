use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio_util::sync::CancellationToken;

use crate::{BridgeError, Output, route_aware_redirect as redirects, runtime::run};
use codex_http_client::{
    ClientRouteClass, HttpClient, HttpClientFactory, HttpResponse, OutboundProxyPolicy,
    RouteAwareClientPool, RouteAwareRequestBuilder, build_reqwest_client_with_custom_ca,
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    timeout_ms: u64,
    proxy_policy: String,
    proxy_url: String,
    no_proxy: bool,
    disable_redirects: bool,
    user_agent: String,
    chatgpt_cookies: Option<Vec<String>>,
    tls_backend_fallback: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    method: String,
    url: String,
    // Base64 preserves arbitrary non-UTF-8 header values.
    headers: Vec<(String, String)>,
    has_body: bool,
    has_json: bool,
    json: Option<Box<RawValue>>,
}

#[derive(Serialize)]
struct Response {
    status_code: u16,
    url: String,
    protocol: String,
    headers: Vec<(String, String)>,
    content_length: Option<u64>,
}

struct Client {
    inner: ClientBackend,
    timeout_ms: u64,
    user_agent: Option<HeaderValue>,
    cancel: CancellationToken,
}

enum ClientBackend {
    Automatic(Box<RouteAwareClientPool>),
    Fixed(FixedClient),
}

struct FixedClient {
    raw: reqwest::Client,
    sdk: HttpClient,
    follow_redirects: bool,
}

enum PreparedRequest {
    Automatic(Box<RouteAwareRequestBuilder>),
    Fixed {
        client: HttpClient,
        request: Box<reqwest::Request>,
        follow_redirects: bool,
    },
}

impl PreparedRequest {
    async fn send(self) -> Result<HttpResponse, BridgeError> {
        match self {
            Self::Automatic(builder) => builder.send().await.map_err(Into::into),
            Self::Fixed {
                client,
                request,
                follow_redirects,
            } => send_fixed(client, *request, follow_redirects).await,
        }
    }
}

async fn send_fixed(
    client: HttpClient,
    mut request: reqwest::Request,
    follow: bool,
) -> Result<HttpResponse, BridgeError> {
    let deadline = request
        .timeout()
        .map(|timeout| tokio::time::Instant::now() + *timeout);
    let send = async move {
        let mut hops = 0;
        loop {
            let remaining =
                deadline.map(|end| end.saturating_duration_since(tokio::time::Instant::now()));
            let mut builder = client
                .request(request.method().clone(), request.url().clone())
                .headers(request.headers().clone());
            if let Some(remaining) = remaining {
                builder = builder.timeout(remaining);
            }
            if let Some(body) = request.body() {
                let bytes = body
                    .as_bytes()
                    .ok_or_else(|| BridgeError::new("internal", "unbuffered bridge request"))?;
                builder = builder.body(bytes.to_vec());
            }
            let response = builder.send().await?;
            if !follow || !redirects::is_redirect(response.status()) {
                return Ok(response);
            }
            let Some(next_url) = redirects::redirect_url(&response) else {
                return Ok(response);
            };
            if !matches!(next_url.scheme(), "http" | "https") {
                return Err(BridgeError::new(
                    "request",
                    "redirect target must use http or https",
                ));
            }
            if hops >= redirects::MAX_REDIRECTS {
                return Err(BridgeError::new("request", "too many redirects"));
            }
            let Some(mut next) = redirects::redirect_request(
                response.status(),
                request.method().clone(),
                request.headers().clone(),
                request.version(),
                remaining,
                request.try_clone(),
                next_url,
            ) else {
                return Ok(response);
            };
            let next_url = next.url().clone();
            redirects::remove_sensitive_headers(next.headers_mut(), request.url(), &next_url);
            redirects::insert_referer(next.headers_mut(), request.url(), &next_url);
            request = next;
            hops += 1;
            // A fresh SDK send recomputes proxy authentication for every hop.
            // Native reqwest redirects can strip that header across origins.
        }
    };
    if let Some(end) = deadline {
        tokio::time::timeout_at(end, send)
            .await
            .map_err(|_| BridgeError::new("timeout", "request timed out"))?
    } else {
        send.await
    }
}

// Delegate cookie scope and filtering to the SDK even when a fixed proxy is used.
struct SdkCookieStore(HttpClientFactory);

impl reqwest::cookie::CookieStore for SdkCookieStore {
    fn set_cookies(&self, cookies: &mut dyn Iterator<Item = &HeaderValue>, url: &url::Url) {
        if let Ok(uri) = url.as_str().parse::<http::Uri>() {
            let mut headers = HeaderMap::new();
            for cookie in cookies {
                headers.append(http::header::SET_COOKIE, cookie.clone());
            }
            self.0.store_chatgpt_response_cookies(&uri, &headers);
        }
    }

    fn cookies(&self, url: &url::Url) -> Option<HeaderValue> {
        self.0.chatgpt_cookie_header(&url.as_str().parse().ok()?)
    }
}

type BodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, codex_http_client::HttpError>> + Send>>;

enum Phase {
    Ready(PreparedRequest),
    Streaming { stream: BodyStream, pending: Bytes },
    Done,
}

struct Call {
    client_id: u64,
    cancel: CancellationToken,
    phase: tokio::sync::Mutex<Phase>,
}

#[derive(Default)]
struct Registry {
    next_id: u64,
    clients: HashMap<u64, Arc<Client>>,
    calls: HashMap<u64, Arc<Call>>,
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

pub(crate) fn create(config: &[u8]) -> Result<Output, BridgeError> {
    let config: Config =
        serde_json::from_slice(config).map_err(|error| BridgeError::invalid(error.to_string()))?;
    let policy = match config.proxy_policy.as_str() {
        "" | "respect_system_proxy" => OutboundProxyPolicy::RespectSystemProxy,
        "reqwest_default" => OutboundProxyPolicy::ReqwestDefault,
        _ => return Err(BridgeError::invalid("invalid proxy policy")),
    };
    if config.no_proxy && !config.proxy_url.is_empty() {
        return Err(BridgeError::invalid(
            "proxy_url and no_proxy are mutually exclusive",
        ));
    }
    let explicit_route = config.no_proxy || !config.proxy_url.is_empty();
    if explicit_route && config.tls_backend_fallback {
        return Err(BridgeError::invalid(
            "TLSBackendFallback requires automatic proxy routing",
        ));
    }
    let proxy = if config.proxy_url.is_empty() {
        None
    } else {
        Some(parse_proxy(&config.proxy_url)?)
    };
    let cookies = config
        .chatgpt_cookies
        .unwrap_or_default()
        .into_iter()
        .map(|value| HeaderValue::from_str(&value).map_err(|e| BridgeError::invalid(e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    let factory = HttpClientFactory::new(policy).with_chatgpt_cookies(cookies);
    let user_agent = if config.user_agent.is_empty() {
        None
    } else {
        Some(
            HeaderValue::from_str(&config.user_agent)
                .map_err(|e| BridgeError::invalid(e.to_string()))?,
        )
    };
    run(async move {
        let inner = if explicit_route {
            // The SDK's route-aware pool does not expose fixed proxy selection.
            // Configure the underlying builder, then retain SDK CA handling,
            // request tracing and HttpClient/RequestBuilder for all network I/O.
            let mut builder = reqwest::Client::builder().no_proxy();
            if let Some(proxy) = proxy {
                builder = builder.proxy(proxy);
            }
            builder = builder.redirect(reqwest::redirect::Policy::none());
            if factory.has_chatgpt_cookies() {
                builder = builder.cookie_provider(Arc::new(SdkCookieStore(factory)));
            }
            let inner = build_reqwest_client_with_custom_ca(builder)
                .map_err(|error| BridgeError::new("configuration", error.to_string()))?;
            ClientBackend::Fixed(FixedClient {
                sdk: HttpClient::new_without_request_logging(inner.clone()),
                raw: inner,
                follow_redirects: !config.disable_redirects,
            })
        } else {
            let mut pool = match (config.disable_redirects, factory.has_chatgpt_cookies()) {
                (true, true) => RouteAwareClientPool::with_chatgpt_cloudflare_cookies_without_redirects_or_request_logging(factory, ClientRouteClass::Api),
                (false, true) => RouteAwareClientPool::with_chatgpt_cloudflare_cookies_without_request_logging(factory, ClientRouteClass::Api),
                (true, false) => RouteAwareClientPool::new_without_redirects_or_request_logging(factory, ClientRouteClass::Api),
                (false, false) => RouteAwareClientPool::new_without_request_logging(factory, ClientRouteClass::Api),
            };
            if config.tls_backend_fallback {
                pool = pool.with_tls_backend_fallback();
            }
            ClientBackend::Automatic(Box::new(pool))
        };
        let client = Arc::new(Client {
            inner,
            timeout_ms: config.timeout_ms,
            user_agent,
            cancel: CancellationToken::new(),
        });
        let mut registry = registry()?;
        let id = registry.next()?;
        registry.clients.insert(id, client);
        Ok(Output::handle(id))
    })
}

pub(crate) fn parse_proxy(value: &str) -> Result<reqwest::Proxy, BridgeError> {
    // Never include the supplied URL in validation errors: it may carry a password.
    let url = url::Url::parse(value).map_err(|_| BridgeError::invalid("invalid ProxyURL"))?;
    if !matches!(url.scheme(), "http" | "https" | "socks5" | "socks5h") {
        return Err(BridgeError::invalid(
            "ProxyURL must use http, https, socks5 or socks5h",
        ));
    }
    if url.host_str().is_none()
        || url.port() == Some(0)
        || !matches!(url.path(), "" | "/")
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(BridgeError::invalid(
            "ProxyURL requires a host and valid port, without a path, query or fragment",
        ));
    }
    reqwest::Proxy::all(url)
        .map(|proxy| proxy.no_proxy(None))
        .map_err(|_| BridgeError::invalid("invalid ProxyURL"))
}

pub(crate) fn close(id: u64) -> Result<Output, BridgeError> {
    let (client, calls) = {
        let mut registry = registry()?;
        let client = registry.clients.remove(&id);
        let call_ids: Vec<_> = registry
            .calls
            .iter()
            .filter_map(|(call_id, call)| (call.client_id == id).then_some(*call_id))
            .collect();
        let calls: Vec<_> = call_ids
            .into_iter()
            .filter_map(|call_id| registry.calls.remove(&call_id))
            .collect();
        (client, calls)
    };
    if let Some(client) = client {
        client.cancel.cancel();
    }
    // Drop streams and connection pools outside the registry lock.
    drop(calls);
    Ok(Output::handle(0))
}

pub(crate) fn create_request(
    client_id: u64,
    metadata: &[u8],
    body: &[u8],
) -> Result<Output, BridgeError> {
    let client = registry()?
        .clients
        .get(&client_id)
        .cloned()
        .ok_or_else(|| BridgeError::new("closed", "client is closed"))?;
    let request: Request = serde_json::from_slice(metadata)
        .map_err(|error| BridgeError::invalid(error.to_string()))?;
    if request.has_json && request.has_body {
        return Err(BridgeError::invalid("body and json are mutually exclusive"));
    }
    let method = http::Method::from_bytes(request.method.as_bytes())
        .map_err(|error| BridgeError::invalid(error.to_string()))?;
    let url =
        url::Url::parse(&request.url).map_err(|error| BridgeError::invalid(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(BridgeError::invalid("URL must use http or https"));
    }
    let mut headers = HeaderMap::new();
    for (name, value) in request.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| BridgeError::invalid(error.to_string()))?;
        let value = STANDARD
            .decode(value)
            .map_err(|error| BridgeError::invalid(error.to_string()))?;
        let value = HeaderValue::from_bytes(&value)
            .map_err(|error| BridgeError::invalid(error.to_string()))?;
        headers.append(name, value);
    }
    if let Some(user_agent) = &client.user_agent {
        headers
            .entry(http::header::USER_AGENT)
            .or_insert(user_agent.clone());
    }
    macro_rules! prepare {
        ($builder:expr) => {{
            let mut builder = $builder.headers(headers);
            if client.timeout_ms > 0 {
                builder = builder.timeout(Duration::from_millis(client.timeout_ms));
            }
            if request.has_json {
                // RawValue preserves large JSON numbers. None serializes as JSON null.
                builder = builder.json(&request.json);
            } else if request.has_body {
                builder = builder.body(body.to_vec());
            }
            Box::new(builder)
        }};
    }
    let prepared = match &client.inner {
        ClientBackend::Automatic(pool) => {
            PreparedRequest::Automatic(prepare!(pool.request(method, url)))
        }
        ClientBackend::Fixed(fixed) => PreparedRequest::Fixed {
            client: fixed.sdk.clone(),
            request: Box::new(prepare!(fixed.raw.request(method, url)).build()?),
            follow_redirects: fixed.follow_redirects,
        },
    };
    let call = Arc::new(Call {
        client_id,
        cancel: client.cancel.child_token(),
        phase: tokio::sync::Mutex::new(Phase::Ready(prepared)),
    });
    let mut registry = registry()?;
    // Client.Close may have raced with request construction.
    if !registry.clients.contains_key(&client_id) {
        return Err(BridgeError::new("closed", "client is closed"));
    }
    let id = registry.next()?;
    registry.calls.insert(id, call);
    Ok(Output::handle(id))
}

fn lookup(id: u64) -> Result<Arc<Call>, BridgeError> {
    registry()?
        .calls
        .get(&id)
        .cloned()
        .ok_or_else(|| BridgeError::new("closed", "request is closed"))
}

fn cancelled() -> BridgeError {
    BridgeError::new("cancelled", "request cancelled")
}

pub(crate) fn send(id: u64) -> Result<Output, BridgeError> {
    let call = lookup(id)?;
    run(async move {
        tokio::select! {
            biased;
            _ = call.cancel.cancelled() => Err(cancelled()),
            result = async {
                let mut phase = call.phase.lock().await;
                if !matches!(*phase, Phase::Ready(_)) {
                    return Err(BridgeError::invalid("request has already been sent"));
                }
                let Phase::Ready(request) = std::mem::replace(&mut *phase, Phase::Done) else {
                    unreachable!();
                };
                let response = request.send().await?;
                let metadata = Response {
                    status_code: response.status().as_u16(),
                    url: response.url().to_string(),
                    protocol: format!("{:?}", response.version()),
                    headers: response.headers().iter().map(|(name, value)| {
                        (name.to_string(), STANDARD.encode(value.as_bytes()))
                    }).collect(),
                    content_length: response.content_length(),
                };
                let data = serde_json::to_vec(&metadata)
                    .map_err(|error| BridgeError::new("internal", error.to_string()))?;
                *phase = Phase::Streaming {
                    stream: Box::pin(response.bytes_stream()),
                    pending: Bytes::new(),
                };
                Ok(Output::data(data))
            } => result,
        }
    })
}

pub(crate) fn read(id: u64, max_len: usize) -> Result<Output, BridgeError> {
    if max_len == 0 {
        return Err(BridgeError::invalid("read buffer must not be empty"));
    }
    let max_len = max_len.min(64 * 1024);
    let call = lookup(id)?;
    run(async move {
        tokio::select! {
            biased;
            _ = call.cancel.cancelled() => Err(cancelled()),
            result = async {
                let mut phase = call.phase.lock().await;
                loop {
                    match &mut *phase {
                        Phase::Ready(_) => return Err(BridgeError::invalid("request has not been sent")),
                        Phase::Done => return Ok(Output::eof()),
                        Phase::Streaming { stream, pending } => {
                            if !pending.is_empty() {
                                let len = max_len.min(pending.len());
                                return Ok(Output::data(pending.split_to(len).to_vec()));
                            }
                            match stream.next().await {
                                Some(Ok(chunk)) => *pending = chunk,
                                Some(Err(error)) => {
                                    *phase = Phase::Done;
                                    return Err(error.into());
                                }
                                None => {
                                    *phase = Phase::Done;
                                    return Ok(Output::eof());
                                }
                            }
                        }
                    }
                }
            } => result,
        }
    })
}

pub(crate) fn close_request(id: u64) -> Result<Output, BridgeError> {
    let call = registry()?.calls.remove(&id);
    if let Some(call) = call {
        call.cancel.cancel();
    }
    Ok(Output::handle(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    const METADATA: &[u8] = br#"{"method":"POST","url":"http://127.0.0.1/","headers":[],"has_body":true,"has_json":false,"json":null}"#;

    #[test]
    fn close_cancels_live_calls_and_removes_all_client_handles() {
        let client = create(br#"{}"#).ok().unwrap().handle;
        let one = create_request(client, METADATA, b"binary\0body")
            .ok()
            .unwrap()
            .handle;
        let two = create_request(client, METADATA, b"").ok().unwrap().handle;
        let live = lookup(one).ok().unwrap();
        assert!(!live.cancel.is_cancelled());
        assert!(close(client).is_ok());
        assert!(live.cancel.is_cancelled());
        assert!(lookup(one).is_err());
        assert!(lookup(two).is_err());
        assert!(!registry().ok().unwrap().clients.contains_key(&client));
        assert!(create_request(client, METADATA, b"").is_err());
        assert!(close_request(one).is_ok());
        assert!(close(client).is_ok());
    }

    #[test]
    fn request_close_is_idempotent_and_does_not_close_client() {
        let client = create(br#"{}"#).ok().unwrap().handle;
        let request = create_request(client, METADATA, b"").ok().unwrap().handle;
        let live = lookup(request).ok().unwrap();
        assert!(read(request, 1).is_err()); // Cannot read before send.
        assert!(close_request(request).is_ok());
        assert!(close_request(request).is_ok());
        assert!(live.cancel.is_cancelled());
        assert!(registry().ok().unwrap().clients.contains_key(&client));
        let next = create_request(client, METADATA, b"").ok().unwrap().handle;
        assert_ne!(next, request); // A stale integer handle is never reused.
        assert!(close(client).is_ok());
    }
}
