//! Synchronous C ABI over the Codex HTTP and WebSocket SDKs.
//!
//! Only integer handles and owned byte buffers cross the ABI. Registry lookups
//! clone an Arc before unlocking, allowing Close to race with in-flight calls.
//! No Go pointers are retained.

mod http_client;
mod runtime;
mod websocket_client;
#[allow(dead_code, clippy::result_large_err)] // Preserve the complete SDK and its upstream tests.
mod websocket_sdk {
    include!(concat!(env!("OUT_DIR"), "/websocket_sdk.rs"));
}
// The SDK's dialer and tests refer to these types through their crate root.
use websocket_sdk::{AsyncIo, ConnectionInner, TcpNodelay};
#[cfg(test)]
use websocket_sdk::{WebSocketConnection, WebSocketConnector, WebSocketTlsMode};
// Reuse the pinned SDK's redirect rules and its credential-stripping policy.
#[path = "../../codex/codex-rs/http-client/src/route_aware_redirect.rs"]
mod route_aware_redirect;

use serde::Serialize;
use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{ptr, slice};

#[derive(Serialize)]
pub(crate) struct BridgeError {
    kind: &'static str,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    details: Option<serde_json::Value>,
}

impl BridgeError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            details: None,
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_input", message)
    }

    pub(crate) fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    fn from_source(kind: &'static str, error: &(dyn std::error::Error + 'static)) -> Self {
        let mut message = error.to_string();
        let mut source = error.source();
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Self::new(kind, message)
    }
}

impl From<codex_http_client::HttpError> for BridgeError {
    fn from(error: codex_http_client::HttpError) -> Self {
        let kind = if error.is_timeout() {
            "timeout"
        } else if error.is_builder() {
            "invalid_input"
        } else if is_dns_error(&error) {
            "dns"
        } else if is_unexpected_eof(&error) {
            "unexpected_eof"
        } else if error.is_connect() && has_handshake_failure_source(&error) {
            "connect"
        } else {
            "request"
        };
        // Preserve the source chain (including TLS errors), without the URL.
        let error = error.without_url();
        Self::from_source(kind, &error)
    }
}

fn is_dns_error(error: &codex_http_client::HttpError) -> bool {
    if !error.is_connect() {
        return false;
    }
    // Hyper's DNS connector error type is private. Its source node has this
    // fixed phase label, independent of the resolver's localized cause. Match
    // the node only during connection establishment, never a response body or
    // a flattened message containing the same words.
    let mut source = std::error::Error::source(error);
    while let Some(cause) = source {
        if matches!(
            cause.to_string().as_str(),
            "dns error" | "error resolving for socks proxy"
        ) && cause.source().is_some()
        {
            return true;
        }
        source = cause.source();
    }
    false
}

fn has_handshake_failure_source(mut error: &(dyn std::error::Error + 'static)) -> bool {
    // OpenSSL's ssl.h/sslerr.h encode received TLS alerts as 1000 + alert byte.
    // Local certificate verification failure is a different SSL reason code.
    const SSL_LIBRARY: i32 = 20;
    const CERTIFICATE_VERIFY_FAILED: i32 = 134;
    const ALERT_REASON_OFFSET: i32 = 1000;
    let mut recognized = false;
    loop {
        if let Some(tls) = error.downcast_ref::<rustls::Error>() {
            match tls {
                rustls::Error::InvalidCertificate(_) => return false,
                rustls::Error::AlertReceived(_) => recognized = true,
                _ => {}
            }
        }
        if let Some(stack) = error.downcast_ref::<openssl::error::ErrorStack>() {
            for error in stack.errors() {
                if error.library_code() != SSL_LIBRARY {
                    continue;
                }
                if error.reason_code() == CERTIFICATE_VERIFY_FAILED {
                    return false;
                }
                if (ALERT_REASON_OFFSET..=ALERT_REASON_OFFSET + 255).contains(&error.reason_code())
                {
                    recognized = true;
                }
            }
        }
        // Reqwest's SOCKS error type is private; use its connection-phase node,
        // not the operating-system wording or text from an HTTP response body.
        if error.to_string() == "error connecting to socks proxy" && error.source().is_some() {
            recognized = true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return recognized,
        }
    }
}

// Preserve the HTTP framing failure independently of backend error wording.
// Clean response EOF is returned separately by the ABI and never reaches here.
fn is_unexpected_eof(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|error| error.kind() == std::io::ErrorKind::UnexpectedEof)
            || error
                .downcast_ref::<hyper::Error>()
                .is_some_and(hyper::Error::is_incomplete_message)
        {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

impl From<codex_http_client::RouteAwareRequestError> for BridgeError {
    fn from(error: codex_http_client::RouteAwareRequestError) -> Self {
        match error {
            // Use the same classification and URL redaction as fixed routes.
            codex_http_client::RouteAwareRequestError::Request(error) => error.into(),
            error => {
                let kind = if error.is_timeout() {
                    "timeout"
                } else {
                    "request"
                };
                Self::from_source(kind, &error)
            }
        }
    }
}

#[repr(C)]
pub struct Buffer {
    data: *mut u8,
    len: usize,
}

impl Buffer {
    fn new(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            return Self {
                data: ptr::null_mut(),
                len: 0,
            };
        }
        let bytes = Box::leak(bytes.into_boxed_slice());
        Self {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
        }
    }
}

#[repr(C)]
pub struct FfiResult {
    handle: u64,
    // 0 = success, 1 = error JSON, 2 = EOF.
    code: i32,
    buffer: Buffer,
}

pub(crate) struct Output {
    handle: u64,
    data: Vec<u8>,
    eof: bool,
}

impl Output {
    pub(crate) fn handle(handle: u64) -> Self {
        Self {
            handle,
            data: Vec::new(),
            eof: false,
        }
    }

    pub(crate) fn data(data: Vec<u8>) -> Self {
        Self {
            handle: 0,
            data,
            eof: false,
        }
    }

    pub(crate) fn eof() -> Self {
        Self {
            eof: true,
            ..Self::handle(0)
        }
    }
}

fn ffi(call: impl FnOnce() -> Result<Output, BridgeError>) -> FfiResult {
    let result = catch_unwind(AssertUnwindSafe(call))
        .unwrap_or_else(|_| Err(BridgeError::new("panic", "Rust bridge panicked")));
    match result {
        Ok(output) => FfiResult {
            handle: output.handle,
            code: if output.eof { 2 } else { 0 },
            buffer: Buffer::new(output.data),
        },
        Err(error) => FfiResult {
            handle: 0,
            code: 1,
            buffer: Buffer::new(
                serde_json::to_vec(&error).expect("serializing string fields cannot fail"),
            ),
        },
    }
}

// SAFETY: Nonempty input points to len initialized bytes valid for this call.
unsafe fn input<'a>(data: *const u8, len: usize) -> Result<&'a [u8], BridgeError> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null() || len > isize::MAX as usize {
        return Err(BridgeError::invalid("invalid input buffer"));
    }
    // SAFETY: The caller guarantees this allocation and lifetime.
    Ok(unsafe { slice::from_raw_parts(data, len) })
}

/// Returns a process-lifetime string. Do not modify or free it.
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_bridge_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// # Safety
/// buffer must be an unmodified buffer returned by this library and freed once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_buffer_free(buffer: Buffer) {
    if !buffer.data.is_null() {
        // SAFETY: Buffer::new allocated this exact boxed slice.
        drop(unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(buffer.data, buffer.len)) });
    }
}

/// # Safety
/// config points to len initialized bytes for this call; NULL requires length 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_http_client_new(config: *const u8, len: usize) -> FfiResult {
    ffi(|| http_client::create(unsafe { input(config, len)? }))
}

#[unsafe(no_mangle)]
pub extern "C" fn gocodex_http_client_close(handle: u64) -> FfiResult {
    ffi(|| http_client::close(handle))
}

/// Copies the request into Rust-owned memory without network I/O.
///
/// # Safety
/// metadata and body point to initialized buffers of their specified lengths
/// for this call. NULL is permitted only for a zero-length buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_http_request_new(
    client: u64,
    metadata: *const u8,
    metadata_len: usize,
    body: *const u8,
    body_len: usize,
) -> FfiResult {
    ffi(|| {
        http_client::create_request(client, unsafe { input(metadata, metadata_len)? }, unsafe {
            input(body, body_len)?
        })
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn gocodex_http_request_send(handle: u64) -> FfiResult {
    ffi(|| http_client::send(handle))
}

#[unsafe(no_mangle)]
pub extern "C" fn gocodex_http_response_read(handle: u64, max_len: usize) -> FfiResult {
    ffi(|| http_client::read(handle, max_len))
}

/// Idempotently cancels and releases a request, including its response stream.
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_http_request_close(handle: u64) -> FfiResult {
    ffi(|| http_client::close_request(handle))
}

/// # Safety
/// config points to len initialized bytes; NULL requires length zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_ws_client_new(config: *const u8, len: usize) -> FfiResult {
    ffi(|| websocket_client::create(unsafe { input(config, len)? }))
}
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_ws_client_close(id: u64) -> FfiResult {
    ffi(|| websocket_client::close(id))
}
/// # Safety
/// metadata points to len initialized bytes; NULL requires length zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_ws_connection_new(
    id: u64,
    metadata: *const u8,
    len: usize,
) -> FfiResult {
    ffi(|| websocket_client::create_connection(id, unsafe { input(metadata, len)? }))
}
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_ws_connect(id: u64) -> FfiResult {
    ffi(|| websocket_client::connect(id))
}
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_ws_read(id: u64) -> FfiResult {
    ffi(|| websocket_client::read(id))
}
/// # Safety
/// data points to len initialized bytes; NULL requires length zero.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn gocodex_ws_write(
    id: u64,
    kind: u8,
    data: *const u8,
    len: usize,
) -> FfiResult {
    ffi(|| websocket_client::write(id, kind, unsafe { input(data, len)? }))
}
#[unsafe(no_mangle)]
pub extern "C" fn gocodex_ws_connection_close(id: u64) -> FfiResult {
    ffi(|| websocket_client::close_connection(id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_peer_alerts_are_distinct_from_local_certificate_failures() {
        assert!(has_handshake_failure_source(&rustls::Error::AlertReceived(
            rustls::AlertDescription::InternalError,
        )));
        assert!(!has_handshake_failure_source(
            &rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer),
        ));
        assert!(!has_handshake_failure_source(&std::io::Error::other(
            "tlsv1 alert internal error; SOCKS error: general server failure",
        )));
    }

    #[tokio::test]
    async fn dns_classification_does_not_depend_on_resolver_wording() {
        struct FailingResolver;
        impl reqwest::dns::Resolve for FailingResolver {
            fn resolve(&self, _: reqwest::dns::Name) -> reqwest::dns::Resolving {
                Box::pin(async { Err(std::io::Error::other("opaque resolver failure").into()) })
            }
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(std::sync::Arc::new(FailingResolver))
            .build()
            .unwrap();
        let error = client
            .get("http://dns-fixture.invalid/private?token=secret")
            .send()
            .await
            .unwrap_err();
        let error = BridgeError::from(error);
        assert_eq!(error.kind, "dns");
        assert!(error.message.contains("opaque resolver failure"));
        assert!(!error.message.contains("private"));
        assert!(!error.message.contains("secret"));
    }

    #[test]
    fn premature_eof_uses_error_types_not_backend_messages() {
        let truncated =
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "opaque read failure");
        assert!(is_unexpected_eof(&truncated));
        let other = std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "connection closed before message completed; end of file before message length reached",
        );
        assert!(!is_unexpected_eof(&other));
    }

    fn error_kind(result: FfiResult) -> String {
        assert_eq!(result.code, 1);
        // SAFETY: This is a buffer allocated by the ABI and freed once below.
        let bytes = unsafe { slice::from_raw_parts(result.buffer.data, result.buffer.len) };
        let value: serde_json::Value = serde_json::from_slice(bytes).unwrap();
        unsafe { gocodex_buffer_free(result.buffer) };
        value["kind"].as_str().unwrap().to_owned()
    }

    #[test]
    fn ffi_rejects_null_nonempty_inputs() {
        // SAFETY: The API explicitly checks null inputs before dereferencing them.
        let result = unsafe { gocodex_http_client_new(ptr::null(), 1) };
        assert_eq!(error_kind(result), "invalid_input");
        let result = unsafe { gocodex_ws_client_new(ptr::null(), 1) };
        assert_eq!(error_kind(result), "invalid_input");
    }

    #[test]
    fn panic_does_not_cross_abi() {
        let result = ffi(|| panic!("simulated request panic"));
        assert_eq!(error_kind(result), "panic");
    }

    #[test]
    fn rust_owned_buffers_preserve_binary_bytes() {
        for bytes in [vec![], vec![0, 255, 128, 0, 1]] {
            let buffer = Buffer::new(bytes.clone());
            let received = if buffer.len == 0 {
                &[]
            } else {
                // SAFETY: Buffer is still live and has its original length.
                unsafe { slice::from_raw_parts(buffer.data, buffer.len) }
            };
            assert_eq!(received, bytes);
            unsafe { gocodex_buffer_free(buffer) };
        }
    }
}
