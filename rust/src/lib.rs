//! Synchronous C ABI over the Codex HTTP and WebSocket SDKs.
//!
//! Only integer handles and owned byte buffers cross the ABI. Registry lookups
//! clone an Arc before unlocking, allowing Close to race with in-flight calls.
//! No Go pointers are retained.

mod http_client;
mod runtime;
mod websocket_client;

use serde::Serialize;
use std::ffi::c_char;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{ptr, slice};

#[derive(Serialize)]
pub(crate) struct BridgeError {
    kind: &'static str,
    message: String,
}

impl BridgeError {
    pub(crate) fn new(kind: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new("invalid_input", message)
    }
}

impl From<codex_http_client::HttpError> for BridgeError {
    fn from(error: codex_http_client::HttpError) -> Self {
        let kind = if error.is_timeout() {
            "timeout"
        } else if error.is_builder() {
            "invalid_input"
        } else {
            "request"
        };
        // Preserve the source chain (including TLS errors), without the URL.
        let error = error.without_url();
        let mut message = error.to_string();
        let mut source = std::error::Error::source(&error);
        while let Some(cause) = source {
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Self::new(kind, message)
    }
}

impl From<codex_http_client::RouteAwareRequestError> for BridgeError {
    fn from(error: codex_http_client::RouteAwareRequestError) -> Self {
        let kind = if error.is_timeout() {
            "timeout"
        } else {
            "request"
        };
        Self::new(kind, error.to_string())
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
