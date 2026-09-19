//go:build cgo && linux && (amd64 || arm64)

// Package bridge owns the shared cgo ABI for both public transport packages.
// Linux amd64 and arm64 link the bundled Rust static library automatically.
package bridge

/*
#cgo linux,amd64 LDFLAGS: ${SRCDIR}/../../native/linux_amd64/libgocodex_ffi.a
#cgo linux,arm64 LDFLAGS: ${SRCDIR}/../../native/linux_arm64/libgocodex_ffi.a
#cgo LDFLAGS: -lgcc_s -ldl -lpthread -lm -lrt -lutil -lresolv
#include "bridge.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"unsafe"
)

// Version returns the Rust bridge version, not the SDK version.
func Version() string {
	return C.GoString(C.gocodex_bridge_version())
}

func bytePointer(b []byte) *C.uint8_t {
	return (*C.uint8_t)(unsafe.Pointer(unsafe.SliceData(b)))
}

func takeResult(result C.gocodex_result) (uint64, []byte, error) {
	defer C.gocodex_buffer_free(result.buffer)
	// Avoid C.GoBytes' signed 32-bit limit. Copy before freeing Rust memory.
	if uint64(result.buffer.len) > uint64(^uint(0)>>1) {
		return 0, nil, fmt.Errorf("gocodex: native buffer exceeds Go address space")
	}
	data := append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(result.buffer.data)), int(result.buffer.len))...)
	switch result.code {
	case 0:
		return uint64(result.handle), data, nil
	case 1:
		var err Error
		if jsonErr := json.Unmarshal(data, &err); jsonErr != nil {
			return 0, nil, fmt.Errorf("gocodex: invalid native error: %w", jsonErr)
		}
		return 0, nil, &err
	case 2:
		return 0, nil, io.EOF
	default:
		return 0, nil, fmt.Errorf("gocodex: unknown native result code %d", result.code)
	}
}

func HTTPClientNew(config []byte) (uint64, error) {
	result := C.gocodex_http_client_new(bytePointer(config), C.size_t(len(config)))
	runtime.KeepAlive(config)
	handle, _, err := takeResult(result)
	return handle, err
}

func HTTPClientClose(handle uint64) error {
	_, _, err := takeResult(C.gocodex_http_client_close(C.uint64_t(handle)))
	return err
}

func HTTPRequestNew(client uint64, metadata, body []byte) (uint64, error) {
	result := C.gocodex_http_request_new(C.uint64_t(client), bytePointer(metadata), C.size_t(len(metadata)), bytePointer(body), C.size_t(len(body)))
	runtime.KeepAlive(metadata)
	runtime.KeepAlive(body)
	handle, _, err := takeResult(result)
	return handle, err
}

func HTTPRequestSend(handle uint64) ([]byte, error) {
	_, data, err := takeResult(C.gocodex_http_request_send(C.uint64_t(handle)))
	return data, err
}

func HTTPResponseRead(handle uint64, maxLen int) ([]byte, error) {
	_, data, err := takeResult(C.gocodex_http_response_read(C.uint64_t(handle), C.size_t(maxLen)))
	return data, err
}

func HTTPRequestClose(handle uint64) error {
	_, _, err := takeResult(C.gocodex_http_request_close(C.uint64_t(handle)))
	return err
}

func WSClientNew(config []byte) (uint64, error) {
	result := C.gocodex_ws_client_new(bytePointer(config), C.size_t(len(config)))
	runtime.KeepAlive(config)
	handle, _, err := takeResult(result)
	return handle, err
}

func WSClientClose(handle uint64) error {
	_, _, err := takeResult(C.gocodex_ws_client_close(C.uint64_t(handle)))
	return err
}

func WSConnectionNew(client uint64, metadata []byte) (uint64, error) {
	result := C.gocodex_ws_connection_new(C.uint64_t(client), bytePointer(metadata), C.size_t(len(metadata)))
	runtime.KeepAlive(metadata)
	handle, _, err := takeResult(result)
	return handle, err
}
func WSConnect(handle uint64) ([]byte, error) {
	_, data, err := takeResult(C.gocodex_ws_connect(C.uint64_t(handle)))
	return data, err
}
func WSRead(handle uint64) ([]byte, error) {
	_, data, err := takeResult(C.gocodex_ws_read(C.uint64_t(handle)))
	return data, err
}
func WSWrite(handle uint64, kind uint8, data []byte) error {
	result := C.gocodex_ws_write(C.uint64_t(handle), C.uint8_t(kind), bytePointer(data), C.size_t(len(data)))
	runtime.KeepAlive(data)
	_, _, err := takeResult(result)
	return err
}
func WSConnectionClose(handle uint64) error {
	_, _, err := takeResult(C.gocodex_ws_connection_close(C.uint64_t(handle)))
	return err
}
