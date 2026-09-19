//go:build cgo && linux && (amd64 || arm64)

package websocket

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/csbxd/gocodex/internal/bridge"
	"net/http"
	"sync/atomic"
	"time"
)

var (
	ErrClientClosed = errors.New("codexws: client is closed")
	ErrConnClosed   = errors.New("codexws: connection is closed")
)

// Error reports a Rust SDK or bridge failure. HTTP upgrade failures are errors.
type Error = bridge.Error

// Version returns the shared Rust bridge version.
func Version() string { return bridge.Version() }

type ProxyPolicy string

const (
	RespectSystemProxy ProxyPolicy = "respect_system_proxy"
	ReqwestDefault     ProxyPolicy = "reqwest_default"
)

// Options configures a reusable Rust WebSocketConnector. Zero values use the
// SDK's system proxy policy, native/custom CA roots and message size limits.
type Options struct {
	ProxyPolicy           ProxyPolicy
	ProxyURL              string // Explicit HTTP/HTTPS/SOCKS5/SOCKS5h proxy; overrides system/environment routing.
	NoProxy               bool   // Force direct connections; mutually exclusive with ProxyURL.
	HandshakeTimeout      time.Duration
	LoopbackDirect        bool // Bypass proxies only for SDK-validated loopback URLs.
	TCPNoDelay            bool
	MaxMessageSize        int // Zero keeps the SDK default; positive values limit bytes.
	MaxFrameSize          int
	ChatGPTCookies        []string
	WriteFrameSize        int // Outbound data fragment size; default 32 KiB, maximum 1 MiB.
	MaxHandshakeBodyBytes int // Captured rejected-upgrade body limit; default 64 KiB, maximum 1 MiB.
}

// Client owns a connector and its connections. Do not copy; call Close when done.
// Methods are safe for concurrent use.
type Client struct{ handle atomic.Uint64 }

func NewClient(options Options) (*Client, error) {
	if options.HandshakeTimeout < 0 || options.MaxMessageSize < 0 || options.MaxFrameSize < 0 || options.WriteFrameSize < 0 || options.MaxHandshakeBodyBytes < 0 {
		return nil, errors.New("codexws: timeouts and size limits must not be negative")
	}
	if options.ProxyURL != "" && (options.NoProxy || options.LoopbackDirect) {
		return nil, errors.New("codexws: ProxyURL is mutually exclusive with NoProxy and LoopbackDirect")
	}
	if options.WriteFrameSize > 1<<20 || options.MaxHandshakeBodyBytes > 1<<20 {
		return nil, errors.New("codexws: frame and handshake body limits must not exceed 1 MiB")
	}
	if options.ProxyPolicy == "" {
		options.ProxyPolicy = RespectSystemProxy
	}
	if options.ProxyPolicy != RespectSystemProxy && options.ProxyPolicy != ReqwestDefault {
		return nil, errors.New("codexws: invalid ProxyPolicy")
	}
	ms := uint64(options.HandshakeTimeout / time.Millisecond)
	if options.HandshakeTimeout%time.Millisecond != 0 {
		ms++
	}
	config, err := json.Marshal(struct {
		ProxyPolicy           ProxyPolicy `json:"proxy_policy"`
		HandshakeTimeoutMS    uint64      `json:"handshake_timeout_ms"`
		LoopbackDirect        bool        `json:"loopback_direct"`
		TCPNoDelay            bool        `json:"tcp_nodelay"`
		MaxMessageSize        int         `json:"max_message_size,omitempty"`
		MaxFrameSize          int         `json:"max_frame_size,omitempty"`
		ChatGPTCookies        []string    `json:"chatgpt_cookies"`
		ProxyURL              string      `json:"proxy_url"`
		NoProxy               bool        `json:"no_proxy"`
		WriteFrameSize        int         `json:"write_frame_size"`
		MaxHandshakeBodyBytes int         `json:"max_handshake_body_bytes"`
	}{options.ProxyPolicy, ms, options.LoopbackDirect, options.TCPNoDelay,
		options.MaxMessageSize, options.MaxFrameSize, options.ChatGPTCookies,
		options.ProxyURL, options.NoProxy, options.WriteFrameSize, options.MaxHandshakeBodyBytes})
	if err != nil {
		return nil, err
	}
	id, err := bridge.WSClientNew(config)
	if err != nil {
		return nil, err
	}
	client := &Client{}
	client.handle.Store(id)
	return client, nil
}

func (c *Client) id() uint64 {
	if c == nil {
		return 0
	}
	return c.handle.Load()
}

// Close immediately cancels handshakes and all open connections, releasing Rust
// resources. It is idempotent and does not wait for a WebSocket closing handshake.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if id := c.handle.Swap(0); id != 0 {
		return bridge.WSClientClose(id)
	}
	return nil
}

type Handshake struct {
	StatusCode    int
	Headers       http.Header
	Body          []byte // Captured HTTP error body; empty on successful upgrades.
	BodyTruncated bool   // Body capped or completeness could not be established.
}

// HandshakeError preserves rejected-upgrade metadata. Dial also returns the same
// Handshake as its second result. Unwrap retains errors.As(err, **Error).
type HandshakeError struct {
	Handshake *Handshake
	Err       error
}

func (e *HandshakeError) Error() string { return e.Err.Error() }
func (e *HandshakeError) Unwrap() error { return e.Err }

// Dial establishes a ws/wss connection. ctx controls only the handshake; use
// per-operation contexts for ReadMessage and WriteMessage after Dial returns.
// Headers can include authentication, cookies and Sec-WebSocket-Protocol.
// A rejected HTTP upgrade returns a non-nil Handshake and *HandshakeError.
func (c *Client) Dial(ctx context.Context, url string, headers http.Header) (*Conn, *Handshake, error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, err
	}
	id := c.id()
	if id == 0 {
		return nil, nil, ErrClientClosed
	}
	wire := struct {
		URL     string      `json:"url"`
		Headers [][2]string `json:"headers"`
	}{URL: url, Headers: make([][2]string, 0)}
	for name, values := range headers {
		for _, value := range values {
			wire.Headers = append(wire.Headers, [2]string{name, base64.StdEncoding.EncodeToString([]byte(value))})
		}
	}
	metadata, err := json.Marshal(wire)
	if err != nil {
		return nil, nil, err
	}
	connID, err := bridge.WSConnectionNew(id, metadata)
	if err != nil {
		if c.id() == 0 {
			err = ErrClientClosed
		}
		return nil, nil, err
	}
	conn := &Conn{client: c}
	conn.handle.Store(connID)
	stop := watchContext(ctx, conn)
	data, err := bridge.WSConnect(connID)
	stop()
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if c.id() == 0 {
		err = ErrClientClosed
	}
	if err != nil {
		_ = conn.Close()
		var native *Error
		if errors.As(err, &native) && native.Kind == "handshake" {
			var failure struct {
				Details json.RawMessage `json:"details"`
			}
			if decodeErr := json.Unmarshal(data, &failure); decodeErr != nil {
				return nil, nil, decodeErr
			}
			handshake, decodeErr := decodeHandshake(failure.Details)
			if decodeErr != nil {
				return nil, nil, decodeErr
			}
			return nil, handshake, &HandshakeError{Handshake: handshake, Err: err}
		}
		return nil, nil, err
	}
	handshake, err := decodeHandshake(data)
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return conn, handshake, nil
}

func decodeHandshake(data []byte) (*Handshake, error) {
	var response struct {
		StatusCode    int         `json:"status_code"`
		Headers       [][2]string `json:"headers"`
		Body          []byte      `json:"body"`
		BodyTruncated bool        `json:"body_truncated"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return nil, fmt.Errorf("codexws: invalid native handshake: %w", err)
	}
	handshake := &Handshake{StatusCode: response.StatusCode, Headers: make(http.Header), Body: response.Body, BodyTruncated: response.BodyTruncated}
	for _, header := range response.Headers {
		value, err := base64.StdEncoding.DecodeString(header[1])
		if err != nil {
			return nil, err
		}
		handshake.Headers.Add(header[0], string(value))
	}
	return handshake, nil
}

type MessageType uint8

const (
	TextMessage   MessageType = 1
	BinaryMessage MessageType = 2
	CloseMessage  MessageType = 8
	PingMessage   MessageType = 9
	PongMessage   MessageType = 10
)

// Conn supports concurrent reads and writes; Rust serializes each direction
// independently. Messages are buffered in memory. Call Close when finished.
// Cancellation of an in-flight operation closes the whole connection because a
// partially transmitted frame cannot safely be retried.
type Conn struct {
	client *Client
	handle atomic.Uint64
}

func (c *Conn) id() uint64 {
	if c == nil {
		return 0
	}
	return c.handle.Load()
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("codexws: nil context")
	}
	return ctx.Err()
}

func watchContext(ctx context.Context, conn *Conn) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = conn.Close(); close(done) })
	return func() {
		if !stop() {
			<-done
		}
	}
}

func (c *Conn) operationError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if c.client.id() == 0 {
		return ErrClientClosed
	}
	if c.id() == 0 {
		return ErrConnClosed
	}
	return err
}

// ReadMessage returns a complete message. Ping and close frames are returned
// after the SDK flushes its automatic reply. A close payload contains a big-endian
// uint16 status followed by a UTF-8 reason, or is empty. Clean termination returns
// io.EOF; protocol and transport failures return *Error.
func (c *Conn) ReadMessage(ctx context.Context) (MessageType, []byte, error) {
	if err := checkContext(ctx); err != nil {
		return 0, nil, err
	}
	id := c.id()
	if id == 0 {
		return 0, nil, ErrConnClosed
	}
	stop := watchContext(ctx, c)
	data, err := bridge.WSRead(id)
	stop()
	err = c.operationError(ctx, err)
	if err != nil {
		_ = c.Close()
		return 0, nil, err
	}
	if len(data) == 0 {
		_ = c.Close()
		return 0, nil, errors.New("codexws: empty native message")
	}
	return MessageType(data[0]), data[1:], nil
}

// WriteMessage sends one message as masked data fragments without interleaving
// other data messages. Controls take priority between fragments. Automatic replies
// require an active ReadMessage call.
func (c *Conn) WriteMessage(ctx context.Context, kind MessageType, data []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	id := c.id()
	if id == 0 {
		return ErrConnClosed
	}
	stop := watchContext(ctx, c)
	err := bridge.WSWrite(id, uint8(kind), data)
	stop()
	err = c.operationError(ctx, err)
	if err != nil {
		var native *Error
		if !errors.As(err, &native) || (native.Kind != "invalid_input" && native.Kind != "closing") {
			_ = c.Close()
		}
	}
	return err
}

// WriteControl sends Ping, Pong or Close concurrently with data writes, taking
// priority at the next frame boundary. Cancellation closes the connection.
// A closing error does not discard the peer's pending Close frame.
func (c *Conn) WriteControl(ctx context.Context, kind MessageType, data []byte) error {
	if kind != PingMessage && kind != PongMessage && kind != CloseMessage {
		return errors.New("codexws: WriteControl requires Ping, Pong or Close")
	}
	return c.WriteMessage(ctx, kind, data)
}

// Close releases the socket immediately and interrupts blocked operations.
// To perform the protocol closing handshake, first WriteMessage(CloseMessage,
// payload), then ReadMessage with a bounded context until the peer replies.
func (c *Conn) Close() error {
	if c == nil {
		return nil
	}
	if id := c.handle.Swap(0); id != 0 {
		return bridge.WSConnectionClose(id)
	}
	return nil
}

// ClosePayload encodes a close status and reason. WriteMessage validates the
// status, UTF-8 encoding and the control frame's 125-byte payload limit.
func ClosePayload(code uint16, reason string) []byte {
	data := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(data, code)
	return append(data, reason...)
}

// ParseClosePayload decodes an incoming close frame. Empty frames have status 1005.
func ParseClosePayload(data []byte) (uint16, string, error) {
	if len(data) == 0 {
		return 1005, "", nil
	}
	if len(data) < 2 {
		return 0, "", fmt.Errorf("codexws: invalid close payload")
	}
	return binary.BigEndian.Uint16(data), string(data[2:]), nil
}
