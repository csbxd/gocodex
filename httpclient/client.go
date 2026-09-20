//go:build cgo && linux && (amd64 || arm64)

package httpclient

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/csbxd/gocodex/internal/bridge"
)

var (
	ErrClientClosed = errors.New("codexhttp: client is closed")
	ErrBodyClosed   = errors.New("codexhttp: response body is closed")
)

// Error reports a Rust-side failure. Kind is invalid_input, configuration,
// request, dns, unexpected_eof, timeout, cancelled, closed, internal, or panic.
// HTTP 4xx/5xx statuses are not errors.
type Error = bridge.Error

// Version returns the shared Rust bridge version.
func Version() string { return bridge.Version() }

// ProxyPolicy selects the SDK's outbound routing policy.
type ProxyPolicy string

const (
	RespectSystemProxy ProxyPolicy = "respect_system_proxy"
	ReqwestDefault     ProxyPolicy = "reqwest_default"
)

// Options configures codex-http-client. The default policy is RespectSystemProxy.
// Custom CAs use CODEX_CA_CERTIFICATE and SSL_CERT_FILE, as in the Rust SDK.
type Options struct {
	Timeout            time.Duration
	ProxyPolicy        ProxyPolicy
	ProxyURL           string // Explicit http/https/socks5/socks5h proxy; overrides ProxyPolicy and environment proxies.
	NoProxy            bool   // Force a direct connection; mutually exclusive with ProxyURL.
	DisableRedirects   bool
	UserAgent          string
	ChatGPTCookies     []string
	TLSBackendFallback bool
}

// Client owns a Rust codex-http-client and connection pool. It supports concurrent
// requests. Construct it with NewClient, do not copy it, and call Close when done.
type Client struct {
	mu     sync.RWMutex
	handle uint64
}

// NewClient initializes a reusable SDK client. Automatic routing builds
// transports lazily; ProxyURL and NoProxy build a fixed-route client immediately.
// Explicit routing cannot be combined with TLSBackendFallback.
func NewClient(options Options) (*Client, error) {
	if options.Timeout < 0 {
		return nil, errors.New("codexhttp: Timeout must not be negative")
	}
	if options.NoProxy && options.ProxyURL != "" {
		return nil, errors.New("httpclient: ProxyURL and NoProxy are mutually exclusive")
	}
	if (options.NoProxy || options.ProxyURL != "") && options.TLSBackendFallback {
		return nil, errors.New("httpclient: TLSBackendFallback requires automatic proxy routing")
	}
	if options.ProxyPolicy == "" {
		options.ProxyPolicy = RespectSystemProxy
	}
	if options.ProxyPolicy != RespectSystemProxy && options.ProxyPolicy != ReqwestDefault {
		return nil, errors.New("codexhttp: invalid ProxyPolicy")
	}
	config, err := json.Marshal(struct {
		TimeoutMS          uint64      `json:"timeout_ms"`
		ProxyPolicy        ProxyPolicy `json:"proxy_policy"`
		ProxyURL           string      `json:"proxy_url"`
		NoProxy            bool        `json:"no_proxy"`
		DisableRedirects   bool        `json:"disable_redirects"`
		UserAgent          string      `json:"user_agent"`
		ChatGPTCookies     []string    `json:"chatgpt_cookies"`
		TLSBackendFallback bool        `json:"tls_backend_fallback"`
	}{durationMS(options.Timeout), options.ProxyPolicy, options.ProxyURL, options.NoProxy, options.DisableRedirects,
		options.UserAgent, options.ChatGPTCookies, options.TLSBackendFallback})
	if err != nil {
		return nil, err
	}
	handle, err := bridge.HTTPClientNew(config)
	if err != nil {
		return nil, err
	}
	return &Client{handle: handle}, nil
}

func durationMS(d time.Duration) uint64 {
	// Round positive sub-millisecond durations up without overflowing Duration.
	ms := uint64(d / time.Millisecond)
	if d%time.Millisecond != 0 {
		ms++
	}
	return ms
}

func (c *Client) id() uint64 {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.handle
}

// Close releases the pool and cancels all requests and response bodies belonging
// to this client. Close is idempotent and may run concurrently with Do or Read.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handle == 0 {
		return nil
	}
	handle := c.handle
	c.handle = 0
	return bridge.HTTPClientClose(handle)
}

// Request describes one HTTP request. Method defaults to GET. Body is binary
// data copied into Rust before network I/O. JSON is serialized using encoding/json
// and the SDK's JSON builder. Body and JSON are mutually exclusive. Use
// json.RawMessage("null") or PostJSON(..., nil) to explicitly send JSON null.
// Request bodies are buffered; response bodies stream.
type Request struct {
	Method  string
	URL     string
	Headers http.Header
	Body    []byte
	JSON    any
}

// Response includes headers and a streamed body. Close Body after use, including
// on non-2xx responses. URL is the final URL after the SDK follows redirects.
type Response struct {
	StatusCode    int
	URL           string
	Protocol      string
	Headers       http.Header
	ContentLength int64 // -1 when unknown.
	Body          io.ReadCloser
}

type requestMetadata struct {
	Method  string          `json:"method"`
	URL     string          `json:"url"`
	Headers [][2]string     `json:"headers"`
	HasBody bool            `json:"has_body"`
	HasJSON bool            `json:"has_json"`
	JSON    json.RawMessage `json:"json"`
}

type responseMetadata struct {
	StatusCode    int         `json:"status_code"`
	URL           string      `json:"url"`
	Protocol      string      `json:"protocol"`
	Headers       [][2]string `json:"headers"`
	ContentLength *int64      `json:"content_length"`
}

// Do sends a request through codex-http-client. It returns as soon as response headers
// arrive. ctx remains active until the body reaches EOF or is closed. Cancellation
// interrupts pending Rust network operations, including response-body reads.
func (c *Client) Do(ctx context.Context, request Request) (*Response, error) {
	if ctx == nil {
		return nil, errors.New("codexhttp: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	clientID := c.id()
	if clientID == 0 {
		return nil, ErrClientClosed
	}
	if request.Body != nil && request.JSON != nil {
		return nil, errors.New("codexhttp: Body and JSON are mutually exclusive")
	}
	metadata := requestMetadata{
		Method: request.Method, URL: request.URL, Headers: make([][2]string, 0),
		HasBody: request.Body != nil, HasJSON: request.JSON != nil,
	}
	if metadata.Method == "" {
		metadata.Method = http.MethodGet
	}
	if metadata.HasJSON {
		data, err := json.Marshal(request.JSON)
		if err != nil {
			return nil, fmt.Errorf("codexhttp: encode JSON request: %w", err)
		}
		metadata.JSON = data
	}
	for name, values := range request.Headers {
		for _, value := range values {
			metadata.Headers = append(metadata.Headers, [2]string{name, base64.StdEncoding.EncodeToString([]byte(value))})
		}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	handle, err := bridge.HTTPRequestNew(clientID, encoded, request.Body)
	if err != nil {
		return nil, c.requestError(ctx, err)
	}
	// Closing a numeric handle is safe even when it races with send/read/Close.
	stop := context.AfterFunc(ctx, func() { _ = bridge.HTTPRequestClose(handle) })
	data, err := bridge.HTTPRequestSend(handle)
	if err != nil {
		stop()
		_ = bridge.HTTPRequestClose(handle)
		return nil, c.requestError(ctx, err)
	}
	var wire responseMetadata
	if err = json.Unmarshal(data, &wire); err != nil {
		stop()
		_ = bridge.HTTPRequestClose(handle)
		return nil, fmt.Errorf("codexhttp: decode response metadata: %w", err)
	}
	response := &Response{
		StatusCode: wire.StatusCode, URL: wire.URL, Protocol: wire.Protocol,
		Headers: make(http.Header), ContentLength: -1,
	}
	for _, header := range wire.Headers {
		value, decodeErr := base64.StdEncoding.DecodeString(header[1])
		if decodeErr != nil {
			stop()
			_ = bridge.HTTPRequestClose(handle)
			return nil, fmt.Errorf("codexhttp: decode response header: %w", decodeErr)
		}
		response.Headers.Add(header[0], string(value))
	}
	if wire.ContentLength != nil {
		response.ContentLength = *wire.ContentLength
	}
	response.Body = &responseBody{client: c, ctx: ctx, handle: handle, stop: stop}
	return response, nil
}

func (c *Client) requestError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if c.id() == 0 {
		return ErrClientClosed
	}
	var native *Error
	if errors.As(err, &native) && native.Kind == "dns" {
		// Preserve the dial-stage error chain used by net/http consumers. The
		// native resolver does not expose Name/Server or reason flags; guessing
		// them from the request URL would misidentify failed proxy lookups.
		return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{
			Err:       native.Error(),
			UnwrapErr: err,
		}}
	}
	return err
}

// Get sends a GET request.
func (c *Client) Get(ctx context.Context, url string) (*Response, error) {
	return c.Do(ctx, Request{URL: url})
}

// PostJSON sends a POST request using the SDK's JSON request builder.
func (c *Client) PostJSON(ctx context.Context, url string, value any) (*Response, error) {
	if value == nil {
		value = json.RawMessage("null")
	}
	return c.Do(ctx, Request{Method: http.MethodPost, URL: url, JSON: value})
}

type responseBody struct {
	client *Client
	ctx    context.Context
	handle uint64
	stop   func() bool

	readMu sync.Mutex // Serialize reads from the Rust response stream.
	mu     sync.Mutex // Close must not wait for a blocked Read.
	closed bool
	err    error // Stable terminal read result, including EOF.
}

func (b *responseBody) Read(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	b.mu.Lock()
	closed, terminal := b.closed, b.err
	b.mu.Unlock()
	if closed {
		return 0, ErrBodyClosed
	}
	if terminal != nil {
		return 0, terminal
	}
	if len(p) == 0 {
		return 0, nil
	}
	data, err := bridge.HTTPResponseRead(b.handle, len(p))
	if err != nil {
		b.mu.Lock()
		if b.closed {
			err = ErrBodyClosed
		} else {
			err = b.client.requestError(b.ctx, err)
		}
		b.err = err
		b.mu.Unlock()
		b.stop()
		_ = bridge.HTTPRequestClose(b.handle)
		return 0, err
	}
	return copy(p, data), nil
}

func (b *responseBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	b.stop()
	return bridge.HTTPRequestClose(b.handle)
}
