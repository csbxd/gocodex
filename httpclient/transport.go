//go:build cgo && linux && (amd64 || arm64)

package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTransportClosed is returned after Transport.Close, including by requests
// and response-body reads that Close interrupts.
var ErrTransportClosed = errors.New("httpclient: transport is closed")

// Transport implements http.RoundTripper using the Codex HTTP SDK. Its zero
// value is ready for use. A Transport is safe for concurrent use, must not be
// copied after first use, and should be reused across requests. Closing each
// response body releases its request resources. The runtime cleans up the native
// client when the Transport becomes unreachable; Close is optional and releases
// the client immediately. GC cleanup timing is not deterministic.
//
// Request bodies are buffered in memory; response bodies stream. Redirects,
// authentication and cookies are handled by http.Client. The SDK does not expose
// response TLS state or trailers. CONNECT tunnels, protocol upgrades and request
// trailers are not supported. HTTPS requests must use the URL's Host: the SDK
// does not apply a custom Host header to the HTTP/2 :authority pseudo-header.
type Transport struct {
	mu       sync.Mutex
	client   *Client
	lifetime context.Context
	cancel   context.CancelFunc
	timeout  time.Duration
	closed   bool
	cleanup  runtime.Cleanup
}

var _ http.RoundTripper = (*Transport)(nil)

// Keep only the native-client owner in the cleanup argument, not the Transport
// or its context tree. A request context may itself contain a Transport value.
func cleanupTransport(client *Client) {
	_ = client.Close()
}

func (t *Transport) registerCleanup() {
	t.cleanup = runtime.AddCleanup(t, cleanupTransport, t.client)
}

// NewTransport configures a reusable transport. DisableRedirects is always true
// internally, so http.Client.CheckRedirect owns redirect policy. ChatGPTCookies
// must be empty; set Request cookies or use http.Client.Jar instead.
// Timeout covers buffering the upload, sending, and reading the response body.
func NewTransport(options Options) (*Transport, error) {
	if options.Timeout < 0 {
		return nil, errors.New("httpclient: Timeout must not be negative")
	}
	if len(options.ChatGPTCookies) != 0 {
		return nil, errors.New("httpclient: Transport cookies must be configured through http.Client.Jar or request headers")
	}
	timeout := options.Timeout
	options.Timeout = 0 // A Go context also covers the buffered upload phase.
	options.DisableRedirects = true
	client, err := NewClient(options)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	t := &Transport{client: client, lifetime: lifetime, cancel: cancel, timeout: timeout}
	t.registerCleanup()
	return t, nil
}

func (t *Transport) state() (*Client, context.Context, time.Duration, error) {
	if t == nil {
		return nil, nil, 0, ErrTransportClosed
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, nil, 0, ErrTransportClosed
	}
	if t.client == nil {
		client, err := NewClient(Options{DisableRedirects: true})
		if err != nil {
			return nil, nil, 0, err
		}
		t.client = client
		t.lifetime, t.cancel = context.WithCancel(context.Background())
		t.registerCleanup()
	}
	return t.client, t.lifetime, t.timeout, nil
}

// Close optionally releases the Rust client immediately and cancels requests,
// including blocked uploads and response reads. It is not required for ordinary
// http.Client use. Close is idempotent and permanently closes t.
func (t *Transport) Close() error {
	if t == nil {
		return nil
	}
	defer runtime.KeepAlive(t)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cleanup.Stop()
	client, cancel := t.client, t.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return client.Close()
}

// RoundTrip executes exactly one HTTP transaction. It consumes and closes the
// request body on every path, but does not mutate other request fields. As with
// net/http, Request.Body.Close must unblock a concurrent Body.Read.
func (t *Transport) RoundTrip(req *http.Request) (response *http.Response, err error) {
	defer runtime.KeepAlive(t)
	if req == nil {
		return nil, errors.New("httpclient: nil request")
	}
	upload := req.Body
	closeUpload := sync.OnceFunc(func() {
		if upload != nil {
			_ = upload.Close()
		}
	})
	defer closeUpload()
	if req.URL == nil {
		return nil, errors.New("httpclient: nil request URL")
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return nil, errors.New("httpclient: URL must use http or https")
	}
	if req.URL.Host == "" {
		return nil, errors.New("httpclient: request URL has no host")
	}
	if req.URL.Scheme == "https" && req.Host != "" && !strings.EqualFold(req.Host, req.URL.Host) {
		// Do not discover this after sending: an HTTP/2 request would already
		// have reached the URL authority instead of the requested virtual host.
		return nil, errors.New("httpclient: HTTPS Host overrides are not supported by the SDK")
	}
	if req.RequestURI != "" {
		return nil, errors.New("httpclient: RequestURI must be empty in client requests")
	}
	if req.ContentLength < -1 {
		return nil, errors.New("httpclient: invalid ContentLength")
	}
	if req.Method == http.MethodConnect || hasHeaderValue(req.Header, "Upgrade") || headerToken(req.Header, "Connection", "upgrade") {
		return nil, errors.New("httpclient: CONNECT and protocol upgrades are not supported")
	}
	if len(req.Trailer) != 0 || hasHeaderValue(req.Header, "Trailer") {
		return nil, errors.New("httpclient: request trailers are not supported")
	}
	if len(req.TransferEncoding) != 0 && (len(req.TransferEncoding) != 1 || req.TransferEncoding[0] != "chunked") {
		return nil, errors.New("httpclient: unsupported request TransferEncoding")
	}
	client, lifetime, timeout, err := t.state()
	if err != nil {
		return nil, err
	}
	requestContext := req.Context()
	ctx, finishContext := transportContext(requestContext, req.Cancel, lifetime, timeout)
	finish := sync.OnceFunc(func() {
		finishContext()
		// A temporary http.Client may disappear while its response body is still
		// being read. Retain the Transport through this request's terminal event;
		// OnceFunc drops this closure (and its owner reference) afterwards.
		runtime.KeepAlive(t)
	})
	defer func() {
		if response == nil {
			finish()
		}
	}()
	mapError := func(err error) error { return transportError(requestContext, ctx, lifetime, err) }
	if err := mapError(ctx.Err()); err != nil {
		return nil, err
	}

	var body []byte
	if upload != nil {
		stop := context.AfterFunc(ctx, closeUpload)
		body, err = io.ReadAll(upload)
		stop()
		closeUpload() // Also waits for a concurrent cancellation's Close call.
		if err = mapError(err); err != nil {
			return nil, err
		}
	}
	if len(req.TransferEncoding) == 0 && req.ContentLength > 0 && int64(len(body)) != req.ContentLength {
		return nil, fmt.Errorf("httpclient: ContentLength=%d with Body length %d", req.ContentLength, len(body))
	}

	// These fields have dedicated representations in http.Request. Ignore stale
	// framing/Host headers and let the SDK compute length from the buffered body.
	headers := make(http.Header, len(req.Header)+1)
	for key, values := range req.Header {
		switch strings.ToLower(key) {
		case "host", "content-length", "transfer-encoding":
			continue
		}
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	if req.Host != "" {
		headers.Set("Host", req.Host)
	}
	if req.Close {
		headers.Set("Connection", "close")
	}
	if len(req.TransferEncoding) != 0 {
		headers.Set("Transfer-Encoding", "chunked")
	}
	url := *req.URL
	url.User = nil // http.Client applies URL credentials, not RoundTripper.
	url.Fragment = ""
	url.RawFragment = ""
	result, err := client.Do(ctx, Request{Method: req.Method, URL: url.String(), Headers: headers, Body: body})
	if err != nil {
		return nil, mapError(err)
	}
	major, minor, ok := http.ParseHTTPVersion(result.Protocol)
	if !ok || result.StatusCode == http.StatusSwitchingProtocols {
		_ = result.Body.Close()
		return nil, errors.New("httpclient: unsupported response protocol")
	}
	status := strconv.Itoa(result.StatusCode)
	if text := http.StatusText(result.StatusCode); text != "" {
		status += " " + text
	}
	contentLength := result.ContentLength
	if req.Method == http.MethodHead {
		contentLength = -1
		if text := result.Headers.Get("Content-Length"); text != "" {
			if n, parseErr := strconv.ParseInt(text, 10, 64); parseErr == nil && n >= 0 {
				contentLength = n
			}
		}
	}
	var transferEncoding []string
	if headerToken(result.Headers, "Transfer-Encoding", "chunked") {
		transferEncoding = []string{"chunked"}
		result.Headers.Del("Transfer-Encoding")
	}
	closeConnection := req.Close || headerToken(result.Headers, "Connection", "close") ||
		(major == 1 && minor == 0 && !headerToken(result.Headers, "Connection", "keep-alive"))
	if major == 1 && contentLength < 0 && len(transferEncoding) == 0 && req.Method != http.MethodHead {
		closeConnection = true // HTTP/1 response delimited by connection EOF.
	}
	return &http.Response{
		Status: status, StatusCode: result.StatusCode,
		Proto: result.Protocol, ProtoMajor: major, ProtoMinor: minor,
		Header: result.Headers, ContentLength: contentLength,
		TransferEncoding: transferEncoding, Close: closeConnection, Request: req,
		Body: &transportBody{body: result.Body, finish: finish, mapError: mapError},
	}, nil
}

func transportContext(parent context.Context, legacyCancel <-chan struct{}, lifetime context.Context, timeout time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	cancelDeadline := func() {}
	if timeout > 0 {
		ctx, cancelDeadline = context.WithTimeout(ctx, timeout)
	}
	if legacyCancel != nil {
		// net/http.Client may still populate Cancel on custom RoundTrippers.
		select {
		case <-legacyCancel:
			cancel(context.Canceled)
		default:
		}
		go func() {
			select {
			case <-legacyCancel:
				cancel(context.Canceled)
			case <-ctx.Done():
			}
		}()
	}
	stop := context.AfterFunc(lifetime, func() { cancel(ErrTransportClosed) })
	return ctx, sync.OnceFunc(func() {
		stop()
		cancelDeadline()
		cancel(nil)
	})
}

func transportError(request, operation, lifetime context.Context, err error) error {
	if request.Err() != nil {
		return context.Cause(request)
	}
	if lifetime.Err() != nil {
		return ErrTransportClosed
	}
	if operation.Err() != nil {
		return context.Cause(operation)
	}
	return err
}

func hasHeaderValue(header http.Header, name string) bool {
	for key, values := range header {
		if strings.EqualFold(key, name) {
			for _, value := range values {
				if strings.TrimSpace(value) != "" {
					return true
				}
			}
		}
	}
	return false
}

func headerToken(header http.Header, name, token string) bool {
	for key, values := range header {
		if !strings.EqualFold(key, name) {
			continue
		}
		for _, value := range values {
			for _, part := range strings.Split(value, ",") {
				if strings.EqualFold(strings.TrimSpace(part), token) {
					return true
				}
			}
		}
	}
	return false
}

type transportBody struct {
	body     io.ReadCloser
	finish   func()
	mapError func(error) error
	closed   atomic.Bool
	readMu   sync.Mutex
	terminal error
}

func (b *transportBody) Read(p []byte) (int, error) {
	b.readMu.Lock()
	defer b.readMu.Unlock()
	if b.closed.Load() {
		return 0, ErrBodyClosed
	}
	if b.terminal != nil {
		return 0, b.terminal
	}
	n, err := b.body.Read(p)
	if err != nil {
		if b.closed.Load() {
			err = ErrBodyClosed
		} else {
			err = b.mapError(err)
		}
		b.terminal = err
		b.finish()
	}
	return n, err
}

func (b *transportBody) Close() error {
	b.closed.Store(true)
	err := b.body.Close()
	b.finish()
	return err
}
