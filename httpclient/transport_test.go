//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/csbxd/gocodex/httpclient"
)

func testTransport(t *testing.T, options httpclient.Options) *httpclient.Transport {
	t.Helper()
	transport, err := httpclient.NewTransport(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	return transport
}

type countedBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *countedBody) Close() error { b.closed.Add(1); return nil }

func TestTransportRequestAndResponse(t *testing.T) {
	payload := []byte{'a', 0, 255, 128, 'z'}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.Host != "virtual.example" || r.URL.RawQuery != "q=a%20b" {
			t.Errorf("request: %s host=%s url=%s", r.Method, r.Host, r.URL)
		}
		if r.ContentLength != int64(len(payload)) || r.Header.Get("Authorization") != "" {
			t.Errorf("length=%d auth=%q", r.ContentLength, r.Header.Get("Authorization"))
		}
		if !reflect.DeepEqual(r.Header.Values("X-Multi"), []string{"one", "two"}) {
			t.Error("repeated headers lost")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, payload) {
			t.Errorf("request body=%q err=%v", body, err)
		}
		w.Header().Add("Set-Cookie", "one=1")
		w.Header().Add("Set-Cookie", "two=2")
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	transport := testTransport(t, httpclient.Options{})
	body := &countedBody{Reader: bytes.NewReader(payload)}
	req, err := http.NewRequest(http.MethodPut, server.URL+"/data?q=a%20b#fragment", body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = int64(len(payload))
	req.Host = "virtual.example"
	req.URL.User = url.UserPassword("ignored-by-RoundTrip", "secret")
	req.Header = http.Header{"X-Multi": {"one", "two"}, "host": {"ignored.example"}, "content-length": {"999"}}
	beforeHeader, beforeURL := req.Header.Clone(), *req.URL
	response, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 418 || response.Status != "418 I'm a teapot" || response.Proto != "HTTP/1.1" || response.ProtoMajor != 1 || response.ProtoMinor != 1 || response.Request != req {
		t.Fatalf("response metadata: %+v", response)
	}
	if response.ContentLength != int64(len(payload)) || len(response.Cookies()) != 2 {
		t.Fatalf("response length/cookies: %+v", response)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("response body=%q err=%v", data, err)
	}
	if _, err := response.Body.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("repeat EOF = %v", err)
	}
	if body.closed.Load() != 1 || req.Body != body || !reflect.DeepEqual(req.Header, beforeHeader) || !reflect.DeepEqual(*req.URL, beforeURL) {
		t.Fatalf("request mutated or body closed %d times", body.closed.Load())
	}
}

func TestTransportRedirectsAndCookieJar(t *testing.T) {
	var reached atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "from-go-jar", Path: "/"})
			http.Redirect(w, r, "/end", http.StatusTemporaryRedirect)
			return
		}
		reached.Add(1)
		body, _ := io.ReadAll(r.Body)
		_, _ = fmt.Fprintf(w, "%s|%s|%s|%s", r.Method, body, r.Header.Get("Cookie"), r.Header.Get("Authorization"))
	}))
	defer server.Close()
	transport := testTransport(t, httpclient.Options{})
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Get(server.URL + "/start")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != 307 || reached.Load() != 0 {
		t.Fatal("Rust followed the redirect before CheckRedirect")
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	client = &http.Client{Transport: transport, Jar: jar}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/start", strings.NewReader("replay"))
	req.URL.User = url.UserPassword("user", "pass")
	response, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "POST|replay|session=from-go-jar|Basic dXNlcjpwYXNz" || response.Request.URL.Path != "/end" {
		t.Fatalf("redirect/cookie/auth handling: %q %v", data, err)
	}
	// The shared transport must not store or replay cookies on its own.
	response, err = (&http.Client{Transport: transport}).Get(server.URL + "/end")
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "GET|||" {
		t.Fatalf("transport replayed cookies: %q %v", data, err)
	}
}

func TestTransportStreamingAndTimeout(t *testing.T) {
	for _, kind := range []string{"http-client", "transport", "context", "close", "body-close"} {
		t.Run(kind, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "first")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer server.Close()
			options := httpclient.Options{}
			if kind == "transport" {
				options.Timeout = 150 * time.Millisecond
			}
			transport := testTransport(t, options)
			defer transport.Close()
			client := &http.Client{Transport: transport}
			if kind == "http-client" {
				client.Timeout = 150 * time.Millisecond
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
			response, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.ContentLength != -1 || !reflect.DeepEqual(response.TransferEncoding, []string{"chunked"}) || response.Header.Get("Transfer-Encoding") != "" {
				t.Fatalf("stream framing: %+v", response)
			}
			first := make([]byte, 5)
			if _, err := io.ReadFull(response.Body, first); err != nil || string(first) != "first" {
				t.Fatalf("first chunk = %q %v", first, err)
			}
			done := make(chan error, 1)
			go func() { _, err := io.ReadAll(response.Body); done <- err }()
			want := context.DeadlineExceeded
			switch kind {
			case "context":
				want = context.Canceled
				cancel()
			case "close":
				want = httpclient.ErrTransportClosed
				_ = transport.Close()
			case "body-close":
				want = httpclient.ErrBodyClosed
				_ = response.Body.Close()
			}
			if err := await(t, done); !errors.Is(err, want) {
				t.Fatalf("read = %v, want %v", err, want)
			}
			if _, err := response.Body.Read(make([]byte, 1)); !errors.Is(err, want) {
				t.Fatalf("terminal read changed to %v", err)
			}
		})
	}
}

type blockedUpload struct {
	entered chan struct{}
	done    chan struct{}
	started sync.Once
	stop    sync.Once
	closed  atomic.Int32
}

func (b *blockedUpload) Read([]byte) (int, error) {
	b.started.Do(func() { close(b.entered) })
	<-b.done
	return 0, io.ErrClosedPipe
}

func (b *blockedUpload) Close() error {
	b.closed.Add(1)
	b.stop.Do(func() { close(b.done) })
	return nil
}

func TestTransportCancelsBufferedUpload(t *testing.T) {
	for _, kind := range []string{"context", "legacy-cancel", "close", "timeout"} {
		t.Run(kind, func(t *testing.T) {
			options := httpclient.Options{}
			if kind == "timeout" {
				options.Timeout = 150 * time.Millisecond
			}
			transport := testTransport(t, options)
			body := &blockedUpload{entered: make(chan struct{}), done: make(chan struct{})}
			defer body.stop.Do(func() { close(body.done) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1:1/", body)
			legacy := make(chan struct{})
			if kind == "legacy-cancel" {
				req.Cancel = legacy
			}
			done := make(chan error, 1)
			go func() { _, err := transport.RoundTrip(req); done <- err }()
			await(t, body.entered)
			want := context.Canceled
			switch kind {
			case "context":
				cancel()
			case "legacy-cancel":
				close(legacy)
			case "close":
				want = httpclient.ErrTransportClosed
				_ = transport.Close()
			case "timeout":
				want = context.DeadlineExceeded
			}
			if err := await(t, done); !errors.Is(err, want) {
				t.Fatalf("upload = %v, want %v", err, want)
			}
			if n := body.closed.Load(); n != 1 {
				t.Fatalf("upload closed %d times", n)
			}
		})
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestTransportErrorPathsCloseRequestBody(t *testing.T) {
	transport := testTransport(t, httpclient.Options{})
	readErr := errors.New("upload failed")
	for _, kind := range []string{"nil-url", "scheme", "host", "request-uri", "length", "negative-length", "trailer", "upgrade", "connect", "encoding", "cancelled", "read-error", "invalid-method", "invalid-header"} {
		t.Run(kind, func(t *testing.T) {
			body := &countedBody{Reader: strings.NewReader("body")}
			req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:1/", body)
			switch kind {
			case "nil-url":
				req.URL = nil
			case "scheme":
				req.URL.Scheme = "file"
			case "host":
				req.URL.Host = ""
			case "request-uri":
				req.RequestURI = "/"
			case "length":
				req.ContentLength = 999
			case "negative-length":
				req.ContentLength = -2
			case "trailer":
				req.Header["trailer"] = []string{"X-Trailer"}
			case "upgrade":
				req.Header["upgrade"] = []string{"websocket"}
			case "connect":
				req.Method = http.MethodConnect
			case "encoding":
				req.TransferEncoding = []string{"gzip"}
			case "cancelled":
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			case "read-error":
				body.Reader = failingReader{readErr}
			case "invalid-method":
				req.Method = "BAD METHOD"
			case "invalid-header":
				req.Header.Set("X-Bad", "value\r\ninjected")
			}
			_, err := transport.RoundTrip(req)
			if err == nil || body.closed.Load() != 1 {
				t.Fatalf("err=%v, body closed %d times", err, body.closed.Load())
			}
			if kind == "read-error" && !errors.Is(err, readErr) {
				t.Fatalf("reader error lost: %v", err)
			}
		})
	}
	if _, err := transport.RoundTrip(nil); err == nil {
		t.Fatal("nil request accepted")
	}
	for _, options := range []httpclient.Options{{Timeout: -1}, {ProxyPolicy: "invalid"}, {ChatGPTCookies: []string{"a=b"}}} {
		if unexpected, err := httpclient.NewTransport(options); err == nil {
			_ = unexpected.Close()
			t.Errorf("invalid options accepted: %+v", options)
		}
	}
}

func TestTransportZeroValueConcurrentAndClosed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	transport := &httpclient.Transport{}
	defer transport.Close()
	client := &http.Client{Transport: transport}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			response, err := client.Get(server.URL)
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			if response.Body == nil || response.StatusCode != 204 || response.ContentLength != 0 {
				t.Error("invalid empty response")
				return
			}
			if data, err := io.ReadAll(response.Body); err != nil || len(data) != 0 {
				t.Errorf("empty body = %q %v", data, err)
			}
		})
	}
	wg.Wait()
	_ = transport.Close()
	_ = transport.Close()
	body := &countedBody{Reader: strings.NewReader("closed")}
	req, _ := http.NewRequest(http.MethodPost, server.URL, body)
	if _, err := transport.RoundTrip(req); !errors.Is(err, httpclient.ErrTransportClosed) || body.closed.Load() != 1 {
		t.Fatalf("closed transport = %v, body close count = %d", err, body.closed.Load())
	}
}

func TestTransportRequestMayBeReusedAfterError(t *testing.T) {
	transport := testTransport(t, httpclient.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 64 {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:1/", nil)
		req.Cancel = make(chan struct{})
		if _, err := transport.RoundTrip(req); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		// No asynchronous cancellation watcher may inspect request fields after
		// RoundTrip has failed and closed the upload. Also checked with -race.
		*req = http.Request{}
	}
}

func TestTransportChunkedUploadAndHTTP2Head(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !reflect.DeepEqual(r.TransferEncoding, []string{"chunked"}) {
			t.Errorf("upload encoding = %v", r.TransferEncoding)
		}
		data, _ := io.ReadAll(r.Body)
		_, _ = w.Write(data)
	}))
	defer server.Close()
	transport := testTransport(t, httpclient.Options{})
	req, _ := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("chunked upload"))
	req.TransferEncoding = []string{"chunked"}
	response, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || string(data) != "chunked upload" {
		t.Fatalf("chunked response = %q %v", data, err)
	}

	var tlsRequests atomic.Int32
	tlsServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tlsRequests.Add(1)
		w.Header().Set("Content-Length", "321")
	}))
	tlsServer.EnableHTTP2 = true
	certificate, root := testCertificate(t)
	tlsServer.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	tlsServer.StartTLS()
	defer tlsServer.Close()
	file := filepath.Join(t.TempDir(), "root.pem")
	if err := os.WriteFile(file, root, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_CA_CERTIFICATE", file)
	trusted := testTransport(t, httpclient.Options{})
	head, _ := http.NewRequest(http.MethodHead, tlsServer.URL, nil)
	head.Host = "virtual.example"
	if unexpected, err := (&http.Client{Transport: trusted}).Do(head); err == nil {
		_ = unexpected.Body.Close()
		t.Fatal("unsupported HTTPS authority override was sent")
	}
	if tlsRequests.Load() != 0 {
		t.Fatal("a request reached the wrong HTTP/2 authority")
	}
	head.Host = head.URL.Host
	response, err = (&http.Client{Transport: trusted}).Do(head)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 || response.ProtoMinor != 0 || response.ContentLength != 321 {
		t.Fatalf("HTTP/2 HEAD metadata: %+v", response)
	}
	if body, err := io.ReadAll(response.Body); err != nil || len(body) != 0 {
		t.Fatalf("HEAD body = %q %v", body, err)
	}
}

// forceCleanup waits for both Go cleanup eligibility and the observable native
// socket release. Cleanup execution is asynchronous, so one GC is insufficient.
func forceCleanup(t *testing.T, collected, connectionClosed <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for collected != nil || connectionClosed != nil {
		runtime.GC()
		select {
		case <-collected:
			collected = nil
		case <-connectionClosed:
			connectionClosed = nil
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("unused transport did not release its native connection")
		}
	}
}

func TestTransportCleanupPreservesLiveResponse(t *testing.T) {
	for _, constructor := range []bool{false, true} {
		for _, end := range []string{"eof", "close"} {
			t.Run(fmt.Sprintf("constructor=%t/end=%s", constructor, end), func(t *testing.T) {
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.WriteString(w, "first")
					w.(http.Flusher).Flush()
					select {
					case <-release:
						_, _ = io.WriteString(w, "last")
					case <-r.Context().Done():
					}
				}))
				connectionClosed := make(chan struct{})
				var closed sync.Once
				server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateClosed {
						closed.Do(func() { close(connectionClosed) })
					}
				}
				server.Start()
				defer server.Close()
				defer unblock()
				collected := make(chan struct{})
				response, err := func() (*http.Response, error) {
					transport := &httpclient.Transport{}
					if constructor {
						var err error
						transport, err = httpclient.NewTransport(httpclient.Options{})
						if err != nil {
							return nil, err
						}
					}
					runtime.AddCleanup(transport, func(done chan struct{}) { close(done) }, collected)
					// Return only the response. The http.Client and its Transport have
					// no caller-owned references after this function returns.
					return (&http.Client{Transport: transport}).Get(server.URL)
				}()
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				first := make([]byte, 5)
				if _, err := io.ReadFull(response.Body, first); err != nil || string(first) != "first" {
					t.Fatalf("first response chunk: %q %v", first, err)
				}
				for range 3 {
					runtime.GC()
				}
				select {
				case <-collected:
					t.Fatal("transport was collected while its response was still active")
				default:
				}
				if end == "eof" {
					unblock()
					data, err := io.ReadAll(response.Body)
					if err != nil || string(data) != "last" {
						t.Fatalf("remaining response: %q %v", data, err)
					}
				} else if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
				forceCleanup(t, collected, connectionClosed)
				// Keep the finished body reachable: it must release its Transport
				// reference on EOF/Close, rather than only when the body is GC'd.
				if end == "eof" {
					if _, err := response.Body.Read(first); !errors.Is(err, io.EOF) {
						t.Fatalf("EOF changed after transport collection: %v", err)
					}
				}
				runtime.KeepAlive(response)
			})
		}
	}
}

func TestTransportReusesPoolWithoutExplicitClose(t *testing.T) {
	var connections atomic.Int32
	connectionClosed := make(chan struct{})
	var closed sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "pooled")
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			connections.Add(1)
		case http.StateClosed:
			closed.Do(func() { close(connectionClosed) })
		}
	}
	server.Start()
	defer server.Close()
	collected := make(chan struct{})
	func() {
		transport := &httpclient.Transport{}
		runtime.AddCleanup(transport, func(done chan struct{}) { close(done) }, collected)
		client := &http.Client{Transport: transport}
		defer runtime.KeepAlive(client)
		for range 5 {
			response, err := client.Get(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(response.Body)
			_ = response.Body.Close()
			if err != nil || string(data) != "pooled" {
				t.Fatalf("response = %q %v", data, err)
			}
			runtime.GC()
		}
		if got := connections.Load(); got != 1 {
			t.Fatalf("closing request bodies used %d connections instead of reusing one", got)
		}
	}()
	forceCleanup(t, collected, connectionClosed)
}
