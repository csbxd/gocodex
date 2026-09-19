//go:build cgo && linux && (amd64 || arm64)

package httpclient_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	codexhttp "github.com/csbxd/gocodex/httpclient"
)

// Proxy and CA behavior is tested explicitly; ambient settings must not route
// local test traffic through the developer's proxy or custom trust store.
func TestMain(m *testing.M) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		_ = os.Unsetenv(key)
	}
	os.Exit(m.Run())
}

func testClient(t *testing.T, options codexhttp.Options) *codexhttp.Client {
	t.Helper()
	client, err := codexhttp.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}

func readBody(t *testing.T, response *codexhttp.Response) []byte {
	t.Helper()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not finish within 5s")
		var zero T
		return zero
	}
}

func TestBinaryRequestAndHeaders(t *testing.T) {
	payload := bytes.Repeat([]byte{0, 255, 128, 'x', '\n'}, 50000)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.RawQuery != "q=a%20b" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		if r.UserAgent() != "codexhttp-test" {
			t.Errorf("User-Agent = %q", r.UserAgent())
		}
		if values := r.Header.Values("X-Multi"); !reflect.DeepEqual(values, []string{"one", "two"}) {
			t.Errorf("duplicate request headers = %q", values)
		}
		if value := r.Header.Get("X-Binary"); value != "\x80\xff" {
			t.Errorf("binary header = %q", value)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(body, payload) {
			t.Errorf("request body: len=%d, err=%v", len(body), err)
		}
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.Header().Set("X-Binary", "\x80\xff")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	}))
	defer server.Close()
	client := testClient(t, codexhttp.Options{UserAgent: "codexhttp-test"})
	response, err := client.Do(context.Background(), codexhttp.Request{
		Method: http.MethodPut, URL: server.URL + "/echo?q=a%20b", Body: payload,
		Headers: http.Header{"X-Multi": {"one", "two"}, "X-Binary": {"\x80\xff"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 201 || response.Protocol != "HTTP/1.1" || response.ContentLength != int64(len(payload)) {
		t.Fatalf("response metadata: %+v", response)
	}
	if !reflect.DeepEqual(response.Headers.Values("Set-Cookie"), []string{"a=1", "b=2"}) || response.Headers.Get("X-Binary") != "\x80\xff" {
		t.Fatalf("response headers: %v", response.Headers)
	}
	// A small buffer exercises partial consumption of each Rust Bytes chunk.
	var received bytes.Buffer
	if _, err := io.CopyBuffer(struct{ io.Writer }{&received}, response.Body, make([]byte, 7)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(received.Bytes(), payload) {
		t.Fatalf("binary response changed: received %d bytes", received.Len())
	}
	if n, err := response.Body.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("repeat EOF = %d, %v", n, err)
	}
}

func TestJSONAndHTTPErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("JSON request: %s, %v", r.Method, r.Header)
		}
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = io.Copy(w, r.Body)
	}))
	defer server.Close()
	client := testClient(t, codexhttp.Options{})
	for _, value := range []any{
		map[string]any{"hello": "世界", "id": uint64(18446744073709551615)},
		json.RawMessage(`{"n":123456789012345678901234567890,"decimal":0.1234567890123456789}`),
		nil,
	} {
		want, _ := json.Marshal(value)
		response, err := client.PostJSON(context.Background(), server.URL, value)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != 422 {
			t.Fatalf("status = %d", response.StatusCode)
		}
		if got := readBody(t, response); !bytes.Equal(got, want) {
			t.Fatalf("JSON changed: got %s, want %s", got, want)
		}
	}
}

func TestStreamingArrivesBeforeServerFinishes(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
			_, _ = io.WriteString(w, "data: second\n\n")
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := testClient(t, codexhttp.Options{})
	response, err := client.Get(ctx, server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ContentLength != -1 {
		t.Fatalf("chunked content length = %d", response.ContentLength)
	}
	first := make([]byte, len("data: first\n\n"))
	if _, err := io.ReadFull(response.Body, first); err != nil {
		t.Fatal(err)
	}
	if string(first) != "data: first\n\n" {
		t.Fatalf("first event = %q", first)
	}
	// The server cannot finish until the client has already consumed event one.
	close(release)
	if rest := readBody(t, response); string(rest) != "data: second\n\n" {
		t.Fatalf("remaining events = %q", rest)
	}
}

func TestCancellationBeforeHeaders(t *testing.T) {
	for _, mode := range []string{"context", "client"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			left := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-r.Context().Done()
				close(left)
			}))
			defer server.Close()
			client := testClient(t, codexhttp.Options{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer client.Close()
			done := make(chan error, 1)
			go func() {
				response, err := client.Get(ctx, server.URL)
				if response != nil {
					response.Body.Close()
				}
				done <- err
			}()
			await(t, entered)
			want := context.Canceled
			if mode == "client" {
				want = codexhttp.ErrClientClosed
				_ = client.Close()
			} else {
				cancel()
			}
			if err := await(t, done); !errors.Is(err, want) {
				t.Fatalf("cancelled request = %v, want %v", err, want)
			}
			await(t, left) // Cancellation closes the underlying network operation.
		})
	}
}

func TestBlockedReadCancellation(t *testing.T) {
	for _, mode := range []string{"context", "body", "client"} {
		t.Run(mode, func(t *testing.T) {
			left := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				<-r.Context().Done()
				close(left)
			}))
			defer server.Close()
			client := testClient(t, codexhttp.Options{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.Get(ctx, server.URL)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			done := make(chan error, 1)
			go func() {
				_, err := response.Body.Read(make([]byte, 16))
				done <- err
			}()
			// No response data exists, so the read must wait for cancellation.
			select {
			case err := <-done:
				t.Fatalf("read returned before cancellation: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			var want error
			switch mode {
			case "context":
				want = context.Canceled
				cancel()
			case "body":
				want = codexhttp.ErrBodyClosed
				_ = response.Body.Close()
			case "client":
				want = codexhttp.ErrClientClosed
				_ = client.Close()
			}
			if err := await(t, done); !errors.Is(err, want) {
				t.Fatalf("read = %v, want %v", err, want)
			}
			await(t, left)
		})
	}
}

func TestDeadlineAndSDKTimeout(t *testing.T) {
	for _, body := range []bool{false, true} {
		for _, useContext := range []bool{false, true} {
			t.Run(fmt.Sprintf("body=%v/context=%v", body, useContext), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if body {
						w.WriteHeader(200)
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
				}))
				defer server.Close()
				options := codexhttp.Options{}
				ctx := context.Background()
				if useContext {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
					defer cancel()
				} else {
					options.Timeout = 100 * time.Millisecond
				}
				client := testClient(t, options)
				response, err := client.Get(ctx, server.URL)
				if body {
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					_, err = io.ReadAll(response.Body)
				}
				if useContext {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("deadline = %v", err)
					}
				} else {
					var native *codexhttp.Error
					if !errors.As(err, &native) || !native.Timeout() {
						t.Fatalf("SDK timeout = %v", err)
					}
				}
			})
		}
	}
}

func testCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "codexhttp test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
}

func TestTLSVerificationAndNativeRoots(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "verified TLS")
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.EnableHTTP2 = true
	certificate, root := testCertificate(t)
	server.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	server.StartTLS()
	defer server.Close()
	client := testClient(t, codexhttp.Options{})
	if response, err := client.Get(context.Background(), server.URL); err == nil {
		response.Body.Close()
		t.Fatal("untrusted TLS certificate was accepted")
	}
	certFile := filepath.Join(t.TempDir(), "roots.pem")
	if err := os.WriteFile(certFile, root, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_CA_CERTIFICATE", certFile)
	trusted := testClient(t, codexhttp.Options{})
	response, err := trusted.Get(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if response.Protocol != "HTTP/2.0" {
		response.Body.Close()
		t.Fatalf("default HTTP/2 feature was not negotiated: %s", response.Protocol)
	}
	if string(readBody(t, response)) != "verified TLS" {
		t.Fatal("wrong HTTPS body")
	}
	// rustls-native-certs honors SSL_CERT_FILE; no custom root option here.
	t.Setenv("CODEX_CA_CERTIFICATE", "")
	t.Setenv("SSL_CERT_FILE", certFile)
	t.Setenv("SSL_CERT_DIR", t.TempDir())
	nativeRoots := testClient(t, codexhttp.Options{})
	response, err = nativeRoots.Get(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("native certificate roots were not loaded: %v", err)
	}
	_ = readBody(t, response)
}

func TestRedirectAndProxy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", 302)
			return
		}
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	defer server.Close()
	for _, disable := range []bool{false, true} {
		client := testClient(t, codexhttp.Options{DisableRedirects: disable})
		response, err := client.Get(context.Background(), server.URL+"/redirect")
		if err != nil {
			t.Fatal(err)
		}
		_ = readBody(t, response)
		if disable && response.StatusCode != 302 || !disable && (response.StatusCode != 200 || response.URL != server.URL+"/final") {
			t.Fatalf("redirect response: %+v", response)
		}
	}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "http://unresolvable.invalid/resource" {
			t.Errorf("proxy request = %s", r.URL)
		}
		_, _ = io.WriteString(w, "proxied")
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	client := testClient(t, codexhttp.Options{})
	response, err := client.Get(context.Background(), "http://unresolvable.invalid/resource")
	if err != nil {
		t.Fatal(err)
	}
	if string(readBody(t, response)) != "proxied" {
		t.Fatal("request did not use proxy")
	}
}

func TestPoolReuseAndConcurrentRequests(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.URL.Path)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	client := testClient(t, codexhttp.Options{})
	for range 5 {
		response, err := client.Get(context.Background(), server.URL)
		if err != nil {
			t.Fatal(err)
		}
		_ = readBody(t, response)
	}
	if count := connections.Load(); count != 1 {
		t.Fatalf("sequential requests used %d connections", count)
	}
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/request/%d", i)
			response, err := client.Get(context.Background(), server.URL+path)
			if err != nil {
				t.Error(err)
				return
			}
			defer response.Body.Close()
			data, err := io.ReadAll(response.Body)
			if err != nil || string(data) != path {
				t.Errorf("concurrent response: %q, %v", data, err)
			}
		}()
	}
	wg.Wait()
}

func TestInputErrorsAndClosedClient(t *testing.T) {
	client := testClient(t, codexhttp.Options{})
	for _, request := range []codexhttp.Request{
		{URL: "not a URL"}, {URL: "file:///etc/hosts"},
		{URL: "http://127.0.0.1", Method: "BAD METHOD"},
		{URL: "http://127.0.0.1", Headers: http.Header{"X-Bad": {"a\r\nb"}}},
		{URL: "http://127.0.0.1", Body: []byte{}, JSON: true},
		{URL: "http://127.0.0.1", JSON: make(chan int)},
		{URL: "http://127.0.0.1", JSON: json.RawMessage("invalid JSON")},
	} {
		if _, err := client.Do(context.Background(), request); err == nil {
			t.Errorf("invalid request accepted: %+v", request)
		}
	}
	for _, options := range []codexhttp.Options{
		{Timeout: -time.Second}, {ProxyPolicy: "invalid"},
		{UserAgent: "bad\r\nagent"}, {ChatGPTCookies: []string{"bad\r\ncookie"}},
	} {
		if unexpected, err := codexhttp.NewClient(options); err == nil {
			unexpected.Close()
			t.Errorf("invalid options accepted: %+v", options)
		}
	}
	if _, err := client.Get(nil, "http://127.0.0.1"); err == nil {
		t.Error("nil context accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Get(ctx, "http://127.0.0.1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled context = %v", err)
	}
	_ = client.Close()
	_ = client.Close()
	if _, err := client.Get(context.Background(), "http://127.0.0.1"); !errors.Is(err, codexhttp.ErrClientClosed) {
		t.Fatalf("closed client = %v", err)
	}
}

func TestTruncatedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "short")
	}))
	defer server.Close()
	client := testClient(t, codexhttp.Options{})
	response, err := client.Get(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err == nil || !strings.Contains(string(data), "short") {
		t.Fatalf("truncated response = %q, %v", data, err)
	}
}

func TestClientCloseRacesWithRequestCreation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	for range 10 {
		client := testClient(t, codexhttp.Options{})
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				response, err := client.Get(context.Background(), server.URL)
				if err == nil {
					_, err = io.ReadAll(response.Body)
					_ = response.Body.Close()
				}
				if err != nil && !errors.Is(err, codexhttp.ErrClientClosed) {
					t.Errorf("close/request race: %v", err)
				}
			}()
		}
		close(start)
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		wg.Wait()
	}
}

func TestHeadAndEmptyResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := testClient(t, codexhttp.Options{})
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		response, err := client.Do(context.Background(), codexhttp.Request{Method: method, URL: server.URL})
		if err != nil {
			t.Fatal(err)
		}
		if n, err := response.Body.Read(nil); n != 0 || err != nil {
			t.Fatalf("zero-length read = %d, %v", n, err)
		}
		if got := readBody(t, response); len(got) != 0 {
			t.Fatalf("empty response = %q", got)
		}
		if _, err := response.Body.Read(make([]byte, 1)); !errors.Is(err, codexhttp.ErrBodyClosed) {
			t.Fatalf("read after close = %v", err)
		}
	}
}
