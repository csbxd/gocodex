//go:build cgo && linux && (amd64 || arm64)

package websocket_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
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

	codexws "github.com/csbxd/gocodex/websocket"
	"github.com/gorilla/websocket"
)

func TestMain(m *testing.M) {
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy", "CODEX_CA_CERTIFICATE", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		_ = os.Unsetenv(key)
	}
	os.Exit(m.Run())
}

func newClient(t *testing.T, options codexws.Options) *codexws.Client {
	t.Helper()
	c, err := codexws.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
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

func serve(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		handler(conn)
	}))
	t.Cleanup(s.Close)
	return s
}

func echo(conn *websocket.Conn) {
	for {
		kind, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if conn.WriteMessage(kind, data) != nil {
			return
		}
	}
}

func dial(t *testing.T, c *codexws.Client, s *httptest.Server) *codexws.Conn {
	t.Helper()
	conn, handshake, err := c.Dial(deadline(t), "ws"+strings.TrimPrefix(s.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if handshake.StatusCode != 101 {
		t.Fatal(handshake)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestMessagesAndHandshake(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer local-test" || r.URL.RawQuery != "q=1" {
			t.Error("handshake metadata was lost")
		}
		if !reflect.DeepEqual(r.Header.Values("X-Multi"), []string{"one", "two"}) {
			t.Error("duplicate handshake headers were lost")
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"chat"}}
		conn, err := upgrader.Upgrade(w, r, http.Header{"Set-Cookie": {"a=1", "b=2"}})
		if err != nil {
			return
		}
		defer conn.Close()
		echo(conn)
	}))
	defer s.Close()
	c := newClient(t, codexws.Options{TCPNoDelay: true})
	ctx, cancel := context.WithCancel(deadline(t))
	conn, hs, err := c.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http")+"?q=1", http.Header{
		"Authorization": {"Bearer local-test"}, "X-Multi": {"one", "two"}, "Sec-WebSocket-Protocol": {"chat"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cancel() // The handshake's context must not govern the established connection.
	if hs.Headers.Get("Sec-WebSocket-Protocol") != "chat" || !reflect.DeepEqual(hs.Headers.Values("Set-Cookie"), []string{"a=1", "b=2"}) {
		t.Fatalf("handshake response = %+v", hs)
	}
	for _, message := range []struct {
		kind codexws.MessageType
		data []byte
	}{
		{codexws.TextMessage, []byte("你好，Codex\x00")},
		{codexws.BinaryMessage, bytes.Repeat([]byte{0, 255, 128, 1}, 100000)},
		{codexws.BinaryMessage, nil},
	} {
		if err := conn.WriteMessage(deadline(t), message.kind, message.data); err != nil {
			t.Fatal(err)
		}
		kind, data, err := conn.ReadMessage(deadline(t))
		if err != nil || kind != message.kind || !bytes.Equal(data, message.data) {
			t.Fatalf("echo: type=%d len=%d err=%v", kind, len(data), err)
		}
	}
}

func TestConcurrentReadAndWrite(t *testing.T) {
	s := serve(t, echo)
	c := newClient(t, codexws.Options{})
	conn := dial(t, c, s)
	ctx := deadline(t)
	done := make(chan error, 1)
	go func() {
		for i := range 50 {
			_, data, err := conn.ReadMessage(ctx)
			if err != nil {
				done <- err
				return
			}
			if !bytes.Equal(data, []byte{byte(i)}) {
				done <- errors.New("message order changed")
				return
			}
		}
		done <- nil
	}()
	for i := range 50 {
		if err := conn.WriteMessage(ctx, codexws.BinaryMessage, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestPingPongAndClose(t *testing.T) {
	pong := make(chan string, 1)
	s := serve(t, func(conn *websocket.Conn) {
		conn.SetPongHandler(func(data string) error { pong <- data; return nil })
		_ = conn.WriteMessage(websocket.PingMessage, []byte("server-ping"))
		echo(conn)
	})
	c := newClient(t, codexws.Options{})
	conn := dial(t, c, s)
	kind, data, err := conn.ReadMessage(deadline(t))
	if err != nil || kind != codexws.PingMessage || string(data) != "server-ping" {
		t.Fatalf("ping: %d %q %v", kind, data, err)
	}
	if got := await(t, pong); got != "server-ping" {
		t.Fatal(got)
	}
	if err := conn.WriteMessage(deadline(t), codexws.PingMessage, []byte("client-ping")); err != nil {
		t.Fatal(err)
	}
	kind, data, err = conn.ReadMessage(deadline(t))
	if err != nil || kind != codexws.PongMessage || string(data) != "client-ping" {
		t.Fatalf("pong: %d %q %v", kind, data, err)
	}
	if err := conn.WriteMessage(deadline(t), codexws.CloseMessage, codexws.ClosePayload(1000, "done")); err != nil {
		t.Fatal(err)
	}
	kind, data, err = conn.ReadMessage(deadline(t))
	if err != nil || kind != codexws.CloseMessage {
		t.Fatalf("close: %d %v", kind, err)
	}
	code, _, err := codexws.ParseClosePayload(data)
	if err != nil || code != 1000 {
		t.Fatalf("close payload: %d %v", code, err)
	}
	_, _, err = conn.ReadMessage(deadline(t))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("clean close = %v", err)
	}
}

func TestCancellationAndCloseInterruptRead(t *testing.T) {
	for _, action := range []string{"context", "connection", "client"} {
		t.Run(action, func(t *testing.T) {
			s := serve(t, echo)
			c := newClient(t, codexws.Options{})
			conn := dial(t, c, s)
			ctx, cancel := context.WithCancel(deadline(t))
			defer cancel()
			done := make(chan error, 1)
			go func() { _, _, err := conn.ReadMessage(ctx); done <- err }()
			switch action {
			case "context":
				cancel()
			case "connection":
				_ = conn.Close()
			case "client":
				_ = c.Close()
			}
			err := await(t, done)
			want := map[string]error{"context": context.Canceled, "connection": codexws.ErrConnClosed, "client": codexws.ErrClientClosed}[action]
			if !errors.Is(err, want) {
				t.Fatalf("error=%v want=%v", err, want)
			}
		})
	}
}

func TestHandshakeTimeoutAndCancellation(t *testing.T) {
	for _, useContext := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "context"}[useContext], func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
			defer s.Close()
			options := codexws.Options{HandshakeTimeout: 100 * time.Millisecond}
			ctx := deadline(t)
			if useContext {
				options.HandshakeTimeout = 0
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
			}
			c := newClient(t, options)
			_, _, err := c.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), nil)
			if useContext {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			} else {
				var native *codexws.Error
				if !errors.As(err, &native) || !native.Timeout() {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestTLSCustomCA(t *testing.T) {
	upgrader := websocket.Upgrader{}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		echo(conn)
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	certificate, root := testCertificate(t)
	s.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
	s.StartTLS()
	defer s.Close()
	c := newClient(t, codexws.Options{})
	url := "wss" + strings.TrimPrefix(s.URL, "https")
	if unexpected, _, err := c.Dial(deadline(t), url, nil); err == nil {
		_ = unexpected.Close()
		t.Fatal("untrusted TLS was accepted")
	}
	file := filepath.Join(t.TempDir(), "root.pem")
	if err := os.WriteFile(file, root, 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_CA_CERTIFICATE", file)
	trusted := newClient(t, codexws.Options{})
	conn, _, err := trusted.Dial(deadline(t), url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(deadline(t), codexws.TextMessage, []byte("TLS")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage(deadline(t))
	if err != nil || string(data) != "TLS" {
		t.Fatalf("TLS echo: %q %v", data, err)
	}
}

func TestProxyTunnelAndLoopbackRestriction(t *testing.T) {
	s := serve(t, echo)
	var used atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != "unresolvable.invalid:80" {
			t.Errorf("unexpected proxy request: %s %s", r.Method, r.Host)
			http.Error(w, "bad", 400)
			return
		}
		upstream, err := net.Dial("tcp", strings.TrimPrefix(s.URL, "http://"))
		if err != nil {
			t.Error(err)
			return
		}
		defer upstream.Close()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		used.Add(1)
		_, _ = io.WriteString(conn, "HTTP/1.1 200 Connection Established\r\n\r\n")
		done := make(chan struct{})
		go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close(); close(done) }()
		_, _ = io.Copy(conn, upstream)
		_ = conn.Close()
		<-done
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	c := newClient(t, codexws.Options{})
	conn, _, err := c.Dial(deadline(t), "ws://unresolvable.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.WriteMessage(deadline(t), codexws.TextMessage, []byte("proxy")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage(deadline(t))
	if err != nil || string(data) != "proxy" || used.Load() != 1 {
		t.Fatalf("proxy echo: %q %v", data, err)
	}
	direct := newClient(t, codexws.Options{LoopbackDirect: true})
	local := dial(t, direct, s)
	_ = local.Close()
	if unexpected, _, err := direct.Dial(deadline(t), "ws://example.com/", nil); err == nil {
		_ = unexpected.Close()
		t.Fatal("non-loopback direct destination accepted")
	}
}

func TestInvalidInputAndLimits(t *testing.T) {
	c := newClient(t, codexws.Options{})
	for _, url := range []string{"bad url", "http://localhost/", "file:///etc/hosts"} {
		if conn, _, err := c.Dial(deadline(t), url, nil); err == nil {
			_ = conn.Close()
			t.Errorf("accepted %s", url)
		}
	}
	for _, options := range []codexws.Options{{HandshakeTimeout: -1}, {MaxFrameSize: -1}, {MaxMessageSize: -1}, {ProxyPolicy: "bad"}, {ChatGPTCookies: []string{"a\r\nb"}}} {
		if client, err := codexws.NewClient(options); err == nil {
			_ = client.Close()
			t.Errorf("accepted %+v", options)
		}
	}
	s := serve(t, echo)
	conn := dial(t, c, s)
	for _, message := range []struct {
		kind codexws.MessageType
		data []byte
	}{
		{99, nil}, {codexws.TextMessage, []byte{255}}, {codexws.PingMessage, make([]byte, 126)},
		{codexws.CloseMessage, []byte{1}}, {codexws.CloseMessage, codexws.ClosePayload(1005, "")},
	} {
		if err := conn.WriteMessage(deadline(t), message.kind, message.data); err == nil {
			t.Errorf("invalid message accepted: %d", message.kind)
		}
	}
	if err := conn.WriteMessage(nil, codexws.TextMessage, nil); err == nil {
		t.Error("nil context accepted")
	}
	if err := conn.WriteMessage(deadline(t), codexws.TextMessage, []byte("still usable")); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.ReadMessage(deadline(t))
	if err != nil || string(data) != "still usable" {
		t.Fatalf("invalid input broke connection: %q %v", data, err)
	}
	limited := dial(t, newClient(t, codexws.Options{MaxMessageSize: 8}), s)
	if err := limited.WriteMessage(deadline(t), codexws.BinaryMessage, make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := limited.ReadMessage(deadline(t)); err == nil {
		t.Error("message size limit ignored")
	}
}

func TestClientCloseRacesWithDial(t *testing.T) {
	s := serve(t, echo)
	for range 10 {
		c := newClient(t, codexws.Options{})
		ctx := deadline(t)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				conn, _, err := c.Dial(ctx, "ws"+strings.TrimPrefix(s.URL, "http"), nil)
				if err == nil {
					_ = conn.Close()
				}
			})
		}
		_ = c.Close()
		wg.Wait()
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
